//! Tests targeting uncovered code paths in `src/format.rs`.
//!
//! Covers: V1 format migration, LegacyFormat wrapped expansion, V2 element
//! header / footer validation errors, `parse_versioned_header` /
//! `parse_legacy_header` overflow guards, and `validate_v2_slot_data` error
//! paths.

use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use queue_file::{Error, QueueFile};

// ── Constants ─────────────────────────────────────────────────────────────────

const V2_MAGIC: u32 = 0x5146_4D48;
const V2_SLOT_A_OFFSET: u64 = 0;
const V2_SLOT_B_OFFSET: u64 = 4096;
const V2_SLOT_LEN: usize = 56;
const V2_DATA_START: u64 = 8192;
const V2_INITIAL_LEN: u64 = 8192;

/// Magic for V1 / versioned header format: `0x8000_0001`.
const V1_VERSIONED_HEADER: u32 = 0x8000_0001;

/// Byte length of a V2 element header.
const V2_ELEM_HDR_LEN: usize = 28;

// ── Helper functions ───────────────────────────────────────────────────────────

fn temp_path() -> auto_delete_path::AutoDeletePath {
    auto_delete_path::AutoDeletePath::temp()
}

/// Overwrite bytes at `offset` in an existing file.
fn patch_file(path: impl AsRef<Path>, offset: u64, data: &[u8]) {
    let mut f = fs::OpenOptions::new().write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.write_all(data).unwrap();
}

/// Build a 56-byte V2 header slot with correct CRC.
fn build_v2_slot(
    file_len: u64,
    elem_cnt: u32,
    first_pos: u64,
    last_pos: u64,
    generation: u64,
    next_seq: u64,
) -> [u8; V2_SLOT_LEN] {
    let mut buf = [0u8; V2_SLOT_LEN];
    buf[0..4].copy_from_slice(&V2_MAGIC.to_be_bytes());
    buf[4] = 2; // version
    buf[8..16].copy_from_slice(&(file_len as i64).to_be_bytes());
    buf[16..20].copy_from_slice(&(elem_cnt as i32).to_be_bytes());
    buf[20..28].copy_from_slice(&(first_pos as i64).to_be_bytes());
    buf[28..36].copy_from_slice(&(last_pos as i64).to_be_bytes());
    buf[36..44].copy_from_slice(&(generation as i64).to_be_bytes());
    buf[44..52].copy_from_slice(&(next_seq as i64).to_be_bytes());
    let crc = crc32fast::hash(&buf[..52]);
    buf[52..56].copy_from_slice(&crc.to_be_bytes());
    buf
}

/// Create a `V2_INITIAL_LEN`-byte file with one valid slot at offset 0.
/// Slot B is left as zeros (invalid magic → ignored by elect_canonical_slot).
fn create_v2_file_with_slot_a(path: impl AsRef<Path>, slot: [u8; V2_SLOT_LEN]) {
    let path = path.as_ref();
    fs::write(path, vec![0u8; V2_INITIAL_LEN as usize]).unwrap();
    patch_file(path, V2_SLOT_A_OFFSET, &slot);
}

/// Write a raw 32-byte V1 (versioned) header into `buf`.
///
/// `elem_cnt` elements of `payload` are placed at data_start (offset 32),
/// each prefixed by a 4-byte big-endian length.
fn build_v1_file(elements: &[&[u8]]) -> Vec<u8> {
    let data_start: usize = 32;
    let data_size: usize = elements.iter().map(|e| 4 + e.len()).sum();
    let total = data_start + data_size;
    let file_len = total.next_power_of_two().max(4096) as u64;

    let mut data = vec![0u8; file_len as usize];

    // Header
    data[0..4].copy_from_slice(&V1_VERSIONED_HEADER.to_be_bytes());
    data[4..12].copy_from_slice(&file_len.to_be_bytes()); // file_len as u64
    data[12..16].copy_from_slice(&(elements.len() as i32).to_be_bytes()); // elem_cnt

    if !elements.is_empty() {
        let first_phys: u64 = data_start as u64;
        let mut pos = data_start;
        let mut last_phys = first_phys;

        for elem in elements {
            last_phys = pos as u64;
            data[pos..pos + 4].copy_from_slice(&(elem.len() as u32).to_be_bytes());
            data[pos + 4..pos + 4 + elem.len()].copy_from_slice(elem);
            pos += 4 + elem.len();
        }

        data[16..24].copy_from_slice(&first_phys.to_be_bytes()); // first_phys
        data[24..32].copy_from_slice(&last_phys.to_be_bytes()); // last_phys
    }

    data
}

// ── V1 format migration ────────────────────────────────────────────────────────

/// An empty V1 file is recognised, migrated to V2, and reopens successfully.
///
/// Covers: `parse_versioned_header` (happy path), `V1Format::layout`,
/// `V1Format::elem_hdr_len`, `V1Format::elem_span`, `FormatState::V1` dispatch.
#[test]
fn v1_empty_file_migrates_to_v2() {
    let path = temp_path();

    fs::write(&path, build_v1_file(&[])).unwrap();

    let qf = QueueFile::open(&path).unwrap();
    assert!(qf.is_empty());
}

/// A V1 file with two elements is migrated and all elements survive.
///
/// Covers: `V1Format::read_element`, `V1Format::validate_footer` (no-op),
/// `FormatState::V1` read dispatch, `parse_versioned_header` with elem positions.
#[test]
fn v1_file_with_elements_migrates_to_v2() {
    let path = temp_path();

    fs::write(&path, build_v1_file(&[b"hello", b"world"])).unwrap();

    let mut qf = QueueFile::open(&path).unwrap();
    assert_eq!(qf.size(), 2);

    let first = qf.peek().unwrap().unwrap();
    assert_eq!(first, b"hello");
    qf.remove().unwrap();

    let second = qf.peek().unwrap().unwrap();
    assert_eq!(second, b"world");
}

// ── LegacyFormat::on_expansion with wrapped queue ─────────────────────────────

/// A legacy queue that has wrapped around is expanded correctly.
///
/// Triggers `LegacyFormat::on_expansion` with `last.pos < first.pos`.
///
/// Setup (all elements have 100-byte payload → 104-byte span):
///   - Add 39 elements  → tail at logical 3952, ring used 39×104 = 4056 B
///   - Remove 2          → head advances to logical 208
///   - Add 1             → tail at 4056 (fits, end 4160 wraps at ring cap 4080)
///   - Add 1             → wraps to logical 80 (last.pos=80 < first.pos=208) ✓
///   - Add 1             → free space 208-184=24 < 104 → expansion with wrapping
#[test]
fn legacy_on_expansion_wrapped() {
    let path = temp_path();
    let mut qf = QueueFile::open_legacy(&path).unwrap();
    // Ring capacity = file_len(4096) - data_start(16) = 4080 bytes.
    // Each element: 4-byte header + 100-byte payload = 104 bytes.
    let payload = vec![0u8; 100];

    for _ in 0..39 {
        qf.add(&payload).unwrap();
    }
    qf.remove_n(2).unwrap();

    // These two pushes cause the tail to wrap.
    qf.add(&payload).unwrap(); // tail → 4056 (end 4160, wraps at 4080)
    qf.add(&payload).unwrap(); // tail → (4056+104)%4080 = 80 < head 208

    // This add has only 24 bytes of free space left before the head,
    // so an expansion is required while the queue is in the wrapped state.
    qf.add(&payload).unwrap();

    // All 39 - 2 + 3 = 40 elements must still be intact.
    assert_eq!(qf.size(), 40);
}

// ── V2 element header validation errors ───────────────────────────────────────

/// Physical offset of the first element in a freshly-grown V2 file.
///
/// A new V2 queue has file_len == data_start == 8192 (ring capacity 0).
/// Adding one element causes a doubling to 16384; the element lands at the
/// start of the data region, i.e. physical offset V2_DATA_START.
const FIRST_ELEM_PHYS: u64 = V2_DATA_START;

/// Returns the physical offset of the first element's footer given a payload len.
fn footer_phys(payload_len: u64) -> u64 {
    FIRST_ELEM_PHYS + V2_ELEM_HDR_LEN as u64 + payload_len
}

/// Opening a V2 queue whose first element header has a wrong magic value fails.
///
/// Covers `validate_v2_element_header` → magic-mismatch branch.
#[test]
fn v2_element_header_magic_mismatch() {
    let path = temp_path();
    {
        let mut qf = QueueFile::open(&path).unwrap();
        qf.add(b"hello").unwrap();
    }

    // Overwrite the element magic with zeros.
    patch_file(&path, FIRST_ELEM_PHYS, &[0u8; 4]);

    let err = QueueFile::open(&path).unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

/// Opening a V2 queue whose element header carries a negative `payload_len` fails.
///
/// Covers `validate_v2_element_header` → negative-payload-len branch.
/// The magic must be preserved so the magic check passes; the payload_len check
/// fires before the CRC check in the validation sequence.
#[test]
fn v2_element_header_negative_payload_len() {
    let path = temp_path();
    {
        let mut qf = QueueFile::open(&path).unwrap();
        qf.add(b"hello").unwrap();
    }

    // Patch only the payload_len field (bytes 20-23 relative to element start).
    patch_file(&path, FIRST_ELEM_PHYS + 20, &0xFFFF_FFFFu32.to_be_bytes());

    let err = QueueFile::open(&path).unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

/// Opening a V2 queue whose element header has `seq = 0` fails.
///
/// Covers `validate_v2_element_header` → seq-less-than-1 branch.
/// The seq check fires before the CRC check.
#[test]
fn v2_element_header_seq_zero() {
    let path = temp_path();
    {
        let mut qf = QueueFile::open(&path).unwrap();
        qf.add(b"hello").unwrap();
    }

    // Patch the seq field (bytes 4-11) to zero while leaving magic intact.
    patch_file(&path, FIRST_ELEM_PHYS + 4, &[0u8; 8]);

    let err = QueueFile::open(&path).unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

/// Opening a V2 queue with a CRC-corrupted element header fails.
///
/// Covers `validate_v2_element_header` → CRC-mismatch branch.
/// Magic, seq, and payload_len are left valid; only the stored CRC is wrong.
#[test]
fn v2_element_header_crc_mismatch() {
    let path = temp_path();
    {
        let mut qf = QueueFile::open(&path).unwrap();
        qf.add(b"hello").unwrap();
    }

    // Patch only the 4-byte header CRC (bytes 24-27 relative to element start).
    patch_file(&path, FIRST_ELEM_PHYS + 24, &[0u8; 4]);

    let err = QueueFile::open(&path).unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

// ── V2 element footer validation errors ───────────────────────────────────────

/// Peeking into a V2 queue whose element footer has a wrong magic value fails.
///
/// Covers `validate_v2_footer` → magic-mismatch branch.
#[test]
fn v2_element_footer_magic_mismatch() {
    let path = temp_path();
    {
        let mut qf = QueueFile::open(&path).unwrap();
        qf.add(b"hello").unwrap();
    }

    // Footer starts right after header (28 B) + payload (5 B).
    let ftr_offset = footer_phys(5);
    patch_file(&path, ftr_offset, &[0u8; 4]);

    let qf = QueueFile::open(&path).unwrap();
    let err = qf.peek().unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

/// Peeking into a V2 queue whose footer seq doesn't match the header seq fails.
///
/// Covers `validate_v2_footer` → seq-mismatch branch.
/// The footer magic is kept correct; only the seq field is changed.
#[test]
fn v2_element_footer_seq_mismatch() {
    let path = temp_path();
    {
        let mut qf = QueueFile::open(&path).unwrap();
        qf.add(b"hello").unwrap();
    }

    // Footer seq is at bytes 4-11 relative to footer start; change to seq=2.
    let ftr_offset = footer_phys(5);
    patch_file(&path, ftr_offset + 4, &2u64.to_be_bytes());

    let qf = QueueFile::open(&path).unwrap();
    let err = qf.peek().unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

/// Peeking into a V2 queue with a CRC-corrupted element footer fails.
///
/// Covers `validate_v2_footer` → CRC-mismatch branch.
/// Footer magic and seq are left valid; only the stored footer CRC is wrong.
#[test]
fn v2_element_footer_crc_mismatch() {
    let path = temp_path();
    {
        let mut qf = QueueFile::open(&path).unwrap();
        qf.add(b"hello").unwrap();
    }

    // Footer CRC is at bytes 12-15 relative to footer start.
    let ftr_offset = footer_phys(5);
    patch_file(&path, ftr_offset + 12, &[0u8; 4]);

    let qf = QueueFile::open(&path).unwrap();
    let err = qf.peek().unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

// ── parse_versioned_header error paths ────────────────────────────────────────

/// Opening a file whose versioned header claims version 2 (unsupported) fails.
///
/// Covers `parse_versioned_header` → unsupported-version branch.
#[test]
fn versioned_header_unsupported_version() {
    let path = temp_path();

    // First 4 bytes: high bit set (versioned) + version=2.
    let mut data = vec![0u8; 4096];
    data[0..4].copy_from_slice(&0x8000_0002u32.to_be_bytes());
    data[4..12].copy_from_slice(&4096u64.to_be_bytes()); // file_len
    fs::write(&path, &data).unwrap();

    let err = QueueFile::open(&path).unwrap_err();
    assert!(
        matches!(err, Error::UnsupportedVersion { detected: 2, .. }),
        "expected UnsupportedVersion(2), got {err:?}"
    );
}

// ── parse_legacy_header overflow guards ───────────────────────────────────────

/// Opening (via `open_legacy`) a file whose 4-byte `file_len` field exceeds
/// `i32::MAX` is rejected.
///
/// With `force_legacy = true` the high bit in the first byte does not trigger
/// versioned-header detection, so `parse_legacy_header` runs and checks the
/// field range.
///
/// Covers `parse_legacy_header` → file_len-overflow branch.
#[test]
fn legacy_header_file_len_overflow() {
    let path = temp_path();

    // file_len field = 0x8000_0000 = i32::MAX + 1.
    let mut data = vec![0u8; 64];
    data[0..4].copy_from_slice(&0x8000_0000u32.to_be_bytes());
    fs::write(&path, &data).unwrap();

    let err = QueueFile::open_legacy(&path).unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

/// `parse_legacy_header` rejects a file whose `elem_cnt` field exceeds
/// `i32::MAX`.
///
/// Covers `parse_legacy_header` → elem_cnt-overflow branch.
#[test]
fn legacy_header_elem_cnt_overflow() {
    let path = temp_path();

    let mut data = vec![0u8; 64];
    // file_len = 64 (valid, fits in i32)
    data[0..4].copy_from_slice(&64u32.to_be_bytes());
    // elem_cnt = 0x8000_0000 > i32::MAX
    data[4..8].copy_from_slice(&0x8000_0000u32.to_be_bytes());
    fs::write(&path, &data).unwrap();

    let err = QueueFile::open_legacy(&path).unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

/// `parse_legacy_header` rejects a file whose `first_pos` field exceeds
/// `i32::MAX`.
///
/// Covers `parse_legacy_header` → first_pos-overflow branch.
#[test]
fn legacy_header_first_pos_overflow() {
    let path = temp_path();

    let mut data = vec![0u8; 64];
    data[0..4].copy_from_slice(&64u32.to_be_bytes()); // file_len
    data[4..8].copy_from_slice(&1u32.to_be_bytes()); // elem_cnt
    data[8..12].copy_from_slice(&0x8000_0000u32.to_be_bytes()); // first_pos > i32::MAX
    data[12..16].copy_from_slice(&16u32.to_be_bytes()); // last_pos
    fs::write(&path, &data).unwrap();

    let err = QueueFile::open_legacy(&path).unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

/// `parse_legacy_header` rejects a file whose `last_pos` field exceeds
/// `i32::MAX`.
///
/// Covers `parse_legacy_header` → last_pos-overflow branch.
#[test]
fn legacy_header_last_pos_overflow() {
    let path = temp_path();

    let mut data = vec![0u8; 64];
    data[0..4].copy_from_slice(&64u32.to_be_bytes()); // file_len
    data[4..8].copy_from_slice(&1u32.to_be_bytes()); // elem_cnt
    data[8..12].copy_from_slice(&16u32.to_be_bytes()); // first_pos (valid)
    data[12..16].copy_from_slice(&0x8000_0000u32.to_be_bytes()); // last_pos > i32::MAX
    fs::write(&path, &data).unwrap();

    let err = QueueFile::open_legacy(&path).unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

// ── elect_canonical_slot and validate_v2_slot_data error paths ────────────────

/// A V2 file where both header slots have invalid CRCs cannot be opened.
///
/// Covers `elect_canonical_slot` → both-None branch ("both v2 header slots are
/// invalid").
#[test]
fn v2_both_slots_invalid_crc() {
    let path = temp_path();

    // Build a file where both slots carry the correct magic but a wrong CRC.
    // parse_slot() returns None for each, so elect_canonical_slot gets (None, None).
    let mut data = vec![0u8; V2_INITIAL_LEN as usize];
    // Slot A: magic + version=2, rest zeros, CRC = 0 (wrong).
    data[0..4].copy_from_slice(&V2_MAGIC.to_be_bytes());
    data[4] = 2;
    // Slot B: same layout at offset 4096.
    data[V2_SLOT_B_OFFSET as usize..V2_SLOT_B_OFFSET as usize + 4]
        .copy_from_slice(&V2_MAGIC.to_be_bytes());
    data[V2_SLOT_B_OFFSET as usize + 4] = 2;
    fs::write(&path, &data).unwrap();

    let err = QueueFile::open(&path).unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

/// A V2 slot that claims `file_length < V2_DATA_START` is rejected.
///
/// Covers `validate_v2_slot_data` → file_length-too-small branch.
#[test]
fn v2_slot_file_length_too_small() {
    let path = temp_path();

    // file_length=100 < V2_DATA_START=8192 → invalid.
    let slot = build_v2_slot(100, 0, 0, 0, 1, 1);
    create_v2_file_with_slot_a(&path, slot);

    let err = QueueFile::open(&path).unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

/// A V2 slot whose `file_length` exceeds the actual file size is rejected
/// ("file is truncated").
///
/// Covers `validate_v2_slot_data` → file_length-exceeds-real-size branch.
#[test]
fn v2_slot_file_length_truncated() {
    let path = temp_path();

    // Slot claims 16384 bytes but the file is only 8192.
    let slot = build_v2_slot(16384, 0, 0, 0, 1, 1);
    create_v2_file_with_slot_a(&path, slot);

    let err = QueueFile::open(&path).unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

/// A V2 slot with `next_sequence_number = 0` is rejected.
///
/// Covers `validate_v2_slot_data` → next_seq-less-than-1 branch.
#[test]
fn v2_slot_next_seq_zero() {
    let path = temp_path();

    let slot = build_v2_slot(V2_INITIAL_LEN, 0, 0, 0, 1, 0 /* next_seq = 0 */);
    create_v2_file_with_slot_a(&path, slot);

    let err = QueueFile::open(&path).unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

/// An empty-queue V2 slot that has non-zero element pointers is rejected.
///
/// Covers `validate_v2_slot_data` → empty-queue-nonzero-pointers branch.
#[test]
fn v2_slot_empty_queue_nonzero_pointers() {
    let path = temp_path();

    // elem_cnt=0 but first_position and last_position are non-zero.
    let slot = build_v2_slot(V2_INITIAL_LEN, 0, V2_DATA_START, V2_DATA_START, 1, 1);
    create_v2_file_with_slot_a(&path, slot);

    let err = QueueFile::open(&path).unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

/// A non-empty-queue V2 slot with a zero `first_position` is rejected.
///
/// Covers `validate_v2_slot_data` → nonempty-queue-zero-pointer branch.
#[test]
fn v2_slot_nonempty_queue_zero_pointer() {
    let path = temp_path();

    // elem_cnt=1 but first_position=0 → invalid.
    let slot = build_v2_slot(V2_INITIAL_LEN, 1, 0, 0, 1, 2);
    create_v2_file_with_slot_a(&path, slot);

    let err = QueueFile::open(&path).unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

/// A V2 slot whose `first_position` is below `V2_DATA_START` is rejected.
///
/// Covers `validate_slot_bounds` → position-out-of-range branch.
#[test]
fn v2_slot_first_position_out_of_bounds() {
    let path = temp_path();

    // first_position=100 < V2_DATA_START=8192 → out of range.
    let slot = build_v2_slot(V2_INITIAL_LEN, 1, 100, 100, 1, 2);
    create_v2_file_with_slot_a(&path, slot);

    let err = QueueFile::open(&path).unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}

/// A V2 slot whose `last_position` equals or exceeds `file_length` is rejected.
///
/// Covers `validate_slot_bounds` with an out-of-range last_position.
#[test]
fn v2_slot_last_position_out_of_bounds() {
    let path = temp_path();

    // last_position = file_length → out of range [data_start, file_length).
    let slot = build_v2_slot(V2_INITIAL_LEN, 1, V2_DATA_START, V2_INITIAL_LEN, 1, 2);
    create_v2_file_with_slot_a(&path, slot);

    let err = QueueFile::open(&path).unwrap_err();
    assert!(
        matches!(err, Error::CorruptedFile { .. }),
        "expected CorruptedFile, got {err:?}"
    );
}
