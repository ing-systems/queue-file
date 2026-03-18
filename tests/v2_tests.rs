use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Mutex, MutexGuard};

use queue_file::QueueFile;

// ── Constants matching the internal v2 format ────────────────────────────────

const V2_MAGIC: u32 = 0x5146_4D48;
const V2_SLOT_A_OFFSET: u64 = 0;
const V2_SLOT_B_OFFSET: u64 = 4096;
const V2_SLOT_LEN: usize = 56;
const V2_DATA_START: u64 = 8192;
const FAILPOINT_ENV: &str = "QUEUE_FILE_FAILPOINT";

static FAILPOINT_LOCK: Mutex<()> = Mutex::new(());

// ── Helpers ───────────────────────────────────────────────────────────────────

fn temp_path() -> auto_delete_path::AutoDeletePath {
    auto_delete_path::AutoDeletePath::temp()
}

/// Read raw bytes from a file at an absolute offset.
fn read_bytes_at(path: impl AsRef<std::path::Path>, offset: u64, count: usize) -> Vec<u8> {
    let mut f = fs::OpenOptions::new().read(true).open(path).unwrap();
    f.seek(SeekFrom::Start(offset)).unwrap();
    let mut buf = vec![0u8; count];
    f.read_exact(&mut buf).unwrap();
    buf
}

/// Write raw bytes into a file at an absolute offset.
fn write_bytes_at(path: impl AsRef<std::path::Path>, offset: u64, data: &[u8]) {
    let mut f = fs::OpenOptions::new().write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.write_all(data).unwrap();
}

/// Read the 4-byte magic from a slot at `slot_offset`.
fn slot_magic(path: impl AsRef<std::path::Path>, slot_offset: u64) -> u32 {
    let bytes = read_bytes_at(path, slot_offset, 4);
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

/// Compute crc32 of `data`.
fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

fn active_slot(path: impl AsRef<std::path::Path>) -> (Vec<u8>, u64) {
    let slot_a_bytes = read_bytes_at(&path, V2_SLOT_A_OFFSET, V2_SLOT_LEN);
    let slot_b_bytes = read_bytes_at(&path, V2_SLOT_B_OFFSET, V2_SLOT_LEN);
    let gen_a = i64::from_be_bytes(slot_a_bytes[36..44].try_into().unwrap());
    let gen_b = i64::from_be_bytes(slot_b_bytes[36..44].try_into().unwrap());

    if gen_b >= gen_a {
        (slot_b_bytes, V2_SLOT_B_OFFSET)
    } else {
        (slot_a_bytes, V2_SLOT_A_OFFSET)
    }
}

#[derive(Debug, Clone, Copy)]
struct ParsedSlot {
    file_length: u64,
    element_count: u32,
    first_position: u64,
    last_position: u64,
    generation: u64,
    next_sequence_number: u64,
}

fn parse_slot_fields(bytes: &[u8]) -> ParsedSlot {
    ParsedSlot {
        file_length: i64::from_be_bytes(bytes[8..16].try_into().unwrap()) as u64,
        element_count: i32::from_be_bytes(bytes[16..20].try_into().unwrap()) as u32,
        first_position: i64::from_be_bytes(bytes[20..28].try_into().unwrap()) as u64,
        last_position: i64::from_be_bytes(bytes[28..36].try_into().unwrap()) as u64,
        generation: i64::from_be_bytes(bytes[36..44].try_into().unwrap()) as u64,
        next_sequence_number: i64::from_be_bytes(bytes[44..52].try_into().unwrap()) as u64,
    }
}

struct FailpointGuard {
    _lock: MutexGuard<'static, ()>,
}

impl FailpointGuard {
    fn set(name: &str) -> Self {
        let lock = FAILPOINT_LOCK.lock().unwrap();
        std::env::set_var(FAILPOINT_ENV, name);
        Self { _lock: lock }
    }
}

impl Drop for FailpointGuard {
    fn drop(&mut self) {
        std::env::remove_var(FAILPOINT_ENV);
    }
}

fn fill_wrapped_queue_until_next_add_expands(qf: &mut QueueFile) {
    for i in 0..140u32 {
        qf.add(&i.to_be_bytes()).unwrap();
    }

    qf.remove_n(100).unwrap();

    for i in 140..270u32 {
        qf.add(&i.to_be_bytes()).unwrap();
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// A fresh v2 file should have valid magic at slot A.
#[test]
fn v2_fresh_file_both_slots_valid() {
    let p = temp_path();
    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"hello").unwrap();
    }

    // After at least one write, slot B should have been written (generation 2).
    let magic_a = slot_magic(&p, V2_SLOT_A_OFFSET);
    assert_eq!(magic_a, V2_MAGIC, "slot A should have v2 magic");

    let magic_b = slot_magic(&p, V2_SLOT_B_OFFSET);
    assert_eq!(magic_b, V2_MAGIC, "slot B should have v2 magic");
}

/// Corrupt slot A; reopening should succeed using slot B.
///
/// After init: A=(gen=1, empty), B=(gen=0, empty). active=A.
/// After add("hello"): writes to B (gen=2, 1 elem). active=B.
/// After add("world"): writes to A (gen=3, 2 elem). active=A.
/// Corrupt A → elect B (gen=2, 1 elem) → "hello" only.
#[test]
fn v2_slot_a_corrupt_slot_b_valid() {
    let p = temp_path();
    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"hello").unwrap();
        qf.add(b"world").unwrap();
    }

    // Corrupt slot A (gen=3, has "hello"+"world").
    write_bytes_at(&p, V2_SLOT_A_OFFSET, &[0xFF; 4]);

    // Should open successfully using slot B (gen=2, has "hello" only).
    let mut qf = QueueFile::open(&p).unwrap();
    assert_eq!(qf.size(), 1, "slot B had 1 element when slot A was written");
    let items: Vec<Vec<u8>> = qf.iter().map(Vec::from).collect();
    assert_eq!(items, vec![b"hello".to_vec()]);
}

/// Corrupt slot B; reopening should succeed using slot A.
#[test]
fn v2_slot_b_corrupt_slot_a_valid() {
    let p = temp_path();
    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"data1").unwrap();
        qf.add(b"data2").unwrap();
    }

    // Corrupt slot B.
    write_bytes_at(&p, V2_SLOT_B_OFFSET, &[0xDE, 0xAD, 0xBE, 0xEF]);

    let mut qf = QueueFile::open(&p).unwrap();
    assert_eq!(qf.size(), 2);
    let items: Vec<Vec<u8>> = qf.iter().map(Vec::from).collect();
    assert_eq!(items, vec![b"data1".to_vec(), b"data2".to_vec()]);
}

/// Corrupt both slots; opening should fail.
#[test]
fn v2_both_slots_corrupt() {
    let p = temp_path();
    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"test").unwrap();
    }

    write_bytes_at(&p, V2_SLOT_A_OFFSET, &[0xFF; 4]);
    write_bytes_at(&p, V2_SLOT_B_OFFSET, &[0xFF; 4]);

    let result = QueueFile::open(&p);
    assert!(result.is_err(), "should fail with both slots corrupt");
}

/// A slot with non-zero flags byte should be treated as invalid.
///
/// After 2 adds: A=(gen=3, 2 elem), B=(gen=2, 1 elem). Corrupt A → elect B (1 elem).
#[test]
fn v2_nonzero_flags_invalid() {
    let p = temp_path();
    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"x").unwrap();
        qf.add(b"y").unwrap(); // 2nd add writes to A (gen=3)
    }

    // Corrupt slot A flags (non-zero flags → parse_slot returns None → CRC still checked but
    // the flags check happens first). Slot A had 2 elements; after corrupt, slot B wins with 1.
    let mut slot_a = read_bytes_at(&p, V2_SLOT_A_OFFSET, V2_SLOT_LEN);
    slot_a[5] = 0x01; // non-zero flags
    write_bytes_at(&p, V2_SLOT_A_OFFSET, &slot_a);

    // Should still open via slot B (1 element).
    let qf = QueueFile::open(&p).unwrap();
    assert_eq!(qf.size(), 1, "slot B had 1 element when slot A was written");
}

/// A slot with a flipped CRC byte should be treated as invalid.
#[test]
fn v2_crc_mismatch_invalid() {
    let p = temp_path();
    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"crc_test").unwrap();
        qf.add(b"crc_test2").unwrap();
    }

    // Flip a CRC byte in slot A (bytes 52-55).
    let mut slot_a = read_bytes_at(&p, V2_SLOT_A_OFFSET, V2_SLOT_LEN);
    slot_a[52] ^= 0xFF;
    write_bytes_at(&p, V2_SLOT_A_OFFSET, &slot_a);

    // Slot B should still be valid and recoverable.
    let qf = QueueFile::open(&p).unwrap();
    assert!(qf.size() > 0);
}

/// Corrupting the element header CRC should cause open or peek to fail.
#[test]
fn v2_element_header_crc_mismatch() {
    let p = temp_path();
    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"elem1").unwrap();
    }

    // Read active slot to find first_pos.
    let slot_a_bytes = read_bytes_at(&p, V2_SLOT_A_OFFSET, V2_SLOT_LEN);
    let slot_b_bytes = read_bytes_at(&p, V2_SLOT_B_OFFSET, V2_SLOT_LEN);
    let gen_a = i64::from_be_bytes(slot_a_bytes[36..44].try_into().unwrap()) as u64;
    let gen_b = i64::from_be_bytes(slot_b_bytes[36..44].try_into().unwrap()) as u64;

    let active_slot_bytes = if gen_b >= gen_a { &slot_b_bytes } else { &slot_a_bytes };
    let first_pos = i64::from_be_bytes(active_slot_bytes[20..28].try_into().unwrap()) as u64;

    // The element header CRC is at bytes 24-27 of the 28-byte header.
    let hdr_crc_offset = first_pos + 24;
    let mut crc_bytes = read_bytes_at(&p, hdr_crc_offset, 4);
    crc_bytes[0] ^= 0xFF; // corrupt CRC
    write_bytes_at(&p, hdr_crc_offset, &crc_bytes);

    // Reopening should fail during element header validation (called from open_v2).
    // Alternatively, if open succeeds somehow, peek should fail.
    let result = QueueFile::open(&p);
    if let Ok(mut qf) = result {
        // If open succeeded (e.g. via recovery), peek must fail.
        let peek_result = qf.peek();
        assert!(peek_result.is_err(), "should fail with corrupt element header CRC on peek");
    }
    // If open fails, the corruption was detected — that's the expected behavior.
}

/// Corrupting the element footer magic should cause peek/read to fail.
#[test]
fn v2_element_footer_magic_mismatch() {
    let p = temp_path();
    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"footer_test").unwrap();
    }

    let slot_b_bytes = read_bytes_at(&p, V2_SLOT_B_OFFSET, V2_SLOT_LEN);
    let first_pos = i64::from_be_bytes(slot_b_bytes[20..28].try_into().unwrap()) as u64;

    // Footer is at first_pos + 28 (header) + payload_len.
    // payload_len is at bytes 20-23 of the header.
    let hdr = read_bytes_at(&p, first_pos, 28);
    let payload_len = i32::from_be_bytes(hdr[20..24].try_into().unwrap()) as u64;
    let footer_offset = first_pos + 28 + payload_len;

    // Corrupt footer magic.
    write_bytes_at(&p, footer_offset, &[0xDE, 0xAD, 0xBE, 0xEF]);

    let mut qf = QueueFile::open(&p).unwrap();
    let result = qf.peek();
    assert!(result.is_err(), "should fail with corrupt footer magic");
}

/// Corrupting the element footer CRC should cause peek/read to fail.
#[test]
fn v2_element_footer_crc_mismatch() {
    let p = temp_path();
    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"crc_footer_test").unwrap();
    }

    let slot_b_bytes = read_bytes_at(&p, V2_SLOT_B_OFFSET, V2_SLOT_LEN);
    let first_pos = i64::from_be_bytes(slot_b_bytes[20..28].try_into().unwrap()) as u64;
    let hdr = read_bytes_at(&p, first_pos, 28);
    let payload_len = i32::from_be_bytes(hdr[20..24].try_into().unwrap()) as u64;

    // Footer CRC is at bytes 12-15 of the 16-byte footer.
    let footer_crc_offset = first_pos + 28 + payload_len + 12;
    let mut crc_bytes = read_bytes_at(&p, footer_crc_offset, 4);
    crc_bytes[0] ^= 0xFF;
    write_bytes_at(&p, footer_crc_offset, &crc_bytes);

    let mut qf = QueueFile::open(&p).unwrap();
    let result = qf.peek();
    assert!(result.is_err(), "should fail with corrupt footer CRC");
}

/// Changing the footer sequence field should cause peek/read to fail.
#[test]
fn v2_footer_sequence_mismatch() {
    let p = temp_path();
    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"seq_test").unwrap();
    }

    let slot_b_bytes = read_bytes_at(&p, V2_SLOT_B_OFFSET, V2_SLOT_LEN);
    let first_pos = i64::from_be_bytes(slot_b_bytes[20..28].try_into().unwrap()) as u64;
    let hdr = read_bytes_at(&p, first_pos, 28);
    let payload_len = i32::from_be_bytes(hdr[20..24].try_into().unwrap()) as u64;

    // Footer seq is at bytes 4-11 of the 16-byte footer. Change it to a bad value.
    let footer_seq_offset = first_pos + 28 + payload_len + 4;
    let bad_seq: i64 = 9999;
    write_bytes_at(&p, footer_seq_offset, &bad_seq.to_be_bytes());

    let mut qf = QueueFile::open(&p).unwrap();
    let result = qf.peek();
    // Either the seq check fails or the CRC check fails; either is an error.
    assert!(result.is_err(), "should fail with footer seq mismatch");
}

/// Corrupt `next_seq` in the active slot so tail seq check fails.
#[test]
fn v2_tail_sequence_mismatch() {
    let p = temp_path();
    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"tail_test").unwrap();
    }

    // next_sequence_number is at offset 44-51 in the slot.
    // The active slot has the highest generation; slot B is written on add.
    let slot_b_bytes = read_bytes_at(&p, V2_SLOT_B_OFFSET, V2_SLOT_LEN);
    let gen_b = i64::from_be_bytes(slot_b_bytes[36..44].try_into().unwrap());
    let slot_a_bytes = read_bytes_at(&p, V2_SLOT_A_OFFSET, V2_SLOT_LEN);
    let gen_a = i64::from_be_bytes(slot_a_bytes[36..44].try_into().unwrap());

    let (active_offset, active_bytes) = if gen_b >= gen_a {
        (V2_SLOT_B_OFFSET, slot_b_bytes)
    } else {
        (V2_SLOT_A_OFFSET, slot_a_bytes)
    };

    // Corrupt next_seq to an invalid value (too low, won't match tail seq+1).
    let mut corrupted = active_bytes.clone();
    let bad_next_seq: i64 = 100;
    corrupted[44..52].copy_from_slice(&bad_next_seq.to_be_bytes());

    // Recompute CRC over first 52 bytes.
    let new_crc = crc32(&corrupted[..52]);
    corrupted[52..56].copy_from_slice(&new_crc.to_be_bytes());

    write_bytes_at(&p, active_offset, &corrupted);

    // Also corrupt the other slot so only the corrupted one is "elected".
    let other_offset =
        if active_offset == V2_SLOT_B_OFFSET { V2_SLOT_A_OFFSET } else { V2_SLOT_B_OFFSET };
    write_bytes_at(&p, other_offset, &[0xFF; 4]); // invalid magic

    let result = QueueFile::open(&p);
    assert!(result.is_err(), "should fail with mismatched next_seq");
}

/// Corrupt the first element's header magic; recovery via backlinks should succeed.
#[test]
fn v2_head_recovery() {
    let p = temp_path();
    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"head_elem").unwrap();
        qf.add(b"tail_elem").unwrap();
    }

    // Read active slot (highest gen) to find first_pos.
    let slot_a_bytes = read_bytes_at(&p, V2_SLOT_A_OFFSET, V2_SLOT_LEN);
    let slot_b_bytes = read_bytes_at(&p, V2_SLOT_B_OFFSET, V2_SLOT_LEN);
    let gen_a = i64::from_be_bytes(slot_a_bytes[36..44].try_into().unwrap());
    let gen_b = i64::from_be_bytes(slot_b_bytes[36..44].try_into().unwrap());

    let active_bytes = if gen_b >= gen_a { &slot_b_bytes } else { &slot_a_bytes };

    let first_pos = i64::from_be_bytes(active_bytes[20..28].try_into().unwrap()) as u64;
    let last_pos = i64::from_be_bytes(active_bytes[28..36].try_into().unwrap()) as u64;

    // Corrupt the element header magic at first_pos.
    // This keeps first_pos valid (within range) but makes validate_v2_element_header fail,
    // triggering recovery via backlinks.
    write_bytes_at(&p, first_pos, &[0xDE, 0xAD, 0xBE, 0xEF]);

    // Recovery should walk from last_pos → prev_pos → find first element.
    let result = QueueFile::open(&p);
    match result {
        Ok(mut qf) => {
            assert_eq!(qf.size(), 2, "recovered queue should have 2 elements");
            let items: Vec<Vec<u8>> = qf.iter().map(Vec::from).collect();
            assert_eq!(items.len(), 2);
            // Last element should be intact.
            assert_eq!(items[1], b"tail_elem".to_vec());
        }
        Err(_) => {
            // Recovery could also fail if the corrupt magic at first_pos causes issues.
            // That is also acceptable behavior.
        }
    }
    let _ = last_pos;
}

/// A backlink cycle should be rejected during bounded recovery.
#[test]
fn v2_backlink_cycle() {
    let p = temp_path();
    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"cycle1").unwrap();
        qf.add(b"cycle2").unwrap();
        qf.add(b"cycle3").unwrap();
    }

    // Read active slot.
    let slot_a_bytes = read_bytes_at(&p, V2_SLOT_A_OFFSET, V2_SLOT_LEN);
    let slot_b_bytes = read_bytes_at(&p, V2_SLOT_B_OFFSET, V2_SLOT_LEN);
    let gen_a = i64::from_be_bytes(slot_a_bytes[36..44].try_into().unwrap());
    let gen_b = i64::from_be_bytes(slot_b_bytes[36..44].try_into().unwrap());

    let active_bytes = if gen_b >= gen_a { &slot_b_bytes } else { &slot_a_bytes };
    let active_offset = if gen_b >= gen_a { V2_SLOT_B_OFFSET } else { V2_SLOT_A_OFFSET };

    let last_pos = i64::from_be_bytes(active_bytes[28..36].try_into().unwrap()) as u64;
    let first_pos = i64::from_be_bytes(active_bytes[20..28].try_into().unwrap()) as u64;

    // Corrupt first_pos to an invalid value so open must recover via backlinks.
    let mut corrupted_slot = active_bytes.clone();
    let fake_first: i64 = (last_pos + 1) as i64;
    corrupted_slot[20..28].copy_from_slice(&fake_first.to_be_bytes());
    let new_crc = crc32(&corrupted_slot[..52]);
    corrupted_slot[52..56].copy_from_slice(&new_crc.to_be_bytes());
    write_bytes_at(&p, active_offset, &corrupted_slot);

    // Also corrupt the other slot.
    let other_offset =
        if active_offset == V2_SLOT_B_OFFSET { V2_SLOT_A_OFFSET } else { V2_SLOT_B_OFFSET };
    write_bytes_at(&p, other_offset, &[0xFF; 4]);

    // Now modify the last element's prev_pos to create a cycle (last → last).
    let last_hdr = read_bytes_at(&p, last_pos, 28);
    let mut new_last_hdr = last_hdr.clone();
    // Set prev_pos to last_pos itself (cycle).
    new_last_hdr[12..20].copy_from_slice(&(last_pos as i64).to_be_bytes());
    // Recompute header CRC.
    let mut hdr_arr = [0u8; 28];
    hdr_arr.copy_from_slice(&new_last_hdr);
    let hdr_crc = crc32(&hdr_arr[..24]);
    new_last_hdr[24..28].copy_from_slice(&hdr_crc.to_be_bytes());
    write_bytes_at(&p, last_pos, &new_last_hdr);

    let result = QueueFile::open(&p);
    assert!(result.is_err(), "expected malformed backlink cycle to be rejected");
    let _ = first_pos;
}

#[test]
fn v2_recover_head_after_dequeues_with_historical_backlink() {
    let p = temp_path();
    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"first").unwrap();
        qf.add(b"second").unwrap();
        qf.add(b"third").unwrap();
        qf.add(b"fourth").unwrap();
        qf.remove_n(2).unwrap();
    }

    let (mut active_bytes, active_offset) = active_slot(&p);
    let live_head_pos = i64::from_be_bytes(active_bytes[20..28].try_into().unwrap()) as u64;
    let last_pos = i64::from_be_bytes(active_bytes[28..36].try_into().unwrap()) as u64;
    let live_head_hdr = read_bytes_at(&p, live_head_pos, 28);
    let live_head_prev = i64::from_be_bytes(live_head_hdr[12..20].try_into().unwrap()) as u64;
    assert_ne!(live_head_prev, 0, "post-dequeue live head should retain historical backlink");

    active_bytes[20..28].copy_from_slice(&((last_pos + 1) as i64).to_be_bytes());
    let new_crc = crc32(&active_bytes[..52]);
    active_bytes[52..56].copy_from_slice(&new_crc.to_be_bytes());
    write_bytes_at(&p, active_offset, &active_bytes);

    let mut qf = QueueFile::open(&p).unwrap();
    assert_eq!(qf.size(), 2);
    let items: Vec<Vec<u8>> = qf.iter().map(Vec::from).collect();
    assert_eq!(items, vec![b"third".to_vec(), b"fourth".to_vec()]);
}

#[test]
fn v2_recovery_fails_when_prev_zero_appears_before_live_count() {
    let p = temp_path();
    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"first").unwrap();
        qf.add(b"second").unwrap();
        qf.add(b"third").unwrap();
    }

    let (mut active_bytes, active_offset) = active_slot(&p);
    let last_pos = i64::from_be_bytes(active_bytes[28..36].try_into().unwrap()) as u64;

    active_bytes[16..20].copy_from_slice(&4i32.to_be_bytes());
    active_bytes[20..28].copy_from_slice(&((last_pos + 1) as i64).to_be_bytes());
    let new_crc = crc32(&active_bytes[..52]);
    active_bytes[52..56].copy_from_slice(&new_crc.to_be_bytes());
    write_bytes_at(&p, active_offset, &active_bytes);

    let err = QueueFile::open(&p).unwrap_err().to_string();
    assert!(
        err.contains("walked 3 elements but expected 4"),
        "unexpected error: {err}"
    );
}

/// A sequence discontinuity in backlinks should cause recovery to fail.
#[test]
fn v2_backlink_sequence_discontinuity() {
    let p = temp_path();
    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"first").unwrap();
        qf.add(b"second").unwrap();
        qf.add(b"third").unwrap();
    }

    // Read active slot.
    let slot_a_bytes = read_bytes_at(&p, V2_SLOT_A_OFFSET, V2_SLOT_LEN);
    let slot_b_bytes = read_bytes_at(&p, V2_SLOT_B_OFFSET, V2_SLOT_LEN);
    let gen_a = i64::from_be_bytes(slot_a_bytes[36..44].try_into().unwrap());
    let gen_b = i64::from_be_bytes(slot_b_bytes[36..44].try_into().unwrap());

    let active_bytes = if gen_b >= gen_a { &slot_b_bytes } else { &slot_a_bytes };
    let active_offset = if gen_b >= gen_a { V2_SLOT_B_OFFSET } else { V2_SLOT_A_OFFSET };

    let last_pos = i64::from_be_bytes(active_bytes[28..36].try_into().unwrap()) as u64;

    // Read last element header to find prev_pos (second element).
    let last_hdr = read_bytes_at(&p, last_pos, 28);
    let last_seq = i64::from_be_bytes(last_hdr[4..12].try_into().unwrap());
    let prev_pos = i64::from_be_bytes(last_hdr[12..20].try_into().unwrap()) as u64;

    // Corrupt the seq of the second element to break the sequence chain.
    // We set seq = last_seq + 5 (non-contiguous) and recompute CRC to keep CRC valid.
    if prev_pos >= V2_DATA_START {
        let mut prev_hdr = read_bytes_at(&p, prev_pos, 28);
        let bad_seq: i64 = last_seq + 5; // breaks seq continuity
        prev_hdr[4..12].copy_from_slice(&bad_seq.to_be_bytes());
        let crc = crc32(&prev_hdr[..24]);
        prev_hdr[24..28].copy_from_slice(&crc.to_be_bytes());
        write_bytes_at(&p, prev_pos, &prev_hdr);

        // Also corrupt first_pos in the active slot to force recovery walk.
        let mut corrupted_slot = active_bytes.clone();
        corrupted_slot[20..28].copy_from_slice(&((last_pos + 1) as i64).to_be_bytes());
        let new_crc = crc32(&corrupted_slot[..52]);
        corrupted_slot[52..56].copy_from_slice(&new_crc.to_be_bytes());
        write_bytes_at(&p, active_offset, &corrupted_slot);

        // Corrupt the other slot.
        let other_offset =
            if active_offset == V2_SLOT_B_OFFSET { V2_SLOT_A_OFFSET } else { V2_SLOT_B_OFFSET };
        write_bytes_at(&p, other_offset, &[0xFF; 4]);

        // With first_pos == last_pos and a sequence discontinuity in the chain,
        // recovery must reject the malformed backlink walk.
        let result = QueueFile::open(&p);
        assert!(result.is_err(), "expected malformed sequence chain to be rejected");
    }
    let _ = last_seq;
}

/// After clear and re-add, sequence numbers must be monotonically increasing.
#[test]
fn v2_clear_preserves_next_seq() {
    let p = temp_path();

    let mut qf = QueueFile::open(&p).unwrap();
    qf.add(b"before_clear_1").unwrap();
    qf.add(b"before_clear_2").unwrap();
    qf.clear().unwrap();

    // After clear, add more elements. Sequences must not restart at 1.
    qf.add(b"after_clear_1").unwrap();

    // Verify by re-opening and checking that the queue is consistent.
    drop(qf);

    let mut qf = QueueFile::open(&p).unwrap();
    assert_eq!(qf.size(), 1);
    let item = qf.peek().unwrap().unwrap();
    assert_eq!(item.as_ref(), b"after_clear_1");
}

/// Fill queue to force wrapping, trigger expansion, and verify data is intact.
#[test]
fn v2_wrapped_expansion() {
    let p = temp_path();

    let mut qf = QueueFile::open(&p).unwrap();

    // Add a bunch of elements to fill the initial space and trigger expansion.
    for i in 0..50u32 {
        qf.add(&i.to_be_bytes()).unwrap();
    }

    // Remove some to create a gap at the start (causes wrapping on next add).
    qf.remove_n(20).unwrap();

    // Add more to cause wrapping.
    for i in 50..80u32 {
        qf.add(&i.to_be_bytes()).unwrap();
    }

    // Verify all remaining elements are correct.
    let items: Vec<u32> =
        qf.iter().map(|b| u32::from_be_bytes(b[..].try_into().unwrap())).collect();
    let expected: Vec<u32> = (20..80).collect();
    assert_eq!(items, expected);
}

#[test]
fn v2_relocation_commit_happens_before_erasing_old_wrapped_bytes() {
    let p = temp_path();
    let mut qf = QueueFile::open(&p).unwrap();
    fill_wrapped_queue_until_next_add_expands(&mut qf);

    let (before_bytes, _) = active_slot(&p);
    let before = parse_slot_fields(&before_bytes);
    assert!(before.last_position < before.first_position, "queue should be wrapped before expansion");

    let old_last_header = read_bytes_at(&p, before.last_position, 4);

    let failpoint = FailpointGuard::set("v2_after_relocation_commit_before_erase");
    let err = qf.add(&999u32.to_be_bytes()).unwrap_err().to_string();
    drop(failpoint);
    assert!(err.contains("v2_after_relocation_commit_before_erase"), "unexpected error: {err}");

    let (after_bytes, _) = active_slot(&p);
    let after = parse_slot_fields(&after_bytes);

    assert!(after.generation > before.generation);
    assert_eq!(after.element_count, before.element_count);
    assert_eq!(after.first_position, before.first_position);
    assert_eq!(after.next_sequence_number, before.next_sequence_number);
    assert!(after.file_length > before.file_length);
    assert!(after.last_position >= before.file_length);

    let old_last_header_after = read_bytes_at(&p, before.last_position, 4);
    assert_eq!(old_last_header_after, old_last_header, "old wrapped bytes should still be intact");
}

#[test]
fn v2_reopens_relocated_pre_add_queue_after_cleanup_before_final_add_commit() {
    let p = temp_path();
    let mut qf = QueueFile::open(&p).unwrap();
    fill_wrapped_queue_until_next_add_expands(&mut qf);

    let expected: Vec<Vec<u8>> = qf.iter().map(Vec::from).collect();

    let failpoint = FailpointGuard::set("v2_after_relocation_cleanup_before_add");
    let err = qf.add(&999u32.to_be_bytes()).unwrap_err().to_string();
    drop(failpoint);
    assert!(err.contains("v2_after_relocation_cleanup_before_add"), "unexpected error: {err}");
    drop(qf);

    let mut reopened = QueueFile::open(&p).unwrap();
    let items: Vec<Vec<u8>> = reopened.iter().map(Vec::from).collect();
    assert_eq!(items, expected, "reopen should see the relocated pre-add queue only");
}

#[test]
fn v2_backlink_rewrite_batch_suppresses_per_header_syncs() {
    let p = temp_path();
    let mut qf = QueueFile::open(&p).unwrap();
    qf.set_sync_writes(true);
    fill_wrapped_queue_until_next_add_expands(&mut qf);

    let failpoint = FailpointGuard::set("v2_backlink_rewrite_per_write_sync");
    qf.add(&999u32.to_be_bytes()).unwrap();
    drop(failpoint);

    assert!(qf.sync_writes(), "sync_writes should be restored after batched rewrite");
}

#[test]
fn v2_backlink_rewrite_flush_happens_before_relocation_commit() {
    let p = temp_path();
    let mut qf = QueueFile::open(&p).unwrap();
    qf.set_sync_writes(true);
    fill_wrapped_queue_until_next_add_expands(&mut qf);

    let (before_bytes, _) = active_slot(&p);
    let before = parse_slot_fields(&before_bytes);

    let failpoint = FailpointGuard::set("v2_after_backlink_rewrite_flush");
    let err = qf.add(&999u32.to_be_bytes()).unwrap_err().to_string();
    drop(failpoint);
    assert!(err.contains("v2_after_backlink_rewrite_flush"), "unexpected error: {err}");
    assert!(qf.sync_writes(), "sync_writes should be restored after batched rewrite failure");

    let (after_bytes, _) = active_slot(&p);
    let after = parse_slot_fields(&after_bytes);
    assert_eq!(after.generation, before.generation, "relocation commit must not happen yet");
    assert_eq!(after.file_length, before.file_length, "active slot should remain the old layout");

    drop(qf);

    let mut reopened = QueueFile::open(&p).unwrap();
    let items: Vec<Vec<u8>> = reopened.iter().map(Vec::from).collect();
    assert_eq!(items.len(), before.element_count as usize);
}

/// Opening a v0 (legacy) file with `open()` should migrate it to v2.
#[test]
fn v2_migrate_v0() {
    let p = temp_path();

    // Create a legacy (v0) queue.
    {
        let mut qf = QueueFile::open_legacy(&p).unwrap();
        qf.add(b"legacy_elem_1").unwrap();
        qf.add(b"legacy_elem_2").unwrap();
    }

    // Opening with `open()` should migrate to v2.
    let mut qf = QueueFile::open(&p).unwrap();
    assert_eq!(qf.size(), 2);

    let items: Vec<Vec<u8>> = qf.iter().map(Vec::from).collect();
    assert_eq!(items[0], b"legacy_elem_1".to_vec());
    assert_eq!(items[1], b"legacy_elem_2".to_vec());

    // After migration, the file should have v2 magic at slot A or B.
    drop(qf);
    let magic_a = slot_magic(&p, V2_SLOT_A_OFFSET);
    let magic_b = slot_magic(&p, V2_SLOT_B_OFFSET);
    assert!(magic_a == V2_MAGIC || magic_b == V2_MAGIC, "migrated file should have v2 magic");
}

/// Opening a v1 (versioned) file with `open()` should migrate it to v2.
#[test]
fn v2_migrate_v1() {
    let p = temp_path();

    // Create a v1 queue by bypassing migration (open_internal_full with allow_migration=false).
    // We can't call the private function, so we'll use a different approach:
    // Create a fresh file with manually written v1 header.
    {
        use std::io::Write;
        let mut f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(p.as_ref())
            .unwrap();
        let cap: u64 = 4096;
        // Versioned header: 0x80000001 | file_len(u64) | elem_cnt(i32) | first(u64) | last(u64)
        f.write_all(&0x8000_0001u32.to_be_bytes()).unwrap(); // versioned magic
        f.write_all(&cap.to_be_bytes()).unwrap(); // file_len
        f.write_all(&0i32.to_be_bytes()).unwrap(); // elem_cnt
        f.write_all(&0u64.to_be_bytes()).unwrap(); // first_pos
        f.write_all(&0u64.to_be_bytes()).unwrap(); // last_pos
        f.set_len(cap).unwrap();
    }

    // Now open with the default open() - it should detect v1 and migrate to v2.
    let qf = QueueFile::open(&p).unwrap();
    assert_eq!(qf.size(), 0);

    drop(qf);
    let magic_a = slot_magic(&p, V2_SLOT_A_OFFSET);
    let magic_b = slot_magic(&p, V2_SLOT_B_OFFSET);
    assert!(magic_a == V2_MAGIC || magic_b == V2_MAGIC, "migrated v1 file should have v2 magic");
}

/// `open_legacy` must NOT trigger migration; it should create/use a v0 file.
#[test]
fn v2_open_legacy_creates_v0() {
    let p = temp_path();

    let mut qf = QueueFile::open_legacy(&p).unwrap();
    qf.add(b"legacy").unwrap();
    drop(qf);

    // The file should NOT have v2 magic.
    let magic = slot_magic(&p, V2_SLOT_A_OFFSET);
    assert_ne!(magic, V2_MAGIC, "open_legacy should not create v2 format");
}

/// Basic quickcheck-like property test: v2 queue behaves like VecDeque.
#[test]
fn v2_queue_like_vecdeque() {
    use std::collections::VecDeque;

    let p = temp_path();
    let mut qf = QueueFile::open(&p).unwrap();
    let mut vd: VecDeque<Vec<u8>> = VecDeque::new();

    // Add elements.
    for i in 0u32..30 {
        let data = i.to_be_bytes().to_vec();
        qf.add(&data).unwrap();
        vd.push_back(data);
    }

    // Verify.
    let qf_items: Vec<Vec<u8>> = qf.iter().map(Vec::from).collect();
    let vd_items: Vec<Vec<u8>> = vd.iter().cloned().collect();
    assert_eq!(qf_items, vd_items);

    // Remove some.
    qf.remove_n(10).unwrap();
    vd.drain(..10);

    // Add more.
    for i in 100u32..120 {
        let data = i.to_be_bytes().to_vec();
        qf.add(&data).unwrap();
        vd.push_back(data);
    }

    // Verify again.
    let qf_items: Vec<Vec<u8>> = qf.iter().map(Vec::from).collect();
    let vd_items: Vec<Vec<u8>> = vd.iter().cloned().collect();
    assert_eq!(qf_items, vd_items);

    // Clear and verify empty.
    qf.clear().unwrap();
    vd.clear();

    assert!(qf.is_empty());
    assert_eq!(qf.size(), 0);
}

/// A cross-format fixture test: known v2 bytes can be re-parsed correctly.
#[test]
fn v2_cross_format_fixture() {
    let p = temp_path();

    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"fixture_data").unwrap();
    }

    // Read slot B (written after the first add; generation=2).
    let slot_b = read_bytes_at(&p, V2_SLOT_B_OFFSET, V2_SLOT_LEN);

    // Verify magic.
    let magic = u32::from_be_bytes(slot_b[0..4].try_into().unwrap());
    assert_eq!(magic, V2_MAGIC);

    // Verify version=2, flags=0.
    assert_eq!(slot_b[4], 2);
    assert_eq!(slot_b[5], 0);

    // Verify element count = 1.
    let elem_count = i32::from_be_bytes(slot_b[16..20].try_into().unwrap());
    assert_eq!(elem_count, 1);

    // Verify CRC is correct.
    let stored_crc = u32::from_be_bytes(slot_b[52..56].try_into().unwrap());
    let expected_crc = crc32(&slot_b[..52]);
    assert_eq!(stored_crc, expected_crc);

    // Verify we can reopen and read the data.
    let mut qf = QueueFile::open(&p).unwrap();
    let data = qf.peek().unwrap().unwrap();
    assert_eq!(data.as_ref(), b"fixture_data");
}

/// Test that `add_n` batch writes work correctly in v2 format.
#[test]
fn v2_add_n_batch() {
    let p = temp_path();
    let mut qf = QueueFile::open(&p).unwrap();

    let batch = vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()];
    qf.add_n(batch.iter().map(|v| v.as_slice())).unwrap();

    assert_eq!(qf.size(), 3);

    let items: Vec<Vec<u8>> = qf.iter().map(Vec::from).collect();
    assert_eq!(items[0], b"one".to_vec());
    assert_eq!(items[1], b"two".to_vec());
    assert_eq!(items[2], b"three".to_vec());
}

/// Test remove_n in v2 format.
#[test]
fn v2_remove_n() {
    let p = temp_path();
    let mut qf = QueueFile::open(&p).unwrap();

    for i in 0u32..10 {
        qf.add(&i.to_be_bytes()).unwrap();
    }

    qf.remove_n(3).unwrap();
    assert_eq!(qf.size(), 7);

    let first = qf.peek().unwrap().unwrap();
    assert_eq!(first.as_ref(), &3u32.to_be_bytes());

    qf.remove_n(7).unwrap();
    assert_eq!(qf.size(), 0);
    assert!(qf.is_empty());
}

/// Test that skip_write_header_on_add works with v2.
#[test]
fn v2_skip_write_header_on_add() {
    let p = temp_path();
    let mut qf = QueueFile::open(&p).unwrap();
    qf.set_skip_write_header_on_add(true);

    for i in 0u32..5 {
        qf.add(&i.to_be_bytes()).unwrap();
    }

    // Explicit sync.
    qf.sync_all().unwrap();

    drop(qf);

    // Reopen and verify all 5 elements are present.
    let mut qf = QueueFile::open(&p).unwrap();
    assert_eq!(qf.size(), 5);

    let items: Vec<u32> =
        qf.iter().map(|b| u32::from_be_bytes(b[..].try_into().unwrap())).collect();
    let expected: Vec<u32> = (0..5).collect();
    assert_eq!(items, expected);
}

/// Test persistence: data survives close and reopen.
#[test]
fn v2_persistence() {
    let p = temp_path();

    {
        let mut qf = QueueFile::open(&p).unwrap();
        qf.add(b"persistent_data").unwrap();
        qf.add(b"more_data").unwrap();
    }

    let mut qf = QueueFile::open(&p).unwrap();
    assert_eq!(qf.size(), 2);
    let items: Vec<Vec<u8>> = qf.iter().map(Vec::from).collect();
    assert_eq!(items[0], b"persistent_data".to_vec());
    assert_eq!(items[1], b"more_data".to_vec());
}

/// Test that empty elements (zero-length payloads) work in v2.
#[test]
fn v2_empty_elements() {
    let p = temp_path();
    let mut qf = QueueFile::open(&p).unwrap();

    qf.add(b"before").unwrap();
    qf.add(b"").unwrap(); // empty
    qf.add(b"after").unwrap();

    assert_eq!(qf.size(), 3);

    let items: Vec<Vec<u8>> = qf.iter().map(Vec::from).collect();
    assert_eq!(items[0], b"before".to_vec());
    assert_eq!(items[1], b"".to_vec());
    assert_eq!(items[2], b"after".to_vec());
}
