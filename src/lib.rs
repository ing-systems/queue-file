// Java version: Copyright (C) 2010 Square, Inc.
// Rust version: Copyright (C) 2019 ING Systems
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A lightning-fast, transactional, file-based FIFO queue.
//!
//! This crate is a feature-complete port of the `QueueFile` class from
//! [Tape2 by Square, Inc.](https://github.com/square/tape), extended with a new v2 format
//! that adds per-element sequence numbers, CRC-32 integrity checks, and dual-slot atomic
//! header commits.
//!
//! # Overview
//!
//! [`QueueFile`] stores an ordered sequence of byte blobs in a single file, using a ring-buffer
//! layout for O(1) enqueue and dequeue. The file grows on demand (doubling its size) and shrinks
//! back to [`QueueFile::with_capacity`]'s `capacity` floor when the queue is cleared.
//!
//! # Atomicity guarantee
//!
//! Every mutation commits by rewriting the file header **last**. Because the header is small
//! enough that filesystem implementations typically write it atomically, a process crash after
//! data has been written but before the header is committed leaves the queue in its previous
//! consistent state. The stored file-length field additionally allows recovery from a failed
//! expansion: if the file was grown but the copy was not finished, the old length can be recovered
//! from the header.
//!
//! # File formats
//!
//! Three header formats are supported:
//!
//! * **V2** (dual 56-byte slots, default): created by [`QueueFile::open`] and
//!   [`QueueFile::with_capacity`]. Supports per-element CRC integrity, sequence numbers, and
//!   backlink recovery. Files created by older versions are automatically migrated on open.
//! * **V1** (32 bytes): the previous default format, now migrated to V2 on open.
//! * **Legacy** (16 bytes): created by [`QueueFile::open_legacy`]. Binary-compatible with the
//!   original Java `QueueFile`. Supports files up to `i32::MAX` bytes only.
//!
//! # Performance notes
//!
//! * Use [`QueueFile::add_n`] to batch multiple elements into a single write.
//! * Set [`QueueFile::set_sync_writes`] to `false` if durability after every operation is not
//!   required (e.g. when building an in-process write-ahead log that controls flushing itself).
//! * Enable [`QueueFile::set_skip_write_header_on_add`] with `true` together with
//!   [`QueueFile::sync_all`] to defer header writes across a series of additions.
//! * Use [`QueueFile::set_cache_offset_policy`] to speed up random access through [`Iter::nth`]
//!   on large queues.

#![forbid(non_ascii_idents)]
#![deny(
    macro_use_extern_crate,
    missing_copy_implementations,
    missing_debug_implementations,
    rust_2018_idioms,
    rust_2021_compatibility,
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications
)]
#![warn(
    clippy::nursery,
    clippy::pedantic,
    clippy::mutex_atomic,
    clippy::rc_buffer,
    clippy::rc_mutex,
    // clippy::expect_used,
    // clippy::unwrap_used,
)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]

use std::cmp::min;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions, rename};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;
use std::path::Path;

use bytes::{Buf, BufMut, BytesMut};
use snafu::{Snafu, ensure};

// ── V2 constants ─────────────────────────────────────────────────────────────

/// Magic number in each 56-byte header slot: ASCII "QFMH".
const V2_MAGIC: u32 = 0x5146_4D48;
/// File offset of slot A.
const V2_SLOT_A_OFFSET: u64 = 0;
/// File offset of slot B.
const V2_SLOT_B_OFFSET: u64 = 4096;
/// Length of each header slot in bytes.
const V2_SLOT_LEN: usize = 56;
/// Byte offset at which the ring-buffer data region begins.
const V2_DATA_START: u64 = 8192;
/// Minimum / initial file length for v2 files.
const V2_INITIAL_LEN: u64 = 8192;

/// Magic number in a v2 element header: ASCII "ITHD".
const V2_ELEM_HDR_MAGIC: u32 = 0x4954_4844;
/// Length of a v2 element header in bytes.
const V2_ELEM_HDR_LEN: usize = 28;
/// Magic number in a v2 element footer: ASCII "ITFT".
const V2_ELEM_FTR_MAGIC: u32 = 0x4954_4654;
/// Length of a v2 element footer in bytes.
const V2_ELEM_FTR_LEN: usize = 16;
/// Total per-element overhead in v2: header (28) + footer (16).
const V2_ELEM_OVERHEAD: u64 = 44;

// ── Errors ───────────────────────────────────────────────────────────────────

/// Errors that can be returned by [`QueueFile`] operations.
#[derive(Debug, Snafu)]
pub enum Error {
    /// An underlying I/O error occurred (e.g. a read, write, seek, or `sync_data` call failed).
    ///
    /// This variant is constructed automatically from [`io::Error`] via the `snafu`
    /// `context(false)` attribute, so `?` on any `io::Result` inside this crate will produce it.
    #[snafu(context(false))]
    Io { source: io::Error },

    /// The queue already contains `i32::MAX - 1` elements and adding more would overflow the
    /// element-count field in the file header.
    #[snafu(display("too many elements"))]
    TooManyElements {},

    /// The data slice passed to [`QueueFile::add`] or [`QueueFile::add_n`] is larger than
    /// `i32::MAX` bytes, which would overflow the 4-byte per-element length prefix in the file.
    #[snafu(display("element too big"))]
    ElementTooBig {},

    /// The queue file's contents are internally inconsistent (e.g. a position stored in the
    /// header points beyond the file, the file is shorter than the header claims, or a field
    /// value would overflow its type when read back). The `msg` field contains a human-readable
    /// description of which check failed.
    #[snafu(display("corrupted file: {}", msg))]
    CorruptedFile { msg: String },

    /// The versioned header was written with a format version number that this implementation
    /// does not recognise. `detected` is the version found in the file; `supported` is the only
    /// version this crate can read (currently `1`).
    #[snafu(display(
        "unsupported version {}. supported versions is {} and legacy",
        detected,
        supported
    ))]
    UnsupportedVersion { detected: u32, supported: u32 },
}

type Result<T, E = Error> = std::result::Result<T, E>;

// ── Header slot ──────────────────────────────────────────────────────────────

/// One of the two on-disk header slots used by the V2 format.
///
/// The V2 format maintains two 56-byte slots at fixed offsets so that header
/// writes are atomic: data is always written to the *inactive* slot before the
/// active pointer is flipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeaderSlot {
    /// Slot A at file offset [`V2_SLOT_A_OFFSET`].
    A,
    /// Slot B at file offset [`V2_SLOT_B_OFFSET`].
    B,
}

impl HeaderSlot {
    /// Returns the other slot.
    const fn toggle(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }

    /// Returns the file offset of this slot.
    const fn offset(self) -> u64 {
        match self {
            Self::A => V2_SLOT_A_OFFSET,
            Self::B => V2_SLOT_B_OFFSET,
        }
    }
}

// ── Format state ─────────────────────────────────────────────────────────────

/// Which on-disk format this queue file uses, including any format-specific mutable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FormatState {
    /// 16-byte header; binary-compatible with the Java `QueueFile`.
    Legacy,
    /// 32-byte header; the previous default Rust format.
    V1,
    /// Dual 56-byte slots + element headers/footers with CRC-32 integrity.
    V2 { active_slot: HeaderSlot, generation: u64, next_seq: u64 },
}

// ── V2 slot data ─────────────────────────────────────────────────────────────

/// Parsed contents of one 56-byte v2 header slot.
#[derive(Debug, Clone, Copy)]
struct SlotData {
    file_length: u64,
    element_count: u32,
    first_position: u64,
    last_position: u64,
    generation: u64,
    next_sequence_number: u64,
}

#[derive(Debug, Clone, Copy)]
struct LegacyHeaderState {
    format: FormatState,
    file_len: u64,
    elem_cnt: usize,
    first_pos: u64,
    last_pos: u64,
}

#[derive(Debug, Clone, Copy)]
struct V2OpenState {
    active_slot: HeaderSlot,
    slot: SlotData,
    elem_cnt: usize,
}

#[derive(Debug, Clone, Copy)]
struct V2ElementHeader {
    payload_len: usize,
    seq: u64,
    prev_pos: u64,
}

#[derive(Debug, Clone, Copy)]
struct ExpansionPlan {
    orig_file_len: u64,
    new_len: u64,
    end_of_last_elem: u64,
    wraps: bool,
    moved_count: u64,
}

#[derive(Debug, Clone)]
struct QueueStateSnapshot {
    format: FormatState,
    elem_cnt: usize,
    first: Element,
    last: Element,
    overwrite_on_remove: bool,
    cached_offsets: VecDeque<(usize, Element)>,
}

// ── CRC helpers ───────────────────────────────────────────────────────────────

fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

fn compute_slot_crc(slot_bytes: &[u8; V2_SLOT_LEN]) -> u32 {
    crc32(&slot_bytes[..52])
}

fn compute_elem_header_crc(hdr_bytes: &[u8; V2_ELEM_HDR_LEN]) -> u32 {
    crc32(&hdr_bytes[..24])
}

fn compute_elem_footer_crc(payload: &[u8], ftr_bytes_0_to_11: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(payload);
    hasher.update(ftr_bytes_0_to_11);
    hasher.finalize()
}

// ── Slot serialisation / deserialisation ─────────────────────────────────────

/// Build a 56-byte slot from `data`, computing and writing the CRC.
fn build_slot_bytes(data: &SlotData) -> [u8; V2_SLOT_LEN] {
    let mut buf = [0u8; V2_SLOT_LEN];
    {
        let mut w: &mut [u8] = &mut buf;
        w.put_u32(V2_MAGIC);
        w.put_u8(2); // version
        w.put_u8(0); // flags
        w.put_u8(0); // reserved
        w.put_u8(0); // reserved
        // SAFETY: caller (write_header_v2) validates all fields fit in their signed widths.
        w.put_i64(data.file_length as i64);
        w.put_i32(data.element_count as i32);
        w.put_i64(data.first_position as i64);
        w.put_i64(data.last_position as i64);
        w.put_i64(data.generation as i64);
        w.put_i64(data.next_sequence_number as i64);
    }
    let crc = compute_slot_crc(&buf);
    buf[52..56].copy_from_slice(&crc.to_be_bytes());
    buf
}

/// Parse a 56-byte slot, returning `None` if any validation fails.
fn parse_slot(bytes: &[u8; V2_SLOT_LEN]) -> Option<SlotData> {
    let mut r: &[u8] = bytes;
    let magic = r.get_u32();
    if magic != V2_MAGIC {
        return None;
    }
    let version = r.get_u8();
    let flags = r.get_u8();
    let res0 = r.get_u8();
    let res1 = r.get_u8();
    if version != 2 || flags != 0 || res0 != 0 || res1 != 0 {
        return None;
    }
    let file_length = u64::try_from(r.get_i64()).ok()?;
    let element_count = u32::try_from(r.get_i32()).ok()?;
    let first_position = u64::try_from(r.get_i64()).ok()?;
    let last_position = u64::try_from(r.get_i64()).ok()?;
    let generation = u64::try_from(r.get_i64()).ok()?;
    let next_sequence_number = u64::try_from(r.get_i64()).ok()?;

    let stored_crc = u32::from_be_bytes([bytes[52], bytes[53], bytes[54], bytes[55]]);
    let expected_crc = compute_slot_crc(bytes);
    if stored_crc != expected_crc {
        return None;
    }

    Some(SlotData {
        file_length,
        element_count,
        first_position,
        last_position,
        generation,
        next_sequence_number,
    })
}

/// Read 56 bytes from absolute file offset `offset` (no ring-buffer wrapping).
fn read_slot(inner: &QueueFileInner, offset: u64) -> Result<[u8; V2_SLOT_LEN]> {
    let mut buf = [0u8; V2_SLOT_LEN];
    inner.read_exact_at(offset, &mut buf)?;
    Ok(buf)
}

/// Given two optional parsed slots, elect the canonical one.
///
/// Both invalid → error. One valid → use it. Both valid → higher generation wins (A wins ties).
fn elect_canonical_slot(
    slot_a: Option<SlotData>, slot_b: Option<SlotData>,
) -> Result<(HeaderSlot, SlotData)> {
    match (slot_a, slot_b) {
        (None, None) => {
            Err(Error::CorruptedFile { msg: "both v2 header slots are invalid".to_owned() })
        }
        (Some(a), None) => Ok((HeaderSlot::A, a)),
        (None, Some(b)) => Ok((HeaderSlot::B, b)),
        (Some(a), Some(b)) => {
            if a.generation >= b.generation {
                Ok((HeaderSlot::A, a))
            } else {
                Ok((HeaderSlot::B, b))
            }
        }
    }
}

fn validate_v2_slot_data(slot: &SlotData, real_file_len: u64) -> Result<()> {
    ensure!(slot.file_length >= V2_DATA_START, CorruptedFileSnafu {
        msg: format!("v2 file_length {} < data_start {}", slot.file_length, V2_DATA_START)
    });
    ensure!(slot.file_length <= real_file_len, CorruptedFileSnafu {
        msg: format!(
            "v2 file is truncated: header claims {}, actual {}",
            slot.file_length, real_file_len
        )
    });
    ensure!(slot.next_sequence_number >= 1, CorruptedFileSnafu {
        msg: "v2 next_sequence_number must be >= 1".to_owned()
    });
    if slot.element_count == 0 {
        ensure!(slot.first_position == 0 && slot.last_position == 0, CorruptedFileSnafu {
            msg: "v2 empty queue has non-zero pointers".to_owned()
        });
    } else {
        ensure!(slot.first_position != 0 && slot.last_position != 0, CorruptedFileSnafu {
            msg: "v2 non-empty queue has zero pointer".to_owned()
        });
        ensure!(
            slot.first_position >= V2_DATA_START && slot.first_position < slot.file_length,
            CorruptedFileSnafu {
                msg: format!(
                    "v2 first_position {} out of range [data_start={}, file_length={})",
                    slot.first_position, V2_DATA_START, slot.file_length
                )
            }
        );
        ensure!(
            slot.last_position >= V2_DATA_START && slot.last_position < slot.file_length,
            CorruptedFileSnafu {
                msg: format!(
                    "v2 last_position {} out of range [data_start={}, file_length={})",
                    slot.last_position, V2_DATA_START, slot.file_length
                )
            }
        );
    }
    Ok(())
}

fn maybe_inject_failpoint(name: &str) -> Result<()> {
    if std::env::var("QUEUE_FILE_FAILPOINT").ok().as_deref() == Some(name) {
        return Err(Error::CorruptedFile { msg: format!("injected failpoint: {name}") });
    }

    Ok(())
}

// ── QueueFile ────────────────────────────────────────────────────────────────

/// A lightning-fast, transactional, file-based FIFO queue.
///
/// [`QueueFile`] stores a sequence of arbitrary byte blobs in a single backing file. Adding and
/// removing elements are both O(1) operations. By default every write is immediately flushed to
/// disk via `sync_data`, making the queue crash-safe.
///
/// # Ring-buffer layout
///
/// After the fixed-size header, the remaining file space is treated as a circular buffer.
/// - `first` points to the head element (the next one to be dequeued).
/// - `last` points to the tail element (the most-recently enqueued one).
/// - When `last.pos >= first.pos` the data is **contiguous** in the file.
/// - When `last.pos < first.pos` the data **wraps**: the tail portion of the live data is stored
///   near the beginning of the file (just after the header) and the head portion is stored near
///   the end.
/// - The file grows by doubling when there is no room for the next write.
///
/// # Atomicity
///
/// The header is the last thing written on every mutation.
///
/// # Example
///
/// ```
/// use queue_file::QueueFile;
///
/// # let path = auto_delete_path::AutoDeletePath::temp();
/// let mut qf = QueueFile::open(path)
///     .expect("cannot open queue file");
/// let data = "Welcome to QueueFile!".as_bytes();
///
/// qf.add(&data).expect("add failed");
///
/// if let Ok(Some(bytes)) = qf.peek() {
///     assert_eq!(data, bytes.as_slice());
/// }
///
/// qf.remove().expect("remove failed");
/// ```
#[derive(Debug)]
pub struct QueueFile {
    inner: QueueFileInner,
    /// Which on-disk format this file uses, including format-specific metadata.
    format: FormatState,
    /// Number of elements.
    elem_cnt: usize,
    /// Pointer to first (or eldest) element.
    first: Element,
    /// Pointer to last (or newest) element.
    last: Element,
    /// Minimum number of bytes the file shrinks to.
    capacity: u64,
    /// When true, removing an element will also overwrite data with zero bytes.
    overwrite_on_remove: bool,
    /// When true, skips header update upon adding.
    skip_write_header_on_add: bool,
    /// Write buffering.
    write_buf: Vec<u8>,
    /// Offset cache idx->Element. Sorted in ascending order, always unique.
    cached_offsets: VecDeque<(usize, Element)>,
    /// Offset caching policy.
    offset_cache_kind: Option<OffsetCacheKind>,
}

/// Policy controlling how element file-positions are cached to accelerate [`Iter::nth`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetCacheKind {
    /// Cache one position every `offset` elements.
    Linear { offset: usize },
    /// Cache positions at indices that are perfect squares (1, 4, 9, 16, 25, …).
    Quadratic,
}

/// Owns the backing file handle and manages all low-level I/O.
#[derive(Debug)]
struct QueueFileInner {
    file: Option<File>,
    file_len: u64,
    expected_seek: u64,
    last_seek: Option<u64>,
    transfer_buf: Box<[u8]>,
    sync_writes: bool,
    sync_context: SyncContext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncContext {
    Normal,
    ExpansionCopy,
    ClearErase,
    BacklinkRewrite,
    AppendBatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeferredSyncPhase {
    ExpansionCopy,
    ClearErase,
    BacklinkRewrite,
    AppendBatch,
}

impl Drop for QueueFile {
    fn drop(&mut self) {
        if self.skip_write_header_on_add && self.inner.file.is_some() {
            let _ = self.sync_header();
        }
    }
}

impl QueueFile {
    const INITIAL_LENGTH: u64 = 4096;
    const VERSIONED_HEADER: u32 = 0x8000_0001;
    const ZEROES: [u8; 4096] = [0; 4096];

    #[inline]
    const fn is_v2(&self) -> bool {
        matches!(self.format, FormatState::V2 { .. })
    }

    #[inline]
    const fn header_len(&self) -> u64 {
        match self.format {
            FormatState::Legacy => 16,
            FormatState::V1 => 32,
            FormatState::V2 { .. } => V2_SLOT_LEN as u64,
        }
    }

    #[inline]
    const fn data_start(&self) -> u64 {
        match self.format {
            FormatState::Legacy => 16,
            FormatState::V1 => 32,
            FormatState::V2 { .. } => V2_DATA_START,
        }
    }

    #[inline]
    const fn v2_state(&self) -> Option<(HeaderSlot, u64, u64)> {
        match self.format {
            FormatState::V2 { active_slot, generation, next_seq } => {
                Some((active_slot, generation, next_seq))
            }
            _ => None,
        }
    }

    #[inline]
    fn v2_state_mut(&mut self) -> Option<(&mut HeaderSlot, &mut u64, &mut u64)> {
        match &mut self.format {
            FormatState::V2 { active_slot, generation, next_seq } => {
                Some((active_slot, generation, next_seq))
            }
            _ => None,
        }
    }

    // ── Constructors ─────────────────────────────────────────────────────────

    /// Creates a fresh, empty queue file at `path` with the given initial file size.
    fn init(path: &Path, force_legacy: bool, capacity: u64) -> Result<()> {
        let tmp_path = path.with_extension(".tmp");

        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;

            if force_legacy {
                // Legacy / versioned: allocate `capacity` bytes then write header.
                file.set_len(capacity)?;

                let mut buf = BytesMut::with_capacity(16);
                if force_legacy {
                    buf.put_u32(capacity as u32);
                } else {
                    buf.put_u32(Self::VERSIONED_HEADER);
                    buf.put_u64(capacity);
                }
                file.write_all(buf.as_ref())?;
            } else {
                // V2: exactly 8192 bytes, two slots written.
                file.set_len(V2_INITIAL_LEN)?;

                let slot_a = SlotData {
                    file_length: V2_INITIAL_LEN,
                    element_count: 0,
                    first_position: 0,
                    last_position: 0,
                    generation: 1,
                    next_sequence_number: 1,
                };
                let slot_b = SlotData { generation: 0, ..slot_a };

                let bytes_a = build_slot_bytes(&slot_a);
                file.seek(SeekFrom::Start(V2_SLOT_A_OFFSET))?;
                file.write_all(&bytes_a)?;

                let bytes_b = build_slot_bytes(&slot_b);
                file.seek(SeekFrom::Start(V2_SLOT_B_OFFSET))?;
                file.write_all(&bytes_b)?;
            }
        }

        rename(tmp_path, path)?;
        Ok(())
    }

    /// Opens or creates a [`QueueFile`] at `path` with the given initial file size.
    pub fn with_capacity<P: AsRef<Path>>(path: P, capacity: u64) -> Result<Self> {
        Self::open_internal_full(path, true, false, capacity, true)
    }

    /// Opens or creates a [`QueueFile`] at `path` using v2 format with a 4 KiB initial file size.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::with_capacity(path, Self::INITIAL_LENGTH)
    }

    /// Opens or creates a [`QueueFile`] at `path` using the legacy (16-byte) header format.
    pub fn open_legacy<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_internal_full(path, true, true, Self::INITIAL_LENGTH, false)
    }

    /// Returns `true` if the file contains V2_MAGIC at slot A or slot B.
    fn detect_v2_magic(file: &mut File, real_file_len: u64, force_legacy: bool) -> Result<bool> {
        if force_legacy {
            return Ok(false);
        }
        let mut magic_buf = [0u8; 4];
        if real_file_len >= V2_SLOT_LEN as u64 {
            file.seek(SeekFrom::Start(V2_SLOT_A_OFFSET))?;
            file.read_exact(&mut magic_buf)?;
            if u32::from_be_bytes(magic_buf) == V2_MAGIC {
                return Ok(true);
            }
        }
        if real_file_len >= V2_SLOT_B_OFFSET + V2_SLOT_LEN as u64 {
            file.seek(SeekFrom::Start(V2_SLOT_B_OFFSET))?;
            file.read_exact(&mut magic_buf)?;
            if u32::from_be_bytes(magic_buf) == V2_MAGIC {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Parse and validate a versioned (v1) 32-byte header.
    /// Returns `(file_len, elem_cnt, first_pos, last_pos)`.
    fn parse_versioned_header(buf: &mut BytesMut) -> Result<(u64, usize, u64, u64)> {
        let version = buf.get_u32() & 0x7FFF_FFFF;
        ensure!(version == 1, UnsupportedVersionSnafu { detected: version, supported: 1u32 });

        let file_len = buf.get_u64();
        let elem_cnt = buf.get_u32() as usize;
        let first_pos = buf.get_u64();
        let last_pos = buf.get_u64();

        ensure!(i64::try_from(file_len).is_ok(), CorruptedFileSnafu {
            msg: "file length in header is greater than i64::MAX"
        });
        ensure!(i32::try_from(elem_cnt).is_ok(), CorruptedFileSnafu {
            msg: "element count in header is greater than i32::MAX"
        });
        ensure!(i64::try_from(first_pos).is_ok(), CorruptedFileSnafu {
            msg: "first element position in header is greater than i64::MAX"
        });
        ensure!(i64::try_from(last_pos).is_ok(), CorruptedFileSnafu {
            msg: "last element position in header is greater than i64::MAX"
        });
        Ok((file_len, elem_cnt, first_pos, last_pos))
    }

    /// Parse and validate a legacy 16-byte header.
    /// Returns `(file_len, elem_cnt, first_pos, last_pos)`.
    fn parse_legacy_header(buf: &mut BytesMut) -> Result<(u64, usize, u64, u64)> {
        let file_len = u64::from(buf.get_u32());
        let elem_cnt = buf.get_u32() as usize;
        let first_pos = u64::from(buf.get_u32());
        let last_pos = u64::from(buf.get_u32());

        ensure!(i32::try_from(file_len).is_ok(), CorruptedFileSnafu {
            msg: "file length in header is greater than i32::MAX"
        });
        ensure!(i32::try_from(elem_cnt).is_ok(), CorruptedFileSnafu {
            msg: "element count in header is greater than i32::MAX"
        });
        ensure!(i32::try_from(first_pos).is_ok(), CorruptedFileSnafu {
            msg: "first element position in header is greater than i32::MAX"
        });
        ensure!(i32::try_from(last_pos).is_ok(), CorruptedFileSnafu {
            msg: "last element position in header is greater than i32::MAX"
        });
        Ok((file_len, elem_cnt, first_pos, last_pos))
    }

    fn ensure_queue_file_exists(path: &Path, force_legacy: bool, capacity: u64) -> Result<()> {
        if !path.exists() {
            Self::init(
                path,
                force_legacy,
                capacity.max(if force_legacy { Self::INITIAL_LENGTH } else { V2_INITIAL_LEN }),
            )?;
        }

        Ok(())
    }

    fn parse_legacy_or_v1_header(
        file: &mut File, real_file_len: u64, force_legacy: bool,
    ) -> Result<LegacyHeaderState> {
        let mut buf = [0u8; 32];
        file.seek(SeekFrom::Start(0))?;
        let bytes_read = file.read(&mut buf)?;
        ensure!(bytes_read >= 32, CorruptedFileSnafu { msg: "file too short" });

        let versioned = !force_legacy && (buf[0] & 0x80) != 0;
        let mut buf = BytesMut::from(&buf[..]);

        let (format, header_len, file_len, elem_cnt, first_pos, last_pos) = if versioned {
            let (file_len, elem_cnt, first_pos, last_pos) = Self::parse_versioned_header(&mut buf)?;
            (FormatState::V1, 32u64, file_len, elem_cnt, first_pos, last_pos)
        } else {
            let (file_len, elem_cnt, first_pos, last_pos) = Self::parse_legacy_header(&mut buf)?;
            (FormatState::Legacy, 16u64, file_len, elem_cnt, first_pos, last_pos)
        };

        ensure!(file_len <= real_file_len, CorruptedFileSnafu {
            msg: format!(
                "file is truncated. expected length was {file_len} but actual length is {real_file_len}"
            )
        });
        ensure!(file_len >= header_len, CorruptedFileSnafu {
            msg: format!("length stored in header ({file_len}) is invalid")
        });
        ensure!(first_pos <= file_len, CorruptedFileSnafu {
            msg: format!("position of the first element ({first_pos}) is beyond the file")
        });
        ensure!(last_pos <= file_len, CorruptedFileSnafu {
            msg: format!("position of the last element ({last_pos}) is beyond the file")
        });

        let _ = header_len;

        Ok(LegacyHeaderState { format, file_len, elem_cnt, first_pos, last_pos })
    }

    fn build_legacy_queue_file(
        file: File, state: LegacyHeaderState, capacity: u64, overwrite_on_remove: bool,
    ) -> Result<Self> {
        let mut queue_file = Self {
            inner: QueueFileInner {
                file: Some(file),
                file_len: state.file_len,
                expected_seek: 0,
                last_seek: Some(32),
                transfer_buf: vec![0u8; QueueFileInner::TRANSFER_BUFFER_SIZE].into_boxed_slice(),
                sync_writes: cfg!(not(test)),
                sync_context: SyncContext::Normal,
            },
            format: state.format,
            elem_cnt: state.elem_cnt,
            first: Element::EMPTY,
            last: Element::EMPTY,
            capacity,
            overwrite_on_remove,
            skip_write_header_on_add: false,
            write_buf: Vec::new(),
            cached_offsets: VecDeque::new(),
            offset_cache_kind: None,
        };

        if state.file_len < capacity {
            queue_file.inner.sync_set_len(queue_file.capacity)?;
        }

        queue_file.first = queue_file.read_element(state.first_pos)?;
        queue_file.last = queue_file.read_element(state.last_pos)?;

        Ok(queue_file)
    }

    fn open_internal_full<P: AsRef<Path>>(
        path: P, overwrite_on_remove: bool, force_legacy: bool, capacity: u64,
        allow_migration: bool,
    ) -> Result<Self> {
        let path = path.as_ref();

        Self::ensure_queue_file_exists(path, force_legacy, capacity)?;

        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let real_file_len = file.metadata()?.len();

        if Self::detect_v2_magic(&mut file, real_file_len, force_legacy)? {
            return Self::open_v2(file, real_file_len, capacity, overwrite_on_remove, path);
        }

        let state = Self::parse_legacy_or_v1_header(&mut file, real_file_len, force_legacy)?;

        if allow_migration && !force_legacy {
            drop(file);
            Self::migrate_to_v2(path)?;
            return Self::open_internal_full(path, overwrite_on_remove, false, capacity, false);
        }

        Self::build_legacy_queue_file(file, state, capacity, overwrite_on_remove)
    }

    /// Open a file already detected as v2 format.
    fn open_v2(
        file: File, real_file_len: u64, capacity: u64, overwrite_on_remove: bool, _path: &Path,
    ) -> Result<Self> {
        let mut inner = QueueFileInner {
            file: Some(file),
            file_len: real_file_len,
            expected_seek: 0,
            last_seek: None,
            transfer_buf: vec![0u8; QueueFileInner::TRANSFER_BUFFER_SIZE].into_boxed_slice(),
            sync_writes: cfg!(not(test)),
            sync_context: SyncContext::Normal,
        };

        let open_state = Self::read_v2_open_state(&mut inner, real_file_len)?;
        let mut qf = Self::build_v2_queue_file(inner, open_state, capacity, overwrite_on_remove);
        qf.initialize_v2_endpoints(open_state.slot)?;

        if open_state.slot.file_length < qf.capacity {
            qf.inner.sync_set_len(qf.capacity)?;
        }

        Ok(qf)
    }

    fn read_v2_open_state(inner: &mut QueueFileInner, real_file_len: u64) -> Result<V2OpenState> {
        let raw_a = if real_file_len >= V2_SLOT_LEN as u64 {
            read_slot(inner, V2_SLOT_A_OFFSET).ok()
        } else {
            None
        };
        let raw_b = if real_file_len >= V2_SLOT_B_OFFSET + V2_SLOT_LEN as u64 {
            read_slot(inner, V2_SLOT_B_OFFSET).ok()
        } else {
            None
        };

        let slot_a = raw_a.as_ref().and_then(parse_slot);
        let slot_b = raw_b.as_ref().and_then(parse_slot);
        let (active_slot, slot) = elect_canonical_slot(slot_a, slot_b)?;
        validate_v2_slot_data(&slot, real_file_len)?;

        Ok(V2OpenState { active_slot, slot, elem_cnt: slot.element_count as usize })
    }

    fn build_v2_queue_file(
        mut inner: QueueFileInner, open_state: V2OpenState, capacity: u64,
        overwrite_on_remove: bool,
    ) -> Self {
        inner.file_len = open_state.slot.file_length;

        Self {
            inner,
            format: FormatState::V2 {
                active_slot: open_state.active_slot,
                generation: open_state.slot.generation,
                next_seq: open_state.slot.next_sequence_number,
            },
            elem_cnt: open_state.elem_cnt,
            first: Element::EMPTY,
            last: Element::EMPTY,
            capacity: capacity.max(V2_INITIAL_LEN),
            overwrite_on_remove,
            skip_write_header_on_add: false,
            write_buf: Vec::new(),
            cached_offsets: VecDeque::new(),
            offset_cache_kind: None,
        }
    }

    fn initialize_v2_endpoints(&mut self, slot: SlotData) -> Result<()> {
        if self.elem_cnt == 0 {
            return Ok(());
        }

        let last_header = self.validate_v2_element_header(slot.last_position)?;
        ensure!(last_header.seq == slot.next_sequence_number - 1, CorruptedFileSnafu {
            msg: format!(
                "v2 tail seq {} != next_seq-1 {}",
                last_header.seq,
                slot.next_sequence_number - 1
            )
        });

        self.last =
            Element { pos: slot.last_position, len: last_header.payload_len, seq: last_header.seq };

        self.first = match self.validate_v2_element_header(slot.first_position) {
            Ok(first_header) => Element {
                pos: slot.first_position,
                len: first_header.payload_len,
                seq: first_header.seq,
            },
            Err(_) => self.recover_v2_head(slot.last_position, last_header.seq, self.elem_cnt)?,
        };

        Ok(())
    }

    /// Validate a v2 element header at `pos`, returning parsed header state.
    fn validate_v2_element_header(&self, pos: u64) -> Result<V2ElementHeader> {
        let mut hdr = [0u8; V2_ELEM_HDR_LEN];
        self.ring_read(pos, &mut hdr)?;

        let magic = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
        ensure!(magic == V2_ELEM_HDR_MAGIC, CorruptedFileSnafu {
            msg: format!("v2 element header magic mismatch at pos {pos}: {magic:#010x}")
        });

        let seq =
            i64::from_be_bytes([hdr[4], hdr[5], hdr[6], hdr[7], hdr[8], hdr[9], hdr[10], hdr[11]])
                as u64;
        let prev_pos = i64::from_be_bytes([
            hdr[12], hdr[13], hdr[14], hdr[15], hdr[16], hdr[17], hdr[18], hdr[19],
        ]) as u64;
        let payload_len_raw = i32::from_be_bytes([hdr[20], hdr[21], hdr[22], hdr[23]]);
        ensure!(payload_len_raw >= 0, CorruptedFileSnafu {
            msg: format!("v2 element payload_len {payload_len_raw} is negative at pos {pos}")
        });
        let payload_len = payload_len_raw as usize; // safe: non-negative i32 fits in usize
        ensure!(seq >= 1, CorruptedFileSnafu {
            msg: format!("v2 element seq {seq} < 1 at pos {pos}")
        });

        let expected_crc = compute_elem_header_crc(&hdr);
        let stored_crc = u32::from_be_bytes([hdr[24], hdr[25], hdr[26], hdr[27]]);
        ensure!(stored_crc == expected_crc, CorruptedFileSnafu {
            msg: format!("v2 element header CRC mismatch at pos {pos}")
        });

        // Validate span fits.
        let span = V2_ELEM_OVERHEAD + payload_len as u64;
        ensure!(span <= self.file_len() - V2_DATA_START, CorruptedFileSnafu {
            msg: format!("v2 element span {span} exceeds data region")
        });

        Ok(V2ElementHeader { payload_len, seq, prev_pos })
    }

    /// Attempt head recovery by walking backward via `prev_pos` backlinks from the tail.
    fn recover_v2_head(
        &mut self, last_pos: u64, last_seq: u64, elem_cnt: usize,
    ) -> Result<Element> {
        let mut cur_pos = last_pos;

        for step in 0..elem_cnt {
            let current_header = self.validate_v2_element_header(cur_pos)?;

            let expected_seq =
                last_seq.checked_sub(step as u64).ok_or_else(|| Error::CorruptedFile {
                    msg: format!(
                        "v2 recovery: tail seq {last_seq} too small for element_count {elem_cnt}"
                    ),
                })?;
            ensure!(current_header.seq == expected_seq, CorruptedFileSnafu {
                msg: format!("v2 recovery: seq {} != expected {expected_seq}", current_header.seq)
            });

            let current =
                Element { pos: cur_pos, len: current_header.payload_len, seq: current_header.seq };

            if step + 1 == elem_cnt {
                return Ok(current);
            }

            ensure!(current_header.prev_pos != 0, CorruptedFileSnafu {
                msg: format!("v2 recovery: walked {} elements but expected {}", step + 1, elem_cnt)
            });

            cur_pos = current_header.prev_pos;
        }

        Err(Error::CorruptedFile {
            msg: "v2 recovery: could not walk expected live element count".to_owned(),
        })
    }

    // ── Public API ────────────────────────────────────────────────────────────

    #[inline]
    pub const fn overwrite_on_remove(&self) -> bool {
        self.overwrite_on_remove
    }

    #[deprecated(since = "1.4.7", note = "Use `overwrite_on_remove` instead.")]
    pub const fn get_overwrite_on_remove(&self) -> bool {
        self.overwrite_on_remove()
    }

    #[inline]
    pub fn set_overwrite_on_remove(&mut self, value: bool) {
        self.overwrite_on_remove = value;
    }

    #[inline]
    pub const fn sync_writes(&self) -> bool {
        self.inner.sync_writes
    }

    #[deprecated(since = "1.4.7", note = "Use `sync_writes` instead.")]
    pub const fn get_sync_writes(&self) -> bool {
        self.sync_writes()
    }

    #[inline]
    pub fn set_sync_writes(&mut self, value: bool) {
        self.inner.sync_writes = value;
    }

    #[inline]
    pub const fn skip_write_header_on_add(&self) -> bool {
        self.skip_write_header_on_add
    }

    #[deprecated(since = "1.4.7", note = "Use `skip_write_header_on_add` instead.")]
    pub const fn get_skip_write_header_on_add(&self) -> bool {
        self.skip_write_header_on_add()
    }

    #[inline]
    pub fn set_skip_write_header_on_add(&mut self, value: bool) {
        self.skip_write_header_on_add = value;
    }

    #[inline]
    pub const fn cache_offset_policy(&self) -> Option<OffsetCacheKind> {
        self.offset_cache_kind
    }

    #[deprecated(since = "1.4.7", note = "Use `cache_offset_policy` instead.")]
    pub const fn get_cache_offset_policy(&self) -> Option<OffsetCacheKind> {
        self.cache_offset_policy()
    }

    #[inline]
    pub fn set_cache_offset_policy(&mut self, kind: impl Into<Option<OffsetCacheKind>>) {
        self.offset_cache_kind = kind.into();

        if self.offset_cache_kind.is_none() {
            self.cached_offsets.clear();
        }
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.elem_cnt == 0
    }

    #[inline]
    pub const fn size(&self) -> usize {
        self.elem_cnt
    }

    pub fn sync_all(&mut self) -> Result<()> {
        if self.skip_write_header_on_add {
            self.inner.file_mut()?.sync_data()?; // Barrier: ensure payloads are durable
            self.sync_header()?; // Write the header
        }

        Ok(self.inner.file_mut()?.sync_all()?) // Barrier: ensure header is durable
    }

    // ── Cache helpers ─────────────────────────────────────────────────────────

    fn cache_last_offset_if_needed(&mut self, affected_items: usize) {
        if self.elem_cnt == 0 {
            return;
        }

        self.cache_elem_if_needed(self.elem_cnt - 1, self.last, affected_items);
    }

    fn snapshot_queue_state(&self) -> QueueStateSnapshot {
        QueueStateSnapshot {
            format: self.format,
            elem_cnt: self.elem_cnt,
            first: self.first,
            last: self.last,
            overwrite_on_remove: self.overwrite_on_remove,
            cached_offsets: self.cached_offsets.clone(),
        }
    }

    fn restore_queue_state(&mut self, snapshot: QueueStateSnapshot) {
        self.format = snapshot.format;
        self.elem_cnt = snapshot.elem_cnt;
        self.first = snapshot.first;
        self.last = snapshot.last;
        self.overwrite_on_remove = snapshot.overwrite_on_remove;
        self.cached_offsets = snapshot.cached_offsets;
    }

    fn cache_elem_if_needed(&mut self, index: usize, elem: Element, affected_items: usize) {
        debug_assert!(index <= self.elem_cnt);
        debug_assert!(index + 1 >= affected_items);

        let need_to_cache = self.offset_cache_kind.map_or(false, |kind| match kind {
            OffsetCacheKind::Linear { offset } => {
                let last_cached_index = self.cached_offsets.back().map_or(0, |(idx, _)| *idx);
                index.saturating_sub(last_cached_index) >= offset
            }
            OffsetCacheKind::Quadratic => {
                let x = (index as f64).sqrt() as usize;
                x > 1 && (index + 1 - affected_items..=index).contains(&(x * x))
            }
        });

        if need_to_cache {
            if let Some((last_cached_index, last_cached_elem)) = self.cached_offsets.back() {
                if *last_cached_index >= index {
                    if *last_cached_index == index {
                        debug_assert_eq!(last_cached_elem.pos, elem.pos);
                        debug_assert_eq!(last_cached_elem.len, elem.len);
                    }

                    return;
                }
            }

            self.cached_offsets.push_back((index, elem));
        }
    }

    #[inline]
    fn cached_index_up_to(&self, i: usize) -> Option<usize> {
        self.cached_offsets
            .binary_search_by(|(idx, _)| idx.cmp(&i))
            .map_or_else(|i| i.checked_sub(1), Some)
    }

    // ── add_n ─────────────────────────────────────────────────────────────────

    /// Adds multiple elements to the end of the queue in a single write.
    pub fn add_n(&mut self, elems: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Result<()> {
        if self.is_v2() {
            return self.add_n_v2(elems);
        }

        let snapshot = self.snapshot_queue_state();

        let result = (|| {
            let mut count = 0usize;

            for elem in elems {
                self.overwrite_on_remove =
                    if count == 0 { snapshot.overwrite_on_remove } else { false };
                ensure!(self.elem_cnt + 1 < i32::MAX as usize, TooManyElementsSnafu {});

                let elem = elem.as_ref();
                let len = elem.len();
                let span = Element::HEADER_LENGTH as u64 + len as u64;
                self.expand_if_necessary(span)?;

                let pos = if self.is_empty() {
                    self.data_start()
                } else {
                    self.wrap_pos(
                        self.last.pos + Element::HEADER_LENGTH as u64 + self.last.len as u64,
                    )
                };
                let elem_entry = Element::new(pos, len, 0)?;

                self.write_buf.clear();
                self.write_buf.extend(&(len as u32).to_be_bytes());
                self.write_buf.extend(elem);
                self.ring_write_buf(pos)?;

                if self.is_empty() {
                    self.first = elem_entry;
                }
                self.last = elem_entry;
                self.elem_cnt += 1;
                count += 1;
            }

            if count == 0 {
                return Ok(0);
            }

            if !self.skip_write_header_on_add {
                self.write_header(self.file_len(), self.elem_cnt, self.first.pos, self.last.pos)?;
            }

            self.cache_last_offset_if_needed(count);

            Ok(count)
        })();

        self.overwrite_on_remove = snapshot.overwrite_on_remove;

        match result {
            Ok(_) => Ok(()),
            Err(err) => {
                self.restore_queue_state(snapshot);
                Err(err)
            }
        }
    }

    fn add_n_v2(&mut self, elems: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Result<()> {
        let snapshot = self.snapshot_queue_state();
        let (_, _, base_seq) = self.v2_state().ok_or_else(|| Error::CorruptedFile {
            msg: "operation requires V2 format state".to_owned(),
        })?;

        let result = self.with_batched_v2_append_sync(|queue_file| {
            let mut count = 0usize;

            for elem in elems {
                queue_file.overwrite_on_remove =
                    if count == 0 { snapshot.overwrite_on_remove } else { false };
                ensure!(queue_file.elem_cnt + 1 < i32::MAX as usize, TooManyElementsSnafu {});

                let elem = elem.as_ref();
                let len = elem.len();
                ensure!(i32::try_from(len).is_ok(), ElementTooBigSnafu {});

                queue_file.expand_if_necessary(V2_ELEM_OVERHEAD + len as u64)?;

                let pos = if queue_file.is_empty() {
                    V2_DATA_START
                } else {
                    queue_file.wrap_pos(
                        queue_file.last.pos + V2_ELEM_OVERHEAD + queue_file.last.len as u64,
                    )
                };
                let seq = base_seq + count as u64;
                let prev_pos = if queue_file.is_empty() { 0 } else { queue_file.last.pos };

                let ds = queue_file.data_start();
                let fl = queue_file.file_len();
                Self::write_v2_element(&mut queue_file.inner, pos, seq, prev_pos, elem, ds, fl)?;

                let elem_entry = Element { pos, len, seq };
                if queue_file.is_empty() {
                    queue_file.first = elem_entry;
                }
                queue_file.last = elem_entry;
                queue_file.elem_cnt += 1;
                count += 1;
            }

            Ok(count)
        });

        self.overwrite_on_remove = snapshot.overwrite_on_remove;

        match result {
            Ok(count) => {
                if count != 0 {
                    let (_, _, next_seq) =
                        self.v2_state_mut().ok_or_else(|| Error::CorruptedFile {
                            msg: "operation requires V2 format state".to_owned(),
                        })?;
                    *next_seq = base_seq + count as u64;

                    if !self.skip_write_header_on_add {
                        self.write_header(
                            self.file_len(),
                            self.elem_cnt,
                            self.first.pos,
                            self.last.pos,
                        )?;
                    }

                    self.cache_last_offset_if_needed(count);
                }

                Ok(())
            }
            Err(err) => {
                self.restore_queue_state(snapshot);
                Err(err)
            }
        }
    }

    /// Write a single v2 element (header + payload + footer) at `pos` in the ring buffer.
    fn write_v2_element(
        inner: &mut QueueFileInner, pos: u64, seq: u64, prev_pos: u64, payload: &[u8],
        data_start: u64, file_len: u64,
    ) -> Result<()> {
        let payload_len = payload.len();

        // Build 28-byte header.
        let mut hdr = [0u8; V2_ELEM_HDR_LEN];
        {
            let mut w: &mut [u8] = &mut hdr;
            w.put_u32(V2_ELEM_HDR_MAGIC);
            w.put_i64(seq as i64);
            w.put_i64(prev_pos as i64);
            w.put_i32(payload_len as i32);
        }
        let hdr_crc = compute_elem_header_crc(&hdr);
        hdr[24..28].copy_from_slice(&hdr_crc.to_be_bytes());

        // Ring-write header.
        Self::ring_write_raw(inner, pos, &hdr, data_start, file_len)?;

        // Ring-write payload.
        let payload_pos = wrap_pos_fn(pos + V2_ELEM_HDR_LEN as u64, file_len, data_start);
        Self::ring_write_raw(inner, payload_pos, payload, data_start, file_len)?;

        // Build 16-byte footer.
        let footer_pos =
            wrap_pos_fn(pos + V2_ELEM_HDR_LEN as u64 + payload_len as u64, file_len, data_start);
        let mut ftr = [0u8; V2_ELEM_FTR_LEN];
        {
            let mut w: &mut [u8] = &mut ftr;
            w.put_u32(V2_ELEM_FTR_MAGIC);
            w.put_i64(seq as i64);
        }
        let ftr_crc = compute_elem_footer_crc(payload, &ftr[..12]);
        ftr[12..16].copy_from_slice(&ftr_crc.to_be_bytes());

        Self::ring_write_raw(inner, footer_pos, &ftr, data_start, file_len)?;

        Ok(())
    }

    /// Low-level ring write that does not use `self.write_buf`.
    fn ring_write_raw(
        inner: &mut QueueFileInner, pos: u64, data: &[u8], data_start: u64, file_len: u64,
    ) -> Result<()> {
        let pos = wrap_pos_fn(pos, file_len, data_start);

        if pos + data.len() as u64 <= file_len {
            inner.seek(pos);
            inner.write(data)
        } else {
            let before_eof = (file_len - pos) as usize;
            inner.seek(pos);
            inner.write(&data[..before_eof])?;
            inner.seek(data_start);
            inner.write(&data[before_eof..])
        }
    }

    /// Appends a single element to the tail of the queue.
    #[inline]
    pub fn add(&mut self, buf: &[u8]) -> Result<()> {
        ensure!(self.elem_cnt + 1 < i32::MAX as usize, TooManyElementsSnafu {});

        if self.is_v2() {
            let len = buf.len();
            ensure!(i32::try_from(len).is_ok(), ElementTooBigSnafu {});

            self.expand_if_necessary(V2_ELEM_OVERHEAD + len as u64)?;

            let pos = if self.is_empty() {
                V2_DATA_START
            } else {
                self.wrap_pos(self.last.pos + V2_ELEM_OVERHEAD + self.last.len as u64)
            };
            let seq = self
                .v2_state()
                .ok_or_else(|| Error::CorruptedFile {
                    msg: "operation requires V2 format state".to_owned(),
                })?
                .2;
            let prev_pos = if self.is_empty() { 0 } else { self.last.pos };

            self.with_batched_v2_append_sync(|queue_file| {
                let ds = queue_file.data_start();
                let fl = queue_file.file_len();
                Self::write_v2_element(&mut queue_file.inner, pos, seq, prev_pos, buf, ds, fl)
            })?;

            let elem = Element { pos, len, seq };
            if self.is_empty() {
                self.first = elem;
            }
            self.last = elem;
            self.elem_cnt += 1;

            let (_, _, next_seq) = self.v2_state_mut().ok_or_else(|| Error::CorruptedFile {
                msg: "operation requires V2 format state".to_owned(),
            })?;
            *next_seq += 1;
        } else {
            let len = buf.len();
            self.expand_if_necessary(Element::HEADER_LENGTH as u64 + len as u64)?;

            let pos = if self.is_empty() {
                self.data_start()
            } else {
                self.wrap_pos(self.last.pos + Element::HEADER_LENGTH as u64 + self.last.len as u64)
            };
            let elem = Element::new(pos, len, 0)?;

            self.write_buf.clear();
            self.write_buf.extend(&(len as u32).to_be_bytes());
            self.write_buf.extend(buf);
            self.ring_write_buf(pos)?;

            if self.is_empty() {
                self.first = elem;
            }
            self.last = elem;
            self.elem_cnt += 1;
        }

        if !self.skip_write_header_on_add {
            self.write_header(self.file_len(), self.elem_cnt, self.first.pos, self.last.pos)?;
        }

        self.cache_last_offset_if_needed(1);

        Ok(())
    }

    fn validate_v2_footer(&self, footer_pos: u64, seq: u64, payload: &[u8]) -> Result<()> {
        let mut ftr = [0u8; V2_ELEM_FTR_LEN];
        self.ring_read(footer_pos, &mut ftr)?;

        let ftr_magic = u32::from_be_bytes([ftr[0], ftr[1], ftr[2], ftr[3]]);
        ensure!(ftr_magic == V2_ELEM_FTR_MAGIC, CorruptedFileSnafu {
            msg: format!("v2 element footer magic mismatch: {ftr_magic:#010x}")
        });

        let ftr_seq =
            i64::from_be_bytes([ftr[4], ftr[5], ftr[6], ftr[7], ftr[8], ftr[9], ftr[10], ftr[11]])
                as u64;
        ensure!(ftr_seq == seq, CorruptedFileSnafu {
            msg: format!("v2 footer seq {ftr_seq} != element seq {seq}")
        });

        let stored_crc = u32::from_be_bytes([ftr[12], ftr[13], ftr[14], ftr[15]]);
        let expected_crc = compute_elem_footer_crc(payload, &ftr[..12]);
        ensure!(stored_crc == expected_crc, CorruptedFileSnafu {
            msg: "v2 element footer CRC mismatch".to_owned()
        });

        Ok(())
    }

    // ── peek ──────────────────────────────────────────────────────────────────

    /// Returns the head element without removing it.
    pub fn peek(&self) -> Result<Option<Vec<u8>>> {
        if self.is_empty() {
            return Ok(None);
        }

        let mut buf = Vec::with_capacity(self.first.len);

        if self.peek_into(&mut buf)? { Ok(Some(buf)) } else { Ok(None) }
    }

    /// Returns the head element without removing it.
    pub fn peek_into(&self, buf: &mut Vec<u8>) -> Result<bool> {
        if self.is_empty() {
            return Ok(false);
        }

        let len = self.first.len;
        buf.resize(len, 0); // Reuses existing capacity without reallocating

        let payload_start = if self.is_v2() {
            self.wrap_pos(self.first.pos + V2_ELEM_HDR_LEN as u64)
        } else {
            self.first.pos + Element::HEADER_LENGTH as u64
        };

        self.ring_read(payload_start, buf)?;

        if self.is_v2() {
            let footer_pos = self.wrap_pos(payload_start + len as u64);
            self.validate_v2_footer(footer_pos, self.first.seq, buf)?;
        }

        Ok(true)
    }

    // ── remove_n ─────────────────────────────────────────────────────────────

    #[inline]
    pub fn remove(&mut self) -> Result<()> {
        self.remove_n(1)
    }

    pub fn remove_n(&mut self, n: usize) -> Result<()> {
        if n == 0 || self.is_empty() {
            return Ok(());
        }

        if n >= self.elem_cnt {
            return self.clear();
        }

        debug_assert!(
            self.cached_offsets
                .iter()
                .zip(self.cached_offsets.iter().skip(1))
                .all(|(a, b)| a.0 < b.0),
            "{:?}",
            self.cached_offsets
        );

        if self.is_v2() {
            return self.remove_n_v2(n);
        }

        let erase_start_pos = self.first.pos;
        let mut erase_total_len = 0usize;

        let mut new_first_pos = self.first.pos;
        let mut new_first_len = self.first.len;

        // Legacy / versioned path.
        let cached_index = self.cached_index_up_to(n - 1);
        let to_remove = if let Some(i) = cached_index {
            let (index, elem) = self.cached_offsets[i];

            if let Some(index) = index.checked_sub(1) {
                erase_total_len += Element::HEADER_LENGTH * index;
                erase_total_len += (elem.pos
                    + if self.first.pos < elem.pos {
                        0
                    } else {
                        self.file_len() - self.first.pos - self.data_start()
                    }) as usize;
            }

            new_first_pos = elem.pos;
            new_first_len = elem.len;
            n - index
        } else {
            n
        };

        for _ in 0..to_remove {
            erase_total_len += Element::HEADER_LENGTH + new_first_len;
            new_first_pos =
                self.wrap_pos(new_first_pos + Element::HEADER_LENGTH as u64 + new_first_len as u64);

            let mut buf: [u8; 4] = [0; 4];
            self.ring_read(new_first_pos, &mut buf)?;
            new_first_len = u32::from_be_bytes(buf) as usize;
        }

        self.write_header(self.file_len(), self.elem_cnt - n, new_first_pos, self.last.pos)?;
        self.elem_cnt -= n;
        self.first = Element::new(new_first_pos, new_first_len, 0)?;

        if let Some(cached_index) = cached_index {
            self.cached_offsets.drain(..=cached_index);
        }
        self.cached_offsets.iter_mut().for_each(|(i, _)| *i -= n);

        if self.overwrite_on_remove {
            self.ring_erase(erase_start_pos, erase_total_len)?;
        }

        Ok(())
    }

    fn remove_n_v2(&mut self, n: usize) -> Result<()> {
        let erase_start_pos = self.first.pos;
        let mut erase_total_len = 0usize;
        let mut new_first_pos = self.first.pos;
        let mut new_first_len = self.first.len;

        for _ in 0..n {
            erase_total_len += (V2_ELEM_OVERHEAD as usize) + new_first_len;
            new_first_pos = self.wrap_pos(new_first_pos + V2_ELEM_OVERHEAD + new_first_len as u64);

            new_first_len = self.validate_v2_element_header(new_first_pos)?.payload_len;
        }

        let new_first_seq = self.validate_v2_element_header(new_first_pos)?.seq;

        self.write_header(self.file_len(), self.elem_cnt - n, new_first_pos, self.last.pos)?;
        self.elem_cnt -= n;
        self.first = Element { pos: new_first_pos, len: new_first_len, seq: new_first_seq };
        self.cached_offsets.clear();

        if self.overwrite_on_remove {
            self.ring_erase(erase_start_pos, erase_total_len)?;
        }

        Ok(())
    }

    // ── clear ─────────────────────────────────────────────────────────────────

    pub fn clear(&mut self) -> Result<()> {
        if self.is_v2() {
            // For v2: commit empty state, zero data region, truncate to V2_INITIAL_LEN floor.
            let new_cap = self.capacity.max(V2_INITIAL_LEN);

            self.write_header(new_cap, 0, 0, 0)?;

            if self.overwrite_on_remove {
                // Zero the data region.
                let data_end = self.file_len().min(new_cap);
                if data_end > V2_DATA_START {
                    self.with_batched_clear_erase_sync(|queue_file| {
                        queue_file
                            .write_zero_chunks(V2_DATA_START, (data_end - V2_DATA_START) as usize)
                    })?;
                }
            }

            self.cached_offsets.clear();
            self.elem_cnt = 0;
            self.first = Element::EMPTY;
            self.last = Element::EMPTY;

            if self.file_len() > new_cap {
                self.inner.sync_set_len(new_cap)?;
            }

            return Ok(());
        }

        // Legacy / versioned.
        self.write_header(self.capacity, 0, 0, 0)?;

        if self.overwrite_on_remove {
            self.with_batched_clear_erase_sync(|queue_file| {
                queue_file.write_zero_chunks(
                    queue_file.data_start(),
                    (queue_file.capacity - queue_file.data_start()) as usize,
                )
            })?;
        }

        self.cached_offsets.clear();

        self.elem_cnt = 0;
        self.first = Element::EMPTY;
        self.last = Element::EMPTY;

        if self.file_len() > self.capacity {
            self.inner.sync_set_len(self.capacity)?;
        }

        Ok(())
    }

    // ── iter ──────────────────────────────────────────────────────────────────

    pub fn iter(&self) -> Iter<'_> {
        Iter {
            buffer: Vec::new(),
            queue_file: self,
            next_elem_index: 0,
            next_elem_pos: self.first.pos,
        }
    }

    // ── Metrics ───────────────────────────────────────────────────────────────

    #[inline]
    pub const fn file_len(&self) -> u64 {
        self.inner.file_len
    }

    #[inline]
    pub const fn used_bytes(&self) -> u64 {
        if self.elem_cnt == 0 {
            self.data_start()
        } else if self.is_v2() {
            if self.last.pos >= self.first.pos {
                (self.last.pos - self.first.pos)
                    + V2_ELEM_OVERHEAD
                    + self.last.len as u64
                    + self.data_start()
            } else {
                self.last.pos + V2_ELEM_OVERHEAD + self.last.len as u64 + self.file_len()
                    - self.first.pos
            }
        } else if self.last.pos >= self.first.pos {
            (self.last.pos - self.first.pos)
                + Element::HEADER_LENGTH as u64
                + self.last.len as u64
                + self.data_start()
        } else {
            self.last.pos + Element::HEADER_LENGTH as u64 + self.last.len as u64 + self.file_len()
                - self.first.pos
        }
    }

    pub fn into_inner_file(mut self) -> Result<File> {
        if self.skip_write_header_on_add {
            self.sync_header()?;
        }

        self.inner.file.take().ok_or_else(|| Error::Io {
            source: io::Error::new(io::ErrorKind::BrokenPipe, "file handle already consumed"),
        })
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    #[inline]
    const fn remaining_bytes(&self) -> u64 {
        self.file_len() - self.used_bytes()
    }

    fn sync_header(&mut self) -> Result<()> {
        self.write_header(self.file_len(), self.size(), self.first.pos, self.last.pos)
    }

    /// Returns the element header length for the active format.
    #[inline]
    const fn elem_hdr_len(&self) -> u64 {
        match self.format {
            FormatState::V2 { .. } => V2_ELEM_HDR_LEN as u64,
            _ => Element::HEADER_LENGTH as u64,
        }
    }

    /// Returns the total on-disk span of an element with the given payload length.
    #[inline]
    const fn elem_span(&self, payload_len: usize) -> u64 {
        match self.format {
            FormatState::V2 { .. } => V2_ELEM_OVERHEAD + payload_len as u64,
            _ => Element::HEADER_LENGTH as u64 + payload_len as u64,
        }
    }

    fn write_header(
        &mut self, file_len: u64, elem_cnt: usize, first_pos: u64, last_pos: u64,
    ) -> Result<()> {
        if self.is_v2() {
            return self.write_header_v2(file_len, elem_cnt, first_pos, last_pos);
        }

        let mut header = [0u8; 32];
        let mut header_buf: &mut [u8] = &mut header;

        if matches!(self.format, FormatState::V1) {
            ensure!(i64::try_from(file_len).is_ok(), CorruptedFileSnafu {
                msg: "file length in header will exceed i64::MAX"
            });
            ensure!(i32::try_from(elem_cnt).is_ok(), CorruptedFileSnafu {
                msg: "element count in header will exceed i32::MAX"
            });
            ensure!(i64::try_from(first_pos).is_ok(), CorruptedFileSnafu {
                msg: "first element position in header will exceed i64::MAX"
            });
            ensure!(i64::try_from(last_pos).is_ok(), CorruptedFileSnafu {
                msg: "last element position in header will exceed i64::MAX"
            });

            header_buf.put_u32(Self::VERSIONED_HEADER);
            header_buf.put_u64(file_len);
            header_buf.put_i32(elem_cnt as i32);
            header_buf.put_u64(first_pos);
            header_buf.put_u64(last_pos);
        } else {
            ensure!(i32::try_from(file_len).is_ok(), CorruptedFileSnafu {
                msg: "file length in header will exceed i32::MAX"
            });
            ensure!(i32::try_from(elem_cnt).is_ok(), CorruptedFileSnafu {
                msg: "element count in header will exceed i32::MAX"
            });
            ensure!(i32::try_from(first_pos).is_ok(), CorruptedFileSnafu {
                msg: "first element position in header will exceed i32::MAX"
            });
            ensure!(i32::try_from(last_pos).is_ok(), CorruptedFileSnafu {
                msg: "last element position in header will exceed i32::MAX"
            });

            header_buf.put_i32(file_len as i32);
            header_buf.put_i32(elem_cnt as i32);
            header_buf.put_i32(first_pos as i32);
            header_buf.put_i32(last_pos as i32);
        }

        self.inner.seek(0);
        self.inner.write(&header[..self.header_len() as usize])
    }

    fn write_header_v2(
        &mut self, file_len: u64, elem_cnt: usize, first_pos: u64, last_pos: u64,
    ) -> Result<()> {
        let (next_slot, generation, next_seq) = {
            let (active_slot, generation, next_seq) = self.v2_state().ok_or_else(|| {
                Error::CorruptedFile { msg: "operation requires V2 format state".to_owned() }
            })?;
            (active_slot.toggle(), generation, next_seq)
        };

        ensure!(i64::try_from(file_len).is_ok(), CorruptedFileSnafu {
            msg: "file length in V2 header will exceed i64::MAX"
        });
        ensure!(u32::try_from(elem_cnt).is_ok(), CorruptedFileSnafu {
            msg: "element count in V2 header will exceed u32::MAX"
        });
        ensure!(i64::try_from(first_pos).is_ok(), CorruptedFileSnafu {
            msg: "first element position in V2 header will exceed i64::MAX"
        });
        ensure!(i64::try_from(last_pos).is_ok(), CorruptedFileSnafu {
            msg: "last element position in V2 header will exceed i64::MAX"
        });

        let slot_data = SlotData {
            file_length: file_len,
            element_count: elem_cnt as u32, // validated above
            first_position: first_pos,
            last_position: last_pos,
            generation: generation + 1,
            next_sequence_number: next_seq,
        };

        let slot_bytes = build_slot_bytes(&slot_data);

        let offset = next_slot.offset();

        // Write directly to absolute offset (not ring buffer).
        self.inner.seek(offset);
        self.inner.write(&slot_bytes)?;

        let (active_slot, generation, _) = self.v2_state_mut().ok_or_else(|| {
            Error::CorruptedFile { msg: "operation requires V2 format state".to_owned() }
        })?;
        *generation += 1;
        *active_slot = next_slot;

        Ok(())
    }

    fn read_element(&self, pos: u64) -> Result<Element> {
        if pos == 0 {
            return Ok(Element::EMPTY);
        }

        if self.is_v2() {
            let header = self.validate_v2_element_header(pos)?;
            return Ok(Element { pos, len: header.payload_len, seq: header.seq });
        }

        let mut buf: [u8; 4] = [0; Element::HEADER_LENGTH];
        self.ring_read(pos, &mut buf)?;

        Element::new(pos, u32::from_be_bytes(buf) as usize, 0)
    }

    #[inline]
    const fn wrap_pos(&self, pos: u64) -> u64 {
        wrap_pos_fn(pos, self.inner.file_len, self.data_start())
    }

    fn ring_write_buf(&mut self, pos: u64) -> Result<()> {
        let pos = self.wrap_pos(pos);

        if pos + self.write_buf.len() as u64 <= self.file_len() {
            self.inner.seek(pos);
            self.inner.write(&self.write_buf)
        } else {
            let before_eof = (self.file_len() - pos) as usize;

            self.inner.seek(pos);
            self.inner.write(&self.write_buf[..before_eof])?;
            self.inner.seek(self.data_start());
            self.inner.write(&self.write_buf[before_eof..])
        }
    }

    fn ring_erase(&mut self, pos: u64, n: usize) -> Result<()> {
        let mut pos = pos;
        let mut len = n;

        while len > 0 {
            let chunk_len = min(len, Self::ZEROES.len());
            self.ring_write_raw_from_self(pos, &Self::ZEROES[..chunk_len])?;

            len -= chunk_len;
            pos += chunk_len as u64;
        }

        Ok(())
    }

    fn ring_read(&self, pos: u64, buf: &mut [u8]) -> io::Result<()> {
        let pos = self.wrap_pos(pos);

        if pos + buf.len() as u64 <= self.file_len() {
            self.inner.read_exact_at(pos, buf)
        } else {
            let before_eof = (self.file_len() - pos) as usize;

            self.inner.read_exact_at(pos, &mut buf[..before_eof])?;
            self.inner.read_exact_at(self.data_start(), &mut buf[before_eof..])
        }
    }

    /// Compute the smallest power-of-two multiple of `current_len` that adds enough
    /// capacity to satisfy `data_len` given `remaining_bytes` of free space.
    fn compute_expanded_len(current_len: u64, mut remaining_bytes: u64, data_len: u64) -> u64 {
        let mut prev_len = current_len;
        let mut new_len = current_len;
        while remaining_bytes < data_len {
            remaining_bytes = remaining_bytes.saturating_add(prev_len);
            new_len = prev_len.checked_shl(1).unwrap_or(u64::MAX);
            prev_len = new_len;
        }
        new_len
    }

    fn compute_expansion_plan(&self, data_len: u64) -> Option<ExpansionPlan> {
        let rem_bytes = self.remaining_bytes();
        if rem_bytes >= data_len {
            return None;
        }

        let orig_file_len = self.file_len();
        let end_of_last_elem = self.wrap_pos(self.last.pos + self.elem_span(self.last.len));
        let wraps = end_of_last_elem <= self.first.pos;
        let moved_count = if wraps { end_of_last_elem - self.data_start() } else { 0 };

        Some(ExpansionPlan {
            orig_file_len,
            new_len: Self::compute_expanded_len(orig_file_len, rem_bytes, data_len),
            end_of_last_elem,
            wraps,
            moved_count,
        })
    }

    fn relocate_wrapped_data(&mut self, plan: ExpansionPlan) -> Result<()> {
        if !plan.wraps {
            return Ok(());
        }

        self.with_batched_expansion_copy_sync(|queue_file| {
            queue_file.inner.transfer(queue_file.data_start(), plan.orig_file_len, plan.moved_count)
        })?;

        if self.is_v2() {
            let moved_offset = plan.orig_file_len - self.data_start();
            self.rewrite_v2_backlinks_after_expansion(plan.end_of_last_elem, moved_offset)?;

            let new_last_pos = plan.orig_file_len + self.last.pos - self.data_start();
            self.last = Element { pos: new_last_pos, len: self.last.len, seq: self.last.seq };

            maybe_inject_failpoint("v2_before_relocation_commit")?;

            if self.overwrite_on_remove {
                self.write_header(self.file_len(), self.elem_cnt, self.first.pos, self.last.pos)?;
                maybe_inject_failpoint("v2_after_relocation_commit_before_erase")?;
            }
        }

        Ok(())
    }

    fn adjust_positions_after_expansion(&mut self, plan: ExpansionPlan) -> Result<()> {
        let legacy_wrapped = !self.is_v2() && self.last.pos < self.first.pos;
        if legacy_wrapped {
            let new_last_pos = plan.orig_file_len + self.last.pos - self.data_start();
            self.last = Element::new(new_last_pos, self.last.len, 0)?;
        }

        self.cached_offsets.clear();

        Ok(())
    }

    fn cleanup_after_expansion(&mut self, plan: ExpansionPlan) -> Result<()> {
        if self.overwrite_on_remove {
            self.ring_erase(self.data_start(), plan.moved_count as usize)?;
            if self.is_v2() && plan.wraps {
                maybe_inject_failpoint("v2_after_relocation_cleanup_before_add")?;
            }
        }

        Ok(())
    }

    fn expand_if_necessary(&mut self, data_len: u64) -> Result<()> {
        let Some(plan) = self.compute_expansion_plan(data_len) else {
            return Ok(());
        };

        let bytes_used_before = self.used_bytes();
        self.inner.sync_set_len(plan.new_len)?;
        self.relocate_wrapped_data(plan)?;
        self.adjust_positions_after_expansion(plan)?;
        self.cleanup_after_expansion(plan)?;

        let bytes_used_after = self.used_bytes();
        debug_assert_eq!(bytes_used_before, bytes_used_after);

        Ok(())
    }

    /// After expanding and copying `[data_start, wrap_point)` to `[orig_file_len, ...)`,
    /// update `prev_pos` in any element header that pointed into the moved region.
    fn rewrite_v2_backlinks_after_expansion(
        &mut self, wrap_point: u64, moved_offset: u64,
    ) -> Result<()> {
        // Walk all elements from first to last (before last.pos update) and fix backlinks.
        // Elements that were in [data_start, wrap_point) have been moved to [old_len, old_len+count).
        // Their prev_pos fields may also point into [data_start, wrap_point).

        let data_start = self.data_start();

        // Collect element positions (we need to walk from first to last).
        // First, determine the list. Since positions may have changed, use the old positions
        // (before last update) which are still in-place for the elements in the moved range
        // (they now exist in two places: old and new).
        // We iterate all elements by walking the ring from current first.pos.
        let mut positions: Vec<Element> = Vec::with_capacity(self.elem_cnt);
        let mut cur = self.first;
        for _ in 0..self.elem_cnt {
            positions.push(cur);
            let next_pos = wrap_pos_fn(
                cur.pos + V2_ELEM_OVERHEAD + cur.len as u64,
                self.file_len(), // Note: file already expanded.
                data_start,
            );
            if positions.len() < self.elem_cnt {
                let next_header = self.validate_v2_element_header(next_pos)?;
                cur = Element { pos: next_pos, len: next_header.payload_len, seq: next_header.seq };
            }
        }

        self.with_batched_backlink_rewrite_sync(|queue_file| {
            // Now walk all elements and fix backlinks.
            for elem in &positions {
                // Read element header.
                let elem_pos = elem.pos;
                let mut hdr = [0u8; V2_ELEM_HDR_LEN];
                queue_file.ring_read(elem_pos, &mut hdr)?;

                let prev_pos = queue_file.validate_v2_element_header(elem_pos)?.prev_pos;

                if prev_pos >= data_start && prev_pos < wrap_point {
                    let new_prev_pos = prev_pos + moved_offset;

                    let new_prev_bytes = (new_prev_pos as i64).to_be_bytes();
                    hdr[12..20].copy_from_slice(&new_prev_bytes);

                    let new_crc = compute_elem_header_crc(&hdr);
                    hdr[24..28].copy_from_slice(&new_crc.to_be_bytes());

                    queue_file.ring_write_raw_from_self(elem_pos, &hdr)?;
                }
            }

            Ok(())
        })
    }

    fn with_batched_backlink_rewrite_sync<T>(
        &mut self, f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        self.with_deferred_sync(DeferredSyncPhase::BacklinkRewrite, f)
    }

    fn with_batched_clear_erase_sync<T>(
        &mut self, f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        self.with_deferred_sync(DeferredSyncPhase::ClearErase, f)
    }

    fn with_batched_expansion_copy_sync<T>(
        &mut self, f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        self.with_deferred_sync(DeferredSyncPhase::ExpansionCopy, f)
    }

    fn with_batched_v2_append_sync<T>(
        &mut self, f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        self.with_deferred_sync(DeferredSyncPhase::AppendBatch, f)
    }

    fn with_deferred_sync<T>(
        &mut self, phase: DeferredSyncPhase, f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        let sync_writes = self.inner.sync_writes;
        let sync_context = self.inner.sync_context;
        self.inner.sync_writes = false;
        self.inner.sync_context = match phase {
            DeferredSyncPhase::ExpansionCopy => SyncContext::ExpansionCopy,
            DeferredSyncPhase::ClearErase => SyncContext::ClearErase,
            DeferredSyncPhase::BacklinkRewrite => SyncContext::BacklinkRewrite,
            DeferredSyncPhase::AppendBatch => SyncContext::AppendBatch,
        };

        let result = f(self);

        self.inner.sync_context = sync_context;
        self.inner.sync_writes = sync_writes;

        let value = result?;

        if sync_writes {
            self.inner.file_mut()?.sync_data()?;
            maybe_inject_failpoint(match phase {
                DeferredSyncPhase::ExpansionCopy => "v2_after_expansion_copy_flush",
                DeferredSyncPhase::ClearErase => "clear_after_erase_flush",
                DeferredSyncPhase::BacklinkRewrite => "v2_after_backlink_rewrite_flush",
                DeferredSyncPhase::AppendBatch => "v2_after_add_batch_flush",
            })?;
        }

        Ok(value)
    }

    /// Ring write using self's `data_start` and `file_len` (for use when we can't borrow inner separately).
    fn ring_write_raw_from_self(&mut self, pos: u64, data: &[u8]) -> Result<()> {
        let data_start = self.data_start();
        let file_len = self.file_len();
        Self::ring_write_raw(&mut self.inner, pos, data, data_start, file_len)
    }

    fn write_zero_chunks(&mut self, mut pos: u64, mut len: usize) -> Result<()> {
        while len > 0 {
            let chunk_len = min(len, Self::ZEROES.len());
            self.inner.seek(pos);
            self.inner.write(&Self::ZEROES[..chunk_len])?;
            pos += chunk_len as u64;
            len -= chunk_len;
        }

        Ok(())
    }

    // ── Migration ─────────────────────────────────────────────────────────────

    fn migrate_to_v2(path: &Path) -> Result<()> {
        use std::fs;

        let tmp = path.with_extension("v2tmp");

        let result = (|| -> Result<()> {
            // Open source (no migration, legacy or versioned).
            let src = Self::open_internal_full(path, true, false, Self::INITIAL_LENGTH, false)?;

            // Create fresh v2 at tmp.
            Self::init(&tmp, false, V2_INITIAL_LEN)?;
            let mut dst = Self::open_internal_full(
                &tmp,
                src.overwrite_on_remove,
                false,
                V2_INITIAL_LEN,
                false,
            )?;
            dst.inner.sync_writes = false;

            // Copy all elements.
            {
                let mut src_iter = src.iter();
                while let Some(elem) = src_iter.borrowed_next() {
                    // Need owned copy since we borrow src mutably through iter.
                    let owned: Vec<u8> = elem.to_vec();

                    // We need to add to dst but iter borrows src. This is OK because dst is separate.
                    dst.add(&owned)?;
                }
            }

            dst.sync_all()?;

            drop(src);
            drop(dst);

            rename(&tmp, path)?;
            Ok(())
        })();

        if result.is_err() {
            let _ = fs::remove_file(&tmp);
        }

        result
    }
}

// ── Free helper: wrap_pos ─────────────────────────────────────────────────────

#[inline]
const fn wrap_pos_fn(pos: u64, file_len: u64, data_start: u64) -> u64 {
    if pos < file_len { pos } else { data_start + pos - file_len }
}

// ── QueueFileInner I/O helpers ────────────────────────────────────────────────

impl QueueFileInner {
    const TRANSFER_BUFFER_SIZE: usize = 128 * 1024;

    #[inline]
    fn file(&self) -> io::Result<&File> {
        self.file
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "queue file handle unavailable"))
    }

    #[inline]
    fn file_mut(&mut self) -> io::Result<&mut File> {
        self.file
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "queue file handle unavailable"))
    }

    #[inline]
    fn seek(&mut self, pos: u64) -> u64 {
        self.expected_seek = pos;
        pos
    }

    fn real_seek(&mut self) -> io::Result<u64> {
        if Some(self.expected_seek) == self.last_seek {
            return Ok(self.expected_seek);
        }

        let expected_seek = self.expected_seek;
        let res = self.file_mut()?.seek(SeekFrom::Start(expected_seek));
        self.last_seek = res.as_ref().ok().copied();

        res
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }

        self.real_seek()?;
        self.file_mut()?.read_exact(buf)?;
        if let Some(seek) = &mut self.last_seek {
            *seek += buf.len() as u64;
        }
        Ok(())
    }

    fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.real_seek()?;

        self.file_mut()?.write_all(buf)?;

        if let Some(seek) = &mut self.last_seek {
            *seek += buf.len() as u64;
        }

        if self.sync_writes {
            match self.sync_context {
                SyncContext::Normal => {}
                SyncContext::ExpansionCopy => {
                    maybe_inject_failpoint("v2_expansion_copy_per_write_sync")?;
                }
                SyncContext::ClearErase => {
                    maybe_inject_failpoint("clear_erase_per_write_sync")?;
                }
                SyncContext::BacklinkRewrite => {
                    maybe_inject_failpoint("v2_backlink_rewrite_per_write_sync")?;
                }
                SyncContext::AppendBatch => {
                    maybe_inject_failpoint("v2_add_batch_per_write_sync")?;
                }
            }
            self.file_mut()?.sync_data()?;
        }

        Ok(())
    }

    fn read_exact_at(&self, mut offset: u64, mut buf: &mut [u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let read = self.read_at(offset, buf)?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }

            offset += read as u64;
            buf = &mut buf[read..];
        }

        Ok(())
    }

    #[cfg(unix)]
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.file()?.read_at(buf, offset)
    }

    #[cfg(windows)]
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.file()?.seek_read(buf, offset)
    }

    fn transfer_inner(
        &mut self, buf: &mut [u8], mut read_pos: u64, mut write_pos: u64, count: u64,
    ) -> Result<()> {
        debug_assert!(read_pos < self.file_len);
        debug_assert!(write_pos <= self.file_len);
        debug_assert!(count < self.file_len);
        debug_assert!(i64::try_from(count).is_ok());

        let mut bytes_left = count as i64;

        while bytes_left > 0 {
            self.seek(read_pos);
            let bytes_to_read = min(bytes_left as usize, Self::TRANSFER_BUFFER_SIZE);
            self.read(&mut buf[..bytes_to_read])?;

            self.seek(write_pos);
            self.write(&buf[..bytes_to_read])?;

            read_pos += bytes_to_read as u64;
            write_pos += bytes_to_read as u64;
            bytes_left -= bytes_to_read as i64;
        }

        if self.sync_writes {
            self.file_mut()?.sync_data()?;
        }

        Ok(())
    }

    fn transfer(&mut self, read_pos: u64, write_pos: u64, count: u64) -> Result<()> {
        let mut buf = std::mem::take(&mut self.transfer_buf);
        let res = self.transfer_inner(&mut buf, read_pos, write_pos, count);
        self.transfer_buf = buf;

        res
    }

    fn sync_set_len(&mut self, new_len: u64) -> io::Result<()> {
        self.file_mut()?.set_len(new_len)?;
        self.file_len = new_len;
        self.file_mut()?.sync_all()
    }
}

// ── Element ───────────────────────────────────────────────────────────────────

/// A lightweight descriptor for one element stored in the ring buffer.
#[derive(Copy, Clone, Debug)]
struct Element {
    /// Byte offset of this element from the start of the file.
    pos: u64,
    /// Payload length in bytes.
    len: usize,
    /// Sequence number (v2 only; 0 for v0/v1).
    seq: u64,
}

impl Element {
    const EMPTY: Self = Self { pos: 0, len: 0, seq: 0 };
    const HEADER_LENGTH: usize = 4;

    #[inline]
    fn new(pos: u64, len: usize, seq: u64) -> Result<Self> {
        ensure!(i64::try_from(pos).is_ok(), CorruptedFileSnafu {
            msg: "element position must be less or equal to i64::MAX"
        });
        ensure!(i32::try_from(len).is_ok(), ElementTooBigSnafu);

        Ok(Self { pos, len, seq })
    }
}

// ── Iter ──────────────────────────────────────────────────────────────────────

/// An iterator that yields the elements of a [`QueueFile`] from head to tail.
#[derive(Debug)]
pub struct Iter<'a> {
    queue_file: &'a QueueFile,
    buffer: Vec<u8>,
    next_elem_index: usize,
    next_elem_pos: u64,
}

impl Iterator for Iter<'_> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let buffer = self.borrowed_next()?;
        Some(buffer.to_vec())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let elems_left = self.queue_file.elem_cnt - self.next_elem_index;
        (elems_left, Some(elems_left))
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if self.queue_file.elem_cnt - self.next_elem_index < n {
            self.next_elem_index = self.queue_file.elem_cnt;
            return None;
        }

        let left = if let Some(i) = self.queue_file.cached_index_up_to(n) {
            let (index, elem) = self.queue_file.cached_offsets[i];
            if index > self.next_elem_index {
                self.next_elem_index = index;
                self.next_elem_pos = elem.pos;
            }

            n - self.next_elem_index
        } else {
            n
        };

        for _ in 0..left {
            self.skip_next()?;
        }

        self.next()
    }
}

impl Iter<'_> {
    /// Advance the iterator cursor by one element without reading the payload.
    /// Used by [`Iterator::nth`] to skip elements cheaply.
    fn skip_next(&mut self) -> Option<()> {
        if self.next_elem_index >= self.queue_file.elem_cnt {
            return None;
        }
        let current = self.queue_file.read_element(self.next_elem_pos).ok()?;
        self.next_elem_pos =
            self.queue_file.wrap_pos(current.pos + self.queue_file.elem_span(current.len));
        self.next_elem_index += 1;
        Some(())
    }

    /// Returns the next element as a slice into the iterator's internal buffer.
    pub fn borrowed_next(&mut self) -> Option<&[u8]> {
        if self.next_elem_index >= self.queue_file.elem_cnt {
            return None;
        }

        let current = self.queue_file.read_element(self.next_elem_pos).ok()?;

        let payload_start = self.queue_file.wrap_pos(current.pos + self.queue_file.elem_hdr_len());

        if current.len > self.buffer.len() {
            self.buffer.resize(current.len, 0);
        }
        self.queue_file.ring_read(payload_start, &mut self.buffer[..current.len]).ok()?;

        // For v2, validate footer.
        if self.queue_file.is_v2() {
            let footer_pos = self.queue_file.wrap_pos(payload_start + current.len as u64);
            self.queue_file
                .validate_v2_footer(footer_pos, current.seq, &self.buffer[..current.len])
                .ok()?;
        }

        self.next_elem_pos =
            self.queue_file.wrap_pos(current.pos + self.queue_file.elem_span(current.len));
        self.next_elem_index += 1;

        Some(&self.buffer[..current.len])
    }
}
