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
//! This crate is a feature-complete and binary-compatible port of the `QueueFile` class from
//! [Tape2 by Square, Inc.](https://github.com/square/tape).
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
//! Two header formats are supported for reading and writing:
//!
//! * **Versioned** (32 bytes, default): created by [`QueueFile::open`] and
//!   [`QueueFile::with_capacity`]. Supports files up to `i64::MAX` bytes and element counts up to
//!   `i32::MAX`.
//! * **Legacy** (16 bytes): created by [`QueueFile::open_legacy`]. Binary-compatible with the
//!   original Java `QueueFile`. Supports files up to `i32::MAX` bytes only.
//!
//! # Performance notes
//!
//! * Use [`QueueFile::add_n`] to batch multiple elements into a single write.
//! * Set [`QueueFile::set_sync_writes`]`(false)` if durability after every operation is not
//!   required (e.g. when building an in-process write-ahead log that controls flushing itself).
//! * Enable [`QueueFile::set_skip_write_header_on_add`]`(true)` together with
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
    clippy::must_use_candidate
)]

use std::cmp::min;
use std::collections::VecDeque;
use std::fs::{rename, File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::ManuallyDrop;
use std::path::Path;

use bytes::{Buf, BufMut, BytesMut};
use snafu::{ensure, Snafu};

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

/// A lightning-fast, transactional, file-based FIFO queue.
///
/// [`QueueFile`] stores a sequence of arbitrary byte blobs in a single backing file. Adding and
/// removing elements are both O(1) operations. By default every write is immediately flushed to
/// disk via `sync_data`, making the queue crash-safe: a process crash leaves the queue in its
/// previous consistent state (see [Atomicity](#atomicity) below).
///
/// # Ring-buffer layout
///
/// After the fixed-size header, the remaining file space is treated as a circular buffer.
/// - `first` points to the head element (the next one to be dequeued).
/// - `last` points to the tail element (the most-recently enqueued one).
/// - When `last.pos >= first.pos` the data is **contiguous** in the file.
/// - When `last.pos < first.pos` the data **wraps**: the tail portion of the live data is stored
///   near the beginning of the file (just after the header) and the head portion is stored near
///   the end. Both [`ring_read`](Self::ring_read) and [`ring_write_buf`](Self::ring_write_buf)
///   handle the wrap transparently.
/// - The file grows by doubling when there is no room for the next write. On growth, any wrapped
///   data is relocated to immediately follow the non-wrapped portion so that the data becomes
///   contiguous in the enlarged file.
///
/// # Atomicity
///
/// The header is the last thing written on every mutation. As long as the underlying filesystem
/// treats a header-sized write as atomic, a crash between writing data and writing the header
/// leaves the queue in its pre-mutation state—the partially-written data is simply invisible
/// because the header still points to the old head/tail. The header also stores the **logical
/// file length**: if the file was successfully extended but the process crashed before copying
/// wrapped data, the stored length lets the implementation detect and recover from the
/// inconsistency on the next open.
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
///     assert_eq!(data, bytes.as_ref());
/// }
///
/// qf.remove().expect("remove failed");
/// ```
///
/// # File format
///
/// ```text
///   16-32 bytes      Header
///   ...              Data
/// ```
///
/// This implementation supports two versions of the header format.
///
/// ```text
/// Versioned Header (32 bytes):
///   1 bit            Versioned indicator [0 = legacy, 1 = versioned]
///   31 bits          Version, always 1
///   8 bytes          File length  (logical; the file may be longer on disk after a failed grow)
///   4 bytes          Element count
///   8 bytes          Head element position  (byte offset from start of file; 0 = empty)
///   8 bytes          Tail element position  (byte offset from start of file; 0 = empty)
///
/// Legacy Header (16 bytes):
///   1 bit            Legacy indicator, always 0
///   31 bits          File length
///   4 bytes          Element count
///   4 bytes          Head element position
///   4 bytes          Tail element position
/// ```
///
/// Each element is stored as:
///
/// ```text
/// Element:
///   4 bytes          Data length  (big-endian u32)
///   ...              Data         (exactly `data length` bytes)
/// ```
#[derive(Debug)]
pub struct QueueFile {
    inner: QueueFileInner,
    /// True when using the versioned header format. Otherwise use the legacy format.
    versioned: bool,
    /// The header length in bytes: 16 or 32.
    header_len: u64,
    /// Number of elements.
    elem_cnt: usize,
    /// Pointer to first (or eldest) element.
    first: Element,
    /// Pointer to last (or newest) element.
    last: Element,
    /// Minimum number of bytes the file shrinks to.
    capacity: u64,
    /// When true, removing an element will also overwrite data with zero bytes.
    /// It's true by default.
    overwrite_on_remove: bool,
    /// When true, skips header update upon adding.
    /// It's false by default.
    skip_write_header_on_add: bool,
    /// Write buffering.
    write_buf: Vec<u8>,
    /// Offset cache idx->Element. Sorted in ascending order, always unique.
    /// Indices form perfect squares though may skew after removal.
    cached_offsets: VecDeque<(usize, Element)>,
    /// Offset caching policy.
    offset_cache_kind: Option<OffsetCacheKind>,
}

/// Policy controlling how element file-positions are cached to accelerate [`Iter::nth`].
///
/// Walking to the *n*-th element in a [`QueueFile`] normally requires reading *n* element headers
/// sequentially from disk, which is O(n). Enabling an offset cache causes [`QueueFile`] to
/// remember certain (index → file-position) pairs as elements are added and iterated. When
/// [`Iter::nth`] is called, it binary-searches the cache for the largest cached index that is ≤ n,
/// jumps directly to that position, and then reads only the remaining headers—reducing the total
/// number of disk reads.
///
/// The cache is stored as a sorted `VecDeque<(usize, Element)>` and is entirely cleared whenever
/// the ring buffer is expanded (because element positions are relocated during expansion). The
/// cached indices may also drift slightly relative to their original sequence positions after a
/// series of removals, but this does not affect correctness—only the potential improvement to
/// iteration performance.
///
/// Use [`QueueFile::set_cache_offset_policy`] to enable or change the policy at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetCacheKind {
    /// Cache one position every `offset` elements.
    ///
    /// Specifically, after each `add` or `add_n` the implementation checks whether the distance
    /// between the last cached index and the current tail index is ≥ `offset`. If so, the tail
    /// position is cached. This produces a uniformly-spaced sample of the element list that trades
    /// memory for a bounded seek distance on random access.
    Linear { offset: usize },

    /// Cache positions at indices that are perfect squares (1, 4, 9, 16, 25, …).
    ///
    /// This provides logarithmically-diminishing marginal benefit: accessing the *n*-th element
    /// costs O(√n) disk reads rather than O(n). The cache grows as O(√n) entries, making it
    /// suitable for queues where memory overhead must stay small even as the queue grows large.
    Quadratic,
}

/// Owns the backing file handle and manages all low-level I/O.
///
/// `QueueFileInner` is separated from [`QueueFile`] primarily to enable the `ManuallyDrop`
/// pattern needed for [`QueueFile::into_inner_file`]: the [`Drop`] impl on [`QueueFile`] manually
/// drops the file after flushing any deferred header, and `into_inner_file` extracts it without
/// running `drop` at all. All raw file I/O (seeks, reads, writes, transfers) goes through the
/// methods on this struct; [`QueueFile`] only calls them.
///
/// ## Deferred-seek optimisation
///
/// Instead of calling [`File::seek`] immediately, [`QueueFileInner::seek`] stores the target in
/// `expected_seek` and only issues the syscall (via [`real_seek`](Self::real_seek)) just before
/// the next actual read or write. Consecutive operations at the same position thus avoid a
/// redundant `lseek` call, which is tracked by comparing `expected_seek` with `last_seek`.
///
/// ## Read buffer / write-through cache
///
/// [`QueueFileInner::read`] maintains a read-ahead buffer (`read_buffer`) that caches a window of
/// the file starting at `read_buffer_offset`. Reads that fall entirely inside the window are
/// served from memory with no syscall. [`QueueFileInner::write`] keeps the buffer coherent: when
/// a write overlaps the cached window, the relevant portion of the buffer is updated in-place so
/// that subsequent reads reflect the write without going to disk.
#[derive(Debug)]
struct QueueFileInner {
    /// The backing file, wrapped in `ManuallyDrop` so `QueueFile::into_inner_file` can extract it.
    file: ManuallyDrop<File>,
    /// Cached file length. Kept in sync with `set_len` calls; always a power of two.
    file_len: u64,
    /// The byte offset that the *next* I/O operation should target. Updated by `seek()`; the
    /// actual `lseek` syscall is deferred until `real_seek()` is called from `read` or `write`.
    expected_seek: u64,
    /// The byte offset the OS file cursor was last confirmed to be at, or `None` if unknown.
    /// Used to skip `lseek` when consecutive operations land at the same position.
    last_seek: Option<u64>,
    /// Start offset of the data currently held in `read_buffer`, or `None` if the buffer is
    /// invalid (e.g. after a failed read or after `read_buffer` was resized).
    read_buffer_offset: Option<u64>,
    /// Read-ahead / read cache buffer. See struct-level docs for the coherence protocol.
    read_buffer: Vec<u8>,
    /// Scratch buffer used by `transfer`. Stored here (rather than on the stack) so that the
    /// large allocation is reused across calls. Wrapped in `Option` so `transfer` can take
    /// ownership during the operation without unsafe code.
    transfer_buf: Option<Box<[u8]>>,
    /// When `true`, every call to `write` ends with `file.sync_data()`. Disabled in tests and
    /// can be disabled at runtime via `QueueFile::set_sync_writes`.
    sync_writes: bool,
}

impl Drop for QueueFile {
    /// Flushes any deferred header and closes the file.
    ///
    /// If [`skip_write_header_on_add`](QueueFile::set_skip_write_header_on_add) is enabled, the
    /// header has not been written since the last `add`; this drop impl writes it now so that the
    /// queue file is consistent on disk. Errors from that final header write are silently
    /// discarded (there is no way to propagate them from `drop`).
    ///
    /// The [`File`] inside `inner` is stored in a [`ManuallyDrop`] wrapper and is explicitly
    /// dropped here. This arrangement allows [`QueueFile::into_inner_file`] to move the `File`
    /// out and call [`std::mem::forget`] on the `QueueFile`, preventing this destructor from
    /// running a second time.
    fn drop(&mut self) {
        if self.skip_write_header_on_add {
            let _ = self.sync_header();
        }

        unsafe {
            ManuallyDrop::drop(&mut self.inner.file);
        }
    }
}

impl QueueFile {
    const BLOCK_LENGTH: u64 = 4096;
    const INITIAL_LENGTH: u64 = 4096;
    const READ_BUFFER_SIZE: usize = 4096;
    const VERSIONED_HEADER: u32 = 0x8000_0001;
    const ZEROES: [u8; 4096] = [0; 4096];

    /// Creates a fresh, empty queue file at `path` with the given initial file size.
    ///
    /// To avoid leaving a partially-initialised file visible to other processes (or to a
    /// concurrent `open` call on this process), the file is first written to a sibling path with
    /// a `.tmp` extension and then atomically renamed into place.
    ///
    /// The written file consists only of a header: the `capacity` bytes worth of storage are
    /// allocated via `set_len` but the data region is left as zeroes. The header encodes
    /// `elem_cnt = 0`, `first_pos = 0`, `last_pos = 0`, and `file_len = capacity`.
    fn init(path: &Path, force_legacy: bool, capacity: u64) -> Result<()> {
        let tmp_path = path.with_extension(".tmp");

        // Use a temp file so we don't leave a partially-initialized file.
        {
            let mut file =
                OpenOptions::new().read(true).write(true).create(true).open(&tmp_path)?;

            file.set_len(capacity)?;

            let mut buf = BytesMut::with_capacity(16);

            if force_legacy {
                buf.put_u32(capacity as u32);
            } else {
                buf.put_u32(Self::VERSIONED_HEADER);
                buf.put_u64(capacity);
            }

            file.write_all(buf.as_ref())?;
        }

        // A rename is atomic.
        rename(tmp_path, path)?;

        Ok(())
    }

    /// Opens or creates a [`QueueFile`] at `path`, using the versioned (32-byte) header format.
    ///
    /// `capacity` is the minimum file size in bytes. If the file is newly created it will be
    /// exactly `capacity` bytes. If the file already exists and is smaller than `capacity`, it is
    /// extended to `capacity`. The file will never shrink below `capacity` (even after
    /// [`QueueFile::clear`]) so this parameter serves as a permanent floor.
    ///
    /// For most use-cases [`QueueFile::open`] (which uses a 4 KiB default capacity) is
    /// sufficient. Use `with_capacity` when you know in advance roughly how much data the queue
    /// will hold and want to avoid the initial doubling expansions.
    ///
    /// # Example
    ///
    /// ```
    /// # use queue_file::QueueFile;
    /// # let path = auto_delete_path::AutoDeletePath::temp();
    /// let qf = QueueFile::with_capacity(path, 120).expect("failed to open queue");
    /// ```
    pub fn with_capacity<P: AsRef<Path>>(path: P, capacity: u64) -> Result<Self> {
        Self::open_internal(path, true, false, capacity)
    }

    /// Opens or creates a [`QueueFile`] at `path` using the versioned header format and a
    /// 4 KiB initial file size.
    ///
    /// This is the most common way to open a queue. If the file does not exist it is created;
    /// if it already exists its current contents are preserved. The versioned format supports
    /// files up to `i64::MAX` bytes and is not readable by the original Java `QueueFile`
    /// implementation (use [`open_legacy`](Self::open_legacy) for cross-language compatibility).
    ///
    /// # Example
    ///
    /// ```
    /// # use queue_file::QueueFile;
    /// # let path = auto_delete_path::AutoDeletePath::temp();
    /// let qf = QueueFile::open(path).expect("failed to open queue");
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::with_capacity(path, Self::INITIAL_LENGTH)
    }

    /// Opens or creates a [`QueueFile`] at `path` using the legacy (16-byte) header format.
    ///
    /// The legacy format is binary-compatible with Square's original Java `QueueFile` class.
    /// It stores file length and element positions as 32-bit signed integers, limiting the
    /// maximum file size to `i32::MAX` bytes (~2 GiB). If you do not need interoperability with
    /// Java prefer [`QueueFile::open`] which has no such size restriction.
    ///
    /// An existing file that was already created with the versioned format is silently treated as
    /// a legacy file when opened this way (the version bit in the first byte is ignored), which
    /// may produce [`Error::CorruptedFile`] if the stored values exceed 32-bit range.
    ///
    /// # Example
    ///
    /// ```
    /// # use queue_file::QueueFile;
    /// # let path = auto_delete_path::AutoDeletePath::temp();
    /// let qf = QueueFile::open_legacy(path).expect("failed to open queue");
    /// ```
    pub fn open_legacy<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_internal(path, true, true, Self::INITIAL_LENGTH)
    }

    /// Shared implementation behind [`open`](Self::open), [`open_legacy`](Self::open_legacy), and
    /// [`with_capacity`](Self::with_capacity).
    ///
    /// If no file exists at `path`, a fresh one is initialised via [`init`](Self::init).
    /// The file is then opened read-write, the header is read and validated, and the head/tail
    /// `Element` descriptors are reconstructed by reading their 4-byte length prefixes from the
    /// ring buffer. If `file_len` in the header is smaller than `capacity`, the file is
    /// immediately extended (this handles the case of re-opening a queue with a larger capacity
    /// floor than was used originally).
    fn open_internal<P: AsRef<Path>>(
        path: P, overwrite_on_remove: bool, force_legacy: bool, capacity: u64,
    ) -> Result<Self> {
        if !path.as_ref().exists() {
            Self::init(path.as_ref(), force_legacy, capacity)?;
        }

        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        let mut buf = [0u8; 32];

        let bytes_read = file.read(&mut buf)?;

        ensure!(bytes_read >= 32, CorruptedFileSnafu { msg: "file too short" });

        let versioned = !force_legacy && (buf[0] & 0x80) != 0;

        let header_len: u64;
        let file_len: u64;
        let elem_cnt: usize;
        let first_pos: u64;
        let last_pos: u64;

        let mut buf = BytesMut::from(&buf[..]);

        if versioned {
            header_len = 32;

            let version = buf.get_u32() & 0x7FFF_FFFF;

            ensure!(version == 1, UnsupportedVersionSnafu { detected: version, supported: 1u32 });

            file_len = buf.get_u64();
            elem_cnt = buf.get_u32() as usize;
            first_pos = buf.get_u64();
            last_pos = buf.get_u64();

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
        } else {
            header_len = 16;

            file_len = u64::from(buf.get_u32());
            elem_cnt = buf.get_u32() as usize;
            first_pos = u64::from(buf.get_u32());
            last_pos = u64::from(buf.get_u32());

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
        }

        let real_file_len = file.metadata()?.len();

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

        let mut queue_file = Self {
            inner: QueueFileInner {
                file: ManuallyDrop::new(file),
                file_len,
                expected_seek: 0,
                last_seek: Some(32),
                read_buffer_offset: None,
                read_buffer: vec![0; Self::READ_BUFFER_SIZE],
                transfer_buf: Some(
                    vec![0u8; QueueFileInner::TRANSFER_BUFFER_SIZE].into_boxed_slice(),
                ),
                sync_writes: cfg!(not(test)),
            },
            versioned,
            header_len,
            elem_cnt,
            first: Element::EMPTY,
            last: Element::EMPTY,
            capacity,
            overwrite_on_remove,
            skip_write_header_on_add: false,
            write_buf: Vec::new(),
            cached_offsets: VecDeque::new(),
            offset_cache_kind: None,
        };

        if file_len < capacity {
            queue_file.inner.sync_set_len(queue_file.capacity)?;
        }

        queue_file.first = queue_file.read_element(first_pos)?;
        queue_file.last = queue_file.read_element(last_pos)?;

        Ok(queue_file)
    }

    /// Returns `true` if removing an element will overwrite its bytes with zeroes before
    /// advancing the head pointer.
    ///
    /// See [`set_overwrite_on_remove`](Self::set_overwrite_on_remove) for a full description.
    #[inline]
    pub const fn overwrite_on_remove(&self) -> bool {
        self.overwrite_on_remove
    }

    #[deprecated(since = "1.4.7", note = "Use `overwrite_on_remove` instead.")]
    pub const fn get_overwrite_on_remove(&self) -> bool {
        self.overwrite_on_remove()
    }

    /// Controls whether removed elements are zeroed out in the file before the head pointer
    /// advances.
    ///
    /// When `true` (the default), [`remove`](Self::remove), [`remove_n`](Self::remove_n), and
    /// [`clear`](Self::clear) overwrite the vacated bytes with `0x00` before updating the header.
    /// This prevents sensitive data from lingering in the file's unused region and makes forensic
    /// recovery of removed elements harder.
    ///
    /// Set to `false` for a modest performance improvement when the data is not sensitive and
    /// you don't need the zeroing guarantee.
    #[inline]
    pub fn set_overwrite_on_remove(&mut self, value: bool) {
        self.overwrite_on_remove = value;
    }

    /// Returns `true` if every write is followed by an `fsync` (via `sync_data`).
    ///
    /// See [`set_sync_writes`](Self::set_sync_writes) for a full description.
    #[inline]
    pub const fn sync_writes(&self) -> bool {
        self.inner.sync_writes
    }

    #[deprecated(since = "1.4.7", note = "Use `sync_writes` instead.")]
    pub const fn get_sync_writes(&self) -> bool {
        self.sync_writes()
    }

    /// Controls whether each write is immediately flushed to the storage device.
    ///
    /// When `true` (the default in non-test builds), every data write and header write is
    /// followed by a `sync_data()` call so that mutations are durable before the operation
    /// returns. This is the safe default for crash-resilient queues.
    ///
    /// Set to `false` to skip the `sync_data()` call for maximum throughput when durability
    /// per-operation is not required. You can still manually flush at chosen checkpoints by
    /// calling [`sync_all`](Self::sync_all).
    ///
    /// Sync writes are always disabled in test builds (via `cfg!(not(test))`).
    #[inline]
    pub fn set_sync_writes(&mut self, value: bool) {
        self.inner.sync_writes = value;
    }

    /// Returns `true` if the header is **not** updated after each individual `add`.
    ///
    /// See [`set_skip_write_header_on_add`](Self::set_skip_write_header_on_add) for a full
    /// description.
    #[inline]
    pub const fn skip_write_header_on_add(&self) -> bool {
        self.skip_write_header_on_add
    }

    #[deprecated(since = "1.4.7", note = "Use `skip_write_header_on_add` instead.")]
    pub const fn get_skip_write_header_on_add(&self) -> bool {
        self.skip_write_header_on_add()
    }

    /// Controls whether the file header is updated after every individual `add`.
    ///
    /// When `false` (the default), [`add`](Self::add) and [`add_n`](Self::add_n) write the header
    /// at the end of every call, which is the safe choice: if the process crashes after `add`
    /// returns, the element is already committed to disk.
    ///
    /// When `true`, the header write is deferred. This eliminates one `write` + optional
    /// `sync_data()` call per `add`, which can be a significant saving when adding many small
    /// elements one by one. The trade-off is that a crash between two `add` calls may leave some
    /// elements committed only up to the last explicit header flush. Call [`sync_all`](Self::sync_all)
    /// at your chosen durability boundary to commit all pending adds.
    ///
    /// The header is always written (even if this flag is set) by [`Drop`] and by
    /// [`sync_all`](Self::sync_all), so the file is consistent after the `QueueFile` is closed
    /// normally.
    #[inline]
    pub fn set_skip_write_header_on_add(&mut self, value: bool) {
        self.skip_write_header_on_add = value;
    }

    /// Sets the size of the internal read-ahead buffer used by element reads.
    ///
    /// The default size is 4 KiB (`READ_BUFFER_SIZE`). Reads that fit within the current
    /// buffer window are served from memory; reads that miss the window trigger a single
    /// `read` syscall that refills the buffer.
    ///
    /// Increasing the buffer size reduces syscall frequency when iterating over many small
    /// elements. Decreasing it reduces memory usage. If `size` is smaller than the current
    /// buffer length the buffer is shrunk but the cached data is retained; if `size` is larger
    /// the buffer is grown and the cache is invalidated so the next read refills it.
    pub fn set_read_buffer_size(&mut self, size: usize) {
        if self.inner.read_buffer.len() < size {
            self.inner.read_buffer_offset = None;
        }
        self.inner.read_buffer.resize(size, 0);
    }

    /// Returns the currently active offset-cache policy, or `None` if caching is disabled.
    ///
    /// See [`set_cache_offset_policy`](Self::set_cache_offset_policy) and [`OffsetCacheKind`] for
    /// details.
    #[inline]
    pub const fn cache_offset_policy(&self) -> Option<OffsetCacheKind> {
        self.offset_cache_kind
    }

    #[deprecated(since = "1.4.7", note = "Use `cache_offset_policy` instead.")]
    pub const fn get_cache_offset_policy(&self) -> Option<OffsetCacheKind> {
        self.cache_offset_policy()
    }

    /// Sets (or clears) the offset-cache policy used to accelerate [`Iter::nth`].
    ///
    /// Pass an [`OffsetCacheKind`] variant to enable caching, or `None` to disable it.
    /// When caching is disabled, any previously cached entries are discarded immediately.
    ///
    /// The cache is populated lazily as elements are added and iterated; enabling the policy
    /// on an existing queue does not retroactively cache historical positions. The cache is
    /// also cleared whenever the ring buffer is expanded, since element positions change during
    /// expansion.
    #[inline]
    pub fn set_cache_offset_policy(&mut self, kind: impl Into<Option<OffsetCacheKind>>) {
        self.offset_cache_kind = kind.into();

        if self.offset_cache_kind.is_none() {
            self.cached_offsets.clear();
        }
    }

    /// Returns `true` if the queue contains no elements.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.elem_cnt == 0
    }

    /// Returns the number of elements currently in the queue.
    #[inline]
    pub const fn size(&self) -> usize {
        self.elem_cnt
    }

    /// Flushes all pending writes and syncs the file to the storage device.
    ///
    /// If [`skip_write_header_on_add`](Self::set_skip_write_header_on_add) is enabled, this
    /// method first writes the current header to make all recently added elements durable.
    /// Then it calls [`File::sync_all`] which flushes both data and metadata (file size,
    /// modification time, etc.) to disk—stronger than `sync_data` but slower.
    ///
    /// Use this as a manual durability checkpoint when operating with `skip_write_header_on_add`
    /// or `sync_writes = false`.
    pub fn sync_all(&mut self) -> Result<()> {
        if self.skip_write_header_on_add {
            self.sync_header()?;
        }

        Ok(self.inner.file.sync_all()?)
    }

    /// Conditionally caches the tail element's position after a batch of additions.
    ///
    /// Called at the end of [`add_n`](Self::add_n) with `affected_items` set to the number of
    /// elements just added. Delegates to [`cache_elem_if_needed`](Self::cache_elem_if_needed)
    /// for the current `last` element using `elem_cnt - 1` as the index.
    fn cache_last_offset_if_needed(&mut self, affected_items: usize) {
        if self.elem_cnt == 0 {
            return;
        }

        self.cache_elem_if_needed(self.elem_cnt - 1, self.last, affected_items);
    }

    /// Caches the file position of the element at `index` if the active [`OffsetCacheKind`]
    /// policy determines that this index should be cached.
    ///
    /// `affected_items` is the number of elements that were added in the current batch; it is
    /// used by the [`Quadratic`](OffsetCacheKind::Quadratic) policy to decide whether any
    /// perfect-square index falls within the range of newly added elements.
    ///
    /// If the cache already contains an entry whose index is ≥ `index` the call is a no-op,
    /// preventing duplicate or out-of-order entries in the cache's sorted `VecDeque`.
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

    /// Returns the position of the largest cached index that is ≤ `i`, if any.
    ///
    /// Returns `Some(cache_slot)` where `cache_slot` is the index into `cached_offsets` of the
    /// best matching entry, or `None` if no cached index is ≤ `i`. The caller uses the returned
    /// slot to look up the corresponding `Element` and jump ahead in the ring buffer rather than
    /// walking from the beginning.
    #[inline]
    fn cached_index_up_to(&self, i: usize) -> Option<usize> {
        self.cached_offsets
            .binary_search_by(|(idx, _)| idx.cmp(&i))
            .map_or_else(|i| i.checked_sub(1), Some)
    }

    /// Adds multiple elements to the end of the queue in a single write.
    ///
    /// All elements in `elems` are serialised into [`write_buf`](QueueFile::write_buf) and
    /// written to the file with a single call to [`ring_write_buf`](Self::ring_write_buf),
    /// followed by a single header update. This is significantly more efficient than calling
    /// [`add`](Self::add) in a loop because it incurs only one (optional) `sync_data` flush
    /// regardless of how many elements are added.
    ///
    /// The iterator is consumed **twice** (once to compute the total byte count needed for the
    /// expansion check, once to serialise): `elems` must therefore implement `Clone`. A plain
    /// `&[&[u8]]` or `&[Vec<u8>]` slice reference satisfies this requirement.
    ///
    /// If `elems` is empty the method returns `Ok(())` immediately without touching the file.
    ///
    /// # Errors
    ///
    /// Returns [`Error::TooManyElements`] if adding would push the element count past
    /// `i32::MAX - 1`, or [`Error::ElementTooBig`] if any individual element exceeds `i32::MAX`
    /// bytes.
    pub fn add_n(
        &mut self, elems: impl IntoIterator<Item = impl AsRef<[u8]>> + Clone,
    ) -> Result<()> {
        let (count, total_len) = elems
            .clone()
            .into_iter()
            .fold((0, 0), |(c, l), elem| (c + 1, l + Element::HEADER_LENGTH + elem.as_ref().len()));

        if count == 0 {
            return Ok(());
        }

        ensure!(self.elem_cnt + count < i32::max_value() as usize, TooManyElementsSnafu {});

        self.expand_if_necessary(total_len as u64)?;

        let was_empty = self.is_empty();
        let mut pos = if was_empty {
            self.header_len
        } else {
            self.wrap_pos(self.last.pos + Element::HEADER_LENGTH as u64 + self.last.len as u64)
        };

        let mut first_added = None;
        let mut last_added = None;

        self.write_buf.clear();

        for elem in elems {
            let elem = elem.as_ref();
            let len = elem.len();

            if first_added.is_none() {
                first_added = Some(Element::new(pos, len)?);
            }
            last_added = Some(Element::new(pos, len)?);

            self.write_buf.extend(&(len as u32).to_be_bytes());
            self.write_buf.extend(elem);

            pos = self.wrap_pos(pos + Element::HEADER_LENGTH as u64 + len as u64);
        }

        let first_added = first_added.unwrap();
        self.ring_write_buf(first_added.pos)?;

        if was_empty {
            self.first = first_added;
        }
        self.last = last_added.unwrap();

        self.write_header(self.file_len(), self.elem_cnt + count, self.first.pos, self.last.pos)?;
        self.elem_cnt += count;

        self.cache_last_offset_if_needed(count);

        Ok(())
    }

    /// Appends a single element to the tail of the queue.
    ///
    /// This is a convenience wrapper around [`add_n`](Self::add_n) for a single element.
    /// For adding many elements at once prefer `add_n` to avoid redundant header writes.
    #[inline]
    pub fn add(&mut self, buf: &[u8]) -> Result<()> {
        self.add_n(std::iter::once(buf))
    }

    /// Returns the head element without removing it, or `Ok(None)` if the queue is empty.
    ///
    /// The returned bytes are heap-allocated as a `Box<[u8]>`. If you need to peek at many
    /// elements consider using [`iter`](Self::iter) and calling
    /// [`Iter::borrowed_next`](Iter::borrowed_next) to avoid an allocation per element.
    pub fn peek(&mut self) -> Result<Option<Box<[u8]>>> {
        if self.is_empty() {
            Ok(None)
        } else {
            let len = self.first.len;
            let mut data = vec![0; len].into_boxed_slice();

            self.ring_read(self.first.pos + Element::HEADER_LENGTH as u64, &mut data)?;

            Ok(Some(data))
        }
    }

    /// Removes the head element from the queue.
    ///
    /// Equivalent to `remove_n(1)`. If the queue is empty this is a no-op.
    #[inline]
    pub fn remove(&mut self) -> Result<()> {
        self.remove_n(1)
    }

    /// Removes the `n` eldest elements from the queue.
    ///
    /// If `n == 0` or the queue is empty this is a no-op. If `n >= self.size()` this
    /// delegates to [`clear`](Self::clear) which also shrinks the file back to `capacity`.
    ///
    /// The implementation walks forward through the ring buffer from the current head, advancing
    /// `n` element headers, to find the new head position. If an offset cache is active it
    /// uses [`cached_index_up_to`](Self::cached_index_up_to) to skip ahead from the largest
    /// cached index ≤ `n - 1`, reading only the remaining headers one by one. After committing
    /// the new header the cache entries whose indices are ≤ `n` are dropped and the remaining
    /// entries are decremented by `n` to keep them consistent with the new element numbering.
    ///
    /// If [`overwrite_on_remove`](Self::set_overwrite_on_remove) is `true`, the bytes occupied
    /// by the removed elements are zeroed after the header is committed.
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

        let erase_start_pos = self.first.pos;
        let mut erase_total_len = 0usize;

        // Read the position and length of the new first element.
        let mut new_first_pos = self.first.pos;
        let mut new_first_len = self.first.len;

        let cached_index = self.cached_index_up_to(n - 1);
        let to_remove = if let Some(i) = cached_index {
            let (index, elem) = self.cached_offsets[i];

            if let Some(index) = index.checked_sub(1) {
                erase_total_len += Element::HEADER_LENGTH * index;
                erase_total_len += (elem.pos
                    + if self.first.pos < elem.pos {
                        0
                    } else {
                        self.file_len() - self.first.pos - self.header_len
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

        // Commit the header.
        self.write_header(self.file_len(), self.elem_cnt - n, new_first_pos, self.last.pos)?;
        self.elem_cnt -= n;
        self.first = Element::new(new_first_pos, new_first_len)?;

        if let Some(cached_index) = cached_index {
            self.cached_offsets.drain(..=cached_index);
        }
        self.cached_offsets.iter_mut().for_each(|(i, _)| *i -= n);

        if self.overwrite_on_remove {
            self.ring_erase(erase_start_pos, erase_total_len)?;
        }

        Ok(())
    }

    /// Removes all elements and shrinks the file back to the `capacity` floor.
    ///
    /// Commits a header with `elem_cnt = 0`, `first_pos = 0`, `last_pos = 0`, and
    /// `file_len = capacity`. If [`overwrite_on_remove`](Self::set_overwrite_on_remove) is `true`,
    /// the entire data region (from the end of the header to `capacity`) is overwritten with
    /// zeroes before the header is written. If the current file is larger than `capacity` it is
    /// truncated afterwards.
    ///
    /// The offset cache is cleared unconditionally.
    pub fn clear(&mut self) -> Result<()> {
        // Commit the header.
        self.write_header(self.capacity, 0, 0, 0)?;

        if self.overwrite_on_remove {
            self.inner.seek(self.header_len);
            let first_block = self.capacity.min(Self::BLOCK_LENGTH) - self.header_len;
            self.inner.write(&Self::ZEROES[..first_block as usize])?;

            if let Some(left) = self.capacity.checked_sub(Self::BLOCK_LENGTH) {
                for _ in 0..left / Self::BLOCK_LENGTH {
                    self.inner.write(&Self::ZEROES)?;
                }

                let tail = left % Self::BLOCK_LENGTH;

                if tail != 0 {
                    self.inner.write(&Self::ZEROES[..tail as usize])?;
                }
            }
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

    /// Returns an iterator that yields the queue's elements from head to tail.
    ///
    /// The returned [`Iter`] holds a mutable borrow of the `QueueFile` for its lifetime, so the
    /// queue cannot be modified while the iterator is alive. Each call to [`Iterator::next`]
    /// allocates a new `Box<[u8]>`. To avoid per-element allocations use
    /// [`Iter::borrowed_next`] instead; the borrowed slice is valid only until the next call.
    ///
    /// As an implementation detail, `iter` temporarily moves the queue's internal `write_buf`
    /// into the `Iter` to reuse the allocation as a scratch buffer. The buffer is returned to
    /// the `QueueFile` when the `Iter` is dropped.
    ///
    /// # Example
    ///
    /// ```
    /// # use queue_file::QueueFile;
    /// # let path = auto_delete_path::AutoDeletePath::temp();
    /// let mut qf = QueueFile::open(path).expect("failed to open queue");
    /// let items = vec![vec![1, 2], vec![], vec![3]];
    /// qf.add_n(&items).expect("failed to add elements to queue");
    ///
    /// let stored = qf.iter().map(Vec::from).collect::<Vec<_>>();
    /// assert_eq!(items, stored);
    /// ```
    pub fn iter(&mut self) -> Iter<'_> {
        let pos = self.first.pos;

        Iter {
            // We are using write buffer for reducing number of allocations.
            // BorrowedIter doesn't modify any data and will return it back on drop.
            buffer: std::mem::take(&mut self.write_buf),
            queue_file: self,
            next_elem_index: 0,
            next_elem_pos: pos,
        }
    }

    /// Returns the total size of the backing file in bytes.
    ///
    /// This is always a power of two and is always ≥ [`used_bytes`](Self::used_bytes).
    /// The file grows by doubling when it runs out of space and shrinks back to `capacity` only
    /// when [`clear`](Self::clear) is called.
    #[inline]
    pub const fn file_len(&self) -> u64 {
        self.inner.file_len
    }

    /// Returns the number of bytes currently occupied by the queue's header and element data.
    ///
    /// For an empty queue this equals `header_len` (16 or 32 bytes). Otherwise it is the sum
    /// of the header, all element length prefixes (4 bytes each), and all element payloads.
    /// When the ring buffer wraps (i.e. `last.pos < first.pos`), the two physical segments are
    /// added together correctly.
    ///
    /// `file_len() - used_bytes()` gives the number of free bytes available before the next
    /// file expansion.
    #[inline]
    pub const fn used_bytes(&self) -> u64 {
        if self.elem_cnt == 0 {
            self.header_len
        } else if self.last.pos >= self.first.pos {
            // Contiguous queue.
            (self.last.pos - self.first.pos)
                + Element::HEADER_LENGTH as u64
                + self.last.len as u64
                + self.header_len
        } else {
            // tail < head. The queue wraps.
            self.last.pos + Element::HEADER_LENGTH as u64 + self.last.len as u64 + self.file_len()
                - self.first.pos
        }
    }

    /// Consumes the `QueueFile` and returns the underlying [`File`] handle.
    ///
    /// Before transferring ownership, any deferred header write is flushed (the same logic as in
    /// [`Drop`]). The [`File`] is then extracted from the [`ManuallyDrop`] wrapper and the
    /// `QueueFile` value is forgotten via [`std::mem::forget`] to prevent the `Drop`
    /// implementation from running a second time.
    ///
    /// The caller receives a raw `File` positioned at an unspecified offset.
    pub fn into_inner_file(mut self) -> File {
        if self.skip_write_header_on_add {
            let _ = self.sync_header();
        }

        let file = unsafe { ManuallyDrop::take(&mut self.inner.file) };
        std::mem::forget(self);

        file
    }

    /// Returns the number of free bytes in the ring buffer (i.e. `file_len - used_bytes`).
    ///
    /// When this drops below the size of the next element to be written,
    /// [`expand_if_necessary`](Self::expand_if_necessary) doubles the file.
    #[inline]
    const fn remaining_bytes(&self) -> u64 {
        self.file_len() - self.used_bytes()
    }

    /// Writes the current in-memory queue state back to the file header.
    ///
    /// Used by [`sync_all`](Self::sync_all) and [`Drop`] when
    /// `skip_write_header_on_add` is enabled.
    fn sync_header(&mut self) -> Result<()> {
        self.write_header(self.file_len(), self.size(), self.first.pos, self.last.pos)
    }

    /// Atomically commits a queue state change by writing a new header to offset 0.
    ///
    /// All arguments (`file_len`, `elem_cnt`, `first_pos`, `last_pos`) represent the **new**
    /// values that should become visible after this write. The caller is responsible for updating
    /// the corresponding struct fields *after* this method returns successfully—this separation
    /// ensures that the in-memory state is only advanced when the on-disk state is known to be
    /// consistent.
    ///
    /// The method validates that each value fits within the range permitted by the active header
    /// format (32-bit for legacy, 64/32-bit for versioned) and returns
    /// [`Error::CorruptedFile`] if any would overflow.
    fn write_header(
        &mut self, file_len: u64, elem_cnt: usize, first_pos: u64, last_pos: u64,
    ) -> Result<()> {
        let mut header = [0; 32];
        let mut header_buf = &mut header[..];

        // Never allow write values that will render file unreadable by Java library.
        if self.versioned {
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
        self.inner.write(&header.as_ref()[..self.header_len as usize])
    }

    /// Reads the 4-byte length prefix at ring-buffer position `pos` and constructs an [`Element`].
    ///
    /// If `pos == 0`, returns [`Element::EMPTY`] (used to represent the head/tail of an empty
    /// queue). Otherwise reads 4 bytes via [`ring_read`](Self::ring_read) and interprets them as
    /// a big-endian `u32` element length, then validates and returns the `Element`.
    fn read_element(&mut self, pos: u64) -> Result<Element> {
        if pos == 0 {
            Ok(Element::EMPTY)
        } else {
            let mut buf: [u8; 4] = [0; Element::HEADER_LENGTH];
            self.ring_read(pos, &mut buf)?;

            Element::new(pos, u32::from_be_bytes(buf) as usize)
        }
    }

    /// Wraps a byte offset around the end of the ring buffer.
    ///
    /// If `pos` is within the file, it is returned unchanged. If it equals or exceeds
    /// `file_len`, it is mapped back to the data region just after the header
    /// (`header_len + pos - file_len`). This handles the single-wrap case that arises when an
    /// element or the write cursor passes the end of the file.
    #[inline]
    const fn wrap_pos(&self, pos: u64) -> u64 {
        if pos < self.file_len() { pos } else { self.header_len + pos - self.file_len() }
    }

    /// Writes `self.write_buf` to the ring buffer starting at `pos`, wrapping if necessary.
    ///
    /// If the entire buffer fits before the end of the file it is written in one call.
    /// If it straddles the end-of-file boundary it is split into two writes: the first from
    /// `pos` to the end of the file, the second from `header_len` onwards.
    ///
    /// This method does not modify `write_buf`; callers must populate it before calling.
    fn ring_write_buf(&mut self, pos: u64) -> Result<()> {
        let pos = self.wrap_pos(pos);

        if pos + self.write_buf.len() as u64 <= self.file_len() {
            self.inner.seek(pos);
            self.inner.write(&self.write_buf)
        } else {
            let before_eof = (self.file_len() - pos) as usize;

            self.inner.seek(pos);
            self.inner.write(&self.write_buf[..before_eof])?;
            self.inner.seek(self.header_len);
            self.inner.write(&self.write_buf[before_eof..])
        }
    }

    /// Overwrites `n` bytes of the ring buffer starting at `pos` with zeroes.
    ///
    /// Used by [`remove_n`](Self::remove_n) and [`clear`](Self::clear) when
    /// [`overwrite_on_remove`](Self::set_overwrite_on_remove) is `true`. The zeroing is done in
    /// chunks of up to `ZEROES` size (4 KiB) and wraps around the ring buffer boundary just like
    /// any other ring write.
    fn ring_erase(&mut self, pos: u64, n: usize) -> Result<()> {
        let mut pos = pos;
        let mut len = n;

        self.write_buf.clear();
        self.write_buf.extend(Self::ZEROES);

        while len > 0 {
            let chunk_len = min(len, Self::ZEROES.len());
            self.write_buf.truncate(chunk_len);

            self.ring_write_buf(pos)?;

            len -= chunk_len;
            pos += chunk_len as u64;
        }

        Ok(())
    }

    /// Reads exactly `buf.len()` bytes from the ring buffer starting at `pos`, wrapping if needed.
    ///
    /// If the read range fits entirely before the end of the file it is satisfied by a single
    /// call to [`QueueFileInner::read`]. If it straddles the end-of-file boundary it is split
    /// into two reads: bytes from `pos` to end-of-file fill the first portion of `buf`, and
    /// bytes from `header_len` onwards fill the rest.
    fn ring_read(&mut self, pos: u64, buf: &mut [u8]) -> io::Result<()> {
        let pos = self.wrap_pos(pos);

        if pos + buf.len() as u64 <= self.file_len() {
            self.inner.seek(pos);
            self.inner.read(buf)
        } else {
            let before_eof = (self.file_len() - pos) as usize;

            self.inner.seek(pos);
            self.inner.read(&mut buf[..before_eof])?;
            self.inner.seek(self.header_len);
            self.inner.read(&mut buf[before_eof..])
        }
    }

    /// Doubles the backing file until it has at least `data_len` free bytes.
    ///
    /// The file size is doubled repeatedly until `remaining_bytes() >= data_len`. After extending
    /// the file with `set_len`, the method checks whether the ring buffer is currently wrapped
    /// (i.e. `end_of_last_elem <= first.pos`). If it is, the wrapped tail segment (from
    /// `header_len` to `end_of_last_elem`) is copied to immediately after the old end of file
    /// using [`transfer`](QueueFileInner::transfer), making the data contiguous again. The `last`
    /// element's position is updated to reflect its new location in the larger file.
    ///
    /// The offset cache is cleared after every expansion because element positions change.
    /// If `overwrite_on_remove` is `true`, the old copy of any relocated data is zeroed.
    fn expand_if_necessary(&mut self, data_len: u64) -> Result<()> {
        let mut rem_bytes = self.remaining_bytes();

        if rem_bytes >= data_len {
            return Ok(());
        }

        let orig_file_len = self.file_len();
        let mut prev_len = orig_file_len;
        let mut new_len = prev_len;

        while rem_bytes < data_len {
            rem_bytes += prev_len;
            new_len = prev_len << 1;
            prev_len = new_len;
        }

        let bytes_used_before = self.used_bytes();

        // Calculate the position of the tail end of the data in the ring buffer
        let end_of_last_elem =
            self.wrap_pos(self.last.pos + Element::HEADER_LENGTH as u64 + self.last.len as u64);
        self.inner.sync_set_len(new_len)?;

        let mut count = 0u64;

        // If the buffer is split, we need to make it contiguous
        if end_of_last_elem <= self.first.pos {
            count = end_of_last_elem - self.header_len;

            self.inner.transfer(self.header_len, orig_file_len, count)?;
        }

        // Commit the expansion.
        if self.last.pos < self.first.pos {
            let new_last_pos = orig_file_len + self.last.pos - self.header_len;
            self.last = Element::new(new_last_pos, self.last.len)?;
        }

        // TODO: cached offsets might be recalculated after transfer
        self.cached_offsets.clear();

        if self.overwrite_on_remove {
            self.ring_erase(self.header_len, count as usize)?;
        }

        let bytes_used_after = self.used_bytes();
        debug_assert_eq!(bytes_used_before, bytes_used_after);

        Ok(())
    }
}

// I/O Helpers
impl QueueFileInner {
    /// Size of the scratch buffer used by [`transfer_inner`](Self::transfer_inner): 128 KiB.
    ///
    /// Chosen as a balance between minimising the number of read/write pairs during large
    /// ring-buffer relocations and not allocating an excessively large stack or heap buffer.
    const TRANSFER_BUFFER_SIZE: usize = 128 * 1024;

    /// Records the intended target offset for the next I/O operation without issuing a syscall.
    ///
    /// The actual `lseek` is deferred to [`real_seek`](Self::real_seek), which is called lazily
    /// from [`read`](Self::read) and [`write`](Self::write). Returns `pos` for convenience.
    #[inline]
    fn seek(&mut self, pos: u64) -> u64 {
        self.expected_seek = pos;

        pos
    }

    /// Issues the deferred `lseek` if the file cursor is not already at `expected_seek`.
    ///
    /// Compares `expected_seek` against `last_seek`. If they match, skips the syscall and
    /// returns immediately. On success, `last_seek` is updated to the new position; on failure,
    /// `last_seek` is set to `None` so subsequent calls re-issue the seek rather than assuming
    /// the cursor is valid.
    fn real_seek(&mut self) -> io::Result<u64> {
        if Some(self.expected_seek) == self.last_seek {
            return Ok(self.expected_seek);
        }

        let res = self.file.seek(SeekFrom::Start(self.expected_seek));
        self.last_seek = res.as_ref().ok().copied();

        res
    }

    /// Reads exactly `buf.len()` bytes from `expected_seek` into `buf`.
    ///
    /// Uses a read-ahead cache (`read_buffer`) to serve reads without a syscall when the
    /// requested range falls within the cached window. If the cache misses (or if `buf` is
    /// larger than the read buffer), `real_seek` is called and the underlying file is read into
    /// `read_buffer`; `buf` is then filled from the freshly loaded cache.
    ///
    /// If `buf` is larger than `read_buffer`, the read buffer is grown to match before the
    /// read, with the cache invalidated.
    ///
    /// On any I/O error, both `read_buffer_offset` and `last_seek` are cleared so that the
    /// next operation does not rely on stale state.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }

        let size = buf.len();

        let not_enough_data = if let Some(left) = self.read_buffer.len().checked_sub(size) {
            self.read_buffer_offset
                .and_then(|o| self.expected_seek.checked_sub(o))
                .and_then(|skip| left.checked_sub(skip as usize))
                .is_none()
        } else {
            self.read_buffer.resize(size, 0);

            true
        };

        if not_enough_data {
            use std::io::{Error, ErrorKind};

            self.real_seek()?;

            let mut read = 0;
            let mut res = Ok(());

            while !buf.is_empty() {
                match self.file.read(&mut self.read_buffer[read..]) {
                    Ok(0) => break,
                    Ok(n) => read += n,
                    Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                    Err(e) => {
                        res = Err(e);
                        break;
                    }
                }
            }

            if res.is_ok() && read < size {
                res = Err(Error::new(ErrorKind::UnexpectedEof, "failed to fill whole buffer"));
            }

            if let Err(err) = res {
                self.read_buffer_offset = None;
                self.last_seek = None;

                return Err(err);
            }

            self.read_buffer_offset = Some(self.expected_seek);

            if let Some(seek) = &mut self.last_seek {
                *seek += read as u64;
            }
        }

        let start = (self.expected_seek - self.read_buffer_offset.unwrap()) as usize;

        buf.copy_from_slice(&self.read_buffer[start..start + size]);

        Ok(())
    }

    /// Writes `buf` to the file at `expected_seek`, then keeps the read cache coherent.
    ///
    /// After the write, the method inspects whether the written byte range overlaps the
    /// currently cached read-buffer window (`read_buffer_offset .. read_buffer_offset + len`).
    /// If it does, the overlapping portion of `read_buffer` is updated in-place so that
    /// subsequent reads in the same window reflect the write without going back to disk. Four
    /// overlap cases are handled:
    ///
    /// - Write range fully inside cache window → copy the whole `buf` into the middle of the cache.
    /// - Write start is before cache window, end is inside → copy the tail of `buf` to the cache start.
    /// - Write start is inside cache window, end is beyond → copy the head of `buf` to the end of the cache.
    /// - Write range fully contains the cache window → copy the corresponding slice of `buf` over the entire cache.
    ///
    /// If `sync_writes` is `true`, `sync_data()` is called after every write.
    fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.real_seek()?;

        self.file.write_all(buf)?;

        if let Some(seek) = &mut self.last_seek {
            *seek += buf.len() as u64;
        }

        if let Some(read_buffer_offset) = self.read_buffer_offset {
            let write_size_u64 = buf.len() as u64;
            let read_buffer_end_offset = read_buffer_offset + self.read_buffer.len() as u64;
            let read_buffered = read_buffer_offset..read_buffer_end_offset;

            let has_start = read_buffered.contains(&self.expected_seek);
            let buf_end = self.expected_seek + write_size_u64;
            let has_end = read_buffered.contains(&buf_end);

            match (has_start, has_end) {
                // rd_buf_offset .. exp_seek .. exp_seek+buf.len .. rd_buf_end
                // need to copy whole write buffer
                (true, true) => {
                    let start = (self.expected_seek - read_buffer_offset) as usize;

                    self.read_buffer[start..start + buf.len()].copy_from_slice(buf);
                }
                // exp_seek .. rd_buf_offset .. exp_seek+buf.len .. rd_buf_end
                // need to copy only a tail of write buffer
                (false, true) => {
                    let need_to_skip = (read_buffer_offset - self.expected_seek) as usize;
                    let need_to_copy = buf.len() - need_to_skip;

                    self.read_buffer[..need_to_copy].copy_from_slice(&buf[need_to_skip..]);
                }
                // rd_buf_offset .. exp_seek .. rd_buf_end .. exp_seek+buf.len
                // need to copy only a head of write buffer
                (true, false) => {
                    let need_to_skip = (self.expected_seek - read_buffer_offset) as usize;
                    let need_to_copy = self.read_buffer.len() - need_to_skip;

                    self.read_buffer[need_to_skip..need_to_skip + need_to_copy]
                        .copy_from_slice(&buf[..need_to_copy]);
                }
                // exp_seek .. rd_buf_offset .. rd_buf_end .. exp_seek+buf.len
                // read buffer is inside writing range, need to rewrite it completely
                (false, false)
                    if (self.expected_seek + 1..buf_end).contains(&read_buffer_offset) =>
                {
                    let need_to_skip = (read_buffer_offset - self.expected_seek) as usize;
                    let need_to_copy = self.read_buffer.len();

                    self.read_buffer[..]
                        .copy_from_slice(&buf[need_to_skip..need_to_skip + need_to_copy]);
                }
                // nothing to do, read & write buffers do not overlap
                (false, false) => {}
            }
        }

        if self.sync_writes {
            self.file.sync_data()?;
        }

        Ok(())
    }

    /// Core byte-copy loop used by [`transfer`](Self::transfer).
    ///
    /// Copies `count` bytes from `read_pos` to `write_pos` using `buf` as the scratch buffer,
    /// in chunks of at most `buf.len()` bytes. After all chunks are written, a single
    /// `sync_data()` call is issued if `sync_writes` is `true` (rather than syncing after each
    /// individual chunk, which would be far more expensive for large transfers).
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

        // Should we `sync_data()` in internal loop instead?
        if self.sync_writes {
            self.file.sync_data()?;
        }

        Ok(())
    }

    /// Copies `count` bytes from `read_pos` to `write_pos` within the file.
    ///
    /// Temporarily takes ownership of `transfer_buf` (stored as `Option<Box<[u8]>>`) to pass a
    /// mutable scratch buffer to [`transfer_inner`](Self::transfer_inner) without unsafe code.
    /// The buffer is unconditionally returned after the inner call, whether it succeeded or not.
    fn transfer(&mut self, read_pos: u64, write_pos: u64, count: u64) -> Result<()> {
        let mut buf = self.transfer_buf.take().unwrap();
        let res = self.transfer_inner(&mut buf, read_pos, write_pos, count);
        self.transfer_buf = Some(buf);

        res
    }

    /// Extends or truncates the file to `new_len` bytes and updates the cached `file_len`.
    ///
    /// Calls `set_len` followed by `sync_all` to ensure the new file size is durably committed
    /// before any data is written into the expanded region. This is important for crash safety:
    /// the header records the logical `file_len`, and if the process dies before the length is
    /// stable on disk, recovery could see a smaller file than expected.
    fn sync_set_len(&mut self, new_len: u64) -> io::Result<()> {
        self.file.set_len(new_len)?;
        self.file_len = new_len;
        self.file.sync_all()
    }
}

/// A lightweight descriptor for one element stored in the ring buffer.
///
/// `pos` is the byte offset of the 4-byte length prefix (the element header) from the beginning
/// of the file. `len` is the payload length in bytes, not including the 4-byte prefix. An element
/// therefore occupies `HEADER_LENGTH + len` bytes in the file starting at `pos`.
///
/// The special value [`Element::EMPTY`] (`pos = 0, len = 0`) is used as the sentinel for the
/// head and tail pointers of an empty queue, because file offset 0 is always the queue header
/// and can never be a valid element position.
#[derive(Copy, Clone, Debug)]
struct Element {
    /// Byte offset of this element's 4-byte length prefix from the start of the file.
    pos: u64,
    /// Payload length in bytes (does not include the 4-byte `HEADER_LENGTH` prefix).
    len: usize,
}

impl Element {
    /// Sentinel value representing "no element" used when the queue is empty.
    ///
    /// Both `pos` and `len` are 0. File offset 0 is always the queue header, so this value can
    /// never collide with a real element.
    const EMPTY: Self = Self { pos: 0, len: 0 };

    /// Size in bytes of the per-element length prefix stored before each payload.
    ///
    /// The 4-byte big-endian `u32` encodes the payload length. This is binary-compatible with
    /// the original Java `QueueFile` format.
    const HEADER_LENGTH: usize = 4;

    /// Constructs an `Element`, validating that `pos` fits in `i64` and `len` fits in `i32`.
    ///
    /// These bounds are checked to ensure that values written to the file header will be
    /// readable by the Java `QueueFile` implementation (which uses signed 64- or 32-bit
    /// integers).
    #[inline]
    fn new(pos: u64, len: usize) -> Result<Self> {
        ensure!(i64::try_from(pos).is_ok(), CorruptedFileSnafu {
            msg: "element position must be less or equal to i64::MAX"
        });
        ensure!(i32::try_from(len).is_ok(), ElementTooBigSnafu);

        Ok(Self { pos, len })
    }
}

/// An iterator that yields the elements of a [`QueueFile`] from head to tail.
///
/// Obtained by calling [`QueueFile::iter`]. Holds a mutable borrow of the `QueueFile` for
/// its lifetime, preventing concurrent mutations. Internally it reuses the queue's `write_buf`
/// allocation (returned via [`Drop`]) and maintains a lightweight cursor (`next_elem_index`,
/// `next_elem_pos`) that advances through the ring buffer without re-reading the header.
#[derive(Debug)]
pub struct Iter<'a> {
    /// Mutable borrow of the queue; held for the iterator's lifetime.
    queue_file: &'a mut QueueFile,
    /// Scratch buffer for element payloads. Borrowed from `QueueFile::write_buf` for the
    /// lifetime of the iterator and returned on drop. See [`QueueFile::iter`].
    buffer: Vec<u8>,
    /// Zero-based index of the next element to yield (0 = head).
    next_elem_index: usize,
    /// Ring-buffer byte offset of the *length prefix* of the next element to read.
    next_elem_pos: u64,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Box<[u8]>;

    /// Returns the next element as a freshly allocated `Box<[u8]>`, or `None` at the end.
    ///
    /// Delegates to [`borrowed_next`](Iter::borrowed_next) and copies the result into a
    /// heap allocation. Prefer `borrowed_next` when the caller can process each element
    /// before requesting the next one.
    fn next(&mut self) -> Option<Self::Item> {
        let buffer = self.borrowed_next()?;

        Some(buffer.to_vec().into_boxed_slice())
    }

    /// Returns `(remaining, Some(remaining))` where `remaining` is the number of elements
    /// between the current position and the tail of the queue.
    fn size_hint(&self) -> (usize, Option<usize>) {
        let elems_left = self.queue_file.elem_cnt - self.next_elem_index;

        (elems_left, Some(elems_left))
    }

    /// Advances the iterator by `n` positions and returns the element at that index.
    ///
    /// If an offset cache is active, [`cached_index_up_to`](QueueFile::cached_index_up_to)
    /// is consulted first. If the cache provides a jump point closer to `n` than the current
    /// cursor, the cursor is moved there and only the remaining distance is walked by calling
    /// [`borrowed_next`](Iter::borrowed_next) in a loop, avoiding re-reading headers from the
    /// beginning.
    ///
    /// Returns `None` if `n` is out of bounds.
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
            self.borrowed_next();
        }

        self.next()
    }
}

impl Iter<'_> {
    /// Returns the next element as a slice into the iterator's internal buffer, without
    /// allocating.
    ///
    /// The returned slice is valid only until the next call to any `Iter` method, because the
    /// buffer may be overwritten. Use [`Iterator::next`] if you need an owned copy that outlives
    /// the iterator call.
    ///
    /// Also drives the offset cache: after reading each element, it calls
    /// [`cache_elem_if_needed`](QueueFile::cache_elem_if_needed) so that the active cache policy
    /// can record the current element's position for future [`nth`](Iter::nth) calls.
    pub fn borrowed_next(&mut self) -> Option<&[u8]> {
        if self.next_elem_index >= self.queue_file.elem_cnt {
            return None;
        }

        let current = self.queue_file.read_element(self.next_elem_pos).ok()?;
        self.next_elem_pos = self.queue_file.wrap_pos(current.pos + Element::HEADER_LENGTH as u64);

        if current.len > self.buffer.len() {
            self.buffer.resize(current.len, 0);
        }
        self.queue_file.ring_read(self.next_elem_pos, &mut self.buffer[..current.len]).ok()?;

        self.next_elem_pos = self
            .queue_file
            .wrap_pos(current.pos + Element::HEADER_LENGTH as u64 + current.len as u64);

        self.queue_file.cache_elem_if_needed(self.next_elem_index, current, 1);
        self.next_elem_index += 1;

        Some(&self.buffer[..current.len])
    }
}

impl Drop for Iter<'_> {
    /// Returns the scratch buffer back to the `QueueFile` when the iterator is dropped.
    ///
    /// The iterator borrows `QueueFile::write_buf` for the duration of its lifetime to avoid
    /// a redundant heap allocation. This `Drop` implementation puts it back so that subsequent
    /// `add_n` or `iter` calls can reuse the same allocation.
    fn drop(&mut self) {
        self.queue_file.write_buf = std::mem::take(&mut self.buffer);
    }
}
