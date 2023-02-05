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

//! `queue-file` crate is a feature complete and binary compatible port of `QueueFile` class from
//! Tape2 by Square, Inc. Check the original project [here](https://github.com/square/tape).

#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::must_use_candidate)]

use std::cmp::min;
use std::fs::{rename, File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::ManuallyDrop;
use std::path::Path;

use bytes::{Buf, BufMut, BytesMut};
use snafu::{ensure, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(context(false))]
    Io { source: std::io::Error },
    #[snafu(display("too many elements"))]
    TooManyElements {},
    #[snafu(display("element too big"))]
    ElementTooBig {},
    #[snafu(display("corrupted file: {}", msg))]
    CorruptedFile { msg: String },
    #[snafu(display(
        "unsupported version {}. supported versions is {} and legacy",
        detected,
        supported
    ))]
    UnsupportedVersion { detected: u32, supported: u32 },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// `QueueFile` is a lightning-fast, transactional, file-based FIFO.
///
/// Addition and removal from an instance is an O(1) operation and is atomic.
/// Writes are synchronous by default; data will be written to disk before an operation returns.
///
/// The underlying file. Uses a ring buffer to store entries. Designed so that a modification
/// isn't committed or visible until we write the header. The header is much smaller than a
/// segment. So long as the underlying file system supports atomic segment writes, changes to the
/// queue are atomic. Storing the file length ensures we can recover from a failed expansion
/// (i.e. if setting the file length succeeds but the process dies before the data can be copied).
///
/// # Example
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
/// # File format
///
/// ```text
///   16-32 bytes      Header
///   ...              Data
/// ```
/// This implementation supports two versions of the header format.
/// ```text
/// Versioned Header (32 bytes):
///   1 bit            Versioned indicator [0 = legacy, 1 = versioned]
///   31 bits          Version, always 1
///   8 bytes          File length
///   4 bytes          Element count
///   8 bytes          Head element position
///   8 bytes          Tail element position
///
/// Legacy Header (16 bytes):
///   1 bit            Legacy indicator, always 0
///   31 bits          File length
///   4 bytes          Element count
///   4 bytes          Head element position
///   4 bytes          Tail element position
/// ```
/// Each element stored is represented by:
/// ```text
/// Element:
///   4 bytes          Data length
///   ...              Data
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
    cached_offsets: Vec<(usize, Element)>,
    /// Offset caching policy.
    offset_cache_kind: Option<OffsetCacheKind>,
}

/// Policy for offset caching if enabled.
/// Notice that offsets frequency might be skewed due after series of adding/removal.
/// This shall not affect functional properties, only performance one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetCacheKind {
    /// Linear offseting.
    ///
    /// Next offset would be cached after `offset` additions.
    Linear { offset: usize },
    /// Quadratic offseting.
    ///
    /// Cached offsets form a sequence of perfect squares (e.g. cached 1st, 4th, 9th, .. offsets).
    Quadratic,
}

#[derive(Debug)]
struct QueueFileInner {
    file: ManuallyDrop<File>,
    /// Cached file length. Always a power of 2.
    file_len: u64,
    /// Intention seek offset.
    expected_seek: u64,
    /// Real last seek offset.
    last_seek: Option<u64>,
    /// Offset for the next read from buffer.
    read_buffer_offset: Option<u64>,
    /// Buffer for reads.
    read_buffer: Vec<u8>,
    /// Buffer used by `transfer` function.
    transfer_buf: Option<Box<[u8]>>,
    /// When true, every write to file will be followed by `sync_data()` call.
    /// It's true by default.
    sync_writes: bool,
}

impl Drop for QueueFile {
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

    /// Open or create [`QueueFile`] at `path` with specified minimal file size.
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

    /// Open or create [`QueueFile`] at `path`.
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

    /// Open or create [`QueueFile`] at `path` forcing legacy format.
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
            cached_offsets: vec![],
            offset_cache_kind: None,
        };

        if file_len < capacity {
            queue_file.inner.sync_set_len(queue_file.capacity)?;
        }

        queue_file.first = queue_file.read_element(first_pos)?;
        queue_file.last = queue_file.read_element(last_pos)?;

        Ok(queue_file)
    }

    /// Returns true if removing an element will also overwrite data with zero bytes.
    #[inline]
    pub const fn overwrite_on_remove(&self) -> bool {
        self.overwrite_on_remove
    }

    /// Use [`QueueFile::overwrite_on_remove`] instead.
    #[deprecated]
    pub const fn get_overwrite_on_remove(&self) -> bool {
        self.overwrite_on_remove()
    }

    /// If set to true removing an element will also overwrite data with zero bytes.
    #[inline]
    pub fn set_overwrite_on_remove(&mut self, value: bool) {
        self.overwrite_on_remove = value;
    }

    /// Returns true if every write to file will be followed by `sync_data()` call.
    #[inline]
    pub const fn sync_writes(&self) -> bool {
        self.inner.sync_writes
    }

    /// Use [`QueueFile::sync_writes`] instead.
    #[deprecated]
    pub const fn get_sync_writes(&self) -> bool {
        self.sync_writes()
    }

    /// If set to true every write to file will be followed by `sync_data()` call.
    #[inline]
    pub fn set_sync_writes(&mut self, value: bool) {
        self.inner.sync_writes = value;
    }

    /// Returns true if skips header update upon adding enabled.
    #[inline]
    pub const fn skip_write_header_on_add(&self) -> bool {
        self.skip_write_header_on_add
    }

    /// Use [`QueueFile::skip_write_header_on_add`] instead.
    #[deprecated]
    pub const fn get_skip_write_header_on_add(&self) -> bool {
        self.skip_write_header_on_add()
    }

    /// If set to true skips header update upon adding.
    #[inline]
    pub fn set_skip_write_header_on_add(&mut self, value: bool) {
        self.skip_write_header_on_add = value;
    }

    /// Changes buffer size used for data reading.
    pub fn set_read_buffer_size(&mut self, size: usize) {
        if self.inner.read_buffer.len() < size {
            self.inner.read_buffer_offset = None;
        }
        self.inner.read_buffer.resize(size, 0);
    }

    #[inline]
    pub const fn cache_offset_policy(&self) -> Option<OffsetCacheKind> {
        self.offset_cache_kind
    }

    /// Use [`QueueFile::cache_offset_policy`] instead.
    #[deprecated]
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

    /// Returns true if this queue contains no entries.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.elem_cnt == 0
    }

    /// Returns the number of elements in this queue.
    #[inline]
    pub const fn size(&self) -> usize {
        self.elem_cnt
    }

    /// Synchronizes the underlying file, look at [`File::sync_all`] doc for more info.
    pub fn sync_all(&mut self) -> Result<()> {
        if self.skip_write_header_on_add {
            self.sync_header()?;
        }

        Ok(self.inner.file.sync_all()?)
    }

    fn cache_last_offset(&mut self) {
        debug_assert!(self.elem_cnt != 0);

        let i = self.elem_cnt - 1;

        if let Some((index, elem)) = self.cached_offsets.last() {
            if *index == i {
                debug_assert_eq!(elem.pos, self.last.pos);
                debug_assert_eq!(elem.len, self.last.len);

                return;
            }
        }

        self.cached_offsets.push((i, self.last));
    }

    #[inline]
    fn cached_index_up_to(&self, i: usize) -> Option<usize> {
        self.cached_offsets
            .binary_search_by(|(idx, _)| idx.cmp(&i))
            .map_or_else(|i| i.checked_sub(1), Some)
    }

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

            ensure!(i32::try_from(len).is_ok(), ElementTooBigSnafu {});

            if first_added.is_none() {
                first_added = Some(Element::new(pos, len));
            }
            last_added = Some(Element::new(pos, len));

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

        if let Some(kind) = self.offset_cache_kind {
            let last_index = self.elem_cnt - 1;
            let need_to_cache = match kind {
                OffsetCacheKind::Linear { offset } => {
                    let last_cached_index = self.cached_offsets.last().map_or(0, |(idx, _)| *idx);
                    last_index - last_cached_index >= offset
                }
                OffsetCacheKind::Quadratic => {
                    let x = (last_index as f64).sqrt() as usize;
                    x > 1 && (self.elem_cnt - count..=last_index).contains(&(x * x))
                }
            };

            if need_to_cache {
                self.cache_last_offset();
            }
        }

        Ok(())
    }

    /// Adds an element to the end of the queue.
    #[inline]
    pub fn add(&mut self, buf: &[u8]) -> Result<()> {
        self.add_n(std::iter::once(buf))
    }

    /// Reads the eldest element. Returns `OK(None)` if the queue is empty.
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

    /// Removes the eldest element.
    #[inline]
    pub fn remove(&mut self) -> Result<()> {
        self.remove_n(1)
    }

    /// Removes the eldest `n` elements.
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
        self.first = Element::new(new_first_pos, new_first_len);

        if let Some(cached_index) = cached_index {
            self.cached_offsets.drain(..=cached_index);
        }
        self.cached_offsets.iter_mut().for_each(|(i, _)| *i -= n);

        if self.overwrite_on_remove {
            self.ring_erase(erase_start_pos, erase_total_len)?;
        }

        Ok(())
    }

    /// Clears this queue. Truncates the file to the initial size.
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

    /// Returns an iterator over elements in this queue.
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

    /// Returns the amount of bytes used by the backed file.
    /// Always >= [`Self::used_bytes`].
    #[inline]
    pub const fn file_len(&self) -> u64 {
        self.inner.file_len
    }

    /// Returns the amount of bytes used by the queue.
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

    /// Returns underlying file of the queue.
    pub fn into_inner_file(mut self) -> File {
        if self.skip_write_header_on_add {
            let _ = self.sync_header();
        }

        let file = unsafe { ManuallyDrop::take(&mut self.inner.file) };
        std::mem::forget(self);

        file
    }

    #[inline]
    const fn remaining_bytes(&self) -> u64 {
        self.file_len() - self.used_bytes()
    }

    fn sync_header(&mut self) -> Result<()> {
        self.write_header(self.file_len(), self.size(), self.first.pos, self.last.pos)
    }

    /// Writes header atomically. The arguments contain the updated values. The struct member fields
    /// should not have changed yet. This only updates the state in the file. It's up to the caller
    /// to update the class member variables *after* this call succeeds. Assumes segment writes are
    /// atomic in the underlying file system.
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

    fn read_element(&mut self, pos: u64) -> io::Result<Element> {
        if pos == 0 {
            Ok(Element::EMPTY)
        } else {
            let mut buf: [u8; 4] = [0; Element::HEADER_LENGTH];
            self.ring_read(pos, &mut buf)?;

            Ok(Element::new(pos, u32::from_be_bytes(buf) as usize))
        }
    }

    /// Wraps the position if it exceeds the end of the file.
    #[inline]
    const fn wrap_pos(&self, pos: u64) -> u64 {
        if pos < self.file_len() { pos } else { self.header_len + pos - self.file_len() }
    }

    /// Writes `n` bytes from buffer to position in file. Automatically wraps write if position is
    /// past the end of the file or if buffer overlaps it.
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

    /// Reads `n` bytes into buffer from file. Wraps if necessary.
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

    /// If necessary, expands the file to accommodate an additional element of the given length.
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
            self.last = Element::new(new_last_pos, self.last.len);
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
    const TRANSFER_BUFFER_SIZE: usize = 128 * 1024;

    #[inline]
    fn seek(&mut self, pos: u64) -> u64 {
        self.expected_seek = pos;

        pos
    }

    fn real_seek(&mut self) -> io::Result<u64> {
        if Some(self.expected_seek) == self.last_seek {
            return Ok(self.expected_seek);
        }

        let res = self.file.seek(SeekFrom::Start(self.expected_seek));
        self.last_seek = res.as_ref().ok().copied();

        res
    }

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

    /// Transfer `count` bytes starting from `read_pos` to `write_pos`.
    fn transfer(&mut self, read_pos: u64, write_pos: u64, count: u64) -> Result<()> {
        let mut buf = self.transfer_buf.take().unwrap();
        let res = self.transfer_inner(&mut buf, read_pos, write_pos, count);
        self.transfer_buf = Some(buf);

        res
    }

    fn sync_set_len(&mut self, new_len: u64) -> io::Result<()> {
        self.file.set_len(new_len)?;
        self.file_len = new_len;
        self.file.sync_all()
    }
}

#[derive(Copy, Clone, Debug)]
struct Element {
    pos: u64,
    len: usize,
}

impl Element {
    const EMPTY: Self = Self { pos: 0, len: 0 };
    const HEADER_LENGTH: usize = 4;

    fn new(pos: u64, len: usize) -> Self {
        assert!(
            i64::try_from(pos).is_ok(),
            "element position must be less than {}",
            i64::max_value()
        );
        assert!(
            i32::try_from(len).is_ok(),
            "element length must be less than {}",
            i32::max_value()
        );

        Self { pos, len }
    }
}

/// Iterator over items in the queue.
pub struct Iter<'a> {
    queue_file: &'a mut QueueFile,
    buffer: Vec<u8>,
    next_elem_index: usize,
    next_elem_pos: u64,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Box<[u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        let buffer = self.borrowed_next()?;

        Some(buffer.to_vec().into_boxed_slice())
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
            self.next();
        }

        self.next()
    }
}

impl Iter<'_> {
    /// Returns the next element in the queue.
    /// Similar to `Iter::next` but returned value bounded to internal buffer,
    /// i.e not allocated at each call.
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
        self.next_elem_index += 1;

        Some(&self.buffer[..current.len])
    }
}

impl Drop for Iter<'_> {
    fn drop(&mut self) {
        self.queue_file.write_buf = std::mem::take(&mut self.buffer);
    }
}
