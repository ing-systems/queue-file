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

#[cfg(test)]
extern crate pretty_assertions;

use snafu::{ensure, Snafu};
use std::cmp::min;
use std::fs::{rename, File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::Path;

use bytes::{Buf, BufMut, BytesMut};

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

/// QueueFile is a lightning-fast, transactional, file-based FIFO.
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
/// let mut qf = QueueFile::open("example.qf")
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
}

#[derive(Debug)]
struct QueueFileInner {
    file: File,
    /// Cached file length. Always a power of 2.
    file_len: u64,
    /// Last seek value
    last_seek: Option<u64>,
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
    }
}

impl QueueFile {
    const BLOCK_LENGTH: u64 = 4096;
    const INITIAL_LENGTH: u64 = 4096;
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
                buf.put_u32(QueueFile::VERSIONED_HEADER);
                buf.put_u64(capacity);
            }

            file.write_all(buf.as_ref())?;
        }

        // A rename is atomic.
        rename(tmp_path, path)?;

        Ok(())
    }

    pub fn with_capacity<P: AsRef<Path>>(path: P, capacity: u64) -> Result<QueueFile> {
        Self::open_internal(path, true, false, capacity)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<QueueFile> {
        Self::with_capacity(path, QueueFile::INITIAL_LENGTH)
    }

    pub fn open_legacy<P: AsRef<Path>>(path: P) -> Result<QueueFile> {
        Self::open_internal(path, true, true, QueueFile::INITIAL_LENGTH)
    }

    fn open_internal<P: AsRef<Path>>(
        path: P, overwrite_on_remove: bool, force_legacy: bool, capacity: u64,
    ) -> Result<QueueFile> {
        if !path.as_ref().exists() {
            QueueFile::init(path.as_ref(), force_legacy, capacity)?;
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

            assert!(file_len <= i64::max_value() as u64);
            assert!(elem_cnt <= i32::max_value() as usize);
            assert!(first_pos <= i64::max_value() as u64);
            assert!(last_pos <= i64::max_value() as u64);
        } else {
            header_len = 16;

            file_len = u64::from(buf.get_u32());
            elem_cnt = buf.get_u32() as usize;
            first_pos = u64::from(buf.get_u32());
            last_pos = u64::from(buf.get_u32());

            assert!(file_len <= i32::max_value() as u64);
            assert!(elem_cnt <= i32::max_value() as usize);
            assert!(first_pos <= i32::max_value() as u64);
            assert!(last_pos <= i32::max_value() as u64);
        }

        let real_file_len = file.metadata()?.len();

        ensure!(file_len <= real_file_len, CorruptedFileSnafu {
            msg: format!(
                "file is truncated. expected length was {} but actual length is {}",
                file_len, real_file_len
            )
        });
        ensure!(file_len >= header_len, CorruptedFileSnafu {
            msg: format!("length stored in header ({}) is invalid", file_len)
        });
        ensure!(first_pos <= file_len, CorruptedFileSnafu {
            msg: format!("position of the first element ({}) is beyond the file", first_pos)
        });
        ensure!(last_pos <= file_len, CorruptedFileSnafu {
            msg: format!("position of the last element ({}) is beyond the file", last_pos)
        });

        file.rewind()?;

        let mut queue_file = QueueFile {
            inner: QueueFileInner {
                file,
                file_len,
                last_seek: Some(0),
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
        };

        if file_len < capacity {
            queue_file.inner.sync_set_len(queue_file.capacity)?;
        }

        queue_file.first = queue_file.read_element(first_pos)?;
        queue_file.last = queue_file.read_element(last_pos)?;

        Ok(queue_file)
    }

    /// Returns true if removing an element will also overwrite data with zero bytes.
    pub fn get_overwrite_on_remove(&self) -> bool {
        self.overwrite_on_remove
    }

    /// If set to true removing an element will also overwrite data with zero bytes.
    pub fn set_overwrite_on_remove(&mut self, value: bool) {
        self.overwrite_on_remove = value
    }

    /// Returns true if every write to file will be followed by `sync_data()` call.
    pub fn get_sync_writes(&self) -> bool {
        self.inner.sync_writes
    }

    /// If set to true every write to file will be followed by `sync_data()` call.
    pub fn set_sync_writes(&mut self, value: bool) {
        self.inner.sync_writes = value
    }

    /// Returns true if skips header update upon adding enabled.
    pub fn get_skip_write_header_on_add(&self) -> bool {
        self.skip_write_header_on_add
    }

    /// If set to true skips header update upon adding.
    pub fn set_skip_write_header_on_add(&mut self, value: bool) {
        self.skip_write_header_on_add = value
    }

    /// Returns true if this queue contains no entries.
    pub fn is_empty(&self) -> bool {
        self.elem_cnt == 0
    }

    /// Returns the number of elements in this queue.
    pub fn size(&self) -> usize {
        self.elem_cnt
    }

    /// Synchronizes the underlying file, look at [File::sync_all] doc for more info.
    pub fn sync_all(&mut self) -> Result<()> {
        if self.skip_write_header_on_add {
            self.sync_header()?;
        }

        Ok(self.inner.file.sync_all()?)
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
            ensure!(len <= i32::max_value() as usize, ElementTooBigSnafu {});

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

        Ok(())
    }

    /// Adds an element to the end of the queue.
    pub fn add(&mut self, buf: &[u8]) -> Result<()> {
        ensure!(self.elem_cnt + 1 < i32::max_value() as usize, TooManyElementsSnafu {});

        let len = buf.len();

        ensure!(len <= i32::max_value() as usize, ElementTooBigSnafu {});

        self.expand_if_necessary((Element::HEADER_LENGTH + len) as u64)?;

        // Insert a new element after the current last element.
        let was_empty = self.is_empty();
        let pos = if was_empty {
            self.header_len
        } else {
            self.wrap_pos(self.last.pos + Element::HEADER_LENGTH as u64 + self.last.len as u64)
        };

        let new_last = Element::new(pos, len);

        self.write_buf.clear();
        // Add length.
        self.write_buf.extend(&(len as u32).to_be_bytes());
        // Add data.
        self.write_buf.extend(buf);

        // Make actual write.
        self.ring_write_buf(new_last.pos)?;

        if !self.skip_write_header_on_add {
            // Commit the addition. If was empty, first == last.
            let first_pos = if was_empty { new_last.pos } else { self.first.pos };
            self.write_header(self.file_len(), self.elem_cnt + 1, first_pos, new_last.pos)?;
        }
        self.last = new_last;
        self.elem_cnt += 1;

        if was_empty {
            self.first = self.last;
        }

        Ok(())
    }

    /// Reads the eldest element. Returns `OK(None)` if the queue is empty.
    pub fn peek(&mut self) -> Result<Option<Box<[u8]>>> {
        if self.is_empty() {
            Ok(None)
        } else {
            let len = self.first.len;
            let mut data = vec![0; len as usize].into_boxed_slice();

            self.ring_read(self.first.pos + Element::HEADER_LENGTH as u64, &mut data)?;

            Ok(Some(data))
        }
    }

    /// Removes the eldest element.
    pub fn remove(&mut self) -> Result<()> {
        self.remove_n(1)
    }

    /// Removes the eldest `n` elements.
    pub fn remove_n(&mut self, n: usize) -> Result<()> {
        if n == 0 || self.is_empty() {
            return Ok(());
        }

        let n = min(n, self.elem_cnt);

        if n == self.elem_cnt {
            return self.clear();
        }

        let erase_start_pos = self.first.pos;
        let mut erase_total_len = 0usize;

        // Read the position and length of the new first element.
        let mut new_first_pos = self.first.pos;
        let mut new_first_len = self.first.len;

        for _ in 0..n {
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
            self.inner.seek(self.header_len)?;
            let first_block = self.capacity.min(Self::BLOCK_LENGTH) - self.header_len;
            self.inner.write(&QueueFile::ZEROES[..first_block as usize])?;

            if let Some(left) = self.capacity.checked_sub(Self::BLOCK_LENGTH) {
                for _ in 0..left / Self::BLOCK_LENGTH {
                    self.inner.write(&QueueFile::ZEROES)?;
                }
                let tail = left % Self::BLOCK_LENGTH;
                if tail != 0 {
                    self.inner.write(&QueueFile::ZEROES[..tail as usize])?;
                }
            }
        }

        self.elem_cnt = 0;
        self.first = Element::EMPTY;
        self.last = Element::EMPTY;

        if self.file_len() > self.capacity {
            self.inner.sync_set_len(self.capacity)?;
        }
        self.inner.file_len = self.capacity;

        Ok(())
    }

    /// Returns an iterator over elements in this queue.
    pub fn iter(&mut self) -> Iter<'_> {
        let pos = self.first.pos;

        Iter { queue_file: self, next_elem_index: 0, next_elem_pos: pos }
    }

    /// Returns the amount of bytes used by the backed file.
    /// Always >= [Self::used_bytes].
    pub fn file_len(&self) -> u64 {
        self.inner.file_len
    }

    /// Returns the amount of bytes used by the queue.
    pub fn used_bytes(&self) -> u64 {
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

    fn remaining_bytes(&self) -> u64 {
        self.file_len() - self.used_bytes()
    }

    fn sync_header(&mut self) -> io::Result<()> {
        self.write_header(self.file_len(), self.size(), self.first.pos, self.last.pos)
    }

    /// Writes header atomically. The arguments contain the updated values. The struct member fields
    /// should not have changed yet. This only updates the state in the file. It's up to the caller
    /// to update the class member variables *after* this call succeeds. Assumes segment writes are
    /// atomic in the underlying file system.
    fn write_header(
        &mut self, file_len: u64, elem_cnt: usize, first_pos: u64, last_pos: u64,
    ) -> io::Result<()> {
        let mut header = [0; 32];
        let mut header_buf = &mut header[..];

        // Never allow write values that will render file unreadable by Java library.
        if self.versioned {
            assert!(file_len <= i64::max_value() as u64);
            assert!(elem_cnt <= i32::max_value() as usize);
            assert!(first_pos <= i64::max_value() as u64);
            assert!(last_pos <= i64::max_value() as u64);

            header_buf.put_u32(QueueFile::VERSIONED_HEADER);
            header_buf.put_u64(file_len);
            header_buf.put_i32(elem_cnt as i32);
            header_buf.put_u64(first_pos);
            header_buf.put_u64(last_pos);
        } else {
            assert!(file_len <= i32::max_value() as u64);
            assert!(elem_cnt <= i32::max_value() as usize);
            assert!(first_pos <= i32::max_value() as u64);
            assert!(last_pos <= i32::max_value() as u64);

            header_buf.put_i32(file_len as i32);
            header_buf.put_i32(elem_cnt as i32);
            header_buf.put_i32(first_pos as i32);
            header_buf.put_i32(last_pos as i32);
        }

        self.inner.seek(0)?;
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
    fn wrap_pos(&self, pos: u64) -> u64 {
        if pos < self.file_len() { pos } else { self.header_len + pos - self.file_len() }
    }

    /// Writes `n` bytes from buffer to position in file. Automatically wraps write if position is
    /// past the end of the file or if buffer overlaps it.
    fn ring_write_buf(&mut self, pos: u64) -> io::Result<()> {
        let pos = self.wrap_pos(pos);

        if pos + self.write_buf.len() as u64 <= self.file_len() {
            self.inner.seek(pos)?;
            self.inner.write(&self.write_buf)
        } else {
            let before_eof = (self.file_len() - pos) as usize;

            self.inner.seek(pos)?;
            self.inner.write(&self.write_buf[..before_eof])?;
            self.inner.seek(self.header_len)?;
            self.inner.write(&self.write_buf[before_eof..])
        }
    }

    fn ring_erase(&mut self, pos: u64, n: usize) -> io::Result<()> {
        let mut pos = pos;
        let mut len = n;

        self.write_buf.clear();
        self.write_buf.extend(QueueFile::ZEROES);
        while len > 0 {
            let chunk_len = min(len, QueueFile::ZEROES.len());
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
            self.inner.seek(pos)?;
            self.inner.read(buf)
        } else {
            let before_eof = (self.file_len() - pos) as usize;

            self.inner.seek(pos)?;
            self.inner.read(&mut buf[..before_eof])?;
            self.inner.seek(self.header_len)?;
            self.inner.read(&mut buf[before_eof..])
        }
    }

    /// If necessary, expands the file to accommodate an additional element of the given length.
    fn expand_if_necessary(&mut self, data_len: u64) -> io::Result<()> {
        let mut rem_bytes = self.remaining_bytes();

        if rem_bytes >= data_len {
            return Ok(());
        }

        let mut prev_len = self.file_len();
        let mut new_len = prev_len;

        while rem_bytes < data_len {
            rem_bytes += prev_len;
            new_len = prev_len << 1;
            prev_len = new_len;
        }

        self.inner.sync_set_len(new_len)?;

        // // Calculate the position of the tail end of the data in the ring buffer
        let end_of_last_elem =
            self.wrap_pos(self.last.pos + Element::HEADER_LENGTH as u64 + self.last.len as u64);
        let mut count = 0u64;

        // If the buffer is split, we need to make it contiguous
        if end_of_last_elem <= self.first.pos {
            count = end_of_last_elem - self.header_len;

            let write_pos = self.inner.seek(self.file_len())?;
            self.inner.transfer(self.header_len, write_pos, count)?;
        }

        // Commit the expansion.
        if self.last.pos < self.first.pos {
            let new_last_pos = self.file_len() + self.last.pos - self.header_len;
            self.last = Element::new(new_last_pos, self.last.len);
        }

        self.inner.file_len = new_len;

        if self.overwrite_on_remove {
            self.ring_erase(self.header_len, count as usize)?;
        }

        Ok(())
    }
}

// I/O Helpers
impl QueueFileInner {
    const TRANSFER_BUFFER_SIZE: usize = 128 * 1024;

    fn seek(&mut self, pos: u64) -> io::Result<u64> {
        if Some(pos) == self.last_seek {
            Ok(pos)
        } else {
            let res = self.file.seek(SeekFrom::Start(pos));
            self.last_seek = res.as_ref().ok().copied();
            res
        }
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<()> {
        if let Some(seek) = &mut self.last_seek {
            *seek += buf.len() as u64;
        }
        let r = self.file.read_exact(buf);
        if r.is_err() {
            self.last_seek = None;
        }
        r
    }

    fn write(&mut self, buf: &[u8]) -> io::Result<()> {
        if let Some(seek) = &mut self.last_seek {
            *seek += buf.len() as u64;
        }
        if let Err(err) = self.file.write_all(buf) {
            self.last_seek = None;
            return Err(err);
        }

        if self.sync_writes { self.file.sync_data() } else { Ok(()) }
    }

    fn transfer_inner(
        &mut self, buf: &mut [u8], mut read_pos: u64, mut write_pos: u64, count: u64,
    ) -> io::Result<()> {
        debug_assert!(read_pos < self.file_len);
        debug_assert!(write_pos <= self.file_len);
        debug_assert!(count < self.file_len);
        debug_assert!(count <= i64::max_value() as u64);

        let mut bytes_left = count as i64;

        while bytes_left > 0 {
            self.seek(read_pos)?;
            let bytes_to_read = min(bytes_left as usize, Self::TRANSFER_BUFFER_SIZE);
            self.read(&mut buf[..bytes_to_read])?;

            self.seek(write_pos)?;
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
    fn transfer(&mut self, read_pos: u64, write_pos: u64, count: u64) -> io::Result<()> {
        let mut buf = self.transfer_buf.take().unwrap();
        let res = self.transfer_inner(&mut buf, read_pos, write_pos, count);
        self.transfer_buf = Some(buf);

        res
    }

    fn sync_set_len(&mut self, new_len: u64) -> io::Result<()> {
        self.file.set_len(new_len)?;
        self.file.sync_all()
    }
}

#[derive(Copy, Clone, Debug)]
struct Element {
    pos: u64,
    len: usize,
}

impl Element {
    const EMPTY: Element = Element { pos: 0, len: 0 };
    const HEADER_LENGTH: usize = 4;

    fn new(pos: u64, len: usize) -> Self {
        assert!(
            pos <= i64::max_value() as u64,
            "element position must be less than {}",
            i64::max_value()
        );
        assert!(
            len <= i32::max_value() as usize,
            "element length must be less than {}",
            i32::max_value()
        );

        Element { pos, len }
    }
}

pub struct Iter<'a> {
    queue_file: &'a mut QueueFile,
    next_elem_index: usize,
    next_elem_pos: u64,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Box<[u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.queue_file.is_empty() || self.next_elem_index >= self.queue_file.elem_cnt {
            return None;
        }

        let current = self.queue_file.read_element(self.next_elem_pos).ok()?;
        self.next_elem_pos = self.queue_file.wrap_pos(current.pos + Element::HEADER_LENGTH as u64);

        let mut data = vec![0; current.len].into_boxed_slice();
        self.queue_file.ring_read(self.next_elem_pos, &mut data).ok()?;

        self.next_elem_pos = self
            .queue_file
            .wrap_pos(current.pos + Element::HEADER_LENGTH as u64 + current.len as u64);
        self.next_elem_index += 1;

        Some(data)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let elems_left = self.queue_file.elem_cnt - self.next_elem_index;

        (elems_left, Some(elems_left))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::fs::remove_file;
    use std::iter;
    use std::path::PathBuf;

    #[cfg(test)]
    use pretty_assertions::assert_eq;
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    use super::*;

    fn gen_rand_data(size: usize) -> Box<[u8]> {
        let mut buf = vec![0u8; size];
        thread_rng().fill(buf.as_mut_slice());

        buf.into_boxed_slice()
    }

    fn gen_rand_file_name() -> String {
        let mut rng = thread_rng();
        let mut file_name = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .map(char::from)
            .take(16)
            .collect::<String>();

        file_name.push_str(".qf");

        file_name
    }

    fn new_queue_file(overwrite_on_remove: bool) -> (PathBuf, QueueFile) {
        new_queue_file_ex(overwrite_on_remove, false, None)
    }

    fn new_legacy_queue_file(overwrite_on_remove: bool) -> (PathBuf, QueueFile) {
        new_queue_file_ex(overwrite_on_remove, true, None)
    }

    fn new_queue_file_ex(
        overwrite_on_remove: bool, force_legacy: bool, capacity: Option<u64>,
    ) -> (PathBuf, QueueFile) {
        let file_name = gen_rand_file_name();
        let path = Path::new(file_name.as_str());
        let mut queue_file = if force_legacy {
            assert!(capacity.is_none(), "capacity can be set only for non-legacy queue");
            QueueFile::open_legacy(path).unwrap()
        } else {
            if let Some(capacity) = capacity {
                QueueFile::with_capacity(path, capacity)
            } else {
                QueueFile::open(path)
            }
            .unwrap()
        };

        queue_file.set_overwrite_on_remove(overwrite_on_remove);

        (path.to_owned(), queue_file)
    }

    const ITERATIONS: usize = 100;
    const MIN_N: usize = 1;
    const MAX_N: usize = 10;
    const MIN_DATA_SIZE: usize = 0;
    const MAX_DATA_SIZE: usize = 4096;
    const CLEAR_PROB: f64 = 0.05;
    const REOPEN_PROB: f64 = 0.01;

    #[test]
    fn test_queue_file_iter() {
        let (path, mut qf) = new_queue_file(false);
        let mut q: VecDeque<Box<[u8]>> = VecDeque::with_capacity(128);

        add_rand_n_elems(&mut q, &mut qf, 10, 15, MIN_DATA_SIZE, MAX_DATA_SIZE);

        for elem in qf.iter() {
            assert_eq!(elem, q.pop_front().unwrap());
        }

        assert_eq!(q.is_empty(), true);

        let qv = qf.iter().collect::<Vec<_>>();
        assert_eq!(qv.len(), qf.size());

        for e in qf.iter().zip(qv.iter()) {
            assert_eq!(&e.0, e.1);
        }

        remove_file(&path).unwrap_or(());
    }

    #[test]
    fn test_queue_file() {
        let (path, qf) = new_queue_file(false);
        simulate_use(
            &path,
            qf,
            ITERATIONS,
            MIN_N,
            MAX_N,
            MIN_DATA_SIZE,
            MAX_DATA_SIZE,
            CLEAR_PROB,
            REOPEN_PROB,
        );
        remove_file(&path).unwrap_or(());
    }

    #[test]
    fn test_queue_file_with_zero() {
        let (path, qf) = new_queue_file(true);
        simulate_use(
            &path,
            qf,
            ITERATIONS,
            MIN_N,
            MAX_N,
            MIN_DATA_SIZE,
            MAX_DATA_SIZE,
            CLEAR_PROB,
            REOPEN_PROB,
        );
        remove_file(&path).unwrap_or(());
    }

    #[test]
    fn test_legacy_queue_file() {
        let (path, qf) = new_legacy_queue_file(false);
        simulate_use(
            &path,
            qf,
            ITERATIONS,
            MIN_N,
            MAX_N,
            MIN_DATA_SIZE,
            MAX_DATA_SIZE,
            CLEAR_PROB,
            REOPEN_PROB,
        );
        remove_file(&path).unwrap_or(());
    }

    #[test]
    fn test_legacy_queue_file_with_zero() {
        let (path, qf) = new_legacy_queue_file(true);
        simulate_use(
            &path,
            qf,
            ITERATIONS,
            MIN_N,
            MAX_N,
            MIN_DATA_SIZE,
            MAX_DATA_SIZE,
            CLEAR_PROB,
            REOPEN_PROB,
        );
        remove_file(&path).unwrap_or(());
    }

    #[test]
    fn queue_capacity_preserved() {
        for is_overwrite in [false, true] {
            eprintln!("is_overwrite = {is_overwrite}");

            let initial_size = 517;
            let (p, mut qf) = new_queue_file_ex(is_overwrite, false, Some(initial_size));

            assert_eq!(std::fs::metadata(&p).unwrap().len(), initial_size);

            for i in 0u32..40 {
                qf.add(&i.to_be_bytes()).unwrap();
            }
            assert_eq!(std::fs::metadata(&p).unwrap().len(), initial_size);

            for i in 0u32..40 {
                qf.add(&i.to_be_bytes()).unwrap();
            }
            assert_eq!(std::fs::metadata(&p).unwrap().len(), initial_size * 2);

            qf.clear().unwrap();
            assert_eq!(std::fs::metadata(&p).unwrap().len(), initial_size);

            remove_file(&p).unwrap_or(());
        }
    }

    #[test]
    fn existing_queue_extended_on_new_capacity() {
        for is_overwrite in [false, true] {
            eprintln!("is_overwrite = {is_overwrite}");
            let initial_size = 200;
            let (p, qf) = new_queue_file_ex(is_overwrite, false, Some(initial_size));
            assert_eq!(std::fs::metadata(&p).unwrap().len(), initial_size);

            drop(qf);

            let initial_size2 = 350;
            let qf = QueueFile::with_capacity(&p, initial_size2).unwrap();
            assert_eq!(std::fs::metadata(&p).unwrap().len(), initial_size2);
            drop(qf);

            remove_file(&p).unwrap_or(());
        }
    }

    fn add_rand_n_elems(
        q: &mut VecDeque<Box<[u8]>>, qf: &mut QueueFile, min_n: usize, max_n: usize,
        min_data_size: usize, max_data_size: usize,
    ) -> usize {
        let mut rng = thread_rng();

        let n = rng.gen_range(min_n..max_n);

        for _ in 0..n {
            let data_size = rng.gen_range(min_data_size..max_data_size);
            let data = gen_rand_data(data_size);

            assert_eq!(qf.add(data.as_ref()).is_ok(), true);
            q.push_back(data);
        }

        n
    }

    fn verify_rand_n_elems(
        q: &mut VecDeque<Box<[u8]>>, qf: &mut QueueFile, min_n: usize, max_n: usize,
    ) -> usize {
        if qf.is_empty() {
            return 0;
        }

        let n = if qf.size() == 1 { 1 } else { thread_rng().gen_range(min_n..max_n) };

        for _ in 0..n {
            let d0 = q.pop_front().unwrap();
            let d1 = qf.peek().unwrap().unwrap();
            assert_eq!(qf.remove().is_ok(), true);

            assert_eq!(d0, d1);
        }

        n
    }

    fn simulate_use(
        path: &Path, mut qf: QueueFile, iters: usize, min_n: usize, max_n: usize,
        min_data_size: usize, max_data_size: usize, clear_prob: f64, reopen_prob: f64,
    ) {
        let mut q: VecDeque<Box<[u8]>> = VecDeque::with_capacity(128);

        add_rand_n_elems(&mut q, &mut qf, min_n, max_n, min_data_size, max_data_size);

        for _ in 0..iters {
            assert_eq!(q.len(), qf.size());

            if thread_rng().gen_bool(reopen_prob) {
                let overwrite_on_remove = qf.get_overwrite_on_remove();

                drop(qf);
                qf = QueueFile::open(path).unwrap();
                qf.set_overwrite_on_remove(overwrite_on_remove);
            }
            if thread_rng().gen_bool(clear_prob) {
                q.clear();
                qf.clear().unwrap()
            }

            let elem_cnt = qf.size();
            verify_rand_n_elems(&mut q, &mut qf, 1, elem_cnt);
            add_rand_n_elems(&mut q, &mut qf, min_n, max_n, min_data_size, max_data_size);
        }

        while let Ok(Some(data)) = qf.peek() {
            assert_eq!(qf.remove().is_ok(), true);
            assert_eq!(data, q.pop_front().unwrap());
        }

        assert_eq!(q.is_empty(), true);
        assert_eq!(qf.is_empty(), true);
    }

    #[test]
    fn add_n_works() {
        let (path, mut qf) = new_queue_file(true);

        let data = vec![vec![1, 2, 3], vec![4, 5, 6]];

        qf.add_n(data.iter()).unwrap();

        let stored = qf.iter().map(Vec::from).collect::<Vec<_>>();
        assert_eq!(data, stored);

        qf.remove_n(1).unwrap();

        let stored = qf.iter().map(Vec::from).collect::<Vec<_>>();
        assert_eq!(&data[1..], stored);

        qf.remove_n(1).unwrap();

        let stored = qf.iter().map(Vec::from).collect::<Vec<_>>();
        assert_eq!(Vec::<Vec<u8>>::new(), stored);

        let data = vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8]];

        qf.add_n(data.iter()).unwrap();

        let stored = qf.iter().map(Vec::from).collect::<Vec<_>>();
        assert_eq!(data, stored);

        let data: Vec<Vec<_>> = (0..=5).map(|i| vec![i, i, i, i, i, i, i, i, i, i]).collect();

        qf.add_n(data.iter()).unwrap();

        let stored = qf.iter().map(Vec::from).collect::<Vec<_>>();
        assert_eq!(&data, &stored[3..]);

        remove_file(&path).unwrap_or(());
    }
}
