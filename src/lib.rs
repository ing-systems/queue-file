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
//!
//! # Thread safety
//!
//! [`QueueFile`] is not thread-safe: it is neither `Send` nor `Sync`. File I/O operations on a
//! shared object from multiple threads would corrupt the on-disk state and the in-memory queue
//! bookkeeping. Wrap it in a synchronization primitive for cross-thread use, e.g.:
//!
//! ```ignore
//! use std::sync::{Mutex, RwLock};
//!
//! // Safe: `Mutex` provides exclusive access
//! let qf = Mutex::new(QueueFile::open("queue.qf")?);
//! qf.lock().unwrap().add(b"data")?;
//!
//! // Safe: `RwLock` allows concurrent reads with exclusive writes
//! let qf = RwLock::new(QueueFile::open("queue.qf")?);
//! let data = qf.read().unwrap().peek()?;
//! ```
//!
//! Opening the same queue file from multiple processes simultaneously is not supported and will
//! result in data corruption.

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
    clippy::rc_mutex
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

mod cache;
mod element;
mod error;
mod factory;
mod format;
mod header;
mod qio;

use std::cell::Cell;
use std::fs::File;
use std::io;
use std::marker::PhantomData;
use std::path::Path;

use cache::OffsetCache;
pub use cache::OffsetCacheKind;
pub(crate) use element::Element;
pub use error::Error;
pub(crate) use error::{Result, maybe_inject_failpoint};
pub(crate) use format::{
    ExpansionPlan, FileFormat, FormatState, LegacyHeaderState, QueueMetadata, QueueStateSnapshot,
    V2Format, V2OpenState,
};
pub(crate) use header::{SlotData, V2_INITIAL_LEN};
pub(crate) use qio::{DataRing, DataRingMut, DeferredSyncPhase, QueueFileInner};

#[derive(Debug)]
pub struct QueueFile {
    pub(crate) inner: QueueFileInner,
    format: FormatState,
    elem_cnt: usize,
    first: Element,
    last: Element,
    pub(crate) capacity: u64,
    overwrite_on_remove: bool,
    skip_write_header_on_add: bool,
    cache: OffsetCache,
    #[allow(dead_code)]
    _marker: PhantomData<Cell<()>>,
}

impl Drop for QueueFile {
    fn drop(&mut self) {
        if self.skip_write_header_on_add && self.inner.file.is_some() {
            let _ = self.sync_header();
        }
    }
}

impl QueueFile {
    pub(crate) const INITIAL_LENGTH: u64 = 4096;

    #[inline]
    const fn data_start(&self) -> u64 {
        self.format.data_start()
    }

    // ── Constructors ─────────────────────────────────────────────────────────

    /// Opens or creates a [`QueueFile`] at `path` with the given initial file size.
    pub fn with_capacity<P: AsRef<Path>>(path: P, capacity: u64) -> Result<Self> {
        factory::open_internal_full(path, true, false, capacity, true)
    }

    /// Opens or creates a [`QueueFile`] at `path` using v2 format with a 4 KiB initial file size.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::with_capacity(path, Self::INITIAL_LENGTH)
    }

    /// Opens or creates a [`QueueFile`] at `path` using the legacy (16-byte) header format.
    pub fn open_legacy<P: AsRef<Path>>(path: P) -> Result<Self> {
        factory::open_internal_full(path, true, true, Self::INITIAL_LENGTH, false)
    }

    pub(crate) fn build_legacy_queue_file(
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
                deferred_sync_phase: None,
            },
            format: state.format,
            elem_cnt: state.elem_cnt,
            first: Element::EMPTY,
            last: Element::EMPTY,
            capacity,
            overwrite_on_remove,
            skip_write_header_on_add: false,
            cache: OffsetCache::new(),
            _marker: PhantomData,
        };

        if state.file_len < capacity {
            queue_file.inner.sync_set_len(queue_file.capacity)?;
        }

        if state.elem_cnt > 0 {
            let data_start = state.format.data_start();
            let first_logical =
                state.first_pos.checked_sub(data_start).ok_or_else(|| Error::OutOfBounds {
                    msg: format!("first_pos {} < data_start {}", state.first_pos, data_start),
                })?;
            let last_logical =
                state.last_pos.checked_sub(data_start).ok_or_else(|| Error::OutOfBounds {
                    msg: format!("last_pos {} < data_start {}", state.last_pos, data_start),
                })?;

            queue_file.first = queue_file.read_element_at(first_logical)?;
            queue_file.last = queue_file.read_element_at(last_logical)?;
        }

        Ok(queue_file)
    }

    pub(crate) fn build_v2_queue_file(
        mut inner: QueueFileInner, open_state: V2OpenState, capacity: u64,
        overwrite_on_remove: bool,
    ) -> Self {
        inner.file_len = open_state.slot.file_length;

        Self {
            inner,
            format: FormatState::V2(V2Format {
                active_slot: open_state.active_slot,
                generation: open_state.slot.generation,
                next_seq: open_state.slot.next_sequence_number,
            }),
            elem_cnt: open_state.elem_cnt,
            first: Element::EMPTY,
            last: Element::EMPTY,
            capacity: capacity.max(V2_INITIAL_LEN),
            overwrite_on_remove,
            skip_write_header_on_add: false,
            cache: OffsetCache::new(),
            _marker: PhantomData,
        }
    }

    pub(crate) fn initialize_v2_endpoints(&mut self, slot: SlotData) -> Result<()> {
        if self.elem_cnt == 0 {
            return Ok(());
        }

        let data_start = self.data_start();
        let first_logical =
            slot.first_position.checked_sub(data_start).ok_or_else(|| Error::OutOfBounds {
                msg: format!("v2 first_pos {} < data_start {}", slot.first_position, data_start),
            })?;
        let last_logical =
            slot.last_position.checked_sub(data_start).ok_or_else(|| Error::OutOfBounds {
                msg: format!("v2 last_pos {} < data_start {}", slot.last_position, data_start),
            })?;

        let last_header = self.format.validate_v2_element_header(&self.ring(), last_logical)?;
        ensure!(
            last_header.seq == slot.next_sequence_number - 1,
            SequenceMismatch { expected: slot.next_sequence_number - 1, found: last_header.seq }
        );

        self.last =
            Element { pos: last_logical, len: last_header.payload_len, seq: last_header.seq };

        self.first = match self.format.validate_v2_element_header(&self.ring(), first_logical) {
            Ok(first_header) => {
                Element { pos: first_logical, len: first_header.payload_len, seq: first_header.seq }
            }
            Err(_) => self.recover_v2_head(last_logical, last_header.seq, self.elem_cnt)?,
        };

        Ok(())
    }

    fn recover_v2_head(
        &self, last_logical_pos: u64, last_seq: u64, elem_cnt: usize,
    ) -> Result<Element> {
        let mut cur_logical_pos = last_logical_pos;

        for step in 0..elem_cnt {
            let expected_seq =
                last_seq.checked_sub(step as u64).ok_or_else(|| Error::InvalidValue {
                    msg: format!(
                        "v2 recovery: tail seq {last_seq} too small for element_count {elem_cnt}"
                    ),
                })?;

            let (current, next_pos) =
                self.read_prev_v2_header_pos(cur_logical_pos, expected_seq, step, elem_cnt)?;

            if step + 1 == elem_cnt {
                return Ok(current);
            }

            cur_logical_pos = next_pos;
        }

        Err(Error::CorruptedFile {
            msg: "v2 recovery: could not walk expected live element count".to_string(),
        })
    }

    fn read_prev_v2_header_pos(
        &self, pos: u64, expected_seq: u64, step: usize, elem_cnt: usize,
    ) -> Result<(Element, u64)> {
        let current_header = self.format.validate_v2_element_header(&self.ring(), pos)?;

        ensure!(
            current_header.seq == expected_seq,
            SequenceMismatch { expected: expected_seq, found: current_header.seq }
        );

        let current = Element { pos, len: current_header.payload_len, seq: current_header.seq };

        if step + 1 == elem_cnt {
            return Ok((current, 0));
        }

        ensure!(
            current_header.prev_pos != 0,
            InvalidValue {
                msg: format!("v2 recovery: walked {} elements but expected {}", step + 1, elem_cnt)
            }
        );

        let data_start = self.data_start();
        let next_pos =
            current_header.prev_pos.checked_sub(data_start).ok_or_else(|| Error::OutOfBounds {
                msg: format!(
                    "v2 recovery: prev_pos {} < data_start {}",
                    current_header.prev_pos, data_start
                ),
            })?;

        Ok((current, next_pos))
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
        self.cache.kind
    }

    #[deprecated(since = "1.4.7", note = "Use `cache_offset_policy` instead.")]
    pub const fn get_cache_offset_policy(&self) -> Option<OffsetCacheKind> {
        self.cache_offset_policy()
    }

    #[inline]
    pub fn set_cache_offset_policy(&mut self, kind: impl Into<Option<OffsetCacheKind>>) {
        self.cache.set_policy(kind);
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
            self.inner.file_mut()?.sync_data()?;
            self.sync_header()?;
        }

        Ok(self.inner.file_mut()?.sync_all()?)
    }

    // ── Cache helpers ─────────────────────────────────────────────────────────

    #[inline]
    fn cache_last_offset_if_needed(&mut self, affected_items: usize) {
        if self.elem_cnt == 0 {
            return;
        }

        self.cache_elem_if_needed(self.elem_cnt - 1, self.last, affected_items);
    }

    #[inline]
    fn snapshot_queue_state(&self) -> QueueStateSnapshot {
        QueueStateSnapshot {
            format: self.format,
            elem_cnt: self.elem_cnt,
            first: self.first,
            last: self.last,
            overwrite_on_remove: self.overwrite_on_remove,
            cache: self.cache.clone(),
        }
    }

    #[inline]
    fn restore_queue_state(&mut self, snapshot: QueueStateSnapshot) {
        self.format = snapshot.format;
        self.elem_cnt = snapshot.elem_cnt;
        self.first = snapshot.first;
        self.last = snapshot.last;
        self.overwrite_on_remove = snapshot.overwrite_on_remove;
        self.cache = snapshot.cache;
    }

    fn cache_elem_if_needed(&mut self, index: usize, elem: Element, affected_items: usize) {
        self.cache.cache_elem_if_needed(index, elem, self.elem_cnt, affected_items);
    }

    #[inline]
    fn cached_index_up_to(&self, i: usize) -> Option<usize> {
        self.cache.cached_index_up_to(i)
    }

    fn with_batched_append_sync<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let sync_writes = self.inner.sync_writes;
        let deferred_sync_phase = self.inner.deferred_sync_phase;
        self.inner.sync_writes = false;
        self.inner.deferred_sync_phase = Some(DeferredSyncPhase::AppendBatch);

        let result = f(self);

        self.inner.deferred_sync_phase = deferred_sync_phase;
        self.inner.sync_writes = sync_writes;

        let value = result?;

        if sync_writes {
            self.inner.file_mut()?.sync_data()?;
            maybe_inject_failpoint("v2_after_add_batch_flush")?;
        }

        Ok(value)
    }

    // ── add_n ─────────────────────────────────────────────────────────────────

    pub fn add_n(&mut self, elems: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Result<()> {
        let elems: Vec<_> = elems.into_iter().collect();
        if elems.is_empty() {
            return Ok(());
        }

        let mut total_span: u64 = 0;
        for elem in &elems {
            let len = elem.as_ref().len();
            ensure!(i32::try_from(len).is_ok(), Error::ElementTooBig);
            total_span = total_span.saturating_add(self.format.elem_span(len));
        }

        self.expand_if_necessary(total_span)?;

        let snapshot = self.snapshot_queue_state();

        let result = self.with_batched_append_sync(|queue_file| {
            let mut count = 0usize;
            let base_seq = queue_file.format.next_seq();

            for elem in &elems {
                queue_file.append_single_element(elem.as_ref(), base_seq, count)?;
                count += 1;
            }

            Ok(count)
        });

        match result {
            Ok(count) => {
                if count != 0 {
                    self.finalize_append(count)?;
                }
                Ok(())
            }
            Err(err) => {
                self.restore_queue_state(snapshot);
                Err(err)
            }
        }
    }

    fn finalize_append(&mut self, count: usize) -> Result<()> {
        let next_seq = self.format.next_seq();
        if next_seq != 0 {
            self.format.set_next_seq(next_seq + count as u64);
        }

        if !self.skip_write_header_on_add {
            self.sync_header()?;
        }

        self.cache_last_offset_if_needed(count);
        Ok(())
    }

    fn append_single_element(&mut self, buf: &[u8], base_seq: u64, count: usize) -> Result<()> {
        ensure!(self.elem_cnt + 1 < i32::MAX as usize, Error::TooManyElements);

        let is_empty = self.is_empty();
        let last_pos = self.last.pos;
        let last_len = self.last.len;

        let len = buf.len();

        let pos =
            if is_empty { 0 } else { self.ring().add(last_pos, self.format.elem_span(last_len)) };
        let seq = if base_seq == 0 { 0 } else { base_seq + count as u64 };
        let prev_pos = if is_empty { None } else { Some(last_pos) };

        let format = self.format;
        let mut ring = self.ring_mut();
        format.write_element(&mut ring, pos, buf, seq, prev_pos)?;

        let elem_entry = Element::new(pos, len, seq)?;
        if self.is_empty() {
            self.first = elem_entry;
        }
        self.last = elem_entry;
        self.elem_cnt += 1;
        Ok(())
    }

    #[inline]
    pub fn add(&mut self, buf: &[u8]) -> Result<()> {
        self.add_n(std::iter::once(buf))
    }

    // ── peek ──────────────────────────────────────────────────────────────────

    pub fn peek(&self) -> Result<Option<Vec<u8>>> {
        if self.is_empty() {
            return Ok(None);
        }

        let mut buf = Vec::with_capacity(self.first.len);

        if self.peek_into(&mut buf)? { Ok(Some(buf)) } else { Ok(None) }
    }

    pub fn peek_into(&self, buf: &mut Vec<u8>) -> Result<bool> {
        if self.is_empty() {
            return Ok(false);
        }

        let len = self.first.len;
        buf.resize(len, 0);

        let ring = self.ring();
        let payload_start = ring.add(self.first.pos, self.elem_hdr_len());

        ring.read_at(payload_start, buf)?;

        self.format.validate_footer(&ring, payload_start, &self.first, buf)?;

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
            self.cache
                .offsets
                .iter()
                .zip(self.cache.offsets.iter().skip(1))
                .all(|(a, b)| a.0 < b.0),
            "{:?}",
            self.cache.offsets
        );

        let old_first_pos = self.first.pos;

        let new_first = self.skip_elements_from(n)?;

        let erase_total_len = if self.overwrite_on_remove {
            self.ring().distance(old_first_pos, new_first.pos) as usize
        } else {
            0
        };

        self.elem_cnt -= n;
        self.first = new_first;

        self.sync_header()?;

        self.drop_cached_offsets_up_to(n);

        if self.overwrite_on_remove {
            self.ring_mut().erase_at(old_first_pos, erase_total_len)?;
        }

        Ok(())
    }

    fn skip_elements_from(&self, n: usize) -> Result<Element> {
        let mut new_first = self.first;
        let mut remaining_to_skip = n;

        if let Some(i) = self.cached_index_up_to(n) {
            let (index, elem) = self.cache.offsets[i];
            if index <= n {
                new_first = elem;
                remaining_to_skip = n - index;
            }
        }

        for _ in 0..remaining_to_skip {
            let span = self.elem_span(new_first.len);
            let next_pos = self.ring().add(new_first.pos, span);
            new_first = self.read_element_at(next_pos)?;
        }

        Ok(new_first)
    }

    fn drop_cached_offsets_up_to(&mut self, n: usize) {
        self.cache.drop_up_to(n);
    }

    // ── clear ─────────────────────────────────────────────────────────────────

    pub fn clear(&mut self) -> Result<()> {
        let new_cap = self.capacity.max(self.data_start());

        self.elem_cnt = 0;
        self.first = Element::EMPTY;
        self.last = Element::EMPTY;

        self.sync_header()?;

        if self.overwrite_on_remove {
            let ds = self.data_start();
            let data_region_len = self.file_len().min(new_cap).saturating_sub(ds);
            if data_region_len > 0 {
                self.inner.with_batched_clear_erase_sync(|inner| {
                    inner.write_zero_chunks(ds, data_region_len as usize)
                })?;
            }
        }

        self.cache.clear();

        if self.file_len() > new_cap {
            self.inner.sync_set_len(new_cap)?;
        }

        Ok(())
    }

    // ── iter ──────────────────────────────────────────────────────────────────

    #[inline]
    pub const fn iter(&self) -> Iter<'_> {
        Iter {
            buffer: Vec::new(),
            queue_file: self,
            next_elem_index: 0,
            next_elem_pos: self.first.pos,
        }
    }

    // ── Metrics ───────────────────────────────────────────────────────────────

    #[inline]
    const fn ring(&self) -> DataRing<'_> {
        DataRing::new(&self.inner, self.format.data_start())
    }

    #[inline]
    fn ring_mut(&mut self) -> DataRingMut<'_> {
        let data_start = self.format.data_start();
        DataRingMut::new(&mut self.inner, data_start)
    }

    #[inline]
    fn read_element_at(&self, logical_pos: u64) -> Result<Element> {
        self.format.read_element(&self.ring(), logical_pos)
    }

    #[inline]
    pub const fn file_len(&self) -> u64 {
        self.inner.file_len
    }

    #[inline]
    pub fn used_bytes(&self) -> u64 {
        if self.elem_cnt == 0 {
            self.data_start()
        } else {
            self.ring().distance(self.first.pos, self.last.pos)
                + self.elem_span(self.last.len)
                + self.data_start()
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
    fn remaining_bytes(&self) -> u64 {
        self.file_len() - self.used_bytes()
    }

    fn sync_header(&mut self) -> Result<()> {
        self.commit_header(self.file_len(), self.size(), self.first.pos, self.last.pos)
    }

    #[inline]
    fn elem_hdr_len(&self) -> u64 {
        self.format.elem_hdr_len()
    }

    #[inline]
    fn elem_span(&self, payload_len: usize) -> u64 {
        self.format.elem_span(payload_len)
    }

    fn commit_header(
        &mut self, file_len: u64, elem_cnt: usize, first_pos: u64, last_pos: u64,
    ) -> Result<()> {
        let metadata = QueueMetadata { file_len, elem_cnt, first_pos, last_pos };
        self.format.commit_header(&mut self.inner, metadata)
    }

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
        let (end_of_last_elem, wraps) = self.calculate_tail_metrics();
        let moved_count = if wraps { end_of_last_elem } else { 0 };

        Some(ExpansionPlan {
            orig_file_len,
            new_len: Self::compute_expanded_len(orig_file_len, rem_bytes, data_len),
            end_of_last_elem: self.data_start() + end_of_last_elem,
            wraps,
            moved_count,
        })
    }

    fn calculate_tail_metrics(&self) -> (u64, bool) {
        if self.elem_cnt > 0 {
            let ring = self.ring();
            let end = ring.add(self.last.pos, self.format.elem_span(self.last.len));
            (end, end <= self.first.pos)
        } else {
            (0, false)
        }
    }

    fn relocate_wrapped_data(&mut self, plan: ExpansionPlan) -> Result<()> {
        if !plan.wraps {
            return Ok(());
        }

        self.ring_mut().relocate(plan.orig_file_len, plan.moved_count)?;

        let first = self.first;
        let last = self.last;
        let elem_cnt = self.elem_cnt;
        let format = self.format;

        if let Some(new_last_pos) =
            format.on_expansion(&mut self.ring_mut(), &plan, first, last, elem_cnt)?
        {
            self.last = Element { pos: new_last_pos, len: self.last.len, seq: self.last.seq };

            maybe_inject_failpoint("v2_before_relocation_commit")?;

            if self.overwrite_on_remove {
                self.sync_header()?;
                maybe_inject_failpoint("v2_after_relocation_commit_before_erase")?;
            }
        }

        Ok(())
    }

    fn cleanup_after_expansion(&mut self, plan: ExpansionPlan) -> Result<()> {
        if self.overwrite_on_remove {
            self.ring_mut().erase_at(0, plan.moved_count as usize)?;
            self.format.on_expansion_cleanup(&plan)?;
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
        self.cache.clear();
        self.cleanup_after_expansion(plan)?;

        let bytes_used_after = self.used_bytes();
        debug_assert_eq!(bytes_used_before, bytes_used_after);

        Ok(())
    }
}

impl<'a> IntoIterator for &'a QueueFile {
    type IntoIter = Iter<'a>;
    type Item = Vec<u8>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

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
        let target_idx = self.next_elem_index.checked_add(n)?;

        if target_idx >= self.queue_file.elem_cnt {
            self.next_elem_index = self.queue_file.elem_cnt;
            return None;
        }

        self.jump_to_cache_if_closer(target_idx);

        let remaining_to_skip = target_idx - self.next_elem_index;
        for _ in 0..remaining_to_skip {
            self.skip_next()?;
        }

        self.next()
    }
}

impl Iter<'_> {
    fn jump_to_cache_if_closer(&mut self, target_idx: usize) {
        if let Some(cache_idx) = self.queue_file.cached_index_up_to(target_idx) {
            let (index, elem) = self.queue_file.cache.offsets[cache_idx];
            if index > self.next_elem_index {
                self.next_elem_index = index;
                self.next_elem_pos = elem.pos;
            }
        }
    }

    fn skip_next(&mut self) -> Option<()> {
        if self.next_elem_index >= self.queue_file.elem_cnt {
            return None;
        }
        let current = self.queue_file.read_element_at(self.next_elem_pos).ok()?;
        self.next_elem_pos =
            self.queue_file.ring().add(current.pos, self.queue_file.elem_span(current.len));
        self.next_elem_index += 1;
        Some(())
    }

    pub fn borrowed_next(&mut self) -> Option<&[u8]> {
        if self.next_elem_index >= self.queue_file.elem_cnt {
            return None;
        }

        let current = self.queue_file.read_element_at(self.next_elem_pos).ok()?;

        let max_possible_len = self.queue_file.ring().capacity();
        if current.len as u64 > max_possible_len {
            return None;
        }

        let ring = self.queue_file.ring();
        let payload_start = ring.add(current.pos, self.queue_file.elem_hdr_len());

        if current.len > self.buffer.len() {
            self.buffer.resize(current.len, 0);
        }
        ring.read_at(payload_start, &mut self.buffer[..current.len]).ok()?;

        self.queue_file
            .format
            .validate_footer(&ring, payload_start, &current, &self.buffer[..current.len])
            .ok()?;

        self.next_elem_pos = ring.add(current.pos, self.queue_file.elem_span(current.len));
        self.next_elem_index += 1;

        Some(&self.buffer[..current.len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;

    fn create_inner(len: u64) -> (QueueFileInner, auto_delete_path::AutoDeletePath) {
        let p = auto_delete_path::AutoDeletePath::temp();
        let file =
            OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&p).unwrap();
        file.set_len(len).unwrap();
        (
            QueueFileInner {
                file: Some(file),
                file_len: len,
                expected_seek: 0,
                last_seek: None,
                transfer_buf: vec![0u8; 1024].into_boxed_slice(),
                sync_writes: false,
                deferred_sync_phase: None,
            },
            p,
        )
    }

    #[test]
    fn test_data_ring_addressing() {
        let (inner, _p) = create_inner(100);
        let ring = DataRing::new(&inner, 20);

        assert_eq!(ring.capacity(), 80);
        assert_eq!(ring.phys_pos(0), 20);
        assert_eq!(ring.phys_pos(79), 99);
        assert_eq!(ring.phys_pos(80), 20);
        assert_eq!(ring.phys_pos(160), 20);

        assert_eq!(ring.add(10, 20), 30);
        assert_eq!(ring.add(70, 20), 10);

        assert_eq!(ring.distance(10, 30), 20);
        assert_eq!(ring.distance(70, 10), 20);
    }

    #[test]
    fn test_data_ring_io() {
        let (mut inner, _p) = create_inner(100);
        let mut ring_mut = DataRingMut::new(&mut inner, 20);

        let data = b"hello world";
        ring_mut.write_at(10, data).unwrap();
        let mut buf = [0u8; 11];
        ring_mut.as_read_only().read_at(10, &mut buf).unwrap();
        assert_eq!(&buf, data);

        let data2 = b"wrapmebase";
        ring_mut.write_at(75, data2).unwrap();
        let mut buf2 = [0u8; 10];
        ring_mut.as_read_only().read_at(75, &mut buf2).unwrap();
        assert_eq!(&buf2, data2);

        let mut phys_buf = [0u8; 5];
        ring_mut.inner.read_exact_at(95, &mut phys_buf).unwrap();
        assert_eq!(&phys_buf, b"wrapm");
        ring_mut.inner.read_exact_at(20, &mut phys_buf).unwrap();
        assert_eq!(&phys_buf, b"ebase");
    }

    #[test]
    fn remove_n_reindexes_cached_offsets_without_dropping_survivors() {
        let path = auto_delete_path::AutoDeletePath::temp();
        let mut qf = QueueFile::with_capacity(&path, 4096).unwrap();
        qf.set_cache_offset_policy(Some(OffsetCacheKind::Linear { offset: 1 }));

        for i in 0u8..8 {
            qf.add(&[i]).unwrap();
        }

        assert_eq!(
            qf.cache.offsets.iter().map(|(idx, _)| *idx).collect::<Vec<_>>(),
            (1..8).collect::<Vec<_>>()
        );

        qf.remove_n(3).unwrap();

        assert_eq!(
            qf.cache.offsets.iter().map(|(idx, _)| *idx).collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4]
        );
        assert_eq!(qf.iter().map(|v| v[0]).collect::<Vec<_>>(), vec![3, 4, 5, 6, 7]);
        assert_eq!(qf.iter().nth(2).map(|v| v[0]), Some(5));
    }
}
