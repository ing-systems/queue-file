use std::cmp::min;
use std::fs::File;
use std::io as io_std;
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

use crate::error::{Result, maybe_inject_failpoint};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeferredSyncPhase {
    ExpansionCopy,
    ClearErase,
    BacklinkRewrite,
    AppendBatch,
}

impl DeferredSyncPhase {
    #[inline]
    pub const fn failpoint_name(self) -> &'static str {
        match self {
            Self::ExpansionCopy => "v2_expansion_copy_per_write_sync",
            Self::ClearErase => "clear_erase_per_write_sync",
            Self::BacklinkRewrite => "v2_backlink_rewrite_per_write_sync",
            Self::AppendBatch => "v2_add_batch_per_write_sync",
        }
    }
}

#[derive(Debug)]
pub struct QueueFileInner {
    pub file: Option<File>,
    pub file_len: u64,
    pub expected_seek: u64,
    pub last_seek: Option<u64>,
    pub transfer_buf: Box<[u8]>,
    pub sync_writes: bool,
    pub deferred_sync_phase: Option<DeferredSyncPhase>,
}

impl QueueFileInner {
    pub const TRANSFER_BUFFER_SIZE: usize = 128 * 1024;

    #[inline]
    pub fn file_ref(&self) -> io_std::Result<&File> {
        self.file.as_ref().ok_or_else(|| {
            io_std::Error::new(io_std::ErrorKind::Other, "queue file handle unavailable")
        })
    }

    #[inline]
    pub fn file_mut(&mut self) -> io_std::Result<&mut File> {
        self.file.as_mut().ok_or_else(|| {
            io_std::Error::new(io_std::ErrorKind::Other, "queue file handle unavailable")
        })
    }

    #[inline]
    pub fn seek(&mut self, pos: u64) -> u64 {
        self.expected_seek = pos;
        pos
    }

    pub fn real_seek(&mut self) -> io_std::Result<u64> {
        if Some(self.expected_seek) == self.last_seek {
            return Ok(self.expected_seek);
        }

        let expected_seek = self.expected_seek;
        let res = self.file_mut()?.seek(SeekFrom::Start(expected_seek));
        self.last_seek = res.as_ref().ok().copied();

        res
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.real_seek()?;

        self.file_mut()?.write_all(buf)?;

        if let Some(seek) = &mut self.last_seek {
            *seek += buf.len() as u64;
        }

        if self.sync_writes {
            if let Some(phase) = self.deferred_sync_phase {
                maybe_inject_failpoint(phase.failpoint_name())?;
            }
            self.file_mut()?.sync_data()?;
        }

        Ok(())
    }

    pub fn read_exact_at(&self, mut offset: u64, mut buf: &mut [u8]) -> io_std::Result<()> {
        while !buf.is_empty() {
            let read = self.read_at(offset, buf)?;
            if read == 0 {
                return Err(io_std::Error::new(
                    io_std::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }

            offset += read as u64;
            buf = &mut buf[read..];
        }

        Ok(())
    }

    #[cfg(unix)]
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> io_std::Result<usize> {
        self.file_ref()?.read_at(buf, offset)
    }

    #[cfg(windows)]
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> io_std::Result<usize> {
        self.file_ref()?.seek_read(buf, offset)
    }

    pub fn transfer(&mut self, read_pos: u64, write_pos: u64, count: u64) -> Result<()> {
        if write_pos > read_pos && write_pos < read_pos + count {
            return Err(crate::error::Error::Io {
                source: io_std::Error::new(
                    io_std::ErrorKind::InvalidInput,
                    "overlapping transfer ranges not supported",
                ),
            });
        }

        let file = self.file.as_mut().ok_or_else(|| crate::error::Error::Io {
            source: io_std::Error::new(io_std::ErrorKind::InvalidInput, "no file"),
        })?;

        let mut bytes_left = count as i64;
        let mut current_read = read_pos;
        let mut current_write = write_pos;

        while bytes_left > 0 {
            let bytes_to_read = min(bytes_left as usize, Self::TRANSFER_BUFFER_SIZE);
            let slice = &mut self.transfer_buf[..bytes_to_read];

            #[cfg(unix)]
            {
                file.read_exact_at(slice, current_read)?;
                file.write_all_at(slice, current_write)?;
            }

            #[cfg(windows)]
            {
                file.seek(SeekFrom::Start(current_read))?;
                file.read_exact(slice)?;

                file.seek(SeekFrom::Start(current_write))?;
                file.write_all(slice)?;
            }

            current_read += bytes_to_read as u64;
            current_write += bytes_to_read as u64;
            bytes_left -= bytes_to_read as i64;
        }

        if self.sync_writes {
            file.sync_data()?;
        }

        self.expected_seek = current_write;
        self.last_seek = Some(current_write);

        Ok(())
    }

    pub fn sync_set_len(&mut self, new_len: u64) -> io_std::Result<()> {
        self.file_mut()?.set_len(new_len)?;
        self.file_len = new_len;
        self.file_mut()?.sync_all()
    }

    pub fn write_zeroes(&mut self, len: u64) -> Result<()> {
        struct InnerWriter<'a>(&'a mut QueueFileInner);

        impl Write for InnerWriter<'_> {
            fn write(&mut self, buf: &[u8]) -> io_std::Result<usize> {
                self.0
                    .write(buf)
                    .map_err(|e| io_std::Error::new(io_std::ErrorKind::Other, e.to_string()))?;
                Ok(buf.len())
            }

            fn flush(&mut self) -> io_std::Result<()> {
                Ok(())
            }
        }

        io_std::copy(&mut io_std::repeat(0).take(len), &mut InnerWriter(self))?;
        Ok(())
    }

    pub fn write_zero_chunks(&mut self, pos: u64, len: usize) -> Result<()> {
        self.seek(pos);
        self.write_zeroes(len as u64)
    }

    #[inline]
    pub fn with_batched_backlink_rewrite_sync<T>(
        &mut self, f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        self.with_deferred_sync(DeferredSyncPhase::BacklinkRewrite, f)
    }

    #[inline]
    pub fn with_batched_clear_erase_sync<T>(
        &mut self, f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        self.with_deferred_sync(DeferredSyncPhase::ClearErase, f)
    }

    #[inline]
    pub fn with_batched_expansion_copy_sync<T>(
        &mut self, f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        self.with_deferred_sync(DeferredSyncPhase::ExpansionCopy, f)
    }

    pub fn with_deferred_sync<T>(
        &mut self, phase: DeferredSyncPhase, f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        let sync_writes = self.sync_writes;
        let deferred_sync_phase = self.deferred_sync_phase;
        self.sync_writes = false;
        self.deferred_sync_phase = Some(phase);

        let result = f(self);

        self.deferred_sync_phase = deferred_sync_phase;
        self.sync_writes = sync_writes;

        let value = result?;

        if sync_writes {
            self.file_mut()?.sync_data()?;
            maybe_inject_failpoint(match phase {
                DeferredSyncPhase::ExpansionCopy => "v2_after_expansion_copy_flush",
                DeferredSyncPhase::ClearErase => "clear_after_erase_flush",
                DeferredSyncPhase::BacklinkRewrite => "v2_after_backlink_rewrite_flush",
                DeferredSyncPhase::AppendBatch => "v2_after_add_batch_flush",
            })?;
        }

        Ok(value)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DataRing<'a> {
    inner: &'a QueueFileInner,
    pub data_start: u64,
}

impl<'a> DataRing<'a> {
    #[inline]
    pub const fn new(inner: &'a QueueFileInner, data_start: u64) -> Self {
        Self { inner, data_start }
    }

    #[inline]
    pub const fn capacity(&self) -> u64 {
        self.inner.file_len - self.data_start
    }

    #[inline]
    #[allow(dead_code)]
    pub const fn phys_pos(&self, logical_pos: u64) -> u64 {
        self.data_start + (logical_pos % self.capacity())
    }

    #[inline]
    pub const fn add(&self, logical_pos: u64, delta: u64) -> u64 {
        (logical_pos + delta) % self.capacity()
    }

    #[inline]
    pub const fn distance(&self, from: u64, to: u64) -> u64 {
        let cap = self.capacity();
        (to + cap - from) % cap
    }

    pub fn read_at(&self, logical_pos: u64, buf: &mut [u8]) -> io_std::Result<()> {
        let cap = self.capacity();
        let mut pos = logical_pos % cap;
        let mut bytes_read = 0;
        let mut remaining = buf.len();

        while remaining > 0 {
            let phys = self.data_start + pos;
            let can_read = min(remaining as u64, cap - pos) as usize;
            self.inner.read_exact_at(phys, &mut buf[bytes_read..bytes_read + can_read])?;

            bytes_read += can_read;
            remaining -= can_read;
            pos = 0;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct DataRingMut<'a> {
    pub inner: &'a mut QueueFileInner,
    pub data_start: u64,
}

impl<'a> DataRingMut<'a> {
    #[inline]
    pub fn new(inner: &'a mut QueueFileInner, data_start: u64) -> Self {
        Self { inner, data_start }
    }

    #[inline]
    pub fn as_read_only(&self) -> DataRing<'_> {
        DataRing { inner: self.inner, data_start: self.data_start }
    }

    #[inline]
    pub fn capacity(&self) -> u64 {
        self.inner.file_len - self.data_start
    }

    #[inline]
    pub fn add(&self, logical_pos: u64, delta: u64) -> u64 {
        (logical_pos + delta) % self.capacity()
    }

    pub fn write_at(&mut self, logical_pos: u64, buf: &[u8]) -> Result<()> {
        let cap = self.capacity();
        let mut pos = logical_pos % cap;
        let mut bytes_written = 0;
        let mut remaining = buf.len();

        while remaining > 0 {
            let phys = self.data_start + pos;
            let can_write = min(remaining as u64, cap - pos) as usize;
            self.inner.seek(phys);
            self.inner.write(&buf[bytes_written..bytes_written + can_write])?;

            bytes_written += can_write;
            remaining -= can_write;
            pos = 0;
        }

        Ok(())
    }

    pub fn relocate(&mut self, orig_file_len: u64, moved_count: u64) -> Result<()> {
        if moved_count == 0 {
            return Ok(());
        }

        let data_start = self.data_start;
        self.inner.with_batched_expansion_copy_sync(|inner| {
            inner.transfer(data_start, orig_file_len, moved_count)
        })
    }
}
