//! Queue file initialization and opening.
//!
//! This module handles the creation, detection, and opening of queue files
//! in various formats (legacy, v1, and v2), including format migration.

use std::fs::{File, OpenOptions, rename};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::format::{
    FormatState, LegacyHeaderState, parse_legacy_header, parse_versioned_header, read_v2_open_state,
};
use crate::header::{
    SlotData, V2_INITIAL_LEN, V2_MAGIC, V2_SLOT_A_OFFSET, V2_SLOT_B_OFFSET, V2_SLOT_LEN,
    build_slot_bytes,
};
use crate::qio::QueueFileInner;
use crate::{QueueFile, Result, ensure};

/// Initializes a new queue file at the given path.
pub fn init(path: &Path, force_legacy: bool, capacity: u64) -> Result<()> {
    let tmp_path = path.with_extension(".tmp");

    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;

        if force_legacy {
            init_legacy_file(&mut file, capacity)?;
        } else {
            init_v2_file(&mut file)?;
        }
    }

    rename(tmp_path, path)?;
    Ok(())
}

/// Initializes a new legacy-format queue file.
pub fn init_legacy_file(file: &mut File, capacity: u64) -> Result<()> {
    file.set_len(capacity)?;
    file.write_all(&(capacity as u32).to_be_bytes())?;
    Ok(())
}

/// Initializes a new V2-format queue file.
pub fn init_v2_file(file: &mut File) -> Result<()> {
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

    file.seek(SeekFrom::Start(V2_SLOT_A_OFFSET))?;
    file.write_all(&build_slot_bytes(&slot_a))?;

    file.seek(SeekFrom::Start(V2_SLOT_B_OFFSET))?;
    file.write_all(&build_slot_bytes(&slot_b))?;

    Ok(())
}

/// Detects whether a file uses the V2 format by checking for the V2 magic number.
pub fn detect_v2_magic(file: &mut File, real_file_len: u64, force_legacy: bool) -> Result<bool> {
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

/// Parses either a legacy (16-byte) or v1 (32-byte) header from a file.
pub fn parse_legacy_or_v1_header(
    file: &mut File, real_file_len: u64, force_legacy: bool,
) -> Result<LegacyHeaderState> {
    let mut buf = [0u8; 32];
    file.seek(SeekFrom::Start(0))?;
    let bytes_read = file.read(&mut buf)?;
    ensure!(bytes_read >= 32, OutOfBounds { msg: "file too short".to_string() });

    let versioned = !force_legacy && (buf[0] & 0x80) != 0;
    let (format, parse): (_, fn(&mut &[u8]) -> Result<_>) = if versioned {
        (FormatState::V1, parse_versioned_header)
    } else {
        (FormatState::Legacy, parse_legacy_header)
    };
    let (file_len, elem_cnt, first_pos, last_pos) = parse(&mut &buf[..])?;
    let header_len = format.data_start();

    ensure!(file_len <= real_file_len, OutOfBounds {
        msg: format!(
            "file is truncated. expected length was {file_len} but actual length is {real_file_len}"
        )
    });
    ensure!(file_len >= header_len, InvalidValue {
        msg: format!("length stored in header ({file_len}) is invalid")
    });
    ensure!(first_pos <= file_len, OutOfBounds {
        msg: format!("position of the first element ({first_pos}) is beyond the file")
    });
    ensure!(last_pos <= file_len, OutOfBounds {
        msg: format!("position of the last element ({last_pos}) is beyond the file")
    });

    Ok(LegacyHeaderState { format, file_len, elem_cnt, first_pos, last_pos })
}

pub fn open_internal_full<P: AsRef<Path>>(
    path: P, overwrite_on_remove: bool, force_legacy: bool, capacity: u64,
    mut allow_migration: bool,
) -> Result<QueueFile> {
    let path = path.as_ref();

    let min_len = if force_legacy { QueueFile::INITIAL_LENGTH } else { V2_INITIAL_LEN };

    loop {
        if !path.exists() {
            init(path, force_legacy, capacity.max(min_len))?;
        }

        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let real_file_len = file.metadata()?.len();

        if detect_v2_magic(&mut file, real_file_len, force_legacy)? {
            return open_v2(file, real_file_len, capacity, overwrite_on_remove);
        }

        let state = parse_legacy_or_v1_header(&mut file, real_file_len, force_legacy)?;

        if allow_migration && !force_legacy {
            drop(file);
            migrate_to_v2(path)?;
            allow_migration = false;
            continue;
        }

        return QueueFile::build_legacy_queue_file(file, state, capacity, overwrite_on_remove);
    }
}

/// Opens a V2-format queue file.
pub fn open_v2(
    file: File, real_file_len: u64, capacity: u64, overwrite_on_remove: bool,
) -> Result<QueueFile> {
    let inner = QueueFileInner {
        file: Some(file),
        file_len: real_file_len,
        expected_seek: 0,
        last_seek: None,
        transfer_buf: vec![0u8; QueueFileInner::TRANSFER_BUFFER_SIZE].into_boxed_slice(),
        sync_writes: cfg!(not(test)),
        deferred_sync_phase: None,
    };

    let open_state = read_v2_open_state(&inner, real_file_len)?;
    let mut qf = QueueFile::build_v2_queue_file(inner, open_state, capacity, overwrite_on_remove);
    qf.initialize_v2_endpoints(open_state.slot)?;

    if open_state.slot.file_length < qf.capacity {
        qf.inner.sync_set_len(qf.capacity)?;
    }

    Ok(qf)
}

/// Migrates a legacy or v1 queue file to v2 format.
///
/// Creates a new v2 file, copies all elements, and atomically replaces
/// the original file.
pub fn migrate_to_v2(path: &Path) -> Result<()> {
    use std::fs;

    let tmp = path.with_extension("v2tmp");

    let result = (|| -> Result<()> {
        let src = open_internal_full(path, true, false, QueueFile::INITIAL_LENGTH, false)?;

        init(&tmp, false, V2_INITIAL_LEN)?;
        let mut dst =
            open_internal_full(&tmp, src.overwrite_on_remove(), false, V2_INITIAL_LEN, false)?;
        dst.set_sync_writes(false);

        let mut src_iter = src.iter();
        while let Some(elem) = src_iter.borrowed_next() {
            dst.add(elem)?;
        }
        drop(src_iter);

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
