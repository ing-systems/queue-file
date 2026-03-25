use bytes::{BufMut, BytesMut};
use std::fs::{File, OpenOptions, rename};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::{
    QueueFile, Result, ensure,
    format::{
        FormatState, LegacyFormat, LegacyHeaderState, V1Format, parse_legacy_header,
        parse_versioned_header, read_v2_open_state,
    },
    header::{
        SlotData, V2_INITIAL_LEN, V2_MAGIC, V2_SLOT_A_OFFSET, V2_SLOT_B_OFFSET, V2_SLOT_LEN,
        build_slot_bytes,
    },
    qio::QueueFileInner,
};

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

pub fn init_legacy_file(file: &mut File, capacity: u64) -> Result<()> {
    file.set_len(capacity)?;
    let mut buf = BytesMut::with_capacity(16);
    buf.put_u32(capacity as u32);
    file.write_all(buf.as_ref())?;
    Ok(())
}

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

pub fn ensure_queue_file_exists(path: &Path, force_legacy: bool, capacity: u64) -> Result<()> {
    if !path.exists() {
        init(
            path,
            force_legacy,
            capacity.max(if force_legacy { QueueFile::INITIAL_LENGTH } else { V2_INITIAL_LEN }),
        )?;
    }

    Ok(())
}

pub fn parse_legacy_or_v1_header(
    file: &mut File, real_file_len: u64, force_legacy: bool,
) -> Result<LegacyHeaderState> {
    let mut buf = [0u8; 32];
    file.seek(SeekFrom::Start(0))?;
    let bytes_read = file.read(&mut buf)?;
    ensure!(bytes_read >= 32, OutOfBounds { msg: "file too short".to_string() });

    let versioned = !force_legacy && (buf[0] & 0x80) != 0;
    let mut buf = BytesMut::from(&buf[..]);

    let (format, header_len, file_len, elem_cnt, first_pos, last_pos) = if versioned {
        let (file_len, elem_cnt, first_pos, last_pos) = parse_versioned_header(&mut buf)?;
        (FormatState::V1(V1Format), 32u64, file_len, elem_cnt, first_pos, last_pos)
    } else {
        let (file_len, elem_cnt, first_pos, last_pos) = parse_legacy_header(&mut buf)?;
        (FormatState::Legacy(LegacyFormat), 16u64, file_len, elem_cnt, first_pos, last_pos)
    };

    ensure!(
        file_len <= real_file_len,
        OutOfBounds {
            msg: format!(
                "file is truncated. expected length was {file_len} but actual length is {real_file_len}"
            )
        }
    );
    ensure!(
        file_len >= header_len,
        InvalidValue { msg: format!("length stored in header ({file_len}) is invalid") }
    );
    ensure!(
        first_pos <= file_len,
        OutOfBounds {
            msg: format!("position of the first element ({first_pos}) is beyond the file")
        }
    );
    ensure!(
        last_pos <= file_len,
        OutOfBounds {
            msg: format!("position of the last element ({last_pos}) is beyond the file")
        }
    );

    let _ = header_len;

    Ok(LegacyHeaderState { format, file_len, elem_cnt, first_pos, last_pos })
}

pub fn open_internal_full<P: AsRef<Path>>(
    path: P, overwrite_on_remove: bool, force_legacy: bool, capacity: u64,
    mut allow_migration: bool,
) -> Result<QueueFile> {
    let path = path.as_ref();

    loop {
        ensure_queue_file_exists(path, force_legacy, capacity)?;

        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let real_file_len = file.metadata()?.len();

        if detect_v2_magic(&mut file, real_file_len, force_legacy)? {
            return open_v2(file, real_file_len, capacity, overwrite_on_remove, path);
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

pub fn open_v2(
    file: File, real_file_len: u64, capacity: u64, overwrite_on_remove: bool, _path: &Path,
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

pub fn migrate_to_v2(path: &Path) -> Result<()> {
    use std::fs;

    let tmp = path.with_extension("v2tmp");

    let result = (|| -> Result<()> {
        let src = open_internal_full(path, true, false, QueueFile::INITIAL_LENGTH, false)?;

        init(&tmp, false, V2_INITIAL_LEN)?;
        let mut dst =
            open_internal_full(&tmp, src.overwrite_on_remove(), false, V2_INITIAL_LEN, false)?;
        dst.set_sync_writes(false);

        {
            let mut src_iter = src.iter();
            while let Some(elem) = src_iter.borrowed_next() {
                let owned: Vec<u8> = elem.to_vec();
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
