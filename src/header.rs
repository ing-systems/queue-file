use bytes::{Buf, BufMut};

pub const V2_MAGIC: u32 = 0x5146_4D48;
pub const V2_SLOT_A_OFFSET: u64 = 0;
pub const V2_SLOT_B_OFFSET: u64 = 4096;
pub const V2_SLOT_LEN: usize = 56;
pub const V2_DATA_START: u64 = 8192;
pub const V2_INITIAL_LEN: u64 = 8192;
pub const VERSIONED_HEADER: u32 = 0x8000_0001;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderSlot {
    A,
    B,
}

impl HeaderSlot {
    #[inline]
    pub const fn toggle(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }

    #[inline]
    pub const fn offset(self) -> u64 {
        match self {
            Self::A => V2_SLOT_A_OFFSET,
            Self::B => V2_SLOT_B_OFFSET,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SlotData {
    pub file_length: u64,
    pub element_count: u32,
    pub first_position: u64,
    pub last_position: u64,
    pub generation: u64,
    pub next_sequence_number: u64,
}

#[inline]
pub fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

#[inline]
pub fn compute_slot_crc(slot_bytes: &[u8; V2_SLOT_LEN]) -> u32 {
    crc32(&slot_bytes[..52])
}

pub fn build_slot_bytes(data: &SlotData) -> [u8; V2_SLOT_LEN] {
    let mut buf = [0u8; V2_SLOT_LEN];
    {
        let mut w: &mut [u8] = &mut buf;
        w.put_u32(V2_MAGIC);
        w.put_u8(2);
        w.put_u8(0);
        w.put_u8(0);
        w.put_u8(0);
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

pub fn parse_slot(bytes: &[u8; V2_SLOT_LEN]) -> Option<SlotData> {
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
