//! V2 file format header structures and utilities.
//!
//! This module provides constants and functions for working with the V2
//! queue file format, which uses a dual-slot header design for atomic
//! commits and includes CRC-32 integrity checks.
//!
//! # V2 Header Layout
//!
//! The V2 format uses two 56-byte header slots at offsets 0 and 4096,
//! allowing for atomic commit switching between slots.

use bytes::{Buf, BufMut};

/// The V2 magic number: `0x51464D48` (ASCII "QFMH")
pub const V2_MAGIC: u32 = 0x5146_4D48;
/// Offset of the first header slot (slot A) from the start of the file.
pub const V2_SLOT_A_OFFSET: u64 = 0;
/// Offset of the second header slot (slot B) from the start of the file.
pub const V2_SLOT_B_OFFSET: u64 = 4096;
/// Size of each V2 header slot in bytes.
pub const V2_SLOT_LEN: usize = 56;
/// Offset where queue data begins in V2 format files.
pub const V2_DATA_START: u64 = 8192;
/// Initial file length for newly created V2 format files.
pub const V2_INITIAL_LEN: u64 = 8192;
/// The V1/versioned header magic number.
pub const VERSIONED_HEADER: u32 = 0x8000_0001;

/// Represents which header slot is currently active.
///
/// V2 format uses two slots for atomic commits. The active slot is toggled
/// after each write to provide durability guarantees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderSlot {
    /// The first header slot (at offset 0).
    A,
    /// The second header slot (at offset 4096).
    B,
}

impl HeaderSlot {
    /// Returns the other slot, toggling between A and B.
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

/// The parsed contents of a V2 header slot.
///
/// This structure holds all the metadata stored in each 56-byte header slot,
/// including file length, element count, positions, generation, and sequence numbers.
#[derive(Debug, Clone, Copy)]
pub struct SlotData {
    /// The current file length as recorded in the header.
    pub file_length: u64,
    /// The number of elements currently in the queue.
    pub element_count: u32,
    /// The physical position of the first element.
    pub first_position: u64,
    /// The physical position of the last element.
    pub last_position: u64,
    /// A monotonically increasing generation counter for this slot.
    pub generation: u64,
    /// The next sequence number to assign to a new element.
    pub next_sequence_number: u64,
}

/// Computes the CRC-32 hash of the given data.
#[inline]
pub fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// Computes the CRC-32 for a V2 header slot (excluding the 4-byte CRC field).
#[inline]
pub fn compute_slot_crc(slot_bytes: &[u8; V2_SLOT_LEN]) -> u32 {
    crc32(&slot_bytes[..52])
}

/// Serializes a [`SlotData`] into a 56-byte slot for writing to disk.
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

/// Parses a V2 header slot from raw bytes.
///
/// Returns [`None`] if the magic number is incorrect, version is not 2,
/// or the CRC validation fails.
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
