//! File format abstractions for different queue versions.
//!
//! [`FormatState`] records which on-disk format (legacy, v1, or v2) a queue uses and implements
//! the format-specific logic for headers and element framing. This module also holds the
//! supporting structs for element headers, metadata, and expansion plans.

use bytes::{Buf, BufMut};

use crate::element::Element;
use crate::ensure;
use crate::error::{Error, Result, maybe_inject_failpoint};
use crate::header::{
    HeaderSlot, SlotData, V2_DATA_START, V2_SLOT_A_OFFSET, V2_SLOT_B_OFFSET, V2_SLOT_LEN,
    VERSIONED_HEADER, build_slot_bytes, crc32, parse_slot,
};
use crate::qio::{DataRing, DataRingMut, DeferredSyncPhase, QueueFileInner};

/// Magic number for V2 element headers: `0x49544844` (ASCII "ITHD")
pub const V2_ELEM_HDR_MAGIC: u32 = 0x4954_4844;
/// Size of V2 element header (including magic and CRC).
pub const V2_ELEM_HDR_LEN: usize = 28;
/// Magic number for V2 element footers: `0x49544654` (ASCII "ITFT")
pub const V2_ELEM_FTR_MAGIC: u32 = 0x4954_4654;
/// Size of V2 element footer (including magic, seq, and CRC).
pub const V2_ELEM_FTR_LEN: usize = 16;
/// Total overhead per element in V2 format (header + footer).
pub const V2_ELEM_OVERHEAD: u64 = 44;

/// Metadata stored in queue file headers.
#[derive(Debug, Clone, Copy)]
pub struct QueueMetadata {
    /// Current file length.
    pub file_len: u64,
    /// Number of elements in queue.
    pub elem_cnt: usize,
    /// Position of first element.
    pub first_pos: u64,
    /// Position of last element.
    pub last_pos: u64,
}

/// State from parsing a legacy or v1 header.
#[derive(Debug, Clone, Copy)]
pub struct LegacyHeaderState {
    /// The detected format version.
    pub format: FormatState,
    /// File length from header.
    pub file_len: u64,
    /// Element count from header.
    pub elem_cnt: usize,
    /// First element position.
    pub first_pos: u64,
    /// Last element position.
    pub last_pos: u64,
}

/// State from parsing a v2 header.
#[derive(Debug, Clone, Copy)]
pub struct V2OpenState {
    /// Which slot (A or B) is currently active.
    pub active_slot: HeaderSlot,
    /// The parsed slot data.
    pub slot: SlotData,
    /// Number of elements.
    pub elem_cnt: usize,
}

/// V2 element header information.
#[derive(Debug, Clone, Copy)]
pub struct V2ElementHeader {
    /// Length of the element payload.
    pub payload_len: usize,
    /// Sequence number.
    pub seq: u64,
    /// Position of previous element (for backlink).
    pub prev_pos: u64,
}

/// Plan for file expansion operations.
#[derive(Debug, Clone, Copy)]
pub struct ExpansionPlan {
    /// Original file length before expansion.
    pub orig_file_len: u64,
    /// New file length after expansion.
    pub new_len: u64,
    /// Physical position of end of last element.
    pub end_of_last_elem: u64,
    /// Whether the queue data wraps around in the ring buffer.
    pub wraps: bool,
    /// Number of bytes that need to be moved.
    pub moved_count: u64,
}

/// V2 format state: dual-slot headers, CRC-32 integrity, and sequence numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V2Format {
    /// Currently active header slot.
    pub active_slot: HeaderSlot,
    /// Generation counter for this format instance.
    pub generation: u64,
    /// Next sequence number to assign.
    pub next_seq: u64,
}

/// The on-disk format of a queue file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatState {
    /// 16-byte header, 4-byte element length. Binary-compatible with the original Java
    /// `QueueFile`.
    Legacy,
    /// 32-byte versioned header, supports files up to `i64::MAX`.
    V1,
    /// Dual-slot headers, CRC-32 integrity, and sequence numbers.
    V2(V2Format),
}

impl FormatState {
    #[inline]
    pub const fn data_start(&self) -> u64 {
        match self {
            Self::Legacy => 16,
            Self::V1 => 32,
            Self::V2(_) => V2_DATA_START,
        }
    }

    #[inline]
    pub const fn elem_hdr_len(&self) -> u64 {
        match self {
            Self::Legacy | Self::V1 => Element::HEADER_LENGTH as u64,
            Self::V2(_) => V2_ELEM_HDR_LEN as u64,
        }
    }

    #[inline]
    pub const fn elem_span(&self, payload_len: usize) -> u64 {
        let overhead = match self {
            Self::Legacy | Self::V1 => Element::HEADER_LENGTH as u64,
            Self::V2(_) => V2_ELEM_OVERHEAD,
        };
        overhead + payload_len as u64
    }

    #[inline]
    pub const fn next_seq(&self) -> u64 {
        match self {
            Self::Legacy | Self::V1 => 0,
            Self::V2(f) => f.next_seq,
        }
    }

    #[inline]
    pub fn set_next_seq(&mut self, seq: u64) {
        if let Self::V2(f) = self {
            f.next_seq = seq;
        }
    }

    pub fn commit_header(
        &mut self, inner: &mut QueueFileInner, metadata: QueueMetadata,
    ) -> Result<()> {
        let ds = self.data_start();
        let QueueMetadata { file_len, elem_cnt, first_pos, last_pos } = metadata;
        let (first_phys, last_phys) =
            if elem_cnt == 0 { (0, 0) } else { (ds + first_pos, ds + last_pos) };

        match self {
            Self::Legacy => {
                let bytes = encode_legacy_header(file_len, elem_cnt, first_phys, last_phys)?;
                inner.seek(0);
                inner.write(&bytes)
            }
            Self::V1 => {
                let bytes = encode_v1_header(file_len, elem_cnt, first_phys, last_phys)?;
                inner.seek(0);
                inner.write(&bytes)
            }
            Self::V2(f) => {
                let next_slot = f.active_slot.toggle();
                let slot_bytes = encode_v2_slot(
                    file_len,
                    elem_cnt,
                    first_phys,
                    last_phys,
                    f.generation + 1,
                    f.next_seq,
                )?;

                inner.seek(next_slot.offset());
                inner.write(&slot_bytes)?;

                f.generation += 1;
                f.active_slot = next_slot;
                Ok(())
            }
        }
    }

    pub fn read_element(&self, ring: &DataRing<'_>, logical_pos: u64) -> Result<Element> {
        match self {
            Self::Legacy | Self::V1 => {
                let mut buf = [0u8; Element::HEADER_LENGTH];
                ring.read_at(logical_pos, &mut buf)?;
                Element::new(logical_pos, u32::from_be_bytes(buf) as usize, 0)
            }
            Self::V2(_) => {
                let header = read_v2_element_header(ring, logical_pos)?;
                Ok(Element { pos: logical_pos, len: header.payload_len, seq: header.seq })
            }
        }
    }

    /// Appends the on-disk encoding of an element to `out`.
    ///
    /// `prev_logical_pos` is the logical position of the preceding element, if any (v2 only).
    pub fn encode_element(
        &self, out: &mut Vec<u8>, payload: &[u8], seq: u64, prev_logical_pos: Option<u64>,
    ) {
        match self {
            Self::Legacy | Self::V1 => {
                out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
                out.extend_from_slice(payload);
            }
            Self::V2(_) => {
                let prev_phys = prev_logical_pos.map_or(0, |p| V2_DATA_START + p);
                out.extend_from_slice(&encode_v2_element_header(seq, prev_phys, payload.len()));
                out.extend_from_slice(payload);
                out.extend_from_slice(&encode_v2_footer(seq, payload));
            }
        }
    }

    pub fn validate_footer(
        &self, ring: &DataRing<'_>, payload_start: u64, elem: &Element, payload: &[u8],
    ) -> Result<()> {
        match self {
            Self::Legacy | Self::V1 => Ok(()),
            Self::V2(_) => {
                let footer_pos = ring.add(payload_start, elem.len as u64);
                validate_v2_footer(ring, footer_pos, elem.seq, payload)
            }
        }
    }

    /// Fixes up element metadata after wrapped data has been relocated during expansion.
    ///
    /// Returns the new logical position of the last element if it moved.
    pub fn on_expansion(
        &self, ring: &mut DataRingMut<'_>, plan: &ExpansionPlan, first: Element, last: Element,
        elem_cnt: usize,
    ) -> Result<Option<u64>> {
        if let Self::V2(_) = self {
            rewrite_v2_backlinks_after_expansion(ring, plan, first, elem_cnt)?;
        }

        // The wrapped data at logical [0, moved_count) was copied to
        // logical [orig_capacity, orig_capacity + moved_count). `last.pos`
        // falls in that range, so its new logical position is
        // `last.pos + orig_capacity`, where
        // `orig_capacity = orig_file_len - data_start`.
        Ok((last.pos < first.pos).then(|| plan.orig_file_len - ring.data_start + last.pos))
    }

    pub fn on_expansion_cleanup(&self, plan: &ExpansionPlan) -> Result<()> {
        if matches!(self, Self::V2(_)) && plan.wraps {
            maybe_inject_failpoint("v2_after_relocation_cleanup_before_add")?;
        }
        Ok(())
    }

    pub fn validate_v2_element_header(
        &self, ring: &DataRing<'_>, logical_pos: u64,
    ) -> Result<V2ElementHeader> {
        match self {
            Self::V2(_) => read_v2_element_header(ring, logical_pos),
            _ => Err(Error::UnsupportedVersion { detected: 0, supported: 2 }),
        }
    }
}

// ── V2 element framing ────────────────────────────────────────────────────────

fn read_v2_element_header(ring: &DataRing<'_>, logical_pos: u64) -> Result<V2ElementHeader> {
    let mut hdr = [0u8; V2_ELEM_HDR_LEN];
    ring.read_at(logical_pos, &mut hdr)?;
    parse_v2_element_header(&hdr, logical_pos, ring.capacity())
}

fn parse_v2_element_header(
    hdr: &[u8; V2_ELEM_HDR_LEN], logical_pos: u64, capacity: u64,
) -> Result<V2ElementHeader> {
    let mut r: &[u8] = hdr;
    let magic = r.get_u32();
    let seq = r.get_u64();
    let prev_pos = r.get_u64();
    let payload_len_raw = r.get_i32();
    let stored_crc = r.get_u32();

    ensure!(magic == V2_ELEM_HDR_MAGIC, Error::CorruptedFile {
        msg: format!(
            "v2 element header magic mismatch at logical pos {logical_pos}: {magic:#010x}"
        )
    });
    ensure!(payload_len_raw >= 0, Error::CorruptedFile {
        msg: format!(
            "v2 element payload_len {payload_len_raw} is negative at logical pos {logical_pos}"
        )
    });
    let payload_len = payload_len_raw as usize;
    ensure!(seq >= 1, Error::CorruptedFile {
        msg: format!("v2 element seq {seq} < 1 at logical pos {logical_pos}")
    });
    ensure!(stored_crc == crc32(&hdr[..24]), Error::CorruptedFile {
        msg: format!("v2 element header CRC mismatch at logical pos {logical_pos}")
    });

    let span = V2_ELEM_OVERHEAD + payload_len as u64;
    ensure!(span <= capacity, Error::CorruptedFile {
        msg: format!("v2 element span {span} exceeds capacity")
    });

    Ok(V2ElementHeader { payload_len, seq, prev_pos })
}

fn encode_v2_element_header(seq: u64, prev_phys: u64, payload_len: usize) -> [u8; V2_ELEM_HDR_LEN] {
    let mut hdr = [0u8; V2_ELEM_HDR_LEN];
    let mut w: &mut [u8] = &mut hdr;
    w.put_u32(V2_ELEM_HDR_MAGIC);
    w.put_u64(seq);
    w.put_u64(prev_phys);
    w.put_i32(payload_len as i32);
    let crc = crc32(&hdr[..24]);
    hdr[24..].copy_from_slice(&crc.to_be_bytes());
    hdr
}

fn encode_v2_footer(seq: u64, payload: &[u8]) -> [u8; V2_ELEM_FTR_LEN] {
    let mut ftr = [0u8; V2_ELEM_FTR_LEN];
    ftr[..4].copy_from_slice(&V2_ELEM_FTR_MAGIC.to_be_bytes());
    ftr[4..12].copy_from_slice(&seq.to_be_bytes());
    let crc = compute_elem_footer_crc(payload, &ftr[..12]);
    ftr[12..].copy_from_slice(&crc.to_be_bytes());
    ftr
}

fn validate_v2_footer(
    ring: &DataRing<'_>, footer_pos: u64, seq: u64, payload: &[u8],
) -> Result<()> {
    let mut ftr = [0u8; V2_ELEM_FTR_LEN];
    ring.read_at(footer_pos, &mut ftr)?;

    let mut r: &[u8] = &ftr;
    let ftr_magic = r.get_u32();
    let ftr_seq = r.get_u64();
    let stored_crc = r.get_u32();

    ensure!(ftr_magic == V2_ELEM_FTR_MAGIC, Error::CorruptedFile {
        msg: format!(
            "v2 element footer magic mismatch at logical pos {footer_pos}: {ftr_magic:#010x}"
        )
    });
    ensure!(ftr_seq == seq, Error::CorruptedFile {
        msg: format!("v2 footer seq {ftr_seq} != element seq {seq}")
    });
    ensure!(stored_crc == compute_elem_footer_crc(payload, &ftr[..12]), Error::CorruptedFile {
        msg: "v2 element footer CRC mismatch".to_string()
    });

    Ok(())
}

/// Rewrites the backlinks of elements whose predecessor was relocated by a wrapped expansion.
///
/// Walks the queue from `first` in a single pass, reading each element header once.
fn rewrite_v2_backlinks_after_expansion(
    ring: &mut DataRingMut<'_>, plan: &ExpansionPlan, first: Element, elem_cnt: usize,
) -> Result<()> {
    let data_start = ring.data_start;
    let moved_offset = plan.orig_file_len - data_start;
    let boundary = plan.end_of_last_elem - data_start;

    ring.inner.with_deferred_sync(DeferredSyncPhase::BacklinkRewrite, |inner| {
        let mut ring = DataRingMut::new(inner, data_start);
        let mut pos = first.pos;

        for _ in 0..elem_cnt {
            let mut hdr = [0u8; V2_ELEM_HDR_LEN];
            ring.as_read_only().read_at(pos, &mut hdr)?;
            let header = parse_v2_element_header(&hdr, pos, ring.capacity())?;

            if header.prev_pos < boundary {
                let patched = encode_v2_element_header(
                    header.seq,
                    header.prev_pos + moved_offset,
                    header.payload_len,
                );
                ring.write_at(pos, &patched)?;
            }

            pos = ring.add(pos, V2_ELEM_OVERHEAD + header.payload_len as u64);
        }

        Ok(())
    })
}

#[inline]
fn compute_elem_footer_crc(payload: &[u8], ftr_bytes_0_to_11: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(payload);
    hasher.update(ftr_bytes_0_to_11);
    hasher.finalize()
}

// ── Header encoding / parsing ─────────────────────────────────────────────────

fn check_i32(value: u64, msg: &str) -> Result<()> {
    ensure!(i32::try_from(value).is_ok(), Error::CorruptedFile { msg: msg.to_string() });
    Ok(())
}

fn check_i64(value: u64, msg: &str) -> Result<()> {
    ensure!(i64::try_from(value).is_ok(), Error::CorruptedFile { msg: msg.to_string() });
    Ok(())
}

fn encode_legacy_header(
    file_len: u64, elem_cnt: usize, first_phys: u64, last_phys: u64,
) -> Result<[u8; 16]> {
    check_i32(file_len, "file length in header will exceed i32::MAX")?;
    check_i32(elem_cnt as u64, "element count in header will exceed i32::MAX")?;
    check_i32(first_phys, "first element position in header will exceed i32::MAX")?;
    check_i32(last_phys, "last element position in header will exceed i32::MAX")?;

    let mut header = [0u8; 16];
    let mut w: &mut [u8] = &mut header;
    w.put_i32(file_len as i32);
    w.put_i32(elem_cnt as i32);
    w.put_i32(first_phys as i32);
    w.put_i32(last_phys as i32);

    Ok(header)
}

fn encode_v1_header(
    file_len: u64, elem_cnt: usize, first_phys: u64, last_phys: u64,
) -> Result<[u8; 32]> {
    check_i64(file_len, "file length in header will exceed i64::MAX")?;
    check_i32(elem_cnt as u64, "element count in header will exceed i32::MAX")?;
    check_i64(first_phys, "first element position in header will exceed i64::MAX")?;
    check_i64(last_phys, "last element position in header will exceed i64::MAX")?;

    let mut header = [0u8; 32];
    let mut w: &mut [u8] = &mut header;
    w.put_u32(VERSIONED_HEADER);
    w.put_u64(file_len);
    w.put_i32(elem_cnt as i32);
    w.put_u64(first_phys);
    w.put_u64(last_phys);

    Ok(header)
}

fn encode_v2_slot(
    file_len: u64, elem_cnt: usize, first_phys: u64, last_phys: u64, generation: u64, next_seq: u64,
) -> Result<[u8; V2_SLOT_LEN]> {
    check_i64(file_len, "file length in V2 header will exceed i64::MAX")?;
    ensure!(u32::try_from(elem_cnt).is_ok(), Error::CorruptedFile {
        msg: "element count in V2 header will exceed u32::MAX".to_string()
    });
    check_i64(first_phys, "first element position in V2 header will exceed i64::MAX")?;
    check_i64(last_phys, "last element position in V2 header will exceed i64::MAX")?;

    Ok(build_slot_bytes(&SlotData {
        file_length: file_len,
        element_count: elem_cnt as u32,
        first_position: first_phys,
        last_position: last_phys,
        generation,
        next_sequence_number: next_seq,
    }))
}

pub fn parse_versioned_header(buf: &mut &[u8]) -> Result<(u64, usize, u64, u64)> {
    let version = buf.get_u32() & 0x7FFF_FFFF;
    ensure!(version == 1, Error::UnsupportedVersion { detected: version, supported: 1u32 });

    let file_len = buf.get_u64();
    let elem_cnt = buf.get_u32() as usize;
    let first_pos = buf.get_u64();
    let last_pos = buf.get_u64();

    check_i64(file_len, "file length in header is greater than i64::MAX")?;
    check_i32(elem_cnt as u64, "element count in header is greater than i32::MAX")?;
    check_i64(first_pos, "first element position in header is greater than i64::MAX")?;
    check_i64(last_pos, "last element position in header is greater than i64::MAX")?;

    Ok((file_len, elem_cnt, first_pos, last_pos))
}

pub fn parse_legacy_header(buf: &mut &[u8]) -> Result<(u64, usize, u64, u64)> {
    let file_len = u64::from(buf.get_u32());
    let elem_cnt = buf.get_u32() as usize;
    let first_pos = u64::from(buf.get_u32());
    let last_pos = u64::from(buf.get_u32());

    check_i32(file_len, "file length in header is greater than i32::MAX")?;
    check_i32(elem_cnt as u64, "element count in header is greater than i32::MAX")?;
    check_i32(first_pos, "first element position in header is greater than i32::MAX")?;
    check_i32(last_pos, "last element position in header is greater than i32::MAX")?;

    Ok((file_len, elem_cnt, first_pos, last_pos))
}

// ── V2 slot election ──────────────────────────────────────────────────────────

#[inline]
pub fn read_slot(inner: &QueueFileInner, offset: u64) -> Result<[u8; V2_SLOT_LEN]> {
    let mut buf = [0u8; V2_SLOT_LEN];
    inner.read_exact_at(offset, &mut buf)?;
    Ok(buf)
}

pub fn read_v2_open_state(inner: &QueueFileInner, real_file_len: u64) -> Result<V2OpenState> {
    let read_valid_slot = |offset: u64| {
        if real_file_len >= offset + V2_SLOT_LEN as u64 {
            read_slot(inner, offset).ok().as_ref().and_then(parse_slot)
        } else {
            None
        }
    };

    let slot_a = read_valid_slot(V2_SLOT_A_OFFSET);
    let slot_b = read_valid_slot(V2_SLOT_B_OFFSET);
    let (active_slot, slot) = elect_canonical_slot(slot_a, slot_b)?;
    validate_v2_slot_data(&slot, real_file_len)?;

    Ok(V2OpenState { active_slot, slot, elem_cnt: slot.element_count as usize })
}

pub fn elect_canonical_slot(
    slot_a: Option<SlotData>, slot_b: Option<SlotData>,
) -> Result<(HeaderSlot, SlotData)> {
    match (slot_a, slot_b) {
        (None, None) => {
            Err(Error::CorruptedFile { msg: "both v2 header slots are invalid".to_string() })
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

pub fn validate_v2_slot_data(slot: &SlotData, real_file_len: u64) -> Result<()> {
    ensure!(slot.file_length >= V2_DATA_START, Error::CorruptedFile {
        msg: format!("v2 file_length {} < data_start {}", slot.file_length, V2_DATA_START)
    });
    ensure!(slot.file_length <= real_file_len, Error::CorruptedFile {
        msg: format!(
            "v2 file is truncated: header claims {}, actual {}",
            slot.file_length, real_file_len
        )
    });
    ensure!(slot.next_sequence_number >= 1, Error::CorruptedFile {
        msg: "v2 next_sequence_number must be >= 1".to_string()
    });
    if slot.element_count == 0 {
        ensure!(slot.first_position == 0 && slot.last_position == 0, Error::CorruptedFile {
            msg: "v2 empty queue has non-zero pointers".to_string()
        });
    } else {
        ensure!(slot.first_position != 0 && slot.last_position != 0, Error::CorruptedFile {
            msg: "v2 non-empty queue has zero pointer".to_string()
        });
        validate_slot_bounds(slot.first_position, slot.file_length, "first_position")?;
        validate_slot_bounds(slot.last_position, slot.file_length, "last_position")?;
    }
    Ok(())
}

#[inline]
fn validate_slot_bounds(pos: u64, file_length: u64, name: &str) -> Result<()> {
    ensure!(pos >= V2_DATA_START && pos < file_length, Error::CorruptedFile {
        msg: format!(
            "v2 {name} {pos} out of range [data_start={V2_DATA_START}, file_length={file_length})"
        )
    });
    Ok(())
}
