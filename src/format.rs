use bytes::{Buf, BufMut};

use crate::element::Element;
use crate::ensure;
use crate::error::{Error, Result, maybe_inject_failpoint};
use crate::header::{
    HeaderSlot, SlotData, V2_DATA_START, V2_SLOT_A_OFFSET, V2_SLOT_B_OFFSET, V2_SLOT_LEN,
    VERSIONED_HEADER, build_slot_bytes, crc32, parse_slot,
};
use crate::qio::{DataRing, DataRingMut, QueueFileInner};

pub const V2_ELEM_HDR_MAGIC: u32 = 0x4954_4844;
pub const V2_ELEM_HDR_LEN: usize = 28;
pub const V2_ELEM_FTR_MAGIC: u32 = 0x4954_4654;
pub const V2_ELEM_FTR_LEN: usize = 16;
pub const V2_ELEM_OVERHEAD: u64 = 44;

#[derive(Debug, Clone, Copy)]
pub struct LayoutMetrics {
    #[allow(dead_code)]
    header_len: u64,
    #[allow(dead_code)]
    data_start: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct QueueMetadata {
    pub file_len: u64,
    pub elem_cnt: usize,
    pub first_pos: u64,
    pub last_pos: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct LegacyHeaderState {
    pub format: FormatState,
    pub file_len: u64,
    pub elem_cnt: usize,
    pub first_pos: u64,
    pub last_pos: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct V2OpenState {
    pub active_slot: HeaderSlot,
    pub slot: SlotData,
    pub elem_cnt: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct V2ElementHeader {
    pub payload_len: usize,
    pub seq: u64,
    pub prev_pos: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct ExpansionPlan {
    pub orig_file_len: u64,
    pub new_len: u64,
    pub end_of_last_elem: u64,
    pub wraps: bool,
    pub moved_count: u64,
}

#[derive(Debug, Clone)]
pub struct QueueStateSnapshot {
    pub format: FormatState,
    pub elem_cnt: usize,
    pub first: Element,
    pub last: Element,
    pub overwrite_on_remove: bool,
    pub cached_offsets: std::collections::VecDeque<(usize, Element)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatState {
    Legacy,
    V1,
    V2 { active_slot: HeaderSlot, generation: u64, next_seq: u64 },
}

impl FormatState {
    #[inline]
    pub const fn layout(&self) -> LayoutMetrics {
        match self {
            Self::Legacy => LayoutMetrics { header_len: 16, data_start: 16 },
            Self::V1 => LayoutMetrics { header_len: 32, data_start: 32 },
            Self::V2 { .. } => {
                LayoutMetrics { header_len: V2_SLOT_LEN as u64, data_start: V2_DATA_START }
            }
        }
    }

    #[inline]
    pub const fn data_start(&self) -> u64 {
        self.layout().data_start
    }

    #[inline]
    pub const fn elem_hdr_len(&self) -> u64 {
        match self {
            Self::V2 { .. } => V2_ELEM_HDR_LEN as u64,
            _ => Element::HEADER_LENGTH as u64,
        }
    }

    #[inline]
    pub const fn elem_span(&self, payload_len: usize) -> u64 {
        match self {
            Self::V2 { .. } => V2_ELEM_OVERHEAD + payload_len as u64,
            _ => Element::HEADER_LENGTH as u64 + payload_len as u64,
        }
    }

    #[inline]
    pub const fn next_seq(&self) -> u64 {
        match self {
            Self::V2 { next_seq, .. } => *next_seq,
            _ => 0,
        }
    }

    #[inline]
    pub fn set_next_seq(&mut self, seq: u64) {
        if let Self::V2 { next_seq, .. } = self {
            *next_seq = seq;
        }
    }

    pub fn commit_header(
        &mut self, inner: &mut QueueFileInner, metadata: QueueMetadata,
    ) -> Result<()> {
        let ds = self.data_start();
        let (first_phys, last_phys) = if metadata.elem_cnt == 0 {
            (0, 0)
        } else {
            (ds + metadata.first_pos, ds + metadata.last_pos)
        };

        match self {
            Self::Legacy | Self::V1 => {
                let bytes = if matches!(self, Self::V1) {
                    encode_v1_header(metadata.file_len, metadata.elem_cnt, first_phys, last_phys)?
                        .to_vec()
                } else {
                    encode_legacy_header(
                        metadata.file_len,
                        metadata.elem_cnt,
                        first_phys,
                        last_phys,
                    )?
                    .to_vec()
                };

                inner.seek(0);
                inner.write(&bytes)
            }
            Self::V2 { active_slot, generation, next_seq } => {
                let next_slot = active_slot.toggle();

                let slot_bytes = encode_v2_slot(
                    metadata.file_len,
                    metadata.elem_cnt,
                    first_phys,
                    last_phys,
                    *generation + 1,
                    *next_seq,
                )?;

                let offset = next_slot.offset();

                inner.seek(offset);
                inner.write(&slot_bytes)?;

                *generation += 1;
                *active_slot = next_slot;

                Ok(())
            }
        }
    }

    #[allow(clippy::unused_self)]
    #[allow(clippy::unused_self)]
    pub fn read_element(&self, ring: &DataRing<'_>, logical_pos: u64) -> Result<Element> {
        match self {
            Self::Legacy | Self::V1 => {
                let mut buf: [u8; 4] = [0; Element::HEADER_LENGTH];
                ring.read_at(logical_pos, &mut buf)?;

                Element::new(logical_pos, u32::from_be_bytes(buf) as usize, 0)
            }
            Self::V2 { .. } => {
                let header = self.validate_v2_element_header(ring, logical_pos)?;
                Ok(Element { pos: logical_pos, len: header.payload_len, seq: header.seq })
            }
        }
    }

    #[allow(clippy::unused_self)]
    pub fn validate_v2_element_header(
        &self, ring: &DataRing<'_>, logical_pos: u64,
    ) -> Result<V2ElementHeader> {
        let mut hdr = [0u8; V2_ELEM_HDR_LEN];
        ring.read_at(logical_pos, &mut hdr)?;

        let magic = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
        ensure!(magic == V2_ELEM_HDR_MAGIC, Error::CorruptedFile {
            msg: format!(
                "v2 element header magic mismatch at logical pos {logical_pos}: {magic:#010x}"
            )
        });

        let seq =
            i64::from_be_bytes([hdr[4], hdr[5], hdr[6], hdr[7], hdr[8], hdr[9], hdr[10], hdr[11]])
                as u64;
        let prev_pos = i64::from_be_bytes([
            hdr[12], hdr[13], hdr[14], hdr[15], hdr[16], hdr[17], hdr[18], hdr[19],
        ]) as u64;
        let payload_len_raw = i32::from_be_bytes([hdr[20], hdr[21], hdr[22], hdr[23]]);
        ensure!(payload_len_raw >= 0, Error::CorruptedFile {
            msg: format!(
                "v2 element payload_len {payload_len_raw} is negative at logical pos {logical_pos}"
            )
        });
        let payload_len = payload_len_raw as usize;
        ensure!(seq >= 1, Error::CorruptedFile {
            msg: format!("v2 element seq {seq} < 1 at logical pos {logical_pos}")
        });

        let expected_crc = compute_elem_header_crc(&hdr);
        let stored_crc = u32::from_be_bytes([hdr[24], hdr[25], hdr[26], hdr[27]]);
        ensure!(stored_crc == expected_crc, Error::CorruptedFile {
            msg: format!("v2 element header CRC mismatch at logical pos {logical_pos}")
        });

        let span = V2_ELEM_OVERHEAD + payload_len as u64;
        ensure!(span <= ring.capacity(), Error::CorruptedFile {
            msg: format!("v2 element span {span} exceeds capacity")
        });

        Ok(V2ElementHeader { payload_len, seq, prev_pos })
    }

    #[allow(clippy::unused_self)]
    #[allow(clippy::unused_self)]
    pub fn write_element(
        &self, ring: &mut DataRingMut<'_>, logical_pos: u64, payload: &[u8], seq: u64,
        prev_logical_pos: Option<u64>,
    ) -> Result<()> {
        match self {
            Self::Legacy | Self::V1 => {
                let len = payload.len();
                let mut buf = Vec::with_capacity(4 + len);
                buf.extend(&(len as u32).to_be_bytes());
                buf.extend(payload);
                ring.write_at(logical_pos, &buf)
            }
            Self::V2 { .. } => {
                self.write_v2_element(ring, logical_pos, seq, prev_logical_pos, payload)
            }
        }
    }

    #[allow(clippy::unused_self)]
    pub fn write_v2_element(
        &self, ring: &mut DataRingMut<'_>, logical_pos: u64, seq: u64,
        prev_logical_pos: Option<u64>, payload: &[u8],
    ) -> Result<()> {
        let payload_len = payload.len();
        let prev_phys = prev_logical_pos.map_or(0, |p| ring.data_start + p);

        let mut hdr = [0u8; V2_ELEM_HDR_LEN];
        {
            let mut w: &mut [u8] = &mut hdr;
            w.put_u32(V2_ELEM_HDR_MAGIC);
            w.put_i64(seq as i64);
            w.put_i64(prev_phys as i64);
            w.put_i32(payload_len as i32);
        }
        let hdr_crc = compute_elem_header_crc(&hdr);
        hdr[24..28].copy_from_slice(&hdr_crc.to_be_bytes());

        ring.write_at(logical_pos, &hdr)?;

        let payload_pos = ring.add(logical_pos, V2_ELEM_HDR_LEN as u64);
        ring.write_at(payload_pos, payload)?;

        let footer_pos = ring.add(logical_pos, V2_ELEM_HDR_LEN as u64 + payload_len as u64);
        let mut ftr = [0u8; V2_ELEM_FTR_LEN];
        {
            let mut w: &mut [u8] = &mut ftr;
            w.put_u32(V2_ELEM_FTR_MAGIC);
            w.put_i64(seq as i64);
        }
        let ftr_crc = compute_elem_footer_crc(payload, &ftr[..12]);
        ftr[12..16].copy_from_slice(&ftr_crc.to_be_bytes());

        ring.write_at(footer_pos, &ftr)?;

        Ok(())
    }

    pub fn on_expansion(
        &self, ring: &mut DataRingMut<'_>, plan: &ExpansionPlan, first: Element, last: Element,
        elem_cnt: usize,
    ) -> Result<Option<u64>> {
        if matches!(self, Self::V2 { .. }) {
            self.rewrite_v2_backlinks_after_expansion(ring, plan, first, elem_cnt)?;
            if last.pos < first.pos {
                return Ok(Some(plan.orig_file_len - ring.data_start + last.pos));
            }
        } else if last.pos < first.pos {
            return Ok(Some(plan.orig_file_len - ring.data_start + last.pos));
        }
        Ok(None)
    }

    pub fn rewrite_v2_backlinks_after_expansion(
        &self, ring: &mut DataRingMut<'_>, plan: &ExpansionPlan, first: Element, elem_cnt: usize,
    ) -> Result<()> {
        let mut positions: Vec<Element> = Vec::with_capacity(elem_cnt);
        let mut cur = first;
        for _ in 0..elem_cnt {
            positions.push(cur);
            let next_pos = ring.add(cur.pos, V2_ELEM_OVERHEAD + cur.len as u64);
            if positions.len() < elem_cnt {
                let next_header =
                    self.validate_v2_element_header(&ring.as_read_only(), next_pos)?;
                cur = Element { pos: next_pos, len: next_header.payload_len, seq: next_header.seq };
            }
        }

        let moved_offset = plan.orig_file_len - ring.data_start;

        ring.inner.with_batched_backlink_rewrite_sync(|inner| {
            let mut ring = DataRingMut::new(inner, ring.data_start);
            for elem in &positions {
                let elem_pos = elem.pos;
                let mut hdr = [0u8; V2_ELEM_HDR_LEN];
                ring.as_read_only().read_at(elem_pos, &mut hdr)?;

                let prev_pos =
                    self.validate_v2_element_header(&ring.as_read_only(), elem_pos)?.prev_pos;

                if prev_pos < plan.end_of_last_elem - ring.data_start {
                    let new_prev_pos = prev_pos + moved_offset;

                    let new_prev_bytes = (new_prev_pos as i64).to_be_bytes();
                    hdr[12..20].copy_from_slice(&new_prev_bytes);

                    let new_crc = compute_elem_header_crc(&hdr);
                    hdr[24..28].copy_from_slice(&new_crc.to_be_bytes());

                    ring.write_at(elem_pos, &hdr)?;
                }
            }

            Ok(())
        })
    }

    #[allow(clippy::unused_self)]
    #[allow(clippy::unused_self)]
    pub fn validate_footer(
        &self, ring: &DataRing<'_>, payload_start: u64, elem: &Element, payload: &[u8],
    ) -> Result<()> {
        if let Self::V2 { .. } = self {
            let footer_pos = ring.add(payload_start, elem.len as u64);
            self.validate_v2_footer(ring, footer_pos, elem.seq, payload)?;
        }
        Ok(())
    }

    #[allow(clippy::unused_self)]
    pub fn validate_v2_footer(
        &self, ring: &DataRing<'_>, footer_pos: u64, seq: u64, payload: &[u8],
    ) -> Result<()> {
        let mut ftr = [0u8; V2_ELEM_FTR_LEN];
        ring.read_at(footer_pos, &mut ftr)?;

        let ftr_magic = u32::from_be_bytes([ftr[0], ftr[1], ftr[2], ftr[3]]);
        ensure!(ftr_magic == V2_ELEM_FTR_MAGIC, Error::CorruptedFile {
            msg: format!(
                "v2 element footer magic mismatch at logical pos {footer_pos}: {ftr_magic:#010x}"
            )
        });

        let ftr_seq =
            i64::from_be_bytes([ftr[4], ftr[5], ftr[6], ftr[7], ftr[8], ftr[9], ftr[10], ftr[11]])
                as u64;
        ensure!(ftr_seq == seq, Error::CorruptedFile {
            msg: format!("v2 footer seq {ftr_seq} != element seq {seq}")
        });

        let stored_crc = u32::from_be_bytes([ftr[12], ftr[13], ftr[14], ftr[15]]);
        let expected_crc = compute_elem_footer_crc(payload, &ftr[..12]);
        ensure!(stored_crc == expected_crc, Error::CorruptedFile {
            msg: "v2 element footer CRC mismatch".to_string()
        });

        Ok(())
    }

    pub fn on_expansion_cleanup(&self, plan: &ExpansionPlan) -> Result<()> {
        if matches!(self, Self::V2 { .. }) && plan.wraps {
            maybe_inject_failpoint("v2_after_relocation_cleanup_before_add")?;
        }
        Ok(())
    }
}

fn encode_legacy_header(
    file_len: u64, elem_cnt: usize, first_phys: u64, last_phys: u64,
) -> Result<[u8; 16]> {
    ensure!(i32::try_from(file_len).is_ok(), Error::CorruptedFile {
        msg: "file length in header will exceed i32::MAX".to_string()
    });
    ensure!(i32::try_from(elem_cnt).is_ok(), Error::CorruptedFile {
        msg: "element count in header will exceed i32::MAX".to_string()
    });
    ensure!(i32::try_from(first_phys).is_ok(), Error::CorruptedFile {
        msg: "first element position in header will exceed i32::MAX".to_string()
    });
    ensure!(i32::try_from(last_phys).is_ok(), Error::CorruptedFile {
        msg: "last element position in header will exceed i32::MAX".to_string()
    });

    let mut header = [0u8; 16];
    let mut header_buf: &mut [u8] = &mut header;

    header_buf.put_i32(file_len as i32);
    header_buf.put_i32(elem_cnt as i32);
    header_buf.put_i32(first_phys as i32);
    header_buf.put_i32(last_phys as i32);

    Ok(header)
}

fn encode_v1_header(
    file_len: u64, elem_cnt: usize, first_phys: u64, last_phys: u64,
) -> Result<[u8; 32]> {
    ensure!(i64::try_from(file_len).is_ok(), Error::CorruptedFile {
        msg: "file length in header will exceed i64::MAX".to_string()
    });
    ensure!(i32::try_from(elem_cnt).is_ok(), Error::CorruptedFile {
        msg: "element count in header will exceed i32::MAX".to_string()
    });
    ensure!(i64::try_from(first_phys).is_ok(), Error::CorruptedFile {
        msg: "first element position in header will exceed i64::MAX".to_string()
    });
    ensure!(i64::try_from(last_phys).is_ok(), Error::CorruptedFile {
        msg: "last element position in header will exceed i64::MAX".to_string()
    });

    let mut header = [0u8; 32];
    let mut header_buf: &mut [u8] = &mut header;

    header_buf.put_u32(VERSIONED_HEADER);
    header_buf.put_u64(file_len);
    header_buf.put_i32(elem_cnt as i32);
    header_buf.put_u64(first_phys);
    header_buf.put_u64(last_phys);

    Ok(header)
}

fn encode_v2_slot(
    file_len: u64, elem_cnt: usize, first_phys: u64, last_phys: u64, generation: u64, next_seq: u64,
) -> Result<[u8; V2_SLOT_LEN]> {
    ensure!(i64::try_from(file_len).is_ok(), Error::CorruptedFile {
        msg: "file length in V2 header will exceed i64::MAX".to_string()
    });
    ensure!(u32::try_from(elem_cnt).is_ok(), Error::CorruptedFile {
        msg: "element count in V2 header will exceed u32::MAX".to_string()
    });
    ensure!(i64::try_from(first_phys).is_ok(), Error::CorruptedFile {
        msg: "first element position in V2 header will exceed i64::MAX".to_string()
    });
    ensure!(i64::try_from(last_phys).is_ok(), Error::CorruptedFile {
        msg: "last element position in V2 header will exceed i64::MAX".to_string()
    });

    let slot_data = SlotData {
        file_length: file_len,
        element_count: elem_cnt as u32,
        first_position: first_phys,
        last_position: last_phys,
        generation,
        next_sequence_number: next_seq,
    };

    Ok(build_slot_bytes(&slot_data))
}

pub fn read_slot(inner: &QueueFileInner, offset: u64) -> Result<[u8; V2_SLOT_LEN]> {
    let mut buf = [0u8; V2_SLOT_LEN];
    inner.read_exact_at(offset, &mut buf)?;
    Ok(buf)
}

pub fn parse_versioned_header(buf: &mut bytes::BytesMut) -> Result<(u64, usize, u64, u64)> {
    let version = buf.get_u32() & 0x7FFF_FFFF;
    ensure!(version == 1, Error::UnsupportedVersion { detected: version, supported: 1u32 });

    let file_len = buf.get_u64();
    let elem_cnt = buf.get_u32() as usize;
    let first_pos = buf.get_u64();
    let last_pos = buf.get_u64();

    ensure!(i64::try_from(file_len).is_ok(), Error::CorruptedFile {
        msg: "file length in header is greater than i64::MAX".to_string()
    });
    ensure!(i32::try_from(elem_cnt).is_ok(), Error::CorruptedFile {
        msg: "element count in header is greater than i32::MAX".to_string()
    });
    ensure!(i64::try_from(first_pos).is_ok(), Error::CorruptedFile {
        msg: "first element position in header is greater than i64::MAX".to_string()
    });
    ensure!(i64::try_from(last_pos).is_ok(), Error::CorruptedFile {
        msg: "last element position in header is greater than i64::MAX".to_string()
    });
    Ok((file_len, elem_cnt, first_pos, last_pos))
}

pub fn parse_legacy_header(buf: &mut bytes::BytesMut) -> Result<(u64, usize, u64, u64)> {
    let file_len = u64::from(buf.get_u32());
    let elem_cnt = buf.get_u32() as usize;
    let first_pos = u64::from(buf.get_u32());
    let last_pos = u64::from(buf.get_u32());

    ensure!(i32::try_from(file_len).is_ok(), Error::CorruptedFile {
        msg: "file length in header is greater than i32::MAX".to_string()
    });
    ensure!(i32::try_from(elem_cnt).is_ok(), Error::CorruptedFile {
        msg: "element count in header is greater than i32::MAX".to_string()
    });
    ensure!(i32::try_from(first_pos).is_ok(), Error::CorruptedFile {
        msg: "first element position in header is greater than i32::MAX".to_string()
    });
    ensure!(i32::try_from(last_pos).is_ok(), Error::CorruptedFile {
        msg: "last element position in header is greater than i32::MAX".to_string()
    });
    Ok((file_len, elem_cnt, first_pos, last_pos))
}

fn compute_elem_header_crc(hdr_bytes: &[u8; V2_ELEM_HDR_LEN]) -> u32 {
    crc32(&hdr_bytes[..24])
}

fn compute_elem_footer_crc(payload: &[u8], ftr_bytes_0_to_11: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(payload);
    hasher.update(ftr_bytes_0_to_11);
    hasher.finalize()
}

pub fn read_v2_open_state(inner: &QueueFileInner, real_file_len: u64) -> Result<V2OpenState> {
    let raw_a = if real_file_len >= V2_SLOT_LEN as u64 {
        read_slot(inner, V2_SLOT_A_OFFSET).ok()
    } else {
        None
    };
    let raw_b = if real_file_len >= V2_SLOT_B_OFFSET + V2_SLOT_LEN as u64 {
        read_slot(inner, V2_SLOT_B_OFFSET).ok()
    } else {
        None
    };

    let slot_a = raw_a.as_ref().and_then(parse_slot);
    let slot_b = raw_b.as_ref().and_then(parse_slot);
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
        ensure!(
            slot.first_position >= V2_DATA_START && slot.first_position < slot.file_length,
            Error::CorruptedFile {
                msg: format!(
                    "v2 first_position {} out of range [data_start={}, file_length={})",
                    slot.first_position, V2_DATA_START, slot.file_length
                )
            }
        );
        ensure!(
            slot.last_position >= V2_DATA_START && slot.last_position < slot.file_length,
            Error::CorruptedFile {
                msg: format!(
                    "v2 last_position {} out of range [data_start={}, file_length={})",
                    slot.last_position, V2_DATA_START, slot.file_length
                )
            }
        );
    }
    Ok(())
}
