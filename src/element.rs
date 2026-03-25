//! Element representation for queue entries.
//!
//! This module provides the [`Element`] struct which represents a single
//! element in the queue, storing its position, length, and sequence number.

use crate::ensure;
use crate::error::Result;

/// Represents a single element in the queue.
///
/// An element stores the logical position within the ring buffer,
/// its payload length, and (for v2 format) its sequence number.
#[derive(Copy, Clone, Debug)]
pub struct Element {
    /// The logical position of this element in the ring buffer.
    pub pos: u64,
    /// The length of the element's payload in bytes.
    pub len: usize,
    /// The sequence number (v2 format only, 0 for legacy/v1).
    pub seq: u64,
}

impl Element {
    /// An empty element representing an uninitialized state.
    pub const EMPTY: Self = Self { pos: 0, len: 0, seq: 0 };
    /// The size of the element header in legacy/v1 formats (4 bytes for length).
    pub const HEADER_LENGTH: usize = 4;

    /// Creates a new element with the given position, length, and sequence number.
    ///
    /// Returns an error if the position or length exceeds `i64::MAX` or
    /// `i32::MAX` respectively.
    #[inline]
    pub fn new(pos: u64, len: usize, seq: u64) -> Result<Self> {
        ensure!(
            i64::try_from(pos).is_ok(),
            OutOfBounds { msg: "element position must be less or equal to i64::MAX".to_string() }
        );
        ensure!(i32::try_from(len).is_ok(), ElementTooBig);

        Ok(Self { pos, len, seq })
    }
}
