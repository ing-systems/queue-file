use crate::ensure;
use crate::error::Result;

#[derive(Copy, Clone, Debug)]
pub struct Element {
    pub pos: u64,
    pub len: usize,
    pub seq: u64,
}

impl Element {
    pub const EMPTY: Self = Self { pos: 0, len: 0, seq: 0 };
    pub const HEADER_LENGTH: usize = 4;

    #[inline]
    pub fn new(pos: u64, len: usize, seq: u64) -> Result<Self> {
        ensure!(i64::try_from(pos).is_ok(), crate::error::Error::CorruptedFile {
            msg: "element position must be less or equal to i64::MAX".to_string()
        });
        ensure!(i32::try_from(len).is_ok(), crate::error::Error::ElementTooBig);

        Ok(Self { pos, len, seq })
    }
}
