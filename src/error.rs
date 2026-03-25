use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io {
        #[from]
        source: io::Error,
    },

    #[error("too many elements")]
    TooManyElements,

    #[error("element too big")]
    ElementTooBig,

    #[error("corrupted file: {msg}")]
    CorruptedFile { msg: String },

    #[error("magic mismatch: expected {expected:#010x}, found {found:#010x}")]
    MagicMismatch { expected: u32, found: u32 },

    #[error("checksum mismatch: expected {expected:#010x}, found {found:#010x}")]
    ChecksumMismatch { expected: u32, found: u32 },

    #[error("invalid value: {msg}")]
    InvalidValue { msg: String },

    #[error("out of bounds: {msg}")]
    OutOfBounds { msg: String },

    #[error("sequence mismatch: expected {expected}, found {found}")]
    SequenceMismatch { expected: u64, found: u64 },

    #[error("unsupported version {detected}. supported versions is {supported} and legacy")]
    UnsupportedVersion { detected: u32, supported: u32 },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[macro_export]
macro_rules! ensure {
    ($cond:expr, $variant:ident { $($field:ident : $val:expr),* $(,)? }) => {
        if !($cond) {
            return Err($crate::Error::$variant { $($field : $val),* });
        }
    };
    ($cond:expr, $variant:ident) => {
        if !($cond) {
            return Err($crate::Error::$variant);
        }
    };
    ($cond:expr, $err:expr) => {
        if !($cond) {
            return Err($err);
        }
    };
}

#[inline]
pub fn maybe_inject_failpoint(name: &str) -> Result<()> {
    if std::env::var("QUEUE_FILE_FAILPOINT").ok().as_deref() == Some(name) {
        return Err(Error::CorruptedFile { msg: format!("injected failpoint: {name}") });
    }

    Ok(())
}
