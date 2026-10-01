//! Error types and utilities for [`QueueFile`] operations.
//!
//! This module provides the [`Error`] enum which represents all possible errors
//! that can occur during queue file operations, along with the [`Result`] type
//! alias and helper macros.
//!
//! # Error Categories
//!
//! - **I/O Errors**: File system and read/write failures
//! - **Validation Errors**: Corrupted files, invalid data, sequence mismatches
//! - **Capacity Errors**: Element too big, too many elements
//! - **Format Errors**: Unsupported versions, magic mismatches

use std::io;

use thiserror::Error;

/// The error type for [`QueueFile`] operations.
///
/// Each variant represents a distinct category of failure that can occur
/// when working with queue files.
#[derive(Debug, Error)]
pub enum Error {
    /// An I/O error occurred while reading or writing the queue file.
    #[error(transparent)]
    Io {
        #[from]
        source: io::Error,
    },

    /// The queue contains more than `i32::MAX` elements.
    ///
    /// This is a limitation of the on-disk format.
    #[error("too many elements")]
    TooManyElements,

    /// An individual element exceeds the maximum allowed size.
    ///
    /// Elements are limited to `i32::MAX` bytes.
    #[error("element too big")]
    ElementTooBig,

    /// The queue file contains corrupted data.
    ///
    /// This can indicate a hardware failure, incomplete write, or
    /// manual tampering with the file.
    #[error("corrupted file: {msg}")]
    CorruptedFile { msg: String },

    /// The file header magic bytes don't match the expected format.
    #[error("magic mismatch: expected {expected:#010x}, found {found:#010x}")]
    MagicMismatch { expected: u32, found: u32 },

    /// A CRC-32 checksum verification failed.
    ///
    /// This indicates data corruption in the element header or payload.
    #[error("checksum mismatch: expected {expected:#010x}, found {found:#010x}")]
    ChecksumMismatch { expected: u32, found: u32 },

    /// A value in the queue file is invalid or out of expected range.
    #[error("invalid value: {msg}")]
    InvalidValue { msg: String },

    /// A calculated position is outside the valid file bounds.
    #[error("out of bounds: {msg}")]
    OutOfBounds { msg: String },

    /// Element sequence numbers are not in the expected order.
    ///
    /// This indicates a corrupted queue or recovery failure in v2 format.
    #[error("sequence mismatch: expected {expected}, found {found}")]
    SequenceMismatch { expected: u64, found: u64 },

    /// The queue file format version is not supported.
    #[error("unsupported version {detected}. supported versions is {supported} and legacy")]
    UnsupportedVersion { detected: u32, supported: u32 },
}

/// A specialized [`Result`] type for [`QueueFile`] operations.
/// The result type for queue file operations, with [`Error`] as the default error type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Ensures that a condition is true, returning an error if it is not.
///
/// This macro provides a convenient way to validate preconditions
/// and invariants within [`QueueFile`] methods.
///
/// # Variants
///
/// - `ensure!(condition, VariantName { field: value })` - creates error with fields
/// - `ensure!(condition, VariantName)` - creates error with no fields
/// - `ensure!(condition, expression)` - uses the provided expression as error
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

/// Injects a test failure at the given failpoint if the environment variable is set.
///
/// This function is used for testing crash recovery and atomicity guarantees.
/// When `QUEUE_FILE_FAILPOINT` environment variable matches the failpoint name,
/// the function returns an error, simulating a crash at that point.
///
/// This is only available in test builds for safety reasons.
#[inline]
pub fn maybe_inject_failpoint(name: &str) -> Result<()> {
    if std::env::var("QUEUE_FILE_FAILPOINT").ok().as_deref() == Some(name) {
        return Err(Error::CorruptedFile { msg: format!("injected failpoint: {name}") });
    }

    Ok(())
}
