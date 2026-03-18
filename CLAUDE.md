# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build                          # Build the library
cargo test                           # Run all tests
cargo test <test_name>               # Run a specific test (e.g. cargo test iter_nth)
cargo clippy                         # Lint (clippy::nursery + clippy::pedantic enabled)
cargo fmt                            # Format code (uses rustfmt.toml settings)
cargo fmt -- --check                 # Check formatting without modifying
```

## Architecture

The entire library lives in `src/lib.rs`. It is a Rust port of Square's [Tape2 QueueFile](https://github.com/square/tape), maintaining binary compatibility with the Java format.

### Core Types

- **`QueueFile`** — the public-facing struct. Manages a file-based ring-buffer FIFO with atomic add/remove. Wraps `QueueFileInner` and carries metadata about the queue state (element count, head/tail pointers, capacity, options).
- **`QueueFileInner`** — owns the `File` handle and I/O buffers (read buffer, write buffer, transfer buffer). Tracks file length and sync settings.
- **`Element`** — a (position, length) pair representing an element's location in the ring buffer.
- **`Iter`** — iterator yielding `Box<[u8]>` by traversing the ring buffer from head, with optional offset caching.
- **`OffsetCacheKind`** — optional policy (`Linear` or `Quadratic`) for caching element positions to speed up random access in `Iter::nth`.

### File Format

Two header versions are supported, controlled by bit 31 of the first 4 bytes:

```
Versioned Header (32 bytes): version bit=1, version=1, file_len(8), elem_cnt(4), first_pos(8), last_pos(8)
Legacy Header (16 bytes):    version bit=0, file_len(4), elem_cnt(4), first_pos(4), last_pos(4)
```

Each element is prefixed by a 4-byte length. The file grows as a power of 2 (doubles when full, shrinks on `clear`). `QueueFile::open` creates versioned format; `open_legacy` creates legacy format.

### Key Behaviors

- Writes are sync by default (`sync_data()` after each mutation); disable with `set_sync_writes(false)`.
- `overwrite_on_remove` (default: true) zeroes data bytes on removal for security.
- `skip_write_header_on_add` (default: false) defers the header write for batched adds.
- `add_n` batches multiple element additions in a single write.
- Ring buffer wrapping: positions modulo file length, handled by the `wrappos` helper.

### Lint Configuration

The crate enforces strict lints: `#![deny(...)]` with many compiler lints and `#![warn(clippy::nursery, clippy::pedantic)]`. Several casts and missing-doc lints are explicitly allowed. New code must pass `cargo clippy` without warnings.

### `src/main.rs`

Present on the `recovery-tool` branch only — a one-off utility to filter empty elements from a queue file. Not part of the library.

## Testing

Tests use `quickcheck` for property-based testing (queue behaves like `VecDeque`) and `test-case` for parameterized cases. Regression tests for specific bugs live in `tests/bug_cases.rs`. `auto-delete-path` handles temp file cleanup.

MSRV is **1.58.1** — avoid features stabilized after that.
