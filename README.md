# queue-file

[![Crate](https://img.shields.io/crates/v/queue-file.svg)](https://crates.io/crates/queue-file)
[![API](https://docs.rs/queue-file/badge.svg)](https://docs.rs/queue-file)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Windows Build Status](https://ci.appveyor.com/api/projects/status/loj512o2qo6q0rwg?svg=true)](https://ci.appveyor.com/project/khrs/queue-file)

queue-file is a lightning-fast, transactional, file-based FIFO.

Addition and removal of an element from the queue is an O(1) operation and is atomic.
Writes are synchronous by default; data will be written to disk before an operation returns.

queue-file 1.x crate is a feature complete and binary compatible port of `QueueFile` class from
Tape2 by Square, Inc. Check the original project [here](https://github.com/square/tape).

queue-file 2.x builds on that foundation with a new, more robust V2 on-disk format —
dual-slot atomic header commits, CRC-32 integrity checks, and faster crash recovery —
while transparently reading and migrating existing 1.x files. See [What's New](#whats-new)
for the full list of changes.

[Documentation](https://docs.rs/queue-file)

## What's New

### 2.x

- **New V2 file format with automatic migration.** `QueueFile::open` now creates
  the V2 format, which stores a back-link in each element to enable faster, scan-free
  recovery of the queue state. Existing V1 (and legacy) files are detected and
  migrated transparently, preserving binary compatibility.
- **Dual-slot atomic header commits.** The V2 format keeps two header slots (at
  offsets `0` and `4096`), each tagged with a monotonic generation counter. Each
  commit writes the inactive slot and then switches to it, so a crash mid-write
  always leaves a valid prior header to fall back to — no torn headers.
- **CRC-32 integrity checks.** Every V2 header slot, element header, and element
  footer carries a CRC-32 (via `crc32fast`). Corruption is detected on read and
  surfaced as `Error::ChecksumMismatch` instead of returning garbage data. Each
  element also stores a sequence number for ordering/recovery.
- **Safer API — panics replaced with errors.** Operations that previously could
  panic (corrupted headers, oversized elements, invalid payload lengths) now return
  a proper `Error` instead. Error handling was reworked from `snafu` to `thiserror`.
- **`peek_into` for zero-allocation reads.** Read the head element into a
  caller-owned `Vec<u8>` to reuse buffers across calls and avoid per-read allocation.
- **`peek` and `iter` now yield `Vec<u8>`** instead of `Box<[u8]>`, making returned
  data easier to reuse and mutate. (Breaking change.)
- **`add_n` no longer requires `Clone`** on the element type, and batches multiple
  additions into a single write with batched syncs for better throughput.
- **`QueueFile` is now `!Send` + `!Sync`** to statically prevent unsafe concurrent
  access to the underlying file.
- **Faster iteration and random access.** Offset caching during iteration (with a
  bounded cache size), header-only skipping in `Iter::nth`, and various I/O batching
  reduce both allocations and `fsync` calls.
- **Codebase modularized** and lints tightened; MSRV raised to **1.71**.

## Usage

To use `queue-file`, first add this to your `Cargo.toml`:

```toml
[dependencies]
queue-file = "2"
```

## Example

```rust
use queue_file::QueueFile;

fn main() {
    let mut qf = QueueFile::open("example.qf")
        .expect("cannot open queue file");

    qf.add("ELEMENT #1".as_bytes()).expect("add failed");
    qf.add("ELEMENT #2".as_bytes()).expect("add failed");
    qf.add("ELEMENT #3".as_bytes()).expect("add failed");

    qf.remove().expect("remove failed");

    for (index, elem) in qf.iter().enumerate() {
        println!(
            "{}: {} bytes -> {}",
            index,
            elem.len(),
            std::str::from_utf8(&elem).unwrap_or("<invalid>")
        );
    }

    qf.clear().expect("clear failed");
}
```

## MSRV

Current MSRV is 1.71

## License

This project is licensed under the [Apache 2.0 license](LICENSE).
