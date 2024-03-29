# queue-file

[![Crate](https://img.shields.io/crates/v/queue-file.svg)](https://crates.io/crates/queue-file)
[![API](https://docs.rs/queue-file/badge.svg)](https://docs.rs/queue-file)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Windows Build Status](https://ci.appveyor.com/api/projects/status/loj512o2qo6q0rwg?svg=true)](https://ci.appveyor.com/project/khrs/queue-file)

queue-file is a lightning-fast, transactional, file-based FIFO.

Addition and removal of an element from the queue is an O(1) operation and is atomic.
Writes are synchronous by default; data will be written to disk before an operation returns.

queue-file crate is a feature complete and binary compatible port of `QueueFile` class from
Tape2 by Square, Inc. Check the original project [here](https://github.com/square/tape).

[Documentation](https://docs.rs/queue-file)

## Usage

To use `queue-file`, first add this to your `Cargo.toml`:

```toml
[dependencies]
queue-file = "1"
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

Current MSRV is 1.58.1

## License

This project is licensed under the [Apache 2.0 license](LICENSE).
