# Gemini CLI Project Context: queue-file

This project is a high-performance, transactional, file-based FIFO queue written in Rust. It is a feature-complete and binary-compatible port of the `QueueFile` class from Tape2 by Square, Inc.

## Project Overview

- **Core Purpose:** Provides an O(1) atomic addition and removal of elements from a persistent queue.
- **Main Technology:** Rust (MSRV 1.58.1).
- **Key Features:**
    - Transactional writes (atomic header commits).
    - Binary compatibility with Tape2 (Legacy format).
    - V2 format with per-element CRC-32 integrity checks and sequence numbers.
    - Automatic migration from V1 to V2 on open.
    - Synchronous writes by default (configurable).
    - Ring-buffer layout for efficient I/O.

## Architecture & Core Components

The entire library implementation resides in `src/lib.rs`.

- **`QueueFile`**: The public API. Manages metadata, capacity, and high-level operations.
- **`QueueFileInner`**: Handles low-level file I/O, buffering, and atomic synchronization.
- **`DataRing` / `DataRingMut`**: Encapsulates ring-buffer logic and logical-to-physical address translation.
- **`FormatState`**: Handles format-specific header serialization, element validation, and layout metrics for Legacy, V1, and V2 formats.
- **`Element`**: Represents an element's position, length, and sequence number.
- **`Iter`**: Provides an iterator over the queue's elements.
- **Header Formats**:
    - **Legacy (16B)**: Square/Tape2 compatible, 32-bit pointers.
    - **V1 (32B)**: 64-bit pointers.
    - **V2 (112B)**: Dual 56-byte slots for atomic commits, includes sequence numbers and CRC.

## Development Workflow

### Key Commands

Standard Cargo commands are the primary way to interact with the project:

```bash
cargo build                          # Build the library
cargo test                           # Run all tests (unit, integration, doc)
cargo test <test_name>               # Run a specific test
cargo clippy                         # Linting (pedantic & nursery enabled)
cargo fmt                            # Format code (per rustfmt.toml)
```

The project also uses `just` for specific tasks:

```bash
just fmt                             # Format with nightly rustfmt
just clippy                          # Run clippy with nightly toolchain
```

### OpenSpec Workflow

The project follows a spec-driven development process located in `openspec/`.

- **New Changes**: Use `openspec new change "<name>"` to start a feature or fix.
- **Artifacts**: Each change involves creating a `proposal.md`, `design.md` (optional), and `tasks.md`.
- **Implementation**: Tasks in `tasks.md` are executed following the Plan-Act-Validate cycle.
- **Archive**: Completed changes are moved to `openspec/changes/archive/`.

Available OpenSpec skills can be activated via:
- `activate_skill openspec-propose`
- `activate_skill openspec-explore`
- `activate_skill openspec-apply-change`
- `activate_skill openspec-archive-change`

## Coding Standards & Conventions

- **Rust Version**: Adhere to Rust 2021 idioms and MSRV 1.58.1.
- **Error Handling**: Uses `thiserror` for descriptive, structured error types.
- **Linting**: Strict linting is enforced. Check `src/lib.rs` for `#![deny(...)]` and `#![warn(...)]` blocks.
- **Formatting**: Defined in `rustfmt.toml` (100-character max width, reordered impls, grouped imports).
- **Naming**: `snake_case` for functions/variables/tests, `PascalCase` for types, `SCREAMING_SNAKE_CASE` for constants.
- **Safety**: Do not weaken durability guarantees (atomic writes, sync-to-disk) without explicit documentation and justification.

## Testing Strategy

- **Unit/Integration Tests**: Located in `tests/` and within `src/lib.rs`.
- **Property-based Testing**: Uses `quickcheck` to verify invariants against `VecDeque`.
- **Regression Tests**: Found in `tests/bug_cases.rs`. Always add a reproduction case here for any fixed bug.
- **Parameterized Tests**: Uses `test-case` for covering various scenarios (e.g., across different file formats).
- **Temporary Files**: Uses `auto-delete-path` for automatic cleanup of test files.
