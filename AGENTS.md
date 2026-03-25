# Repository Guidelines

## Project Structure & Module Organization
`src/lib.rs` contains the main `queue-file` library implementation and public API. `src/main.rs` is a local utility binary, not the primary product surface, so keep reusable logic in the library. Integration tests live in `tests/`, with general behavior checks in `tests/test.rs` and regression coverage in `tests/bug_cases.rs`. CI helper scripts are in `ci/`, and `example.qf` is a sample queue file for local experiments.

## Build, Test, and Development Commands
Use Cargo for day-to-day work:

- `cargo build` builds the crate and the local binary.
- `cargo test` runs unit, integration, and doctests.
- `cargo test --test bug_cases` runs the regression-focused integration suite only.
- `cargo run` executes `src/main.rs`; review its hard-coded paths before using it.
- `just fmt` formats the codebase with nightly `rustfmt`.
- `just clippy` runs Clippy on the workspace with the nightly toolchain.

## Coding Style & Naming Conventions
Follow Rust 2021 idioms and keep code compatible with the MSRV declared in `Cargo.toml` (`1.58.1`). Formatting is defined in `rustfmt.toml`: 100-column width, grouped imports, module-granularity imports, and reordered impl items. Use `snake_case` for functions, variables, and test names; `CamelCase` for types and enums; `SCREAMING_SNAKE_CASE` for constants. Prefer small, explicit helpers over ad hoc inline logic in tests and queue-manipulation code.

## Testing Guidelines
This crate relies heavily on integration and property-style testing. Use `#[test]` for targeted regressions and `quickcheck`-based tests for queue invariants and behavior against `VecDeque`. Add new bug reproductions to `tests/bug_cases.rs` when fixing a defect. Name tests after the behavior being verified, for example `transfer_expand_invalid_file_len`.

## Commit & Pull Request Guidelines
Keep commit messages short and imperative. Existing history mixes plain summaries (`add main.rs`) with scoped prefixes (`docs: update`, `build: add CLAUDE.md`); prefer the scoped form when it adds clarity. Pull requests should explain the behavioral change, note any format or compatibility impact, and list the commands used for verification. Link related issues when relevant and include output examples only when a CLI-facing change affects users.

## Safety & Configuration Tips
Do not weaken the durability guarantees in `QueueFile` without documenting the tradeoff. Changes to on-disk format, sync behavior, or capacity growth logic should include regression tests covering reopen and recovery paths.
