## 1. Header Serialization Extraction

- [x] 1.1 Extract `encode_v1_header` logic out of `commit_header` into a standalone helper function.
- [x] 1.2 Extract `encode_legacy_header` logic out of `commit_header` into a standalone helper function.
- [x] 1.3 Extract `encode_v2_slot` logic out of `commit_header` into a standalone helper function.
- [x] 1.4 Refactor `commit_header` to use the new byte-packing helper functions.
- [x] 1.5 Run the full test suite (`cargo test`) to ensure no file format regressions.

## 2. Refactoring Add Operations

- [x] 2.1 Refactor `add` to call `add_n` internally with a single-element iterator, reducing code duplication.
- [x] 2.2 Remove the redundant `overwrite_on_remove` read-only toggling inside `add_n`'s element loop.
- [x] 2.3 Extract the individual element addition logic inside the `add_n` loop into a clean helper method (e.g., `append_single_element`).
- [x] 2.4 Run `cargo test` to verify batched additions work identically.

## 3. Untangling Remove Operation

- [x] 3.1 Separate the cached offset skipping logic from the erasure size calculation in `remove_n`.
- [x] 3.2 Ensure the physical erase (`ring_erase_logical`) occurs distinctly after the loop calculates the span correctly.
- [x] 3.3 Verify tests still pass, focusing on cache and overwrite cases (`cargo test`).

## 4. Final Validation

- [x] 4.1 Run standard formatting and linting: `cargo fmt` and `cargo clippy`.
- [x] 4.2 Verify all tests pass including those with failure injections if present.