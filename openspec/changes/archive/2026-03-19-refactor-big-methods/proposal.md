## Why

The `src/lib.rs` file has grown to over 2500 lines, with several core methods (`commit_header`, `add`, `add_n`, `remove_n`) becoming overly complex, duplicating logic, and mixing multiple concerns (e.g., metadata calculation, file I/O, format-specific byte packing). This complexity makes the codebase harder to maintain, understand, and safely modify. Refactoring these methods will improve code readability, reduce duplication, and lower the risk of bugs during future changes.

## What Changes

- Extract header byte packing logic from `commit_header` into specialized helper functions for each format (Legacy, V1, V2).
- Refactor `add` to reuse the logic in `add_n` (or extract their shared appending logic into a clean helper method).
- Remove redundant state toggling (like `overwrite_on_remove` inside loops) where it has no effect.
- Untangle `remove_n` by separating the logic for skipping cached elements from the logic for secure erasure of the ring buffer.

## Capabilities

### New Capabilities
None.

### Modified Capabilities
None. This is a purely internal structural and logic refactoring. The public API and observed behavior will remain identical.

## Impact

- **Code:** `src/lib.rs` will be heavily modified internally.
- **APIs:** The public API of `QueueFile` will remain completely unchanged.
- **Dependencies:** No new dependencies introduced.
- **Systems:** Data durability and file format guarantees must be strictly preserved. Existing unit and regression tests must all pass to ensure correctness.