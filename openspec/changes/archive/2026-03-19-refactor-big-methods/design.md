## Context

The `QueueFile` struct and its internal types are all defined within `src/lib.rs`. Over time, a few core methods (`commit_header`, `add_n`, `add`, `remove_n`) have absorbed excessive complexity due to:
- Supporting three different header formats (Legacy, V1, V2).
- Maintaining synchronous file I/O intertwined with ring buffer mathematical logic.
- Accumulating redundant state management.

## Goals / Non-Goals

**Goals:**
- Break down monolithic functions into smaller, single-purpose helper functions.
- Simplify `commit_header` by abstracting out the format-specific byte packing logic.
- Simplify `add_n` by extracting the single-element append operations.
- Refactor `add` to be a straightforward wrapper around `add_n` (or its shared helper).
- Refactor `remove_n` to clearly separate skipping cached elements from the physical erasure logic.

**Non-Goals:**
- Changing any publicly observable behavior.
- Modifying the underlying file formats.
- Rewriting the entirety of `lib.rs` into multiple files (this change focuses on method-level simplification; module extraction may happen in a future change).

## Decisions

- **Header Serialization Abstraction**: Instead of maintaining a large `match` block inside `commit_header` that handles byte packing and limits, we'll create format-specific methods (e.g., `build_v2_slot_bytes`, `build_v1_header_bytes`, `build_legacy_header_bytes`) that purely take metadata and return the serialized byte arrays. Then `commit_header` will just handle writing the bytes and updating the generation state.
- **Append Logic Extraction**: `add_n` will use a helper method `append_elements` that iterates through the provided elements, writes each, and updates the `queue_file` state. `add` will simply call `add_n` with a single-element iterator, reducing code duplication.
- **Redundant State Removal**: We will remove the `overwrite_on_remove` flag toggling inside `add_n`'s internal loop, as it is a read-only parameter during addition.

## Risks / Trade-offs

- [Risk] Accidental modification of file format serialization -> We rely heavily on the existing test suite (unit tests and integration/regression tests) to catch any byte misalignments. We must ensure we test both the Legacy and V2 paths completely.
- [Risk] Performance regression due to abstraction -> The extracted byte-packing functions will be simple inlineable functions, avoiding any measurable performance impact.
