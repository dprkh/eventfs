Never use your own knowledge. When planning, read `SPEC.md` fully.

## Source Organization

- Rust source files MUST order definitions from most important to least important.
- Public API definitions MUST appear before crate-private and private definitions in the same file.
- Public API methods MUST delegate implementation details to crate-private or private functions.
- Within the required importance and visibility order, source files MUST group related definitions by role.
- Definition groups MUST be separated from other definition groups by one empty line.
- Related constants MUST be adjacent and use a shared leading namespace prefix.
- Definition groups MUST NOT mix unrelated domains only because definitions share the same Rust item kind.
- Test files MUST place user-facing scenario tests before local helper functions.
- Integration test files under `tests/` MUST be organized as vertical slices by user-facing capability.
- Each vertical slice integration test file MUST contain that capability's success, error, edge-case, stress, load, and regression scenarios when applicable.
- Integration test files MUST NOT be organized only by test technique, such as errors, concurrency, stress, or operations.
- Shared integration test infrastructure MAY live under `tests/support`.
