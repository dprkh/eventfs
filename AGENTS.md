## Development Under macOS

- When developing on macOS, do not run Linux-sensitive Cargo or Rust verification commands directly on the host.
- Use `./dev.sh` for checks, tests, examples, benchmark compilation, clippy, rustfmt checks, and arbitrary Cargo commands.
- The required host runtime is Apple `container`; do not use macFUSE for eventfs verification.
- Before running repo commands, Apple `container` must be installed and `container system status --format json` must report `"status":"running"`. If it is not running, start it with `container system start --enable-kernel-install`.
- `./dev.sh` builds the development image from the root `Containerfile`, bind-mounts the repo at `/work`, validates FUSE inside the container, then runs the requested command in that container.
- The normal full verification command is `./dev.sh test`.
- Use `./dev.sh check` for `cargo check --locked --all-targets`.
- Use `./dev.sh lib`, `./dev.sh tests`, `./dev.sh examples`, or `./dev.sh benches` for narrower verification.
- `./dev.sh fmt` is the host-side formatting exception and runs `cargo fmt --all -- --check` without the container; use host `cargo fmt --all` only when formatting edits are intended.
- Use `./dev.sh cargo -- <args...>` for any other Cargo command.
- Override container resources with command options, for example `./dev.sh test --cpus 14 --memory 48G`.
- Container-backed commands reuse the existing image by default. Rebuild first with `./dev.sh build-image`, or pass `--build`, for example `./dev.sh check --build`.
- Container Cargo caches live under `target/container`; keep them out of commits.

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
