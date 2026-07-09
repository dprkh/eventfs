## macOS Development

- On macOS, never run Linux-sensitive Cargo or Rust verification directly on the host. Use `./dev.sh` for checks, tests, examples, benchmark compilation, Clippy, rustfmt checks, and arbitrary Cargo commands.
- Apple `container` is required; never use macFUSE for eventfs verification. Before any repository command, `container` MUST be installed and `container system status --format json` MUST report `"status":"running"`. If it is not running, run `container system start --enable-kernel-install`.
- `./dev.sh` builds the development image from the root `Containerfile`, bind-mounts the repository at `/work`, validates FUSE in the container, and runs the requested command there.

| Task | Command |
| --- | --- |
| Normal full verification | `./dev.sh test` |
| All-target check | `./dev.sh check` (`cargo check --locked --all-targets`) |
| Narrow verification | `./dev.sh lib`, `./dev.sh tests`, `./dev.sh examples`, or `./dev.sh benches` |
| Rustfmt check (host-side exception) | `./dev.sh fmt`; runs `cargo fmt --all -- --check` without a container |
| Other Cargo | `./dev.sh cargo -- <args...>` |
| Resource override via command options | `./dev.sh test --cpus 14 --memory 48G` |
| Image rebuild | `./dev.sh build-image` first, or pass `--build`, e.g. `./dev.sh check --build` |

Container-backed commands reuse the existing image by default. Use host `cargo fmt --all` only for intended formatting edits. Container Cargo caches live under `target/container`; keep them out of commits.

## Source Organization

### Rust source

- Each file MUST order definitions from most to least important; public API definitions MUST precede crate-private and private definitions.
- Public API methods MUST delegate implementation details to crate-private or private functions.
- Within that order, related definitions MUST be grouped by role, with one empty line between groups. Groups MUST NOT mix unrelated domains merely because definitions share the same Rust item kind.
- Related constants MUST be adjacent and use a shared leading namespace prefix.

### Tests

- Test files MUST place user-facing scenario tests before local helper functions.
- Integration test files under `tests/` MUST be vertical slices by user-facing capability, never organized only by technique (such as errors, concurrency, stress, or operations).
- Each slice MUST contain its capability's success, error, edge-case, stress, load, and regression scenarios when applicable.
- Shared integration test infrastructure MAY live in `tests/support`.
