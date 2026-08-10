# Contributing

Contributions are welcome for reproducible bugs, portability improvements, tests, documentation, and measured audio improvements.

## Before submitting

1. Work from a clean branch.
2. Do not commit recordings, model files, credentials, local paths, session data, generated app bundles, or dependency/build directories.
3. Add deterministic tests for behavior changes. Audio changes should include synthetic fixtures or instructions for locally replaying private recordings; private recordings must not enter the repository.
4. Run:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
node --check ui/app.js
npm audit
./scripts/check-public-readiness.sh
```

5. Explain any safety, privacy, permission, network-recovery, or in-car interaction impact in the pull request.

## Design constraints

- Keep the real-time audio callback allocation- and blocking-free.
- Keep control actions idempotent and generation-aware.
- Preserve workspace-write as the default and require explicit consent for Full Mac access.
- Keep SSH key-only and non-interactive.
- Do not make the local narrator an execution authority.
- Avoid UI interaction requirements while a vehicle is moving.
