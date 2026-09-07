# CONTRIBUTING

Thank you for your interest in contributing to the project. Issues, Bug reports or feature request can be made via GitHub issues. For detailed developer information please see the sections below.

In any case please also acknowledge our [Code of Conduct](CODE_OF_CONDUCT.md)


## Developer Contributions Guidance

During local work, use focused checks for the changed behavior and run
`cargo +nightly fmt --all -- --check`. Do not run the general test suite per
edit or commit; documentation-only changes need reference and formatting checks.

After submitting a PR, run the full checks in parallel CI jobs against its final
revision. The required workspace commands are:

```bash
cargo test --workspace --all-targets
cargo +nightly fmt --all -- --check
cargo +nightly clippy --workspace --all-targets -- -D warnings
```

All required PR checks must pass before merge. A failed, partial, or earlier
run is not a pass; rerun affected checks after corrections. Use the pinned
Rust toolchain, independent CI workspaces and bounded aggregate concurrency.

[CI](.github/workflows/ci.yml) is the upstream check reference. It also runs
the all-feature nextest and doctest suites, S3 backend checks, dependency and
shell checks, and deployment recipe checks. The workspace trio is not the entire
CI matrix.

### Workflow

Please create an issue or a draft PR first so contributors can discuss the
approach. Automated contributors must have permission before publishing one.
