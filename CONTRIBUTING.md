# CONTRIBUTING

Thank you for your interest in contributing to the project. Issues, Bug reports or feature request can be made via GitHub issues. For detailed developer information please see the sections below.

In any case please also acknowledge our [Code of Conduct](CODE_OF_CONDUCT.md)


## Developer Contributions Guidance

During local work, use focused checks for the changed behavior and run
`just lint` (the same formatting and Clippy commands as CI). The lint recipe
pins `nightly-2026-08-23`; install it with
`rustup toolchain install nightly-2026-08-23 -c rustfmt -c clippy` if it is
missing. Do not run the general test suite per edit or commit;
documentation-only changes need reference and formatting checks.

After submitting a PR, run the full checks in parallel CI jobs against its final
revision. The same commands are available locally: `just check` compiles every
supported feature selection, and `just test` runs the nextest and doctest
commands (it requires `cargo-nextest`).

All required PR checks must pass before merge. A failed, partial, or earlier
run is not a pass; rerun affected checks after corrections. Use the pinned
Rust toolchain, independent CI workspaces and bounded aggregate concurrency.

[CI](.github/workflows/ci.yml) is the upstream check reference. It also runs
the S3 and Docker backend checks, dependency and shell checks, and deployment
recipe checks. The local recipes are not the entire CI matrix.

### Workflow

Please create an issue or a draft PR first so contributors can discuss the
approach. Automated contributors must have permission before publishing one.
