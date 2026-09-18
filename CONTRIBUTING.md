<!-- Contributor guide: where to report issues and how to work on the code. -->
<!-- Copyright (c) 2026 The Aruna Contributors -->
<!-- SPDX-License-Identifier: MIT or Apache-2.0 -->

# CONTRIBUTING

Thank you for your interest in contributing to the project. Issues, Bug reports or feature request can be made via GitHub issues. For detailed developer information please see the sections below.

In any case please also acknowledge our [Code of Conduct](CODE_OF_CONDUCT.md)


## Developer Contributions Guidance

Where new behavior belongs, and the request and startup paths end to end, are
described in [docs/source-guide.md](docs/source-guide.md).

During local work, use focused checks for the changed behavior and run
`just lint`, which runs the style checker self-tests and checker together with
the same formatting and Clippy commands as CI. The lint recipe
pins `nightly-2026-09-14`; install it with
`rustup toolchain install nightly-2026-09-14 -c rustfmt -c clippy` if it is
missing. Builds use stable `1.97.1` from `rust-toolchain.toml`. Do not run the
general test suite per edit or commit; documentation-only changes need
reference and formatting checks.

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

### Structure and naming

Naming, folder, comment, and fixture rules are owned by root
[STYLE.md](STYLE.md), not by this file; apply that policy directly instead of
keeping a local copy. The root policy is the single owner of the folder
threshold, the shared-prefix exception, term limits, and hoisting.

- **Behavior homes.** Bucket, object, multipart, and access operations belong
  under `operations/src/s3/<family>/`; routes under
  `api/src/routes/{execution,access,storage}/`; records under
  `core/src/structs/{placement,identity,storage,execution}/`; effect adapters
  under `operations/src/effect_adapters/`. See
  [docs/source-guide.md](docs/source-guide.md) for the full map.

### Workflow

Please create an issue or a draft PR first so contributors can discuss the
approach. Automated contributors must have permission before publishing one.
