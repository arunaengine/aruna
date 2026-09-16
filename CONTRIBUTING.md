# CONTRIBUTING

Thank you for your interest in contributing to the project. Issues, Bug reports or feature request can be made via GitHub issues. For detailed developer information please see the sections below.

In any case please also acknowledge our [Code of Conduct](CODE_OF_CONDUCT.md)


## Developer Contributions Guidance

Where new behavior belongs, and the request and startup paths end to end, are
described in [docs/source-guide.md](docs/source-guide.md).

During local work, use focused checks for the changed behavior and run
`just lint` (the same formatting and Clippy commands as CI). The lint recipe
pins `nightly-2026-09-14`; install it with
`rustup toolchain install nightly-2026-09-14 -c rustfmt -c clippy` if it is
missing. Builds use stable `1.98.1` from `rust-toolchain.toml`. Do not run the
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

- **Folder threshold.** An optional source subfolder needs at least five real,
  related, directly contained files, excluding an optional `mod.rs`; child
  directories do not count as entries. Three or more related sibling files
  sharing a meaningful domain prefix must group into the matching submodule,
  and the resulting three- or four-file domain is a valid exception to the
  five-file minimum; remove the prefix from the child filenames. A one-file
  folder is hoisted into its parent. Explicit exceptions remain for required
  package or tooling roots (workspace and package roots, Cargo `src`,
  `.github/workflows`, `.cargo`, `.config`), genuine integration-test roots,
  and the designated `tests/fixtures` asset root.
- **Three-term names.** Functions, tests, types, traits, aliases, modules,
  filenames, fields, variants, and constants use at most three terms. Count
  underscore-separated or CamelCase words; `HTTP`, `UUID`, `S3`, `SHA256`,
  `OIDC`, and `RO-Crate` each count once. Do not invent abbreviations or fuse
  words.

  | Kind | Good | Bad |
  | --- | --- | --- |
  | function | `drive_effects` | `drive_effects_without_io_or_deadline` |
  | test | `stop_between_phases` | `stop_between_phases_prevents_later_phases` |
  | type | `GetBucketOperation` | `GetBucketInfoOperation` |
  | trait | `ArunaValidationState` | `ArunaBearerTokenValidationState` |
  | alias | `ObjectCursor` | `MetadataObjectSearchCursor` |
  | module | `effect_adapters` | `effect_adapters_for_operations` |
  | filename | `part_upload.rs` | `multipart_part_upload_operation.rs` |
  | field | `bucket_name` | `bucket_name_for_the_operation` |
  | variant | `ReconciliationRequired` | `ReconciliationRequiredForClaim` |
  | constant | `BLOB_CLEANUP_AFTER` | `BLOB_CLEANUP_AFTER_RETENTION_WINDOW` |

- **Hoisted tests.** A unit test file keeps its logical module name beside its
  owner:

  ```rust
  #[cfg(test)]
  #[path = "owner_tests.rs"]
  mod <logical_name>;
  ```

  The `#[path]` line belongs directly in the owner source file, outside any
  inline module. Keep the `pure_tests`, `state_machine_tests`, and
  `decision_tests` names; a child module does not require a private one-file
  directory.
- **Comments.** A logical comment (line, block, or Rustdoc) is at most three
  physical lines after formatting; one or two are preferred. Longer rationale
  belongs in `docs/` or the commit message.
- **Fixtures.** Raw assets live under `tests/fixtures/` in the owning crate
  (`operations/tests/fixtures/`, `blob/tests/fixtures/`). Fixture helper code
  lives in a `src/tests/` folder beside its domain when the helper family
  qualifies under the folder threshold (five direct files, or a valid
  shared-prefix domain; `api/src/tests/assistant.rs`,
  `operations/src/tests/s3.rs`);
  otherwise it stays a hoisted sibling file declared with an explicit `#[path]`
  (`aruna/src/bootstrap_tests.rs`).
- **Behavior homes.** Bucket, object, multipart, and access operations belong
  under `operations/src/s3/<family>/`; routes under
  `api/src/routes/{execution,access,storage}/`; records under
  `core/src/structs/{placement,identity,storage,execution}/`; effect adapters
  under `operations/src/effect_adapters/`. See
  [docs/source-guide.md](docs/source-guide.md) for the full map.

### Workflow

Please create an issue or a draft PR first so contributors can discuss the
approach. Automated contributors must have permission before publishing one.
