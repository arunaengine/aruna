<!-- Source guide: which crate owns what, and the request and startup paths end to end. -->
<!-- Copyright (c) 2026 The Aruna Contributors -->
<!-- SPDX-License-Identifier: MIT or Apache-2.0 -->

# Source guide

Where new behavior belongs, and the two paths most changes follow. If this
guide disagrees with the code, the code wins; update the guide in the same
change.

## Crates

- `core`: domain types, effects, events, keyspaces, the `Operation` trait.
- `operations`: state machines and their adapters (metadata, sync, jobs, S3).
- `storage`, `blob`, `net`, `tasks`, `compute`: I/O-facing subsystems.
- `api`: REST, MCP, and S3 transports over `operations`.
- `aruna`: the node binary: settings, startup, shutdown, process entry.
- `aruna-doctor`: operator diagnostics.

## Where new files live

Add behavior to the family that already owns it:

- bucket, object, multipart, and access operations:
  `operations/src/s3/{bucket,object,multipart,access}/`;
- routes: `api/src/routes/{execution,access,storage}/`; other route domains
  stay flat under `api/src/routes/`;
- persisted records: `core/src/structs/{placement,identity,storage,execution}/`;
- effect adapters: `operations/src/effect_adapters/`;
- test helpers: in a `src/tests/` folder beside the domain when the helper
  family qualifies under the folder threshold (five direct files, or a valid
  shared-prefix domain; `api/src/tests/assistant.rs`,
  `operations/src/tests/s3.rs`), otherwise a hoisted sibling file declared
  with an explicit `#[path]` (`aruna/src/bootstrap_tests.rs`);
- raw fixture assets: the owning crate's `tests/fixtures/`
  (`operations/tests/fixtures/`, `blob/tests/fixtures/`).

A new subfolder needs at least five real, directly contained files besides
`mod.rs`, unless three or more related siblings share a meaningful domain
prefix and group into it; root `STYLE.md` owns the threshold, hoisting, and
naming rules.

## One request, end to end

1. A transport accepts bytes and authenticates the caller:
   `api/src/server/mod.rs` (REST), `api/src/s3/server.rs` (S3),
   `api/src/mcp/mod.rs` (MCP).
2. The transport handler builds a request value and calls one named operation
   through `aruna_operations::driver::drive`, for example
   `api/src/s3/auth.rs` calling `GetBucketOperation`
   (`operations/src/s3/bucket/get.rs`).
3. `drive` calls `start`/`step` on the operation. The operation returns
   `Effect` values (`core/src/effects.rs`) and consumes `Event` values
   (`core/src/events.rs`). It never performs I/O itself.
4. The effect adapters in `operations/src/effect_adapters/` execute storage, blob,
   net, metadata, and task effects and feed the results back as events.
5. `finalize` maps the completed state to `Result<Output, Error>`; the
   transport maps that to a response.

A deterministic operation test does all of this by hand: construct, `start`,
assert effects, `step` explicit events, assert effects, `finalize`.

## Add an operation

1. Define the input, output, error, and a private state enum in
   `operations/src/<area>/<verb>.rs`. Keep `Output` one domain value: `Option<T>`
   only where absence is a real success, never a nested `Option<Result<..>>`;
   failure belongs in the error type.
2. Implement `Operation` (`core/src/operation.rs`): `start`, `step`,
   `is_complete`, `finalize`, `abort`, and `expected_error` for ordinary
   client outcomes such as not-found.
3. Add a child `state_machine_tests` module with fixed inputs covering the
   accepted path, absence, malformed records, storage errors, wrong events,
   and premature finalization.
4. Call it from transports with `drive`; never construct raw storage effect
   handles in a transport when a named operation exists.

## Add a REST route

1. Put the handler in `api/src/routes/<domain>/<feature>.rs` for the
   execution, access, and storage domains (other domains stay flat as
   `api/src/routes/<feature>.rs`) and register it in that module's `router()`
   with a `#[utoipa::path]` attribute.
2. `api/src/routes/mod.rs::rest_api` merges the module routers; the runtime
   router and the OpenAPI document come from that one assembly, so they cannot
   diverge.
3. Convert inputs to an operation call, authenticate through the existing
   middleware, and map operation errors to `ServerError` explicitly.
4. Test the handler in-process: allowed, denied (wrong actor, scope, realm),
   intentionally public, and the error shape.

## Add an MCP tool

1. Add the `#[tool]` method to the router in `api/src/mcp/<area>.rs` and
   register the area router in `api/src/mcp/mod.rs::router`.
2. Reuse the same operation call as REST; do not call REST handlers from MCP.
3. Map operation errors to MCP results with the area's existing mapper.
4. Test the tool with a wrong-identity case that proves the privileged
   operation was not reached.

## Add a background queue or timer

1. Add the `TaskKey` variant in `core/src/task.rs`.
2. Handle `TaskEvent` for it in `operations/src/tasks/incoming/mod.rs` and
   register the handler/queue with the shared `TaskHandle` lifecycle owner:
   `install_task_queues` installs the handler, and
   `TaskQueues::restore_and_start` restores durable timers and starts the
   re-arm loop under the caller's `Shutdown`
   (`operations/src/tasks/incoming/restore.rs`).
3. Production startup starts queues in `aruna/src/startup/background.rs`
   (`STARTUP_PHASES`); put restore/install work there, not in the task crate
   root.
4. Test the due/not-due decision with explicit times; test cancellation and
   ownership with a small runtime boundary.

## Add a compute backend

1. Implement `ExecutorBackend` in `compute/src/executor/<backend>.rs`.
2. Add the backend's typed settings to `aruna/src/compute_setup/mod.rs` and
   read them once in `aruna/src/compute_setup/settings.rs::collect`.
3. Add the feature to `compute/Cargo.toml` and `aruna/Cargo.toml`, gate the
   module at the backend boundary, build the registry in
   `aruna/src/compute_setup/<backend>.rs`.
4. Check every selection locally, feature-explicit and per package, as
   `just check` and CI do:
   `cargo check -p aruna-compute --all-targets --no-default-features --features <backend>`
   `cargo check -p aruna --all-targets --no-default-features --features <backend>`.
5. Test path/planning refusals with fixed data; keep real daemon integration
   tests separate and few.

## Add a persisted record

1. Add the keyspace constant in `core/src/keyspaces.rs` and the record type
   with `to_bytes`/`from_bytes` in the matching family under
   `core/src/structs/{placement,identity,storage,execution}/`.
2. Never change an existing encoding silently; add a field with a default or a
   versioned record and keep old fixtures decoding.
3. Test round-trips and malformed bytes at the record boundary, and one real
   temporary-storage persistence test for the adapter.

## Node lifecycle

`aruna/src/main.rs` (process entry) starts the Tokio runtime and calls
`aruna::application::run_node`, which runs in order:

1. `startup::resources::acquire` opens storage, then resolves identity,
   enrollment, and settings inside one owned boundary before building the
   network, metadata, blob, compute, ops, and task resources; on failure or an
   accepted stop it tears down only what it acquired, reports a clean
   cancellation only when that teardown released every owner, and an incomplete
   teardown is a nonzero startup error.
2. `startup::realm::prepare` replays metadata and prepares the realm mode
   (initialize, join, or provision).
3. `startup::listeners::bind` binds REST, S3, portal, and session S3.
4. `startup::background::start` announces readiness and then runs the durable
   startup phases in `STARTUP_PHASES` order.
5. `application::run_node` supervises ingress exits and the termination signal,
   then runs `shutdown::NodeShutdown::run` (ingress, admissions, tasks, jobs,
   background, network, metadata, blob, storage) before the owner wipe path.

Startup instrumentation lives in `aruna/src/startup/test_hooks.rs`; each hook
must name its consumer in `aruna/tests/observability.rs`.

## Tests and commands

- `just test-fast` selects the `--lib` targets of `aruna-core` and
  `aruna-operations` and executes the `state_machine_tests`, `pure_tests`,
  and `decision_tests` modules plus the pure `reducer::tests` family. Cargo
  still compiles both selected packages and their transitive dependencies,
  and the selected targets run only those name-filtered tests; a narrow
  selection is not a narrow build. `just test-fast-workspace` widens the
  target set under the same names. These tests use no runtime, storage,
  network, process, or environment mutation, and a module with one of those
  names must keep the guarantee: filesystem, database, or discovery coverage
  belongs in an ordinary `tests` module beside it. The selection is not
  evidence for transport, runtime, storage, process, or environment changes,
  which need focused module runs recorded against the exact revision.
- Runtime, storage, and multi-node behavior lives in the ordinary test modules
  and `aruna/tests`.
- Focused loop: `cargo nextest run -p <crate> --lib --locked --profile fast`.
- Full local gate: `just lint`, `just check`, `just test` (see
  `CONTRIBUTING.md`). CI runs the full matrix after a pull request is opened.
