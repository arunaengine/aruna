<!-- Repository structure: which crate owns what, and the request and startup paths end to end. -->
<!-- Copyright (c) 2026 The Aruna Contributors -->
<!-- SPDX-License-Identifier: MIT or Apache-2.0 -->

# Repository structure

Where new behavior belongs, and the paths most changes follow. If this guide
disagrees with the code, the code wins; update the guide in the same change.
Root `STYLE.md` owns naming, folder and test rules.

## Four layers

Information takes one path through the workspace:

```
Inputs (api, node triggers, doctor, cli)
   -> Operations (sync state machines)  <->  Drivers (async runner)  <->  Backends (I/O)
```

- An input checks its input, builds one operation and hands it to the drivers.
- An operation decides. It returns effects and reads events. It never waits,
  spawns or does I/O.
- The drivers run the operation. They send each effect to its backend and feed
  the answer back as an event.
- A backend carries out effects. It holds no domain policy.

A timer that fires, a message from a peer node and node startup are inputs too.
They start an operation in the same way as an HTTP request does.

## Crates by layer

| Layer | Crate | Owns |
| --- | --- | --- |
| Inputs | `api` | REST, S3 and MCP transports: input checks, auth, error mapping, OpenAPI |
| Inputs | `aruna` | the node binary: settings, wiring, startup, shutdown |
| Inputs | `aruna-doctor`, `cli` | operator tool on a stopped node; user client |
| Operations | `operations` | use-case state machines by domain |
| Drivers | `operations/src/driver.rs`, `operations/src/effect_adapters/` | the drive loop and effect routing; they move to their own crate `aruna-drivers` at the end of the migration |
| Backends | `storage`, `net`, `blob`, `tasks`, `compute`, `plugins` | Fjall, Iroh and document sync, blob backends, timers, executors, the WASM host |
| Foundations | `core`, `kpabe` | domain types, record formats, keyspaces, effects, events, the `Operation` trait, pure helpers |

Today `operations` also holds async services: handles, workers, queue drains,
the timer map in `operations/src/tasks/incoming/mod.rs`, the peer message map in
`operations/src/sync/incoming.rs` and the plugin broker. They are the part being
migrated. Each is converted into operations that the drivers run, domain by
domain. Do not add new async services there.

## Call flows

- **External request.** The `api` handler checks the input and builds the
  operation. `aruna_operations::driver::drive` calls `start`, sends each effect
  to its backend, calls `step` with the answering event and ends with
  `finalize`. The handler maps the result to a response.
- **Timer.** `tasks` fires a timer key. Today the timer map in
  `operations/src/tasks/incoming/mod.rs` runs the work. In the target the
  trigger table in `operations` names the operation and the drivers run it; a
  queue drain handles one page per run and schedules the next timer as an
  effect.
- **Peer message.** `net` receives a request or a document update. Today the
  map in `operations/src/sync/incoming.rs` handles it. In the target the
  trigger table names the operation and the answer goes back as a net effect.
- **Sub-operation.** An operation emits a sub-operation effect, the drivers run
  the child operation and return its result as an event.
- **Startup.** `aruna` builds the backends and the drivers, then runs the
  startup phases (see Node lifecycle).

## What goes where

| Kind of code | Home |
| --- | --- |
| Domain type, stored record format, key layout | `core`, in the module of its domain |
| Effect and event types | `core/src/effects.rs`, `core/src/events.rs` |
| A decision, a rule, a check of an event against the state | an operation in `operations` |
| A use case with several steps | an operation built from sub-operations |
| Retry after a transaction conflict | one rule in the drivers |
| Background work, queue drains, repair sweeps | a timer trigger and an operation per page |
| Cache, connection pool, concurrency limit | the backend that owns the resource |
| Transport checks, auth, error mapping, OpenAPI | `api` |
| Settings parsing and wiring | `aruna` |
| Shutdown coordination | `tasks` |

Inside `operations`, domains form three groups. A domain may use its own group
and lower groups, and never forms a cycle inside its group:

1. Identity and realm: realm, auth, users, groups, session, device, onboarding,
   secrets, abe, placement, shard, sync, owner index, document repository,
   endpoint screening, forward, vaults, tasks.
2. Data: s3, blob, staging, connectors, replication, distribution, metadata,
   git, shares.
3. Work and integrations: jobs, plugins, assistant, notifications, federation,
   harvest, node.

## The layer guard

`scripts/dev/check_layers.py` runs in `just style`, `just lint` and the CI
style job. It counts async code, references between groups, runner references
into domains, trigger arms that still run async code, real sleeps and silent
skips in tests, duplicate function bodies and internal crate dependencies
outside the target table. A count may not rise above
`scripts/dev/layers_baseline.json`, and a domain listed there as clean may not
hold async code. Raising a count is an explicit edit of that file with the
reason in the pull request. A change that lowers a count lowers the baseline in
the same change.

## Where new files live

- bucket, object, multipart and access operations:
  `operations/src/s3/{bucket,object,multipart,access}/`;
- routes: `api/src/routes/{execution,access,storage}/`; other route domains
  stay flat under `api/src/routes/`;
- persisted records: `core/src/structs/{placement,identity,storage,execution}/`;
- effect adapters: `operations/src/effect_adapters/`;
- test helpers: in a `src/tests/` folder beside the domain when the helper
  family qualifies under the folder threshold, otherwise a hoisted sibling file
  declared with an explicit `#[path]` (`aruna/src/bootstrap_tests.rs`);
- shared integration test fixtures of `operations`: `operations/tests/topology/`
  (realm nodes, realm config, loopback net) and `operations/tests/convergence/`
  (progress waits and hang caps);
- raw fixture assets: the owning crate's `tests/fixtures/`.

## Add an operation

1. Define the input, output, error and a private state enum in
   `operations/src/<area>/<verb>.rs`. Keep `Output` one domain value; failure
   belongs in the error type.
2. Implement `Operation` (`core/src/operation.rs`): `start`, `step`,
   `is_complete`, `finalize`, `abort`, and `expected_error` for ordinary client
   outcomes such as not found. `step` rejects an event that does not fit the
   current state.
3. Test it with state and event combinations and no runtime: the success path,
   every error event, events that do not fit, `abort` in every phase, early
   `finalize`, and the exact effects of each step.
4. Call it from inputs with `drive`; never build raw storage effects in a
   transport when a named operation exists.

## Add a REST route or MCP tool

1. Put a REST handler in `api/src/routes/<domain>/<feature>.rs` for the
   execution, access and storage domains (other domains stay flat) and register
   it in that module's `router()` with a `#[utoipa::path]` attribute.
   `api/src/routes/mod.rs::rest_api` merges the routers, so the runtime router
   and the OpenAPI document cannot diverge.
2. Put an MCP tool in `api/src/mcp/<area>.rs` and register the area router in
   `api/src/mcp/mod.rs::router`. Reuse the operation call of the REST route.
3. Map operation errors to responses explicitly. Test allowed, denied and
   public cases and the error shape.

## Add a background queue or timer

1. Add the `TaskKey` variant in `core/src/task.rs`; stored keys keep their
   bytes, so new variants are appended.
2. Today the work is wired in the timer map in
   `operations/src/tasks/incoming/mod.rs`. Write the work itself as an
   operation that handles one page and schedules the next run as an effect, so
   the arm only starts it.
3. Production startup starts queues in `aruna/src/startup/background.rs`
   (`STARTUP_PHASES`).
4. Test the due and not-due decision with explicit times.

## Add a compute backend

1. Implement `ExecutorBackend` in `compute/src/executor/<backend>.rs`.
2. Add the typed settings to `aruna/src/compute_setup/mod.rs`, read them in
   `aruna/src/compute_setup/settings.rs::collect` and build the registry in
   `aruna/src/compute_setup/<backend>.rs`.
3. Add the feature to `compute/Cargo.toml` and `aruna/Cargo.toml` and check
   every selection as `just check` does.

## Add a persisted record

1. Add the keyspace constant in `core/src/keyspaces.rs` and the record type
   with `to_bytes` and `from_bytes` in the matching family under
   `core/src/structs/`.
2. Never change an existing encoding silently.
3. Test round trips and malformed bytes at the record boundary, and one real
   temporary-storage persistence test.

## Node lifecycle

`aruna/src/main.rs` starts the Tokio runtime and calls
`aruna::application::run_node`, which runs in order:

1. `startup::resources::acquire` opens storage, resolves identity, enrollment
   and settings, then builds the network, metadata, blob, compute, operations
   and task resources. On failure it tears down only what it acquired.
2. `startup::realm::prepare` replays metadata and prepares the realm mode
   (initialize, join or provision).
3. `startup::listeners::bind` binds REST, S3, portal and session S3.
4. `startup::background::start` announces readiness and runs the startup phases
   in `STARTUP_PHASES` order.
5. `application::run_node` supervises ingress exits and the termination signal,
   then runs `shutdown::NodeShutdown::run`.

## Tests, measurements and commands

- `just test-fast` runs the no-I/O `state_machine_tests`, `pure_tests` and
  `decision_tests` modules of `aruna-core` and `aruna-operations`. Cargo still
  compiles both packages and their dependencies.
- Runtime, storage and multi-node behavior lives in ordinary test modules,
  `operations/tests` and `aruna/tests`. Tests wait on progress with a hang cap
  and assert no speed.
- Speed is measured on request with
  `cargo run --release -p aruna-operations --example measure`, which prints
  rates for fixed workloads on real backends in temporary folders. Peak memory
  is read with `/usr/bin/time -v` around the built binary.
- `core/tests/type_sizes.rs` fixes upper limits for the sizes of `Effect`,
  `Event` and `Effects`; a change may lower them.
- Full local gate: `just lint`, `just check`, `just test` (see
  `CONTRIBUTING.md`). CI runs the full matrix after a pull request is opened.
