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

## Top-level folders

| Folder | Holds |
| --- | --- |
| `apps/` | runnable binaries: node, doctor, cli, registry, compute helper; the applet builder image |
| `frontends/` | external client protocols and wire DTOs |
| `domain/` | pure sans-IO models, crypto, contracts and state machines |
| `runtime/` | async I/O execution, OS interfaces and effect backends |
| `plugins/` | plugin SDKs, the shared WIT contract, examples, fixture and templates |
| `testing/` | test helpers that only dev-dependencies use |

## Crates by layer

| Layer | Crate | Owns |
| --- | --- | --- |
| Inputs | `frontends/api`, `frontends/wire` | REST, S3 and MCP transports: input checks, auth, error mapping, OpenAPI, wire DTOs |
| Inputs | `frontends/smb`, `frontends/nfs`, `frontends/admission` | embedded SMB 3.1.1 and NFSv4.1 file-share servers and shared frontend IP/principal admission limits |
| Inputs | `frontends/gateway` | the stateless applet gateway (`aruna applet-gateway`): serves applet hosts through the node's edge routes |
| Inputs | `apps/applet-builder` | the applet builder image and its starter template; no Rust crate |
| Inputs | `apps/node`, `runtime/config` | the node binary: settings, portal assets, wiring, startup, shutdown, trigger shells |
| Inputs | `apps/doctor`, `apps/cli`, `apps/registry` | operator tool on a stopped node; user client; standalone identifier registry |
| Operations | `domain/ops-identity`, `domain/ops-data`, `domain/ops-work` | pure sans-IO use-case state machines in three layer crates (`aruna-ops-identity` -> `aruna-ops-data` -> `aruna-ops-work`, which also holds the trigger tables) |
| Drivers | `runtime/runner` (`aruna-runner`) | the drive loop, effect routing to the backends and the job runtime |
| Backends | `runtime/storage`, `runtime/p2p`, `runtime/blob`, `runtime/rdf-graph`, `runtime/scheduler`, `runtime/compute`, `runtime/egress`, `runtime/wasm-host`, `runtime/vfs` | Fjall, Iroh and document sync, blob backends, RDF/RO-Crate graph store, timers, executors, egress HTTP/SMTP, the WASM host, local VFS staging/locking/fencing |
| Foundations | `domain/core`, `domain/kpabe`, `domain/plugin-contract` | domain types, record formats, keyspaces, effects, events, the `Operation` trait, pure helpers, plugin manifests/recipes |
| Foundations | `runtime/clock` | host clocks and the OS random source for the runner, backends and inputs; never core or operations |
| Tests | `testing/harness` (`aruna-test-harness`) | test helpers that drive operations through the drivers; only dev-dependencies use it |

The three operations crates hold only synchronous sans-IO state machines;
`domain/ops-work` also holds the trigger tables (`domain/ops-work/src/triggers.rs`).
`aruna-ops-work` is no longer a facade: callers import each operation from the crate
that owns it (`aruna-ops-identity`, `aruna-ops-data` or `aruna-ops-work`). An
operations crate never depends on a higher one, also not as a dev-dependency. The small number of remaining unconverted timer
and peer arms live in the `apps/node` trigger shells (`apps/node/src/tasks/incoming/mod.rs`
and `apps/node/src/inbound.rs`) and are tracked by `scripts/dev/layers_baseline.json`.
Do not add new async services to the operations crates.

## Call flows

- **External request.** The `frontends/api` handler checks the input and builds the
  operation. `aruna_runner::driver::drive` wraps it in `CallRun`, calls `first`,
  sends each effect to its backend, calls `next` with the answering result, and
  returns the output or error. The handler maps the result to a response.
- **Timer.** `runtime/scheduler` fires a timer key. The shell in
  `apps/node/src/tasks/incoming/mod.rs` looks up `timer_operation` in
  `domain/ops-work/src/triggers.rs` and drives it; a queue drain handles one page per
  run and returns its next schedule plan.
- **Peer message.** `runtime/p2p` receives a request or a document update. The shell in
  `apps/node/src/inbound.rs` looks up `peer_operation` in
  `domain/ops-work/src/triggers.rs`, drives it, and writes the answer back on the stream.
- **Sub-operation.** An operation emits a sub-operation effect, the drivers run
  the child operation and return its result as an event.
- **Startup.** `apps/node` builds the backends and the drivers, then runs the
  startup phases (see Node lifecycle).

## What goes where

| Kind of code | Home |
| --- | --- |
| Domain type, stored record format, key layout | `domain/core`, in the module of its domain |
| Effect and event types | `domain/core/src/effects.rs`, `domain/core/src/events.rs` |
| A decision, a rule, a check of an event against the state | an operation in the `domain/ops-*` crate of its domain group |
| A use case with several steps | an operation built from sub-operations |
| Retry after a transaction conflict | one rule in the drivers |
| Background work, queue drains, repair sweeps | a timer trigger and an operation per page |
| Cache, connection pool, concurrency limit | the backend that owns the resource |
| Transport checks, auth, error mapping, OpenAPI | `frontends/api` |
| Settings parsing and wiring | `apps/node` |
| Shutdown coordination | `runtime/scheduler` |

The operations domains form three groups, one per crate. A domain may use its own
group and lower groups, and never forms a cycle inside its group:

1. Identity and realm (`domain/ops-identity`): realm, auth, users, groups, session, device, onboarding,
   secrets, abe, placement, shard, sync, owner index, document repository,
   endpoint screening, forward, vaults, tasks.
2. Data (`domain/ops-data`): s3, blob, staging, connectors, replication, distribution, metadata,
   git, shares, vfs.
3. Work and integrations (`domain/ops-work`): jobs, plugins, assistant, notifications, federation,
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

The guard also enforces the layer order derived from each crate's top-level folder
(`LAYERS` in the script), so a new crate needs no extra rule. For domain crates,
the rule applies to production and build dependencies; tests may still use the
runner, backends and test harness as dev-dependencies, while higher operations
crates remain forbidden even as dev-dependencies. The current state:

- `StoragePort` in `domain/core/src/handle.rs` lets `runtime/blob` and `runtime/p2p`
  use storage without a production dependency on `runtime/storage`; only their tests
  name it.
- `runtime/config` depends only on `aruna-core`, `aruna-clock` and `aruna-wire`, not
  on storage, p2p or irokle.
- The `crate_allow` list in `scripts/dev/layers_baseline.json` is empty, so
  `apps/doctor` and every other crate stay inside the target table.
- `frontends/api` production code builds no raw storage, net or blob effects
  (`api_raw_effects: 0`), and tests hold no real sleeps (`test_sleeps: 0`).
- Two trigger shell arms still run async code (`trigger_arms: 2`): `Alpn::Bao` and
  `Alpn::DocumentSync` in `apps/node/src/inbound.rs`.

## Where new files live

- bucket, object, multipart and access operations:
  `domain/ops-data/src/s3/{bucket,object,multipart,access}/`;
- routes: a route domain with several features gets a folder under
  `frontends/api/src/routes/` (for example `execution/`, `access/`, `storage/` and
  `info/`); a single-file domain stays flat there;
- S3 handlers: `frontends/api/src/s3/service/{buckets,lists,reads,writes,uploads}.rs`;
- persisted records: `domain/core/src/structs/{placement,identity,storage,execution}/`;
- effect adapters: `runtime/runner/src/effect_adapters/`;
- test helpers: in a `src/tests/` folder beside the domain when the helper
  family qualifies under the folder threshold, otherwise a hoisted sibling file
  declared with an explicit `#[path]` (`apps/node/src/bootstrap_tests.rs`);
- shared node test fixtures: `apps/node/tests/operations/{topology,convergence}/`
  (realm nodes, realm config and loopback net; progress waits and hang caps);
- raw fixture assets: the owning crate's `tests/fixtures/`.

## Add an operation

1. Define the input, output, error and a private state enum in
   `domain/ops-<group>/src/<area>/<verb>.rs`. Keep `Output` one domain value; failure
   belongs in the error type.
2. Implement `Operation` (`domain/core/src/operation.rs`): `first`, `next`, and as
   needed `transaction`, `hold_scope`, `abort`, `stamp`, `seed`, `on_conflict`,
   and `expected_error` for ordinary client outcomes such as not found. `next`
   rejects any result that does not fit the current state.
3. Test it with state and result combinations and no runtime: the success path,
   every error result, unexpected results, `abort` in every phase, and the exact
   calls of each step.
4. Call it from inputs with `drive`; never build raw storage effects in a
   transport when a named operation exists.

## Add a REST route or MCP tool

1. Put a REST handler in `frontends/api/src/routes/<domain>/<feature>.rs` for the
   execution, access and storage domains (other domains stay flat) and register
   it in that module's `router()` with a `#[utoipa::path]` attribute.
   `frontends/api/src/routes/mod.rs::rest_api` merges the routers, so the runtime router
   and the OpenAPI document cannot diverge.
2. Put an MCP tool in `frontends/api/src/mcp/<area>.rs` and register the area router in
   `frontends/api/src/mcp/mod.rs::router`. Reuse the operation call of the REST route.
3. Map operation errors to responses explicitly, with a `From<Error> for ServerError`
   impl beside the handler. Test allowed, denied and
   public cases and the error shape.

## Add a background queue or timer

1. Add the `TaskKey` variant in `domain/core/src/task.rs`; stored keys keep their
   bytes, so new variants are appended.
2. Wire the timer operation in `domain/ops-work/src/triggers.rs` (`timer_operation`)
   so `apps/node/src/tasks/incoming/mod.rs` drives it without adding a new trigger
   arm. Write the work itself as an operation that handles one page and returns
   its `TimerPlan`.
3. Production startup starts queues in `apps/node/src/startup/background.rs`
   (`STARTUP_PHASES`).
4. Test the due and not-due decision with explicit times.

## Add a compute backend

1. Implement `ExecutorBackend` in `runtime/compute/src/executor/<backend>.rs`.
2. Add the typed settings to `apps/node/src/compute_setup/mod.rs`, read them in
   `apps/node/src/compute_setup/settings.rs::collect` and build the registry in
   `apps/node/src/compute_setup/<backend>.rs`.
3. Add the feature to `runtime/compute/Cargo.toml` and `apps/node/Cargo.toml` and check
   every selection as `just check` does.

## Add a persisted record

1. Add the keyspace constant in `domain/core/src/keyspaces.rs` and the record type
   with `to_bytes` and `from_bytes` in the matching family under
   `domain/core/src/structs/`.
2. Never change an existing encoding silently.
3. Test round trips and malformed bytes at the record boundary, and one real
   temporary-storage persistence test.

## Node lifecycle

`apps/node/src/main.rs` starts the Tokio runtime and calls
`aruna::application::run_node`, which runs in order:

1. `startup::resources::acquire` opens storage, resolves identity, enrollment
   and settings, then builds the network, metadata, blob, compute, operations
   and task resources. On failure it tears down only what it acquired.
2. `startup::realm::prepare` replays metadata and prepares the realm mode
   (initialize, join or provision).
3. `startup::listeners::bind` binds REST, S3, portal, session S3 and optional SMB/NFS listeners.
4. `startup::background::start` announces readiness and runs the startup phases
   in `STARTUP_PHASES` order.
5. `application::run_node` supervises ingress exits and the termination signal,
   then runs `shutdown::NodeShutdown::run`.

## Tests, measurements and commands

- `just test-fast` runs the library tests of `aruna-core` and the three
  `aruna-ops-*` crates with the `fast` nextest profile.
- Most operation tests in `domain/ops-*` and route tests in `frontends/api` run on
  `MemoryStorage::open()` (`runtime/storage/src/storage/memory.rs`), with no disk.
  Tests of durability, reopen, readiness, backup, filesystem and blob fixtures, and
  manual timing profiles still use `FjallStorage` in temporary folders.
- Runtime, storage and multi-node behavior of the node lives in one test binary,
  `apps/node/tests/regimes.rs`. It groups the tests into seven modules:
  `identity_regime`, `storage_regime`, `federation_regime`, `metadata_regime`,
  `restart_regime`, `compute_regime` and `ops_regime`. Filter one with a test name
  prefix, for example `cargo nextest run -p aruna --test regimes storage_regime::`.
  Tests wait on progress with a hang cap and assert no speed.
- Speed is measured on request with
  `cargo run --release -p aruna --example measure`, which prints
  rates for fixed workloads on real backends in temporary folders. Peak memory
  is read with `/usr/bin/time -v` around the built binary.
- `domain/core/tests/type_sizes.rs` fixes upper limits for the sizes of `Effect`,
  `Event` and `Effects`; a change may lower them.
- Full local gate: `just lint`, `just check`, `just test` (see
  `CONTRIBUTING.md`). CI runs the full matrix after a pull request is opened.
