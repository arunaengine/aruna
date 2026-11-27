<!-- Plugin author guide: how to write, test, build, publish, and operate Aruna WASM plugins. -->
<!-- Copyright (c) 2026 The Aruna Contributors -->
<!-- SPDX-License-Identifier: MIT or Apache-2.0 -->

# Plugin guide

An Aruna plugin is a WebAssembly component (`aruna:plugin@0.3.0`) that reacts to events in a group, runs on a schedule, handles signed webhooks, or runs on demand from a form. A plugin manifest only declares what the plugin asks for; it grants nothing by itself. A realm admin publishes the built `.wasm` artifact, and a group admin previews and installs it into one group with explicit data grants, settings, and vault secret bindings.

If this guide disagrees with the code, the code wins; update the guide in the same change.

## Quick start

1. Create a project in Rust (default), Python, or JavaScript:

```sh
aruna-cli plugin new dataset-announcer --language rust
aruna-cli plugin new dataset-announcer --language python
aruna-cli plugin new dataset-announcer --language javascript
```

Toolchains required by `aruna-cli plugin build`:

| Language | Build tool |
| --- | --- |
| `rust` | `cargo` with target `wasm32-unknown-unknown` and the `aruna-plugin` crate |
| `python` | `uv` (runs `componentize-py`) |
| `javascript` | `npx` (runs `jco componentize`) |

2. Build and verify the component:

```sh
aruna-cli plugin build dataset-announcer
```

`plugin build` compiles the component, embeds the checked manifest, verifies that the code's registered handlers match `on` in `plugin.toml` one-to-one, rejects disallowed imports, and writes one `.wasm` artifact alongside its `sha256:` digest.

3. Publish the artifact to the realm (requires realm admin rights):

```sh
aruna-cli plugin publish dataset-announcer/dataset-announcer.wasm
aruna-cli plugin list --name dataset-announcer
```

4. Preview and install into a group (requires group admin rights):

```sh
# Preview grants, secret bindings, external services, container tasks, and warnings:
aruna-cli plugin install dataset-announcer \
  --group <group-id> \
  --setting endpoint=https://example.org \
  --grant 'read=data/*' \
  --secret api=group:my-api-token \
  --preview

# Activate using the install id and digest shown by the preview:
aruna-cli plugin install dataset-announcer \
  --group <group-id> \
  --id <install-id> \
  --version <sha256-digest> \
  --setting endpoint=https://example.org \
  --grant 'read=data/*' \
  --secret api=group:my-api-token
```

Every install is scoped to one group. Leaving out `--grant read=...` or `--grant write=...` grants access across the whole group (within the plugin's declared `access` level); passing `--grant write=` removes write access.

## Manifest (`plugin.toml`)

Authors write a compact `plugin.toml`; `plugin build` expands and validates it into the embedded manifest (max 64 KiB, max 32 entries per section).

```toml
name = "map-reads"
version = "0.1.0"
title = "Map FASTQ reads to a reference"
on = ["manual", "job_finished"]
access = "write"

[settings]
results = { title = "Output prefix", default = "results" }

[manual]
dataset = { title = "Dataset", type = "dataset" }
reads = "Prefix containing .fastq.gz files"
reference = "RefSeq accession (e.g. GCF_000005845.2)"

[tasks.map]
image = "ghcr.io/example/mapper@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
command = ["map", "--ref", "{reference}", "/in/reads", "/out/results"]
cpu = 2
memory = "4 GiB"
time = "2h"
network = "none"
report = "/out/report.json"

[tasks.map.parameters]
reference = "GCF_[0-9]{9}\\.[0-9]+"
```

### Top-level fields

| Field | Required | Notes |
| --- | --- | --- |
| `name` | yes | 1 to 64 chars of `a-z`, `0-9`, `-`; starts with a letter, cannot end with `-` |
| `version` | yes | Semantic version (`major.minor.patch`, optional `-`/`+` suffix) |
| `title` | no | Human-readable label (1 to 128 chars) |
| `on` | yes | Non-empty list of trigger names (no duplicates) |
| `access` | no | `"read"` (default) or `"write"` |

`access = "read"` requests `metadata.read`, `objects.read`, and `transfers.upload`. `access = "write"` adds `metadata.write`, `metadata.create`, `objects.copy`, and `transfers.download`. Declaring `[secrets]` adds `secrets.value`; declaring `[tasks.*]` requires `access = "write"` and adds `jobs.run`.

### Triggers (`on`)

| Trigger | Extra table | Required read permission |
| --- | --- | --- |
| `manual` | optional `[manual]` form fields | none |
| `timer` | required `[timer]` with `every = "<N><m\|h\|d>"` (`1m` to `366d`) | none |
| `webhook` | required `[webhook]` with `secret` and optional `delivery_id` | none |
| `dataset_created` | none | `metadata.read` |
| `dataset_updated` | none | `metadata.read` |
| `data_uploaded` | none | `objects.read` |
| `sync_completed` | none | `objects.read` |
| `sync_failed` | none | `objects.read` |
| `job_finished` | none | none |

### Settings (`[settings]`) and manual forms (`[manual]`)

Each entry is either a plain title string (creating a required `string` field) or a table `{ title, type, default, choices }`:

- Allowed `type` values in `[settings]`: `string` (default), `integer`, `boolean`, `url`, `choice` (requires `choices`).
- Extra `type` values allowed in `[manual]` forms: `dataset` and `object` (the portal renders a picker and the host resolves the selection before calling the handler).
- `url` settings must be `https://` origins without credentials, query strings, or fragments.

### External services (`[services.<name>]`)

Plugins cannot open raw sockets. All outbound HTTP traffic goes through named services:

```toml
[services.elab]
url = "{elab_url}/api/v2/experiments"
methods = ["GET", "POST", "PATCH"]
secret = { title = "eLabFTW API key", kind = "header" }
```

- `url`: fixed `https://host/prefix` or `{url_setting}/prefix` referencing a `url` setting.
- `methods`: optional subset of `GET`, `HEAD`, `POST`, `PUT`, `PATCH`, `DELETE` (defaults to all six).
- `secret`: optional vault slot bound at install time (`"Title"` defaults to `kind = "bearer"`, or `{ title, kind = "bearer" | "basic" | "header" }`). The host injects the credential on every call; plugin code never sees the secret bytes.

### Incoming webhooks (`[webhook]`)

```toml
[webhook]
secret = "Webhook signing key"
delivery_id = "x-delivery-id"
```

- `secret` defaults to `kind = "hmac"` (also accepts `bearer` or `header`; `basic` is not valid for verification). The host verifies every request before invoking the plugin; plugin code never reads the verifying secret.
- `delivery_id` is an optional lowercase header name used to deduplicate retried webhook deliveries.

### Readable secrets (`[secrets]`)

Use `[services.<name>].secret` or `[webhook].secret` whenever possible. Declare `[secrets.<name>]` only when the plugin code itself must read secret fields via `aruna::secret("name")` / `aruna.secret("name")`:

- Secret slots under `[secrets]` are marked readable and raise an explicit warning during install preview.
- Secrets are bound at install time from the vault as `group:<name>`, `realm:<name>`, or `node:<name>`.

### Container tasks (`[tasks.<name>]`)

Plugins can launch isolated compute jobs from fixed, reviewable recipes (requires `access = "write"`):

| Field | Default | Rules |
| --- | --- | --- |
| `image` | required | Must be pinned by `@sha256:<64 lowercase hex>` digest |
| `command` | image entrypoint | Up to 64 arguments (max 4,096 bytes each); `{param}` placeholders are substituted once |
| `cpu` | `1` | Positive integer |
| `memory` | `"2 GiB"` | Positive size in `MiB` or `GiB` |
| `time` | `"1h"` | Positive duration in `m`, `h`, or `d` |
| `network` | `"none"` | `"none"` or `"open"` (`"open"` raises an install preview warning) |
| `parameters` | `{}` | Regex per `{param}` in `command`; defaults to `[A-Za-z0-9][A-Za-z0-9._:-]{0,127}` (blocks leading `-`) |
| `report` | none | Optional absolute container file path read back into `job.report_json()`, never exported |

Container secret injection is not supported yet; declaring `secrets` inside a `[tasks.<name>]` block is rejected at build time.

## Writing handlers

Register one handler function per trigger listed in `on`. Return a JSON-serializable value (or nothing) on success.

### Rust

```rust
use aruna::Result;

fn hello() -> Result<String> {
    Ok(format!("hello from {}", aruna::run().install))
}

aruna::plugin! { manual => hello }
```

Core modules and functions in the `aruna` crate (`aruna-plugin`):

- `aruna::run()`, `aruna::settings()`, `aruna::secret(name)`, `aruna::log!(...)`
- `aruna::datasets`, `aruna::objects`, `aruna::jobs`, `aruna::state`
- `aruna::service(name)`, `aruna::task(name)`
- `aruna::fail(...)` (permanent failure) and `aruna::retry(...)` (transient retryable failure)

### Python

```python
import aruna


@aruna.on("manual")
def hello():
    return f"hello from {aruna.run.install}"
```

Core SDK surface (`import aruna`):

- `aruna.run`, `aruna.settings`, `aruna.secret(name)`, `aruna.log`
- `aruna.datasets`, `aruna.objects`, `aruna.jobs`, `aruna.state`
- `aruna.service(name)`, `aruna.task(name)`
- Exceptions: `InvalidInput`, `Fail`, `Retry`, `ServiceError`, `NotAllowed`, `NotFound`, `Conflict`, `Unavailable`, `LimitReached`, `UnknownOutcome`

### JavaScript

```js
import aruna from "aruna";

aruna.on("manual", () => `hello from ${aruna.run.install}`);
```

See `plugins/sdk/javascript/aruna.d.ts` for the full TypeScript declaration (`service`, `datasets`, `objects`, `jobs`, `state`, `settings`, `run`, `Fail`, `Retry`, `InvalidInput`).

## Bulk data, container tasks, and durable state

Never stream large objects through WebAssembly memory. Instead, ask the host to move bytes directly or process them in a container task:

- **External transfers**: use `service.download(..., to=...)` and `service.upload(source, path=..., version=...)` so the node transfers bytes directly between Aruna storage and the declared service.
- **Object copies**: copy objects via `aruna.objects` without downloading bytes into the guest.
- **Compute jobs**: start a declared task via `aruna.task("name").start(parameters=..., inputs=..., files=..., outputs=...)`, store correlation state in `aruna.state[f"job/{job.id}"]`, and handle completion in a `job_finished` handler.
- **Crash-safe state cleanup**: persist external updates and dataset changes first, then delete the `aruna.state` entry last. If a run crashes or is retried, it can still recover its correlation record.

## Sandbox and runtime limits

Plugins execute inside a closed WASI 0.2 sandbox with no filesystem, environment variables, CLI arguments, or direct network sockets.

Default per-run ceilings (node configuration may lower them further; a plugin cannot raise them):

| Limit | Default ceiling |
| --- | --- |
| Guest linear memory | 256 MiB |
| Compute fuel | 2,000,000,000 units |
| Wall time (including host calls) | 60 s |
| Started-write grace window | 30 s |
| Counted host calls | 1,000 calls |
| Total host-call bytes (in + out) | 16 MiB |
| Single host-call payload | 1 MiB |
| Concurrent in-flight host calls | 8 |
| Run input (settings, form, webhook) | 1 MiB |
| Handler output | 64 KiB |
| Log line length | 1,024 bytes |
| Artifact / manifest size | 64 MiB / 64 KiB |

If a run times out or fails while a write was in flight and its outcome cannot be determined automatically, the run enters a needs-attention state. An operator inspects external state and resolves it explicitly with `aruna-cli plugin resolve`.

## Operating installs

```sh
# Inspect installs and update grants or version
aruna-cli plugin installs --group <group-id>
aruna-cli plugin grants <install-id> --grant 'read=data/*' --grant 'write=results/*'
aruna-cli plugin update <install-id> <sha256-digest>

# Pause, resume, or remove (deactivates the install's service account)
aruna-cli plugin pause <install-id>
aruna-cli plugin resume <install-id>
aruna-cli plugin remove <install-id>

# Trigger manually and inspect runs, logs, and event delivery
aruna-cli plugin run <install-id> --input '{"reference":"GCF_000005845.2"}'
aruna-cli plugin runs <install-id>
aruna-cli plugin runs <install-id> <run-id>
aruna-cli plugin delivery <install-id>

# Recover failed runs or drop held job completions
aruna-cli plugin resolve <install-id> <run-id> --done true
aruna-cli plugin retry <install-id> <run-id>
aruna-cli plugin drop <install-id> <job-id>
```

## Testing and examples

- **Python unit tests**: use `aruna.testing.Fake(settings=..., manifest="plugin.toml")` (`plugins/sdk/python/aruna/testing.py`) to run handlers directly in plain Python against in-memory datasets, objects, service replies, secrets, jobs, and state.
- **Rust and integration tests**: see `runtime/wasm-host/tests/` for end-to-end host execution tests against compiled examples.
- **Reference examples** in `plugins/examples/`:
  - `announcer`: Rust and JavaScript webhook/event announcements.
  - `elabftw` and `elabftw-python`: two-way sync with an external service using timers, manual conflict resolution, and header-injected API keys.
  - `map-reads`: manual form with dataset picker, pinned container task, `job_finished` handler, report parsing, and durable `aruna.state` cleanup.
  - `zenodo-deposit`: bulk upload to an external repository with `service.upload`.
  - `archive-uploads`, `fetch-reads`, `fill-defaults`, `fill-license`, `import-record`, `link-objects`, `link-uploads`, `nightly-report`, `object-report`, `orcid-names`, `release-datasets`, `set-keywords`, `sync-alerts`, `tag-updates`, `upload-counter`: focused single-purpose patterns.
