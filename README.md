# Aruna

> [!WARNING]
> Work in progress. Interfaces, behavior, and deployment details may still change.

Aruna is a federated peer-to-peer data orchestration network for organizations that need to share data without handing control to a central platform. It connects sovereign data sources into a unified mesh where each participant keeps ownership of its data while making selected data discoverable and accessible across organizational boundaries.

Each Aruna node exposes an S3 endpoint and REST API, federates with peer nodes over encrypted connections, and keeps metadata synchronized with gossip and CRDTs. The network has no central coordinator and scales horizontally as new nodes join.

## Features

### S3-compatible interface

Every Aruna node exposes a standard S3 API. Existing tools, libraries, and automation can be pointed at the node endpoint after S3 credentials have been created.

### Federated resources with CRDTs

Management resources such as groups, and users are stored as [Automerge](https://automerge.org/) documents. Metadata itself is stored in a CRDT based triple-store, following the [RO-Crate](https://www.researchobject.org/ro-crate/) JSON-LD schema. Changes propagate through gossip and merge automatically, even when nodes were offline or edits happened concurrently.

### Content-addressed storage

All data is hashed with BLAKE3 and deduplicated at the storage layer. Upload the same file through different paths or on different nodes, and it's stored once. Replication uses [Bao tree](https://github.com/oconnor663/bao) verified streaming, where integrity is checked incrementally as bytes arrive.

### Sovereign trust model

Aruna organizes the network into **realms**, each identified by an Ed25519 keypair. A realm represents a trust boundary - an organization, a department, a consortium. Every node belongs to exactly one realm. Realms can establish mutual trust relationships to enable cross-organization collaboration, but the decision is always unilateral: trusting someone else doesn't grant them access to your data.

Within a realm, **groups** define fine-grained access control with path-scoped roles and wildcard permissions.

### Storage layer

Aruna's blob layer builds on [OpenDAL](https://opendal.apache.org/). The default runtime configuration in this repository uses filesystem-backed blob storage, while the underlying storage layer also supports additional backend integrations.

### Single binary deployment

Aruna runs as a single binary and stores local state in an embedded LSM database ([fjall](https://github.com/fjall-rs/fjall)). When launched from this repository, configuration is provided through a repo-root `.env` file.

### Distributed full-text search

Aruna is expanding toward distributed full-text search with per-node Tantivy indexes, fan-out queries, and authorization-filtered results.

### Policy engine

Aruna is adding a CEL-based policy engine for constraint enforcement such as upload size limits, file type restrictions, and archival locks that synchronize across nodes.

### Compute orchestration

Aruna is growing toward GA4GH TES/DRS-based compute orchestration with data-locality-aware scheduling, automatic provenance tracking through RO-Crate Process Runs, and pluggable execution backends such as Docker, Kubernetes, and SLURM.

### Event subscriptions

Aruna is adding path-based event subscriptions with optional CEL filters and automated actions such as webhooks, compute triggers, and replication rules.

### Transparent request forwarding

Aruna is working toward transparent request forwarding so any node can serve as an entry point for any resource in the realm, backed by single-hop DHT-based discovery.

### Cross-realm data exchange

Aruna is extending its cross-realm-capable DHT foundations toward direct cross-realm data exchange.

## Getting Started

For most users, the fastest way to evaluate Aruna is the local 3-node smoke test.

### Prerequisites

- Rust `1.93.0+` for source builds
- OpenSSL development headers
- On `x86_64-unknown-linux-gnu`, `clang` and `mold` are required because the repository linker configuration uses both
- `just`, `curl`, and `ss` for the local smoke tests
- `docker` for `just test-deploy-oidc`
- `docker-compose` for `just compose`

### Run a single node with an external identity provider

To start one node, run:

```bash
just compose
```

With the default example configuration:

- the REST API and Swagger UI are served on `http://127.0.0.1:3000/swagger-ui`
- the S3 endpoint listens on `http://127.0.0.1:1337`

### Evaluate a local cluster

For a quick end-to-end evaluation, run:

```bash
just test-deploy
# or
just test-deploy-oidc
```

This smoke test:

- builds the workspace in release mode
- boots 3 local Aruna nodes
- waits for readiness at `http://127.0.0.1:<port>/swagger-ui`
- writes per-node logs and `summary.txt` under `target/test-deploy/`
- prints an `ADMIN_TOKEN=...` line that you can use for authenticated API calls during the session

Common overrides:

- `ARUNA_TEST_DEPLOY_BASE_PORT` to move the whole local port range
- `ARUNA_TEST_DEPLOY_EXIT_AFTER_READY=1` to stop after readiness instead of keeping the cluster running

`just test-deploy-oidc` adds a local Keycloak instance on top of the same 3-node smoke test.

### Run a single node from source

To start one node directly from source, copy the example environment file and run the main binary from the workspace root:

```bash
cp .env.example .env
cargo run -p aruna
```

With the default example configuration:

- the REST API and Swagger UI are served on `http://127.0.0.1:3000/swagger-ui`
- the S3 endpoint listens on `http://127.0.0.1:1337`

## Configuration

Aruna reads configuration from environment variables. For most source deployments, start from `.env.example` and adjust the values below.

Required variables:

| Variable | Example | Purpose |
|----------|---------|---------|
| `STORAGE_PATH` | `/var/lib/aruna/data` | Root path for persisted node state and default local storage locations |
| `SOCKET_ADDRESS` | `0.0.0.0:3000` | REST API and Swagger UI listen address |
| `S3_PORT` | `1337` | S3 server port |
| `S3_HOST` | `localhost` | Hostname presented to S3 clients |
| `S3_ADDRESS` | `0.0.0.0` | S3 server listen address |

Common optional variables:

| Variable | Default | Purpose |
|----------|---------|---------|
| `P2P_SOCKET_ADDRESS` | `SOCKET_ADDRESS` | P2P bind address |
| `REALM_DESCRIPTION` | `Aruna Realm` | Display name for a newly initialized realm |
| `ONBOARDING_SECRET` | unset | Used only when a new node joins an existing realm on first boot |
| `BOOTSTRAP_NODES` | unset | Comma-separated bootstrap node public keys |
| `METADATA_REPLICATION_FACTOR` | `3` | Default metadata replication factor, clamped to at least `1` |
| `CRAQLE_STORAGE_PATH` | `${STORAGE_PATH}/craqle` | Metadata storage path |
| `BLOB_ROOT` | `${STORAGE_PATH}/blobstore` | Blob storage root |
| `BLOB_MULTIPART_BUCKET` | `uploaded-parts` | Bucket used for multipart uploads |
| `BLOB_BUCKET_PREFIX` | unset | Optional bucket naming prefix |
| `BLOB_MAX_BUCKET_SIZE` | `100000` | Maximum bucket size setting |
| `BLOB_CONTROL_PLANE_CONNECT_TIMEOUT_SECS` | `30` | Blob control-plane connect timeout |
| `BLOB_CONTROL_PLANE_IO_TIMEOUT_SECS` | `30` | Blob control-plane I/O timeout |
| `BLOB_TRANSFER_IDLE_TIMEOUT_SECS` | `1800` | Blob transfer idle timeout |

Configure OIDC providers by listing provider IDs and defining one environment block per ID:

```bash
OIDC_PROVIDER_IDS=main
OIDC_MAIN_ISSUER=https://issuer.example/realms/aruna
OIDC_MAIN_AUDIENCE=aruna-api
OIDC_MAIN_DISCOVERY_URL=https://issuer.example/realms/aruna/.well-known/openid-configuration
```

Logging and tracing:

- `RUST_LOG` controls log verbosity
- `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` enables OTLP trace export

## State And Onboarding

A node started without an `ONBOARDING_SECRET` initializes a new realm on first boot and persists its identity under `STORAGE_PATH`.

Additional nodes join an existing realm by starting with an `ONBOARDING_SECRET` on their first boot. The first management node also logs an initial local onboarding secret when it creates a brand-new realm.

Once a node has persisted state, later `.env` changes do not re-bootstrap or re-onboard it. If a data directory already contains node state, changing `ONBOARDING_SECRET` does not re-onboard that node. Use a fresh `STORAGE_PATH` when repeating onboarding tests or bootstrap flows.

For a ready-made local multi-node onboarding flow, use `just test-deploy` instead of stepping through the onboarding APIs by hand.

## S3 Access

Aruna's S3 interface is authenticated. Create S3 credentials via [the API](http://127.0.0.1:3000/swagger-ui/#/credentials) before using the endpoint, then point your AWS CLI or S3-compatible client at the node.

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

aws --endpoint-url http://127.0.0.1:1337 s3 mb s3://my-bucket
aws --endpoint-url http://127.0.0.1:1337 s3 cp data.csv s3://my-bucket/
aws --endpoint-url http://127.0.0.1:1337 s3 ls s3://my-bucket
```

## Concepts

**Realm** - a trust boundary identified by an Ed25519 keypair. Represents an organization, a project, or any logical grouping. Nodes belong to exactly one realm. Realms can establish trust relationships for cross-boundary collaboration.

**Node** - a running Aruna instance that handles storage, networking, and API serving. Participates in DHT routing and gossip-based metadata replication.

**Meta Resources** - structured metadata documents stored as Automerge CRDTs following the RO-Crate JSON-LD schema. They sync automatically, support concurrent editing, and carry full version history.

**Data Resources** - immutable blobs identified by their BLAKE3 content hash. Updating a file creates a new blob; the old version is preserved.

**Groups** - permission boundaries within a realm. Each group has roles that map to path patterns with read/write/admin permissions and wildcard support.

## License

Dual-licensed under [MIT](LICENSE-MIT) and [Apache 2.0](LICENSE-APACHE).
