# Aruna

Aruna is a federated peer-to-peer data orchestration network. It connects sovereign data sources into a unified mesh where every participant keeps full ownership of their data while making it discoverable and accessible across organizational boundaries.

Each Aruna node is a self-contained data platform that speaks S3, serves a REST API, and automatically federates with other nodes over encrypted connections. Nodes discover each other through a Kademlia DHT, synchronize metadata via gossip and CRDTs, and replicate data with cryptographic integrity verification. The network has no central coordinator and scales horizontally as new nodes join.

## Features

### S3-compatible interface

Every Aruna node exposes a standard S3 API. Existing tools, libraries, and workflows work out of the box.

```bash
aws --endpoint-url http://localhost:1337 s3 cp dataset.parquet s3://my-bucket/
```

### Federated metadata with CRDTs

Metadata is stored as [Automerge](https://automerge.org/) documents following the [RO-Crate](https://www.researchobject.org/ro-crate/) JSON-LD schema. Changes propagate through gossip and merge automatically, even when nodes were offline or edits happened concurrently.

### Content-addressed storage

All data is hashed with BLAKE3 and deduplicated at the storage layer. Upload the same file through different paths or on different nodes, and it's stored once. Replication uses [Bao tree](https://github.com/oconnor663/bao) verified streaming, where integrity is checked incrementally as bytes arrive.

### Sovereign trust model

Aruna organizes the network into **realms**, each identified by an Ed25519 keypair. A realm represents a trust boundary - an organization, a department, a consortium. Every node belongs to exactly one realm. Realms can establish mutual trust relationships to enable cross-organization collaboration, but the decision is always unilateral: trusting someone else doesn't grant them access to your data.

Within a realm, **groups** define fine-grained access control with path-scoped roles and wildcard permissions.

### Pluggable backends

The blob layer supports filesystem, S3, PostgreSQL, and HTTP storage backends through [OpenDAL](https://opendal.apache.org/). Pick the right backend for your infrastructure.

### Single binary deployment

Aruna compiles to a single binary with an embedded LSM database ([fjall](https://github.com/fjall-rs/fjall)). Configuration is a repo-root `.env` file.

## Roadmap

- **Distributed full-text search** with per-node Tantivy indexes, fan-out queries, and authorization-filtered results via roaring bitmaps
- **Policy engine** using CEL expressions for constraint enforcement (upload size limits, file type restrictions, archival locks) that sync across nodes
- **Compute orchestration** through a GA4GH TES/DRS interface with data-locality-aware scheduling, automatic provenance tracking (RO-Crate Process Runs), and pluggable execution backends (Docker, Kubernetes, SLURM)
- **Event subscriptions** on path patterns with optional CEL filters and automated actions (webhooks, compute triggers, replication rules)
- **Transparent request forwarding** so any node can serve as entry point for any resource in the realm, with single-hop DHT-based discovery
- **Cross-realm data exchange** building on the already cross-realm-capable DHT layer

## Getting started

### Prerequisites

- Rust 1.93.0+ (edition 2024)
- A C compiler and OpenSSL dev headers

### Build and run

```bash
cargo build
cargo run -p aruna
```

### Configuration

Aruna reads configuration from environment variables. Copy `.env.example` to `.env` in the repository root and fill in the key material:

```bash
cp .env.example .env
```

Required variables from `aruna/src/config.rs`:

```bash
STORAGE_PATH=/var/lib/aruna/data
SOCKET_ADDRESS=0.0.0.0:8080
REALM_PUBLIC_KEY="-----BEGIN PUBLIC KEY----- ..."
REALM_PRIVATE_KEY="-----BEGIN PRIVATE KEY----- ..."
NODE_PUBLIC_KEY="-----BEGIN PUBLIC KEY----- ..."
NODE_PRIVATE_KEY="-----BEGIN PRIVATE KEY----- ..."
S3_PORT=1337
S3_HOST=localhost
S3_ADDRESS=0.0.0.0
```

Optional variables:

```bash
# Defaults to ${STORAGE_PATH}/blobstore when omitted
BLOB_ROOT=/var/lib/aruna/data/blobstore

# Optional bucket naming prefix
BLOB_BUCKET_PREFIX=aruna-

# Defaults to 100000
BLOB_MAX_BUCKET_SIZE=100000

# Defaults to SOCKET_ADDRESS when omitted
P2P_SOCKET_ADDRESS=0.0.0.0:4433

# Comma-separated iroh public keys
BOOTSTRAP_NODES=pubkey1,pubkey2

# Defaults to 3 and is clamped to at least 1
METADATA_REPLICATION_FACTOR=3
```

Generate Ed25519 keypairs with OpenSSL:

```bash
openssl genpkey -algorithm ed25519 -out key.pem
openssl pkey -in key.pem -pubout -out key.pub.pem
```

### Using S3

```bash
aws --endpoint-url http://localhost:1337 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:1337 s3 cp data.csv s3://my-bucket/
aws --endpoint-url http://localhost:1337 s3 ls s3://my-bucket
```

### Connecting nodes

Start a second instance with `BOOTSTRAP_NODES` pointing at the first node's P2P address. The nodes will discover each other through DHT and begin synchronizing.

## Development

```bash
cargo test --lib               # fast unit tests
cargo test -p aruna-net        # test a specific crate
cargo test test_name           # run one test
cargo +nightly fmt             # format
cargo +nightly clippy          # lint
cargo test --test integration  # full integration suite (needs multi-node setup)
```

### Crate layout

| Crate | Purpose |
|-------|---------|
| `aruna-core` | Foundation types, effect/event system, traits |
| `aruna-storage` | Local persistence (fjall LSM database) |
| `aruna-net` | P2P networking: Kademlia DHT, gossip, encrypted streams |
| `aruna-blob` | Content-addressed blobs, Bao streaming, storage backends |
| `aruna-tasks` | Periodic and scheduled task execution |
| `aruna-operations` | State machine implementations and driver loop |
| `aruna-search` | Distributed full-text search (in progress) |
| `aruna-api` | REST API (axum) and S3 server (s3s) |
| `aruna` | Main binary |

## Concepts

**Realm** - a trust boundary identified by an Ed25519 keypair. Represents an organization, a project, or any logical grouping. Nodes belong to exactly one realm. Realms can establish trust relationships for cross-boundary collaboration.

**Node** - a running Aruna instance that handles storage, networking, and API serving. Participates in DHT routing and gossip-based metadata replication.

**Meta Resources** - structured metadata documents stored as Automerge CRDTs following the RO-Crate JSON-LD schema. They sync automatically, support concurrent editing, and carry full version history.

**Data Resources** - immutable blobs identified by their BLAKE3 content hash. Updating a file creates a new blob; the old version is preserved.

**Groups** - permission boundaries within a realm. Each group has roles that map to path patterns with read/write/admin permissions and wildcard support.

## License

Dual-licensed under [MIT](LICENSE-MIT) and [Apache 2.0](LICENSE-APACHE).
