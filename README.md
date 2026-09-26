<!-- Project readme: what Aruna is, how to run a node, and where the guides are. -->
<!-- Copyright (c) 2026 The Aruna Contributors -->
<!-- SPDX-License-Identifier: MIT or Apache-2.0 -->

<p align="center">
  <img alt="" src="./img/icon-mark.png" width="240">
  <br>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./img/wordmark-white.png">
    <img alt="Aruna" src="./img/wordmark.png" width="320">
  </picture>
</p>

<h1 align="center">A FAIR, federated data orchestration engine</h1>

<p align="center">
  <a href="https://www.rust-lang.org/"><img alt="Built with Rust" src="https://img.shields.io/badge/built_with-Rust-08216C.svg"></a>
  <a href="https://github.com/arunaengine/aruna/blob/main/LICENSE-APACHE"><img alt="Apache 2.0 license" src="https://img.shields.io/badge/License-Apache_2.0-335DC6.svg"></a>
  <a href="https://github.com/arunaengine/aruna/blob/main/LICENSE-MIT"><img alt="MIT license" src="https://img.shields.io/badge/License-MIT-335DC6.svg"></a>
  <a href="https://github.com/arunaengine/aruna/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/arunaengine/aruna/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://codecov.io/gh/arunaengine/aruna"><img alt="Coverage" src="https://codecov.io/github/arunaengine/aruna/coverage.svg?branch=main"></a>
</p>

<p align="center">
  <a href="https://v3.aruna-engine.org/app/docs/v1">Portal documentation</a> ·
  <a href="https://api.node-1.v3.aruna-engine.org/swagger-ui/">Swagger UI</a> ·
  <a href="#features">Features</a> ·
  <a href="#architecture-and-goals">Architecture</a> ·
  <a href="#getting-started">Getting started</a> ·
  <a href="#feedback--contributions">Contributing</a>
</p>

> [!NOTE]
> **Aruna v3 is now in public testing.** You can [try it out](https://v3.aruna-engine.org) and share feedback through [GitHub issues](https://github.com/arunaengine/aruna/issues). Aruna v2 remains available on the [v2 branch](https://github.com/arunaengine/aruna/tree/v2).

Aruna helps organizations share and organize research data and metadata while keeping control of their own infrastructure. Each organization runs its own node and connects with others through a peer-to-peer network.

## Features

- **Web portal**: Browse files, edit datasets, manage access, and follow compute runs in the browser.
- **Sovereign trust model**: Each node belongs to one organization. Realms define shared trust between them.
- **Fine-grained access control**: Path-based permissions with wildcard support and group-based roles.
- **S3-compatible API**: Every node exposes an [S3 API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html) for data access.
- **Virtual buckets**: Buckets are virtual collections of local and remote data resources, with configurable materialization behavior.
- **Extensible storage backends**: Support for a variety of storage backends through [OpenDAL](https://opendal.apache.org/).
- **Standardized metadata**: Metadata is stored as [RO-Crate](https://www.researchobject.org/ro-crate/) JSON-LD enabling rich, interoperable descriptions of datasets, files, and processes.
- **Metadata profiles**: Define dataset requirements with SHACL profiles and check descriptions against them.
- **Metadata queries and editing**: Query and update RO-Crate metadata with SPARQL.
- **Distributed full-text search**: Per-node [Tantivy](https://github.com/quickwit-oss/tantivy) indexes with fan-out queries and authorization filtering.
- **Built-in replication and synchronization**: Metadata edits converge across holders. Blob copies move through explicit copy or replication requests; each node owns its S3 keys, versions, and current heads.
- **Interoperable using open standards**: [OIDC](https://openid.net/connect/) for authentication, [GA4GH DRS](https://www.ga4gh.org/product/data-repository-service-drs/) for data referencing, [OAI-PMH](https://www.openarchives.org/pmh/) for metadata harvesting.
- **Repository publishing**: Publish datasets with a DOI to [Zenodo](https://zenodo.org/) or other [InvenioRDM](https://inveniordm.docs.cern.ch/) repositories, import records, and keep both in sync.
- **Compute jobs**: Run container workloads with Docker, Apptainer, or Kubernetes through the portal or GA4GH TES API.
- **Interactive notebooks**: Work with `.ipynb` notebooks in the portal, run cells in live sessions, and access files in S3 buckets.
- **AI assistant tools**: Authenticated [MCP](https://modelcontextprotocol.io/) access to Aruna context, data, metadata, and compute operations.
- **Deployment**: Run a node as a single binary or deploy a multi-node cluster.

## Architecture and Goals

Research data is spread across universities, labs and archives. Each of them has its own storage
and its own rules. Aruna connects these places without taking control away from them.

- **Nodes**: each organization runs its own node. It decides where its data lives and who may
  access it.
- **Realms**: nodes that trust each other form a realm, for example an institute or a consortium.
  Joining a realm does not grant access to data. Access comes only from groups, roles and paths.
- **Peer-to-peer network**: nodes find and reach each other with
  [iroh](https://www.iroh.computer/), also behind NATs and firewalls. No central server is needed.

The goal is FAIR research data: findable, accessible, interoperable and reusable, while each
institution stays responsible for its own data.

### Data

- Every node offers an S3 API, so existing tools and scripts work without changes.
- A virtual bucket can hold local files, copies and references to files on other nodes. Aruna
  decides whether to keep a local copy or fetch a file when it is needed.
- Files are hashed with BLAKE3. This detects damaged data and stores identical files only once.
- Copies between nodes are checked while they stream in, so a broken transfer is caught early.

### Metadata

- Metadata is stored as RO-Crate JSON-LD. This common format describes a dataset together with
  its files, people, instruments, software and workflows.
- Edits made on different nodes merge automatically. Nodes catch up after a network outage.
- Users and groups are shared between the nodes of a realm in the same way.

## Getting Started

Pick the way that fits you, fastest first:

1. **Try it online**: use the [public v3 portal](https://v3.aruna-engine.org), read the
   [portal documentation](https://v3.aruna-engine.org/app/docs/v1) or explore the
   [Swagger UI](https://api.node-1.v3.aruna-engine.org/swagger-ui/).
2. **Run a local cluster**: start three nodes on your machine with one command.
3. **Run a single node**: start one node from the scripts or from source.

### Prerequisites

- **Local cluster**: `docker` with Compose v2, `curl`, `ss` and optionally `just`.
- **Building from source**: Rust `1.97.1` (see [rust-toolchain.toml](rust-toolchain.toml)),
  OpenSSL development headers and the `mold` linker.

### Run a local cluster

```bash
just local-cluster        # three nodes
just local-cluster-oidc   # three nodes plus a local Keycloak login
just preview              # three nodes, Keycloak and a web portal per node
```

Each command:

- builds Aruna and starts three nodes,
- waits until every node is ready,
- prints the URLs of each node, the test logins and an `ADMIN_TOKEN` for API calls.

Logs and credentials are written to `target/test-deploy/`. Press Ctrl-C to stop the cluster. If
the terminal is already closed, run `just stop`.

Two settings help when the defaults do not fit:

- `ARUNA_TEST_DEPLOY_BASE_PORT` moves all ports, for example when a port is already taken.
- `ARUNA_TEST_DEPLOY_EXIT_AFTER_READY=1` exits once the cluster is ready, for use in scripts.

### Run a single node

`just local` starts one node that uses an external identity provider. To start a node from
source instead:

```bash
cp .env.example .env
cargo run -p aruna
```

The node then serves:

- the REST API and Swagger UI on `http://127.0.0.1:3000/swagger-ui`
- the S3 API on `http://127.0.0.1:1337`

> [!WARNING]
> The tracked `.env` contains demo keys that are public. A node refuses to start with them, so
> that no real node runs on known keys. Set your own `REALM_*_KEY` and `NODE_*_KEY` values. For
> a quick local test only, `--dangerously-use-default-env` starts the node anyway.

## Running a Node

### First start

- The first node creates a new realm when it starts for the first time. Its identity is saved
  under `STORAGE_PATH`.
- Further nodes join that realm by setting `ONBOARDING_SECRET` before their first start.
- This happens only once per data directory. Later `.env` changes do not onboard a node again.
  To start over, use an empty `STORAGE_PATH`.
- The admin secret is never logged, so it cannot leak through log files. Create one with
  `aruna-doctor recover-admin`. The local scripts do this for you.

### Backups

Aruna encrypts stored secrets, such as connector tokens and S3 secret keys. The key comes from the
node's identity in `STORAGE_PATH`. Always back up the whole `STORAGE_PATH`: a node restored
without it cannot read its secrets anymore.

### Upgrading

1. Stop the node.
2. Run `aruna-doctor migrate <STORAGE_PATH>`.
3. Start the new version.

The new version cannot read data stored in an older layout without this step. Running it twice
is safe.

### Durability

`ARUNA_FJALL_PERSIST_MODE` chooses between speed and safety:

- `buffer` (default): faster. Safe when Aruna crashes, but the latest writes can be lost on power
  loss or an operating system crash.
- `sync_all`: slower. Every write is on disk before Aruna confirms it.

Keep free disk space for imports: an RO-Crate import briefly needs about twice the archive's size.

### Notebook sessions

Notebook sessions run user code. To limit what that code can reach, a session can only talk to
its node's S3 API.

- **Docker**: sessions use the internal network `aruna-sessions`. Its subnet is
  `172.30.255.0/24` unless `ARUNA_COMPUTE_DOCKER_SESSION_SUBNET` sets another one. Choose a subnet
  that no other host network uses.
- **Kubernetes**: set an S3 URL that pods can reach in `ARUNA_COMPUTE_S3_URL` or `S3_PUBLIC_URL`.
  The node's service account needs `create` on `pods/exec` to talk to the notebook kernel.
  Clusters with special network setups, such as Cilium, can add policies with
  `ARUNA_COMPUTE_K8S_POLICY_MANIFESTS`.
- **Apptainer**: sessions are not limited and use the host network.

## Publishing to Invenio and Zenodo

Researchers can publish datasets to Zenodo or another InvenioRDM repository. This gives a dataset
a DOI and a public record without copying files by hand. Records can also be imported.

- **Check**: see what a dataset still lacks before anything is sent.
- **Export**: create a record with a reserved DOI, as a draft or published. It uses the
  researcher's own token, so the record belongs to them.
- **Import**: bring in a record by ID, DOI or URL. Copy its files, reference them, or take only the
  metadata.
- **Links**: keep a dataset and a record in sync. Changes go to a draft, and the researcher decides
  when to publish. Pull links fetch new versions from the repository.

## License

Aruna is licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Aruna by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.


## Feedback & Contributions

Found a bug or have an idea? Open an [issue](https://github.com/arunaengine/aruna/issues) or send a pull request. Reports from the v3 public test phase help us understand what works and what needs attention. See the [Contributor Guidelines](./CONTRIBUTING.md) and [Code of Conduct](./CODE_OF_CONDUCT.md) before contributing.
