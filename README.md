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
- **Compute jobs**: Run container workloads with Docker, Apptainer, or Kubernetes through the portal or GA4GH TES API.
- **Interactive notebooks**: Work with `.ipynb` notebooks in the portal, run cells in live sessions, and access files in S3 buckets.
- **AI assistant tools**: Authenticated [MCP](https://modelcontextprotocol.io/) access to Aruna context, data, metadata, and compute operations.
- **Deployment**: Run a node as a single binary or deploy a multi-node cluster.

## Architecture and Goals

Research data rarely lives in one place. Universities, labs, archives, and infrastructure providers have their own storage systems, policies, and responsibilities. Aruna connects these systems so researchers can find and work with data across participating nodes, while each organization decides how its data is stored and who can access it.

Nodes are organized into **realms**, which define a shared trust boundary for an institute, consortium, or project network. Each node belongs to one realm. Membership alone does not grant access to data: permissions are assigned explicitly through groups, roles, and paths.

### Data, metadata, and access

Each Aruna node exposes an **S3-compatible API** for the tools, scripts, and workflow systems researchers already use. Virtual buckets bring together local data, replicated copies, and references to remote resources. Aruna tracks where each object lives and whether to keep a local copy or fetch it when needed.

> [!NOTE]
> Object keys for `PutObject`, `CreateMultipartUpload`, `UploadPart`, and `CompleteMultipartUpload` must be non-empty relative paths; they are rejected if they begin with `/`, contain an exact `..` path segment, or contain control characters.
 
Metadata is stored as **RO-Crate JSON-LD**, describing datasets alongside their files, people, instruments, software, and workflows. A CRDT-based triple store merges concurrent metadata edits across nodes. Users, groups, and other management resources also synchronize between nodes, allowing them to reconcile changes after a network outage.

File contents are hashed with **BLAKE3** for integrity checks and deduplication within each storage backend. Replication uses Bao-tree verified streaming to check data as it arrives. Each node owns its local object keys and versions; copying a file to another node creates a copy managed by that node.

### Network and research workflows

The network layer uses **iroh** for peer discovery, authenticated connections, and data exchange, including connections across NATs and firewalls.

Researchers can search across nodes, describe datasets, share files, and run compute jobs through the same system. Aruna supports GA4GH DRS for data references, OAI-PMH for metadata harvesting, and GA4GH TES for compute execution. Policies and permissions govern access throughout these workflows.

The goal is practical support for FAIR research data: making it findable, accessible, interoperable, and reusable across the institutions responsible for it.

## Getting Started

Try the [public v3 portal](https://v3.aruna-engine.org) and follow the [portal documentation](https://v3.aruna-engine.org/app/docs/v1), or explore the REST API in [Swagger UI](https://api.node-1.v3.aruna-engine.org/swagger-ui/). To run Aruna locally, start with the 3-node demo deployment below.

### Prerequisites

#### For local builds

- Rust `1.97.1` (see [rust-toolchain.toml](rust-toolchain.toml), for source builds)
- OpenSSL development headers
- `mold` linker

#### For local test deployments

- `curl` (`ss` for cluster setup)
- `docker`
- Docker Compose v2 (`docker compose`)
- `just` (optional, for convenience)

### Run a single node with an external identity provider

Start one node with:

```bash
just local
```

or invoke [scripts/local_deploy.sh](scripts/local_deploy.sh) directly.

The default example configuration exposes:

- the REST API and Swagger UI on `http://127.0.0.1:3000/swagger-ui`
- the S3 endpoint on `http://127.0.0.1:1337`

The repository also tracks a `.env` holding a demonstration profile whose keys are
published. A node refuses to start while `REALM_PUBLIC_KEY`, `NODE_PUBLIC_KEY`,
`REALM_PRIVATE_KEY` or `NODE_PRIVATE_KEY` still holds one of those published keys, and
names it. Replace them with your own, or pass `--dangerously-use-default-env` (or set
`ARUNA_DANGEROUSLY_USE_DEFAULT_ENV=1`) to start anyway, which logs a warning per key.

### Evaluate a local cluster

For a quick end-to-end evaluation, run:

```bash
just local-cluster
# or
just local-cluster-oidc
```

This demo deployment:

- builds the workspace in release mode
- launches 3 local Aruna nodes
- waits for `/readyz` on each node's ops port
- writes per-node logs, `summary.txt` and a private `credentials.txt` to `target/test-deploy/`
- prints an `ADMIN_TOKEN=...` line for use in authenticated API calls during the session
- prints a summary listing every node's API, portal, S3 and ops URLs next to the test logins

`just preview` additionally serves the portal. The portal has its own listener,
so each node exposes the SPA on a separate port from the REST API; the REST port
redirects `/` to the Swagger UI.

Docker images use the same website build: stage its `dist/` contents in the
ignored `.portal-embed/` directory before building, or set the
`PORTAL_EMBED_DIR` build argument to another staged directory in the build context.
The Dockerfile copies those assets to `/run/portal`; without staged assets the
image is headless. Portal source belongs in the separate website repository,
not a second maintained bundle under `docker/`.

Useful overrides:

- `ARUNA_TEST_DEPLOY_BASE_PORT` shifts the entire local port range
- `ARUNA_TEST_DEPLOY_EXIT_AFTER_READY=1` exits once the cluster is ready instead of keeping it running

Ctrl-C stops the cluster again. A deployment that outlived its terminal is
stopped with:

```bash
just stop
```

It interrupts a deploy script that still monitors the nodes, stops every node
named by a pid file under `target/test-deploy/`, and removes the Keycloak
compose project. Logs, `summary.txt` and `credentials.txt` stay in place.

`just local-cluster-oidc` extends the same 3-node startup check with a local Keycloak instance.

### Run a single node from source

To run a node directly from source, copy the example environment file and start the main binary from the workspace root:

```bash
cp .env.example .env
cargo run -p aruna
```

The default example configuration exposes:

- the REST API and Swagger UI on `http://127.0.0.1:3000/swagger-ui`
- the S3 endpoint on `http://127.0.0.1:1337`

## State And Onboarding

A node started without an `ONBOARDING_SECRET` initializes a new realm on first boot and persists its identity under `STORAGE_PATH`. It does not log the initial administrator secret. The local deployment scripts stop the node and use `aruna-doctor recover-admin` against its database to mint a secret for the initial administrator claim.

Additional nodes join an existing realm by setting `ONBOARDING_SECRET` on their first boot.

Onboarding only takes effect on a fresh data directory. Once a node has persisted state, later `.env` changes, including a new `ONBOARDING_SECRET`, do not re-bootstrap or re-onboard it. To repeat an onboarding or bootstrap flow, point the node at a fresh `STORAGE_PATH`.

For a ready-made multi-node onboarding flow, use `just local-cluster` instead of walking through the onboarding APIs manually.

## Interactive Session Networking

Interactive notebook sessions run in a container that must reach this node's S3 plane and nothing
else. With the Docker executor the node creates an internal bridge network named `aruna-sessions`
from `ARUNA_COMPUTE_DOCKER_SESSION_SUBNET` (default `172.30.255.0/24`). The network has no external
route, and the node serves S3 on the bridge's gateway address, the first host address of that
subnet, on the port from `S3_ADDRESS`.

The bridge gives session containers a host-side S3 endpoint. It does not isolate other host services
that bind that address or all interfaces. Restrict those listeners appropriately, and keep the S3
port free on the bridge gateway. Pick a subnet that does not overlap an existing host network.

Kubernetes keeps its existing S3-only network policy, and Apptainer keeps the host network. On
Kubernetes the Aruna node's controller service account needs the `create` verb on `pods/exec`:
the node talks to a session's kernel through an exec into the running pod. The task workload
service account stays unprivileged, with its token unmounted.

Some clusters need policies the standard Kubernetes ones cannot express. List those manifests in
`ARUNA_COMPUTE_K8S_POLICY_MANIFESTS`, a comma-separated list of YAML files and directories; a
directory contributes its `*.yaml` and `*.yml` files in name order, and a file may hold several
documents. Every document needs an `apiVersion`, a `kind` and a `metadata.name`, and its namespace
must be absent or the compute namespace. The node applies them next to its own network policies at
startup and before each job, so its service account needs `create`, `get` and `patch` on those
kinds in that namespace. An unreadable file, an invalid document or a kind the cluster does not
serve stops the node.

A Cilium cluster is the common case: the S3 endpoint often resolves to the ingress load balancer,
and Cilium treats that traffic as its reserved `ingress` entity, which no `ipBlock` rule matches. A
CiliumNetworkPolicy with `toEntities: [ingress]` on the S3 port, selecting pods labelled
`aruna-engine.org/network: s3`, opens it. DNS egress is allowed by port with no peer, because a
node-local resolver runs on a host address that is neither a pod nor a CIDR peer.

Kubernetes workspaces and sessions need a pod-reachable endpoint in `ARUNA_COMPUTE_S3_URL`
or `S3_PUBLIC_URL`. `ARUNA_COMPUTE_K8S_S3_PORT` defaults to that URL's explicit or known
scheme port, falling back to 443. `ARUNA_COMPUTE_LOCAL_ONLY` disables workspaces and
sessions for that executor.

The session images are built from `scripts/session-python` and `scripts/session-deno`, which share
the helper in `scripts/session-helper`. Build them with their `build.sh`; the runtime catalog names
`harbor.computational.bio.uni-giessen.de/aruna/aruna-session-python:0.2.0` and `harbor.computational.bio.uni-giessen.de/aruna/aruna-session-deno:0.1.0`.
Python notebooks accept `requirements.txt` for pip packages or `environment.yml` for one Conda
environment shared by the Python kernel and Bash cells. Use **Dependencies** in the notebook
to save the definition and restart the kernel to install changes.

On Kubernetes with `ARUNA_COMPUTE_K8S_S3_MOUNT_DRIVER` set, a session sees a folder of its
workspace bucket below its working directory: `data/` at `/work/data` unless the submission's
`session_mount` names another bucket folder (an empty prefix is the whole bucket) and kernel
folder. Files written there land in the bucket and objects put into the bucket show up there.
The mount follows S3 semantics: files are written in one go, and there is no append or rename.
Without the driver, or on Docker and Apptainer, a session reaches its bucket over S3 only.

## Durability Configuration

`ARUNA_FJALL_PERSIST_MODE` controls the Fjall persist mode used by Aruna's local storage engine and document-sync metadata state.

| Value | Durability contract |
| --- | --- |
| `buffer` (default) | Flushes data to OS buffers before local Fjall persistence returns. This keeps write latency low and protects against an application crash, but recently acknowledged writes are not guaranteed after an OS crash or power loss. |
| `sync_all` | Flushes data and metadata with `fsync` before local Fjall persistence returns. This gives stronger local crash durability at higher write latency. |

The setting does not change replication, authorization, or RO-Crate semantics. Metadata requests marked `MetadataRequestDurability::WalAlreadyDurable` have already been accepted by the metadata event-log phase, so document-sync projection flushes may be deferred. The event-log write and later projection flush still use the configured Fjall mode; `buffer` does not become fsync-durable because a request is WAL-first.

Object-backed RO-Crate imports copy the archive into a hidden seekable spool. Until that spool is
deleted at the end of the import, the importing node can temporarily use roughly twice the archive's
stored bytes; operators should reserve capacity accordingly.

### Invenio and Zenodo transfers

The native REST API transfers crates through durable jobs. Create an HTTP source connector
with `public_config.endpoint` set to the repository API root, for example
`https://zenodo.org/api/` or `https://sandbox.zenodo.org/api/`. Store a repository personal
access token in `secret_config.token` for private imports. Exports require the requesting user's
own Invenio/Zenodo access token in `repository.access_token`; the connector token is never used
for publishing. The node's egress policy applies to all requests.

Search published records with `GET /api/v1/metadata/invenio/records`, passing `group_id`,
`connector_id`, `q`, `page` and `size` as query parameters. Results use the native repository
JSON representation. Pages start at 1, size is at most 25, and `all_versions=true` includes
older published versions. Search requires READ on the connector group and does not import data.

Import a record with `POST /api/v1/metadata/invenio/imports`:

```json
{
  "group_id": "<connector-group-id>",
  "connector_id": "<http-connector-id>",
  "record_id": "1234567",
  "mode": "copy",
  "all_versions": true,
  "target": {"bucket": "research", "prefix": "zenodo/1234567"},
  "metadata": {"group_id": "<destination-group-id>", "path": "datasets/zenodo", "public": false},
  "idempotency_key": "import-zenodo-1234567"
}
```

Every accessible published version becomes a separate dataset within the imported crate by
default. Set `all_versions: false` to import only the selected version. Mode `copy` copies files
and checks their source sizes and checksums. Mode `reference` creates native Aruna object
references, reading repository bytes on demand; the target bucket and connector must share
a group. Mode `metadata` skips attached files and file-list requests, allowing metadata imports
without access to restricted data. References depend on remote availability and credentials.
Each version also
contains `invenio-record.json`, preserving its complete record and file metadata, including
DOIs, concept identifiers, timestamps, relations, creator identifiers and custom fields.
Foreign identifiers remain provenance; Aruna assigns local document and object identities.
Filenames are encoded in storage paths so repeated or unsafe source names cannot collide.
Source names remain in the metadata. Missing files, incomplete pagination and checksum
failures fail the transfer. Hidden edit histories and inaccessible or deleted records are
not exposed by the repository API and cannot be reconstructed. Native metadata scalar values
are queryable as `additionalProperty` entries whose `propertyID` is a JSON-pointer-style path,
such as `metadata/funding/0/award/number`. Partial publication dates use their earliest day for
crate validation; `https://w3id.org/aruna/invenio/publicationDate` retains the exact original
date or interval, which is restored on export when the mapped date has not been edited.

Export with `POST /api/v1/metadata/{document_id}/invenio/exports`:

```json
{
  "repository": {
    "group_id": "<connector-group-id>",
    "connector_id": "<http-connector-id>",
    "access_token": "<personal-access-token>",
    "publish": false
  },
  "idempotency_key": "export-research-dataset"
}
```

Exports create native Invenio records with each data file uploaded separately under its crate
path. Files can be listed and downloaded directly through Invenio/Zenodo. The RO-Crate JSON is
also retained as a provenance file for fields without a native equivalent. Repository metadata
is derived from the crate's standard schema.org
fields; optional `repository.metadata` fields override the mapping. Imported native metadata,
including affiliations, funding, relations and resource type, is retained when its corresponding
crate fields are unchanged. Custom fields are also restored; the destination must support
their vocabulary. Override controlled vocabulary fields for the target repository as needed.
Source identifiers become provenance relations;
the transfer does not claim an existing source DOI as a newly issued repository DOI.
Exports with omitted files fail.
`publish: false` (the default) leaves an unpublished draft with restricted file access;
`publish: true` publishes after verifying every uploaded file. Repository validation and
publication permissions still apply. Set `repository.public_files: true` explicitly to make
the files public when publishing; otherwise files remain restricted. Existing drafts retain
their configured access, with their metadata replaced by the mapped crate metadata.

Set `repository.new_version` to an existing published record ID to create its next version
under the same parent identifier. This requires permission on that record. A new-version draft
inherits repository access settings and may already exist; unexpected files cause a failure.
Supply `draft_id` alongside `new_version` when recovering that draft. Metadata updates use the
captured draft revision and fail on conflicts. Metadata and the complete file set are checked
before and after publication; the upstream publication action has no atomic revision guard.
Exporting an imported history remains one crate snapshot unless versions are submitted
separately. Original publication timestamps and hidden edit histories are not recreated.

The user's personal token determines the owning Invenio/Zenodo account; bibliographic authors
come from the crate's creators. Aruna encrypts the token for the job's retention period, binding
it to the requesting user, node, connector and endpoint. It is never echoed in responses,
debug output or public job results. Each export submission requires the user's token; changing the
token changes the idempotency identity. No shared publishing account is selected implicitly.

The response provides job status and report URLs; the existing job API also supports cancellation.
Successful exports include `result.repository` with the record ID, parent ID, revision, assigned
DOI when available, API URL and publication state. Both transfers support `idempotency_key`.
An ambiguous draft-creation response stops
automatic creation; inspect the repository and supply `repository.draft_id` in a new request
to reuse the unpublished draft. Failed or cancelled transfers leave remote drafts available
for inspection. Import requires connector-group READ and destination WRITE; export requires
crate READ and connector-group WRITE.

## License

Aruna is licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Aruna by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.


## Feedback & Contributions

Found a bug or have an idea? Open an [issue](https://github.com/arunaengine/aruna/issues) or send a pull request. Reports from the v3 public test phase help us understand what works and what needs attention. See the [Contributor Guidelines](./CONTRIBUTING.md) and [Code of Conduct](./CODE_OF_CONDUCT.md) before contributing.
