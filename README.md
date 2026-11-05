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
  <a href="#getting-started">Getting started</a> ·
  <a href="#command-line-client">Command line client</a> ·
  <a href="#desktop-app">Desktop app</a> ·
  <a href="#concepts-in-brief">Concepts</a> ·
  <a href="#operations">Operations</a> ·
  <a href="#feedback-and-contributions">Contributing</a>
</p>

> [!NOTE]
> **Aruna v3 is now in public testing.** You can [try it out](https://v3.aruna-engine.org) and share feedback through [GitHub issues](https://github.com/arunaengine/aruna/issues). Aruna v2 remains available on the [v2 branch](https://github.com/arunaengine/aruna/tree/v2).

Aruna helps research organizations store, describe, share and reuse data. Each organization runs
its own node and keeps control of its storage and access rules. Nodes connect directly to each
other. Researchers can then find and use data across institutions.

Aruna is for research groups, institutes, consortia and computing centers that want to share data
without giving it to a central service.

## Features

- **Web portal**: browse files, edit datasets, manage access and follow compute runs.
- **S3 interface**: every node speaks [S3](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html), so common tools and workflow systems work as they are.
- **Dataset descriptions**: datasets use [RO-Crate](https://www.researchobject.org/ro-crate/). Profiles check that a description is complete.
- **Native Git**: every dataset is also a Git repository in the [ARC](https://arc-rdm.org/) layout.
- **History**: see every version of a dataset, compare versions and merge drafts.
- **Search**: find datasets on all connected nodes, limited to what you may see.
- **Access control**: permissions for people, groups and roles. Group admins see group statistics and can revoke group tokens.
- **Offline edits**: edits made on different nodes merge when the nodes reconnect.
- **Persistent identifiers**: datasets resolve through `https://w3id.org/aruna/<id>`.
- **Publishing**: send datasets with a DOI to [Zenodo](https://zenodo.org/) or other [InvenioRDM](https://inveniordm.docs.cern.ch/) repositories.
- **Compute**: run analysis containers from the portal or through [GA4GH TES](https://www.ga4gh.org/product/task-execution-service-tes/), and write Jupyter notebooks in the portal.
- **AI assistants**: connect assistants through [MCP](https://modelcontextprotocol.io/). They act with your permissions.
- **Flexible storage**: use local disks, your own S3 storage, or local and NFS folders imported in place.
- **Encrypted buckets**: new files are stored encrypted, with keys held by the people you choose.
- **Safe transfers**: files are checked when they are stored and copied.
- **Open standards**: [OIDC](https://openid.net/connect/) sign-in, [GA4GH DRS](https://www.ga4gh.org/product/data-repository-service-drs/) and [OAI-PMH](https://www.openarchives.org/pmh/).
- **Simple to run**: one program or one container per node, with live backups.

## Getting started

### Try it online

- Open the [public v3 portal](https://v3.aruna-engine.org).
- Read the [portal documentation](https://v3.aruna-engine.org/app/docs/v1) for step-by-step guides.
- Explore the REST API in the [Swagger UI](https://api.node-1.v3.aruna-engine.org/swagger-ui/).

### Run with Docker: single node

You need Docker. The node image is published on Harbor for linux/amd64. It contains `aruna`,
`aruna-doctor` and the web portal:

```bash
IMAGE=harbor.computational.bio.uni-giessen.de/aruna/aruna-v3-demo:v3.0.0-alpha.157-test.2a6fb687d-portal.7c844cae
docker pull "$IMAGE"
```

To build the image from a checkout of this repository instead, run `docker build -t aruna:local .`
and set `IMAGE=aruna:local`. A first build takes 10 to 30 minutes. A built image has no web portal
unless you stage one (see the [operations guide](docs/operations.md#configuration)).

Create a file `aruna.env`. The node reads it as `/run/.env`. Replace the OIDC values with those of
your login server.

```bash
STORAGE_PATH=/data
SOCKET_ADDRESS=0.0.0.0:3000
P2P_SOCKET_ADDRESS=0.0.0.0:3001
OPS_SOCKET_ADDRESS=0.0.0.0:3002
S3_ADDRESS=0.0.0.0:1337
S3_HOST=127.0.0.1:1337
API_PUBLIC_URL=http://127.0.0.1:3000
S3_PUBLIC_URL=http://127.0.0.1:1337
REALM_DESCRIPTION="My realm"
OIDC_PROVIDER_IDS=main
OIDC_MAIN_ISSUER=https://id.example.org/realms/aruna
OIDC_MAIN_AUDIENCE=aruna-api
OIDC_MAIN_DISCOVERY_URL=https://id.example.org/realms/aruna/.well-known/openid-configuration
```

Start the node and check that it is ready:

```bash
docker run -d --name aruna \
  -p 3000:3000 -p 1337:1337 -p 3001:3001/udp -p 127.0.0.1:3002:3002 \
  -v aruna-data:/data \
  -v "$PWD/backups:/backups" \
  -v "$PWD/aruna.env:/run/.env:ro" \
  "$IMAGE"
curl http://127.0.0.1:3002/readyz
```

The first start creates a new realm. The API documentation is then at
`http://127.0.0.1:3000/swagger-ui` and S3 at `http://127.0.0.1:1337`. Port 3002 serves health and
metrics without sign-in, so it stays on loopback.

Create the first admin token. The database allows one writer, so stop the node to mint the
secret:

```bash
docker stop aruna
SECRET=$(docker run --rm -v aruna-data:/data -v "$PWD/aruna.env:/run/.env:ro" \
  "$IMAGE" /run/aruna-doctor recover-admin | tail -n 1)
docker start aruna

export ARUNA_OIDC_PASSWORD='<password of your OIDC user>'
docker exec -e ARUNA_OIDC_PASSWORD aruna /run/aruna-doctor create-token \
  --oidc-username <your OIDC user> --bootstrap-secret "$SECRET"
```

The last line printed is the admin token. `create-token` signs in with the OIDC password grant, so
the OIDC client must allow direct access grants.

### Small local realm

This starts three connected nodes as local processes, with a test login server and a portal per
node. It runs on Linux and needs Rust (see [rust-toolchain.toml](rust-toolchain.toml)),
[`just`](https://github.com/casey/just), `curl`, `ss` and Docker for the login server. The first
run builds the workspace from source and downloads the latest portal prerelease.

```bash
just preview          # three nodes, a login server and a portal per node
just local-cluster    # three nodes without login server and portal
just stop             # stop everything, also after the terminal was closed
```

The command prints the address of each node, test logins and admin credentials. Press Ctrl-C to
stop it. The local nodes run compute jobs with Docker by default.

For development, `just local` builds the image and starts one node with a test login server,
Prometheus and Grafana in Docker Compose. It uses host networking, so it needs Linux.

## Command line client

`aruna-cli` works with a realm from the terminal. It talks to a node's REST and S3 APIs and needs
no local node. With it you can:

- list buckets and objects, and create buckets,
- copy files and sync folders, with multipart uploads that resume,
- search the realm and create, import, validate and export RO-Crate metadata,
- submit and follow compute jobs through GA4GH TES,
- create share links and print S3 credentials for other tools.

### Install

There are no release binaries yet. Install it with Rust 1.97.1 or newer, from a checkout of this
repository or straight from Git:

```bash
cargo install --locked --path cli
cargo install --locked --git https://github.com/arunaengine/aruna aruna-cli
```

Pre-built binaries are planned. They will appear on the
[releases page](https://github.com/arunaengine/aruna/releases) with tags that start with `cli-v`.

### Sign in

Create a token in the portal under Settings, API sessions. Then sign in with the API address of
your node:

```bash
aruna-cli login --api https://node.example --token <TOKEN>
aruna-cli whoami
```

`login` creates a CLI session and keeps its token in the OS keyring. Without `--token` it reads the
token from standard input. Without a keyring, set `ARUNA_API` and `ARUNA_TOKEN` instead of signing
in. Scripts can do the same.

### Examples

```bash
aruna-cli buckets ls
aruna-cli cp results.csv my-bucket/run-42/
aruna-cli sync ./run-42 my-bucket/run-42 --dry-run
aruna-cli meta search "proteomics"
aruna-cli jobs submit task.json
aruna-cli share my-bucket/run-42/ --expires 7d
```

For `cp` and `sync`, the source sets the direction: a local source uploads, a remote one
downloads. `aruna-cli --help` describes every command.

### Third-party notice

On Linux, `aruna-cli` includes a copy of libdbus, built from source, to reach the Secret Service
keyring. libdbus, part of the freedesktop.org D-Bus project, is offered under the Academic Free
License version 2.1 or the GPL version 2; `aruna-cli` uses it under the
[Academic Free License version 2.1](https://spdx.org/licenses/AFL-2.1.html). Its copyright and
license notices are in the `vendor/dbus` folder of the `libdbus-sys` crate.

## Desktop app

[Aruna Desktop](https://github.com/arunaengine/aruna-desktop) connects your computer to a realm. It
runs a small Aruna node on your computer and shows the same portal as the web. With it you can:

- sign in with your realm account, through the login providers of your realm,
- browse, upload and share data in the built-in portal,
- link a local folder to a bucket and keep both in sync, one way or both ways,
- run jobs on your computer with Docker, or Apptainer on Linux,
- pause the device, which stops sync and local jobs until you resume.

### Download

Installers will appear as pre-releases on the
[Aruna Desktop releases page](https://github.com/arunaengine/aruna-desktop/releases). They cover
Linux (`.deb`, `.rpm`, `.AppImage`), Windows (`-setup.exe`) and macOS on Apple silicon (`.dmg`).
Pick the release whose version matches the version your realm runs. The
[Aruna Desktop readme](https://github.com/arunaengine/aruna-desktop#how-to-install) explains how
to install it on each system and how to add your computer as a device.

### Build from source

Aruna Desktop is a [Tauri](https://tauri.app/) app. A build bundles an `aruna` node from this
repository and the portal, both at the versions pinned in its `versions.json`. The
[release workflow](https://github.com/arunaengine/aruna-desktop/blob/main/.github/workflows/release.yml)
shows every step.

## Concepts in brief

- **Node**: one organization's server. It decides where the data lives and who may access it.
- **Realm**: nodes that trust each other, for example an institute or a consortium. Being in a
  realm gives no access to data. Access is always granted through groups, roles and permissions.
- **Direct connections**: nodes connect to each other, also from behind firewalls. A node keeps
  working while others are offline and shares its changes once they are back.
- **Datasets**: files and their descriptions move together. Crates can reference other crates, so
  you can trace how a result was produced.

The goal is FAIR research data: findable, accessible, interoperable and reusable. Each institution
stays responsible for its own data.

## Operations

- **Configuration**: the node reads environment variables and a `.env` file.
  [`.env.example`](.env.example) lists every variable with its default.
- **Keys**: the node creates its realm and node keys on the first start. The tracked `.env` holds
  published demo keys, and a node refuses to start with them.
- **First start**: a node with an empty data folder creates a new realm. With `ONBOARDING_SECRET`
  it joins an existing realm instead. This works only on the first start.
- **Backups**: `aruna-doctor backup` copies the database of a running node. Copy the stored files
  separately, or stop the node and copy the whole data folder. In Docker:
  `docker exec aruna /run/aruna-doctor backup /data /backups/aruna.bak`.
- **Upgrades**: take a backup and stop the node. Then run `aruna-doctor migrate` with the new
  version and start it. In Docker, pull or build the new image and set `IMAGE` to it, then remove
  the old container with
  `docker stop aruna && docker rm aruna`. Migrate with
  `docker run --rm -v aruna-data:/data "$IMAGE" /run/aruna-doctor migrate /data` and start a
  new container with the same `docker run` command as above.
- **Local and NFS folders**: groups can import folders of the node in place. In a container, the
  folder must be mounted, and its path is the path inside the container.
- **Realm registry**: a realm with public addresses lists itself in the global registry. Set
  `FEDERATION_REGISTRY_URL=` (empty) before the first start to opt out.

The [operations guide](docs/operations.md) covers configuration, restore, encrypted buckets, your
own storage, NFS settings and the registry in detail.

## Learn more

- [Portal documentation](https://v3.aruna-engine.org/app/docs/v1): how to use Aruna in the browser.
- [Swagger UI](https://api.node-1.v3.aruna-engine.org/swagger-ui/): the full REST API.
- [Operations guide](docs/operations.md): running nodes and managing storage.
- [Native Git guide](scripts/arc-native/README.md): working with datasets as Git repositories.
- [Repository structure](REPO_STRUCTURE.md): how the source code is organized.

## License

Aruna is licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Aruna by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.


## Feedback and contributions

Found a bug or have an idea? Open an [issue](https://github.com/arunaengine/aruna/issues) or send a pull request. Reports from the v3 public test phase help us understand what works and what needs attention. See the [Contributor Guidelines](./CONTRIBUTING.md) and [Code of Conduct](./CODE_OF_CONDUCT.md) before contributing.
