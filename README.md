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
  <a href="#why-aruna">Why Aruna</a> ·
  <a href="#features">Features</a> ·
  <a href="#getting-started">Getting started</a> ·
  <a href="#avoiding-common-pitfalls">Pitfalls</a> ·
  <a href="#feedback--contributions">Contributing</a>
</p>

> [!NOTE]
> **Aruna v3 is now in public testing.** You can [try it out](https://v3.aruna-engine.org) and share feedback through [GitHub issues](https://github.com/arunaengine/aruna/issues). Aruna v2 remains available on the [v2 branch](https://github.com/arunaengine/aruna/tree/v2).

Aruna helps research organizations store, describe, share and reuse their data, while every
organization keeps control of its own infrastructure. Each organization runs its own Aruna node.
The nodes connect to each other directly, so researchers can find and work with data across
institutions without a central service in the middle.

## Why Aruna

Research data rarely lives in one place. It is spread over universities, labs, archives and
computing centers. Each of them has its own storage, its own rules and its own responsibility for
the data. Researchers who work together still need to find data, understand what it is, share it
and analyze it, often across all of these places.

Central platforms solve this by collecting everything in one spot. That works for some data, but
many institutions cannot or do not want to give up control over where their data is stored and who
may see it. Copying data around by hand is slow and error-prone, and descriptions of the data get
lost on the way.

Aruna takes a different path:

- **Every organization stays in charge.** Each organization runs its own node, decides where its
  data lives and who may access it.
- **Nodes work together as a network.** Nodes that trust each other form a *realm*, for example an
  institute, a consortium or a project. Being in the same realm does not give anyone access to data;
  access is always granted explicitly.
- **Data and its description belong together.** Every dataset carries rich, standardized metadata,
  so others can understand and reuse it years later.

The goal is to make research data FAIR: findable, accessible, interoperable and reusable, without
taking it away from the people and institutions responsible for it.

## Features

### Store and access data

- **Works with your existing tools**: every node offers an [S3](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)-compatible interface, so common tools, scripts and workflow systems work without changes.
- **Flexible storage**: keep data on local disks or connect other storage systems. Buckets can combine local files, copies and references to files on other nodes.
- **Safe transfers**: files are checked when they are stored and while they are copied, so damaged data is noticed early.

### Describe and find data

- **Rich descriptions**: datasets are described with [RO-Crate](https://www.researchobject.org/ro-crate/), a widely used standard that covers files, people, instruments, software and workflows.
- **Quality checks**: profiles define what a good description of a dataset needs, and Aruna shows what is still missing.
- **Search across nodes**: find datasets on all nodes of a realm, limited to what you are allowed to see.
- **Changes merge on their own**: edits made on different nodes, even while offline, come together automatically.

### Work together

- **Web portal**: browse files, edit datasets, manage access and follow compute runs in the browser.
- **Groups and permissions**: give people access to exactly the data they need, alone or through groups and roles.
- **Native Git for datasets**: every dataset is also a Git repository in the [ARC](https://arc-rdm.org/) layout. Clone it, work on branches and push changes back; the dataset description stays in sync.
- **Dataset history**: see every version of a dataset, compare versions and merge draft changes in the portal.

### Analyze data

- **Compute jobs**: run analysis containers on your own infrastructure from the portal or through the standard [GA4GH TES](https://www.ga4gh.org/product/task-execution-service-tes/) interface.
- **Interactive notebooks**: write and run Jupyter notebooks in the portal, with direct access to your data.
- **AI assistants**: connect AI assistants through [MCP](https://modelcontextprotocol.io/) so they can search, read and work with your data on your behalf, with your permissions.

### Share and publish

- **Publish with a DOI**: send datasets to [Zenodo](https://zenodo.org/) or other [InvenioRDM](https://inveniordm.docs.cern.ch/) repositories and keep both in sync. Existing records can be imported, too.
- **Open standards**: single sign-on with [OIDC](https://openid.net/connect/), data references with [GA4GH DRS](https://www.ga4gh.org/product/data-repository-service-drs/) and metadata harvesting with [OAI-PMH](https://www.openarchives.org/pmh/).

### Run it your way

- **Simple to deploy**: run a single node as one program, or a cluster of nodes.
- **Works across networks**: nodes find and reach each other directly, also behind firewalls.

## Getting Started

Choose the way that fits you best. They are ordered from quickest to most hands-on.

### 1. Try it online

The easiest start needs no installation:

- Open the [public v3 portal](https://v3.aruna-engine.org) and look around.
- Read the [portal documentation](https://v3.aruna-engine.org/app/docs/v1) for step-by-step guides.
- Developers can explore the REST API in the [Swagger UI](https://api.node-1.v3.aruna-engine.org/swagger-ui/).

### 2. Run a small cluster on your computer

This starts three connected nodes on your machine, so you can see how nodes work together. You
need `docker` with Docker Compose and, for convenience, [`just`](https://github.com/casey/just).

```bash
just preview          # three nodes, a login server and the web portal
just local-cluster    # three nodes without the portal
```

When everything is ready, the command prints the addresses of each node, test logins and an
admin token. Press Ctrl-C to stop the cluster, or run `just stop` if the terminal was closed.

### 3. Run a single node

To run one node yourself, for example to test it with your own login server:

```bash
cp .env.example .env
cargo run -p aruna
```

Building from source needs the Rust version named in [rust-toolchain.toml](rust-toolchain.toml).
The node then serves the API documentation on `http://127.0.0.1:3000/swagger-ui` and the S3
interface on `http://127.0.0.1:1337`.

## Avoiding Common Pitfalls

Most problems with a new node come from a few settings. These tips help you avoid them.

- **Use your own keys.** The example configuration contains demo keys that everybody can see. A
  node refuses to start with them. Create your own realm and node keys (`REALM_*_KEY` and
  `NODE_*_KEY`) before running a real node.
- **Set up a node right the first time.** A node creates or joins its realm only on its very first
  start: without `ONBOARDING_SECRET` it creates a new realm, with it it joins an existing one.
  Changing the configuration afterwards does not move it to another realm. To start over, use a
  new, empty data directory.
- **Back up the whole data directory.** The data directory (`STORAGE_PATH`) also holds the node's
  identity, which protects stored secrets such as access tokens. A node restored without it cannot
  read these secrets anymore.
- **Migrate before upgrading.** Stop the node, run `aruna-doctor migrate` on its data directory,
  then start the new version. Running the migration twice does no harm.
- **Keep enough free disk space.** Importing a large dataset briefly needs about twice its size.
- **Choose safety or speed on purpose.** By default, Aruna favors speed: the last few changes can be
  lost if the machine loses power. Set `ARUNA_FJALL_PERSIST_MODE=sync_all` if that is not
  acceptable for you.
- **Keep notebook networks separate.** Notebook sessions run in their own network so they can only
  reach your data. Make sure this network (`ARUNA_COMPUTE_DOCKER_SESSION_SUBNET`) does not overlap
  with other networks on the host.
- **Use Git the usual way.** Log in to the Git repository of a dataset with an Aruna access token
  as the password, pull before you push, and store large files with Git LFS.

## Learn More

- [Portal documentation](https://v3.aruna-engine.org/app/docs/v1): how to use Aruna in the browser.
- [Swagger UI](https://api.node-1.v3.aruna-engine.org/swagger-ui/): the full REST API.
- [Native Git guide](scripts/arc-native/README.md): working with datasets as Git repositories.
- [Repository structure](REPO_STRUCTURE.md): how the source code is organized.

## License

Aruna is licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Aruna by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.


## Feedback & Contributions

Found a bug or have an idea? Open an [issue](https://github.com/arunaengine/aruna/issues) or send a pull request. Reports from the v3 public test phase help us understand what works and what needs attention. See the [Contributor Guidelines](./CONTRIBUTING.md) and [Code of Conduct](./CODE_OF_CONDUCT.md) before contributing.
