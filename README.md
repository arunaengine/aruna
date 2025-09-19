[![Rust](https://img.shields.io/badge/built_with-Rust-dca282.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/License-Apache_2.0-brightgreen.svg)](https://github.com/arunaengine/aruna/blob/main/LICENSE-APACHE)
[![License](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://github.com/arunaengine/aruna/blob/main/LICENSE-MIT)
![CI](https://github.com/arunaengine/aruna/actions/workflows/test.yml/badge.svg)
[![Codecov](https://codecov.io/github/arunaengine/aruna/coverage.svg?branch=main)](https://codecov.io/gh/ArunaStorage/aruna)
[![dependency status](https://deps.rs/repo/github/ArunaStorage/aruna/status.svg?path=components%2Fserver)](https://deps.rs/repo/github/ArunaStorage/aruna?path=components%2Fserver)
___

<p align="center">
    <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./img/aruna_white_font.png">
    <img alt="Aruna logo" src="./img/aruna_dark_font.png" width="70%">
    </picture>
</p>

<br>

# Aruna Data Orchestration Engine

**Aruna** is a geo-redundant data orchestration engine that manages scientific data and a rich set of associated metadata according to [FAIR](https://www.go-fair.org/fair-principles/) principles.

It supports multiple data storage backends (e.g. S3, File ...) via [data proxies](https://github.com/arunaengine/aruna/tree/main/components/data_proxy) that expose an S3-compatible interface.
The main [server](https://github.com/arunaengine/aruna/tree/main/components/server) handles metadata, user and resource hierarchies while the [data proxies](https://github.com/arunaengine/aruna/tree/main/components/data_proxy) handle the data itself.
Data proxies can communicate with each other in a peer-to-peer-like network and share data.

This repository is split into two components, the server and the data proxy.

## Features

- [FAIR](https://www.go-fair.org/fair-principles/), geo-redundant, data storage for multiple scientific domains
- A decentralized data storage system with a global catalog and authorization functionality provided by servers and a fully distributed network of data locations that sovereignly manage access.
- Dedicated rule system to enforce custom policies for your project using [Common Expression Language (CEL)](https://github.com/google/cel-spec)
- Data catalog for listing, searching, and viewing all available (meta)data
- Unified access to data backends via the S3-compliant interface provided by data proxies
- Proxies enforce sovereign domain- and location-specific rules and policies, leaving the final decision of who gets access to data to the data owners
- Ingest existing data and automatically register it in the distributed catalog
- Organize data as objects and group them into projects, collections, and datasets; link internal and external data in a sophisticated relationship graph  
- Flexible, file format and data structure and ontology independent metadata annotation via labels and dedicated metadata files (e.g. [schema.org](https://schema.org/))
- Full transparency via notification streams for all performed actions
- Compatible with multiple (existing) backend data storage architectures (S3, File, ...)
- S3-compatible API for pre-authenticated upload and download URLs
- REST-API and dedicated client libraries for Python, Rust, Go and Java
- Hook system to integrate external workflows for data validation and transformation and processing

## Getting started

Aruna is build as a managed service for our scientific partners or as a self-deployed open source collection of components for your own needs. Visit [aruna-engine.org](https://aruna-engine.org) to learn more about our managed data management service. 

### How to run a local test instance

To get started with a local instance for testing you first need to have [docker](https://docs.docker.com/engine/install/)/[podman](https://podman.io/docs/installation) and [docker-compose](https://docs.docker.com/compose/install/)/[podman-compose](https://github.com/containers/podman-compose) installed.

Start the needed containers:

```bash
curl -L https://demo.aruna-storage.org | docker compose -f - up
```

or download and run the local [compose_deploy.yaml](compose_deploy.yaml) for yourself.

> [!CAUTION]
> This deployment contains hard-coded credentials and is therefore NOT suitable for any production or public use, only use it for local testing

This will run the following pre-required components at ports:

- S3 Server: [minio](https://github.com/minio/minio) `:9000`
- Serach index: [meilisearch](https://github.com/meilisearch/meilisearch) `:7700`
- Authorization/OIDC: [keycloak](https://github.com/keycloak/keycloak) `:1998`
- Messaging system: [nats](https://github.com/nats-io/nats-server) `:4222` / `:8222`
- SQL Database: [yugabyte](https://github.com/yugabyte/yugabyte-db) `:5433`

Additionally this will also run the following aruna specific components:

- Aruna Server `:50051`
- Aruna Web `:3000`
- Aruna REST Gateway `:8080`
- Aruna Dataproxy_1 `:50052` / `:1337`
- Aruna Dataproxy_2 `:50055` / `:1338`

### Interacting with aruna

Test tokens and Website credentials for the local test deployment can be found in [here](./components/server/tests/common/keycloak/fake-tokens.md).

A detailed user guide is found in the [Documentation](https://docs.aruna-engine.org/latest). For language specific details please visit our specific documentations:

**TLDR:**
- **REST**: see our [Swagger Documentation](https://aruna-engine.org/swagger-ui/)
- **Python**: [PyPI](https://pypi.org/project/Aruna-Python-API/) 
- **Rust**: [Crates.io](https://crates.io/crates/aruna-rust-api)
- **Go**: [Go package](https://github.com/arunaengine/go-api/releases/)


## License

The API is licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion for Aruna by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions. 


## Feedback & Contributions

If you have any ideas, suggestions, or issues, please don't hesitate to open an issue and/or PR. Contributions to this project are always welcome ! We appreciate your help in making this project better. Please have a look at our [Contributor Guidelines](./CONTRIBUTING.md) as well as our [Code of Conduct](./CODE_OF_CONDUCT.md) for more information.
