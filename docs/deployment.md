<!-- Production and pre-production deployment guide for Aruna v3 node operators. -->
<!-- Copyright (c) 2026 The Aruna Contributors -->
<!-- SPDX-License-Identifier: MIT or Apache-2.0 -->

# Production deployment guide

This guide walks operators through preparing a host, choosing a release, configuring networking and
identity providers, running a persistent Aruna v3 node with Docker Compose, configuring storage
backends and setting up backups and upgrades.

For day-to-day storage rules, encrypted buckets, NFS import roots, the global registry and node
trust, see the [operations guide](operations.md). For every environment variable and its default,
see [`.env.example`](../.env.example).

## Contents

- [Releases and image tags](#releases-and-image-tags)
- [Host sizing and preparation](#host-sizing-and-preparation)
- [Public endpoints, DNS and firewall ports](#public-endpoints-dns-and-firewall-ports)
- [Recommended deployment with Docker Compose](#recommended-deployment-with-docker-compose)
- [Persistent directories and storage layout](#persistent-directories-and-storage-layout)
- [Storage backends and quotas](#storage-backends-and-quotas)
- [Identity provider (OIDC) setup](#identity-provider-oidc-setup)
- [First start and initial admin bootstrap](#first-start-and-initial-admin-bootstrap)
- [Backups and disaster recovery](#backups-and-disaster-recovery)
- [Upgrades and migrations](#upgrades-and-migrations)
- [Observability, compute and multi-node realms](#observability-compute-and-multi-node-realms)
- [Operator pre-flight checklist](#operator-pre-flight-checklist)

## Releases and image tags

Aruna v3 is currently in public alpha (`v3.0.0-alpha.*`). Pre-built Linux (`linux/amd64`) container
images are published on the GitHub Container Registry:

```text
ghcr.io/arunaengine/aruna:<tag>
```

Each published image uses a distroless Debian 13 runtime (no shell inside the container) and
bundles:

- `/run/aruna`: the node server binary (default entrypoint),
- `/run/aruna-doctor`: offline and live maintenance CLI (backups, restores, migrations, admin
  recovery and diagnostics),
- `/run/iroh-doctor`: peer-to-peer QUIC and relay diagnostics,
- `/run/portal`: the pre-built web portal single-page application when the release embeds a portal
  (marked by a `v<version>-portal.<portal-tag>` tag in the release's `release-images.json`).
  `PORTAL_DIR=/run/portal` is set in the image; setting `PORTAL_MODE=artifact` serves this embedded
  copy without downloading. Headless releases omit `/run/portal` and require `PORTAL_ARTIFACT_URL`
  and `PORTAL_ARTIFACT_SHA256` to serve the portal.

### Which tag to deploy

- **Persistent pre-production and production (recommended):** pin an explicit release tag, starting
  at `v3.0.0-alpha.210` on GHCR (or the latest tagged release listed on the
  [GitHub releases page](https://github.com/arunaengine/aruna/releases)). When serving the embedded
  portal with `PORTAL_MODE=artifact` and no `PORTAL_ARTIFACT_URL`, select the portal-qualified tag
  (`v<version>-portal.<portal-tag>`) or pinned digest `reference` from the release's
  `release-images.json`:

  ```bash
  curl -fsSL https://github.com/arunaengine/aruna/releases/download/v3.0.0-alpha.210/release-images.json \
    | jq -r '.images[] | select(.name == "aruna") | (.tags[] | select(contains("-portal."))), .reference'
  ```

  Pinning prevents a container recreation from silently pulling a newer release before you have
  taken a backup and run `aruna-doctor migrate`.
- **Short-lived evaluation only:** the floating `v3` tag tracks the newest v3 release (and is
  headless when that release was built without a portal tag). Do not use it for persistent
  installations where you want controlled upgrades.
- **Client version alignment:** match [Aruna Desktop](https://github.com/arunaengine/aruna-desktop/releases)
  and `aruna-cli` versions to the release your realm runs.
- **Building from source:** run `docker build -t aruna:local .` from a tagged commit checkout if
  you need a custom build (for example, to enable optional features or audit the build). Source
  builds omit the web portal unless the built
  [website](https://github.com/arunaengine/website) is staged in `.portal-embed` before building, or
  you configure `PORTAL_MODE=artifact` with `PORTAL_ARTIFACT_URL` and `PORTAL_ARTIFACT_SHA256` to
  fetch a signed release archive at startup. `bash scripts/build_images.sh --portal <portal tag> aruna`
  builds the image with a portal release of the website whose SHA-256 it checks first.

## Host sizing and preparation

A single Linux VM (such as Debian 13 or Ubuntu LTS) running Docker Engine and the Docker Compose
plugin is the recommended baseline for a persistent single-node installation.

- **CPU and RAM:** 4 vCPUs and 8 GiB RAM are sufficient for small pilot nodes; 8 to 16 vCPUs and
  16 to 32+ GiB RAM give comfortable headroom for concurrent S3 transfers, metadata indexing and
  database caching (more if you also enable local container compute).
- **Disk layout:** place persistent data on a dedicated filesystem (for example `/srv/aruna`).
  Separate the database directory (`DATABASE_DIR`) from object storage (`CONTENT_DIR` or external
  S3 backends) when possible so database backups stay small and fast. Leave free space on
  `SPOOL_PATH` (default `DATABASE_DIR/spool`): importing a large RO-Crate dataset briefly requires
  roughly twice the dataset's size.
- **Time synchronization:** enable NTP (`systemd-timesyncd` or `chrony`) on every node. Aruna
  rejects metadata events whose timestamp skews beyond `MAX_CLOCK_SKEW_SECS` (default 300 seconds).
- **Database durability and cache:**
  - Set `ARUNA_FJALL_PERSIST_MODE=sync_all` for pre-production and production so committed writes
    fsync data and metadata before returning. The default (`buffer`) favors throughput and can lose
    the last few writes on sudden power loss.
  - Tune `ARUNA_FJALL_CACHE_MIB` (default `256` MiB). On hosts with 16 to 32+ GiB RAM, setting
    `2048` or `4096` reduces disk I/O for metadata and object lookups.
- **Registry opt-out for private pilots:** by default, a realm with public `API_PUBLIC_URL` and
  `PORTAL_PUBLIC_URL` registers its public metadata (realm ID, name, URLs and aggregate counts) in
  `https://registry.aruna-engine.org`. If your pre-production node should stay unlisted, set
  `FEDERATION_REGISTRY_URL=` (empty) **before the first start**.

## Public endpoints, DNS and firewall ports

An Aruna node serves up to four network interfaces plus an internal operations listener.

### DNS hostnames

Give each public HTTP service its own DNS hostname. Under [Native HTTPS](operations.md#native-https),
distinct hostnames are required because one TLS listener routes requests by SNI and `Host` header,
and S3 owns every subdomain beneath its host.

If your institution delegates a single subdomain such as `rdm.example.org`, a clean layout is:

| Service | Example hostname | Required DNS record | TLS certificate SAN |
| :--- | :--- | :--- | :--- |
| Web portal | `rdm.example.org` | `A` / `AAAA` to node | `rdm.example.org` |
| REST, Git and MCP API | `api.rdm.example.org` | `A` / `AAAA` to node | `api.rdm.example.org` |
| S3 API (path-style) | `s3.rdm.example.org` | `A` / `AAAA` to node | `s3.rdm.example.org` |
| S3 API (virtual-hosted, optional) | `*.s3.rdm.example.org` | Wildcard `A` / `AAAA` | `*.s3.rdm.example.org` |

> [!NOTE]
> Virtual-hosted S3 URLs (`https://<bucket>.s3.rdm.example.org/<key>`) need both a wildcard DNS
> record and a wildcard TLS certificate for `*.s3.rdm.example.org`. A wildcard certificate covers
> single-label bucket names only; buckets containing dots always use path-style requests
> (`https://s3.rdm.example.org/<bucket>/<key>`). If you do not obtain a wildcard certificate,
> configure S3 clients for path-style addressing.

### Network port matrix

| Purpose | Container port | Host exposure | Protocol | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Native HTTPS (Portal, REST, MCP, S3) | `8443` | Public (`443:8443`) | TCP | Recommended standalone setup; clients must connect by hostname, not IP. |
| HTTP-to-HTTPS redirect (optional) | `8080` | Public (`80:8080`) | TCP | Sends `308` to configured HTTPS origins. Do not rely on it for S3 clients. |
| Peer-to-peer federation (Iroh QUIC) | `3001` | Public (`3001:3001/udp`) | **UDP** | Direct node-to-node and Aruna Desktop sync. Cannot pass through an HTTP reverse proxy. |
| REST API, Swagger UI, Git, MCP | `3000` | Reverse proxy only | TCP | Do not publish directly if using Native HTTPS. |
| S3 API | `1337` | Reverse proxy / compute only | TCP | Do not publish directly if using Native HTTPS. |
| Web portal | `3003` | Reverse proxy only | TCP | Can stay `127.0.0.1:3003` under Native HTTPS. |
| Health probes and Prometheus metrics | `3002` | **Loopback only** (`127.0.0.1:3002`) | TCP | **Unauthenticated.** Never expose to the public network. |

### Peer-to-peer networking and outbound access

Aruna uses [Iroh](https://www.iroh.computer/) over QUIC (`P2P_SOCKET_ADDRESS`, default port
`3001/udp`) for node-to-node replication, federated transfers and Aruna Desktop device sync.

- **Inbound UDP `3001`:** open `3001/udp` on the host and campus firewall so peer nodes and desktop
  clients establish fast, direct encrypted QUIC connections.
- **Relay fallback:** if a restrictive firewall or symmetric NAT blocks direct UDP, Iroh
  automatically falls back to outbound HTTPS/QUIC relay connections (using the default n0 relays
  plus any custom relays configured in `P2P_ADDITIONAL_RELAY_URLS`). Direct transfers are faster
  and avoid relay bandwidth limits, so opening `3001/udp` is strongly recommended for server nodes.
- **Outbound TCP `443`:** allow outbound HTTPS from the node to:
  - your OIDC identity provider (discovery document and JWKS endpoint),
  - Iroh relay servers (if direct UDP cannot be established or during peer discovery),
  - external S3 backends, Zenodo / InvenioRDM or the global realm registry (if enabled).
- **Verify P2P connectivity** after startup with `/run/aruna-doctor iroh check` or
  `/run/iroh-doctor`.

## Recommended deployment with Docker Compose

Choose **Option A (Native HTTPS)** for a dedicated VM with no existing ingress controller: it
minimizes moving parts, handles streaming S3 uploads cleanly and avoids reverse-proxy buffering
pitfalls. Choose **Option B (Reverse proxy)** if your site already terminates TLS on a shared
Nginx, Caddy, HAProxy or Kubernetes ingress.

### Prepare host directories

Create a persistent directory tree under `/srv/aruna`:

```bash
sudo mkdir -p /srv/aruna/{config,data,blobs,backups,tls,legal}
sudo chmod 700 /srv/aruna/config /srv/aruna/data /srv/aruna/backups
```

### Option A: Docker Compose with Native HTTPS (recommended)

Create `/srv/aruna/config/aruna.env` (mode `0600`):

```bash
# Core storage paths inside the container
DATABASE_DIR=/data
CONTENT_DIR=/blobs
SPOOL_PATH=/data/spool
ARUNA_FJALL_PERSIST_MODE=sync_all
ARUNA_FJALL_CACHE_MIB=2048

# Internal plain listeners (not published on the host in Native HTTPS mode)
SOCKET_ADDRESS=0.0.0.0:3000
S3_ADDRESS=0.0.0.0:1337
PORTAL_SOCKET_ADDRESS=127.0.0.1:3003
OPS_SOCKET_ADDRESS=0.0.0.0:3002
P2P_SOCKET_ADDRESS=0.0.0.0:3001

# Native HTTPS listener and HTTP redirect
HTTPS_SOCKET_ADDRESS=0.0.0.0:8443
HTTPS_CERTIFICATE_PATH=/tls/fullchain.pem
HTTPS_KEY_PATH=/tls/privkey.pem
REDIRECT_SOCKET_ADDRESS=0.0.0.0:8080

# Public endpoints (distinct DNS hostnames required for Native HTTPS)
API_PUBLIC_URL=https://api.rdm.example.org
S3_PUBLIC_URL=https://s3.rdm.example.org
S3_HOST=s3.rdm.example.org
PORTAL_PUBLIC_URL=https://rdm.example.org
CORS_ALLOWED_ORIGINS=https://rdm.example.org

# Web portal (uses embedded /run/portal from a portal-qualified image; add PORTAL_ARTIFACT_URL and
# PORTAL_ARTIFACT_SHA256 when deploying a headless image)
PORTAL_MODE=artifact
PORTAL_LANDING=enabled

# Realm identity and metadata
REALM_DESCRIPTION="Example Research Data Realm"
# Set empty before first start to keep a private/pre-production realm out of the global registry:
FEDERATION_REGISTRY_URL=

# Identity provider (OIDC) - replace with your provider's values
OIDC_PROVIDER_IDS=main
OIDC_MAIN_ISSUER=https://id.example.org/realms/aruna
OIDC_MAIN_AUDIENCE=aruna-api
OIDC_MAIN_DISCOVERY_URL=https://id.example.org/realms/aruna/.well-known/openid-configuration

# Compute (keep disabled until needed)
ARUNA_COMPUTE_EXECUTOR=none

# Logging
LOG_FORMAT=json
RUST_LOG=info
```

Create `/srv/aruna/compose.yaml`:

```yaml
services:
  aruna:
    # Copy the portal-qualified tag or pinned digest reference from the release's release-images.json
    # (or use ghcr.io/arunaengine/aruna:v3.0.0-alpha.210 for headless or PORTAL_ARTIFACT_URL setups).
    image: ghcr.io/arunaengine/aruna:v3.0.0-alpha.210-portal.<portal-tag>
    container_name: aruna
    restart: unless-stopped
    stop_grace_period: 30s
    ports:
      - "443:8443"
      - "80:8080"
      - "3001:3001/udp"
      - "127.0.0.1:3002:3002"
    volumes:
      - /srv/aruna/data:/data
      - /srv/aruna/blobs:/blobs
      - /srv/aruna/backups:/backups
      - /srv/aruna/tls:/tls:ro
      - /srv/aruna/config/aruna.env:/run/.env:ro
      # Uncomment if using a custom backends.toml (also set BLOB_BACKENDS_PATH=/etc/aruna/backends.toml):
      # - /srv/aruna/config/backends.toml:/etc/aruna/backends.toml:ro
      # Uncomment if serving local Markdown legal pages via PORTAL_LEGAL:
      # - /srv/aruna/legal:/etc/aruna/legal:ro
```

Certificate notes for Native HTTPS:

- Place the leaf certificate first followed by intermediates in `/srv/aruna/tls/fullchain.pem`, and
  the private key in `/srv/aruna/tls/privkey.pem`. For a private or institutional CA not in public
  root stores, mount the CA certificate and set `HTTPS_CA_PATH=/tls/ca.pem`.
- Mount the **directory** (`/srv/aruna/tls:/tls:ro`), not individual files, so atomic file renames
  during renewal are visible inside the container.
- Aruna checks for renewed certificates hourly and immediately on `SIGHUP`. In your ACME / certbot
  deploy hook, write files atomically into `/srv/aruna/tls/` and run:

  ```bash
  docker kill --signal=HUP aruna
  ```

### Option B: Docker Compose behind an existing reverse proxy

If Nginx, Caddy, Traefik or a site load balancer terminates TLS on ports 80 and 443:

1. Leave `HTTPS_SOCKET_ADDRESS`, `HTTPS_CERTIFICATE_PATH`, `HTTPS_KEY_PATH`, `HTTPS_CA_PATH` and
   `REDIRECT_SOCKET_ADDRESS` **unset** in `aruna.env`.
2. Set `PORTAL_SOCKET_ADDRESS=0.0.0.0:3003` and set `TRUSTED_PROXIES` to the reverse proxy's IP or
   CIDR (for example `TRUSTED_PROXIES=127.0.0.1/32,172.18.0.0/16`) so Aruna honors
   `X-Forwarded-For`, `X-Forwarded-Proto` and `X-Forwarded-Host` for rate limiting and client URLs.
3. Publish ports `3000`, `1337` and `3003` on loopback (or only on the shared proxy Docker network),
   and keep `3001/udp` published directly on the host:

   ```yaml
   ports:
     - "127.0.0.1:3000:3000"
     - "127.0.0.1:1337:1337"
     - "127.0.0.1:3003:3003"
     - "127.0.0.1:3002:3002"
     - "3001:3001/udp"
   ```

4. Configure your reverse proxy carefully for S3 and large streaming transfers (example Nginx
   directives for each virtual host):

   ```nginx
   # S3 virtual host (s3.rdm.example.org and optional *.s3.rdm.example.org)
   server {
       listen 443 ssl http2;
       server_name s3.rdm.example.org *.s3.rdm.example.org;

       client_max_body_size 0;
       proxy_request_buffering off;
       proxy_buffering off;
       proxy_http_version 1.1;
       proxy_read_timeout 1800s;
       proxy_send_timeout 1800s;

       location / {
           proxy_pass http://127.0.0.1:1337;
           proxy_set_header Host $http_host;
           proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
           proxy_set_header X-Forwarded-Proto $scheme;
           proxy_set_header X-Forwarded-Host $http_host;
       }
   }
   ```

   - **Preserve `Host` verbatim (`$http_host`):** S3 Signature V4 signs the `Host` header, and Aruna
     uses `Host` together with `S3_HOST` to parse virtual-hosted bucket names.
   - **Disable buffering and body-size caps on S3:** prevents Nginx from spooling multi-gigabyte
     object uploads to temporary proxy disk or failing with `413 Request Entity Too Large`.
   - **REST API (`api.rdm.example.org` -> `127.0.0.1:3000`) and Portal (`rdm.example.org` -> `127.0.0.1:3003`):**
     set `proxy_http_version 1.1`, `Host`, `X-Forwarded-For`, `X-Forwarded-Proto` and
     `X-Forwarded-Host`, and allow streaming responses (`proxy_buffering off;` on `/mcp` and
     download/export routes).

## Persistent directories and storage layout

Understanding what lives on disk ensures complete backups and avoids accidental key loss.

| Host path (example) | Container path / variable | Contents | Persistent? | Included in `aruna-doctor backup`? |
| :--- | :--- | :--- | :--- | :--- |
| `/srv/aruna/data` | `/data` (`DATABASE_DIR`) | Primary Fjall database, **node and realm cryptographic keys**, user/group/bucket/object metadata, roles, tokens, sync metadata | **Critical** | **Yes** (`main/`) |
| `/srv/aruna/data/craqle` | `CRAQLE_STORAGE_PATH` | RO-Crate RDF metadata graph (`craqle/store`) and Tantivy search index | **Critical** | **Yes** (`craqle/store`; search index rebuilds) |
| `/srv/aruna/data/document-sync` | `DOCUMENT_SYNC_STORAGE_PATH` | Replicated realm, group and user documents (Irokle) | **Critical** | **Yes** (`document-sync/`) |
| `/srv/aruna/data/git` | `<DATABASE_DIR>/git` | Bare Git repositories for native Git / ARC datasets | **Critical** | **Yes** (`git`) |
| `/srv/aruna/blobs` | `/blobs` (`CONTENT_DIR`, default `<DATABASE_DIR>/blobstore`) | Stored object file bytes for the default local filesystem backend | **Critical** | **No (back up separately)** |
| `/srv/aruna/data/spool` | `SPOOL_PATH` | Temporary spool files during imports and large transfers | Transient | No |
| `/srv/aruna/data/backup.sock` | `<DATABASE_DIR>/backup.sock` | Live backup Unix socket created while running | Transient | No |
| `/srv/aruna/config` | `/run/.env`, `BLOB_BACKENDS_PATH` | Node environment file and optional `backends.toml` | **Critical** | No (back up with host config) |
| `/srv/aruna/tls` | `HTTPS_CERTIFICATE_PATH` / `_KEY_PATH` | TLS certificate chain and private key (Native HTTPS) | Config | No (managed by ACME / PKI) |
| `/srv/aruna/legal` | `PORTAL_LEGAL` files | Optional Markdown legal documents (Imprint, Privacy, Terms) | Config | No (back up with host config) |
| `/srv/aruna/compute-state` | `ARUNA_COMPUTE_STATE_ROOT` | Active compute workspaces and Apptainer SIF cache (when enabled) | Transient | No |

> [!CAUTION]
> The node identity key stored inside `DATABASE_DIR` protects secrets stored in the database (such
> as external storage credentials and access tokens) and authenticates this node to peer nodes.
> Treat `DATABASE_DIR` and `aruna-doctor` backup files as sensitive material with restricted
> filesystem permissions (`0700` / `0600`), and never restore a node without its database backup.

## Storage backends and quotas

Out of the box, Aruna stores object bytes in `CONTENT_DIR` (or `DATABASE_DIR/blobstore`). To add
external S3 storage (such as Ceph RGW, MinIO or institutional S3), tiered storage classes, per-backend
quotas, retention policies or local/NFS import roots, mount a TOML file and set
`BLOB_BACKENDS_PATH=/etc/aruna/backends.toml`.

### Example `backends.toml`

Every `[backend.<name>]` entry **must** set `multipart_bucket`, and **exactly one** backend must
set `default = true`. Keep S3 credentials out of TOML and supply them as environment variables
(`BLOB_BACKEND_<NAME>_ACCESS_KEY_ID` and `BLOB_BACKEND_<NAME>_SECRET_ACCESS_KEY`, where `<NAME>` is
uppercased with non-alphanumeric characters replaced by `_`).

```toml
# Default local filesystem backend with a 150 GiB quota
[backend.local]
type = "filesystem"
default = true
class = "standard"
root = "/blobs"
multipart_bucket = "uploaded-parts"
quota_bytes = 161061273600
cleanup = "reclaim"          # "reclaim" (default) or "retain"
reclaim_after_secs = 86400   # grace period in seconds before unreferenced bytes are deleted

# Optional external S3 backend (e.g. institutional Ceph RGW / MinIO)
# Provide BLOB_BACKEND_ARCHIVE_S3_ACCESS_KEY_ID and BLOB_BACKEND_ARCHIVE_S3_SECRET_ACCESS_KEY in env.
[backend.archive-s3]
type = "s3"
class = "archive"
allow_tenants = true
endpoint = "https://object.example.org"
region = "eu-central-1"
bucket_prefix = "aruna-archive-"
max_bucket_size = 100000
multipart_bucket = "aruna-archive-multipart"
force_path_style = true
cleanup = "retain"           # archive / WORM tiers should keep bytes

# Optional operator routing rules (first match wins)
[[routing]]
bucket = "long-term-archive"
target = { class = "archive" }

# Optional local or NFSv4 folders that listed groups may import in place
# [import_root.shared-lab]
# path = "/mnt/nfs/shared-lab"   # path inside the container
# groups = ["01JABCDEF0123456789ABCDEFG"]
# writable = false

# Optional node-level egress restrictions for tenant-configured external storage
[egress]
serve_group_backends = true
deny = ["169.254.0.0/16"]
```

Notes:

- See [Local and NFS folders](operations.md#local-and-nfs-folders) for NFSv4 mount options (`hard`,
  locking enabled, `sync` export) and [Your own storage](operations.md#your-own-storage) for
  group-managed S3 buckets.
- Configured `[compute.<name>]` tables in `backends.toml` are reserved for future use and rejected
  at startup; configure compute through `ARUNA_COMPUTE_EXECUTOR` environment variables instead.

## Identity provider (OIDC) setup

Aruna authenticates users via OpenID Connect (OIDC) and issues short-lived Aruna realm bearer
tokens (up to 24 hours) and API/S3 sessions. Aruna never stores an OIDC client secret; it validates
signed JWTs directly against your provider's JWKS.

### Recommended setup for initial installation

- **Recommended: dedicated Keycloak instance (standalone or Identity Broker).** Deploying Keycloak
  alongside Aruna (or reusing an existing institutional Keycloak) is the fastest, lowest-friction
  path for initial installation:
  1. You can start immediately with local test/admin accounts in a dedicated realm while verifying
     portal sign-in, S3, backups and workflows.
  2. You can later connect institutional SSO (DFN-AAI, EduGAIN, Shibboleth SAML, Life Science AAI,
     LDAP / Active Directory, GitHub or ORCID) as upstream Identity Providers inside Keycloak
     without changing Aruna's `OIDC_*` configuration or breaking existing user links.
- **Direct institutional OIDC provider:** you can also connect Aruna directly to any standards-compliant
  OIDC provider (institutional Keycloak, Authentik, Dex, Entra ID, Life Science AAI, etc.) that
  meets the JWT requirements below.

### OIDC client and token requirements

Configure a **public OIDC client** at your identity provider with these settings (see
[`scripts/keycloak/realm-import/aruna-realm.json`](../scripts/keycloak/realm-import/aruna-realm.json)
for a complete working reference):

- **Client ID:** e.g. `aruna-api` (must match `OIDC_<ID>_AUDIENCE`).
- **Client type:** Public (`client authentication = off`, no client secret).
- **Flow:** Authorization Code Flow (Standard Flow) with **PKCE (`S256`)** enabled.
- **Direct Access Grants (Resource Owner Password Credentials):** enable if you want to bootstrap
  the initial admin via `aruna-doctor create-token`. If your institutional IdP forbids password
  grants, see [Path B in initial admin bootstrap](#path-b-without-direct-access-grants).
- **Valid Redirect URIs:**
  - `https://<your-portal-host>/*` (for example `https://rdm.example.org/*`),
  - `aruna://auth/callback` (if users will connect [Aruna Desktop](https://github.com/arunaengine/aruna-desktop)).
- **Web Origins (CORS at IdP):** `https://<your-portal-host>` (plus `tauri://localhost` and
  `http://tauri.localhost` if using Aruna Desktop).
- **Scopes:** `openid`, `profile`, `email`.
- **Token claims validated by Aruna:**
  - Asymmetric signing algorithm (`RS256/384/512`, `PS256/384/512`, `ES256/384` or `EdDSA`) with a
    `kid` header present in the provider's JWKS (`jwks_uri` from discovery).
  - `iss` matching both `OIDC_<ID>_ISSUER` and the discovery document's `issuer` verbatim.
  - `aud` containing `OIDC_<ID>_AUDIENCE`. If `aud` is an array with more than one entry, `azp`
    must also equal `OIDC_<ID>_AUDIENCE`. (In Keycloak, verify that your client receives its own
    client ID in `aud` via an Audience protocol mapper if needed.)
  - Non-empty `sub`, valid `exp` and (if present) `nbf`.
  - Optional profile claims used for display and account matching: `name`, `preferred_username`,
    `given_name`, `family_name`, `email` + `email_verified` (`true`), and `auth_time`.

### Multiple providers and portal login chooser

You can configure more than one OIDC provider by listing comma-separated IDs in `OIDC_PROVIDER_IDS`:

```bash
OIDC_PROVIDER_IDS=main,campus
OIDC_MAIN_ISSUER=https://id.example.org/realms/aruna
OIDC_MAIN_AUDIENCE=aruna-api
OIDC_MAIN_DISCOVERY_URL=https://id.example.org/realms/aruna/.well-known/openid-configuration
OIDC_CAMPUS_ISSUER=https://sso.campus.example.org/oidc
OIDC_CAMPUS_AUDIENCE=aruna-portal
OIDC_CAMPUS_DISCOVERY_URL=https://sso.campus.example.org/oidc/.well-known/openid-configuration
```

To show custom sign-in buttons or pass Keycloak `kc_idp_hint` parameters to brokered upstream
identity providers, configure `PORTAL_LOGIN` as a single line of JSON (see `.env.example` for the
schema).

## First start and initial admin bootstrap

Start the node and verify that it initializes a new realm and reports ready:

```bash
cd /srv/aruna
docker compose up -d
docker compose logs -f aruna
curl -sf http://127.0.0.1:3002/readyz && echo "READY"
```

On the first start with an empty `DATABASE_DIR` (and `ONBOARDING_SECRET` unset), Aruna generates
its realm and node keys and creates a new realm.

Next, claim initial realm administrator privileges for your first admin user. Because the embedded
database allows only one writer at a time, stop the container briefly to mint the bootstrap secret:

```bash
cd /srv/aruna
docker compose stop aruna
SECRET=$(docker compose run --rm --no-deps aruna /run/aruna-doctor recover-admin | tail -n 1)
docker compose start aruna
```

### Path A: Using `aruna-doctor create-token` (IdP allows Direct Access Grants)

```bash
read -rsp "OIDC password: " ARUNA_OIDC_PASSWORD && echo
export ARUNA_OIDC_PASSWORD
docker exec -e ARUNA_OIDC_PASSWORD aruna /run/aruna-doctor create-token \
  --oidc-username "<admin-oidc-username>" \
  --bootstrap-secret "$SECRET"
unset ARUNA_OIDC_PASSWORD SECRET
```

The final line printed is an Aruna realm administrator bearer token. After signing in to the portal
with that same OIDC user, you have full realm admin access (and you may disable Direct Access
Grants on the OIDC client if you no longer need `aruna-doctor create-token`).

### Path B: Without Direct Access Grants (browser-only institutional SSO)

If your identity provider disables password grants:

1. Obtain an OIDC ID token (or JWT access token with matching `aud`) for the admin user through a
   standard Authorization Code + PKCE sign-in (or temporarily sign in via a brokered Keycloak local
   admin account).
2. Register the user with the bootstrap secret against the node's API:

   ```bash
   curl -sf -X POST https://api.rdm.example.org/api/v1/access/users/register \
     -H "Authorization: Bearer $OIDC_TOKEN" \
     -H "Content-Type: application/json" \
     -d "{\"onboarding_secret\": \"$SECRET\"}"
   ```

3. Sign in to the web portal (`https://rdm.example.org`) with that account, or exchange the OIDC
   token for an Aruna token via `GET https://api.rdm.example.org/api/v1/access/token`.

## Backups and disaster recovery

A complete disaster recovery plan backs up **three** things:

1. **The node database, metadata graph, sync state and Git repositories** (via `aruna-doctor backup`),
2. **The object bytes** (`CONTENT_DIR` and any external storage backends in `BLOB_BACKENDS_PATH`),
3. **Configuration files** (`/srv/aruna/config/aruna.env`, `backends.toml` and `legal/`).

### Automated live database backup (no downtime)

`aruna-doctor backup` connects to `<DATABASE_DIR>/backup.sock` on a running node, captures a
consistent single-instant snapshot across all Fjall stores plus Git repositories, verifies its
BLAKE3 checksum and writes it atomically to a new file (refusing to overwrite existing files):

```bash
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
docker exec aruna /run/aruna-doctor backup /data "/backups/aruna-${STAMP}.bak"
```

Schedule this daily (or more frequently) via a systemd timer or cron job, immediately followed by
an incremental backup of `/srv/aruna/blobs` (for example with `restic`, `borg` or `rsync` to
off-host storage) and your `/srv/aruna/config` files. Example backup script
`/usr/local/sbin/aruna-backup.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
BACKUP_FILE="/srv/aruna/backups/aruna-${STAMP}.bak"

# 1. Live consistent database + metadata + Git snapshot
docker exec aruna /run/aruna-doctor backup /data "/backups/aruna-${STAMP}.bak"
chmod 600 "$BACKUP_FILE"

# 2. Ship DB snapshot, blob directory and config to encrypted off-host storage
# Replace with your institution's backup tool (restic, borg, rsync, etc.):
# restic backup "$BACKUP_FILE" /srv/aruna/blobs /srv/aruna/config

# 3. Retain the last 14 local snapshots
find /srv/aruna/backups -maxdepth 1 -name 'aruna-*.bak' -mtime +14 -delete
```

### Cold full backup (before upgrades or hardware moves)

When downtime is acceptable, stop the node and copy both `/srv/aruna/data` and `/srv/aruna/blobs`
together for an exact point-in-time copy of everything on disk:

```bash
cd /srv/aruna
docker compose stop aruna
rsync -aHAX --delete /srv/aruna/data /srv/aruna/blobs /srv/aruna/config /mnt/offhost-backup/aruna/
docker compose start aruna
```

### Restoring from a backup

1. Restore object files to `/srv/aruna/blobs` (or verify external S3 backends are reachable).
2. Restore the database snapshot into a **new, non-existent** directory (for example
   `/srv/aruna/data-restored`):

   ```bash
   docker run --rm \
     -v /srv/aruna:/srv/aruna \
     ghcr.io/arunaengine/aruna:v3.0.0-alpha.210 \
     /run/aruna-doctor restore /srv/aruna/backups/aruna-<timestamp>.bak /srv/aruna/data-restored
   ```

3. Swap the restored folder into `/srv/aruna/data` (keeping the old directory until verification
   succeeds) and start the node (`docker compose up -d`).
4. On startup, Aruna automatically queues a storage integrity check across every bucket. Inspect
   `GET /api/v1/data/buckets/{bucket}/storage/check` to confirm no object versions are missing their
   stored blobs.

Test a restore onto a staging host or temporary directory periodically to verify your backup chain.

## Upgrades and migrations

Always follow this sequence when moving from one pinned tag to another:

1. **Read release notes** for any configuration or breaking changes between the old and new tags.
2. **Take a fresh backup** before stopping the container:

   ```bash
   docker exec aruna /run/aruna-doctor backup /data "/backups/pre-upgrade-$(date -u +%Y%m%dT%H%M%SZ).bak"
   ```

3. **Pull the new pinned image** and update the tag in `/srv/aruna/compose.yaml`:

   ```bash
   NEW_IMAGE=ghcr.io/arunaengine/aruna:<new-tag>
   docker pull "$NEW_IMAGE"
   ```

4. **Stop the running node and run `aruna-doctor migrate`** with the new image (repeating a
   migration is safe and idempotent):

   ```bash
   cd /srv/aruna
   docker compose stop aruna
   docker run --rm \
     -v /srv/aruna/data:/data \
     -v /srv/aruna/config/aruna.env:/run/.env:ro \
     "$NEW_IMAGE" /run/aruna-doctor migrate /data
   ```

5. **Start the new version** and verify health:

   ```bash
   docker compose up -d
   curl -sf http://127.0.0.1:3002/readyz && echo "READY"
   docker exec aruna /run/aruna-doctor info
   ```

6. Keep the pre-upgrade backup until the new version has run cleanly in operation.

## Observability, compute and multi-node realms

### Health checks, metrics and logs

- **Health endpoints (port `3002`, loopback only):**
  - `GET /healthz` (or `/health`): liveness probe,
  - `GET /readyz` (or `/ready`): readiness probe (returns `200` once storage, network and listeners
    are ready),
  - `GET /metrics`: Prometheus scrape endpoint.
- **Prometheus and alerts:** ready-to-adapt scrape and alert rule examples live in
  [`scripts/observability/prometheus.yml`](../scripts/observability/prometheus.yml) and
  [`scripts/observability/alerts.yml`](../scripts/observability/alerts.yml).
- **Structured logging and tracing:** set `LOG_FORMAT=json` for log shippers (Loki, OpenSearch,
  systemd journal), tune verbosity with `RUST_LOG` (default `info`), and optionally export traces
  with `OTEL_EXPORTER_OTLP_ENDPOINT`.
- **Node diagnostics:** run `/run/aruna-doctor info` (pass `--token` or `ARUNA_TOKEN` for topology
  and backend details) while running, or `/run/aruna-doctor report --database-path /data` while
  stopped.

### Compute executors

Leave `ARUNA_COMPUTE_EXECUTOR=none` during initial installation. When you are ready to run GA4GH
TES jobs or Jupyter notebooks on the node, Aruna supports `docker`, `apptainer`, `kubernetes` and
`slurm` (the `slurm` executor requires a build with the `slurm` feature). For `docker`, mount the
host Docker socket, verify `ARUNA_COMPUTE_DOCKER_SESSION_SUBNET` (default `172.30.255.0/24`) does
not overlap with host or campus networks, and set `ARUNA_COMPUTE_S3_URL` if job containers reach S3
at a different address than `S3_PUBLIC_URL`.

### Growing to a multi-node realm

Additional infrastructure nodes join an existing realm using a one-time onboarding secret minted by
a realm administrator in the portal (or API):

1. Prepare the new node with an **empty** `DATABASE_DIR` and set `ONBOARDING_SECRET=<secret>` in
   its `aruna.env` **before its first start**.
2. Optionally set `ARUNA_NODE_LOCATION`, `ARUNA_NODE_WEIGHT` and `ARUNA_NODE_LABELS` (`k=v,...`)
   before the first start to populate its placement attributes.
3. Start the node. Once it finishes syncing and joins the realm, remove `ONBOARDING_SECRET` from
   `aruna.env` (it is only read on first start).

## Operator pre-flight checklist

Before opening a new deployment to users, verify each item:

- [ ] Pinned image tag set in `compose.yaml` (not floating `v3`).
- [ ] Host clock synchronized with NTP.
- [ ] `ARUNA_FJALL_PERSIST_MODE=sync_all` and an appropriate `ARUNA_FJALL_CACHE_MIB` configured.
- [ ] DNS records and valid TLS certificates in place for Portal, API and S3 (plus `*.s3.*` if
      using virtual-hosted S3), with certificate renewal reloading via `SIGHUP` (or reverse proxy).
- [ ] Firewall allows inbound `443/tcp` (and optional `80/tcp`) plus `3001/udp`; port `3002/tcp`
      is bound to loopback only and unreachable from outside.
- [ ] OIDC provider configured with public client, PKCE `S256`, matching `aud`, and portal (plus
      optional `aruna://auth/callback`) redirect URIs.
- [ ] `FEDERATION_REGISTRY_URL` explicitly set to empty if the deployment is private/pre-production.
- [ ] Initial admin claimed and portal sign-in + S3 upload/download verified.
- [ ] Scheduled `aruna-doctor backup` plus blob and config backups running, and one test restore
      verified.
- [ ] Optional smoke test of the image itself: `bash scripts/alpha_check.sh --image "$IMAGE"`.
