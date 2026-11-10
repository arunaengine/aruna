<!-- Operations guide: configuration, backups, storage, encryption, registry and node trust. -->
<!-- Copyright (c) 2026 The Aruna Contributors -->
<!-- SPDX-License-Identifier: MIT or Apache-2.0 -->

# Operations guide

This guide is for people who run Aruna nodes and for group admins who manage storage. Start with
the [README](../README.md) to get a node running.

## Configuration

- **Environment variables.** The node reads its settings from environment variables. It also loads
  a `.env` file from its working directory, which is `/run/.env` in the image. Values set in the
  process environment win over the file. [`.env.example`](../.env.example) lists every variable
  with its default.
- **Ports.** `3000` serves REST and the Swagger UI, `1337` S3, `3001` UDP for connections between
  nodes, `3002` health and metrics, and `3003` the portal. Inside a container, bind `0.0.0.0` for
  every port you publish. Port `3002` has no authentication, so publish it on loopback only. With
  [native HTTPS](#native-https), one more port serves all three over TLS.
- **Data folder.** Everything below `STORAGE_PATH` is node state: the database, metadata, document
  sync, Git repositories, the node identity and, by default, the stored files (`blobstore`). One
  volume at `STORAGE_PATH` holds all of it unless you point `BLOB_ROOT` or `BLOB_BACKENDS_PATH`
  elsewhere.
- **Keys.** The node creates its realm and node keys on the first start and keeps them in the data
  folder. There is nothing to generate. The tracked `.env` in this repository holds published demo
  keys, and a node refuses to start while they are set.
- **Public addresses.** Set `API_PUBLIC_URL`, `S3_PUBLIC_URL` and, with a portal,
  `PORTAL_PUBLIC_URL` to the addresses clients use. Set `TRUSTED_PROXIES` when a reverse proxy sets
  `x-forwarded-*` headers.
- **Sign-in.** The portal needs at least one OIDC provider: `OIDC_PROVIDER_IDS`, then
  `OIDC_<ID>_ISSUER`, `OIDC_<ID>_AUDIENCE` and `OIDC_<ID>_DISCOVERY_URL` for each id.
- **Portal.** The image contains the portal only when the built
  [website](https://github.com/arunaengine/website) is staged in `.portal-embed` before the build.
  A node can also download a portal release archive with `PORTAL_MODE=artifact` and
  `PORTAL_ARTIFACT_URL`. An enabled portal needs `PORTAL_SOCKET_ADDRESS`, `API_PUBLIC_URL` and the
  portal origin in `CORS_ALLOWED_ORIGINS`.
- **Portal landing page.** `PORTAL_LANDING=disabled` opens the app at `/` instead of the landing
  page. The default is `enabled`.
- **Legal pages.** `PORTAL_LEGAL` lists the imprint, privacy and terms pages of the deployment as
  one JSON list, in display order. Each entry has a `label` and exactly one of `url` (an `https://`
  page) or `file` (a UTF-8 Markdown file on the node). The node reads each file at startup and
  sends its text with every portal start, so keep files to a few KiB and restart the node after a
  change. Without `PORTAL_LEGAL` the portal shows no legal links. Invalid JSON, an empty label, a
  missing, unreadable or non-UTF-8 file, a plain `http` URL or an entry with both or neither source
  stops the start with an error that names `PORTAL_LEGAL`. The node reads `PORTAL_LANDING` and
  `PORTAL_LEGAL` only with `PORTAL_MODE=artifact` and ignores them otherwise.

  Link to the legal pages your institution already publishes:

  ```
  PORTAL_LEGAL='[{"label":"Imprint","url":"https://example.org/imprint"},
    {"label":"Privacy","url":"https://example.org/privacy"}]'
  ```

  Or provide your own texts, shown inside the portal:

  ```
  PORTAL_LEGAL='[{"label":"Imprint","file":"/srv/aruna/legal/imprint.md"},
    {"label":"Privacy","file":"/srv/aruna/legal/privacy.md"},
    {"label":"Terms of service","file":"/srv/aruna/legal/terms.md"}]'
  ```

  Both kinds can also be mixed in one list.
- **Compute.** `ARUNA_COMPUTE_EXECUTOR` is `none` by default. The `docker` executor needs the host's
  Docker socket and an S3 address that job containers can reach.

## Native HTTPS

A node can serve HTTPS itself, so a Docker or Compose installation needs no separate proxy. Plain
HTTP behind an existing ingress stays the default.

- **Turn it on.** Set `HTTPS_SOCKET_ADDRESS`, `HTTPS_CERTIFICATE_PATH` and `HTTPS_KEY_PATH`. The
  certificate file holds the leaf first, then its intermediates. The key file holds one private
  key. Without `HTTPS_SOCKET_ADDRESS`, every other HTTPS setting must stay unset.
- **Hostnames.** One HTTPS port serves REST and MCP on the host of `API_PUBLIC_URL`, S3 on the host
  of `S3_PUBLIC_URL` and its subdomains, and the portal on the host of `PORTAL_PUBLIC_URL`. These
  URLs must be `https` origins with a domain name and no path, query or user; a port is allowed.
  `S3_HOST` must name the same host as `S3_PUBLIC_URL`. With a portal, `CORS_ALLOWED_ORIGINS` must
  allow the portal origin.
- **Domain names only.** Clients must connect by name. A client that connects by IP address sends
  no server name and is refused. Health checks keep using the private port `3002`.
- **Certificate checks.** The node checks the pair at start and refuses to start on any problem:
  the key must fit, the chain must be complete and valid now, and the certificate must cover every
  hostname. Chains are trusted up to the public root certificates built into Aruna. For a private
  CA or a self-signed certificate, list it in `HTTPS_CA_PATH`.
- **Renewal.** The node checks the files once per hour and at once on `SIGHUP`, so send `SIGHUP`
  from your renewal hook (for example certbot's `--deploy-hook`). A new pair replaces the
  active one only when it passes every check; otherwise the active pair stays and the node logs
  the reason. Replace each file atomically: write it under a temporary name in the same folder and
  rename it, as certbot does. Mount the folder, not single files, so renames are seen.
- **Redirect.** `REDIRECT_SOCKET_ADDRESS` adds a plain listener that answers `308` with the same
  path and query on the configured HTTPS origin. Unknown hosts get `400`. Configure S3 clients with
  the HTTPS endpoint; do not rely on the redirect.
- **S3 buckets as hostnames.** Requests like `bucket.s3.example.org` need a wildcard certificate for
  `*.s3.example.org`. It covers bucket names with one label only; buckets with dots in their names
  need path-style requests.
- **Sign-in.** Add the HTTPS portal origin to the redirect URIs and web origins of the OIDC client
  at your identity provider.
- **Other ports.** `SOCKET_ADDRESS`, `S3_ADDRESS` and `PORTAL_SOCKET_ADDRESS` keep serving plain
  HTTP. Do not publish them on the host; job containers still reach S3 on the container network.

The container can listen on unprivileged ports and Docker maps host ports `80` and `443` to them:

```yaml
services:
  aruna:
    image: harbor.computational.bio.uni-giessen.de/aruna/aruna:<tag>
    ports:
      - "443:8443"
      - "80:8080"
      - "3001:3001/udp"
    volumes:
      - aruna-data:/data
      - ./tls:/tls:ro
    environment:
      HTTPS_SOCKET_ADDRESS: 0.0.0.0:8443
      HTTPS_CERTIFICATE_PATH: /tls/fullchain.pem
      HTTPS_KEY_PATH: /tls/privkey.pem
      REDIRECT_SOCKET_ADDRESS: 0.0.0.0:8080
      API_PUBLIC_URL: https://api.example.org
      S3_PUBLIC_URL: https://s3.example.org
      S3_HOST: s3.example.org
      PORTAL_PUBLIC_URL: https://portal.example.org
      CORS_ALLOWED_ORIGINS: https://portal.example.org
      SOCKET_ADDRESS: 0.0.0.0:3000
      S3_ADDRESS: 0.0.0.0:1337
      PORTAL_SOCKET_ADDRESS: 127.0.0.1:3003
      P2P_SOCKET_ADDRESS: 0.0.0.0:3001
```

## Settings to check

- **The first start decides the realm.** Without `ONBOARDING_SECRET`, a node with an empty data
  folder creates a new realm. With it, the node joins an existing realm. Changing the setting later
  does not move the node. To start over, use a new, empty data folder.
- **How changes are saved.** By default Aruna favors speed, and the last few changes can be lost on
  power loss. Set `ARUNA_FJALL_PERSIST_MODE=sync_all` if that is not acceptable.
- **Database cache.** `ARUNA_FJALL_CACHE_MIB` sets the block cache of the node database (default
  256). Nodes with much memory and slow disks read less from disk with a larger cache.
- **Free disk space.** Importing a large dataset briefly needs about twice its size.
- **Notebook networks.** Notebook sessions run in their own network so they can only reach your
  data. Make sure `ARUNA_COMPUTE_DOCKER_SESSION_SUBNET` does not overlap with other networks on
  the host.
- **Git.** Log in to the Git repository of a dataset with an Aruna access token as the password.
  Pull before you push, and store large files with Git LFS. See the
  [native Git guide](../scripts/arc-native/README.md).

## Backups, restore and upgrades

- **Live backup.** `aruna-doctor backup <data folder> <target file>` writes a consistent copy of the
  running node's database. It works on Unix only and refuses to overwrite an existing file. The
  stored files (`BLOB_ROOT` and other storage backends) are not part of it. Copy them separately.
- **Full copy.** To back up everything at once, stop the node and copy the whole data folder. The
  folder holds the node identity, which protects stored secrets such as access tokens. A node
  restored without it cannot read these secrets.
- **Restore.** `aruna-doctor restore <backup file> <new folder>` builds a new data folder from a
  backup. The new folder must not exist yet. Start the node with `STORAGE_PATH` set to it. The
  node then checks every bucket, and `GET /data/buckets/{bucket}/storage/check` lists versions
  whose stored file is missing.
- **Upgrade.** Stop the node, run `aruna-doctor migrate <data folder>` with the new version, then
  start the new version. Running the migration twice does no harm. Keep the backup until the new
  version has run well.

## Encrypted buckets

A group admin can turn on encryption for a bucket, in the portal or with
`PUT /data/buckets/{bucket}/storage/encryption`. The node then stores new files of that bucket
encrypted.

- **Two modes.** `node_managed` is the simple choice: the node keeps the bucket key and opens it
  after a restart, so everything works without unlocking. Use `vault_locked` when the node should
  hold no key: a key holder must unlock the bucket in the portal, and the key stays only in the
  node's memory until it is locked again, a time limit ends, or the node restarts.
- **Key holders.** The bucket creator, the group admins and users granted explicitly hold a sealed
  copy of the bucket key. Keep at least two holders, or one with a recovery key, so the data is not
  lost with one person's key.
- **While locked.** Uploads, listings and object info still work. People with a key grant for a
  folder or file can read it in the portal while the bucket stays locked. Plain S3 tools, jobs,
  copies to another bucket and moves to other storage need an unlock; S3 answers
  `403 AccessDenied` with the header `x-aruna-bucket-locked: true`.
- **Token credentials.** A key holder can create an S3 credential that reads chosen buckets while
  they are locked: list them in `encrypted_buckets` of `POST /access/credentials`. Each bucket
  must be unlocked at that moment. The answer contains a `session_token` once; the node does not
  keep it. Set it as `aws_session_token` next to the access key and secret, for example in
  `~/.aws/credentials`. The token only works in the signed request header, never in presigned
  URLs. It stops working when the credential is revoked, when its creator stops being a key
  holder, or after a key rotation. `GET /data/buckets/{bucket}/storage/encryption/tokens` lists
  the token credentials of a bucket.
- **S3 clients.** Encrypted buckets report `AES256` server-side encryption. `PutBucketEncryption`
  with `AES256` turns on `node_managed` mode; turning encryption off is only possible through the
  REST API. Other encryption headers, such as KMS or customer keys, are refused.
- **What is not covered.** Turning on encryption does not remove plaintext that existed before:
  backups, old disk blocks and copies on other systems stay as they were. Deleting a file is not
  secure erasure. Job and notebook working directories are removed when the job or session ends,
  but logs, reports and files written by your own code are kept as they are; only files written
  to an encrypted bucket are encrypted.
- **Content fingerprints.** For deduplication, the node keeps content hashes of encrypted
  objects. These hashes are extra plaintext fingerprints: they can show that two files are equal.
- **Copies and sync to other nodes.** A target bucket that encrypts stores every copy sealed to
  its own key. When both buckets encrypt, the source node grants each encrypted file to the key of
  the target bucket, so the target never needs the source key. While the source bucket is locked,
  its copies wait and run after the next unlock; the sync status shows how many wait. The target
  checks the content of such a copy when its own bucket is next unlocked. A target bucket without
  encryption gets copies of an encrypted bucket only when the request sets `plaintext: true` and
  the requester holds the key of the source bucket.

## Your own storage

A group can keep its files in its own S3 storage instead of Aruna's storage on the node. Add it in
the group's Storage tab with the endpoint and keys; it needs no bucket. Then add rules:

- **Default rule.** New uploads of all the group's buckets go into one bucket of your storage, in
  Aruna's own layout. Aruna finds or creates the bucket when you save the rule and never reads it
  back. A group has at most one default rule. "Use as default" on Aruna storage sends new uploads
  to the node again; files already stored stay where they are until you move them.
- **Linked buckets.** A bucket (or folder) in Aruna and a bucket (or folder) in your storage show
  the same files both ways: a run imports files found there, and new uploads are stored there
  under their own names. Aruna never deletes them.
- **No overlaps.** Two linked rules may not share folders of one bucket, and no linked rule may use
  the default rule's bucket. Paths in your storage are at most 1024 bytes long.
- **Move files.** Moving a bucket copies its stored files to where the rules send new uploads
  now, then deletes the old copies. A new linked rule can move the folder's files at once.
  Encrypted buckets cannot move yet.
- **Broken files.** A check finds files whose stored copy is gone, for example after a storage
  was removed, and lists them; a cleanup deletes them.

`PUT /data/groups/{group_id}/storage/routing` sets the default rule; linked rules live under
`/data/groups/{group_id}/storage/imports`; `/data/buckets/{bucket}/storage/move` counts and moves
stored files, and `/data/buckets/{bucket}/storage/check` finds and deletes broken files.

## Referenced files

An import keeps a file either as a **snapshot** (Aruna copies the bytes once) or as a
**reference** (Aruna records where the file is and reads it from there). A reference version
stores what the source reported when it was recorded: size, ETag, time and checksums.

- **HEAD** of a reference to external storage answers from this record and does not ask the
  source. A reference to a file on another Aruna node asks that node, so a lost permission or an
  unreachable node fails the HEAD like a GET.
- **Changed source.** A read of the current version that finds a changed file records a new
  version for it and serves that one, at most 100 times per reference (then
  `409 ReferenceAdvanceExhausted`; write the reference again). A file that no longer matches its
  recorded checksums answers `412 PreconditionFailed`. A read of an older version
  (`versionId`) whose file changed answers `404 NoSuchVersion`: its bytes were never kept.
- **Failures are explicit.** A missing file answers `404 NoSuchKey`, a refusal `403 AccessDenied`,
  and an unreachable, failing or changing source `503 ServiceUnavailable`, which a retry may
  clear. With several sources, `404` means that every source lacks the file.
- **Ranges** are read from the source with the same checks. A source whose bytes are checked by
  checksum is read whole first, then the range is cut from it.
- **Snapshots** never read their source again, so a changed or lost source does not affect them.

## Local and NFS folders

A node operator can let groups import a folder of the node in place, for example an NFS share.
Approve the folder in the backends file (`BLOB_BACKENDS_PATH`, which also needs a default
`[backend.<name>]`):

```toml
[import_root.projects]
path = "/mnt/nfs/projects"      # absolute path on this node
groups = ["01JABCDEF0123456789ABCDEFG"]
writable = false                # the default; true lets Aruna write new files here
```

- **Containers.** When the node runs in a container, mount the folder into it first. `path` is the
  path inside the container.
- **Groups.** Only the listed groups see the root. A group lists its roots with
  `GET /data/groups/{group_id}/storage/import-roots`, which never shows paths, and adds a storage
  of kind `fs` with `root` and an optional `folder` below it, like `raw/2026/`.
- **Imports only.** An `fs` storage is the source of linked rules. It cannot be the default rule or
  a bucket routing target. A read-only root needs another, writable storage for new uploads.
- **Safety.** Aruna never follows a symlink below the root and skips special files. A missing
  folder only logs a warning at start, so a late mount does not stop the node.
  `[egress] serve_group_backends = false` withdraws these storages too.
- **NFS.** Use NFSv4 with a `hard` mount, never `soft`, and keep locking on (no `nolock`). The
  server should export the share with `sync`. Aruna writes new files to a temporary name and links
  or renames them into place, which needs hard links on the share. Reading a large share hashes
  every new or changed file once, so schedule runs at quiet times.

## Realm registry

By default, a realm lists itself in the global registry at `https://registry.aruna-engine.org`,
so the portals of other realms can find it.

- **What it sends.** A registration signed with the realm key, renewed every 6 hours. It holds
  the realm's public facts (realm id, name, description, public API and portal URL) and three
  counts: live datasets, groups and configured nodes. It holds no users, data or metadata.
- **How it starts.** When the realm has no federation settings, one management node creates them
  on start: the name from `REALM_DESCRIPTION`, the URLs from `API_PUBLIC_URL` and
  `PORTAL_PUBLIC_URL`, the default registry and registration turned on. Existing settings are
  never changed.
- **Only public realms.** No settings are created when one of the two URLs is missing or points
  to a loopback or private address. The local `just` recipes never register.
- **Opting out.** Before the first start, set `FEDERATION_REGISTRY_URL` to another registry, or to
  an empty value to turn the default off. Once the settings exist, a realm admin changes them with
  `PUT /api/v1/system/realm/federation`. Turning registration off sends up to three withdrawals;
  removing the registry sends nothing more, and the entry expires after 72 hours.

## Trust between nodes

Nodes of a realm read, copy and forward files for their users. The node that receives such a
request from another node decides what it may do as follows.

- **Infrastructure nodes speak for users.** A server or management node configured in the realm
  may say which user a copy, a read or a forwarded request is for, with that user's restrictions.
  The receiving node checks this user's permissions and restrictions itself, so the request can
  do no more than the user could. A taken-over infrastructure node can therefore act as any user
  of its realm: protect node keys like realm administrator credentials.
- **Devices speak only for their owner.** A user node can say a request is for a user only when
  that user is the owner the realm configuration names. A device never sends copies to other
  nodes and is never told where copies are stored.
- **Bearer tokens speak for their user.** Any configured node of the realm, a user node too, may
  pass on a user's valid bearer token, for example with a search or a graph query. The receiving
  node validates the token itself and serves the token's user, whoever owns the sending node.
- **Copies keep their source.** A node accepts an update of a reference only from the node that
  sent the reference, and only for the same source.
- **Unknown nodes and other realms are refused.** A request from a node that is not configured in
  the realm, or that names another realm to serve it, is refused before any data is read. A user
  of another realm is served as that user, with the roles this realm granted.
