# Greenfield cutover runbook

This runbook covers one procedure: **replacing a running realm with a new one on
the current format**. It destroys every byte of realm state on every node. It is
not a rollout, not an upgrade, and not reversible.

Aruna ships one storage format and one realm wire format at a time. There is no
migration, no legacy decoder, no fallback ALPN, and no capability-gated dual
path. A node of another format is refused before it decodes anything, and a
database root of another format fails startup before it decodes any record. That
is the whole compatibility story, so the only way forward is a full wipe.

> **Approval.** This procedure needs the operator's explicit approval for the
> exact cluster and the exact resolved paths listed in section 3. Reading this
> document is not approval, and neither is a general greenfield policy.

## 1. What the fences do

Two independent identities are enforced.

| Fence | Constant | Where it is checked | Failure |
| --- | --- | --- | --- |
| Storage format | `aruna-storage` epoch `1` (`core/src/storage_format.rs`) | Every Aruna-owned fjall root at open (`storage/src/format.rs`) | Startup fails before any record decode |
| Realm wire format | `aruna.document.v3` epoch `3` (`core/src/realm_format.rs`) | Onboarding sync ticket, inbound document-sync stream | Enrollment refused; sync stream refused before any document applies |
| Stream protocols | `aruna/bao/2`, `aruna/metadata/2`, `aruna/job-control/2` (`core/src/alpn.rs`) | ALPN negotiation on dial and accept | Connection refused; no frame is decoded |

Storage-root behaviour in detail:

- a **fresh root** (no marker, no rows) is stamped with the current marker and
  admitted;
- a **matching root** is admitted unchanged;
- a **populated root with no marker** is refused as `Unmarked`;
- a root with **another id or epoch** is refused as `Mismatch`;
- an **undecodable marker** is refused as `Undecodable`.

The two epochs move independently on purpose: a stored-record change need not
condemn a running realm's wire, and a replicated-document change need not condemn
an untouched disk root. Bump each when its own bytes change.

`aruna-doctor` reads any root's marker without repairing it:

```sh
aruna-doctor explore keyspaces "$STORAGE_PATH"
```

`aruna-doctor import` is read-only in the same sense: it prints an explicit
warning naming the epoch when the imported rows carry a foreign or absent marker,
and never writes a marker of its own.

### The gap this runbook exists to close

The craqle metadata root (`CRAQLE_STORAGE_PATH`) carries **no marker of its
own**. Only the main database root and the document-sync root are stamped and
checked at startup. Blob roots, workspace roots, and executor state carry no
marker either. A database-only wipe therefore leaves live old bytes behind, and
an old executor, mounted workspace, outbox, or peer can still write into the new
realm. **Wiping those roots explicitly is mandatory, not optional.**

## 2. Order of operations

Do not reorder. Every step exists because a later step is unsafe without it.

1. Quiesce admission.
2. Stop every old Aruna process.
3. Stop and delete every old external workload.
4. Inventory the resolved paths on every node.
5. Wipe or replace every inventoried root.
6. Provision new empty namespaces.
7. Bootstrap a new realm and a new peer set.
8. Deploy one homogeneous build everywhere.
9. Verify.

## 3. Inventory

Resolve these per node **before** wiping anything, and record the resolved
absolute paths. Defaults derive from `STORAGE_PATH`, so an unset variable does
not mean "absent".

| State | Variable | Default | Marker | Action |
| --- | --- | --- | --- | --- |
| Main database (auth, users, buckets, blobs index, jobs, policies, tasks, outboxes, caches, node identity) | `STORAGE_PATH` | required | stamped and checked | wipe |
| Document-sync root (irokle topics, oplog, cursors) | `DOCUMENT_SYNC_STORAGE_PATH` | `$STORAGE_PATH/document-sync` | stamped and checked | wipe |
| Craqle metadata root (terms, quads, graphs, log) | `CRAQLE_STORAGE_PATH` | `$STORAGE_PATH/craqle` | **none** | wipe explicitly |
| Craqle search index | `CRAQLE_SEARCH_STORAGE` | `disk` (under the craqle root) | none | wiped with the craqle root |
| Blob root (filesystem backend) | `BLOB_ROOT` | `$STORAGE_PATH/blobstore` | none | wipe or replace with a new namespace |
| Extra blob backends | `BLOB_BACKENDS_PATH` | none | none | purge or repoint every declared bucket/prefix |
| Docker executor state and workspaces | fixed | `./compute-state` (process working directory) | none | wipe |
| Apptainer instance state | `ARUNA_COMPUTE_APPTAINER_STATE_ROOT` | `./compute-state/apptainer` | none | wipe |
| Apptainer image cache | `ARUNA_COMPUTE_APPTAINER_SIF_CACHE` | `./compute-state/sif` | none | wipe |
| Apptainer cgroup root | `ARUNA_COMPUTE_APPTAINER_CGROUP_ROOT` | required | none | remove leftover child cgroups |
| Kubernetes workloads | `ARUNA_COMPUTE_K8S_NAMESPACE` | `default` | none | delete Jobs, Pods, PVCs, marker and credential objects |
| Node identity, realm binding, onboarding phase | `NODE_STATE_KEYSPACE` inside the main database | — | with the main root | wiped with the main root |
| Enrollment secret | `ONBOARDING_SECRET` | none | — | reissue for the new realm |

The main database also holds the durable task timers, the document-sync and
notification outboxes, the placement-policy cache, the job family store, and the
compute reservation and departure rows. They are not separate roots; wiping
`STORAGE_PATH` removes all of them.

## 4. Quiesce and stop

Stop new work before touching state, so nothing writes into a half-wiped node.

1. Remove the public REST and S3 Services from the load balancer, or scale the
   ingress to zero. Submission, uploads, and replication all enter here.
2. Wait for in-flight uploads and executions to finish or fail. There is no
   drain endpoint that guarantees this; treat everything still running as lost.
3. Stop every Aruna process on every node. A single surviving process re-seeds
   peers, republishes documents, and re-registers copies.

```sh
kubectl -n "$ARUNA_NAMESPACE" scale deployment/<aruna-deployment> --replicas=0
kubectl -n "$ARUNA_NAMESPACE" rollout status deployment/<aruna-deployment>
```

4. Stop and delete every external workload that could still write outputs.

Kubernetes executor:

```sh
kubectl -n "$ARUNA_COMPUTE_K8S_NAMESPACE" delete jobs,pods,pvc \
  -l aruna-engine.org/job-id --wait=true
kubectl -n "$ARUNA_COMPUTE_K8S_NAMESPACE" get jobs,pods,pvc \
  -l aruna-engine.org/job-id
```

The second command must print nothing. Every object the Kubernetes backend
creates (task pods, workspace PVCs, S3-mount objects, markers, credentials)
carries `aruna-engine.org/job-id`.

Docker executor:

```sh
docker ps -aq --filter 'label=aruna-engine.org/job-id' | xargs -r docker rm -f
docker volume ls -q --filter 'label=aruna-engine.org/job-id' | xargs -r docker volume rm
```

Apptainer executor: stop every instance, then remove the state root and the
image cache listed in section 3, plus any child cgroup left under
`ARUNA_COMPUTE_APPTAINER_CGROUP_ROOT`.

5. Unmount every workspace mount and every S3 mount on every node. A mount that
   survives the wipe writes into a path the new realm believes is empty.

## 5. Wipe

Only after section 4 is complete on **every** node.

```sh
# Per node, with the resolved paths recorded in section 3.
rm -rf "$STORAGE_PATH"
rm -rf "$DOCUMENT_SYNC_STORAGE_PATH"   # if outside STORAGE_PATH
rm -rf "$CRAQLE_STORAGE_PATH"          # mandatory: this root carries no marker
rm -rf "$BLOB_ROOT"                    # if outside STORAGE_PATH
rm -rf ./compute-state                 # or the resolved executor state roots
```

For persistent volumes, delete the PVC rather than emptying it, so the new
cluster binds a genuinely new volume:

```sh
kubectl -n "$ARUNA_NAMESPACE" delete pvc -l app=<aruna-app>
```

For object-storage blob backends, either point `BLOB_ROOT` and every declared
backend bucket/prefix at a **new empty namespace**, or purge the old one. Do not
reuse a populated namespace.

**Retention.** An old root may be kept offline for operator recovery: detached,
unmounted, and outside every configured path. It is never mounted, adopted,
scanned, or backfilled by the new cluster. There is no supported way to read data
out of it with the new build.

## 6. Provision

1. Generate a new realm. Do not reuse the previous `RealmId`; a retained node key
   plus a reused realm id is exactly the case the format fences cannot see.
2. Issue a fresh enrollment secret (`ONBOARDING_SECRET`) for the new realm and
   distribute it to the joining nodes.
3. Rebuild the bootstrap/peer set from the new realm only. Remove every stale
   peer address and every stale node key from configuration and secrets.
4. Deploy the same build to every node and every executor adapter. A mixed-build
   realm cannot form: the other build fails ALPN negotiation and its enrollment
   ticket is refused on the epoch.
5. Start the bootstrap node first, let it publish the core documents, then start
   the joiners.

## 7. Verify

- Every node reports `Ready` on `GET /readyz` (ops listener,
  `OPS_SOCKET_ADDRESS`).
- Each root reports the current epoch:

  ```sh
  aruna-doctor explore keyspaces "$STORAGE_PATH"
  ```

- No old peer negotiates: an old build's dial is refused at ALPN, and an old
  build's document-sync stream is refused with a realm-format error before any
  document is applied. Both appear in the log; neither reaches the reducer.
- No external workload from the old realm exists:

  ```sh
  kubectl -n "$ARUNA_COMPUTE_K8S_NAMESPACE" get jobs,pods,pvc -l aruna-engine.org/job-id
  docker ps -aq --filter 'label=aruna-engine.org/job-id'
  ```

  Both must be empty until the new realm schedules its own work.
- A submission, an upload, and a download succeed end to end on the new realm.

## 8. Downgrade and rollback

Unsupported. The wiped namespaces hold no old-format bytes to roll back to, and
the previous build cannot open a root stamped with the current epoch or negotiate
the current protocols. Rolling the image back after this procedure produces a
node that fails startup, not a recovered cluster.

Ordinary operational rollback of an image within one format generation is a
different procedure; see `operational-recovery.md`, which explicitly does not
include wiping state.
