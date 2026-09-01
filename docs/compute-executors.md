# Compute executors

This page describes the per-node executor backends. How a submitted job is
admitted, planned, duplicated under a partition, versioned, audited, quota-bound
and drained is described in [Distributed execution jobs](distributed-jobs.md).

Aruna can run one compute executor per node. Set `ARUNA_COMPUTE_EXECUTOR` to
`none`, `docker`, `apptainer`, or `kubernetes`; the default is `none`. Selecting
an executor whose Cargo feature was not compiled is a configuration error.
`off` and an empty value are accepted as `none`, so a supervisor can disable
compute by writing the key rather than by unsetting it.

An explicitly selected executor must pass its startup health checks. Set
`ARUNA_COMPUTE_OPTIONAL=1` only when the node may start without compute after a
health failure. Invalid configuration is still rejected.

Set `ARUNA_COMPUTE_LOCAL_ONLY=1` on a user device: the executor then runs only
the owner's local jobs and is never advertised to the realm. See
[Local runs on a user device](#local-runs-on-a-user-device).

Task images are resolved before the attempt intent is committed and the
digest-pinned reference is retained for exact-bits recovery. Kubernetes also
accepts tagged task images without a registry lookup and always pulls them;
retries may therefore run newer bits if the tag moves. The Kubernetes helper
image still has no default and must be configured as an immutable digest
reference.

## Shared security and recovery

Docker and Kubernetes tasks run as `65534:65534`, without Linux capabilities or
privilege escalation, with runtime-default seccomp and no service-account token.
Network access is isolated for Files staging unless a task explicitly carries
the `aruna-engine.org/network=open` execution tag. Direct-S3 is explicit: Docker
and Apptainer require open networking, while Kubernetes restricts task egress to
the configured S3 CIDRs and port. Stage and fetch helpers never receive S3
credentials.

Every external attempt has an immutable attempt epoch and a monotone controller
generation. Takeover keeps the same external name and advances only the
generation. Backend absence never retires an attempt or authorizes a new name;
the controller retries the same attempt with freshly opened input streams.
Terminal evidence or an epoch-specific durable tombstone is required before the
intent can be retired.

Tombstones are retained indefinitely:

- Docker records the epoch and tombstone state in its durable control file.
- Apptainer atomically writes a tombstone under its control directory.
- Kubernetes retains the deterministic Job and marks it as a tombstone.

Automatic cleanup may remove accessories behind a tombstone, but never the
tombstone itself.

Kubernetes live Jobs retain an attempt-protection finalizer so an out-of-band
delete cannot appear absent and trigger a duplicate run. Tombstoning removes
that finalizer after the tombstone annotation is durable, allowing deletion of
retired Jobs and their namespace without weakening live-attempt protection.

## Staging modes

Files mode is the default. The controller streams authorized inputs into the
backend, and streams declared outputs back through the ordinary Aruna storage
path. No task or helper S3 credential is created.

Direct-S3 mode supplies one least-privilege attempt credential to the task only.
It does not stage container paths. Kubernetes stores the credential in the task
Secret; stage and fetch Pods do not mount or reference that Secret. The endpoint
the task is handed is `ARUNA_COMPUTE_S3_URL`, or `S3_PUBLIC_URL` when that is
unset; every backend resolves it the same way.

Input files may share a directory with outputs. Exact input/output collisions,
input-file ancestors, duplicate paths, and root output parents are rejected.
Paths are normalized by components, so `/out` and `/output` remain distinct.

An output path may carry the POSIX wildcards `*`, `?`, and `[...]`, which never
cross `/`. Each match is uploaded below the declared destination with the
required `path_prefix` stripped, and a pattern that matches nothing captures
nothing. A declared path without wildcards must still exist when the task ends.

## Local runs on a user device

A user device runs compute for the owner of that device alone, and only when the
owner asks for it: `POST /compute/jobs` with `target: "local"`, or a TES task tagged
`aruna-engine.org/target=local`. The realm never dispatches to a device, because
a device advertises no executor at all, whatever compute the owner enabled on the
machine.

The desktop app writes these keys for the node it supervises:

| Key | Meaning | Default |
| --- | --- | --- |
| `ARUNA_COMPUTE_EXECUTOR` | `docker` or `apptainer`, resolved from the app's backend probe; `off` when the owner disabled compute | `none` |
| `ARUNA_COMPUTE_LOCAL_ONLY` | `1` selects the local-only profile | unset: shared deployment |
| `ARUNA_COMPUTE_OPTIONAL` | `1` keeps the node up when the daemon does not answer | unset: a health failure fails startup |
| `ARUNA_COMPUTE_MAX_CONCURRENT` | how many of the owner's runs may be in flight at once; a submission beyond it is refused | unset: unmeasured |
| `ARUNA_COMPUTE_MAX_CPU_CORES`, `ARUNA_COMPUTE_MAX_RAM_BYTES`, `ARUNA_COMPUTE_MAX_DISK_BYTES` | the owner's caps, from This device | unset: unmeasured, see [Execution envelope](#execution-envelope) |
| `ARUNA_COMPUTE_KEEP_FAILED` | `1` keeps failed containers for inspection (Docker) | unset: they are removed |
| `ARUNA_COMPUTE_STATE_ROOT` | Docker state root under the app's data directory | `./compute-state` |
| `ARUNA_COMPUTE_APPTAINER_CGROUP_ROOT`, `ARUNA_COMPUTE_APPTAINER_STATE_ROOT`, `ARUNA_COMPUTE_APPTAINER_SIF_CACHE` | delegated cgroup v2 root of the user slice, and the Apptainer roots | the cgroup root is required by Apptainer |

In the local-only profile the Docker builder registers no workspace endpoint and
skips the container-reachable `S3_PUBLIC_URL` and non-loopback `S3_ADDRESS`
requirements: a device stages files, and the S3 listener it binds for its owner
is loopback-only, so no container can reach it and file staging under
`ARUNA_COMPUTE_LOCAL_ONLY=1` is unaffected. Apptainer is unchanged and still
needs its delegated cgroup root.

A local run stages its inputs as files into the node-local workspace bucket
`ws-<jobid>`. It refuses mounted inputs, `workspace.mode` `none` and Direct-S3
staging, all of which need an S3 endpoint a container could reach, and
`workspace.mode` `existing`.

Where an output lands depends on the surface. `POST /compute/jobs` declares workspace
outputs, which stay in `ws-<jobid>` until the owner publishes them. A TES task
declares each output with an `s3://bucket/key` url, and a local task writes it to
the device-local bucket that url names, which must belong to the execution group
and grant the owner WRITE.

An input this device does not hold is refused at submit unless it names the realm
node and version holding it; staging then fetches that exact version into
`ws-<jobid>` as an ordinary local object, never as a reference. A holder it
cannot reach fails the run, not the submission. A run is refused while this
node's compute plane is drained, and while the owner's unfinished runs already
reach `ARUNA_COMPUTE_MAX_CONCURRENT`.

Local runs are listed by the ordinary `GET /compute/jobs` of the device's own API, and
`GET /device/compute` reports the plane the owner configured. Nothing about a
local run is forwarded, replicated or offered to the realm.

## Docker

Required configuration, unless `ARUNA_COMPUTE_LOCAL_ONLY=1`:

- `ARUNA_COMPUTE_S3_URL` or `S3_PUBLIC_URL`: the S3 endpoint a container is
  handed. `ARUNA_COMPUTE_S3_URL` wins when both are set and exists because
  containers may need a different address than browsers do: it keeps the
  portal-facing URL on loopback while workloads get a host-reachable one. Docker
  refuses to start when the resolved endpoint is `localhost`, a loopback address,
  or an unspecified address, because no container could reach it.
- `S3_ADDRESS`: non-loopback listener address.

Optional configuration:

- `ARUNA_COMPUTE_DOCKER_DISK_BYTES`: nonzero writable-layer ceiling in bytes.
  When unset, `storage_opt` is omitted and task disk requests are unenforced.
- `ARUNA_COMPUTE_DOCKER_PULL_DEADLINE`: image pull deadline in seconds;
  defaults to `300`.
- `ARUNA_COMPUTE_STATE_ROOT`: durable Docker state root; defaults to
  `./compute-state`. Apptainer names its own roots.
- `ARUNA_COMPUTE_KEEP_FAILED`: `1` keeps the containers of failed attempts for
  inspection; defaults to removing them. Docker only.

The state root must be durable and
exclusive to one controller for the Docker daemon. A daemon lock enforces that
contract, and a per-attempt lock serializes create, stage, and start.

The backend uses non-root containers, drops all capabilities, sets
`no-new-privileges`, uses runtime-default seccomp, and defaults to
`network_mode=none`. File outputs remain in the container writable layer; named
volumes are not used. Read-only root filesystems with file outputs are rejected.

Docker cannot restrict egress to S3, so an S3-only network request is refused as
an invalid spec: only Kubernetes enforces that mode. The container manifest
independently maps S3-only to `network_mode=none`, so even a spec that reached
the manifest by another path fails closed rather than opening the network.
Direct-S3 on Docker therefore requires open networking.

When a disk ceiling is configured, startup health creates and removes an unstarted
probe container. The ceiling requires overlay2 over XFS with `pquota`; unsupported
daemon or backing-filesystem configurations fail the compute health gate.

## Apptainer

Required configuration:

- `ARUNA_COMPUTE_APPTAINER_CGROUP_ROOT`: writable delegated cgroup v2 root.
- `ARUNA_COMPUTE_APPTAINER_STATE_ROOT`: durable state root; defaults to
  `./compute-state/apptainer`.
- `ARUNA_COMPUTE_APPTAINER_SIF_CACHE`: SIF cache; defaults to
  `./compute-state/sif`.
- `ARUNA_COMPUTE_APPTAINER_PULL_DEADLINE`: image pull deadline in seconds;
  defaults to `300`.
- `ARUNA_COMPUTE_STOP_GRACE`: graceful stop interval in seconds; defaults to
  `10`.

Apptainer performs no user switch at launch, so tasks run as the Aruna service's
own identity and the task manifest reports that identity rather than a fixed
`65534:65534`. The service must run as a non-root user; root is rejected.
Apptainer does not expose root, fakeroot, or a configurable task user.
Per-attempt disk ceilings are not supported and requests containing one are
rejected.

Launch uses two already-execed internal modes. The supervisor creates its own
session and holds `exec.lock`; the launcher creates its own process group,
enters the attempt cgroup, then waits on a post-exec Unix-socket barrier. The
supervisor fsyncs the payload identity before releasing that barrier. There is
no blocking `pre_exec` callback.

Process identity is pidfd-first plus `/proc` start ticks. Cancellation sends the
graceful signal through the verified pidfd, then uses `cgroup.kill` if the tree
does not empty. A recorded launch with a dead supervisor, an empty cgroup, and
no exit record is LOST terminal evidence and is never relaunched. A free
`exec.lock` alone is not launch permission.

Health checks the Apptainer binary and version, that the service process is not
root, state-root durability, and the ability to create and kill an empty child
under the delegated cgroup root. A task whose `run_as` is root, or is not the
process identity, is rejected, because no user switch happens at launch. Crash
survival still depends on the service manager preserving the delegated cgroup;
configure its kill mode accordingly.

## Kubernetes

Required configuration:

- `ARUNA_COMPUTE_K8S_STORAGE_CLASS`: CSI-backed StorageClass.
- `ARUNA_COMPUTE_K8S_HELPER_IMAGE`: helper image pinned by digest.
- `ARUNA_COMPUTE_K8S_NAMESPACE`: compute namespace; defaults to `default`.
- `ARUNA_COMPUTE_K8S_PULL_DEADLINE`: helper mount/deletion deadline in seconds;
  defaults to `300`.
- `ARUNA_COMPUTE_K8S_S3_CIDRS`: comma-separated egress CIDRs for Direct-S3.
- `ARUNA_COMPUTE_K8S_S3_PORT`: S3 TCP port; defaults to `443`.
- `ARUNA_COMPUTE_K8S_S3_MOUNT_DRIVER`: CSI driver name for S3 mounts. Unset
  disables S3-mount staging; set it to the deployed driver, for example
  `s3.csi.scality.com`.
- `ARUNA_COMPUTE_K8S_SERVICE_ACCOUNT`: workload ServiceAccount; defaults to
  `aruna-workload`.
- `ARUNA_COMPUTE_K8S_EXECUTION_LOCATION`: placement location of the worker
  nodes. Workers do not run on the controller, so this is the location the
  backend advertises.
- `ARUNA_COMPUTE_K8S_EXECUTION_LABELS`: `key=value,key2=value2` placement labels
  of those worker nodes.
- `ARUNA_COMPUTE_K8S_NODE_SELECTOR`: `key=value` selector stamped on every task
  and helper pod.

Worker placement counts as proven only when both the execution location and the
node selector are configured. Without them the backend advertises no location
and no labels at all, so it stays eligible for unplaced work and never claims
the controller's site. It then also reports no network-policy enforcement.

`ARUNA_COMPUTE_K8S_EXECUTION_LOCATION` and `ARUNA_COMPUTE_K8S_EXECUTION_LABELS`
are what this backend advertises as the placement site of its workers: they
replace the node's own `ARUNA_NODE_LOCATION` and `ARUNA_NODE_LABELS` in the
compute subject that placement policies are evaluated against, because workers do
not run on the controller and the controller's site would be the wrong answer.

`ARUNA_COMPUTE_K8S_EXECUTION_LABELS` and `ARUNA_COMPUTE_K8S_NODE_SELECTOR` each
accept at most 16 entries and every key must be non-empty; a list that breaks
either rule is a configuration error and the backend does not start, whatever
`ARUNA_COMPUTE_OPTIONAL` is set to. Entries are keyed, so repeating a key keeps
its last value.

## Execution envelope

Every backend advertises static ceilings and refuses an attempt it cannot bound:

- `ARUNA_COMPUTE_MAX_CPU_CORES`, `ARUNA_COMPUTE_MAX_RAM_BYTES`,
  `ARUNA_COMPUTE_MAX_DISK_BYTES`, `ARUNA_COMPUTE_MAX_CONCURRENT`: the node's
  static ceilings. They hard-filter placement, and advertised availability is
  derived from them minus the node's current reservations. Availability only
  ranks targets; exact admission stays the target-side reservation.

An unset ceiling is unmeasured, never zero, so it filters nothing. CPU and
memory of one attempt always carry a bound: the sealed request's own, else the
backend default. An attempt neither of them bounds is refused.

Those per-attempt defaults are compiled in, not configuration. All three
backends apply 2 CPU cores and 2 GiB of memory when the sealed request declares
none, and no environment variable overrides either value:

| Default | Docker | Apptainer | Kubernetes |
| --- | --- | --- | --- |
| CPU cores | 2 | 2 | 2 |
| Memory | 2 GiB | 2 GiB | 2 GiB |
| Disk | unset; `ARUNA_COMPUTE_DOCKER_DISK_BYTES` sets it | not supported; a request naming one is rejected | unset, and not configurable |
| Walltime | 24 h | none; only the request's own | none; only the request's own (`activeDeadlineSeconds`) |
| PID limit | 2048 | 2048 | not set |

Raising a node's throughput is therefore a matter of the static ceilings above,
not of these defaults; a workload that needs more than 2 cores or 2 GiB must
declare it in the request.

The Kubernetes executor supports Kubernetes 1.32 or newer. Files mode requires a
CSI driver that enforces `ReadWriteOncePod`. Each attempt creates a suspended Job
first, then an RWOP PVC.
The controller stages through an exec stream, gracefully deletes the stage Pod
with a UID precondition, waits until GET returns 404, writes the stage marker,
and CAS-unsuspends the same Job by UID and resource version. Pods are never
force-deleted.

Kubernetes log capture reads only the task Pod's stdout stream. The `stderr_*`
fields remain zero, so total captured log size is a lower bound.

The PVC sentinel and ConfigMap marker contain the Job UID, attempt epoch,
controller generation, and layout digest. A helper init container compares them
before the task starts and installs the static probe into an `emptyDir`; the task
startup probe repeats the comparison. Takeover CAS-bumps the Job generation,
gracefully removes lower-generation helpers, restages from fresh streams, writes
a new marker, and only then unsuspends.

The controller ServiceAccount needs these namespace permissions:

| Resource | Verbs |
| --- | --- |
| Jobs | create, get, list, watch, patch, delete |
| Pods | create, get, list, watch, delete |
| pods/exec | create |
| pods/log | get |
| PersistentVolumeClaims | create, get, list, watch, delete |
| Secrets | create, get, delete |
| ConfigMaps | create, get, patch, delete |
| NetworkPolicies | create, get, patch |
| ServiceAccounts | get |

Its narrowly bound ClusterRole needs `get` on StorageClasses, `create` on
SelfSubjectAccessReviews, and `get` on CSIDrivers only when
`ARUNA_COMPUTE_K8S_S3_MOUNT_DRIVER` is set. The `aruna-workload` ServiceAccount must already exist
in the namespace and must not have a Role or ClusterRole binding. Workload and
helper Pods disable token automount.

The operator must also provide:

- a CSI provisioner and sidecars that enforce RWOP;
- an enforcing CNI for NetworkPolicy;
- quota and storage-driver support for requested PVC and ephemeral-storage
  limits;
- sufficient namespace quota for Jobs, Pods, PVCs, ConfigMaps, and Secrets;
- routable, stable S3 CIDRs or an egress proxy when Direct-S3 is enabled.

Startup health is intentionally cheap: namespace access, StorageClass GET, the
S3-mount CSIDriver GET when that driver is configured, workload-ServiceAccount
existence, and SelfSubjectAccessReview checks for the required verbs. It does not launch RWOP, NetworkPolicy, quota, or admission
canaries. Those operator prerequisites are enforced lazily by the first real
object, and a failure parks the attempt without force deletion or a weaker
release fallback.

## Helper image

The helper source is in `scripts/compute-helper`. It provides only `stage`,
`fetch`, `list`, and `probe`: safe tar staging, tar output fetch, workspace
listing for wildcard outputs, marker comparison, and probe installation. It
contains no AWS SDK or credential input.

Build the plain local image from the repository root with:

```bash
scripts/compute-helper/build.sh
```

Set `ARUNA_COMPUTE_HELPER_IMAGE` to choose the local tag during the build. Push
or mirror that image through the operator's normal registry process, resolve its
immutable digest, and set `ARUNA_COMPUTE_K8S_HELPER_IMAGE` to the digest
reference. There is no signing, SBOM, multi-architecture, or default-publication
pipeline in this alpha implementation.
