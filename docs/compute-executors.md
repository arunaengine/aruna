# Compute executors

Aruna can run one compute executor per node. Set `ARUNA_COMPUTE_EXECUTOR` to
`none`, `docker`, `apptainer`, or `kubernetes`; the default is `none`. Selecting
an executor whose Cargo feature was not compiled is a configuration error.

An explicitly selected executor must pass its startup health checks. Set
`ARUNA_COMPUTE_OPTIONAL=1` only when the node may start without compute after a
health failure. Invalid configuration is still rejected.

Task images are resolved before the attempt intent is committed and the
digest-pinned reference is retained for recovery. Kubernetes requires the task
image to be supplied by digest. The Kubernetes helper image also has no default
and must be configured as an immutable digest reference.

## Shared security and recovery

Docker and Kubernetes tasks run as `65534:65534`, without Linux capabilities or
privilege escalation, with runtime-default seccomp and no service-account token.
Network access is isolated for Files staging. Direct-S3 is explicit: Docker and
Apptainer require open networking, while Kubernetes restricts task egress to the
configured S3 CIDRs and port. Stage and fetch helpers never receive S3
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
Secret; stage and fetch Pods do not mount or reference that Secret.

Input files may share a directory with outputs. Exact input/output collisions,
input-file ancestors, duplicate paths, and root output parents are rejected.
Paths are normalized by components, so `/out` and `/output` remain distinct.

## Docker

Required configuration:

- `S3_PUBLIC_URL`: reachable from a container when task-side S3 is used.
- `S3_ADDRESS`: non-loopback listener address.

Optional configuration:

- `ARUNA_COMPUTE_DOCKER_DISK_BYTES`: nonzero writable-layer ceiling in bytes.
  When unset, `storage_opt` is omitted and task disk requests are unenforced.
- `ARUNA_COMPUTE_DOCKER_PULL_DEADLINE`: image pull deadline in seconds;
  defaults to `300`.

Docker uses `./compute-state` by default. The state root must be durable and
exclusive to one controller for the Docker daemon. A daemon lock enforces that
contract, and a per-attempt lock serializes create, stage, and start.

The backend uses non-root containers, drops all capabilities, sets
`no-new-privileges`, uses runtime-default seccomp, and defaults to
`network_mode=none`. File outputs remain in the container writable layer; named
volumes are not used. Read-only root filesystems with file outputs are rejected.

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

Its narrowly bound ClusterRole needs `get` on StorageClasses and `create` on
SelfSubjectAccessReviews. The `aruna-workload` ServiceAccount must already exist
in the namespace and must not have a Role or ClusterRole binding. Workload and
helper Pods disable token automount.

The operator must also provide:

- a CSI provisioner and sidecars that enforce RWOP;
- an enforcing CNI for NetworkPolicy;
- quota and storage-driver support for requested PVC and ephemeral-storage
  limits;
- sufficient namespace quota for Jobs, Pods, PVCs, ConfigMaps, and Secrets;
- routable, stable S3 CIDRs or an egress proxy when Direct-S3 is enabled.

Startup health is intentionally cheap: namespace access, StorageClass GET,
workload-ServiceAccount existence, and SelfSubjectAccessReview checks for the
required verbs. It does not launch RWOP, NetworkPolicy, quota, or admission
canaries. Those operator prerequisites are enforced lazily by the first real
object, and a failure parks the attempt without force deletion or a weaker
release fallback.

## Helper image

The helper source is in `scripts/compute-helper`. It provides only `stage`,
`fetch`, and `probe`: safe tar staging, tar output fetch, marker comparison, and
probe installation. It contains no AWS SDK or credential input.

Build the plain local image from the repository root with:

```bash
scripts/compute-helper/build.sh
```

Set `ARUNA_COMPUTE_HELPER_IMAGE` to choose the local tag during the build. Push
or mirror that image through the operator's normal registry process, resolve its
immutable digest, and set `ARUNA_COMPUTE_K8S_HELPER_IMAGE` to the digest
reference. There is no signing, SBOM, multi-architecture, or default-publication
pipeline in this alpha implementation.
