# Distributed execution jobs

An execution job submitted to Aruna is not owned by one node. The request is
admitted into a replicated **submission family**, planned independently by every
node that holds it, and run by the execution target that signs its own receipt.
There is no leader, no quorum, and no global scheduler, which has direct
consequences for what the API can promise. This page describes those
consequences for operators and users.

For per-backend behaviour of the executors themselves see
[Compute executors](compute-executors.md).

## Identities

| Identity | What it names | Stability |
| --- | --- | --- |
| `job_id` | The alias the caller submitted under | Stable for the caller, forever |
| `submission_id` | The request itself | Stable; two aliases of one request share it |
| `canonical_job_id` | The alias a responder currently reduces as canonical | May change when a partitioned claim is learned |
| `execution_id` | One physical execution attempt | Immutable; duplicates get their own |
| `VersionId` | One exact object version an execution wrote | Immutable |

`GET /jobs/{job_id}` answers for any accepted alias of the request. The `family`
block carries all of the above plus the projection `revision` and digest, so a
client can detect that its view moved without diffing whole responses.

## At-least-once execution

A partition may admit and run the same request more than once: every family
holder plans on its own, and a target that cannot be reached is not a target
that did not run. Aruna never resolves that by cancelling work it cannot see.
Instead every physical execution stays visible:

- one successful execution is **canonical** and supplies the job's result;
- every other successful execution is a **duplicate success**, retrievable and
  auditable, never erased;
- failed or cancelled executions remain exact terminal facts for their own
  `execution_id`.

The canonical choice is content-independent (a digest over the submission id,
the request digest and the execution id), so every replica with the same records
picks the same one, and no publisher can bias it with a timestamp.

Write jobs so that a second run is acceptable: use the exact output VersionIds
the API returns rather than assuming one writer, and avoid side effects outside
Aruna that cannot tolerate a repeat.

## Distributed failure needs proof

The replicated logical state is `failed` only when an authenticated execution
reports a permanent, job-specific failure such as an invalid task or command.
That evidence suppresses retries. A later authenticated success still wins.

Infrastructure errors, retry exhaustion, and silence remain `indeterminate`:
none proves that a partitioned execution cannot still be running or succeed.

When every execution a responder knows about ended without success and it has no
retry armed, the status reports `family.locally_exhausted = true`. That flag is
explicitly responder-local, is excluded from the projection digest, and must not
be treated as a terminal failure. Poll again, or ask another node.

`family.partial` means the responder holds more records than one projection
reduces; the answer is a prefix of the truth, not the whole of it.

## Output versioning

Each output object is named by the exact `VersionId` its execution created, and
by the `execution_id` that produced it. That identity never changes.

This is not the same as the object's S3 latest version:

- **Job canonical output** is what the canonical execution wrote. Retrieve it
  with the exact VersionId (`GET /object?versionId=…`), which is also what the
  TES task log URLs carry.
- **S3 latest** is the convergent last-write-wins head of that key. A duplicate
  execution, a later unrelated upload, or a copy from another node can all make
  a different version the latest one.

Reading the job's outputs without a version id therefore answers "whatever is
current", not "what this job produced". Use the VersionId when you mean the
result of the job.

Concurrent writers in different partitions can each claim the same head
generation. `GET /blobs/contenders` lists every VersionId this node observed
claiming one generation, which is how a caller learns an object had concurrent
versions; S3 `GET` and `HEAD` never expose a second head.

## Audit

`GET /jobs/{job_id}/audit` pages the immutable records of the request by stable
record key: every claim, witness budget, launch intent, receipt, execution state
update, output set and cancellation observation. `scope=submission` additionally
pages the idempotency conflicts of the same submission, each marked with
`conflicting_family`.

Records are projected, never returned raw. Signatures, envelopes and the node
identities of publishers, schedulers and executors are omitted. Records are
retained indefinitely: deleting a subset could resurrect a different projection
or erase exactly the duplicate evidence the audit exists for.

`conflicts` lists records that were refused under a key another record already
held. Both stay addressable; neither overwrites the other.

## Responder-local answers

Every read is answered by the node that received it, from the records it has
replicated. `family.responder_node_id` names that node and
`eventually_consistent` is always true. Coverage and diagnostics surfaces follow
the same rule: `complete` means a bounded local iterator was exhausted, never
that another partition was observed.

Ordinary group members do not receive the node identities of other nodes,
holder topology, other tenants' pressure, or private placement labels. The
realm-admin surfaces below carry the full explanation.

## Quota semantics

Standing compute quotas are configured per realm with an optional per-group
override that replaces the realm default wholesale. An unset dimension is
unbounded, never zero.

- `PUT /admin/compute/config` replaces links and quotas wholesale.
- `GET /admin/compute/snapshots` reports the observed demand and reservation
  snapshots, each stamped with its publisher's membership and publisher
  generations and its observation time.

Two different controls are reported and never summed:

| Control | Counts | Decides |
| --- | --- | --- |
| Logical admitted demand | Nonterminal request families | Whether a NEW submission is admitted |
| Physical reservation | Exact local CPU/RAM/disk per execution | Whether a target accepts a launch |

Quotas bound new admissions only. Lowering a quota, or observing an overshoot
after a partition converges, never cancels, pauses or reclaims work that is
already admitted, queued, preparing or running; the only consequence is that
further admissions are refused with `409` and a typed `quota` body naming the
scope, dimension, observed total, request and limit. Because the demand view is
replicated, concurrent partitions may overshoot a cap before converging. That is
the accepted bound, not a bug.

## Departure, drain and rejoin

`POST /admin/compute/drain` drains this node's compute plane: no planner selects
it for new executions and it declines launch offers, while everything holding a
receipt keeps running. The operator drain is stored separately from the
departure state a placement change causes, so returning to the placement map
never silently undrains a node an operator drained; undrain it explicitly.

On graceful departure a node stops admitting immediately, publishes its final
snapshots, and records every execution it still holds capacity for as
**unresolved**. Unresolved is not finished: a departing node may not declare a
remotely observed execution terminal. Removal is never blocked because governed
bytes or unresolved executions exist, and the report stays readable at
`GET /admin/compute/snapshots`.

A rejoining node uses a new membership epoch and quarantines every local copy
until it has been revalidated.

## Quarantined copies block admission

A subject transition, a rejoin, or a failed revalidation leaves non-compliant
copies quarantined. While any remain, the node serves no governed data and
admits no new governed work, including new execution targets. This is the safe
state, not an error, and it is sticky on purpose: it ends only when an operator
decides what happens to those copies.

1. List them with `GET /admin/placement-diagnostics`; each violation names the
   exact bucket, key and version.
2. Resolve them with `POST /admin/placement-quarantine`:
   - `{"action": "revalidate"}` re-evaluates every local registration against
     the subject the node advertises now and restores the compliant ones;
   - `{"action": "release", "bucket": …, "key": …, "version_id": …}` first drops
     the local registrations of that one version, which makes it locally
     unavailable rather than serveable and never deletes data on another node.
3. `cleared: true` in the response means nothing quarantined is left and
   governed admission is open again.

## Execution-site fencing

A receipt seals the exact execution site (its placement subject generation and
digest) the target accepted the launch under. If the node's advertised subject
drifts before the attempt starts, the start is refused with a retryable error
naming the drift rather than running accepted work at a site nobody authorized.
Local jobs that never took a receipt keep the unfenced path.

## GA4GH TES

The TES facade projects the same logical view as the native REST status: the
same state mapping (`indeterminate` becomes `UNKNOWN`), the canonical
execution's outputs, and task-log URLs that carry the exact `versionId`. A task
with no canonical success has no outputs, rather than the object's current
version.
