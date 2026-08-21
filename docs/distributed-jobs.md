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

The backend decides which class an ended attempt belongs to, and the class
decides the replicated execution state:

| Class | Attempt evidence | Execution state | TES state | Retry |
| --- | --- | --- | --- | --- |
| Job-specific | Non-zero exit, OOM kill, walltime exceeded, log limit exceeded, invalid declared outputs | `failed` | `EXECUTOR_ERROR` | Suppressed |
| Infrastructure | Lost evidence after a recorded start, a pod stuck past the image-pull deadline, a container that died without an exit code, a daemon or API failure, no compute backend configured, a remote input that could not be staged, a record that could not be signed | `error` | `SYSTEM_ERROR` | Re-planned |

Only the first class is proof about the work. An `error` decides nothing: the
logical job stays `indeterminate`, and the execution that ended re-arms the
family's witness deadline on the node that published it and on every holder that
replicates that update, so the family is planned again instead of waiting for a
timeout.

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
  from the `endpoint_url` returned for that output, with the exact VersionId
  (`GET /object?versionId=…`). The endpoint is the node-local S3 owner and may
  differ from the node that answered the job status request.
- **S3 latest** is the node-local last-write-wins head of that key. A duplicate
  execution, a later unrelated upload, or a copy from another node can all make
  a different version the latest one.

Reading the job's outputs without a version id therefore answers "whatever is
current", not "what this job produced". Use the VersionId when you mean the
result of the job.

`endpoint_url` is nullable in both `GET /jobs/{job_id}` and
`GET /jobs/{job_id}/audit`. An output whose owning node has no advertisement
here is returned without an address rather than failing the whole read; the
VersionId and the owning execution are still exact. Retry, or ask a node that
holds that advertisement, for the address.

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

Three realm-admin routes carry this surface. All three need a bearer token
issued for this realm; the two reads need READ on the realm configuration path
and `PUT` needs WRITE. Only a genuinely absent realm-configuration document is
`404`. A read that failed in storage or could not decode is `500`, so absence is
never inferred from a failed read.

- `GET /admin/compute/config` reads the configuration this node holds. It is a
  node-local read of a replicated document, so a change written elsewhere can be
  missing here until it arrives.
- `PUT /admin/compute/config` replaces that configuration wholesale. Links and
  group quotas absent from the body are dropped, so send the complete intended
  configuration. `400` covers a malformed group id, a duplicate directed link or
  group entry, an empty or oversized location, a zero bandwidth and a zero
  witness delay; `409` means another update won the race and the same body may
  be retried.
- `GET /admin/compute/snapshots` reports the observed demand and reservation
  snapshots, each stamped with its publisher's membership and publisher
  generations and its observation time. `?group_id=` adds that group's merged
  demand next to the standing quota it is judged against; a value that is not a
  ULID is `400`.

The configuration document carries operator knowledge no node can measure for
itself:

| Field | Meaning |
| --- | --- |
| `links[].from`, `links[].to` | One directed transfer estimate between two placement locations. Direction matters; both directions are separate entries. |
| `links[].bandwidth_bytes_per_sec` | Bandwidth of that directed link. Zero is refused rather than clamped, because it would make one transfer estimate infinite. |
| `pessimistic_bandwidth_bytes_per_sec` | Assumed for any link nobody configured. Default 12500000 (100 Mbit/s). |
| `availability_stale_after_ms` | Age above which an availability sample only counts as unknown for ranking. Default 300000. |
| `witness_base_delay_ms` | Per-rank fallback delay of the leaderless witness schedule. With replication factor RF, `witness_base_delay_ms * (RF - 1)` is the worst-case wait before any witness launches while higher ranks are down. Must be greater than zero. Default 30000. |
| `default_group_quota` | Standing quota of every group without its own entry. |
| `group_quotas[]` | One `{group_id, quota}` entry per group that has its own. It replaces the default wholesale, so an explicitly unlimited group is an entry whose dimensions are all unset. |

A quota has eight independent dimensions, all optional:

| Dimension | Scope | Counts |
| --- | --- | --- |
| `max_jobs` | Group | Nonterminal admitted request families |
| `max_cpu_cores` | Group | Sealed CPU ceilings of those families |
| `max_ram_bytes` | Group | Sealed RAM ceilings of those families |
| `max_disk_bytes` | Group | Sealed disk ceilings of those families |
| `max_job_cpu_cores` | Job | What one request may ask for |
| `max_job_ram_bytes` | Job | What one request may ask for |
| `max_job_disk_bytes` | Job | What one request may ask for |
| `max_job_walltime_ms` | Job | Walltime sealed into one request |

Per-job ceilings are decided without reading the demand view at all. A quota
that sets only per-job ceilings therefore never depends on replicated state.

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

A snapshot is bounded, so a busy realm can understate itself. Truncation is
tracked per group: one busy group never marks a quiet one, and a snapshot that
had to drop whole groups only understates a group it does not name. A group
whose merged view is understated cannot be shown to be under its cap, so a new
admission for it is refused with the same `409` quota body, reporting `observed`
at the limit because no smaller number stands behind an understated view. A
quota with only per-job ceilings never reads that view and is never refused for
truncation. A peer advertisement this node cannot decode is skipped with a
warning and counts as an unobserved publisher, exactly like a partition.

`503 job_placement_unavailable` on submission is availability, never a quota
verdict. It means the request's family placement or a family holder could not be
reached, the group's demand view could not be read, the group's admission
revision moved under three consecutive reads, admission lost three transactions
in a row to concurrent submissions of the same group, or the id clock is
unhealthy. It is retryable with the same idempotency key. An idempotent replay is
settled from records this node already holds, before any quota read, so a replay
is never quota-refused.

## Departure, drain and rejoin

`POST /admin/compute/drain` drains this node's compute plane: no planner selects
it for new executions and it declines launch offers, while everything holding a
receipt keeps running. The operator drain is stored separately from the
departure state a placement change causes, so returning to the placement map
never silently undrains a node an operator drained; undrain it explicitly.

The flag is durable and is the only authority. Every republication of this
node's advertisement, including the ordinary heartbeat, re-derives
`compute_draining` from that flag inside its own write transaction, so a
concurrent heartbeat cannot carry a stale copy forward and undrain the node. A
launch offer reads the same durable flag directly, so a node whose advertisement
is stale still declines. A node that is leaving the realm stays draining
whatever the flag says.

`changed` in the response reports that the durable flag moved, not that the
advertisement was already republished; the advertisement follows, and a node
that has not advertised yet logs a warning and republishes when it does.
`GET /admin/compute/snapshots` reports the current value as `operator_draining`.
`503` on the drain route means the advertisement could not be republished and is
retryable.

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

Only governed work stops. A write carrying no placement refs never consults the
gate at all, so an ungoverned `PutObject`, `CopyObject`, multipart part-copy,
staging snapshot or inbound replication still succeeds on a blocked or draining
node. What is refused is exactly a write whose refs would have to be evaluated
against a subject this node cannot currently stand behind.

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

A release drops registrations only. The bytes stay on the local backend,
unserveable but present, and nothing in this flow deletes them; reclaiming that
space is a separate operator decision. Sending `bucket`, `key` or `version_id`
together with `revalidate` is refused, so an accidental release is impossible,
and a release without all three is `400`.

The rules that decide what "compliant" means, and the rest of the realm-admin
placement surface, are described in [Placement policies](placement-policies.md).

## Launch offers and declines

A witness offers a planned launch to one target and waits 30 seconds for the
answer. One unanswered offer never retires the target: a target that accepts
slowly would otherwise be replaced by a second launch while it is already
running the work. A target keeps its launch for two full offer deadlines plus
one `witness_base_delay_ms`, which is 90 seconds at the default 30 second base,
and is re-offered at least once inside that window before the next plan excludes
it.

A decline says only as much as it can prove. `Capacity` is reported when the
target's own reservation found no free capacity for the sealed request.
`Draining` is reported only for an operator drain, a membership or gate
refusal, or an unavailable backend. A reservation that fails on a storage
commit conflict is a lost race between two admissions of the same launch, not a
drain: the target reloads the family, answers with the receipt the winner
committed when launch id and digest match, answers `LaunchConflict` when the
same launch id carries a different digest, retries the reservation up to three
times otherwise, and finally leaves the offer undecided so the witness asks
again. Any other storage failure is classified by what it proves about the
commit. A refused write (`QueueFull`) proves nothing was committed, so the
reservation is retried inside the same three-attempt bound and the offer is
left undecided when the bound is reached; it is never a drain. Every other
storage failure, `CommitFailed` first among them, leaves the outcome unknown:
the reservation, the receipt, the outbox entry, and the local execution row may
already be durable. The target then reconciles exactly once and writes nothing:
it reads the family's receipts, answers with the committed receipt when launch
id and digest match, answers `LaunchConflict` when the same launch id carries a
different digest, and leaves the offer undecided when no receipt is visible or
the read is incomplete. A recovered or replayed acceptance re-arms the same
wakeups a fresh acceptance does, so the receipt replicates and the execution
starts at once instead of waiting for an unrelated timer. An unknown storage
outcome is never reported as `Draining`. A node with the operator drain flag set
declines every launch offer, even when its own advertisement is stale.

One scheduling round screens every advertisement the realm publishes. Discovery
walks the members in node id order and each member's backends by executor kind,
buffering them into pages of at most 1024 advertisements; each full page is
screened, routed, and ranked immediately, and the round remembers a cursor on
the last target of the page so the next page must continue strictly past it (a
repeated or out-of-order entry is refused). The realm total is therefore bounded
by membership times the eight backends a node may advertise, not by the page
bound. Across all pages the round keeps the best 128 eligible targets by a total
order, 8 ranked alternatives, and 32 rejection explanations, counting whatever
the audit bound dropped in `omitted`. Selection and the plan digest are sealed
only after the last page, so a lower-ranked target in an early page is never
launched while a better one waits in a later page, and the same advertisement
set produces the same plan whatever the page boundaries or insertion order were.
A round that could not read every advertisement is reported as retryable and
repeated, never as "no eligible target".

Scheduling, target admission, state publication and routing act only on a
family read that is proven complete. A page error, an undecodable row, or the
4096-record bound with records remaining leaves the round undecided: the witness
retries without creating a budget or launch, the target answers undecidable
without reserving resources or appending a receipt, publication appends no
update, and routing answers unavailable. A state update is appended only onto a
receipt whose existing updates form a contiguous chain (no duplicate or missing
sequence, no broken predecessor), and its sequence is the chain's maximum plus
one.

## Record admission

A family record a peer offers is admitted, retained, or refused; it is never
half-accepted. Its predecessors (the spec for a claim, cancellation or budget;
the spec and scheduler budget for a launch; the exact launch for a receipt; the
exact receipt for an update or output; the preceding update for a later update)
are read by exact key or by a complete record-kind scan, never from a fixed
prefix of the family, so a valid record never stays pending merely because its
family grew large; a storage error or an incomplete scan keeps the record
retained rather than admitting it from partial evidence. A family projection
that exceeds 4096 records is reported truncated: it is never cached as fresh,
never bridged into local job rows, and answers `indeterminate` where a state is
asked. Holder authority moves with membership, so a record whose
publisher this node's current view does not rank as a holder is retained and
judged again later rather than rejected. While a record is retained, a
re-arrival of it is answered as unavailable, which keeps the publisher retrying
instead of treating a deferral as acceptance.

A publisher gives up on delivery only against evidence. Unreachable holders
never count; a record that holders definitively refuse 16 times is dropped from
the publishing node's outbox so it stops consuming the queue. The record itself
stays durable and addressable, and both sides of a key conflict are retained.

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
version. A job-specific failure surfaces as `EXECUTOR_ERROR` and an
infrastructure error as `SYSTEM_ERROR`, so the two classes stay distinguishable
through the facade.

`POST /ga4gh/tes/v1/tasks` refuses with the same admission mapping as the native
submit, not with a generic server error:

| Status | Cause |
| --- | --- |
| `400` | Malformed task, an unsupported TES feature, an input that is not a readable object, or more outputs than a task may declare |
| `401` | Missing or invalid bearer token or basic credential |
| `403` | No WRITE on the target group, a group tag contradicting the credential, a path-restricted credential, or a routed authority refusing the submission |
| `409` | The idempotency key tag already names a different task, the group's standing compute quota refuses the admission, or the composition conflicts on a staged key |
| `503` | The availability causes listed under quota semantics; the body carries the fixed text `job_placement_unavailable` and the caller may create the task again with the same idempotency key |
