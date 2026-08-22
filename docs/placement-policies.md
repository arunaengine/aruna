# Placement policies

A placement policy is a residency rule: it names the destinations on which a
governed object version may be materialized. Objects carry references to
policies, nodes advertise a placement subject, and every write or serve of a
governed version evaluates that subject against every referenced rule.

The whole administrative surface is realm-scoped. Every route below needs a
bearer token issued for this realm, and every route except reading one policy by
reference additionally needs a permission on the realm-configuration path
`/{realm_id}/admin/config`: READ for the reads, WRITE for the writes. The
permission check runs inside the operation and ahead of everything else, so no
route is an oracle: a caller without it is refused before a bucket name, a
policy id or this node's subject state can be inferred from the answer.

Quarantined copies and the drain they cause are described in
[Distributed execution jobs](distributed-jobs.md#quarantined-copies-block-admission),
because they block execution targets as well as data.

## The policy model

A policy is an immutable definition. Its identity is the pair
`(policy_id, digest)`, where the digest covers the canonical bytes of the
definition, and every reference names both: an id alone could be answered with
other bytes. A policy id therefore names one definition forever. Changing a rule
means publishing a new policy and re-pointing the references, never editing the
old one.

`allowed` is a list of selectors. A subject is allowed when **any** selector
matches; inside one selector **every** present field must match. A selector must
constrain at least one attribute: publishing one that constrains nothing is
refused, and one that reached evaluation anyway matches nothing, so a malformed
document can never silently allow every subject.

| Selector field | Matches against |
| --- | --- |
| `node_id` | The exact node the subject belongs to |
| `location` | The node's advertised placement location |
| `labels[]` | Key/value pairs on the subject; every listed pair must be present |
| `executor_kind` | The executor kind of a compute subject |

The definition is bounded so that one evaluation is always cheap and
deterministic: a name of 1 to 128 bytes, 1 to 32 selectors, at most 16 label
matches per selector, label keys up to 128 and values up to 256 bytes, a
location up to 64 bytes and an executor kind up to 32 bytes. One governed record
carries at most 8 references, and one evaluation resolves at most 16 policies.

An empty reference set is ungoverned data and never consults the gate at all.
When references are present, **every** one of them must allow the subject.
Nothing short of that is a grant: an unresolved reference, a digest that does not
match the bytes served, a document that fails validation, and an outright denial
all block the operation. Only the reported reason differs, and an incomplete
evaluation is never sold as a definitive denial.

## The node's own subject

The subject a selector matches comes from this node's entry in the realm
placement map. That entry is seeded once, either when this node initializes a
realm or when it onboards into one, from three environment values. They are
inputs to that one event, not live configuration: a node with persisted state
does not reapply them on a later restart, and changing the entry afterwards is a
placement-map operation.

| Variable | Meaning | Default |
| --- | --- | --- |
| `ARUNA_NODE_LOCATION` | Placement location this node advertises. Trimmed; at most 64 bytes, and a longer value is refused. Empty means unset, and an unset location normalizes to `default` for matching, so a selector naming `default` matches it. | unset |
| `ARUNA_NODE_WEIGHT` | Relative selection weight in the placement map, clamped into 1 to 10000. Weight 0 in the map means the node is never selected. | 100 |
| `ARUNA_NODE_LABELS` | Comma-separated `key=value` label pairs. The derived key `aruna-engine.org/kind` and any `aruna-engine.org/storage-class/` key are rejected: both are stamped by the owning node, so operator input may not claim them. | none |

A compute backend may advertise a different site than the node it runs on; see
[Compute executors](compute-executors.md#kubernetes).

## Publishing and reading a policy

`POST /admin/placement-policies` publishes a definition. Omitting `policy_id`
mints one; supplying one makes a retried publication idempotent. Publishing the
same id with the same selectors returns the stored document unchanged. The same
id with different selectors is `409` and never replaces what is stored.

The document is committed on a holder of the bucket its id resolves to. When
this node holds none, the publication is forwarded to a current holder under the
caller's own token and that holder re-runs the same admin check, so a relay
never becomes the author.

`GET /admin/placement-policies/{policy_id}?digest=…` reads one policy back. The
digest is required. A digest that does not match what the holders serve is `404`
rather than a substituted rule.

`GET /admin/placement-policies?limit&cursor` lists policies in ascending policy
id, `limit` defaulting to 50 and capped at 200. It needs realm-config `READ`, and
a caller without it is refused before anything is read, so the listing is no
existence oracle. A document replicates only to the holders its id resolves to,
so a page reports what this responder stores rather than a realm-wide catalog:
`complete` means this node's own bounded iterator was exhausted in the pass, and
`next_cursor` continues after the last policy returned.

| Status | `POST` | `GET` |
| --- | --- | --- |
| `200` | Published, or the identical document that already existed | The authenticated document |
| `400` | Invalid definition, or an unparsable id, node id or digest | Unparsable id or digest |
| `401` | No bearer token | No bearer token |
| `403` | Token belongs to another realm, or no realm-config WRITE | Token belongs to another realm |
| `404` | n/a | No holder has that id at that digest |
| `409` | The policy id already carries another definition | n/a |
| `503` | No holder could commit; nothing was published | No holder answered, or the publication could not be verified |

Policy documents replicate at a fixed revision generation of `1`. The definition
cannot change, so folding in a later provenance record must not look like a
generation regression to a peer that already holds the rule.

## Trust model

A policy is authoritative only after this node re-derives its authority from its
own replicated realm view. The publication is a signature over the realm id, the
policy id, the digest, the authorizing user, the publication time and the
realm-configuration digest of that moment. Verification requires all of:

- the signature verifies against the named original publisher;
- that publisher is a node this realm's configuration ranks as sync-eligible;
- the authorizing user belongs to this realm and holds WRITE on
  `/{realm_id}/admin/config` in this node's own replicated authorization
  document.

Every step fails closed. A relay that restates a document cannot become its
author, a holder cannot supply authority for someone else, a `DENY` on a
matching permission pattern refuses outright, and a permission pattern that does
not compile denies rather than widening authority.

Two authorized publications of byte-identical definitions converge on the
smaller claim digest, so an unauthenticated timestamp can never take provenance
from the other. A known id arriving with different bytes fails closed as id
reuse.

## The policy cache

Resolved policies are cached node-locally, keyed by `(policy_id, digest)`. A
stored positive entry is bytes, never a trusted document: every lookup verifies
the definition and its publication signature again before the rule may be
matched against a subject. Positive entries need no correctness TTL, because the
definition behind that key can never change.

The cache is bounded at 256 entries, 2 MiB in total and 256 KiB per entry.
Eviction only costs a refetch and can never change what a subject is allowed to
do. A corrupt, foreign or mismatched row is treated as a miss and replaced, never
reported as a denial.

A negative entry records that holders could not supply a document. It is an
availability hint with a 10 second expiry, never a denial, so a rule that becomes
reachable again is used as soon as the hint expires.

`GET /admin/placement-diagnostics` reports cache counts and bytes. They are
diagnostics only and never policy truth.

## Bucket defaults

`GET /buckets/{bucket}/placement` returns the bucket's default reference set and
the generation it was written at. This is a node-local read of the replicated
bucket record, so a default written on another node can be missing here until it
arrives. A bucket that never had a default returns an empty list at its current
generation.

`PUT /buckets/{bucket}/placement` replaces the whole set; an empty list clears
it. Every reference is resolved and authenticated through the ordinary policy
read before it can become a default, so a reference no holder can supply is
`503` and the stored default is untouched. A real change advances the
generation exactly once inside the same transaction; submitting the set that is
already stored commits nothing and returns the current generation, so a replay
cannot supersede a bulk run that sealed the same references. Sending
`expected_generation` makes the change a compare-and-set that is `409` when
another writer moved first.

The default governs versions minted after it. Stored versions keep their own
references until a successor is minted for them.

## Attaching a policy set to one object

`POST /buckets/{bucket}/placement/objects` is an exact replacement, not a union:
the successor carries exactly the submitted references, so an explicit mutation
may tighten or relax. Nothing stored is rewritten. A new version is minted that
carries the new references and the predecessor's bytes, and the predecessor keeps
its own references.

The mutation advances the head only while it is still exactly
`expected_version_id` at `expected_generation` and the bucket is still the same
record; a concurrent write is `409` and the caller replans from the new head.
Repeating the same `mutation_id` with the same parameters returns the version the
first attempt assigned, which is what makes a lost response safe; the same id
with different parameters is `409`.

A materialized object needs a verified local copy of its bytes on a destination
the new references admit. The content hash is what binds the copy, so a
zero-length governed object gets its successor from its own registered copy like
any other. Without a usable copy the response is `outcome: "blocked"` with a
reason and nothing was written:

| `blocked_reason` | Meaning |
| --- | --- |
| `source_unavailable` | No verified local copy of the predecessor's bytes to reuse |
| `destination_denied` | The new references do not admit this node |
| `policy_unresolved` | A referenced policy could not be authenticated here |

A reference-only head mints a successor and registers no copy.

## Applying the default in bulk

`POST /buckets/{bucket}/placement/runs` applies a bucket default to this
responder's current heads. The first call under an `operation_id` seals the run
against the bucket's exact identity, generation and default reference set;
repeating that id resumes the sealed run, and every later pass is bound to what
was sealed.

The application is **additive**: each object's successor carries the union of the
references its head already had and the sealed target, so applying a default
never removes a constraint. Exact replacement is the per-object route above.

One pass walks a bounded page of this responder's own heads and returns a
`cursor` to continue with; the default page is 64. Heads that already carry the
target and delete markers count as covered. A head that moved is `replanned` by a
later pass. An object whose bytes cannot be reused, whose destination the
references deny, or whose policy cannot be authenticated becomes a durable
blocked gap and is retried later rather than reported as done.

`status` distinguishes two very different stops:

| `status` | Meaning |
| --- | --- |
| `active` | The run is resumable. This includes a pass that stopped because this node's own placement subject moved under it: every evaluation that pass made is stale, nothing was committed against a subject nobody authorized, and a later pass resumes the same sealed run. |
| `completed` | The sealed run finished. |
| `superseded` | The bucket default itself moved, so the sealed target no longer describes the bucket. Reserved for that case only, so one run never mixes two policies. |

`complete` means this node's bounded iterator was exhausted, never that another
partition converged.

## Coverage and diagnostics

`GET /buckets/{bucket}/placement/coverage` reports, for a bounded page, how far
this responder's own objects carry the bucket default. It names the exact
default reference set and generation it compared against. Attachment gaps and
local copy state are separate answers: an object can carry every reference and
still have no serveable copy here, so zero gaps never implies that every
registered copy is compliant. `scope=current` (the default) walks current heads;
`scope=historical` reports non-head versions that lack the default, which is
diagnostic only, because minting successors never rewrites immutable references.
Reference-only heads are labelled rather than omitted.

The `limits` list states what the report deliberately does not claim:
`responder_local` and `concurrent_writes` always, `bounded_page` when a cursor
was returned, and `historical_excluded` in the current-heads scope.

`GET /admin/placement-diagnostics` reports this node's own enforcement state:
the placement subject it advertises, whether serving is blocked or the node is
policy-draining, and a bounded page of its registered copies. A copy that is
quarantined or was last seen on a departed node is listed as a violation with the
references it was registered under. A serveable registration is counted but never
listed, and being counted is not by itself a compliance claim. `complete` refers
to this node's bounded copy iterator; `cache_truncated` says the cache scan hit
its own bound.

## Governed S3 operations

The gate runs on the write paths that expose new bytes under references:
`PutObject` and `CopyObject`, `CreateMultipartUpload` and
`CompleteMultipartUpload`, `UploadPartCopy`, staging snapshots, and inbound
replication.

`UploadPartCopy` is gated like `CopyObject`. The governed source's references are
unioned with the references the upload already sealed, and that union is
evaluated **before any part is written**, so no byte of a governed source lands
here before this node is admitted for what the finished object will carry. The
source's references are then sealed on the upload record, so a lost merge cannot
let the completed object drop them.

Inbound replication never trusts the requester. The destination subject is
reconstructed from the authenticated peer plus this realm's own placement of that
peer; the subject the requester asserts is only ever a claim, and echoed
references are never authority.

Between the gate and the transaction that exposes the copy, the destination facts
are re-checked: the bucket identity, its policy generation and reference set, and
the live subject row. A missing subject row, an advanced subject generation, or a
node that entered draining in between all mean the copy would commit references
nothing evaluated, and the write is refused as drift instead.

## What a refused caller learns

A refusal never names a policy, a reference or a node. A public S3 caller must
not be able to read the residency rule out of an error, so denials and unresolved
rules differ only in retryability:

| Surface | Refusal | Status |
| --- | --- | --- |
| S3 | `PlacementPolicyDenied`, fixed message naming only the action | `403` |
| S3 | `PlacementUnavailable`, one stable message for an unregistered, quarantined, drift-affected, admission-stopped or unresolvable copy | `503` |
| Admin | `placement_policy_denied` | `400` |
| Admin | `placement_policy_unavailable` | `503` |
| Admin | `placement_subject_drift`, retryable, nothing was written | `503` |
| Admin | `no_placement_subject` or `placement_admission_stopped` | `503` |

Policy ids appear only in the realm-admin read surfaces above, which already
require realm-configuration permission.
