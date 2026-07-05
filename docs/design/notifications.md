# Notification subsystem

This document is the design reference for the in-app notification subsystem
(GitHub issue #251). It describes the shipped implementation, the invariants it
upholds, and the deliberate trade-offs behind them.

## 1. Overview & goals

Aruna needs one shared, human-facing event primitive: a way for a mutating
operation to tell a user "something you care about happened" without inventing a
bespoke delivery path each time. The subsystem provides exactly that and nothing
more.

Design goals, stated as hard constraints:

- **One primitive.** Every user-visible event flows through a single typed
  `ResourceEvent` and a single `NotificationRecord` storage model. New event
  sources add inputs, not new pipelines.
- **Nothing enforces through it.** Notifications are an observation channel, not
  an authorization or convergence mechanism. No operation waits on a
  notification, and no invariant depends on one being delivered.
- **A lost notification never fails the emitting operation.** Emission is best
  effort and strictly post-commit. The host mutation has already succeeded by
  the time any notification work runs; that work can only warn, never abort.
- **The portal stays topology-blind.** A user's client talks to whatever node it
  is connected to. That node resolves the user's inbox holder and either serves
  locally or proxies exactly one hop. Clients never learn or care which node
  physically holds an inbox.

## 2. The four planes

The subsystem is layered into four planes with narrow interfaces, so each can be
reasoned about (and later replaced) in isolation.

### Event plane (origin node, ephemeral)

A mutating operation emits a typed `ResourceEvent`
(`GroupMemberAdded`, `GroupMemberRemoved`, `NodeOnboarded`) once its host
transaction has committed. Events are best effort and never persisted raw: they
exist only long enough to be routed into records.

### Routing plane (origin node, static)

`route_resource_event` maps one event to zero or more recipient-addressed
`NotificationRecord`s. Routing is a pure function of the event plus the relevant
authorization documents (`RoutingContext`):

- Group admins are the users assigned to any role named `admin` in the group's
  authorization document.
- Realm admins are the users assigned to any role named `realm_admin` in the
  realm authorization document.
- Affected users (the member added or removed) are addressed directly.
- **No self-notifications:** the actor never receives a record for its own
  action, and an admin who is also the affected user is not doubly notified.

Recipient order is deterministic and each recipient gets one record with its own
freshly minted `notification_id`.

Watch/subscription routing (#304) slots in here as new `ResourceEvent` inputs
producing `Transient` records. Because a record already carries its `class`,
#304 changes routing only — no storage or transport schema change.

### Inbox plane (holder node)

Every user has **exactly one** inbox holder, resolved by `resolve_inbox_holder`.
The resolver runs the same rendezvous selection as document sync
(`select_topic_peers`, `SELECTOR_DOMAIN` `aruna-sync-rendezvous-v2`) over the
realm's `sync_eligible_node_ids()`, but on a dedicated per-user topic:

```
blake3("aruna-notification-inbox-v1" ++ user.to_storage_key())
```

The replication factor is **R = 1**: one holder per user. The resolver is the
single point that maps a user to a holder; the placement-map epic (#261/#264)
swaps its candidate input without touching any caller, and #264/#265 can raise
R above 1 later.

### Access plane (any node)

REST endpoints resolve the caller's holder and either serve the request locally
(holder is this node) or proxy it exactly one hop over the notification RPC. The
proxy is a single hop by construction — the holder is authoritative, so there is
never a chain.

## 3. Key layouts

All keys are `ByteView`. A `UserId` storage key is 48 bytes: realm 32B ++ user
ulid 16B. Three keyspaces back the subsystem.

### Inbox primary — `notification_inbox`, 72 bytes

```
[ 0..48)  recipient UserId storage key   (realm 32B ++ user ulid 16B)
[48..56)  inverted created_at_ms         (u64::MAX - created_at_ms, big-endian)
[56..72)  notification_id                (Ulid::to_bytes(), 16B)
```

Value: postcard `NotificationRecord`. An ascending lexicographic scan of the 48B
recipient prefix yields that user's records **newest-first** (the timestamp is
inverted), tie-broken by id. The recipient prefix comes first, so every scan is
strictly isolated to one user — there is no key shape that reads across users.

The **cursor** is the 24-byte suffix `[48..72)` (inverted timestamp ++ id). The
recipient prefix is never part of a cursor, so a caller cannot smuggle another
user's prefix into a paginated request.

### Prune index — `notification_inbox_prune_index`, 72 bytes

```
[ 0..8)   expires_at_ms                  (big-endian, NOT inverted: oldest first)
[ 8..56)  recipient UserId storage key   (48B)
[56..72)  notification_id                (16B)
```

Value: empty. Expiry comes first and is not inverted, so an ascending scan walks
records in expiry order — the prune task reads from the front and stops at the
first not-yet-expired row. Read-state changes never touch this index because
expiry depends only on `created_at_ms` and `class`.

### Outbox — `notification_outbox`, 16 bytes

```
[0..16)   outbox_id (Ulid::to_bytes())
```

ULIDs are time-ordered, so a plain ascending scan is FIFO. The holder is
deliberately **not** part of the key or value — see §4.

## 4. Transport & idempotency

Delivery is decoupled from emission by a durable, per-origin-node outbox.

- **Durability.** Emission writes an outbox row; a drain task delivers it. Rows
  are retained for **48h**. A failed delivery reschedules the drain for **30s**
  later. Outbox timers are restored at startup by scanning the outbox, so a
  crash does not strand undelivered rows. A row older than the 48h retention is
  dropped with a `warn!`.
- **Fair paged drain.** Each drain run pages the FIFO in full (batch 512),
  delivering to each holder at most once per page and, once a holder's delivery
  has failed this run, marking its remaining records for retry without another
  RPC. Because the scan covers the whole queue every run and pages **past**
  retry-marked rows, a single dead holder cannot starve deliveries to healthy
  holders behind it: it costs one timeout per run, not one per record, however
  many of its records are queued ahead.
- **Holder re-resolved every drain.** The outbox row stores the record, never
  its holder. `resolve_inbox_holder` runs on every drain, so deliveries
  automatically re-rank when the sync-eligible set changes (see §8).
- **Dedicated RPC.** Remote delivery uses the ALPN `aruna/notification/1`,
  a request/response exchange over one bidirectional stream. Delivery to self
  short-circuits to a local upsert with no network hop.
- **Idempotent upsert.** The holder-local write is write-if-absent, keyed by
  `(recipient, created_at_ms, notification_id)`. The existence check runs
  **inside** the write transaction (fjall optimistic read-set): a conflicting
  concurrent write — for example a mark-read landing between the check and the
  write — aborts the commit instead of clobbering read state. A duplicate
  delivery of the same record is a no-op. On a transaction conflict the whole
  read-filter-write sequence retries once; a second conflict returns an error
  and the drain redelivers later (safe, because the upsert is idempotent).
  Mark-read is likewise idempotent.

**Dedup scope.** Dedup is **per record**, keyed by `notification_id`. A
host-level retry that re-runs routing mints *fresh* ids and is deliberately not
collapsed. Instead, the emitters gate on the underlying transition: group
membership emitters fire only on an actual membership change, so a retried
mutation that is a no-op emits nothing. The one accepted exception is the rare,
operator-driven onboarding-finalize retry, which may re-notify.

## 5. Emission decoupling (hard invariant)

Outbox rows are written in a **separate, small transaction** by a post-commit
suboperation, `EmitNotificationsOperation`, whose associated `Error` type is
`Infallible`. Every failure inside it — serialization, storage, timer scheduling
— is a `tracing::warn!` followed by a transition to `Finish` with `Ok`. The
operation cannot, by construction, fail its host.

**Rejected alternative.** Writing notification outbox rows *inside the host
transaction*, the way the document-sync admin outbox does (see the `BatchWrite`
in `add_user_to_group.rs`), was rejected. Document sync needs write-atomicity
with the host mutation for convergence; notifications are best effort and must
never abort or roll back the host transaction. Sharing the host transaction
would couple the two.

**Latency, stated honestly.** The host *transaction* carries no fan-out work —
no shared locks, no abort coupling. But the emit suboperation still runs inline
on the host operation's critical path, before the host operation's `Finish`. A
stalled emit path therefore *delays* the host response; it never *fails* it.

**Accepted consequences:**

- A crash in the window between the host commit and the emit transaction loses
  that notification.
- Emission for a member-add hangs off the post-commit topic-announcement success
  path. If that announcement fails, the host mutation is already committed but
  the operation returns an error and emits nothing.

## 6. Read model

The read model is intentionally counter-free — the only maintained state is the
record itself.

- **Cursor pagination.** Newest-first, opaque 24-byte suffix cursor,
  `limit + 1` lookahead to compute the next cursor. Limit is clamped to
  `1..=200`.
- **Bounded unread count.** The unread count pages the recipient prefix counting
  `read_at_ms == None`, stopping at a cap of **100** (reported with a `capped`
  flag) and bounding scan *work* at 2000 rows per call. There is deliberately
  **no maintained counter**: a counter would be a write-amplifying invariant to
  keep correct, and the portal only needs to render "99+". A capped or
  work-bounded result is an honest lower bound.
- **Mark-read.** By explicit ids and/or by an up-to timestamp (**inclusive** of
  `created_at_ms == up_to_ms`), stamping `read_at_ms = now`. Already-read
  records and unknown ids are silently skipped (idempotent). Every read-model
  access is strictly scoped to the authenticated user via
  `notification_inbox_prefix(recipient)`.

## 7. Retention & pruning

Two retention rules apply, enforced by the `TaskKey::PruneNotifications` task:

- **Direct** records live **90 days**.
- **Transient** records live **30 days**, plus a **per-user cap of 500**
  (newest survive). The cap is enforced by the sweep, so a user can transiently
  exceed 500 between hourly runs.

The prune task has two phases:

- **Phase A (TTL).** Walk the expiry-ordered prune index from the front. For
  every row with `expires_at_ms <= now`, delete the index row and its primary
  row, stopping at the first not-yet-expired row. Because the two class TTLs
  differ, phase A reconstructs the primary key by deriving
  `created_at_ms = expires_at_ms - class.ttl_ms()` for each class and keeping
  whichever primary row exists.
- **Phase B (cap sweep + expiry backstop).** Sequentially page the *primary*
  keyspace in recipient order (which is newest-first per user). After 500
  transient records for a recipient, delete every further transient record.
  Direct records are never cap-pruned. As a backstop, any record of either
  class whose `expires_at_ms() <= now` is also deleted here — this reclaims a
  primary row whose index row was lost to the mark-read/prune race (mark-read
  rewrites the primary non-transactionally; if that races a phase-A deletion the
  index row can vanish while the primary lingers). Phase B walks the primary
  keyspace directly, so no record outlives its TTL by more than one prune cycle.

Prune timers are restored from durable state at startup and re-armed by the
periodic re-arm loop using `ShortenTimer`, which never *extends* a pending
deadline — so the task is exempt from timer persistence. The re-arm is always
clamped to the hourly poll interval so phase B's cap sweep runs even when every
TTL expiry is weeks away.

**Forward constraint.** Phase A reconstructs primary keys from the index expiry
*because* the two class TTLs differ. If a future class ever shares a TTL with an
existing one, the index value must start carrying the class (or the primary-key
suffix) to disambiguate.

## 8. Failure semantics

- **Holder briefly down.** The outbox retries the delivery every 30s. History is
  preserved once the holder returns.
- **Holder permanently dead.** That inbox's TTL-bounded history is lost. New
  deliveries re-rank to the next rendezvous winner **only once the dead node is
  removed from the realm config** — the resolver ranks over the *configured*
  sync-eligible set, so until an operator removes the node, deliveries to its
  users keep retrying and are dropped at the ~48h retention bound with a warning.
  Best effort stays best effort.
- **Scale-out.** Adding a sync-eligible node re-ranks the topic too: roughly
  `1/N` of users move to the new holder. Their existing records remain on the
  old (healthy, never-queried-again) holder until TTL. From a user's point of
  view, scale-out looks like losing notification history. This is accepted now
  and is consistent with the durability stance of #283; the placement-map
  upgrade (#261/#264) must address it via history migration or double-read.

**Upgrade path.** The placement-map epic replaces the resolver's candidate input
*inside* `resolve_inbox_holder` (a single function), and #264/#265 can raise R
above 1.

## 9. Kinds & categories (append-only rule)

`NotificationKind` is a **postcard enum**: postcard encodes the variant *index*
as the wire and storage format. Therefore existing variants are **never**
removed, reordered, or field-changed; new variants append only at the end. The
same append-only discipline applies to `NotificationClass` and to the transport
message enum.

Each kind maps to a stable `category()` string (frozen, feeding the per-kind
preferences of #288) and a stable machine-readable `name()`.

| Kind | `name()` | `category()` |
|---|---|---|
| `AddedToGroup` | `added_to_group` | `group.membership` |
| `RemovedFromGroup` | `removed_from_group` | `group.membership` |
| `GroupMemberAdded` | `group_member_added` | `group.membership` |
| `NodeOnboarded` | `node_onboarded` | `node.onboarding` |

Each kind also carries the ids the portal deep-links from:
`AddedToGroup` and `RemovedFromGroup` carry `group_id` and `actor_user_id`;
`GroupMemberAdded` adds `member_user_id`; `NodeOnboarded` carries `realm_id` and
`node_id`. An existing `category()` mapping must never change: preferences key
off the string.

## 10. Trust model of the RPC

The notification RPC accepts a request only from a peer that is a
**sync-eligible node** of the recipient's realm — the message names a realm via
`recipient.realm_id`, and the handler requires both that this equals the local
`realm_id()` and that the peer is in that realm's `sync_eligible_node_ids()`.

This gate is **deliberately stricter** than the metadata RPC's `has_node` check.
The recipient user id in a message is *peer-asserted*: the proxying node
authenticated the user's bearer token and vouches for the id. Only server-class
realm nodes — which already replicate the admin documents and are mutually
trusted for user data — may make that assertion. `RealmNodeKind::User` device
nodes are owner-bound laptops and must not be able to read, mark read, or forge
notifications for arbitrary users, so they are excluded even though the metadata
gate would admit them.

A `DeliverBatch` with an empty `records` vector is rejected **first**, before any
`records[0]` access, because a remote peer controls the message shape. Non-empty
batches authorize against `records[0].recipient.realm_id`, and
heterogeneous-realm batches are rejected.

**Recorded alternative.** Forwarding the user's bearer token to the holder for
re-validation (as the metadata RPC optionally does) was deferred. It is worth
revisiting only if realms ever federate *untrusted* server nodes; today all
server-class realm nodes are mutually trusted.

## 11. Observability

Every drop, skip, and retry path in the outbox drain and prune sweep emits a
structured `tracing::warn!` with the relevant fields (`recipient`, `holder`,
`outbox_id`, `realm_id`, `error`), so an operator can see exactly why a record
was dropped, why a holder is being retried, or why a config could not be read.
The drain and prune reschedule quietly on the success path and log only when
something is dropped or retried.

The sibling pipelines this subsystem is modeled on — the document-sync outbox
drain (`event = pipeline.drain.summary`) and the metadata prune queue
(`event = pipeline.metadata_prune.summary`) — additionally emit a per-run `info!`
summary line; those events are the model for any future per-run summary
instrumentation of the notification drain and prune.
