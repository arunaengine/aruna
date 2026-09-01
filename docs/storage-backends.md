# Storage backends, write routing, and the egress guard

A node can hold several storage backends at once. Every write resolves a
routing rule, records the chosen backend and its storage class on the object's
location, and every later read resolves that recorded backend. Rule changes
affect new writes only: nothing ever moves bytes between backends.

## The backends file

Set `BLOB_BACKENDS_PATH` to a TOML file. Without it a node keeps the implicit
single filesystem backend rooted at `BLOB_ROOT` and named `default`.

```toml
[backend.hot]
type = "filesystem"
root = "/srv/aruna/hot"
multipart_bucket = "aruna-hot-parts"
class = "hot"
default = true

[backend.cold]
type = "s3"
endpoint = "https://s3.example.org"
region = "eu-central-1"
bucket_prefix = "aruna-cold-"
multipart_bucket = "aruna-cold-parts"
class = "cold"
allow_tenants = true
quota_bytes = 20000000000000
cleanup = "reclaim"
reclaim_after_secs = 86400

[backend.archive]
type = "s3"
endpoint = "https://archive.internal"
multipart_bucket = "aruna-archive-parts"
class = "archive"
allow_tenants = false
cleanup = "retain"

[[routing]]
bucket = "raw"
key_prefix = "incoming/"
target = { class = "hot" }

[egress]
serve_group_backends = true
deny = ["203.0.113.0/24"]
```

Only `s3` and `filesystem` are valid `type` values for node backends. Exactly
one backend must carry `default = true`; it receives every write no rule
claims. Every backend must set `multipart_bucket`: it names the container that
holds in-flight multipart parts, and a node whose file omits it refuses to
start rather than accept regular writes and fail every `UploadPart`. Backend
names are operator-facing identifiers: they appear in `/info` and in the
doctor's view, never in tenant-facing object records.

Backend names must also stay distinct after non-alphanumeric characters are
folded to `_` and the result is upper-cased, because that token selects the
`BLOB_BACKEND_<NAME>_ACCESS_KEY_ID` and `..._SECRET_ACCESS_KEY` variables.
`cold-s3` and `cold_s3` collide and the node refuses to start.

## The class table

`class` is the vocabulary tenants route with. Classes are per node: two nodes
that both offer `cold` have made two independent operator decisions, and
nothing in Aruna asserts they mean the same thing. Choose class names with
that in mind.

`allow_tenants = false` reserves a class for operator rules and the node
default; a tenant rule naming that class misses and falls through instead of
binding.

`quota_bytes` caps the total user-data bytes on that backend across all groups.
`/info` reports it next to `used_bytes`, the figure it is measured against.
Enforcement happens where a write picks its backend, and the outcome follows
what the rule asked for: a rule that **names** a full backend fails with
`QuotaExceeded`, because writing elsewhere would hide the exhaustion; a rule
that names a **class** treats a full backend like a class this node does not
offer and falls through to the next rung; and a full **node default** fails,
since nothing is left to fall through to. Fullness is read once per request, so
writes already in flight can carry the backend past its cap by their own bytes,
exactly as the group quota behaves. A counter this node cannot read refuses the
write instead of routing past the cap, and `/info` omits `used_bytes` for that
backend. Hidden blobs and job spool never count.

Inbound replication routes through the same catalog, so a full backend refuses
a transfer it would have to store: the sending node gets the quota as its
rejection reason and reschedules with backoff instead of overshooting the cap.
A node whose **default** backend is full therefore refuses inbound transfers for
every bucket with no class rule. What stores no bytes is unaffected: delete
markers, reference items and a blob the destination already holds still apply
at a full backend. A counter this node cannot read closes the stream before any
reply, and the sender retries.

## Cleanup strategy

`cleanup` decides what happens to bytes on a backend once no version references
them any more. `reclaim` deletes them once `reclaim_after_secs` has passed since
the last reference went away; `retain` keeps them forever.

Node backends default to `reclaim` with a 24 hour grace: the operator pays for
the space, so the node frees it. **An archive, WORM or object-locked tier must
set `cleanup = "retain"` explicitly.** Tenant backends default to `retain` and
only their owner can change that, because tenant storage holds tenant data.

Reclaim is a request, not a guarantee. On a versioned or object-locked bucket a
delete "succeeds" by writing a marker, so nothing is freed, nothing fails, and
no signal exists. Do not point `reclaim` at such a bucket.

The sweep deletes a copy only when no materialized version still names that
exact copy on that exact backend, recounted inside the transaction that frees
it. Physical deletion is queued, retried, and reported per backend by
`aruna-doctor reclaim status`; a failing count that never falls is what "reclaim
is blocked" looks like. `aruna-doctor reclaim seed --backend n:<name>` queues
everything already stored on one backend, which is how garbage that predates a
switch to `reclaim` gets picked up. Run it with the node stopped.

A class a node does not offer is a preference, not a demand: resolution falls
through to the next rule and finally to the node default, and each
fall-through is logged. There is no request-time error for an unavailable
class, so an operator who needs hard placement must name a backend rather than
a class.

Every class a node registers is published as a derived node label
`aruna-engine.org/storage-class/<class> = "true"` on that node's `NodeInfo`
document, including classes with `allow_tenants = false`. These labels are
**capability advertisement only and no part of Aruna consumes them today.**
Placement selection reads its selector labels from the realm placement map, and
that map is a separate, operator-written input that the derived labels are
never copied into; a placement affinity rule matching
`aruna-engine.org/storage-class/*` therefore matches nothing. Setting one by
hand is rejected on every write surface (`ARUNA_NODE_LABELS`, the realm
placement API, the admin document, and node onboarding), precisely so a
placement rule can never be steered by an untrue capability claim.

An operator who wants placement to respect a class today must express it with
an ordinary placement label they own, for example `tier=cold` in
`ARUNA_NODE_LABELS` on the nodes that offer the class, and match that label in
the affinity rule. Keeping the two in step is manual: nothing checks that a
node labelled `tier=cold` actually registers a `cold` backend.

## Routing rules

Rules resolve most-specific first: exact key, key prefix (longest first),
bucket, group, node, then the default backend. Operator rules live in the
`[[routing]]` entries of the backends file. Tenants add rules per bucket
(`PUT /data/buckets/{bucket}/storage/routing`) and one group default
(`PUT /data/groups/{group_id}/storage/routing`). The response carries advisory
warnings, for example when no node currently advertises the requested class.

Ties are rejected when rules are written, never broken at write time. Two
operator rules that are equally specific and can both match one write make the
backends file invalid and the node refuses to start, naming both scopes. Two
tenant rules sharing `exact` and `key_prefix` are rejected by the API.

Routing input is the resource path only. `x-amz-storage-class` on a request is
ignored beyond its pre-existing behaviour.

Replication targets do not inherit the writer's routing: every node resolves
its own rules for the copy it materializes. Tenant data therefore does not
stay exclusively on the endpoint the tenant wrote to, and the copies of one
version can sit on different classes and even on different kinds of storage.
`GET /data/blobs/locations` asks every node that might hold a copy and reports where
each one keeps it. Each copy carries the destination bucket and key it is
stored under, so a node reached under several mapped paths has one entry per
path and its node id repeats. `complete` is false whenever any part of the
answer is missing, and `limits` names what was missing.

## The egress guard

Tenant-supplied endpoints (group backends, staging sources, autoindex fetches)
are screened against a compiled-in deny table covering loopback, link-local,
RFC1918, carrier-grade NAT, documentation, benchmarking, multicast, reserved,
segment-routing and IPv6 transition ranges, in both IPv4 and IPv6, after
unwrapping IPv4-mapped and well-known NAT64 translations. It holds every IANA
special-purpose prefix whose registry entry is not globally reachable,
including the RFC 8215 local-use NAT64 prefix, which is denied whole because
its embedding offset is a local choice the node cannot know. The
table cannot be widened. `[egress].deny` only
appends further CIDRs, and `serve_group_backends = false` makes group-backend
routing targets fail loudly.

Operator-registered node backends are exempt: they may legitimately sit on
private addresses.

Enforcement happens at connect time, in a purpose-built HTTP client whose
resolver filters every answer, including redirect targets and connection-pool
refills, and which carries no proxy configuration and bounded timeouts. The
same client covers credential fetches (instance metadata, OAuth token
endpoints), which is what keeps a tenant endpoint from harvesting the node's
ambient cloud credentials.

### Known limitations

These are real and deliberate; none of them makes the guard a substitute for a
network firewall.

- **Pooled connections outlive a policy change.** An established keep-alive
  connection is not re-screened. The idle timeout bounds the window, and the
  compiled-in table never changes at runtime, but a narrowing added to
  `[egress].deny` takes effect for new connections only.
- **A globally routed address that reaches internal infrastructure is
  invisible.** NAT that maps a public address onto an internal service defeats
  any host-side IP check. Only the operator's firewall can express that.
- **FTP staging sources are refused.** opendal's FTP service does not speak
  HTTP, so the guarded client cannot cover it, and it exposes no way to
  constrain the passive data address: the server picks the host and port the
  node connects to. Registering an `ftp` connector fails with `400`, and any
  stored `ftp` record fails at use. The kind stays readable so existing
  records can still be listed and deleted.
- **Backblaze B2 has no counterfactual zero-connect test.** Every other
  provider kind is pinned by a test asserting zero connections to its
  credential endpoint. B2's endpoint is hardcoded upstream, so no such test
  can be written for it. B2 also has no ambient credential chain to close: its
  credentials are always the ones the tenant supplied, which is why this gap
  was accepted rather than fixed.

## Multipart and reserved namespaces

Two prefixes are reserved inside a backend container: `_jobs` for hidden and
job blobs, and `_parts` for in-flight multipart parts. A tenant bucket named
after either is rejected at write time, so a tenant-written key can never
collide with an in-flight part path.

## Operating notes

Removing a backend from the file does not delete data. Locations that named it
stop resolving and their reads fail loudly with the backend named in
structured logs; re-registering the backend under the same name restores them
with no data work. `aruna-doctor explore locations` lists every location whose
backend no longer resolves, against the parsed backends file and the stored
group-backend records.

Stored formats changed with this feature. Deployments upgrading from an
earlier alpha wipe and redeploy; there is no migration path.

Editing a bucket's routing rules never disturbs a write already in flight. The
bucket guard compares identity only, not mutable configuration, so an admin
edit lands on new writes and leaves running ones alone.
