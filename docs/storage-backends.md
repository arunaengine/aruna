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

[backend.archive]
type = "s3"
endpoint = "https://archive.internal"
class = "archive"
allow_tenants = false

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
claims. Backend names are operator-facing identifiers: they appear in `/info`
and in the doctor's view, never in tenant-facing object records.

## The class table

`class` is the vocabulary tenants route with. Classes are per node: two nodes
that both offer `cold` have made two independent operator decisions, and
nothing in Aruna asserts they mean the same thing. Choose class names with
that in mind.

`allow_tenants = false` reserves a class for operator rules and the node
default; a tenant rule naming that class misses and falls through instead of
binding. `quota_bytes` records the total user-data bytes intended for that
backend across all groups. It is stored and reported, and it is not enforced
in this release.

A class a node does not offer is a preference, not a demand: resolution falls
through to the next rule and finally to the node default, and each
fall-through is logged. There is no request-time error for an unavailable
class, so an operator who needs hard placement must name a backend rather than
a class.

Every class a node registers is published as a derived node label
`aruna-engine.org/storage-class/<class> = "true"`, including classes with
`allow_tenants = false`. The label advertises capability only. Placement
affinity rules can match it to steer objects toward nodes that offer a class;
the labels are read-only and derived, and configuring one by hand is rejected.

## Routing rules

Rules resolve most-specific first: exact key, key prefix (longest first),
bucket, group, node, then the default backend. Operator rules live in the
`[[routing]]` entries of the backends file. Tenants add rules per bucket
(`PUT /buckets/{bucket}/storage-routing`) and one group default
(`PUT /groups/{group_id}/storage-routing`). The response carries advisory
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
`GET /blobs/locations` reports the actual placement of every copy.

## The egress guard

Tenant-supplied endpoints (group backends, staging sources, autoindex fetches)
are screened against a compiled-in deny table covering loopback, link-local,
RFC1918, carrier-grade NAT, documentation, benchmarking, multicast and
reserved ranges, in both IPv4 and IPv6, after unwrapping IPv4-mapped, NAT64
and RFC 8215 translations. The table cannot be widened. `[egress].deny` only
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
- **FTP staging sources keep a preflight-only window.** opendal's FTP service
  does not speak HTTP, so the guarded client cannot cover it. FTP sources are
  screened at creation and again immediately before each operator build; the
  window between that resolution and the library's own connect cannot be
  closed from here. FTP remains read-only staging and is never a write
  backend.
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

Editing a bucket's routing rules while a write to that bucket is in flight
aborts the write with a conflict. This is rare and consistent with the
existing bucket-record semantics: retry the write.
