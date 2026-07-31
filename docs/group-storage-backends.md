# Group storage backends

A group admin can register object stores the group owns and route the group's
writes to them. Data written to such a backend lives on infrastructure Aruna
does not operate: its durability, cost and availability are the group's
responsibility.

## Registering a backend

`POST /groups/{group_id}/storage-backends` with ADMIN on the group:

```json
{
  "name": "lab-minio",
  "kind": "s3",
  "public_config": { "endpoint": "https://minio.lab.example.org", "bucket": "aruna" },
  "secret_config": { "access_key_id": "...", "secret_access_key": "..." }
}
```

Five kinds are supported, each with its own required configuration:

| kind | required public | required secret |
| ---- | --------------- | --------------- |
| `s3` | `endpoint`, `bucket` | `access_key_id`, `secret_access_key` |
| `gcs` | `bucket` | `credential` |
| `azblob` | `endpoint`, `container`, `account_name` | `account_key` or `sas_token` |
| `azdls` | `endpoint`, `filesystem`, `account_name` | `account_key` or `sas_token` |
| `b2` | `bucket`, `bucket_id` | `application_key_id`, `application_key` |

`account_name` is mandatory on both Azure kinds: without it opendal never pushes
the static shared-key provider and signs the request with the node's ambient
Azure identity instead.

Credentials must be static and long-lived. Session tokens are rejected: Aruna
cannot renew them, and a write that outlives the token would
fail mid-stream. Secrets are stored separately from the record, never
returned by the API, and never logged.

Creation probes the endpoint: the node checks the store and writes and deletes
a sentinel key. A backend that fails the probe is not registered. Health is
not probed continuously afterwards.

The endpoint is screened against the egress guard's deny table before any
connection is made, at creation and on every later use. Private, loopback and
link-local addresses are refused. An operator may narrow this further, or
refuse group backends entirely, in which case a rule naming one fails loudly.

## Routing writes to it

A group backend receives data only when a routing rule names it, by backend
id:

```
PUT /buckets/{bucket}/storage-routing
{ "rules": [ { "key_prefix": "archive/", "exact": false,
               "target": { "backend_id": "01J..." } } ] }
```

or as the group default via `PUT /groups/{group_id}/storage-routing`. Rules
apply to new writes only; existing objects stay where they were written.

Aruna uses the container you configured and never creates another one.
In-flight multipart parts live under a reserved `_parts/` prefix inside it,
and hidden internal blobs are never placed on a group backend. Multipart
compose is performed by Aruna, downloading and re-uploading parts, so it works
across every supported provider.

Bytes on a group backend still count against the group's quota in this
release.

## Changing credentials

`PUT /groups/{group_id}/storage-backends/{backend_id}` replaces the stored
credentials and the display name. The backend type and the keys that name the
store, `endpoint`, `bucket` (or `container`, `filesystem`), `account_name` and
`root`, are fixed after create and a request that changes one is refused with
400: stored objects record only the path below `root` and neither the kind nor
the endpoint, so the change would silently redirect them. Register a second
backend to move data.

A disabled backend still accepts this request, so a leaked key can be replaced
without enabling writes again.

## Disabling a backend

`DELETE /groups/{group_id}/storage-backends/{backend_id}` disables the backend.
It does not delete anything: the record and its credentials stay, so objects
already stored there keep being readable and cleanups that were already queued
still reach the store. What stops is writing. Routing no longer chooses the
backend, a rule that names it fails, and any write that had already resolved it
loses its commit. Repeating the request is harmless and answers `204` again.

`POST /groups/{group_id}/storage-backends/{backend_id}/enable` turns writes back
on. `disabled` on the backend record tells you which state it is in.

Credentials can be changed while a backend is disabled, so a leaked key can be
replaced without accepting writes again.

The record and its credentials remain on the node while any stored copy or
queued cleanup still names the backend. Once nothing does, the node deletes the
record and the credentials by itself, on the same schedule as the reclaim sweep.
A backend set to `retain` keeps its stored copies, and so its record, forever;
set `cleanup` to `reclaim` before disabling if you want the node to let go of
both.

## Where your data actually is

Replication does not copy your routing decision. Each node that holds a
replica resolves its own rules, so copies generally land on Aruna-managed
storage even when the origin write went to your own endpoint. This is by
design: it is what keeps the data available when your endpoint is not.

`GET /blobs/locations?bucket={bucket}&path={key}&version_id={ulid}` reports one
version's copies. It needs READ on the object; `version_id` defaults to the
current version.

```json
{
  "bucket": "raw", "key": "archive/run1.tar", "version_id": "01J...",
  "copies": [
    { "node_id": "ae58...", "local": true,  "state": "present",
      "storage": "node-managed", "storage_class": "cold",
      "group_backend_id": null, "group_backend_name": null },
    { "node_id": "b7c2...", "local": false, "state": "present",
      "storage": "group-backend", "storage_class": null,
      "group_backend_id": "01H...", "group_backend_name": "lab-minio" },
    { "node_id": "9f01...", "local": false, "state": "pending",
      "storage": null, "storage_class": null,
      "group_backend_id": null, "group_backend_name": null }
  ],
  "complete": true, "limits": []
}
```

Candidates come from three places: the bucket's configured replication
targets, the queued replication jobs for the version, and the durable holder
index the DHT keeps per content hash. The holder index is what finds a copy on
a node that is no longer a configured target, or whose queue record was already
consumed by a completed replication. Every candidate is then asked directly, so
the reported state is always the answering node's own.

- `present` means that node confirmed it holds the version.
- `pending` means a copy is expected there and has not arrived yet, either
  because the node is a configured replication target or because a
  replication job for the version is queued for it. A node found only through
  the holder index and answering that it does not hold the version is left out
  entirely: it stores the same bytes under some other object, and no copy of
  this one is on its way there. That answer sets `holder-path-unknown` below,
  because the node may hold this version under a path this node cannot name.
- `unreachable` means the node did not answer within the deadline. The
  endpoint never waits on an offline node beyond that deadline, and the local
  entry is always returned.
- `denied` means the node refused to answer because you may not read the
  bucket it would hold that copy under.
- `not-stored` means the version exists there but holds no bytes anywhere: it
  is a delete marker, or a version that only references content held
  elsewhere. Unlike `pending`, no copy is coming.

`denied` is not a bug. A replication target may write into a different bucket,
sometimes owned by a different group, and each node authorizes you against the
bucket it actually holds. READ on the source object is what gets you the
question asked; READ on the destination bucket is what gets it answered. A
caller without it sees the node listed as `denied` and learns nothing else
about it, not even whether a copy exists there. Ask a destination-bucket
reader, or have the destination bucket's admin grant you READ.

`complete` is the honesty flag. It is true only when every node that could
hold a copy was enumerated and asked. When it is false, `limits` names each
reason, and a node absent from `copies` may still hold a copy:

- `queued-scan-truncated`: the queued-replication scan hit its page cap.
- `queued-scan-failed`: that scan failed, so no queued copy is known at all.
- `queued-record-unreadable`: some queued job records would not decode.
- `candidate-cap-reached`: more nodes than one request asks were candidates.
- `holder-lookup-failed`: the holder index could not be queried, so copies
  outside the current configuration and queue are unknown.
- `holder-path-unknown`: a node the holder index named knows no copy under the
  bucket and key it was asked about, so its copy may be recorded under a path
  this node cannot name.

The holder index is refreshed on a TTL, so a copy made moments ago may not be
published yet. `complete` does not promise otherwise: it promises that every
node the three sources named was asked.

The whole request is bounded: the holder-index lookup is given 5 seconds, at
most 64 nodes are asked, no peer is waited on for more than 5 seconds, and the
fan-out as a whole is abandoned after 30 seconds, with any peer not answered by
then reported as `unreachable`. Every deadline closes the stream or query it
gave up on. The local entry is computed first and never waits on a remote node.

Node-managed copies report their storage class, not the operator's backend
name. Group-backend copies report the backend's id and name, because that copy
sits on your infrastructure and its durability is yours to judge.

A copy that lands on a class you did not ask for is not an error: a class your
node does not offer falls through to that node's default storage. If placement
must be exact, name a backend rather than a class, and check this endpoint.
