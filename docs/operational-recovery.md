# Operational recovery runbook

This runbook covers one failure class: **Aruna peers are unavailable**. Hardware,
Kubernetes node, control-plane, and storage-device failures are out of scope.

The contract this runbook assumes:

- An Aruna process stays live and observable while peers are unavailable.
- It serves whenever its local state is safe.
- It performs bounded background recovery.
- It converges after peers return without manufacturing new work on every
  restart.

A node with every peer down is **expected** to be `Ready` and `degraded`. That is
not an incident by itself.

## 1. Endpoints

Everything below is on the ops listener (`OPS_SOCKET_ADDRESS`, `3002` by
default). It is pod-internal and must never be added to the public REST/S3
Service.

| Endpoint | Meaning |
| --- | --- |
| `GET /healthz` (`/health`) | Liveness. Fails **only** when the local storage worker channel is irrecoverably closed. |
| `GET /readyz` (`/ready`) | Readiness plus a structured diagnosis body. |
| `GET /metrics` | Prometheus scrape. |

`/healthz` never touches peers, the DHT, queue depth, or remote I/O. If it fails,
the process is locally broken; peer loss can never cause it.

`/readyz` runs two sequential two-second local probes under a five-second
whole-request deadline. Any client timeout must exceed five seconds; the
reference Kubernetes probe uses six.

## 2. First five commands

```sh
OPS=http://127.0.0.1:3002

curl -sS -m 3  "$OPS/healthz"; echo
curl -sS -m 8  "$OPS/readyz" | jq .
curl -sS -m 8  "$OPS/metrics" | grep -E '^aruna_(recovery|queue)_'
kubectl get pod -l app=aruna -o wide
kubectl get pod <pod> -o jsonpath='{.status.containerStatuses[0].lastState.terminated.exitCode}{"\n"}'
```

Record, in this order:

1. lifecycle state (`/readyz` `checks.startup`) and recovery state
   (`/readyz` `recovery.state`);
2. container restart count;
3. outbox depth **lower bound** and whether it is capped;
4. oldest outbox record age;
5. recovery start/last measurable progress timestamp.

## 3. Reading `/readyz`

```json
{
  "ready": true,
  "checks": {
    "startup": "ok",
    "storage": "ok",
    "sync": "ok: document sync attached; outbox readable"
  },
  "recovery": {
    "state": "degraded",
    "topics_remaining": 12,
    "last_progress_timestamp": 1760000000,
    "last_error_class": "peer_unavailable"
  }
}
```

`topics_remaining` is the current recovery rotation's count of distinct
unresolved topics plus topics in restore units that have not been visited yet.
It is not an outbox-depth value. A non-topic recovery phase can keep the node
`degraded` with a zero count; inspect `last_error_class` as well. The count
reaches zero only after a complete rotation has no unresolved topic or failed
phase.

`recovery.state` is one of:

| State | Meaning | Action |
| --- | --- | --- |
| `pending` | Remote recovery has not started. | Normal for the first seconds after the serving gate. |
| `running` | A bounded recovery pass is active. | Wait. |
| `degraded` | The latest bounded invocation left retryable work or an error. The driver continues an incomplete rotation immediately, or backs off after it has visited the rotation. | Restore peers. Do **not** restart this node. |
| `converged` | A complete current rotation found no unresolved topic or failed recovery phase. | Nothing to do. |

`last_error_class` is a closed set: `peer_unavailable`, `storage`, `panicked`.
`panicked` means the recovery driver itself died; convergence then depends only
on the durable retry timers, so escalate.

The body deliberately carries no peer, topic, or document identifiers. Use the
structured log events in section 5 for detail.

## 4. Metrics

| Series | Use |
| --- | --- |
| `aruna_recovery_state{state=...}` | One-hot; the current lifecycle of remote recovery. |
| `aruna_recovery_topics_remaining` | Distinct unresolved topics plus topics in unvisited restore units in the current rotation; zero is meaningful only after a clean complete rotation. |
| `aruna_recovery_last_progress_timestamp_seconds` | Unix seconds when recovery started or last made measurable progress: a completed work unit, reduced unresolved work, or completed recovery phase. Staleness input for `ArunaRecoveryStalled`. |
| `aruna_recovery_pass_total{outcome=...}` | Completed bounded invocation counts labeled `success`, `partial`, or `failed`. |
| `aruna_queue_depth{queue=...}` | Durable queue depth. |
| `aruna_queue_depth_capped{queue=...}` | **1 means "at least the scan ceiling", not exactly it.** |
| `aruna_queue_oldest_age_seconds{queue=...}` | Convergence SLO input. |
| `aruna_queue_probe_up{queue=...}` | 0 means depth and age are blind, not that the queue is empty. |
| `aruna_queue_probe_last_success_timestamp_seconds{queue=...}` | When the depth view was last true. |

## 5. Logs

Bound the window; do not tail the whole history.

```sh
kubectl logs <pod> --since=15m           | grep -E 'startup\.recovery|pipeline\.drain'
kubectl logs <pod> --since=15m --previous | grep -E 'startup\.recovery|pipeline\.drain'
```

| Event | Meaning |
| --- | --- |
| `startup.recovery.begin` | The tracked recovery driver started, after the serving gate. |
| `startup.recovery.progress` | A shard-restore work unit finished; placement reconciliation also emits a phase progress event. Carries phase and counts where applicable. |
| `startup.recovery.degraded` | A completed rotation left retryable work or a failed recovery phase; carries unresolved `topics_remaining` and `error_class`. |
| `startup.recovery.complete` | Converged. |
| `pipeline.drain.summary` | One bounded outbox invocation: `examined`, `deleted`, `deferred`, and `undeliverable` are record counts; `retry_scheduled`, `has_unvisited`, and `rotation_complete` are booleans; `continuation` and timing/page fields describe the bound. |
| `pipeline.drain.rotation` | One rotation reached its observed high-water boundary; totals across its invocations. `retry_invocations` counts invocations that scheduled retry, not records. |
| `pipeline.drain.stuck` | Records whose buckets this node holds but whose shard genesis never arrived. |
| `pipeline.drain.undeliverable` | Records this node can never publish. Never deleted; needs a placement fix. |

## 6. Distinguishing the two failure shapes

| Symptom | Peer unavailability | Local storage-worker death |
| --- | --- | --- |
| `/healthz` | 200 | 503 `storage worker dead` |
| `/readyz` `checks.storage` | `ok` | `failed: ...` |
| `recovery.last_error_class` | `peer_unavailable` | `storage` |
| `aruna_queue_probe_up` | 1 | 0 |
| Correct action | restore peers | restart / replace the pod |

Peer loss must never be answered with a restart. A restarting node repeats its
recovery work and never gets a stable interval in which to finish it, which is
exactly the incident this design removes.

## 7. Safe and unsafe actions

**Safe**

- Restore peer reachability, then watch `startup.recovery.progress`,
  `aruna_recovery_topics_remaining`, and the queue depth/age gauges fall.
- Leave a `degraded` node in Service. It serves everything that does not need the
  absent peer; operations that do need it fail retryably on their own.
- Scale peers back up one at a time and confirm convergence between steps.

**Never**

- Never delete the document-sync outbox. It holds accepted writes with no replay
  source; deleting it loses acknowledged work permanently.
- Never wipe node state to clear a backlog.
- Never restart a live recovering node merely because peers are unavailable.
- Never treat `aruna_queue_depth` as exact while `aruna_queue_depth_capped` is 1.

## 8. Termination exit codes

| Exit code | Meaning | Action |
| --- | --- | --- |
| `0` | Clean, ordered shutdown inside its grace budget. | None. |
| `75` | The graceful-shutdown watchdog forced exit: the drain exceeded `ARUNA_SHUTDOWN_GRACE_SECS` (default 20 s) plus its margin. | Investigate what did not stop. Check for an in-flight drain or recovery pass at SIGTERM. |
| `143` | A second SIGTERM stopped an active graceful drain immediately. | This is intentional only for an emergency stop; inspect the shutdown logs and let the next start replay durable work. |
| `137` | Kubernetes SIGKILL after `terminationGracePeriodSeconds`. | The pod grace is too short for the application budget, or the process ignored SIGTERM. Ensure pod grace (30 s) exceeds the application budget (20 s) plus the 5 s watchdog margin. |

Check it with:

```sh
kubectl get pod <pod> -o jsonpath='{.status.containerStatuses[0].lastState.terminated.exitCode}{"\n"}'
```

A repeated 75 or any 137 is a shutdown defect, not a peer problem. Exit 143 is
expected only after an operator or supervisor sends a second SIGTERM during the
active drain.

## 9. Alerts

`scripts/observability/aruna-alerts.yml` ships only the four rules whose inputs
this repository's Aruna scrape actually ingests. It is loaded from
`scripts/observability/prometheus.yml` and unit-tested by
`scripts/observability/aruna-alerts.test.yml`:

```sh
PROMETHEUS_IMAGE='prom/prometheus:v3.5.3@sha256:ddc2493835a1509976d5e4e0c94199c4f843ce1f42dd6bcfc8231ba734a93ff7'
promtool() {
  docker run --rm --network none \
    --volume "$PWD/scripts/observability:/rules:ro" \
    --entrypoint /bin/promtool \
    "$PROMETHEUS_IMAGE" "$@"
}
promtool check rules /rules/aruna-alerts.yml
promtool test rules /rules/aruna-alerts.test.yml
```

This uses the Prometheus image pinned by the repository's Compose and CI
configuration; it does not require a host-installed `promtool`.

| Alert | Fires when | First action |
| --- | --- | --- |
| `ArunaRecoveryStalled` | not converged and no measurable progress for more than 10 min | Section 6, then restore peers. |
| `ArunaOutboxCapped` | `queue_depth_capped == 1` for 5 min | Treat depth as a lower bound; check drain summaries. |
| `ArunaOutboxOld` | oldest age > 15 min for 10 min | Convergence SLO breach; check peers and drain summaries. |
| `ArunaQueueProbeDown` | `queue_probe_up == 0` for 5 min | Backlog alerts are blind; check the storage worker. |

Three further alerts from the design are **deployment-stack** rules whose inputs
are Kubernetes or log telemetry, not this scrape. Until that stack is verified to
ingest them, they stay manual checks here:

| Check | Manual command |
| --- | --- |
| `ArunaNoReadyNodes` (zero ready pods for 2 min) | `kubectl get pod -l app=aruna -o wide` |
| `ArunaRestartLoop` (≥2 restarts in 10 min) | `kubectl get pod -l app=aruna -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.containerStatuses[0].restartCount}{"\n"}{end}'` |
| `ArunaForcedShutdown` (exit 75/143 or the watchdog log line) | section 8, plus `kubectl logs <pod> --previous \| grep -i watchdog` |

There is deliberately no app-local forced-exit counter: a forced exit can
terminate before the process persists or exports one, so exit code 75 and the
watchdog stderr line are the alert contract.

## 10. Rollback

Roll the image back if any of these is observed:

- `/healthz` changes with peer availability;
- readiness becomes 200 before the local safety gate;
- distinct revisions disappear from the outbox;
- recovery makes less progress than the predecessor image after peers return;
- shutdown writes after the final storage sync, or exceeds pod grace;
- document-sync conflict or undeliverable counts increase.

```sh
kubectl rollout undo deployment/<aruna-deployment>
kubectl rollout status deployment/<aruna-deployment>
```

Roll one node at a time and require recovery progress plus a stable restart count
before moving to the next.

Rollback never includes deleting the outbox or wiping state. Greenfield wipe
permission does not justify hiding an operational-hardening defect.

## 11. Escalate when

- `/healthz` fails (local storage-worker death);
- `recovery.last_error_class` is `panicked`;
- recovery makes no progress **after** peers are demonstrably reachable again;
- shutdown repeatedly forces exit (75) or is SIGKILLed (137);
- `pipeline.drain.undeliverable` appears: those records need a placement fix, not
  an operator retry.

## 12. Release compatibility

Every peer protocol is negotiated by a versioned ALPN. There is no fallback ALPN
and no downgrade: a peer whose frames differ never negotiates the protocol and
fails the connection instead of decoding foreign bytes.

This release advances three of them:

| Protocol | ALPN | Predecessor now refused |
| --- | --- | --- |
| Blob streaming | `aruna/bao/2` | `aruna/bao/1` |
| Metadata bootstrap | `aruna/metadata/2` | `aruna/metadata/1` |
| Job control | `aruna/job-control/2` | `aruna/job-control/1` |

Document sync additionally carries the event type id `aruna.document.v3`. A peer
that advertises a sync topic with a different event type is refused during
bootstrap rather than synchronized.

**Every node must run the same release.** A node on the previous release cannot
connect to a node on this one in either direction, so during a roll the two
halves behave as a partition: blob replication, metadata bootstrap, job control
and document sync between them all fail closed. That is the intended behaviour,
not a defect, and it is what stops one release from decoding another's frames as
if they were its own.

Consequences for section 10's one-node-at-a-time rule: keep rolling one node at a
time and keep watching health per node, but do not judge convergence until the
last node runs the new release. The same applies to a rollback: rolling one node
back leaves it unable to talk to the rest. Expect `degraded` and outbox backlog
on both halves while a roll is in progress, and expect them to drain once the
fleet is uniform again.

## 13. Startup tunables

These are optional environment values read once at startup. Each of the five
below is rejected when it is zero or not a positive integer, so a typo fails the
start instead of silently removing a bound.

| Variable | Default | What it bounds |
| --- | --- | --- |
| `ONBOARDING_BOOTSTRAP_TIMEOUT_SECS` | `120` | Client-side budget for `POST /api/v1/onboarding/bootstrap`. The seed answers a bootstrap with a realm-config upsert, placement expansion and topic reconciliation inline, so raise this on a large or busy realm. |
| `ONBOARDING_DOCUMENT_SYNC_TIMEOUT_SECS` | `60` | One onboarding document-sync round trip, and the placement wait that repeats it until the seed has granted this node its placement. |
| `S3_INITIAL_REQUEST_TIMEOUT_SECS` | `10` | How long an accepted S3 connection may stay silent before its first request. |
| `S3_CONNECTION_IDLE_TIMEOUT_SECS` | `20` | How long an S3 connection may make no body progress before it is cancelled. Setting it too low truncates slow `GET`s. |
| `S3_STREAM_LIFETIME_TIMEOUT_SECS` | `1800` | Total lifetime of one streamed S3 request. |

`ARUNA_SHUTDOWN_GRACE_SECS` (section 8) is the exception: it does not fail the
start. A value below the 16 second minimum, or one that does not parse, is
logged as invalid and the 20 second default applies, so check the startup log
after changing it.

`ARUNA_FJALL_PERSIST_MODE` is a durability choice rather than a bound. `buffer`
(the default) commits without an fsync per transaction; `sync_all` fsyncs on
every commit and journal persist, which costs write throughput and buys
durability against an unclean host stop. Any other value is a startup error. The
explicit sync at shutdown always uses `sync_all` regardless of this setting.
