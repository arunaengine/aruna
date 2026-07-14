//! Dedicated operations listener: liveness, readiness and Prometheus scrape.
//!
//! These endpoints live on their own port (`OPS_SOCKET_ADDRESS`) so `/metrics`
//! is never reachable through the public REST/portal port and so Kubernetes
//! probes hit a container port that is not exposed via Service/Ingress.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DOCUMENT_SYNC_OUTBOX_KEYSPACE, NODE_STATE_KEYSPACE};
use aruna_core::metrics::NodeMetrics;
use aruna_core::telemetry::QUEUE_LAG_INTERVAL;
use aruna_core::util::unix_timestamp_millis;
use aruna_operations::driver::DriverContext;
use aruna_operations::queue_lag::{QueueLagReporter, QueueLagSnapshot};
use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use byteview::ByteView;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeLabelSet, EncodeMetric};
use prometheus_client::metrics::counter::ConstCounter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::{ConstGauge, Gauge};
use prometheus_client::registry::Unit;
use serde::Serialize;
use tokio::time::MissedTickBehavior;
use tower::ServiceBuilder;
use tower::limit::GlobalConcurrencyLimitLayer;
use tower_http::timeout::TimeoutLayer;

/// Upper bound on the readiness storage probe so a wedged effect worker fails
/// readiness quickly instead of blocking on the much longer request timeout.
const STORAGE_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const SYNC_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const NODE_STATE_PROBE_KEY: &[u8] = b"node_state";

/// Whole-request timeout for the ops listener so a slow or stalled probe caller
/// cannot hold a connection open indefinitely.
const OPS_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// Global cap on concurrent ops requests so an internal actor cannot exhaust FDs
/// or pile storage-probe work onto the production queue.
const OPS_MAX_CONCURRENT_REQUESTS: usize = 32;

const DOCUMENT_SYNC_OUTBOX_QUEUE: &str = "document_sync_outbox";
const METADATA_MATERIALIZATION_QUEUE: &str = "metadata_materialization";
const BLOB_REPLICATION_QUEUE: &str = "blob_replication";
const REFERENCE_METADATA_REFRESH_QUEUE: &str = "reference_metadata_refresh";
const QUEUE_NAMES: [&str; 4] = [
    DOCUMENT_SYNC_OUTBOX_QUEUE,
    METADATA_MATERIALIZATION_QUEUE,
    BLOB_REPLICATION_QUEUE,
    REFERENCE_METADATA_REFRESH_QUEUE,
];

/// Startup and drain gate for readiness. `started` flips once the node has
/// finished bootstrap and its request listeners are bound; `draining` flips on
/// shutdown so `/readyz` sheds traffic before the node drains.
#[derive(Clone, Default)]
pub struct Readiness {
    started: Arc<AtomicBool>,
    draining: Arc<AtomicBool>,
}

impl Readiness {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_ready(&self) {
        self.started.store(true, Ordering::SeqCst);
    }

    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// Marks the node as draining so readiness reports NOT-ready and load
    /// balancers stop routing before graceful shutdown.
    pub fn begin_drain(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }
}

/// Shared state behind the ops router.
pub struct OpsState {
    ctx: Arc<DriverContext>,
    metrics: Arc<NodeMetrics>,
    readiness: Readiness,
}

impl OpsState {
    /// Builds the ops state and registers the built-in scrape-time sources.
    pub async fn new(
        ctx: Arc<DriverContext>,
        metrics: Arc<NodeMetrics>,
        readiness: Readiness,
    ) -> Arc<Self> {
        register_storage_source(&metrics, ctx.clone()).await;
        register_queue_metrics(&metrics, ctx.clone()).await;
        if let Some(net_handle) = &ctx.net_handle {
            net_handle
                .notification_watch_metrics()
                .register(&metrics)
                .await;
        }
        Arc::new(Self {
            ctx,
            metrics,
            readiness,
        })
    }
}

pub fn ops_router(state: Arc<OpsState>) -> Router {
    Router::new()
        .route("/health", get(healthz))
        .route("/healthz", get(healthz))
        .route("/ready", get(readyz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_handler))
        .with_state(state)
        .layer(
            ServiceBuilder::new()
                .layer(TimeoutLayer::with_status_code(
                    StatusCode::REQUEST_TIMEOUT,
                    OPS_REQUEST_TIMEOUT,
                ))
                .layer(GlobalConcurrencyLimitLayer::new(
                    OPS_MAX_CONCURRENT_REQUESTS,
                )),
        )
}

/// Serves the ops router on an already-bound listener until it stops.
pub async fn serve_ops(
    listener: tokio::net::TcpListener,
    state: Arc<OpsState>,
) -> std::io::Result<()> {
    axum::serve(listener, ops_router(state).into_make_service()).await
}

/// Liveness: fails only on the latched, unrecoverable storage-worker death so
/// k8s restarts the pod. A cheap, lock-free read; never blocks or awaits.
async fn healthz(State(state): State<Arc<OpsState>>) -> Response {
    if state.ctx.storage_handle.channel_closed() {
        return (StatusCode::SERVICE_UNAVAILABLE, "storage worker dead").into_response();
    }
    (StatusCode::OK, "ok").into_response()
}

async fn readyz(State(state): State<Arc<OpsState>>) -> Response {
    let startup = if !state.readiness.is_started() {
        CheckOutcome::failed("node has not finished startup")
    } else if state.readiness.is_draining() {
        CheckOutcome::failed("node is draining")
    } else {
        CheckOutcome::ok()
    };
    let storage = check_storage(&state.ctx).await;
    let sync = check_sync(&state.ctx).await;
    let ready = startup.ok && storage.ok && sync.ok;

    let body = ReadinessBody {
        ready,
        checks: ReadinessChecks {
            startup: startup.status,
            storage: storage.status,
            sync: sync.status,
        },
    };
    let code = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(body)).into_response()
}

async fn metrics_handler(State(state): State<Arc<OpsState>>) -> Response {
    state.metrics.set_node_started(state.readiness.is_started());
    let body = state.metrics.render().await;
    (
        [(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

#[derive(Serialize)]
struct ReadinessBody {
    ready: bool,
    checks: ReadinessChecks,
}

#[derive(Serialize)]
struct ReadinessChecks {
    startup: String,
    storage: String,
    sync: String,
}

struct CheckOutcome {
    ok: bool,
    status: String,
}

impl CheckOutcome {
    fn ok() -> Self {
        Self::ok_with("ok")
    }

    fn ok_with(status: impl Into<String>) -> Self {
        Self {
            ok: true,
            status: status.into(),
        }
    }

    fn failed(reason: impl std::fmt::Display) -> Self {
        Self {
            ok: false,
            status: format!("failed: {reason}"),
        }
    }
}

async fn check_storage(ctx: &DriverContext) -> CheckOutcome {
    if ctx.storage_handle.channel_closed() {
        return CheckOutcome::failed("storage effect channel closed");
    }
    let probe = ctx.storage_handle.send_storage_effect(StorageEffect::Read {
        key_space: NODE_STATE_KEYSPACE.to_string(),
        key: ByteView::from(NODE_STATE_PROBE_KEY),
        txn_id: None,
    });
    match tokio::time::timeout(STORAGE_PROBE_TIMEOUT, probe).await {
        Ok(Event::Storage(StorageEvent::ReadResult { .. })) => CheckOutcome::ok(),
        Ok(Event::Storage(StorageEvent::Error { error })) => {
            tracing::warn!(%error, "readiness storage probe failed");
            CheckOutcome::failed("storage read failed")
        }
        Ok(other) => {
            tracing::warn!(?other, "readiness storage probe returned unexpected event");
            CheckOutcome::failed("unexpected storage event")
        }
        Err(_) => CheckOutcome::failed("storage probe timed out"),
    }
}

async fn check_sync(ctx: &DriverContext) -> CheckOutcome {
    if ctx.net_handle.is_none() {
        return CheckOutcome::failed("document sync node not attached");
    }
    let probe = ctx.storage_handle.send_storage_effect(StorageEffect::Iter {
        key_space: DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
        prefix: None,
        start: None,
        limit: 1,
        txn_id: None,
    });
    match tokio::time::timeout(SYNC_PROBE_TIMEOUT, probe).await {
        Ok(Event::Storage(StorageEvent::IterResult { .. })) => {
            CheckOutcome::ok_with("ok: document sync attached; outbox readable")
        }
        Ok(Event::Storage(StorageEvent::Error { error })) => {
            tracing::warn!(%error, "readiness document sync probe failed");
            CheckOutcome::failed("document sync outbox probe failed")
        }
        Ok(other) => {
            tracing::warn!(
                ?other,
                "readiness document sync probe returned unexpected event"
            );
            CheckOutcome::failed("unexpected document sync event")
        }
        Err(_) => CheckOutcome::failed("document sync outbox probe timed out"),
    }
}

/// Scrape-time collector mirroring the storage handle's internal counters into
/// the registry. Reads snapshots synchronously, so it never sends effects of its
/// own; the monotonic totals are exported as counters, not gauges.
#[derive(Debug)]
struct StorageCollector {
    ctx: Arc<DriverContext>,
}

impl Collector for StorageCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let snapshot = self.ctx.storage_handle.snapshot_metrics();

        for (name, help, value) in [
            (
                "storage_requests",
                "Total storage effect requests dispatched",
                snapshot.requests_total,
            ),
            (
                "storage_errors",
                "Total storage effect errors observed",
                snapshot.errors_total,
            ),
            (
                "storage_conflicts",
                "Total storage transaction conflicts observed",
                snapshot.conflicts_total,
            ),
        ] {
            let counter = ConstCounter::new(value);
            let metric_encoder =
                encoder.encode_descriptor(name, help, None, counter.metric_type())?;
            counter.encode(metric_encoder)?;
        }

        let in_flight = ConstGauge::new(self.ctx.storage_handle.in_flight() as i64);
        let metric_encoder = encoder.encode_descriptor(
            "storage_effects_in_flight",
            "Storage effects currently enqueued or being processed",
            None,
            in_flight.metric_type(),
        )?;
        in_flight.encode(metric_encoder)?;

        let channel_closed = ConstGauge::new(i64::from(snapshot.channel_closed));
        let metric_encoder = encoder.encode_descriptor(
            "storage_channel_closed",
            "1 when the storage effect channel has closed",
            None,
            channel_closed.metric_type(),
        )?;
        channel_closed.encode(metric_encoder)?;

        Ok(())
    }
}

async fn register_storage_source(metrics: &NodeMetrics, ctx: Arc<DriverContext>) {
    metrics.register_collector(StorageCollector { ctx }).await;
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct QueueLabels {
    queue: &'static str,
}

/// Background-updated depth and oldest-record age gauges for durable work
/// queues, reusing the same probes as the periodic `queue.lag` monitor.
struct QueueMetrics {
    depth: Family<QueueLabels, Gauge>,
    oldest_age_seconds: Family<QueueLabels, Gauge<f64, AtomicU64>>,
    depth_capped: Family<QueueLabels, Gauge>,
    probe_up: Family<QueueLabels, Gauge>,
    probe_last_success_timestamp_seconds: Family<QueueLabels, Gauge>,
}

impl QueueMetrics {
    fn seed(&self) {
        for queue in QUEUE_NAMES {
            let labels = QueueLabels { queue };
            self.depth.get_or_create(&labels).set(0);
            self.oldest_age_seconds.get_or_create(&labels).set(0.0);
            self.depth_capped.get_or_create(&labels).set(0);
            self.probe_up.get_or_create(&labels).set(0);
            self.probe_last_success_timestamp_seconds
                .get_or_create(&labels)
                .set(0);
        }
    }

    fn apply(&self, queue: &'static str, probe: Result<QueueLagSnapshot, String>) {
        let labels = QueueLabels { queue };
        let Ok(snapshot) = probe else {
            self.probe_up.get_or_create(&labels).set(0);
            return;
        };
        self.depth.get_or_create(&labels).set(snapshot.depth as i64);
        self.oldest_age_seconds
            .get_or_create(&labels)
            .set(snapshot.oldest_age_ms as f64 / 1_000.0);
        self.depth_capped
            .get_or_create(&labels)
            .set(i64::from(snapshot.depth_capped));
        self.probe_up.get_or_create(&labels).set(1);
        self.probe_last_success_timestamp_seconds
            .get_or_create(&labels)
            .set((unix_timestamp_millis() / 1_000) as i64);
    }

    async fn refresh(&self, ctx: &DriverContext, reporter: &mut QueueLagReporter) {
        let sample = reporter.sample(&ctx.storage_handle).await;
        self.apply(DOCUMENT_SYNC_OUTBOX_QUEUE, sample.document_sync_outbox);
        self.apply(
            METADATA_MATERIALIZATION_QUEUE,
            sample.metadata_materialization,
        );
        self.apply(BLOB_REPLICATION_QUEUE, sample.blob_replication);
        self.apply(
            REFERENCE_METADATA_REFRESH_QUEUE,
            sample.reference_metadata_refresh,
        );
    }
}

fn spawn_queue_metrics_refresher(ctx: Weak<DriverContext>, queue_metrics: Arc<QueueMetrics>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(QUEUE_LAG_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut reporter = QueueLagReporter::default();
        loop {
            interval.tick().await;
            let Some(ctx) = ctx.upgrade() else {
                return;
            };
            queue_metrics.refresh(ctx.as_ref(), &mut reporter).await;
        }
    });
}

async fn register_queue_metrics(metrics: &NodeMetrics, ctx: Arc<DriverContext>) {
    let depth = Family::<QueueLabels, Gauge>::default();
    metrics
        .register("queue_depth", "Durable work queue depth", depth.clone())
        .await;
    let oldest_age_seconds = Family::<QueueLabels, Gauge<f64, AtomicU64>>::default();
    metrics
        .register_with_unit(
            "queue_oldest_age",
            "Age of the oldest record in a durable work queue",
            Unit::Seconds,
            oldest_age_seconds.clone(),
        )
        .await;
    let depth_capped = Family::<QueueLabels, Gauge>::default();
    metrics
        .register(
            "queue_depth_capped",
            "1 when a queue depth probe stopped at its page cap",
            depth_capped.clone(),
        )
        .await;
    let probe_up = Family::<QueueLabels, Gauge>::default();
    metrics
        .register(
            "queue_probe_up",
            "1 when the latest durable queue probe succeeded",
            probe_up.clone(),
        )
        .await;
    let probe_last_success_timestamp_seconds = Family::<QueueLabels, Gauge>::default();
    metrics
        .register(
            "queue_probe_last_success_timestamp_seconds",
            "Unix timestamp of the latest successful durable queue probe",
            probe_last_success_timestamp_seconds.clone(),
        )
        .await;

    let queue_metrics = Arc::new(QueueMetrics {
        depth,
        oldest_age_seconds,
        depth_capped,
        probe_up,
        probe_last_success_timestamp_seconds,
    });
    queue_metrics.seed();
    spawn_queue_metrics_refresher(Arc::downgrade(&ctx), queue_metrics);
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_storage::{FjallStorage, StorageHandle};
    use axum::body::{Body, to_bytes};
    use http::Request;
    use tower::ServiceExt;

    fn ctx_with_storage(storage: StorageHandle) -> Arc<DriverContext> {
        Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        })
    }

    fn fjall_ctx() -> (tempfile::TempDir, Arc<DriverContext>) {
        let temp = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        (temp, ctx_with_storage(storage))
    }

    async fn request(router: &Router, path: &str) -> (StatusCode, String) {
        let response = router
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn healthz_ok_live() {
        let (_temp, ctx) = fjall_ctx();
        let ops = OpsState::new(ctx, Arc::new(NodeMetrics::new()), Readiness::new()).await;
        let (status, body) = request(&ops_router(ops), "/healthz").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "ok");
    }

    #[tokio::test]
    async fn healthz_fails_dead() {
        // A dropped receiver latches the storage channel-closed flag once an
        // effect is dispatched; liveness must then fail so k8s restarts the pod.
        let (storage, receiver) = StorageHandle::new();
        drop(receiver);
        let _ = storage
            .send_storage_effect(StorageEffect::Read {
                key_space: NODE_STATE_KEYSPACE.to_string(),
                key: ByteView::from(NODE_STATE_PROBE_KEY),
                txn_id: None,
            })
            .await;
        assert!(storage.channel_closed());
        let ops = OpsState::new(
            ctx_with_storage(storage),
            Arc::new(NodeMetrics::new()),
            Readiness::new(),
        )
        .await;
        let (status, _body) = request(&ops_router(ops), "/healthz").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn readyz_gates_on_startup() {
        let (_temp, ctx) = fjall_ctx();
        let readiness = Readiness::new();
        let ops = OpsState::new(ctx, Arc::new(NodeMetrics::new()), readiness.clone()).await;
        let router = ops_router(ops);

        let (status, body) = request(&router, "/readyz").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("\"startup\":\"failed"), "{body}");

        readiness.set_ready();
        let (status, body) = request(&router, "/readyz").await;
        // Sync still fails without an attached net handle, but startup is now ok.
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("\"startup\":\"ok\""), "{body}");
    }

    #[tokio::test]
    async fn readyz_reports_storage_failure() {
        let (storage, receiver) = StorageHandle::new();
        drop(receiver);
        let readiness = Readiness::new();
        readiness.set_ready();
        let ops = OpsState::new(
            ctx_with_storage(storage),
            Arc::new(NodeMetrics::new()),
            readiness,
        )
        .await;
        let (status, body) = request(&ops_router(ops), "/readyz").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("\"storage\":\"failed"), "{body}");
    }

    #[tokio::test]
    async fn readyz_reports_draining() {
        let (_temp, ctx) = fjall_ctx();
        let readiness = Readiness::new();
        readiness.set_ready();
        let ops = OpsState::new(ctx, Arc::new(NodeMetrics::new()), readiness.clone()).await;
        let router = ops_router(ops);
        readiness.begin_drain();
        let (status, body) = request(&router, "/readyz").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("node is draining"), "{body}");
    }

    #[test]
    fn queue_metrics_probe_failure_sets_up_zero_without_clearing_depth() {
        let queue_metrics = QueueMetrics {
            depth: Family::<QueueLabels, Gauge>::default(),
            oldest_age_seconds: Family::<QueueLabels, Gauge<f64, AtomicU64>>::default(),
            depth_capped: Family::<QueueLabels, Gauge>::default(),
            probe_up: Family::<QueueLabels, Gauge>::default(),
            probe_last_success_timestamp_seconds: Family::<QueueLabels, Gauge>::default(),
        };
        queue_metrics.seed();

        let labels = QueueLabels {
            queue: DOCUMENT_SYNC_OUTBOX_QUEUE,
        };
        assert_eq!(queue_metrics.probe_up.get_or_create(&labels).get(), 0);

        queue_metrics.apply(
            DOCUMENT_SYNC_OUTBOX_QUEUE,
            Ok(QueueLagSnapshot {
                depth: 7,
                depth_capped: true,
                oldest_age_ms: 2_500,
                due: 0,
            }),
        );
        assert_eq!(queue_metrics.depth.get_or_create(&labels).get(), 7);
        assert_eq!(queue_metrics.depth_capped.get_or_create(&labels).get(), 1);
        assert_eq!(queue_metrics.probe_up.get_or_create(&labels).get(), 1);
        let last_success = queue_metrics
            .probe_last_success_timestamp_seconds
            .get_or_create(&labels)
            .get();
        assert!(last_success > 0);

        queue_metrics.apply(
            DOCUMENT_SYNC_OUTBOX_QUEUE,
            Err("storage closed".to_string()),
        );
        assert_eq!(queue_metrics.depth.get_or_create(&labels).get(), 7);
        assert_eq!(queue_metrics.depth_capped.get_or_create(&labels).get(), 1);
        assert_eq!(queue_metrics.probe_up.get_or_create(&labels).get(), 0);
        assert_eq!(
            queue_metrics
                .probe_last_success_timestamp_seconds
                .get_or_create(&labels)
                .get(),
            last_success
        );
    }

    #[tokio::test]
    async fn metrics_endpoint_exposes_build_info_and_storage() {
        let (_temp, ctx) = fjall_ctx();
        let ops = OpsState::new(ctx, Arc::new(NodeMetrics::new()), Readiness::new()).await;
        let response = ops_router(ops)
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(
            content_type.starts_with("application/openmetrics-text"),
            "{content_type}"
        );
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(body.contains("aruna_build_info{version=\""), "{body}");
        assert!(body.contains("aruna_node_started 0"), "{body}");
        assert!(body.contains("aruna_storage_requests_total "), "{body}");
        assert!(
            body.contains("# TYPE aruna_storage_requests counter"),
            "{body}"
        );
        assert!(
            body.contains("aruna_queue_depth{queue=\"document_sync_outbox\"}"),
            "{body}"
        );
        assert!(body.contains("aruna_queue_oldest_age_seconds"), "{body}");
        assert!(
            body.contains("aruna_queue_probe_up{queue=\"document_sync_outbox\"}"),
            "{body}"
        );
    }
}
