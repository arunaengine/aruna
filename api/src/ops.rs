//! Dedicated operations listener: liveness, readiness and Prometheus scrape.
//!
//! These endpoints live on their own port (`OPS_SOCKET_ADDRESS`) so `/metrics`
//! is never reachable through the public REST/portal port and so Kubernetes
//! probes hit a container port that is not exposed via Service/Ingress.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::NODE_STATE_KEYSPACE;
use aruna_core::metrics::{MetricsSource, NodeMetrics};
use aruna_operations::driver::DriverContext;
use aruna_operations::queue_lag::probe_outbox_lag;
use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use byteview::ByteView;
use futures_util::future::BoxFuture;
use prometheus_client::metrics::gauge::Gauge;
use serde::Serialize;

/// Upper bound on the readiness storage probe so a wedged effect worker fails
/// readiness quickly instead of blocking on the much longer request timeout.
const STORAGE_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const NODE_STATE_PROBE_KEY: &[u8] = b"node_state";

/// Startup gate for readiness. Flipped once the node has finished bootstrap and
/// its request listeners are bound.
#[derive(Clone, Default)]
pub struct Readiness {
    started: Arc<AtomicBool>,
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
        Arc::new(Self {
            ctx,
            metrics,
            readiness,
        })
    }

    pub fn readiness(&self) -> Readiness {
        self.readiness.clone()
    }

    pub fn metrics(&self) -> Arc<NodeMetrics> {
        self.metrics.clone()
    }
}

pub fn ops_router(state: Arc<OpsState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}

async fn readyz(State(state): State<Arc<OpsState>>) -> Response {
    let startup = if state.readiness.is_started() {
        CheckOutcome::ok()
    } else {
        CheckOutcome::failed("node has not finished startup")
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
    state.metrics.set_node_ready(state.readiness.is_started());
    let body = state.metrics.render().await;
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response()
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
        Self {
            ok: true,
            status: "ok".to_string(),
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
    if ctx.storage_handle.snapshot_metrics().channel_closed {
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
            CheckOutcome::failed(format!("storage read failed: {error}"))
        }
        Ok(other) => CheckOutcome::failed(format!("unexpected storage event: {other:?}")),
        Err(_) => CheckOutcome::failed("storage probe timed out"),
    }
}

async fn check_sync(ctx: &DriverContext) -> CheckOutcome {
    if ctx.net_handle.is_none() {
        return CheckOutcome::failed("document sync node not attached");
    }
    match probe_outbox_lag(&ctx.storage_handle, false).await {
        Ok(_) => CheckOutcome::ok(),
        Err(error) => CheckOutcome::failed(format!("document sync outbox probe failed: {error}")),
    }
}

/// Scrape-time source mirroring the storage handle's internal counters into the
/// registry. Only reads snapshots, so it never sends effects of its own.
struct StorageSource {
    ctx: Arc<DriverContext>,
    requests: Gauge,
    errors: Gauge,
    conflicts: Gauge,
    in_flight: Gauge,
    channel_closed: Gauge,
}

impl MetricsSource for StorageSource {
    fn refresh(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let snapshot = self.ctx.storage_handle.snapshot_metrics();
            self.requests.set(snapshot.requests_total as i64);
            self.errors.set(snapshot.errors_total as i64);
            self.conflicts.set(snapshot.conflicts_total as i64);
            self.channel_closed.set(i64::from(snapshot.channel_closed));
            self.in_flight
                .set(self.ctx.storage_handle.in_flight() as i64);
        })
    }
}

async fn register_storage_source(metrics: &NodeMetrics, ctx: Arc<DriverContext>) {
    let requests: Gauge = Gauge::default();
    metrics
        .register(
            "storage_requests_total",
            "Total storage effect requests dispatched",
            requests.clone(),
        )
        .await;
    let errors: Gauge = Gauge::default();
    metrics
        .register(
            "storage_errors_total",
            "Total storage effect errors observed",
            errors.clone(),
        )
        .await;
    let conflicts: Gauge = Gauge::default();
    metrics
        .register(
            "storage_conflicts_total",
            "Total storage transaction conflicts observed",
            conflicts.clone(),
        )
        .await;
    let in_flight: Gauge = Gauge::default();
    metrics
        .register(
            "storage_effects_in_flight",
            "Storage effects currently enqueued or being processed",
            in_flight.clone(),
        )
        .await;
    let channel_closed: Gauge = Gauge::default();
    metrics
        .register(
            "storage_channel_closed",
            "1 when the storage effect channel has closed",
            channel_closed.clone(),
        )
        .await;

    metrics
        .register_source(Arc::new(StorageSource {
            ctx,
            requests,
            errors,
            conflicts,
            in_flight,
            channel_closed,
        }))
        .await;
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
    async fn healthz_is_always_ok() {
        let (_temp, ctx) = fjall_ctx();
        let ops = OpsState::new(ctx, Arc::new(NodeMetrics::new()), Readiness::new()).await;
        let (status, body) = request(&ops_router(ops), "/healthz").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "ok");
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
        assert!(content_type.starts_with("text/plain"), "{content_type}");
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(body.contains("aruna_build_info{version=\""), "{body}");
        assert!(body.contains("aruna_storage_requests_total"), "{body}");
    }
}
