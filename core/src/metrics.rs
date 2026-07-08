//! Per-node Prometheus metrics registry and its extension point.
//!
//! Each node owns one [`NodeMetrics`]. The registry is instance-scoped rather
//! than process-global so the integration harness can run several full nodes in
//! one process without their counters merging. In-flight feature branches attach
//! their own metrics after merge via [`NodeMetrics::register`] /
//! [`NodeMetrics::register_source`] without touching this module.

use std::sync::Arc;

use futures::future::BoxFuture;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::metrics::info::Info;
use prometheus_client::registry::{Metric, Registry, Unit};
use tokio::sync::RwLock;

/// Exponential request-duration buckets in seconds, spanning ~1 ms to 60 s.
pub const DURATION_BUCKETS_SECONDS: [f64; 15] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

/// Labels for the request counter: which interface, HTTP method, status code.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RequestLabels {
    pub interface: &'static str,
    pub method: &'static str,
    pub code: u16,
}

/// Collapses nonstandard request methods to `other` so arbitrary extension
/// tokens cannot mint unbounded label values.
pub fn method_label(method: &str) -> &'static str {
    match method {
        "GET" => "GET",
        "HEAD" => "HEAD",
        "POST" => "POST",
        "PUT" => "PUT",
        "DELETE" => "DELETE",
        "OPTIONS" => "OPTIONS",
        "PATCH" => "PATCH",
        "TRACE" => "TRACE",
        "CONNECT" => "CONNECT",
        _ => "other",
    }
}

/// Labels for the request-duration histogram: interface plus the logical
/// operation (REST matched-route template or resolved S3 operation name).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RouteLabels {
    pub interface: &'static str,
    pub op: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BuildInfoLabels {
    version: String,
}

/// A scrape-time metrics contributor. `refresh` runs before each `/metrics`
/// encode so probes that must `await` storage can update their gauges; the
/// sync `Collector` trait cannot express those awaits.
pub trait MetricsSource: Send + Sync {
    fn refresh(&self) -> BoxFuture<'_, ()>;
}

/// Owns a node's Prometheus registry and the built-in request metrics.
pub struct NodeMetrics {
    registry: RwLock<Registry>,
    sources: RwLock<Vec<Arc<dyn MetricsSource>>>,
    pub http_requests: Family<RequestLabels, Counter>,
    pub http_request_duration: Family<RouteLabels, Histogram>,
    node_started: Gauge,
}

impl NodeMetrics {
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("aruna");

        let http_requests = Family::<RequestLabels, Counter>::default();
        registry.register(
            "http_requests",
            "Total number of HTTP and S3 requests served",
            http_requests.clone(),
        );

        let http_request_duration = Family::<RouteLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(DURATION_BUCKETS_SECONDS)
        });
        registry.register_with_unit(
            "http_request_duration",
            "HTTP and S3 request duration",
            Unit::Seconds,
            http_request_duration.clone(),
        );

        let node_started = Gauge::default();
        registry.register(
            "node_started",
            "1 once the node finished startup and is serving traffic",
            node_started.clone(),
        );

        registry.register(
            "build",
            "Build information",
            Info::new(BuildInfoLabels {
                version: env!("CARGO_PKG_VERSION").to_string(),
            }),
        );

        Self {
            registry: RwLock::new(registry),
            sources: RwLock::new(Vec::new()),
            http_requests,
            http_request_duration,
            node_started,
        }
    }

    pub fn set_node_started(&self, started: bool) {
        self.node_started.set(i64::from(started));
    }

    /// Late-register a metric into the node registry (used by feature branches).
    pub async fn register(&self, name: &str, help: &str, metric: impl Metric) {
        self.registry
            .write()
            .await
            .register(name.to_string(), help.to_string(), metric);
    }

    /// Late-register a metric carrying a unit suffix (e.g. `_seconds`).
    pub async fn register_with_unit(
        &self,
        name: &str,
        help: &str,
        unit: Unit,
        metric: impl Metric,
    ) {
        self.registry.write().await.register_with_unit(
            name.to_string(),
            help.to_string(),
            unit,
            metric,
        );
    }

    pub async fn register_source(&self, source: Arc<dyn MetricsSource>) {
        self.sources.write().await.push(source);
    }

    /// Refresh every scrape-time source, then encode the registry as text.
    pub async fn render(&self) -> String {
        let sources = self.sources.read().await.clone();
        for source in &sources {
            source.refresh().await;
        }
        let registry = self.registry.read().await;
        let mut buffer = String::new();
        encode(&mut buffer, &registry)
            .expect("encoding prometheus metrics into a String is infallible");
        buffer
    }
}

impl Default for NodeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for NodeMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The scrape sources are trait objects that do not implement Debug.
        f.debug_struct("NodeMetrics").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[tokio::test]
    async fn render_includes_builtin_metrics() {
        let metrics = NodeMetrics::new();
        metrics
            .http_requests
            .get_or_create(&RequestLabels {
                interface: "rest",
                method: method_label("GET"),
                code: 200,
            })
            .inc();

        let body = metrics.render().await;
        assert!(
            body.contains(
                "aruna_http_requests_total{interface=\"rest\",method=\"GET\",code=\"200\"} 1"
            ),
            "{body}"
        );
        assert!(
            body.contains("aruna_http_request_duration_seconds"),
            "{body}"
        );
        assert!(body.contains("aruna_build_info{version=\""), "{body}");
    }

    #[test]
    fn method_label_collapses_nonstandard_tokens() {
        assert_eq!(method_label("GET"), "GET");
        assert_eq!(method_label("PATCH"), "PATCH");
        assert_eq!(method_label("BREW"), "other");
        assert_eq!(method_label("get"), "other");
    }

    #[tokio::test]
    async fn node_started_gauge_tracks_flag() {
        let metrics = NodeMetrics::new();
        assert!(metrics.render().await.contains("aruna_node_started 0"));
        metrics.set_node_started(true);
        assert!(metrics.render().await.contains("aruna_node_started 1"));
    }

    #[tokio::test]
    async fn late_registration_is_rendered() {
        let metrics = NodeMetrics::new();
        let gauge: Gauge = Gauge::default();
        gauge.set(7);
        metrics
            .register("custom_gauge", "late", gauge.clone())
            .await;
        assert!(metrics.render().await.contains("aruna_custom_gauge 7"));
    }

    #[tokio::test]
    async fn sources_refresh_before_encode() {
        struct CountingSource {
            calls: Arc<AtomicU64>,
            gauge: Gauge,
        }
        impl MetricsSource for CountingSource {
            fn refresh(&self) -> BoxFuture<'_, ()> {
                Box::pin(async move {
                    let next = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
                    self.gauge.set(next as i64);
                })
            }
        }

        let metrics = NodeMetrics::new();
        let gauge: Gauge = Gauge::default();
        metrics
            .register("refreshes", "refresh count", gauge.clone())
            .await;
        let calls = Arc::new(AtomicU64::new(0));
        metrics
            .register_source(Arc::new(CountingSource {
                calls: calls.clone(),
                gauge,
            }))
            .await;

        assert!(metrics.render().await.contains("aruna_refreshes 1"));
        assert!(metrics.render().await.contains("aruna_refreshes 2"));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
