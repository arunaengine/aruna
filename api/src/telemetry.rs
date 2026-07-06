use std::sync::{Arc, LazyLock, OnceLock};
use std::time::{Duration, Instant};

use crate::server_state::ServerState;
use aruna_core::metrics::{RequestLabels, RouteLabels};
use aruna_core::structs::AuthContext;
use aruna_core::telemetry::{LatencyAggregator, RequestStages, duration_ms};
use axum::extract::{MatchedPath, Request, State};
use axum::middleware::Next;
use axum::response::Response;
use http::{HeaderMap, Method};
use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use tracing::{Instrument, Span, error, field, info_span, trace, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use ulid::Ulid;

const DEFAULT_SLOW_REQUEST_THRESHOLD_MS: u64 = 500;
const SLOW_REQUEST_THRESHOLD_ENV: &str = "ARUNA_SLOW_REQUEST_THRESHOLD_MS";

// Unbiased per-route request latency histograms flushed as `latency.summary`.
static HTTP_LATENCY: LazyLock<LatencyAggregator> = LazyLock::new(|| LatencyAggregator::new("http"));

fn slow_request_threshold() -> Duration {
    static THRESHOLD: OnceLock<Duration> = OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        parse_slow_request_threshold(std::env::var(SLOW_REQUEST_THRESHOLD_ENV).ok().as_deref())
    })
}

fn parse_slow_request_threshold(value: Option<&str>) -> Duration {
    Duration::from_millis(
        value
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_SLOW_REQUEST_THRESHOLD_MS),
    )
}

fn is_slow_request(elapsed: Duration, threshold: Duration) -> bool {
    elapsed >= threshold
}

struct HeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(http::HeaderName::as_str).collect()
    }
}

pub async fn request_tracing_middleware(
    State(state): State<Arc<ServerState>>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let query = request.uri().query().map(str::to_string);
    // Router::layer middleware runs after route matching, so the matched
    // route template is available and keeps the latency key cardinality low.
    let matched_route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_string());
    let route = matched_route.clone().unwrap_or_else(|| path.clone());
    // Prometheus label must never carry a raw path; unmatched requests collapse.
    let metric_op = matched_route.unwrap_or_else(|| "unmatched".to_string());
    let span = make_request_span("http", request.headers(), &method, &path);
    let started = Instant::now();

    {
        let _guard = span.enter();
        trace!(
            event = "request.received",
            protocol = "http",
            method = %method,
            path = %path,
            query = query.as_deref().unwrap_or(""),
            "Received HTTP request"
        );
    }

    let stages = RequestStages::default();
    let response = stages
        .clone()
        .scope(next.run(request).instrument(span.clone()))
        .await;
    let elapsed = started.elapsed();
    let status_code = response.status().as_u16();
    emit_request_completed(&span, "http", status_code, started);
    let route_key = format!("{method} {route}");
    HTTP_LATENCY.record(&route_key, elapsed);
    let metrics = state.metrics();
    metrics
        .http_requests
        .get_or_create(&RequestLabels {
            interface: "rest",
            method: method.to_string(),
            code: status_code,
        })
        .inc();
    metrics
        .http_request_duration
        .get_or_create(&RouteLabels {
            interface: "rest",
            op: metric_op,
        })
        .observe(elapsed.as_secs_f64());
    if is_slow_request(elapsed, slow_request_threshold()) {
        let _guard = span.enter();
        warn!(
            event = "request.slow",
            method = %method,
            route = %route_key,
            status_code = response.status().as_u16(),
            total_ms = duration_ms(elapsed),
            threshold_ms = duration_ms(slow_request_threshold()),
            stages = %stages.render(),
            "Slow HTTP request"
        );
    }
    response
}

pub fn make_request_span(
    protocol: &'static str,
    headers: &HeaderMap,
    method: &Method,
    path: &str,
) -> Span {
    let request_id = Ulid::new().to_string();
    let span = info_span!(
        "request",
        "otel.kind" = "server",
        "otel.status_code" = field::Empty,
        "otel.status_description" = field::Empty,
        "http.request.method" = %method,
        "url.path" = %path,
        protocol = protocol,
        request_id = %request_id,
        method = %method,
        path = %path,
        status_code = field::Empty,
        user_id = field::Empty,
        realm_id = field::Empty,
        group_id = field::Empty,
        group_name = field::Empty,
    );

    let parent =
        global::get_text_map_propagator(|propagator| propagator.extract(&HeaderExtractor(headers)));
    let _ = span.set_parent(parent);
    span
}

pub fn record_auth_context(auth_ctx: Option<&AuthContext>) {
    let span = Span::current();
    if let Some(auth_ctx) = auth_ctx {
        span.record("user_id", field::display(auth_ctx.user_id));
        span.record("realm_id", field::display(&auth_ctx.realm_id));
        trace!(
            event = "request.authenticated",
            user_id = %auth_ctx.user_id,
            realm_id = %auth_ctx.realm_id,
            "Resolved request authentication"
        );
    } else {
        trace!(
            event = "request.authentication_missing",
            "Request did not include authentication"
        );
    }
}

pub fn emit_request_completed(
    span: &Span,
    protocol: &'static str,
    status_code: u16,
    started: Instant,
) {
    span.record("status_code", status_code);
    if status_code >= 500 {
        span.record("otel.status_code", "ERROR");
        span.record(
            "otel.status_description",
            field::display(format!("HTTP {status_code}")),
        );
    }
    let _guard = span.enter();
    let latency_ms = duration_ms(started.elapsed());
    match status_code {
        500.. => error!(
            event = "request.failed",
            protocol = protocol,
            status_code,
            latency_ms,
            "Request failed"
        ),
        400..=499 => warn!(
            event = "request.rejected",
            protocol = protocol,
            status_code,
            latency_ms,
            "Request completed with client error"
        ),
        _ => trace!(
            event = "request.completed",
            protocol = protocol,
            status_code,
            latency_ms,
            "Completed request"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slow_request_threshold_defaults_to_500ms() {
        assert_eq!(
            parse_slow_request_threshold(None),
            Duration::from_millis(500)
        );
        assert_eq!(
            parse_slow_request_threshold(Some("garbage")),
            Duration::from_millis(500)
        );
        assert_eq!(
            parse_slow_request_threshold(Some("")),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn slow_request_threshold_parses_override() {
        assert_eq!(
            parse_slow_request_threshold(Some("250")),
            Duration::from_millis(250)
        );
        assert_eq!(
            parse_slow_request_threshold(Some(" 1000 ")),
            Duration::from_millis(1000)
        );
    }

    #[test]
    fn slow_request_gating_is_inclusive_at_threshold() {
        let threshold = parse_slow_request_threshold(Some("500"));
        assert!(!is_slow_request(Duration::from_millis(499), threshold));
        assert!(is_slow_request(Duration::from_millis(500), threshold));
        assert!(is_slow_request(Duration::from_millis(750), threshold));
    }
}
