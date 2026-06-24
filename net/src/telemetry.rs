use std::time::Duration;

use aruna_core::DistributedTraceContext;
pub(crate) use aruna_core::telemetry::duration_ms;
use opentelemetry::Context;
use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};
use tracing::{Span, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub(crate) const SLOW_IROH_PHASE_THRESHOLD: Duration = Duration::from_millis(500);
pub(crate) const SLOW_IROH_REQUEST_THRESHOLD: Duration = Duration::from_secs(2);

#[derive(Default)]
struct TraceContextCarrier {
    traceparent: Option<String>,
    tracestate: Option<String>,
}

impl TraceContextCarrier {
    fn from_trace_context(trace_context: &DistributedTraceContext) -> Self {
        Self {
            traceparent: Some(trace_context.traceparent.clone()),
            tracestate: trace_context.tracestate.clone(),
        }
    }

    fn into_trace_context(self) -> Option<DistributedTraceContext> {
        self.traceparent
            .map(|traceparent| DistributedTraceContext::new(traceparent, self.tracestate))
    }
}

impl Injector for TraceContextCarrier {
    fn set(&mut self, key: &str, value: String) {
        if key.eq_ignore_ascii_case("traceparent") {
            self.traceparent = Some(value);
        } else if key.eq_ignore_ascii_case("tracestate") {
            self.tracestate = Some(value);
        }
    }
}

impl Extractor for TraceContextCarrier {
    fn get(&self, key: &str) -> Option<&str> {
        if key.eq_ignore_ascii_case("traceparent") {
            self.traceparent.as_deref()
        } else if key.eq_ignore_ascii_case("tracestate") {
            self.tracestate.as_deref()
        } else {
            None
        }
    }

    fn keys(&self) -> Vec<&str> {
        let mut keys = Vec::with_capacity(2);
        if self.traceparent.is_some() {
            keys.push("traceparent");
        }
        if self.tracestate.is_some() {
            keys.push("tracestate");
        }
        keys
    }
}

pub(crate) fn current_trace_context() -> Option<DistributedTraceContext> {
    let context = Span::current().context();
    let mut carrier = TraceContextCarrier::default();
    global::get_text_map_propagator(|propagator| propagator.inject_context(&context, &mut carrier));
    carrier.into_trace_context()
}

pub(crate) fn extract_trace_context(trace_context: &DistributedTraceContext) -> Context {
    let carrier = TraceContextCarrier::from_trace_context(trace_context);
    global::get_text_map_propagator(|propagator| propagator.extract(&carrier))
}

pub(crate) fn record_duration_ms(span: &Span, field: &'static str, duration: Duration) {
    span.record(field, duration_ms(duration));
}

pub(crate) fn warn_if_slow_iroh_phase(
    operation: &'static str,
    phase: &'static str,
    duration: Duration,
) {
    if duration >= SLOW_IROH_PHASE_THRESHOLD {
        warn!(
            event = "iroh.network.slow_phase",
            operation,
            phase,
            duration_ms = duration_ms(duration),
            threshold_ms = duration_ms(SLOW_IROH_PHASE_THRESHOLD),
            "Slow Iroh network phase"
        );
    }
}

pub(crate) fn warn_if_slow_iroh_request(operation: &'static str, duration: Duration) {
    if duration >= SLOW_IROH_REQUEST_THRESHOLD {
        warn!(
            event = "iroh.network.slow_request",
            operation,
            duration_ms = duration_ms(duration),
            threshold_ms = duration_ms(SLOW_IROH_REQUEST_THRESHOLD),
            "Slow Iroh network request"
        );
    }
}
