use aruna_core::DistributedTraceContext;
use opentelemetry::Context;
use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

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

pub fn current_trace_context() -> Option<DistributedTraceContext> {
    let context = Span::current().context();
    let mut carrier = TraceContextCarrier::default();
    global::get_text_map_propagator(|propagator| propagator.inject_context(&context, &mut carrier));
    carrier.into_trace_context()
}

pub fn extract_trace_context(trace_context: &DistributedTraceContext) -> Context {
    let carrier = TraceContextCarrier::from_trace_context(trace_context);
    global::get_text_map_propagator(|propagator| propagator.extract(&carrier))
}
