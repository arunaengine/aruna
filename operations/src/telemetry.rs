use opentelemetry::trace::TraceContextExt;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub fn current_trace_id() -> Option<String> {
    let trace_id = Span::current().context().span().span_context().trace_id();
    if trace_id == opentelemetry::trace::TraceId::INVALID {
        None
    } else {
        Some(trace_id.to_string())
    }
}
