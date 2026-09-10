use std::time::Duration;

pub(crate) use aruna_core::telemetry::{
    current_trace_context, duration_ms, extract_trace_context, record_duration_ms,
};
use tracing::warn;

pub(crate) const SLOW_IROH_PHASE_THRESHOLD: Duration = Duration::from_millis(500);
pub(crate) const SLOW_IROH_REQUEST_THRESHOLD: Duration = Duration::from_secs(2);

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
