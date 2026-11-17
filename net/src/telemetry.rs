//! Warns when an Iroh network phase or request runs past its slow threshold, and records the
//! selected path of a connection on its span.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

pub(crate) use aruna_core::telemetry::{
    current_trace_context, duration_ms, extract_trace_context, record_duration_ms,
};
use iroh::endpoint::Connection;
use tracing::{Span, field, warn};

pub(crate) const IROH_PHASE_THRESHOLD: Duration = Duration::from_millis(500);
pub(crate) const IROH_REQUEST_THRESHOLD: Duration = Duration::from_secs(2);

pub(crate) fn warn_iroh_phase(operation: &'static str, phase: &'static str, duration: Duration) {
    if duration >= IROH_PHASE_THRESHOLD {
        warn!(
            event = "iroh.network.slow_phase",
            operation,
            phase,
            duration_ms = duration_ms(duration),
            threshold_ms = duration_ms(IROH_PHASE_THRESHOLD),
            "Slow Iroh network phase"
        );
    }
}

pub(crate) fn warn_iroh_request(operation: &'static str, duration: Duration) {
    if duration >= IROH_REQUEST_THRESHOLD {
        warn!(
            event = "iroh.network.slow_request",
            operation,
            duration_ms = duration_ms(duration),
            threshold_ms = duration_ms(IROH_REQUEST_THRESHOLD),
            "Slow Iroh network request"
        );
    }
}

/// Records the selected path's address and round trip time of `conn` on `span`.
pub(crate) fn record_selected_path(span: &Span, conn: &Connection) {
    let paths = conn.paths();
    let Some(path) = paths.iter().find(|path| path.is_selected()) else {
        return;
    };

    span.record(
        "iroh.selected_address",
        field::display(format!("{:?}", path.remote_addr())),
    );
    span.record("iroh.rtt_ms", duration_ms(path.rtt()));
}
