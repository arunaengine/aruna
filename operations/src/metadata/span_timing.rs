//! Records the stages of one local metadata read on its debug span, timed by the runner's clock.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::metadata::{MetadataError, MetadataQueryResults};
use aruna_core::telemetry::record_error;
use tracing::Span;

/// The debug span of one read, opened when the read starts, with the start of the read and of
/// its current stage on the runner's clock.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ReadTiming {
    span: Option<Span>,
    now_ms: u64,
    started_ms: u64,
    stage_ms: u64,
}

impl ReadTiming {
    pub(crate) fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    pub(crate) fn open(&mut self, span: Span) {
        self.span = Some(span);
        self.started_ms = self.now_ms;
        self.stage_ms = self.now_ms;
    }

    /// Records the time since the last stage under `field` and starts the next stage.
    pub(crate) fn lap(&mut self, field: &'static str) {
        self.note(field, self.now_ms.saturating_sub(self.stage_ms));
        self.stage_ms = self.now_ms;
    }

    /// Starts the next stage without recording the one before.
    pub(crate) fn restart(&mut self) {
        self.stage_ms = self.now_ms;
    }

    pub(crate) fn note(&self, field: &'static str, value: impl tracing::Value) {
        if let Some(span) = &self.span {
            span.record(field, value);
        }
    }

    /// Records the whole read's time and, for a failed read, its error.
    pub(crate) fn finish(&self, error: Option<&MetadataError>) {
        let Some(span) = &self.span else {
            return;
        };
        span.record("elapsed_ms", self.now_ms.saturating_sub(self.started_ms));
        if let Some(error) = error {
            record_error(span, &error.to_string());
        }
    }

    /// Records the kind and size of query results.
    pub(crate) fn results(&self, results: &MetadataQueryResults) {
        if let Some(span) = &self.span {
            results.record_on(span);
        }
    }
}
