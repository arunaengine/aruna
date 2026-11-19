//! Runs metadata effects on the graph store, and best-effort graph syncs through the handle.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Instant;

use aruna_core::events::Event;
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};

use super::MetadataHandle;
use crate::driver::{DriverContext, drive};
use crate::metadata::deletion_guard::DeletionGuardOperation;

/// Runs one metadata effect on this node's graph store and answers with its event.
pub async fn send_metadata_effect(context: &DriverContext, effect: MetadataEffect) -> Event {
    match &context.metadata_handle {
        Some(metadata) => metadata.run_effect(context, effect).await,
        None => Event::Metadata(MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::HandleMissing,
        }),
    }
}

impl MetadataHandle {
    /// Runs one metadata effect on the graph store of `context`; an effect that names a graph
    /// came through the deletion guard first.
    pub(crate) async fn run_effect(
        &self,
        context: &DriverContext,
        effect: MetadataEffect,
    ) -> Event {
        let started = Instant::now();
        let event = run_effect(context, effect).await;
        aruna_core::telemetry::record_stage("craqle", started.elapsed());
        Event::Metadata(event)
    }
}

async fn run_effect(context: &DriverContext, effect: MetadataEffect) -> MetadataEvent {
    let Some(graph) = context.graph_handle.as_ref() else {
        return MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::HandleMissing,
        };
    };
    if matches!(effect, MetadataEffect::FlushGraph) {
        return match graph.flush_persistence().await {
            Ok(()) => MetadataEvent::GraphFlushed,
            Err(error) => MetadataEvent::Error {
                graph_iri: None,
                error,
            },
        };
    }
    graph.run(effect).await
}

/// The RO-Crate JSON-LD of one graph.
pub async fn export_rocrate_jsonld(
    context: &DriverContext,
    graph_iri: String,
) -> Result<String, MetadataError> {
    let export = MetadataEffect::ExportRoCrate {
        graph_iri: graph_iri.clone(),
    };
    match drive(DeletionGuardOperation::new(graph_iri, export), context).await? {
        MetadataEvent::RoCrateExportResult { jsonld, .. } => Ok(jsonld),
        MetadataEvent::Error { error, .. } => Err(error),
        other => Err(MetadataError::Backend(format!(
            "unexpected metadata export event: {}",
            other.label()
        ))),
    }
}
