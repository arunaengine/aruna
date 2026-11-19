//! Runs metadata effects on the graph store directly, and exports one graph's RO-Crate.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::events::Event;
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};

use crate::driver::{DriverContext, drive};
use crate::metadata::deletion_guard::DeletionGuardOperation;

/// Runs one metadata effect on this node's graph store and answers with its event; an effect
/// that names a graph skips the deletion guard here.
pub async fn send_metadata_effect(context: &DriverContext, effect: MetadataEffect) -> Event {
    match &context.graph_handle {
        Some(graph) => Event::Metadata(graph.run(effect).await),
        None => Event::Metadata(MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::HandleMissing,
        }),
    }
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
