//! Guards effects against deleted graphs, prunes deleted graphs and syncs graphs best effort.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::sync::sync_graph_once;

use aruna_core::NodeId;
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};
use tokio::time::sleep;
use tracing::warn;

use super::{GRAPH_SYNC_AFTER, GRAPH_SYNC_ATTEMPTS, MetadataHandle, handles, metadata_graph_fence};
use crate::driver::{DriverContext, drive};
use crate::metadata::graph_deleted::GraphDeletedOperation;

/// Removes the local copy of `graph_iri` when its lifecycle record says it is deleted.
pub(crate) async fn prune_if_deleted(
    context: &DriverContext,
    graph_iri: String,
) -> Result<bool, MetadataError> {
    let (_, graph) = handles(context)?;
    let _graph_fence = metadata_graph_fence(&graph_iri)
        .acquire()
        .await
        .map_err(|error| {
            MetadataError::Backend(format!("metadata graph fence unavailable: {error}"))
        })?;
    if !drive(GraphDeletedOperation::new(&graph_iri), context).await? {
        return Ok(false);
    }
    graph.forget_graph(graph_iri.clone());
    if !graph.contains_graph(graph_iri.clone()).await? {
        return Ok(true);
    }
    graph.delete_graph(graph_iri).await?;
    Ok(true)
}

impl MetadataHandle {
    /// Applies document sync updates from peers; returns how many graphs changed.
    pub(crate) async fn reconcile_document_sync(
        &self,
        context: &DriverContext,
    ) -> Result<usize, MetadataError> {
        let graph = context
            .graph_handle
            .as_ref()
            .ok_or(MetadataError::HandleMissing)?;
        Ok(graph.reconcile().await?.len())
    }

    /// Removes the local copy of every deleted graph; returns how many were removed.
    pub(crate) async fn prune_deleted_graphs(
        &self,
        context: &DriverContext,
    ) -> Result<usize, MetadataError> {
        let graph = context
            .graph_handle
            .as_ref()
            .ok_or(MetadataError::HandleMissing)?;
        let mut pruned = 0usize;
        for graph_iri in graph.graphs().await? {
            if prune_if_deleted(context, graph_iri).await? {
                pruned += 1;
            }
        }
        Ok(pruned)
    }
}

pub(super) fn sync_best_effort(
    context: &DriverContext,
    graph_iri: String,
    mut peers: Vec<NodeId>,
) -> MetadataEvent {
    if let Some(net_handle) = context.net_handle.as_ref() {
        peers.retain(|peer| *peer != net_handle.node_id());
    }
    peers.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    peers.dedup();
    if peers.is_empty() {
        return MetadataEvent::GraphSyncScheduled { graph_iri, peers };
    }

    let context = context.clone();
    let task_graph_iri = graph_iri.clone();
    let task_peers = peers.clone();
    tokio::spawn(async move {
        for attempt in 1..=GRAPH_SYNC_ATTEMPTS {
            match sync_graph_once(&context, task_graph_iri.clone(), task_peers.clone()).await {
                Ok(()) => return,
                Err(error) => {
                    warn!(
                        graph_iri = %task_graph_iri,
                        attempt,
                        attempts = GRAPH_SYNC_ATTEMPTS,
                        error = ?error,
                        "Metadata graph sync attempt failed"
                    );
                    if attempt < GRAPH_SYNC_ATTEMPTS {
                        sleep(GRAPH_SYNC_AFTER).await;
                    }
                }
            }
        }

        warn!(
            graph_iri = %task_graph_iri,
            peer_count = task_peers.len(),
            "Metadata graph sync retries exhausted"
        );
    });

    MetadataEvent::GraphSyncScheduled { graph_iri, peers }
}

pub(super) async fn metadata_graph_deleted(
    context: &DriverContext,
    graph_iri: &str,
) -> Result<bool, MetadataError> {
    let (_, graph) = handles(context)?;
    if let Some((true, _)) = graph.cached_lifecycle(graph_iri) {
        return Ok(true);
    }
    let deleted = drive(GraphDeletedOperation::new(graph_iri), context).await?;
    graph.store_lifecycle(graph_iri.to_string(), deleted);
    Ok(deleted)
}

pub(super) fn effect_rejects_deleted(effect: &MetadataEffect) -> bool {
    matches!(
        effect,
        MetadataEffect::ValidateCreateCrate { .. }
            | MetadataEffect::ValidateRoCrate { .. }
            | MetadataEffect::CreateCrate { .. }
            | MetadataEffect::ApplyRoCrate { .. }
            | MetadataEffect::UpsertDataEntity { .. }
            | MetadataEffect::UpsertContextualEntity { .. }
            | MetadataEffect::SetGraphPolicy { .. }
            | MetadataEffect::AddGraphPeer { .. }
            | MetadataEffect::GetGraphPolicy { .. }
            | MetadataEffect::ExportRoCrate { .. }
            | MetadataEffect::ExportRoCrateSummary { .. }
            | MetadataEffect::ExportSummaryAt { .. }
            | MetadataEffect::DescribeTitle { .. }
            | MetadataEffect::ExportRoCratePage { .. }
            | MetadataEffect::PlanBatch { .. }
            | MetadataEffect::MergeBatch { .. }
    )
}
