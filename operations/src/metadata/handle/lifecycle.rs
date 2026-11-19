//! Reconciles document sync into the graph store and syncs graphs with peers best effort.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::sync::sync_graph_once;

use aruna_core::NodeId;
use aruna_core::metadata::{MetadataError, MetadataEvent};
use tokio::time::sleep;
use tracing::warn;

use super::{GRAPH_SYNC_AFTER, GRAPH_SYNC_ATTEMPTS, MetadataHandle};
use crate::driver::DriverContext;

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
