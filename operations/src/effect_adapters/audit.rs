//! Asks every node for its local audit page and reports each node it could not read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::audit::{AuditPageBatch, AuditPageRequest, AuditPageResponse, MAX_AUDIT_PEERS};
use aruna_core::effects::AuditPageEffect;
use aruna_core::events::{Event, NetEvent};
use aruna_core::id::NodeId;
use futures_util::{StreamExt, stream};
use std::time::Duration;
use tracing::trace;

use crate::driver::DriverContext;
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage};

const AUDIT_FANOUT_CONCURRENCY: usize = 8;
const AUDIT_PEER_DEADLINE: Duration = Duration::from_secs(3);
const AUDIT_FANOUT_DEADLINE: Duration = Duration::from_secs(30);

fn audit_nodes(nodes: Vec<NodeId>, batch: &mut AuditPageBatch) -> BTreeSet<NodeId> {
    let mut queried: BTreeSet<NodeId> = nodes.into_iter().collect();
    // Every node past the fan-out cap is reported missing; querying none of them
    // would render an empty page as an almost complete audit trail.
    while queried.len() > MAX_AUDIT_PEERS {
        if let Some(node) = queried.pop_last() {
            batch.mark_missing(node);
        }
    }
    queried
}

/// Requests every node's local audit page over the metadata control transport,
/// concurrently so one unreachable node cannot spend the whole request deadline.
/// An unreachable or denied node is reported so the aggregator records it missing.
pub(super) async fn dispatch_audit_page(
    effect: AuditPageEffect,
    context: &DriverContext,
    operation_deadline: Option<tokio::time::Instant>,
) -> Event {
    let AuditPageEffect {
        nodes: input_nodes,
        request,
    } = effect;
    let mut batch = AuditPageBatch::with_limit(request.limit);
    let nodes = audit_nodes(input_nodes, &mut batch);
    let mut remaining = nodes.clone();
    if remaining.is_empty() {
        return Event::Net(NetEvent::AuditPages(batch));
    }

    let deadline =
        operation_deadline.unwrap_or_else(|| tokio::time::Instant::now() + AUDIT_FANOUT_DEADLINE);
    let requests = stream::iter(nodes.into_iter().map(|node| {
        let request = request.clone();
        async move {
            let peer_deadline = tokio::time::Instant::now() + AUDIT_PEER_DEADLINE;
            let peer_deadline = if peer_deadline < deadline {
                peer_deadline
            } else {
                deadline
            };
            let result =
                tokio::time::timeout_at(peer_deadline, send_audit_request(context, node, request))
                    .await;
            (node, result)
        }
    }))
    .buffer_unordered(AUDIT_FANOUT_CONCURRENCY);
    futures_util::pin_mut!(requests);
    loop {
        let next = match tokio::time::timeout_at(deadline, requests.next()).await {
            Ok(next) => next,
            Err(_) => break,
        };
        let Some((node, result)) = next else {
            break;
        };
        remaining.remove(&node);
        match result {
            Ok(Ok(response)) => {
                if let Err(error) = batch.add_page(node, response, &request) {
                    trace!(?node, ?error, "Rejected audit page");
                }
            }
            Ok(Err(error)) => {
                trace!(?node, ?error, "Audit page unavailable");
                batch.mark_missing(node);
            }
            Err(_) => {
                trace!(?node, "Audit page request timed out");
                batch.mark_missing(node);
            }
        }
    }
    for node in remaining {
        batch.mark_missing(node);
    }
    Event::Net(NetEvent::AuditPages(batch))
}

/// Adapter I/O for [`AuditPageEffect`]: requests one node's local page over the
/// metadata control transport. Kept out of the sans-I/O operation.
async fn send_audit_request(
    context: &DriverContext,
    node: NodeId,
    request: AuditPageRequest,
) -> Result<AuditPageResponse, MetadataReadError> {
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    match metadata
        .request_forwarded_write(node, MetadataTransportMessage::ForwardAuditPage { request })
        .await
    {
        Ok(MetadataTransportMessage::ForwardedAuditPage { result }) => result,
        Ok(_) | Err(_) => Err(MetadataReadError::Unavailable),
    }
}

#[cfg(test)]
mod pure_tests {
    use super::audit_nodes;
    use aruna_core::audit::{AuditPageBatch, MAX_AUDIT_PEERS};

    #[test]
    fn caps_audit_peers() {
        // Over the cap the fan-out still asks MAX_AUDIT_PEERS nodes and reports
        // the rest as missing instead of returning an empty page.
        let nodes = (1..=(MAX_AUDIT_PEERS as u8 + 2))
            .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
            .collect::<Vec<_>>();
        let mut batch = AuditPageBatch::new();

        let queried = audit_nodes(nodes, &mut batch);

        assert_eq!(queried.len(), MAX_AUDIT_PEERS);
        assert_eq!(batch.missing_nodes.len(), 2);
        assert_eq!(batch.missing_overflow, 0);
        assert!(batch.completed_nodes.is_empty());
        assert!(
            batch
                .missing_nodes
                .iter()
                .all(|node| !queried.contains(node))
        );
    }
}
