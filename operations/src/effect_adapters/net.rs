//! Sends net effects through the net handle and runs the job control frame round trip.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::DocumentSyncPublish;
use aruna_core::effects::{Effect, JobControlEffect, NetEffect};
use aruna_core::events::{Event, JobControlEvent, NetError, NetEvent};
use aruna_core::handle::Handle;
use aruna_core::{DocumentEffect, DocumentNetEvent, DocumentTarget};

use crate::driver::DriverContext;

/// Executes a job-control request by opening the frame stream and reading the
/// owner's reply; an unreachable owner is reported so the routing operation can
/// map it to `Unavailable` (503). The artifact body path stays out of band.
pub(super) async fn dispatch_job_control(
    effect: JobControlEffect,
    context: &DriverContext,
) -> Event {
    let JobControlEffect { owner, request } = effect;
    let event = match crate::jobs::protocol::send_job_request(context, owner, request).await {
        Ok(reply) => JobControlEvent::Response(Box::new(reply.response)),
        Err(error) => JobControlEvent::Unavailable(error.to_string()),
    };
    Event::Net(NetEvent::JobControl(event))
}

pub(super) async fn dispatch_net(effect: NetEffect, context: &DriverContext) -> Event {
    if let NetEffect::DocumentSync(DocumentEffect::ReconcileTopics { topics }) = effect {
        return Event::Net(NetEvent::DocumentSync(
            reconcile_topics(context, topics).await,
        ));
    }
    if let Some(net_handle) = &context.net_handle {
        Box::pin(net_handle.send_effect(Effect::Net(effect))).await
    } else {
        match effect {
            NetEffect::DocumentSync(DocumentEffect::PublishDocuments { documents, .. }) => {
                Event::Net(NetEvent::DocumentSync(
                    DocumentNetEvent::DocumentsPublished {
                        targets: publication_targets(&documents),
                    },
                ))
            }
            _ => Event::Net(NetEvent::Error(NetError::ChannelClosed)),
        }
    }
}

/// Reconciles through the handle, which also reloads realm peers after a realm config change.
async fn reconcile_topics(
    context: &DriverContext,
    topics: Vec<irokle::TopicId>,
) -> DocumentNetEvent {
    let Some(net_handle) = &context.net_handle else {
        return DocumentNetEvent::Error {
            target: None,
            error: "net handle unavailable".to_string(),
        };
    };
    match Box::pin(net_handle.reconcile_sync_topics(topics)).await {
        Ok(result) => DocumentNetEvent::DocumentsReconciled {
            applied: result.applied(),
            targets: result.targets,
            metadata_create_events: result.metadata_create_events,
            metadata_graph_tombstones: result.metadata_graph_tombstones,
        },
        Err(error) => DocumentNetEvent::Error {
            target: None,
            error: error.to_string(),
        },
    }
}

/// Targets a publication without a net handle still reports: exactly the
/// documents the operation selected. Kept pure so the reduced-capability
/// outcome is testable.
fn publication_targets(documents: &[DocumentSyncPublish]) -> Vec<DocumentTarget> {
    documents
        .iter()
        .map(|document| document.target().clone())
        .collect()
}

#[cfg(test)]
mod pure_tests {
    use super::publication_targets;
    use aruna_core::document::{
        DocumentChange, DocumentChangeKind, DocumentSyncPublish, DocumentSyncRevision,
        DocumentTarget,
    };
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::types::GroupId;
    use ulid::Ulid;

    #[test]
    fn publication_targets_reported() {
        let group_id = GroupId::from_bytes([8u8; 16]);
        let target = DocumentTarget::Group { group_id };
        let publish = DocumentSyncPublish::Delete {
            event_id: Ulid::from_parts(1, 1),
            target: target.clone(),
            change: DocumentChange {
                base: None,
                current: DocumentSyncRevision {
                    generation: 1,
                    event_id: Ulid::from_parts(2, 2),
                    actor: iroh::SecretKey::from_bytes(&[2u8; 32]).public(),
                    updated_at_ms: 0,
                },
                kind: DocumentChangeKind::Delete,
                placement: PlacementRef::NIL,
            },
            allow_genesis: false,
        };

        assert_eq!(publication_targets(&[publish]), vec![target]);
        assert!(publication_targets(&[]).is_empty());
    }
}
