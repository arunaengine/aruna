//! Network adapters: handle-backed net effects, the job-control frame round-trip
//! (run here because the runner holds the context), and the no-handle
//! publication fallback that reports the targets the operation already selected.

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
    use aruna_core::structs::placement::placement_record::PlacementRef;
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
