//! Network adapters: the handle-backed net effects, the job-control frame
//! round-trip, and the no-handle document-publication fallback.
//!
//! Job control runs its frame I/O here because the runner holds the context;
//! the net crate never sees that effect. A node without a net handle reports
//! `ChannelClosed` for every net effect except a document publication, whose
//! established reduced-capability outcome is the target list the operation
//! already selected.

use aruna_core::document::DocumentSyncPublish;
use aruna_core::effects::{Effect, JobControlEffect, NetEffect};
use aruna_core::events::{Event, JobControlEvent, NetError, NetEvent};
use aruna_core::handle::Handle;
use aruna_core::{DocumentSyncEffect, DocumentSyncNetEvent, DocumentSyncTarget};

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
            NetEffect::DocumentSync(DocumentSyncEffect::PublishDocuments { documents, .. }) => {
                Event::Net(NetEvent::DocumentSync(
                    DocumentSyncNetEvent::DocumentsPublished {
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
fn publication_targets(documents: &[DocumentSyncPublish]) -> Vec<DocumentSyncTarget> {
    documents
        .iter()
        .map(|document| document.target().clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::publication_targets;
    use aruna_core::document::{
        DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncPublish, DocumentSyncRevision,
        DocumentSyncTarget,
    };
    use aruna_core::structs::PlacementRef;
    use aruna_core::types::GroupId;
    use ulid::Ulid;

    #[test]
    fn no_handle_publication_reports_selected_targets() {
        let group_id = GroupId::from_bytes([8u8; 16]);
        let target = DocumentSyncTarget::Group { group_id };
        let publish = DocumentSyncPublish::Delete {
            event_id: Ulid::generate(),
            target: target.clone(),
            change: DocumentSyncChange {
                base: None,
                current: DocumentSyncRevision {
                    generation: 1,
                    event_id: Ulid::generate(),
                    actor: iroh::SecretKey::from_bytes(&[2u8; 32]).public(),
                    updated_at_ms: 0,
                },
                kind: DocumentSyncChangeKind::Delete,
                placement: PlacementRef::NIL,
            },
            allow_genesis: false,
        };

        assert_eq!(publication_targets(&[publish]), vec![target]);
        assert!(publication_targets(&[]).is_empty());
    }
}
