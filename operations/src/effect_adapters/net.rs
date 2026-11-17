//! Sends net effects through the net handle and runs the job control frame round trip.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::document::DocumentSyncPublish;
use aruna_core::effects::{Effect, JobControlEffect, NetEffect};
use aruna_core::events::{Event, JobControlEvent, NetError, NetEvent};
use aruna_core::handle::Handle;
use aruna_core::heartbeat::{HEARTBEAT_IO_TIMEOUT, HeartbeatEffect, HeartbeatEvent};
use aruna_core::{DocumentEffect, DocumentNetEvent, DocumentTarget};
use aruna_net::NetHandle;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

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

/// Records, sends or keeps one heartbeat with the net handle; a send is cut off after
/// [`HEARTBEAT_IO_TIMEOUT`].
pub(super) async fn dispatch_heartbeat(effect: HeartbeatEffect, context: &DriverContext) -> Event {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return Event::Net(NetEvent::Error(NetError::ChannelClosed));
    };
    let event = match effect {
        HeartbeatEffect::Record(heartbeat) => {
            net_handle.record_own_heartbeat(heartbeat);
            HeartbeatEvent::Peers(net_handle.realm_peers().await)
        }
        HeartbeatEffect::Push { peer, frame } => {
            match timeout(HEARTBEAT_IO_TIMEOUT, push_frame(net_handle, peer, &frame)).await {
                Ok(Ok(())) => HeartbeatEvent::Pushed,
                Ok(Err(error)) => HeartbeatEvent::PushFailed(error),
                Err(_) => HeartbeatEvent::PushFailed("timed out".to_string()),
            }
        }
        HeartbeatEffect::Store { peer, heartbeat } => {
            HeartbeatEvent::Stored(net_handle.record_heartbeat(peer, *heartbeat))
        }
    };
    Event::Net(NetEvent::Heartbeat(event))
}

async fn push_frame(net_handle: &NetHandle, peer: NodeId, frame: &[u8]) -> Result<(), String> {
    let mut stream = net_handle
        .open_stream(peer, Alpn::Heartbeat)
        .await
        .map_err(|error| error.to_string())?;
    stream
        .0
        .write_all(frame)
        .await
        .map_err(|error| error.to_string())?;
    stream.0.finish().map_err(|error| error.to_string())
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
