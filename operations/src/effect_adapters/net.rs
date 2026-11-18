//! Sends net effects through the net handle and answers them when this node has none.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_blob::native::{body_stream, send_request};
use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::document::DocumentSyncPublish;
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::errors::StagingSourceError;
use aruna_core::events::{
    Event, ExecutionSignEvent, JobControlEvent, NetError, NetEvent, PolicySignEvent,
    SnapshotSignEvent, StatementSignEvent, VaultSignEvent,
};
use aruna_core::handle::Handle;
use aruna_core::heartbeat::{HEARTBEAT_IO_TIMEOUT, HeartbeatEffect, HeartbeatEvent, HolderFacts};
use aruna_core::native_reference::{
    DelegatedBody, DelegatedReadEffect, GrantedBody, GrantedReadEffect, NativeAuthority,
    NativeReferenceRequest, NativeReferenceResponse,
};
use aruna_core::structs::storage::distribution::delegation::SignedDelegation;
use aruna_core::structs::storage::read_grant::SignedRequest;
use aruna_core::{DocumentEffect, DocumentNetEvent, DocumentTarget};
use aruna_net::NetHandle;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;
use tracing::warn;

use crate::driver::DriverContext;

/// Why signing fails on a node without a net handle, which holds the node key.
const UNAVAILABLE: &str = "net transport unavailable";
/// Records, sends or keeps one heartbeat with the net handle; a send is cut off after
/// [`HEARTBEAT_IO_TIMEOUT`].
pub(super) async fn dispatch_heartbeat(effect: HeartbeatEffect, context: &DriverContext) -> Event {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return Event::Net(NetEvent::Error(NetError::ChannelClosed));
    };
    let event = match effect {
        HeartbeatEffect::Record(heartbeat) => {
            net_handle.record_own_heartbeat(*heartbeat);
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
        HeartbeatEffect::Avoid { holder, now_ms } => {
            net_handle.avoided_holders().avoid(holder, now_ms);
            HeartbeatEvent::Avoided
        }
        HeartbeatEffect::Holders { holders, now_ms } => {
            let avoid_list = net_handle.avoided_holders();
            let avoided = (holders.iter())
                .filter(|holder| avoid_list.avoided(holder, now_ms))
                .copied()
                .collect();
            let held = (net_handle.held_heartbeats().into_iter())
                .filter(|held| holders.contains(&held.node_id))
                .collect();
            HeartbeatEvent::Holders(Box::new(HolderFacts {
                local: net_handle.node_id(),
                realm_id: *net_handle.realm_id(),
                held,
                avoided,
            }))
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

/// Carries out a change of this node's watch metrics or watch interest; without a net handle
/// only the suppression is logged.
fn record_watch(effect: NetEffect, net_handle: Option<&NetHandle>) -> Result<Event, NetEffect> {
    match effect {
        NetEffect::RecordSuppression(reason) => {
            if let Some(net_handle) = net_handle {
                let metrics = net_handle.notification_watch_metrics();
                metrics.record_delivery_suppression(reason);
            }
            warn!(
                parent: None,
                reason = reason.as_str(),
                "Notification watch delivery suppressed"
            );
        }
        NetEffect::RegisterInterest {
            watch_id,
            realm_id,
            holder,
            entry,
        } => {
            if let Some(net_handle) = net_handle {
                net_handle.register_local_interest(watch_id, realm_id, holder, entry);
            }
        }
        NetEffect::RetractInterest(watch_id) => {
            if let Some(net_handle) = net_handle {
                net_handle.retract_local_interest(watch_id);
            }
        }
        NetEffect::ReplaceInterest(table) => {
            if let Some(net_handle) = net_handle {
                net_handle.replace_watch_interest(*table);
            }
        }
        NetEffect::UpdateRealmInterest { realm_id, nodes } => {
            if let Some(net_handle) = net_handle {
                net_handle.update_realm_interest(realm_id, nodes);
            }
        }
        other => return Err(other),
    }
    Ok(Event::Net(NetEvent::Recorded))
}

/// Signs a granted read with this node's key, sends it and opens the holder's answer.
async fn granted_read(
    read: GrantedReadEffect,
    context: &DriverContext,
) -> Result<Box<GrantedBody>, StagingSourceError> {
    let net = (context.net_handle.as_ref()).ok_or(StagingSourceError::HandleMissing)?;
    if read.request.issuer != net.node_id() {
        return Err(StagingSourceError::AccessDenied);
    }
    let bytes = (read.request.signing_bytes())
        .map_err(|error| StagingSourceError::ReadError(error.to_string()))?;
    let holder = read.request.target;
    let signed = SignedRequest {
        signature: net.sign(&bytes),
        request: read.request,
    };
    let request = NativeReferenceRequest {
        authority: NativeAuthority::Granted(Box::new(signed)),
        bucket: read.bucket,
        key: read.key,
        version_id: read.version_id,
        head: read.head,
        range: read.range,
    };
    let (response, stream) = send_request(net, holder, request).await?;
    match response {
        NativeReferenceResponse::Content(metadata)
        | NativeReferenceResponse::Described { metadata, .. } => Ok(Box::new(GrantedBody {
            metadata,
            blob: body_stream(stream),
        })),
        NativeReferenceResponse::Rejected(reject) => Err(reject.into()),
    }
}

/// Signs a delegation with this node's key, sends it and opens the holder's answer.
async fn delegated_read(
    read: DelegatedReadEffect,
    context: &DriverContext,
) -> Result<Box<DelegatedBody>, StagingSourceError> {
    let net = (context.net_handle.as_ref()).ok_or(StagingSourceError::HandleMissing)?;
    let delegation = read.delegation;
    if delegation.issuer != net.node_id() {
        return Err(StagingSourceError::AccessDenied);
    }
    let bytes = (delegation.signing_bytes())
        .map_err(|error| StagingSourceError::ReadError(error.to_string()))?;
    let holder = delegation.target;
    let (bucket, key) = (delegation.bucket.clone(), delegation.key.clone());
    let signed = SignedDelegation {
        signature: net.sign(&bytes),
        delegation,
    };
    let request = NativeReferenceRequest {
        authority: NativeAuthority::Delegated(Box::new(signed)),
        bucket,
        key,
        version_id: read.version_id,
        head: read.head,
        range: read.range,
    };
    let (response, stream) = send_request(net, holder, request).await?;
    let (metadata, group_id) = match response {
        NativeReferenceResponse::Content(metadata) => (metadata, None),
        NativeReferenceResponse::Described { metadata, group_id } => (metadata, Some(group_id)),
        NativeReferenceResponse::Rejected(reject) => return Err(reject.into()),
    };
    Ok(Box::new(DelegatedBody {
        metadata,
        group_id,
        blob: body_stream(stream),
    }))
}

pub(super) async fn dispatch_net(effect: NetEffect, context: &DriverContext) -> Event {
    let effect = match record_watch(effect, context.net_handle.as_ref()) {
        Ok(recorded) => return recorded,
        Err(effect) => effect,
    };
    if let NetEffect::GrantedRead(read) = effect {
        return Event::Net(NetEvent::GrantedRead(granted_read(*read, context).await));
    }
    if let NetEffect::DelegatedRead(read) = effect {
        let answer = delegated_read(*read, context).await;
        return Event::Net(NetEvent::DelegatedRead(answer));
    }
    if let NetEffect::DocumentSync(DocumentEffect::ReconcileTopics { topics }) = effect {
        return Event::Net(NetEvent::DocumentSync(
            reconcile_topics(context, topics).await,
        ));
    }
    if let NetEffect::RefreshDocumentPeers(config) = effect {
        let refreshed = match &context.net_handle {
            Some(net_handle) => (net_handle.refresh_document_peers(&config).await)
                .map(|_| ())
                .map_err(|error| error.to_string()),
            None => Err(UNAVAILABLE.to_string()),
        };
        return Event::Net(NetEvent::DocumentPeers(refreshed));
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
            // Sign effects keep their own unavailable answer, as when the runner signed them.
            NetEffect::PolicySign(_) => Event::Net(NetEvent::PolicySign(
                PolicySignEvent::Unavailable(UNAVAILABLE.to_string()),
            )),
            NetEffect::VaultSign(_) => Event::Net(NetEvent::VaultSign(
                VaultSignEvent::Unavailable(UNAVAILABLE.to_string()),
            )),
            NetEffect::SnapshotSign(_) => Event::Net(NetEvent::SnapshotSign(
                SnapshotSignEvent::Unavailable(UNAVAILABLE.to_string()),
            )),
            NetEffect::StatementSign(_) => Event::Net(NetEvent::StatementSign(
                StatementSignEvent::Unavailable(UNAVAILABLE.to_string()),
            )),
            NetEffect::GrantSign(_) => Event::Net(NetEvent::GrantSign(None)),
            NetEffect::ExecutionSign(_) => Event::Net(NetEvent::ExecutionSign(
                ExecutionSignEvent::Unavailable(UNAVAILABLE.to_string()),
            )),
            NetEffect::JobControl(_) => {
                Event::Net(NetEvent::JobControl(JobControlEvent::Unavailable(
                    "job owner unavailable: network handle unavailable".to_string(),
                )))
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
