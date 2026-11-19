//! Sends net effects through the net handle and answers them when this node has none.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_blob::native::{body_stream, send_request};
use std::sync::Arc;

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
use aruna_core::member_read::{MemberFetch, TrailerSeal};
use aruna_core::native_reference::{
    DelegatedBody, DelegatedReadEffect, GrantedBody, GrantedReadEffect, NativeAuthority,
    NativeReferenceRequest, NativeReferenceResponse,
};
use aruna_core::outbound::{OutboundEvent, RangeListing};
use aruna_core::structs::storage::distribution::delegation::{
    BodyTrailer, Delegation, SignedDelegation,
};
use aruna_core::structs::storage::read_grant::SignedRequest;
use aruna_core::{DocumentEffect, DocumentNetEvent, DocumentTarget};
use aruna_net::NetHandle;
use aruna_outbound::ListingSigner;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;
use tracing::warn;

use crate::driver::DriverContext;
use aruna_core::events::TopicPeerError;

/// Signs the delegation of a range listing with this node's key.
struct NodeSigner<'a>(Option<&'a NetHandle>);

impl ListingSigner for NodeSigner<'_> {
    fn sign(&self, delegation: Delegation) -> Option<SignedDelegation> {
        let bytes = delegation.signing_bytes().ok()?;
        Some(SignedDelegation {
            signature: self.0?.sign(&bytes),
            delegation,
        })
    }
}

/// Lists one key range through the outbound backend, signing with the net handle's key.
pub(super) async fn dispatch_listing(listing: RangeListing, context: &DriverContext) -> Event {
    let signer = NodeSigner(context.net_handle.as_ref());
    let answer = aruna_outbound::list_range(listing, &signer).await;
    Event::Outbound(OutboundEvent::RangeListed(Box::new(answer)))
}

/// Seals the trailer of a framed member body with the net handle's key.
struct NodeSeal(Option<NetHandle>);

impl TrailerSeal for NodeSeal {
    fn seal(&self, trailer: &BodyTrailer, nonce: &[u8; 16]) -> Option<Vec<u8>> {
        let signature = self.0.as_ref()?.sign(&trailer.signing_bytes(nonce));
        Some(trailer.encode(&signature).ok()?.to_vec())
    }
}

/// Sends one request to another member through the outbound backend.
pub(super) async fn dispatch_fetch(request: MemberFetch, context: &DriverContext) -> Event {
    let seal = Arc::new(NodeSeal(context.net_handle.clone()));
    let answer = aruna_outbound::fetch_member(request, seal).await;
    Event::Outbound(OutboundEvent::MemberAnswered(Box::new(answer)))
}

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

/// Carries out a change of this node's watch metrics or watch interest, or reads the holders it
/// matches; without a net handle only the suppression is logged.
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
        NetEffect::WatchHolders {
            realm_id,
            path,
            kind,
        } => {
            let holders = net_handle.map(|net_handle| {
                let interest = net_handle.watch_interest_snapshot();
                let holders = interest.matching_nodes(realm_id, &path, kind);
                (net_handle.node_id(), holders)
            });
            return Ok(Event::Net(NetEvent::WatchHolders(holders)));
        }
        other => return Err(other),
    }
    Ok(Event::Net(NetEvent::Recorded))
}

/// Runs the topic peer effects through the handle; hands any other effect back.
async fn set_topic_peers(effect: NetEffect, context: &DriverContext) -> Result<Event, NetEffect> {
    let (topics, peers, create) = match effect {
        NetEffect::EnsureTopics { topics, peers } => (topics, peers, true),
        NetEffect::AllowTopicPeers { topics, peers } => (topics, peers, false),
        other => return Err(other),
    };
    let result = match &context.net_handle {
        None => Err(TopicPeerError::NoNetwork),
        Some(net) => match create {
            true => net.ensure_sync_topics(&topics, peers).await,
            false => net.allow_topic_peers(&topics, peers).await,
        }
        .map_err(|error| TopicPeerError::Failed(error.to_string())),
    };
    Ok(Event::Net(NetEvent::TopicPeers(result)))
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
    let effect = match set_topic_peers(effect, context).await {
        Ok(answered) => return answered,
        Err(effect) => effect,
    };
    if let NetEffect::GrantedRead(read) = effect {
        return Event::Net(NetEvent::GrantedRead(granted_read(*read, context).await));
    }
    if let NetEffect::AvoidHolder { node_id, now_ms } = effect {
        context.avoid_holder(node_id, now_ms);
        return Event::Net(NetEvent::HolderAvoided);
    }
    if let NetEffect::DelegatedRead(read) = effect {
        let answer = delegated_read(*read, context).await;
        return Event::Net(NetEvent::DelegatedRead(answer));
    }
    if let NetEffect::DocumentSync(DocumentEffect::ReloadRealmPeers) = effect {
        return Event::Net(NetEvent::DocumentSync(reload_peers(context).await));
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
            NetEffect::SignDeletePlan(_) => {
                Event::Net(NetEvent::DeletePlanSigned(Err(UNAVAILABLE.to_string())))
            }
            NetEffect::SnapshotSign(_) => Event::Net(NetEvent::SnapshotSign(
                SnapshotSignEvent::Unavailable(UNAVAILABLE.to_string()),
            )),
            NetEffect::StatementSign(_) => Event::Net(NetEvent::StatementSign(
                StatementSignEvent::Unavailable(UNAVAILABLE.to_string()),
            )),
            NetEffect::GrantSign(_) => Event::Net(NetEvent::GrantSign(None)),
            NetEffect::DelegationSign(_) => Event::Net(NetEvent::DelegationSign(None)),
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

/// Rebuilds the peer set from the realm config a reconcile just applied.
async fn reload_peers(context: &DriverContext) -> DocumentNetEvent {
    let Some(net_handle) = &context.net_handle else {
        return DocumentNetEvent::Error {
            target: None,
            error: "net handle unavailable".to_string(),
        };
    };
    match Box::pin(net_handle.reload_realm_peers()).await {
        Ok(_) => DocumentNetEvent::RealmPeersReloaded,
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
