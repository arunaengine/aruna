//! Adapter I/O for [`PolicyFetchEffect`], kept out of the sans-I/O operations.
//! One request per resolved holder, in rank order, over the existing control
//! transport; nothing here routes or widens the holder list.

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, PolicyFetchEffect, StorageEffect};
use aruna_core::events::{Event, PolicyFetchEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::structs::{
    PlacementPolicy, PlacementPolicyDocument, PlacementPolicyRef, RealmConfigDocument, RealmId,
    VerifiedPolicy, placement_policy_target,
};
use tokio::time::timeout_at;
use tracing::warn;

use crate::driver::DriverContext;
use crate::metadata::api::load_realm_config;
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage};
use crate::metadata::transport_message_kind;

/// Asks each holder in turn for the document. A holder that answers without it
/// contributes `NotFound`; only a complete absence of answers is `Unavailable`,
/// so a missing policy can never be reported as a definitive denial.
pub(crate) async fn fetch_policy(
    context: &DriverContext,
    effect: PolicyFetchEffect,
) -> PolicyFetchEvent {
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return PolicyFetchEvent::Unavailable("metadata transport unavailable".to_string());
    };
    let deadline = tokio::time::Instant::now() + effect.deadline;
    let mut answered = false;
    for holder in effect.holders.as_slice() {
        let request = MetadataTransportMessage::ForwardPlacementPolicy {
            policy_ref: effect.policy_ref,
        };
        let reply =
            match timeout_at(deadline, metadata.request_forwarded_write(*holder, request)).await {
                Ok(Ok(reply)) => reply,
                Ok(Err(error)) => {
                    warn!(peer = %holder, error = %error, "Policy fetch failed");
                    continue;
                }
                Err(_) => break,
            };
        match reply {
            MetadataTransportMessage::ForwardedPlacementPolicy {
                result: Ok(Some(policy)),
            } => {
                return PolicyFetchEvent::Fetched {
                    publisher: *holder,
                    policy,
                };
            }
            MetadataTransportMessage::ForwardedPlacementPolicy { result: Ok(None) } => {
                answered = true;
            }
            MetadataTransportMessage::ForwardedPlacementPolicy { result: Err(error) } => {
                warn!(peer = %holder, error = ?error, "Policy holder refused the fetch");
            }
            other => {
                // Only the discriminant: a peer reply may carry a bearer token.
                warn!(
                    peer = %holder,
                    reply = transport_message_kind(&other),
                    "Unexpected policy fetch reply"
                );
            }
        }
    }
    if answered {
        return PolicyFetchEvent::NotFound;
    }
    PolicyFetchEvent::Unavailable("no policy holder answered".to_string())
}

/// Serves one policy document to a peer. The peer must be a sync-eligible node
/// of this realm, and the answer must hash to the requested ref.
pub(crate) async fn serve_local_policy(
    context: &Arc<DriverContext>,
    peer: NodeId,
    policy_ref: PlacementPolicyRef,
) -> MetadataTransportMessage {
    MetadataTransportMessage::ForwardedPlacementPolicy {
        result: local_policy_result(context, peer, policy_ref).await,
    }
}

async fn local_policy_result(
    context: &Arc<DriverContext>,
    peer: NodeId,
    policy_ref: PlacementPolicyRef,
) -> Result<Option<Box<PlacementPolicy>>, MetadataReadError> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let realm_id = *net_handle.realm_id();
    let config = load_realm_config(context.as_ref(), realm_id)
        .await
        .ok_or(MetadataReadError::Unavailable)?;
    if !sync_eligible_peer(&config, peer) {
        return Err(MetadataReadError::Forbidden);
    }
    let Some(document) = read_policy(context, realm_id, policy_ref).await? else {
        return Ok(None);
    };
    Ok(Some(Box::new(document)))
}

fn sync_eligible_peer(config: &RealmConfigDocument, peer: NodeId) -> bool {
    config
        .sync_eligible_node_ids()
        .is_ok_and(|eligible| eligible.contains(&peer))
}

async fn read_policy(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    policy_ref: PlacementPolicyRef,
) -> Result<Option<PlacementPolicy>, MetadataReadError> {
    let target = placement_policy_target(policy_ref.policy_id);
    let event = context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        }))
        .await;
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        return Err(MetadataReadError::Unavailable);
    };
    let Some(value) = value else {
        return Ok(None);
    };
    let document =
        PlacementPolicyDocument::from_bytes(&value).map_err(|_| MetadataReadError::Unavailable)?;
    // A stored row that no longer matches the requested ref is not this policy:
    // answering with it would let one id resolve to two definitions.
    if document.realm_id != realm_id {
        return Ok(None);
    }
    match VerifiedPolicy::verify(document.policy.clone()) {
        Ok(verified) if verified.policy_ref() == policy_ref => Ok(Some(document.policy)),
        Ok(_) => Ok(None),
        Err(_) => Err(MetadataReadError::Unavailable),
    }
}
