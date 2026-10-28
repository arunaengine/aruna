//! Promotes a server node to a management node: hands it the realm key, then marks it in config.
//! The key travels only over the node-to-node metadata stream; the node applies it at its next start.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, sleep_until, timeout_at};

use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{NODE_STATE_KEYSPACE, PROMOTED_KEY_ROW};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{
    METADATA_REPLICATION_FACTOR, RealmConfigDocument, RealmId, RealmNodeKind,
};
use byteview::ByteView;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::pkcs8::{DecodePrivateKey, EncodePrivateKey};
use thiserror::Error;

use crate::driver::{DriverContext, drive};
use crate::metadata::handle::{MetadataRequestDelivery, MetadataRequestError};
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage, RealmKeyPem};
use crate::metadata::{MetadataHandle, transport_message_kind};
use crate::placement::process_placements::load_realm_config;
use crate::realm::ensure_config::{EnsureConfigError, EnsureConfigOperation, EnsureConfigParams};
use crate::realm::mutate_placement::{is_management, node_kind};

#[derive(Debug, Clone, PartialEq)]
pub struct PromoteNodeConfig {
    /// The serving management node and the realm admin calling it.
    pub actor: Actor,
    pub node_id: NodeId,
    pub realm_signing_key: SigningKey,
}

#[derive(Debug, Error)]
pub enum PromoteNodeError {
    #[error("realm config document missing")]
    ConfigMissing,
    #[error("this node is not a realm management node")]
    NotManagementNode,
    #[error("no realm node {node_id}")]
    NodeNotFound { node_id: NodeId },
    #[error("only another server node can be promoted")]
    NotServer,
    #[error(
        "the node would take over the database requests the current owner keeps, \
         and moving them is not supported yet"
    )]
    OwnerChange,
    #[error("the node could not be reached: {0}")]
    Unreachable(String),
    #[error("the answer of the node was lost; it may hold the realm key: {0}")]
    KeyUncertain(String),
    #[error("the node refused the realm key: {0:?}")]
    Refused(MetadataReadError),
    #[error("the realm key could not be encoded")]
    KeyEncoding,
    #[error(transparent)]
    Ensure(#[from] EnsureConfigError),
}

/// Time one promotion call may take, readiness checks included. It stays 10 seconds under the
/// 30 second management relay timeout, so a relayed caller gets the answer.
pub const CALL_BUDGET: Duration = Duration::from_secs(20);
const READY_INTERVAL: Duration = Duration::from_millis(500);

/// Hands the key first: an unreachable node leaves the realm unchanged and the call can be
/// repeated. Only a confirmed hand-off marks the node as management. `Ok(true)` means the
/// node's own config lists it, so its next start applies the promotion.
pub async fn promote_node(
    context: &Arc<DriverContext>,
    config: PromoteNodeConfig,
) -> Result<bool, PromoteNodeError> {
    let deadline = Instant::now() + CALL_BUDGET;
    let document = load_realm_config(context, config.actor.realm_id)
        .await
        .ok_or(PromoteNodeError::ConfigMissing)?;
    check_target(&document, config.actor.node_id, config.node_id)?;
    let pem = config
        .realm_signing_key
        .to_pkcs8_pem(LineEnding::default())
        .map_err(|_| PromoteNodeError::KeyEncoding)?;
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or_else(|| PromoteNodeError::Unreachable("metadata transport unavailable".into()))?;
    let message = MetadataTransportMessage::HandRealmKey {
        key: RealmKeyPem(pem.to_string()),
    };
    let reply = metadata
        .request_forwarded_write(config.node_id, message)
        .await;
    handoff_outcome(reply)?;
    let target = config.node_id;
    drive(
        EnsureConfigOperation::new(EnsureConfigParams {
            actor: config.actor,
            target_node_id: config.node_id,
            target_node_kind: RealmNodeKind::Management,
            metadata_replication_factor: METADATA_REPLICATION_FACTOR,
            realm_description: String::new(),
            create_if_missing: false,
            reject_kind_mismatch: false,
        }),
        context.as_ref(),
    )
    .await?;
    Ok(wait_ready(deadline, READY_INTERVAL, || ask_ready(metadata, target)).await)
}

async fn ask_ready(metadata: &MetadataHandle, node_id: NodeId) -> bool {
    let check = MetadataTransportMessage::PromotionCheck;
    matches!(
        metadata.request_forwarded_write(node_id, check).await,
        Ok(MetadataTransportMessage::PromotionReady { result: Ok(true) })
    )
}

/// The config change reaches the node by replication, which may lag behind the local write.
/// Checks and pauses all end at `deadline`; a check still waiting then counts as not ready.
async fn wait_ready<F, R>(deadline: Instant, interval: Duration, mut ready: F) -> bool
where
    F: FnMut() -> R,
    R: Future<Output = bool>,
{
    loop {
        if matches!(timeout_at(deadline, ready()).await, Ok(true)) {
            return true;
        }
        let next = Instant::now() + interval;
        if next >= deadline {
            return false;
        }
        sleep_until(next).await;
    }
}

/// A lost answer can follow a stored key, which no later call can take back.
fn handoff_outcome(
    reply: Result<MetadataTransportMessage, MetadataRequestError>,
) -> Result<(), PromoteNodeError> {
    match reply {
        Ok(MetadataTransportMessage::RealmKeyStored { result: Ok(()) }) => Ok(()),
        Ok(MetadataTransportMessage::RealmKeyStored { result: Err(error) }) => {
            Err(PromoteNodeError::Refused(error))
        }
        Ok(other) => Err(PromoteNodeError::Unreachable(format!(
            "unexpected answer {}",
            transport_message_kind(&other)
        ))),
        Err(error) => match error.delivery() {
            MetadataRequestDelivery::DefinitelyNotSent => {
                Err(PromoteNodeError::Unreachable(error.to_string()))
            }
            MetadataRequestDelivery::PossiblySent => {
                Err(PromoteNodeError::KeyUncertain(error.to_string()))
            }
        },
    }
}

/// A listed management node is accepted again, so a retry after a lost answer succeeds.
fn check_target(
    document: &RealmConfigDocument,
    actor: NodeId,
    target: NodeId,
) -> Result<(), PromoteNodeError> {
    if !is_management(document, actor) {
        return Err(PromoteNodeError::NotManagementNode);
    }
    match node_kind(document, target) {
        None => Err(PromoteNodeError::NodeNotFound { node_id: target }),
        Some(RealmNodeKind::User { .. }) => Err(PromoteNodeError::NotServer),
        Some(_) if target == actor => Err(PromoteNodeError::NotServer),
        Some(RealmNodeKind::Server)
            if request_owner(document).is_some_and(|owner| target < owner) =>
        {
            Err(PromoteNodeError::OwnerChange)
        }
        Some(RealmNodeKind::Server | RealmNodeKind::Management) => Ok(()),
    }
}

/// The lowest management node id keeps every database request (see the management relay).
fn request_owner(document: &RealmConfigDocument) -> Option<NodeId> {
    document
        .nodes
        .iter()
        .filter(|node| node.kind == RealmNodeKind::Management)
        .filter_map(|node| node.node_id.parse::<NodeId>().ok())
        .min()
}

/// Stores a handed realm key for the next start of this node.
pub(crate) async fn serve_realm_key(
    context: &Arc<DriverContext>,
    peer: NodeId,
    key: RealmKeyPem,
) -> Result<(), MetadataReadError> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let realm_id = *net_handle.realm_id();
    let document = load_realm_config(context, realm_id)
        .await
        .ok_or(MetadataReadError::Unavailable)?;
    check_handoff(&document, peer, net_handle.node_id(), realm_id, &key)?;
    let write = StorageEffect::Write {
        key_space: NODE_STATE_KEYSPACE.to_string(),
        key: ByteView::from(PROMOTED_KEY_ROW),
        value: ByteView::from(key.0.as_bytes()),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(write).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        _ => Err(MetadataReadError::Unavailable),
    }
}

/// Whether this node's own config lists it as management while it holds a handed key.
pub(crate) async fn serve_promotion_check(
    context: &Arc<DriverContext>,
    peer: NodeId,
) -> Result<bool, MetadataReadError> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let document = load_realm_config(context, *net_handle.realm_id())
        .await
        .ok_or(MetadataReadError::Unavailable)?;
    if !is_management(&document, peer) {
        return Err(MetadataReadError::Forbidden);
    }
    if !is_management(&document, net_handle.node_id()) {
        return Ok(false);
    }
    let read = StorageEffect::Read {
        key_space: NODE_STATE_KEYSPACE.to_string(),
        key: ByteView::from(PROMOTED_KEY_ROW),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.is_some()),
        _ => Err(MetadataReadError::Unavailable),
    }
}

/// Only a current management node may hand the key, never to a device, and only the realm's key.
fn check_handoff(
    document: &RealmConfigDocument,
    peer: NodeId,
    local: NodeId,
    realm_id: RealmId,
    key: &RealmKeyPem,
) -> Result<(), MetadataReadError> {
    if !is_management(document, peer)
        || !matches!(
            node_kind(document, local),
            Some(RealmNodeKind::Server | RealmNodeKind::Management)
        )
    {
        return Err(MetadataReadError::Forbidden);
    }
    let signing_key =
        SigningKey::from_pkcs8_pem(&key.0).map_err(|_| MetadataReadError::Unauthorized)?;
    if signing_key.verifying_key().to_bytes() != *realm_id.as_bytes() {
        return Err(MetadataReadError::Unauthorized);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::metadata::MetadataError;

    /// Node ids in ascending order of `rank`, so a test knows which one owns database requests.
    fn node(rank: u8) -> NodeId {
        let mut nodes: Vec<NodeId> = (1..=10)
            .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
            .collect();
        nodes.sort();
        nodes[usize::from(rank)]
    }

    fn realm() -> (SigningKey, RealmId, RealmConfigDocument) {
        let key = generate_signing_key();
        let realm_id = RealmId::from_bytes(key.verifying_key().to_bytes());
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(node(1), RealmNodeKind::Management);
        document.ensure_node(node(2), RealmNodeKind::Server);
        let owner = UserId::nil(realm_id);
        document.ensure_node(node(3), RealmNodeKind::User { owner });
        (key, realm_id, document)
    }

    fn pem(key: &SigningKey) -> RealmKeyPem {
        RealmKeyPem(key.to_pkcs8_pem(LineEnding::default()).unwrap().to_string())
    }

    #[test]
    fn server_only_target() {
        let (_, _, document) = realm();
        assert!(check_target(&document, node(1), node(2)).is_ok());
        assert!(matches!(
            check_target(&document, node(1), node(3)),
            Err(PromoteNodeError::NotServer)
        ));
        assert!(matches!(
            check_target(&document, node(1), node(1)),
            Err(PromoteNodeError::NotServer)
        ));
        assert!(matches!(
            check_target(&document, node(1), node(9)),
            Err(PromoteNodeError::NodeNotFound { .. })
        ));
        assert!(matches!(
            check_target(&document, node(2), node(1)),
            Err(PromoteNodeError::NotManagementNode)
        ));
    }

    #[test]
    fn owner_change_refused() {
        let (_, _, mut document) = realm();
        document.ensure_node(node(0), RealmNodeKind::Server);
        assert!(matches!(
            check_target(&document, node(1), node(0)),
            Err(PromoteNodeError::OwnerChange)
        ));
        document.ensure_node(node(0), RealmNodeKind::Management);
        assert!(
            check_target(&document, node(1), node(0)).is_ok(),
            "a node that already owns the requests is accepted again"
        );
    }

    #[test]
    fn retry_accepts_promoted() {
        let (_, _, mut document) = realm();
        document.ensure_node(node(2), RealmNodeKind::Management);
        assert!(check_target(&document, node(1), node(2)).is_ok());
    }

    #[test]
    fn handoff_checks_sender() {
        let (key, realm_id, document) = realm();
        let key = pem(&key);
        assert_eq!(
            check_handoff(&document, node(1), node(2), realm_id, &key),
            Ok(())
        );
        assert_eq!(
            check_handoff(&document, node(2), node(2), realm_id, &key),
            Err(MetadataReadError::Forbidden),
            "a server node may not hand a key"
        );
        assert_eq!(
            check_handoff(&document, node(1), node(3), realm_id, &key),
            Err(MetadataReadError::Forbidden),
            "a device never stores the realm key"
        );
        assert_eq!(
            check_handoff(&document, node(1), node(9), realm_id, &key),
            Err(MetadataReadError::Forbidden)
        );
    }

    #[test]
    fn handoff_rejects_foreign() {
        let (_, realm_id, document) = realm();
        let foreign = pem(&generate_signing_key());
        assert_eq!(
            check_handoff(&document, node(1), node(2), realm_id, &foreign),
            Err(MetadataReadError::Unauthorized)
        );
        let garbage = RealmKeyPem("not a key".to_string());
        assert_eq!(
            check_handoff(&document, node(1), node(2), realm_id, &garbage),
            Err(MetadataReadError::Unauthorized)
        );
    }

    #[test]
    fn lost_answer_uncertain() {
        let lost = MetadataRequestError::possibly_sent(MetadataError::ChannelClosed);
        assert!(matches!(
            handoff_outcome(Err(lost)),
            Err(PromoteNodeError::KeyUncertain(_))
        ));
        let refused = MetadataTransportMessage::RealmKeyStored {
            result: Err(MetadataReadError::Forbidden),
        };
        assert!(matches!(
            handoff_outcome(Ok(refused)),
            Err(PromoteNodeError::Refused(MetadataReadError::Forbidden))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn waits_for_replication() {
        let asked = std::cell::Cell::new(0);
        let deadline = Instant::now() + CALL_BUDGET;
        let ready = wait_ready(deadline, Duration::from_secs(1), || {
            asked.set(asked.get() + 1);
            let replicated = asked.get() >= 3;
            async move { replicated }
        })
        .await;
        assert!(ready, "a lagging config change is waited for");
        assert_eq!(asked.get(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_checks_bounded() {
        let started = Instant::now();
        let asked = std::cell::Cell::new(0);
        let ready = wait_ready(started + CALL_BUDGET, READY_INTERVAL, || {
            asked.set(asked.get() + 1);
            let replicated = asked.get() >= 2;
            async move {
                tokio::time::sleep(Duration::from_secs(15)).await;
                replicated
            }
        })
        .await;
        assert!(
            !ready,
            "a check still waiting at the deadline counts as not ready"
        );
        assert_eq!(asked.get(), 2);
        assert_eq!(
            started.elapsed(),
            CALL_BUDGET,
            "the wait ends at the deadline"
        );
    }

    #[test]
    fn key_debug_redacted() {
        let (key, _, _) = realm();
        let message = MetadataTransportMessage::HandRealmKey { key: pem(&key) };
        assert!(!format!("{message:?}").contains("PRIVATE KEY"));
    }
}
