//! Promotes a server node to a management node: hands it the realm key, then marks it in config.
//! The key travels only over the node-to-node metadata stream; the node applies it at its next start.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

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
use crate::metadata::transport_message_kind;
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

/// Hands the key first: an unreachable node leaves the realm unchanged and the call can be
/// repeated. Only a confirmed hand-off marks the node as management.
pub async fn promote_node(
    context: &Arc<DriverContext>,
    config: PromoteNodeConfig,
) -> Result<(), PromoteNodeError> {
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
    Ok(())
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
        Some(RealmNodeKind::Server | RealmNodeKind::Management) => Ok(()),
    }
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

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
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

    #[test]
    fn key_debug_redacted() {
        let (key, _, _) = realm();
        let message = MetadataTransportMessage::HandRealmKey { key: pem(&key) };
        assert!(!format!("{message:?}").contains("PRIVATE KEY"));
    }
}
