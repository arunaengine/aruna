//! A device's copy of the realm-wide documents.
//!
//! A device runs no document sync, so nothing pushes the realm configuration to
//! it. It fetches the documents from a realm node as an ordinary routed read
//! and installs the copies locally: they are read-only state it is judged by -
//! node kinds, its owner binding, quotas and token revocations - and it never
//! publishes them again.

use std::str::FromStr;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::metadata::MetadataAuthToken;
use aruna_core::structs::{AuthContext, RealmConfigDocument, RealmId, SyncRefusal};
use aruna_core::types::UserId;
use tracing::{debug, warn};

use crate::driver::DriverContext;
use crate::metadata::api::load_realm_config;
use crate::metadata::protocol::{MetadataTransportMessage, RealmDocuments};
use crate::mutate_realm_placement::node_kind;

/// Fetches the realm-wide documents and installs them on this device.
///
/// Answers whether a realm node served them. A node that is not a device, or
/// one whose configuration names no realm peer yet, answers `false` without
/// asking anybody.
pub async fn fetch_realm_documents(context: &Arc<DriverContext>) -> bool {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return false;
    };
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return false;
    };
    let realm_id = *net_handle.realm_id();
    let node_id = net_handle.node_id();
    let Some(config) = load_realm_config(context, realm_id).await else {
        return false;
    };
    let Some(owner) = node_kind(&config, node_id).and_then(|kind| kind.owner()) else {
        return false;
    };
    let auth = AuthContext {
        user_id: owner,
        realm_id,
        path_restrictions: None,
    };

    for peer in realm_peers(&config, node_id) {
        let message = MetadataTransportMessage::FetchRealmDocuments {
            auth_token: MetadataAuthToken::internal(auth.clone()),
        };
        match metadata.request_forwarded_write(peer, message).await {
            Ok(MetadataTransportMessage::FetchedRealmDocuments {
                result: Ok(documents),
            }) => {
                if install_documents(context, realm_id, owner, documents).await {
                    return true;
                }
            }
            Ok(MetadataTransportMessage::FetchedRealmDocuments {
                result: Err(refusal),
            }) => {
                debug!(peer = %peer, refusal = ?refusal, "A realm node refused the document fetch");
                // An authorization verdict is the same at every peer.
                if matches!(refusal, SyncRefusal::Unauthorized | SyncRefusal::Forbidden) {
                    return false;
                }
            }
            Ok(other) => {
                debug!(peer = %peer, message = %crate::metadata::transport_message_kind(&other), "Unexpected answer to a document fetch");
            }
            Err(error) => {
                debug!(peer = %peer, error = %error, "Could not reach a realm node for the documents");
            }
        }
    }
    false
}

/// The realm's own nodes, in configuration order. A device asks infrastructure
/// only: another device holds nothing it could serve.
fn realm_peers(config: &RealmConfigDocument, node_id: NodeId) -> Vec<NodeId> {
    config
        .nodes
        .iter()
        .filter(|node| node.kind.is_sync_eligible())
        .filter_map(|node| NodeId::from_str(&node.node_id).ok())
        .filter(|peer| *peer != node_id)
        .collect()
}

/// Writes the fetched copies where every local read already looks for them.
/// A document that does not decode, or that names another realm, is refused:
/// this is the state the device authorizes itself against.
async fn install_documents(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    owner: UserId,
    documents: RealmDocuments,
) -> bool {
    match RealmConfigDocument::from_bytes(&documents.realm_config) {
        Ok(config) if config.realm_id == realm_id => {}
        Ok(_) => {
            warn!("A realm node served the configuration of another realm");
            return false;
        }
        Err(error) => {
            warn!(error = %error, "A fetched realm configuration does not decode");
            return false;
        }
    }
    let mut writes = vec![(
        DocumentSyncTarget::RealmConfig { realm_id },
        documents.realm_config,
    )];
    if let Some(authorization) = documents.realm_authorization {
        writes.push((
            DocumentSyncTarget::RealmAuthorization { realm_id },
            authorization,
        ));
    }
    for (target, bytes) in writes {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: bytes.into(),
                txn_id: None,
            })
            .await;
        if !matches!(event, Event::Storage(StorageEvent::WriteResult { .. })) {
            warn!(event = ?event, "Failed to install a fetched realm document");
            return false;
        }
    }
    // The peer set and the node kinds this device enforces follow the copy it
    // just installed, exactly as they follow a synced one on a realm node.
    if let Some(net_handle) = context.net_handle.as_ref()
        && let Some(config) = load_realm_config(context, realm_id).await
        && let Err(error) = net_handle.refresh_realm_peers_from_document(&config).await
    {
        warn!(error = %error, "Failed to apply the fetched realm configuration");
    }
    debug!(owner = %owner, "Installed the realm documents on this device");
    true
}
