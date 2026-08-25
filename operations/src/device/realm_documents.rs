//! A device's copy of the realm-wide documents.
//!
//! A device runs no document sync, so nothing pushes the realm configuration to
//! it. It fetches the documents from a realm node as an ordinary routed read
//! and installs the copies locally: read-only state it is judged by - node
//! kinds, its owner binding, quotas and token revocations - never published on.
//!
//! What it installs never regresses. A copy whose realm-config clock has seen
//! less than the installed one is refused, and every revocation the device
//! already holds survives whatever the answer says, so neither a lagging node
//! nor one the realm evicted can hand a device back a revoked token.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::admin_documents::AdminDocumentClock;
use aruna_core::auth::revocation_live;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DEVICE_REALM_MARKER_KEYSPACE;
use aruna_core::metadata::MetadataAuthToken;
use aruna_core::structs::{Actor, AuthContext, RealmConfigDocument, RealmId, SyncRefusal};
use aruna_core::types::UserId;
use aruna_core::util::unix_timestamp_secs;
use rand::seq::SliceRandom;
use tracing::{debug, warn};

use crate::driver::DriverContext;
use crate::metadata::api::load_realm_config;
use crate::metadata::protocol::{MetadataTransportMessage, RealmDocuments};
use crate::mutate_realm_placement::node_kind;

/// One fetch attempt: who this device is, whom it may ask, and how far the copy
/// it already holds has seen.
struct FetchPlan {
    realm_id: RealmId,
    node_id: NodeId,
    owner: UserId,
    auth: AuthContext,
    peers: Vec<NodeId>,
    installed: AdminDocumentClock,
}

/// Fetches the realm-wide documents and installs them on this device.
///
/// `budget` bounds the whole exchange: a realm nobody answers for costs that
/// much once, and the stored copy keeps serving until the next attempt.
pub async fn fetch_realm_documents(context: &Arc<DriverContext>, budget: Duration) -> bool {
    let Some(plan) = fetch_plan(context).await else {
        return false;
    };
    let Ok(Some(documents)) = tokio::time::timeout(budget, ask_realm(context, &plan)).await else {
        return false;
    };
    // The install is deliberately outside the budget: half of it would leave the
    // device with a configuration and an authorization document from two ages.
    install_documents(context, &plan, documents).await
}

async fn fetch_plan(context: &Arc<DriverContext>) -> Option<FetchPlan> {
    let net_handle = context.net_handle.as_ref()?;
    context.metadata_handle.as_ref()?;
    let realm_id = *net_handle.realm_id();
    let node_id = net_handle.node_id();
    let config = load_realm_config(context, realm_id).await?;
    let owner = node_kind(&config, node_id).and_then(|kind| kind.owner())?;
    let mut peers = realm_peers(&config, node_id);
    // A different node answers first every time, so one lagging peer never owns
    // this device's view of the realm.
    peers.shuffle(&mut rand::rng());
    Some(FetchPlan {
        realm_id,
        node_id,
        owner,
        auth: AuthContext {
            user_id: owner,
            realm_id,
            path_restrictions: None,
        },
        peers,
        installed: installed_clock(context, realm_id).await,
    })
}

/// Asks the realm's nodes in turn until one answers with a copy that has seen
/// at least as much as the installed one.
async fn ask_realm(context: &Arc<DriverContext>, plan: &FetchPlan) -> Option<RealmDocuments> {
    let metadata = context.metadata_handle.as_ref()?;
    for peer in &plan.peers {
        let message = MetadataTransportMessage::FetchRealmDocuments {
            auth_token: MetadataAuthToken::internal(plan.auth.clone()),
        };
        match metadata.request_forwarded_write(*peer, message).await {
            Ok(MetadataTransportMessage::FetchedRealmDocuments {
                result: Ok(documents),
            }) => {
                if !covers(&documents.clock, &plan.installed) {
                    debug!(peer = %peer, "A realm node offered an older realm configuration");
                    continue;
                }
                return Some(documents);
            }
            Ok(MetadataTransportMessage::FetchedRealmDocuments {
                result: Err(refusal),
            }) => {
                debug!(peer = %peer, refusal = ?refusal, "A realm node refused the document fetch");
                // An authorization verdict is the same at every peer.
                if matches!(refusal, SyncRefusal::Unauthorized | SyncRefusal::Forbidden) {
                    return None;
                }
            }
            Ok(other) => {
                debug!(
                    peer = %peer,
                    message = %crate::metadata::transport_message_kind(&other),
                    "Unexpected answer to a document fetch"
                );
            }
            Err(error) => {
                debug!(peer = %peer, error = %error, "Could not reach a realm node for the documents");
            }
        }
    }
    None
}

/// Whether `offered` has seen everything `installed` has. A copy missing an
/// origin's events is a rollback, whichever node served it.
fn covers(offered: &AdminDocumentClock, installed: &AdminDocumentClock) -> bool {
    installed
        .origins
        .iter()
        .all(|(origin, seq)| offered.sequence_for(origin) >= *seq)
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
    plan: &FetchPlan,
    documents: RealmDocuments,
) -> bool {
    let mut config = match RealmConfigDocument::from_bytes(&documents.realm_config) {
        Ok(config) if config.realm_id == plan.realm_id => config,
        Ok(_) => {
            warn!("A realm node served the configuration of another realm");
            return false;
        }
        Err(error) => {
            warn!(error = %error, "A fetched realm configuration does not decode");
            return false;
        }
    };
    let stored_config = read_bytes(
        context,
        DocumentSyncTarget::RealmConfig {
            realm_id: plan.realm_id,
        },
    )
    .await;
    if let Some(installed) = stored_config
        .as_deref()
        .and_then(|bytes| RealmConfigDocument::from_bytes(bytes).ok())
    {
        keep_revocations(&mut config, &installed, unix_timestamp_secs());
    }
    let actor = Actor {
        node_id: plan.node_id,
        user_id: plan.owner,
        realm_id: plan.realm_id,
    };
    let Ok(bytes) = config.to_bytes(&actor) else {
        warn!("A fetched realm configuration could not be stored");
        return false;
    };
    // Nothing changed: writing it again would re-register every realm peer on
    // every beat for a copy the device already holds.
    let stored_authorization = read_bytes(
        context,
        DocumentSyncTarget::RealmAuthorization {
            realm_id: plan.realm_id,
        },
    )
    .await;
    if stored_config.as_deref() == Some(bytes.as_slice())
        && stored_authorization == documents.realm_authorization
    {
        debug!("This device already holds the realm documents it fetched");
        return true;
    }
    let mut writes = vec![(
        DocumentSyncTarget::RealmConfig {
            realm_id: plan.realm_id,
        },
        bytes,
    )];
    if let Some(authorization) = documents.realm_authorization {
        writes.push((
            DocumentSyncTarget::RealmAuthorization {
                realm_id: plan.realm_id,
            },
            authorization,
        ));
    }
    for (target, bytes) in writes {
        if !write_bytes(
            context,
            target.storage_keyspace(),
            target.storage_key(),
            bytes,
        )
        .await
        {
            return false;
        }
    }
    // The marker is written last: a copy that was only half installed must be
    // fetched again rather than counted as the state this device holds.
    let Ok(marker) = postcard::to_allocvec(&documents.clock) else {
        return false;
    };
    if !write_bytes(
        context,
        DEVICE_REALM_MARKER_KEYSPACE,
        plan.realm_id.as_bytes().to_vec().into(),
        marker,
    )
    .await
    {
        return false;
    }
    // The peer set and the node kinds this device enforces follow the copy it
    // just installed, exactly as they follow a synced one on a realm node.
    if let Some(net_handle) = context.net_handle.as_ref()
        && let Err(error) = net_handle.refresh_realm_peers_from_document(&config).await
    {
        warn!(error = %error, "Failed to apply the fetched realm configuration");
    }
    debug!(owner = %plan.owner, "Installed the realm documents on this device");
    true
}

/// Carries the device's own revocations into the copy it is about to install.
/// A revoked token stays revoked here whatever the answer forgot, and the floor
/// only ever rises.
fn keep_revocations(fetched: &mut RealmConfigDocument, installed: &RealmConfigDocument, now: u64) {
    fetched.revocation_floor = fetched.revocation_floor.max(installed.revocation_floor);
    for entry in &installed.revoked_tokens {
        if !revocation_live(entry.expires_at, now) {
            continue;
        }
        if !fetched
            .revoked_tokens
            .iter()
            .any(|kept| kept.token_hash == entry.token_hash)
        {
            fetched.revoked_tokens.push(entry.clone());
        }
    }
}

/// How far the copy this device holds had seen. An absent marker reads as
/// nothing seen, which accepts the first copy and refuses nothing.
async fn installed_clock(context: &Arc<DriverContext>, realm_id: RealmId) -> AdminDocumentClock {
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(bytes), ..
    }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: DEVICE_REALM_MARKER_KEYSPACE.to_string(),
            key: realm_id.as_bytes().to_vec().into(),
            txn_id: None,
        })
        .await
    else {
        return AdminDocumentClock::default();
    };
    postcard::from_bytes(&bytes).unwrap_or_default()
}

/// One stored document, or `None` when this device holds it not (yet).
async fn read_bytes(context: &Arc<DriverContext>, target: DocumentSyncTarget) -> Option<Vec<u8>> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            value.map(|bytes| bytes.as_ref().to_vec())
        }
        other => {
            warn!(event = ?other, "Failed to read an installed realm document");
            None
        }
    }
}

async fn write_bytes(
    context: &Arc<DriverContext>,
    key_space: &str,
    key: aruna_core::types::Key,
    value: Vec<u8>,
) -> bool {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key,
            value: value.into(),
            txn_id: None,
        })
        .await;
    if matches!(event, Event::Storage(StorageEvent::WriteResult { .. })) {
        return true;
    }
    warn!(event = ?event, "Failed to install a fetched realm document");
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::TokenRevocation;

    fn clock(entries: &[(u8, u64)]) -> AdminDocumentClock {
        let mut clock = AdminDocumentClock::default();
        for (seed, seq) in entries {
            clock.advance(iroh::SecretKey::from_bytes(&[*seed; 32]).public(), *seq);
        }
        clock
    }

    // A copy that has seen less than the installed one is a rollback, whoever
    // serves it: the realm's own lagging node and an evicted one look the same.
    #[test]
    fn refuses_older_copy() {
        let installed = clock(&[(1, 7), (2, 3)]);
        assert!(covers(&installed, &installed));
        assert!(covers(&clock(&[(1, 8), (2, 3), (3, 1)]), &installed));
        assert!(!covers(&clock(&[(1, 6), (2, 3)]), &installed));
        assert!(!covers(&clock(&[(1, 7)]), &installed));
        // Nothing installed yet accepts the first answer.
        assert!(covers(
            &AdminDocumentClock::default(),
            &AdminDocumentClock::default()
        ));
    }

    fn config(revoked: &[(&str, u64)], floor: u64) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([9u8; 32]), Vec::new(), 1);
        config.revocation_floor = floor;
        config.revoked_tokens = revoked
            .iter()
            .map(|(hash, expires_at)| TokenRevocation {
                token_hash: (*hash).to_string(),
                expires_at: *expires_at,
            })
            .collect();
        config
    }

    // A revocation this device already holds must survive a copy that lost it,
    // and an expired one must not come back with it.
    #[test]
    fn keeps_local_revocations() {
        let now = 1_000;
        let installed = config(&[("live", now + 60), ("gone", now - 60)], 900);
        let mut fetched = config(&[("other", now + 60)], 800);

        keep_revocations(&mut fetched, &installed, now);

        let hashes: Vec<&str> = fetched
            .revoked_tokens
            .iter()
            .map(|entry| entry.token_hash.as_str())
            .collect();
        assert!(
            hashes.contains(&"live"),
            "a live revocation may not be lost"
        );
        assert!(hashes.contains(&"other"));
        assert!(!hashes.contains(&"gone"), "an expired entry stays pruned");
        assert_eq!(fetched.revocation_floor, 900);
    }
}
