//! Routes vault changes to a holder and serves vault fetches between realm nodes.
//! A holder checks the forwarded bearer token itself and serves a vault only to its own user.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::effects::VaultQuery;
use aruna_core::keyspaces::{USER_KEY_KEYSPACE, VAULT_REVISION_KEYSPACE};
use aruna_core::metadata::AuthToken;
use aruna_core::structs::identity::user::vault::{
    MAX_KEY_RECORDS, MAX_VAULT_HEADS, UserKeyRecord, VaultRecords, VaultRevision,
    retain_newest_keys, user_record_prefix,
};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::write::{
    AppendVaultConfig, AppendVaultError, AppendVaultOperation, VaultAppended, VaultRefusal,
};
use crate::document_repository::GetConfigOperation;
use crate::driver::{DriverContext, drive, drive_retry};
use crate::forward::routing::forward_to_holders;
use crate::forward::transport::MetadataWriteError;
use crate::jobs::store::iter_prefix_page;
use crate::metadata::handle::WritePeerError;
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage, WriteAuthError};
use crate::metadata::transport_message_kind;
use crate::realm::peer_trust::{is_sync_eligible, peer_acts_for};
use aruna_core::placement::{holds_placement, target_placement_ref};

#[derive(Debug, Error)]
pub enum VaultRouteError {
    #[error(transparent)]
    Append(#[from] AppendVaultError),
    #[error(transparent)]
    Forward(#[from] MetadataWriteError),
    #[error("a vault holder refused the change: {0:?}")]
    Refused(VaultRefusal),
}

/// Appends on this node when it holds the user's vault, otherwise on one of the
/// holders; a change no holder accepts fails instead of waiting in an outbox (D10).
pub async fn append_vault_routed(
    context: &Arc<DriverContext>,
    config: AppendVaultConfig,
    auth_token: Option<AuthToken>,
) -> Result<VaultAppended, VaultRouteError> {
    let holders = match append_local(context, config.clone()).await {
        Err(AppendVaultError::NotHolder { holders }) => holders,
        result => return Ok(result?),
    };
    let message = MetadataTransportMessage::ForwardVaultChange {
        auth_token,
        record_id: config.record_id,
        change: Box::new(config.change),
        created_at_ms: config.now_ms,
    };
    match forward_to_holders(context, &holders, message, None, false).await? {
        MetadataTransportMessage::ForwardedVaultChange { result } => {
            result.map_err(VaultRouteError::Refused)
        }
        other => Err(MetadataWriteError::Undeliverable(format!(
            "holder answered a vault change with {}",
            transport_message_kind(&other)
        ))
        .into()),
    }
}

/// Retries a save that lost its transaction to a concurrent one on this holder.
async fn append_local(
    context: &Arc<DriverContext>,
    config: AppendVaultConfig,
) -> Result<VaultAppended, AppendVaultError> {
    let seed = config.record_id.to_bytes();
    let build = || AppendVaultOperation::new(config.clone());
    drive_retry(build, context.as_ref(), &seed).await
}

/// Runs one forwarded vault change for the user the forwarded token names.
pub(crate) async fn apply_forwarded_vault(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let MetadataTransportMessage::ForwardVaultChange {
        auth_token,
        record_id,
        change,
        created_at_ms,
    } = message
    else {
        return MetadataTransportMessage::Reject("unexpected vault change".to_string());
    };
    let Some(net_handle) = context.net_handle.as_ref() else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    let user_id = match vault_caller(context, peer, auth_token).await {
        Ok(user_id) => user_id,
        Err(MetadataReadError::Unauthorized) => {
            return MetadataTransportMessage::ForwardedWriteDenied {
                error: WriteAuthError::Unauthorized,
            };
        }
        Err(MetadataReadError::Forbidden) => {
            return MetadataTransportMessage::ForwardedWriteDenied {
                error: WriteAuthError::Forbidden,
            };
        }
        Err(_) => return MetadataTransportMessage::ForwardedWriteUnavailable,
    };
    let config = AppendVaultConfig {
        node_id: net_handle.node_id(),
        realm_id: *net_handle.realm_id(),
        user_id,
        record_id,
        change: *change,
        now_ms: created_at_ms,
    };
    let result = match append_local(context, config).await {
        Ok(appended) => Ok(appended),
        Err(AppendVaultError::Record(_)) => Err(VaultRefusal::Invalid),
        Err(AppendVaultError::TooManyKeys) => Err(VaultRefusal::TooManyKeys),
        // Another holder may still accept it.
        Err(error) => {
            warn!(%error, "Forwarded vault change failed");
            return MetadataTransportMessage::ForwardedWriteUnavailable;
        }
    };
    MetadataTransportMessage::ForwardedVaultChange { result }
}

/// The user a forwarded bearer token names, checked again on this node. Internal
/// principals and path-restricted tokens never reach a vault.
async fn vault_caller(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: Option<AuthToken>,
) -> Result<UserId, MetadataReadError> {
    let (Some(metadata), Some(net_handle)) = (
        context.metadata_handle.as_ref(),
        context.net_handle.as_ref(),
    ) else {
        return Err(MetadataReadError::Unavailable);
    };
    if !matches!(auth_token, Some(AuthToken::Bearer(_))) {
        return Err(MetadataReadError::Unauthorized);
    }
    let auth = metadata
        .authorize_write_peer(peer, auth_token)
        .await
        .map_err(|error| match error {
            WritePeerError::Unauthorized => MetadataReadError::Unauthorized,
            WritePeerError::Unavailable(_) => MetadataReadError::Unavailable,
        })?;
    let realm_id = *net_handle.realm_id();
    let config = drive(GetConfigOperation::new(realm_id), context)
        .await
        .ok()
        .ok_or(MetadataReadError::Unavailable)?;
    if auth.realm_id != realm_id
        || auth.path_restrictions.is_some()
        || !peer_acts_for(&config, peer, auth.user_id)
    {
        return Err(MetadataReadError::Forbidden);
    }
    Ok(auth.user_id)
}

/// Serves this holder's records of one user to a peer.
pub(crate) async fn serve_vault_fetch(
    context: &Arc<DriverContext>,
    peer: NodeId,
    user_id: UserId,
    query: VaultQuery,
) -> MetadataTransportMessage {
    MetadataTransportMessage::FetchedVaultRecords {
        result: local_records(context, peer, user_id, query).await,
    }
}

async fn local_records(
    context: &Arc<DriverContext>,
    peer: NodeId,
    user_id: UserId,
    query: VaultQuery,
) -> Result<VaultRecords, MetadataReadError> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let config = drive(GetConfigOperation::new(*net_handle.realm_id()), context)
        .await
        .ok()
        .ok_or(MetadataReadError::Unavailable)?;
    let (target, key_space, limit) = match query {
        VaultQuery::Heads { auth_token } => {
            if vault_caller(context, peer, auth_token).await? != user_id {
                return Err(MetadataReadError::Forbidden);
            }
            let target = aruna_core::document::DocumentTarget::VaultRevision {
                realm_id: *net_handle.realm_id(),
                user_id,
                revision_id: Ulid::nil(),
            };
            (target, VAULT_REVISION_KEYSPACE, MAX_VAULT_HEADS)
        }
        VaultQuery::Keys => {
            if !is_sync_eligible(&config, peer) {
                return Err(MetadataReadError::Forbidden);
            }
            let target = aruna_core::document::DocumentTarget::UserKey {
                realm_id: *net_handle.realm_id(),
                user_id,
                record_id: Ulid::nil(),
            };
            (target, USER_KEY_KEYSPACE, MAX_KEY_RECORDS)
        }
    };
    let placement = target_placement_ref(&config, &target, Default::default());
    // A node that holds no replica has nothing to say, so the requester asks the next holder.
    if !holds_placement(&config, &placement, net_handle.node_id()) {
        return Err(MetadataReadError::Unavailable);
    }
    read_records(context, user_id, key_space, limit).await
}

async fn read_records(
    context: &DriverContext,
    user_id: UserId,
    key_space: &str,
    limit: usize,
) -> Result<VaultRecords, MetadataReadError> {
    let mut start = None;
    let mut keys = Vec::new();
    loop {
        let (rows, next) = iter_prefix_page(
            &context.storage_handle,
            key_space,
            Some(user_record_prefix(user_id)),
            start,
            limit,
            None,
        )
        .await
        .map_err(|_| MetadataReadError::Unavailable)?;
        let values = rows.iter().map(|(_, value)| value.as_ref());
        if key_space == VAULT_REVISION_KEYSPACE {
            return Ok(VaultRecords::Heads(
                values
                    .map(VaultRevision::from_bytes)
                    .collect::<Result<_, _>>()
                    .map_err(|_| MetadataReadError::Unavailable)?,
            ));
        }
        keys.extend(
            values
                .map(UserKeyRecord::from_bytes)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| MetadataReadError::Unavailable)?,
        );
        retain_newest_keys(&mut keys);
        if next.is_none() {
            return Ok(VaultRecords::Keys(keys));
        }
        start = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::{HolderList, VaultFetchEffect};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use std::time::Duration;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn fetch_effect() -> VaultFetchEffect {
        VaultFetchEffect {
            holders: HolderList::new(vec![node(1), node(2)]).unwrap(),
            user_id: UserId::local(Ulid::from_bytes([7; 16]), RealmId::from_bytes([3; 32])),
            query: VaultQuery::Keys,
            deadline: Duration::from_secs(10),
        }
    }

    #[tokio::test]
    async fn serves_newest_keys() {
        use aruna_core::effects::StorageEffect;
        use aruna_core::events::{Event, StorageEvent};
        use aruna_storage::storage::FjallStorage;

        let dir = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: FjallStorage::open(dir.path().to_str().unwrap()).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let user_id = fetch_effect().user_id;
        let writes = (1..=65)
            .map(|id| {
                let record = UserKeyRecord {
                    realm_id: user_id.realm_id,
                    user_id,
                    record_id: Ulid::from_bytes([id; 16]),
                    key_id: format!("key-{id}"),
                    public_key: [1; 32],
                    fingerprint: aruna_core::vault_format::key_fingerprint(&[1; 32]),
                    has_recovery: false,
                    node_id: node(1),
                    placement: PlacementRef::NIL,
                    created_at_ms: if id == 1 || id == 65 { 2 } else { 1 },
                    signature: None,
                };
                (
                    USER_KEY_KEYSPACE.to_string(),
                    record.target().storage_key(),
                    record.to_bytes().unwrap().into(),
                )
            })
            .collect();
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
        let VaultRecords::Keys(keys) =
            read_records(&context, user_id, USER_KEY_KEYSPACE, MAX_KEY_RECORDS)
                .await
                .unwrap()
        else {
            panic!("expected keys");
        };
        assert_eq!(keys.len(), MAX_KEY_RECORDS);
        assert_eq!(keys[0].record_id, Ulid::from_bytes([65; 16]));
        assert_eq!(keys[1].record_id, Ulid::from_bytes([1; 16]));
        assert!(
            !keys
                .iter()
                .any(|key| key.record_id == Ulid::from_bytes([2; 16]))
        );
    }
}
