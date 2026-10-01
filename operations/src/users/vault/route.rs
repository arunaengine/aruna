//! Routes vault changes to a holder and serves vault fetches between realm nodes.
//! A holder checks the forwarded bearer token itself and serves a vault only to its own user.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::effects::{VaultFetchEffect, VaultQuery};
use aruna_core::events::VaultFetchEvent;
use aruna_core::keyspaces::{USER_KEY_KEYSPACE, VAULT_REVISION_KEYSPACE};
use aruna_core::metadata::AuthToken;
use aruna_core::structs::identity::user::vault::{
    MAX_KEY_RECORDS, MAX_VAULT_HEADS, UserKeyRecord, VaultRecords, VaultRevision,
    retain_newest_keys, user_record_prefix,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::time::timeout_at;
use tracing::warn;
use ulid::Ulid;

use super::write::{AppendVaultConfig, AppendVaultError, AppendVaultOperation, VaultAppended};
use crate::driver::{DriverContext, drive};
use crate::forward::authorize::{is_sync_eligible, peer_acts_for};
use crate::forward::transport::{MetadataWriteError, forward_to_holders};
use crate::jobs::store::iter_prefix_page;
use crate::metadata::handle::WritePeerError;
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage, WriteAuthError};
use crate::metadata::transport_message_kind;
use crate::placement::process_placements::load_realm_config;
use crate::placement::{holds_placement, target_placement_ref};

/// Saves one holder attempt makes when its transaction loses to a concurrent save.
const CONFLICT_ATTEMPTS: usize = 3;

/// Why a holder refused a forwarded vault change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VaultRefusal {
    Invalid,
    TooManyKeys,
}

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
    let mut attempt = 1;
    loop {
        match drive(AppendVaultOperation::new(config.clone()), context.as_ref()).await {
            Err(AppendVaultError::Storage(
                aruna_core::errors::StorageError::TransactionConflict,
            )) if attempt < CONFLICT_ATTEMPTS => {
                attempt += 1;
            }
            result => return result,
        }
    }
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
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(MetadataReadError::Unavailable)?;
    if auth.realm_id != realm_id
        || auth.path_restrictions.is_some()
        || !peer_acts_for(&config, peer, auth.user_id)
    {
        return Err(MetadataReadError::Forbidden);
    }
    Ok(auth.user_id)
}

/// Asks each holder in turn. A holder answering without records is a miss;
/// only a complete absence of answers is unavailable.
pub(crate) async fn fetch_vault(
    context: &DriverContext,
    effect: VaultFetchEffect,
) -> VaultFetchEvent {
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return VaultFetchEvent::Unavailable("metadata transport unavailable".to_string());
    };
    fetch_records(effect, |holder, request| {
        metadata.request_forwarded_write(holder, request)
    })
    .await
}

async fn fetch_records<F, R, E>(effect: VaultFetchEffect, mut request: F) -> VaultFetchEvent
where
    F: FnMut(NodeId, MetadataTransportMessage) -> R,
    R: std::future::Future<Output = Result<MetadataTransportMessage, E>>,
    E: std::fmt::Display,
{
    let deadline = tokio::time::Instant::now() + effect.deadline;
    let mut answered = false;
    for (index, holder) in effect.holders.as_slice().iter().enumerate() {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = (effect.holders.as_slice().len() - index) as u32;
        let attempt_deadline = now + deadline.duration_since(now) / remaining;
        let message = MetadataTransportMessage::FetchVaultRecords {
            user_id: effect.user_id,
            query: effect.query.clone(),
        };
        let reply = match timeout_at(attempt_deadline, request(*holder, message)).await {
            Ok(Ok(reply)) => reply,
            Ok(Err(error)) => {
                warn!(peer = %holder, error = %error, "Vault fetch failed");
                continue;
            }
            Err(_) => continue,
        };
        match reply {
            MetadataTransportMessage::FetchedVaultRecords {
                result: Ok(records),
            } if records.is_empty() => answered = true,
            MetadataTransportMessage::FetchedVaultRecords {
                result: Ok(records),
            } => {
                return VaultFetchEvent::Fetched {
                    holder: *holder,
                    records,
                };
            }
            MetadataTransportMessage::FetchedVaultRecords {
                result: Err(MetadataReadError::Unauthorized | MetadataReadError::Forbidden),
            } => return VaultFetchEvent::Denied,
            MetadataTransportMessage::FetchedVaultRecords { result: Err(_) } => {}
            other => warn!(
                peer = %holder,
                reply = transport_message_kind(&other),
                "Unexpected vault fetch reply"
            ),
        }
    }
    if answered {
        return VaultFetchEvent::NotFound;
    }
    VaultFetchEvent::Unavailable("no vault holder answered".to_string())
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
    let config = load_realm_config(context, *net_handle.realm_id())
        .await
        .ok_or(MetadataReadError::Unavailable)?;
    let (target, key_space, limit) = match query {
        VaultQuery::Heads { auth_token } => {
            if vault_caller(context, peer, auth_token).await? != user_id {
                return Err(MetadataReadError::Forbidden);
            }
            let target = aruna_core::document::DocumentTarget::VaultRevision {
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
    use aruna_core::effects::HolderList;
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
                    user_id,
                    record_id: Ulid::from_bytes([id; 16]),
                    key_id: format!("key-{id}"),
                    public_key: [1; 32],
                    fingerprint: aruna_core::vault_format::key_fingerprint(&[1; 32]),
                    has_recovery: false,
                    node_id: node(1),
                    placement: PlacementRef::NIL,
                    created_at_ms: if id == 1 || id == 65 { 2 } else { 1 },
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

    #[tokio::test(start_paused = true)]
    async fn tries_later_holder() {
        let effect = fetch_effect();
        let record = UserKeyRecord {
            user_id: effect.user_id,
            record_id: Ulid::from_bytes([1; 16]),
            key_id: "key-1".to_string(),
            public_key: [1; 32],
            fingerprint: aruna_core::vault_format::key_fingerprint(&[1; 32]),
            has_recovery: false,
            node_id: node(2),
            placement: PlacementRef::NIL,
            created_at_ms: 1,
        };
        let expected = VaultRecords::Keys(vec![record]);
        let mut attempts = Vec::new();
        let started = tokio::time::Instant::now();
        let event = fetch_records(effect, |holder, _| {
            attempts.push(holder);
            let records = expected.clone();
            async move {
                if holder == node(1) {
                    std::future::pending::<()>().await;
                }
                Ok::<_, &'static str>(MetadataTransportMessage::FetchedVaultRecords {
                    result: Ok(records),
                })
            }
        })
        .await;
        assert_eq!(attempts, [node(1), node(2)]);
        assert_eq!(
            tokio::time::Instant::now() - started,
            Duration::from_secs(5)
        );
        assert!(
            matches!(event, VaultFetchEvent::Fetched { holder, records } if holder == node(2) && records == expected)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn preserves_fetch_results() {
        for case in 0..4 {
            let mut attempts = Vec::new();
            let started = tokio::time::Instant::now();
            let event = fetch_records(fetch_effect(), |holder, _| {
                attempts.push(holder);
                async move {
                    if case == 1 && holder == node(1) {
                        return Ok::<_, &'static str>(
                            MetadataTransportMessage::FetchedVaultRecords {
                                result: Ok(VaultRecords::Keys(Vec::new())),
                            },
                        );
                    }
                    if (case == 2 && holder == node(2)) || case == 3 {
                        return Ok(MetadataTransportMessage::FetchedVaultRecords {
                            result: Err(MetadataReadError::Forbidden),
                        });
                    }
                    std::future::pending::<Result<MetadataTransportMessage, &'static str>>().await
                }
            })
            .await;
            match case {
                0 => {
                    assert!(matches!(event, VaultFetchEvent::Unavailable(_)));
                    assert_eq!(
                        tokio::time::Instant::now() - started,
                        Duration::from_secs(10)
                    );
                }
                1 => assert!(matches!(event, VaultFetchEvent::NotFound)),
                _ => assert!(matches!(event, VaultFetchEvent::Denied)),
            }
            assert_eq!(
                attempts,
                if case == 3 {
                    vec![node(1)]
                } else {
                    vec![node(1), node(2)]
                }
            );
        }
    }
}
