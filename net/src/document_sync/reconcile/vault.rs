//! Stores replicated vault revisions and user key records after checking shape, realm and author.
//! A holder keeps only the current heads; a replaced revision never becomes a head again.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::{VAULT_RETIRED_KEYSPACE, VAULT_REVISION_KEYSPACE};
use aruna_core::structs::identity::user::vault::{
    UserKeyRecord, VaultRevision, head_rows, record_rows, user_record_key,
};

use super::metadata::MetadataOutcome;
use super::*;

/// Both records are immutable and published by the node that admitted the user's own request.
pub(super) async fn apply_vault_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    identity: SyncQuarantineIdentity,
    event: DocumentEvent,
) -> Result<MetadataOutcome> {
    let reject = |reason: &str| {
        warn!(%topic_id, reason, "Rejecting a replicated vault record");
        Ok(MetadataOutcome::Rejected(SyncRejection::new(
            identity,
            event.clone(),
            reason,
        )))
    };
    let DocumentEvent::Upsert {
        target,
        bytes,
        change,
        ..
    } = &event
    else {
        return reject("vault records are upserts");
    };
    let checked = match target {
        DocumentTarget::VaultRevision { .. } => VaultRevision::from_bytes(bytes)
            .ok()
            .filter(|record| record.validate().is_ok())
            .map(|record| (record.target(), record.sync_change(), Some(record))),
        DocumentTarget::UserKey { .. } => UserKeyRecord::from_bytes(bytes)
            .ok()
            .filter(|record| record.validate().is_ok())
            .map(|record| (record.target(), record.sync_change(), None)),
        _ => None,
    };
    let Some((expected, expected_change, revision)) = checked else {
        return reject("vault record is malformed or out of bounds");
    };
    let user_id = match target {
        DocumentTarget::VaultRevision { user_id, .. } | DocumentTarget::UserKey { user_id, .. } => {
            *user_id
        }
        _ => return reject("vault record has another target"),
    };
    if expected != *target || expected_change != *change || user_id.realm_id != service.realm_id {
        return reject("vault record does not match its target, change or realm");
    }
    if actor_id != ::irokle::actor_id_for(topic_id, node_to_peer(&change.current.actor)) {
        return reject("vault record actor is not its publisher");
    }
    let bootstrap =
        |error: aruna_core::errors::ConversionError| NetError::Bootstrap(error.to_string());
    let stored = service
        .storage_read(target.storage_keyspace().to_string(), target.storage_key())
        .await?;
    match (stored, revision) {
        (Some(stored), _) if stored.as_ref() == bytes.as_slice() => Ok(MetadataOutcome::Skipped),
        (Some(_), _) => reject("vault record id is already used by a different record"),
        (None, None) => {
            let rows = record_rows(target, bytes, change).map_err(bootstrap)?;
            service.storage_batch_write(rows).await?;
            Ok(MetadataOutcome::Applied {
                target: target.clone(),
                tombstone: None,
            })
        }
        (None, Some(revision)) => {
            let (writes, deletes) = head_rows(&revision, bytes).map_err(bootstrap)?;
            if store_head(&service.storage, &revision, deletes, writes).await? {
                Ok(MetadataOutcome::Applied {
                    target: target.clone(),
                    tombstone: None,
                })
            } else {
                Ok(MetadataOutcome::Skipped)
            }
        }
    }
}

/// Stores a revision unless a later save already retired it. The marker read and
/// the writes share one transaction, so a concurrent local save cannot revive it.
async fn store_head(
    storage: &StorageHandle,
    revision: &VaultRevision,
    deletes: Vec<(String, ByteView)>,
    writes: Vec<(String, ByteView, Value)>,
) -> Result<bool> {
    let marker = user_record_key(revision.user_id, revision.revision_id);
    let mut last = None;
    for _ in 0..APPLY_CONFLICT_ATTEMPTS {
        let txn_id = start_storage_transaction(storage).await?;
        let retired = transaction_read(
            storage,
            VAULT_RETIRED_KEYSPACE.to_string(),
            marker.clone(),
            Some(txn_id),
        )
        .await;
        let result = match retired {
            Ok(retired) => {
                let mut writes = writes.clone();
                if retired.is_some() {
                    writes.retain(|(keyspace, key, _)| {
                        keyspace != VAULT_REVISION_KEYSPACE || key != &marker
                    });
                }
                replace_batch_in(storage, txn_id, deletes.clone(), writes)
                    .await
                    .map(|()| retired.is_none())
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(stored) => return Ok(stored),
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                if !matches!(error, NetError::Storage(StorageError::TransactionConflict)) {
                    return Err(error);
                }
                last = Some(error);
            }
        }
    }
    Err(last.unwrap_or_else(|| NetError::Dht("vault apply retries exhausted".to_string())))
}
