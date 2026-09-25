//! Applies replicated Invenio links: only the link's owner publishes, deletes leave tombstones.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::repository::RepositoryLink;

use super::metadata::MetadataOutcome;
use super::*;

pub(super) async fn apply_link_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    identity: SyncQuarantineIdentity,
    event: DocumentEvent,
) -> Result<MetadataOutcome> {
    let (target, bytes, change) = match &event {
        DocumentEvent::Upsert {
            target,
            bytes,
            change,
            ..
        } => (target.clone(), Some(bytes.clone()), *change),
        DocumentEvent::Delete { target, change, .. } => (target.clone(), None, *change),
        DocumentEvent::AdminOperation { .. } => {
            let reason = "invenio link events are upserts or deletes";
            return Ok(MetadataOutcome::Rejected(SyncRejection::new(
                identity, event, reason,
            )));
        }
    };
    let reject = |reason: String| {
        warn!(%topic_id, ?target, %reason, "Rejecting a replicated Invenio link");
        Ok(MetadataOutcome::Rejected(SyncRejection::new(
            identity,
            event.clone(),
            reason,
        )))
    };
    if actor_id != ::irokle::actor_id_for(topic_id, node_to_peer(&change.current.actor)) {
        return reject("invenio link revision actor is not its publisher".to_string());
    }
    if let Err(reason) = validate_link(&target, bytes.as_deref(), &change) {
        return reject(format!("invalid invenio link: {reason}"));
    }
    match store_link(&service.storage, service.realm_id, &target, bytes, change).await? {
        MetadataPlacementOutcome::Accepted(true) => Ok(MetadataOutcome::Applied {
            target: target.clone(),
            tombstone: None,
        }),
        MetadataPlacementOutcome::Accepted(false) => Ok(MetadataOutcome::Skipped),
        MetadataPlacementOutcome::Deferred(dependency) => Ok(MetadataOutcome::Deferred(dependency)),
        MetadataPlacementOutcome::Rejected => {
            reject("invenio link has a mismatched placement".to_string())
        }
    }
}

/// An upsert must carry the change its row derives; a delete must carry its link's event id.
pub(in crate::document_sync) fn validate_link(
    target: &DocumentTarget,
    bytes: Option<&[u8]>,
    change: &DocumentChange,
) -> std::result::Result<(), String> {
    let DocumentTarget::RepositoryLink {
        document_id,
        link_id,
    } = target
    else {
        return Err("target is not an invenio link".to_string());
    };
    match bytes {
        Some(bytes) => {
            let link = RepositoryLink::from_bytes(bytes).map_err(|error| error.to_string())?;
            if link.document_id != *document_id || link.link_id != *link_id {
                return Err("payload does not match its target".to_string());
            }
            if *change != link.sync_change(change.placement) {
                return Err("revision does not match its sync change".to_string());
            }
        }
        None => {
            let event_id = Ulid::from_parts(change.current.generation, link_id.random());
            if change.kind != DocumentChangeKind::Delete
                || change.base.is_some()
                || change.current.event_id != event_id
            {
                return Err("delete does not match its sync change".to_string());
            }
        }
    }
    Ok(())
}

/// Folds one link change, its sidecar and manifest entry into a transaction. A newer revision
/// of the same owner wins; a tombstone keeps every later replay of the link out.
pub(in crate::document_sync) async fn store_link(
    storage: &StorageHandle,
    realm_id: RealmId,
    target: &DocumentTarget,
    bytes: Option<Vec<u8>>,
    change: DocumentChange,
) -> Result<MetadataPlacementOutcome<bool>> {
    let DocumentTarget::RepositoryLink { document_id, .. } = target else {
        return Ok(MetadataPlacementOutcome::Rejected);
    };
    for _ in 0..2 {
        let txn_id = start_storage_transaction(storage).await?;
        let result = link_txn(
            storage,
            realm_id,
            *document_id,
            target,
            &bytes,
            change,
            txn_id,
        )
        .await;
        match result {
            Ok(Some(outcome)) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Ok(outcome);
            }
            Ok(None) => return Ok(MetadataPlacementOutcome::Accepted(true)),
            Err(NetError::Storage(StorageError::TransactionConflict)) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
            }
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        }
    }
    Err(NetError::Dht("invenio link conflicted twice".to_string()))
}

/// Commits the change, or returns the outcome that leaves the transaction to be aborted.
async fn link_txn(
    storage: &StorageHandle,
    realm_id: RealmId,
    document_id: Ulid,
    target: &DocumentTarget,
    bytes: &Option<Vec<u8>>,
    change: DocumentChange,
    txn_id: TxnId,
) -> Result<Option<MetadataPlacementOutcome<bool>>> {
    let placement = change.placement;
    match derive_placement_txn(storage, realm_id, None, document_id, placement, txn_id).await? {
        MetadataPlacementOutcome::Accepted(_) => {}
        MetadataPlacementOutcome::Deferred(dependency) => {
            return Ok(Some(MetadataPlacementOutcome::Deferred(dependency)));
        }
        MetadataPlacementOutcome::Rejected => return Ok(Some(MetadataPlacementOutcome::Rejected)),
    }
    let key = sync_revision_key(target);
    let local = transaction_read(
        storage,
        SYNC_REVISION_KEYSPACE.to_string(),
        key,
        Some(txn_id),
    )
    .await?
    .map(|value| postcard::from_bytes::<DocumentChange>(&value))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let newer = match local {
        None => true,
        Some(local) if local.kind == DocumentChangeKind::Delete => false,
        Some(local) => {
            change.current > local.current && change.current.actor == local.current.actor
        }
    };
    if !newer {
        return Ok(Some(MetadataPlacementOutcome::Accepted(false)));
    }
    let row = (target.storage_keyspace().to_string(), target.storage_key());
    let mut writes = vec![
        sync_revision_entry(target, &change)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    ];
    writes.extend(
        shard_manifest_entry(target, &change)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    );
    let deletes = match bytes {
        Some(bytes) => {
            writes.push((row.0, row.1, Value::from(bytes.clone())));
            Vec::new()
        }
        None => vec![row],
    };
    replace_batch_in(storage, txn_id, deletes, writes).await?;
    Ok(None)
}

impl DocumentSyncService {
    /// Direct apply path, for events that do not arrive through a reconcile batch.
    pub(in crate::document_sync) async fn apply_link(
        &self,
        target: DocumentTarget,
        bytes: Option<Vec<u8>>,
        change: DocumentChange,
    ) -> Result<()> {
        validate_link(&target, bytes.as_deref(), &change).map_err(NetError::Bootstrap)?;
        match store_link(&self.storage, self.realm_id, &target, bytes, change).await? {
            MetadataPlacementOutcome::Accepted(_) => Ok(()),
            MetadataPlacementOutcome::Deferred(_) => Err(NetError::Dht(
                "invenio link placement configuration is unavailable".to_string(),
            )),
            MetadataPlacementOutcome::Rejected => Err(NetError::Bootstrap(
                "invenio link has a mismatched placement".to_string(),
            )),
        }
    }
}
