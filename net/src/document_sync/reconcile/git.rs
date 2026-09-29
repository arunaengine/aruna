//! Stores replicated Git records after checking their shape, realm and document.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::{DocumentEvent, DocumentTarget};
use aruna_core::git::{GitPackRecord, GitRecord};
use aruna_core::storage_entries::{shard_manifest_entry, sync_revision_entry};
use aruna_core::structs::SyncQuarantineIdentity;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use tracing::warn;

use crate::document_sync::{DocumentSyncService, SyncRejection};
use crate::error::{NetError, Result};

use super::metadata::MetadataOutcome;

/// Records are immutable: the holder that wrote one authorized its author, as for metadata
/// events. A receiver accepts it once, never replaces it, and rejects foreign or malformed ones.
pub(super) async fn apply_git_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    identity: SyncQuarantineIdentity,
    event: DocumentEvent,
) -> Result<MetadataOutcome> {
    if let DocumentEvent::Upsert {
        target: DocumentTarget::GitPack { .. },
        ..
    } = &event
    {
        return apply_git_pack(service, topic_id, identity, event).await;
    }
    let DocumentEvent::Upsert {
        target: target @ DocumentTarget::GitRecord { .. },
        bytes,
        change,
        ..
    } = &event
    else {
        return Err(NetError::Bootstrap(
            "Git record handler received another event".to_string(),
        ));
    };
    let reject = |reason: &str| {
        warn!(%topic_id, reason, "Rejecting replicated Git record");
        Ok(MetadataOutcome::Rejected(SyncRejection::new(
            identity,
            event.clone(),
            reason,
        )))
    };
    let Some(record) = GitRecord::decode(bytes) else {
        return reject("undecodable Git record");
    };
    if *target
        != (DocumentTarget::GitRecord {
            document_id: record.document_id,
            event_id: record.event_id,
        })
        || record.realm_id != service.realm_id
        || !record.validate()
    {
        return reject("Git record does not match its target, realm or bounds");
    }
    let registry = DocumentTarget::MetadataRegistry {
        group_id: record.group_id,
        document_id: record.document_id,
    };
    if let Some(value) = service
        .storage_read(
            registry.storage_keyspace().to_string(),
            registry.storage_key(),
        )
        .await?
    {
        let document: MetadataRegistryRecord =
            postcard::from_bytes(&value).map_err(|error| NetError::Bootstrap(error.to_string()))?;
        if document.realm_id != record.realm_id || document.placement != record.placement {
            return reject("Git record does not match its document");
        }
    }
    let keyspace = target.storage_keyspace().to_string();
    match service
        .storage_read(keyspace.clone(), target.storage_key())
        .await?
    {
        Some(stored) if stored.as_ref() == bytes.as_slice() => Ok(MetadataOutcome::Skipped),
        Some(_) => reject("Git record id is already used by a different record"),
        None => {
            // The revision and manifest rows let this holder prove the shard in a handover.
            let bootstrap =
                |error: aruna_core::errors::ConversionError| NetError::Bootstrap(error.to_string());
            let mut writes = vec![
                (keyspace, target.storage_key(), bytes.clone().into()),
                sync_revision_entry(target, change).map_err(bootstrap)?,
            ];
            writes.extend(shard_manifest_entry(target, change).map_err(bootstrap)?);
            service.storage_batch_write(writes).await?;
            aruna_core::git::record_written(record.document_id);
            Ok(MetadataOutcome::Applied {
                target: target.clone(),
                tombstone: None,
            })
        }
    }
}

/// Packs are immutable like records: a receiver stores bytes that hash to the target once.
async fn apply_git_pack(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    identity: SyncQuarantineIdentity,
    event: DocumentEvent,
) -> Result<MetadataOutcome> {
    let DocumentEvent::Upsert {
        target:
            target @ DocumentTarget::GitPack {
                document_id,
                sha256,
            },
        bytes,
        change,
        ..
    } = &event
    else {
        return Err(NetError::Bootstrap(
            "Git pack handler received another event".to_string(),
        ));
    };
    let valid = postcard::from_bytes::<GitPackRecord>(bytes)
        .is_ok_and(|pack| pack.document_id == *document_id && pack.valid(sha256));
    if !valid {
        warn!(%topic_id, "Rejecting replicated Git pack");
        return Ok(MetadataOutcome::Rejected(SyncRejection::new(
            identity,
            event.clone(),
            "Git pack does not match its target or size limit",
        )));
    }
    let keyspace = target.storage_keyspace().to_string();
    if service
        .storage_read(keyspace.clone(), target.storage_key())
        .await?
        .is_some()
    {
        // Equal hashes mean equal bytes; which record first named them does not matter.
        return Ok(MetadataOutcome::Skipped);
    }
    let bootstrap =
        |error: aruna_core::errors::ConversionError| NetError::Bootstrap(error.to_string());
    let mut writes = vec![
        (keyspace, target.storage_key(), bytes.clone().into()),
        sync_revision_entry(target, change).map_err(bootstrap)?,
    ];
    writes.extend(shard_manifest_entry(target, change).map_err(bootstrap)?);
    service.storage_batch_write(writes).await?;
    aruna_core::git::record_written(*document_id);
    Ok(MetadataOutcome::Applied {
        target: target.clone(),
        tombstone: None,
    })
}
