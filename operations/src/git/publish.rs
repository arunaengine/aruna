//! Durably writes one Git record on a document holder and queues it for its co-holders.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{GitError, records};
use crate::driver::DriverContext;
use crate::metadata::api::load_realm_config;
use crate::placement::resolve_shard_holders;
use crate::sync::document_outbox::{new_outbox_record, outbox_write_entry, schedule_drain_effect};
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::git::{
    GitChange, GitPackRecord, GitRecord, MAX_RECORDS, git_record_entry, record_change,
};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::GIT_PACK_KEYSPACE;
use aruna_core::storage_entries::{shard_manifest_entry, sync_revision_entry};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

/// The document's current holders when this node is one of them.
pub async fn holders(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
) -> Result<Vec<NodeId>, GitError> {
    let node = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(GitError::Unavailable)?;
    let config = load_realm_config(context, document.realm_id)
        .await
        .ok_or(GitError::Unavailable)?;
    holders_in(&config, node, document)
}

/// The holders in `config`; a write takes its fence from the same config, so a cutover
/// between two reads cannot pair an old holder check with the new generation.
fn holders_in(
    config: &RealmConfigDocument,
    node: NodeId,
    document: &MetadataRegistryRecord,
) -> Result<Vec<NodeId>, GitError> {
    let holders = resolve_shard_holders(config, &document.placement);
    if !holders.contains(&node) {
        return Err(GitError::NotHolder);
    }
    Ok(holders)
}

/// Stores and replicates the bytes of a pack whose record is already stored.
pub async fn publish_pack(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    pack: GitPackRecord,
) -> Result<(), GitError> {
    if stored(context, &pack).await? {
        return Ok(());
    }
    let config = load_realm_config(context, document.realm_id)
        .await
        .ok_or(GitError::Unavailable)?;
    let node_id = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(GitError::Unavailable)?;
    let peers = holders_in(&config, node_id, document)?;
    let mut fence = crate::placement::fence::WriteFence::default();
    fence.add(document.realm_id, &config, [pack.placement]);
    let writes = pack_writes(&pack, document.realm_id, peers, &fence)?;
    records::commit(context, writes, &fence).await?;
    aruna_core::git::record_written(document.document_id);
    Ok(())
}

/// The pack's bytes with the fields of `record`, which names it first.
pub fn pack_record(record: &GitRecord, bytes: bytes::Bytes) -> GitPackRecord {
    GitPackRecord {
        document_id: record.document_id,
        event_id: record.event_id,
        node_id: record.node_id,
        occurred_at_ms: record.occurred_at_ms,
        placement: record.placement,
        bytes,
    }
}

async fn stored(context: &DriverContext, pack: &GitPackRecord) -> Result<bool, GitError> {
    let key = pack.target().storage_key().to_vec();
    Ok(
        records::load::<GitPackRecord>(context, GIT_PACK_KEYSPACE, key)
            .await?
            .is_some(),
    )
}

/// The pack row, its sync revision, shard manifest row and outbox publish.
pub fn pack_writes(
    pack: &GitPackRecord,
    realm_id: RealmId,
    peers: Vec<NodeId>,
    fence: &crate::placement::fence::WriteFence,
) -> Result<Vec<Row>, GitError> {
    let target = pack.target();
    let change = pack.change();
    let bytes = postcard::to_allocvec(pack).map_err(|_| GitError::Invalid)?;
    let mut writes = vec![
        (
            GIT_PACK_KEYSPACE.to_string(),
            target.storage_key(),
            bytes.clone().into(),
        ),
        sync_revision_entry(&target, &change).map_err(|_| GitError::Invalid)?,
    ];
    writes.extend(shard_manifest_entry(&target, &change).map_err(|_| GitError::Invalid)?);
    let outbox = new_outbox_record(
        pack.node_id,
        target,
        peers,
        DocumentOutboxEvent::Upsert { bytes, change },
        pack.placement,
        false,
    )
    .fenced_at(fence.generation(&realm_id, &pack.placement));
    writes.push(outbox_write_entry(&outbox).map_err(|_| GitError::Invalid)?);
    Ok(writes)
}

/// Records the newest checkpoint chain does not cover, which a replay applies on top.
pub fn uncovered(records: &[GitRecord]) -> Vec<&GitRecord> {
    let (_, covered) = super::state::chain(records);
    records
        .iter()
        .filter(|record| !covered.contains(&record.event_id))
        .collect()
}

/// Writes the record, its sync revision, shard manifest row and outbox publish atomically.
pub async fn publish(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    user_id: UserId,
    change: GitChange,
) -> Result<GitRecord, GitError> {
    publish_with(context, document, user_id, change, (None, Vec::new())).await
}

type Row = (String, byteview::ByteView, byteview::ByteView);

/// Like [`publish`], also storing the bytes of the pack the change names and writing `extra`
/// local rows, all in the same transaction.
pub async fn publish_with(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    user_id: UserId,
    change: GitChange,
    (pack, extra): (Option<bytes::Bytes>, Vec<Row>),
) -> Result<GitRecord, GitError> {
    let config = load_realm_config(context, document.realm_id)
        .await
        .ok_or(GitError::Unavailable)?;
    let node_id = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(GitError::Unavailable)?;
    let peers = holders_in(&config, node_id, document)?;
    let existing = records::scan(context, document.document_id).await?;
    if !matches!(change, GitChange::Checkpoint(_)) && uncovered(&existing).len() >= MAX_RECORDS {
        return Err(GitError::Full);
    }
    let record = GitRecord {
        event_id: Ulid::generate(),
        realm_id: document.realm_id,
        group_id: document.group_id,
        document_id: document.document_id,
        placement: document.placement,
        user_id,
        node_id,
        occurred_at_ms: u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| GitError::Unavailable)?
                .as_millis(),
        )
        .map_err(|_| GitError::Unavailable)?,
        change,
    };
    if !record.validate() {
        return Err(GitError::Invalid);
    }
    let mut fence = crate::placement::fence::WriteFence::default();
    fence.add(record.realm_id, &config, [record.placement]);
    let target = DocumentTarget::GitRecord {
        document_id: record.document_id,
        event_id: record.event_id,
    };
    let change = record_change(&record);
    let bytes = postcard::to_allocvec(&record).map_err(|_| GitError::Invalid)?;
    let mut writes = vec![git_record_entry(&record).map_err(|_| GitError::Invalid)?];
    writes.push(sync_revision_entry(&target, &change).map_err(|_| GitError::Invalid)?);
    if let Some(entry) = shard_manifest_entry(&target, &change).map_err(|_| GitError::Invalid)? {
        writes.push(entry);
    }
    // Every record goes onto the document topic, even with no co-holder yet, so holders
    // that join later receive it through topic sync.
    let outbox = new_outbox_record(
        node_id,
        target,
        peers.clone(),
        DocumentOutboxEvent::Upsert { bytes, change },
        record.placement,
        false,
    )
    .fenced_at(fence.generation(&record.realm_id, &record.placement));
    writes.push(outbox_write_entry(&outbox).map_err(|_| GitError::Invalid)?);
    if let Some(bytes) = pack {
        let pack = pack_record(&record, bytes);
        if !stored(context, &pack).await? {
            writes.extend(pack_writes(&pack, record.realm_id, peers, &fence)?);
        }
    }
    writes.extend(extra);
    records::commit(context, writes, &fence).await?;
    aruna_core::git::record_written(record.document_id);
    super::project::forget(record.document_id);
    if let Some(tasks) = context.task_handle.as_ref() {
        // The record is durable; a missed wake-up only delays replication to the next drain.
        let _ = tasks.send_effect(schedule_drain_effect()).await;
    }
    Ok(record)
}
