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
use aruna_core::git::{GitChange, GitRecord, MAX_RECORDS, git_record_entry, record_change};
use aruna_core::handle::Handle;
use aruna_core::storage_entries::{shard_manifest_entry, sync_revision_entry};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use std::collections::BTreeSet;
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
    let holders = resolve_shard_holders(&config, &document.placement);
    if !holders.contains(&node) {
        return Err(GitError::NotHolder);
    }
    Ok(holders)
}

/// Records not listed by the newest checkpoint, which bound what a replay applies on top.
pub fn uncovered(records: &[GitRecord]) -> Vec<&GitRecord> {
    let checkpoint = records
        .iter()
        .rev()
        .find_map(|record| match &record.change {
            GitChange::Checkpoint(checkpoint) => Some((record.event_id, checkpoint)),
            _ => None,
        });
    let covered: BTreeSet<Ulid> = checkpoint
        .map(|(id, checkpoint)| {
            checkpoint
                .covered
                .iter()
                .copied()
                .chain(std::iter::once(id))
                .collect()
        })
        .unwrap_or_default();
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
    let peers = holders(context, document).await?;
    let config = load_realm_config(context, document.realm_id)
        .await
        .ok_or(GitError::Unavailable)?;
    let node_id = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(GitError::Unavailable)?;
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
    let peers: Vec<_> = peers.into_iter().filter(|peer| *peer != node_id).collect();
    if !peers.is_empty() {
        let outbox = new_outbox_record(
            node_id,
            target,
            peers,
            DocumentOutboxEvent::Upsert { bytes, change },
            record.placement,
            false,
        )
        .fenced_at(
            crate::placement::fence::write_generation(&config, &record.placement).unwrap_or(0),
        );
        writes.push(outbox_write_entry(&outbox).map_err(|_| GitError::Invalid)?);
    }
    records::commit(context, writes).await?;
    if let Some(tasks) = context.task_handle.as_ref() {
        // The record is durable; a missed wake-up only delays replication to the next drain.
        let _ = tasks.send_effect(schedule_drain_effect()).await;
    }
    Ok(record)
}
