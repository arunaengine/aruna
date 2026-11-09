//! Writes the notification settings of a user as last-writer-wins registers, decides
//! deliveries from this node's view, and asks a management node to open the sealed secret.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::compute::SecretBytes;
use aruna_core::document::{
    DocumentChange, DocumentChangeKind, DocumentOutboxEvent, DocumentSyncRevision, DocumentTarget,
};
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::register::{RegisterError, RegisterStamp};
use aruna_core::storage_entries::sync_revision_entry;
use aruna_core::structs::execution::job::JobError;
use aruna_core::structs::execution::notification::NotificationRecord;
use aruna_core::structs::execution::setting::{SettingChannel, SettingHead, current_setting};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::TxnId;
use aruna_storage::StorageHandle;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::metadata::protocol::MetadataReadError;
use crate::sync::document_outbox::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};

/// Rows read per page from a settings keyspace.
const SETTING_PAGE: usize = 256;
/// The first delay after no management node opened a secret; it doubles up to `BACKOFF_CAP_MS`.
pub(crate) const BACKOFF_START_MS: u64 = 30_000;
const BACKOFF_CAP_MS: u64 = 30 * 60_000;

/// The current setting with its secret opened when it is on.
#[derive(Clone, PartialEq, Eq)]
pub struct SettingWinner {
    pub bytes: Vec<u8>,
    pub secret: Option<SecretBytes>,
}

impl std::fmt::Debug for SettingWinner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SettingWinner")
            .field("bytes_len", &self.bytes.len())
            .field("has_secret", &self.secret.is_some())
            .finish()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SettingError {
    #[error("the realm configuration cannot be read now: {0}")]
    Config(String),
    #[error("the setting could not be built: {0}")]
    Build(String),
    #[error("the setting could not be stored: {0}")]
    Storage(String),
    #[error(transparent)]
    Register(#[from] RegisterError),
}

/// Opens the sealed secret of a setting on a management node, locally or over the metadata
/// transport.
#[async_trait::async_trait]
pub trait SettingPeers: Send + Sync {
    async fn open(
        &self,
        node: NodeId,
        target: DocumentTarget,
        bytes: Vec<u8>,
    ) -> Result<SecretBytes, MetadataReadError>;
}

/// Opens on this node when it is the asked one, else over the metadata transport.
pub struct MetadataPeers<'a> {
    pub ctx: &'a DriverContext,
    pub local: NodeId,
}

#[async_trait::async_trait]
impl SettingPeers for MetadataPeers<'_> {
    async fn open(
        &self,
        node: NodeId,
        target: DocumentTarget,
        bytes: Vec<u8>,
    ) -> Result<SecretBytes, MetadataReadError> {
        let metadata = self.ctx.metadata_handle.as_ref();
        if node == self.local {
            let key = metadata.and_then(|metadata| metadata.realm_seal_key());
            let served =
                crate::realm_vault::serve_open(self.ctx, key.as_deref(), self.local, target, bytes);
            return Box::pin(served).await;
        }
        let metadata = metadata.ok_or(MetadataReadError::Unavailable)?;
        metadata.request_open(node, target, bytes).await
    }
}

/// The management nodes of `config`, in config order.
pub fn managers(config: &RealmConfigDocument) -> Vec<NodeId> {
    (config.nodes.iter())
        .filter(|node| node.kind == RealmNodeKind::Management)
        .filter_map(|node| node.node_id.parse::<NodeId>().ok())
        .collect()
}

/// Writes a new register write built by `build(stamp)` on this node and returns its bytes.
/// The counter is one above the highest this node holds for the register; document sync
/// replicates the write after the local commit.
pub async fn write_setting(
    ctx: &DriverContext,
    local: NodeId,
    channel: SettingChannel,
    owner: UserId,
    build: &(dyn Fn(RegisterStamp) -> Result<Vec<u8>, SettingError> + Sync),
) -> Result<Vec<u8>, SettingError> {
    let held = stored_settings(&ctx.storage_handle, channel, &owner, None).await;
    let held = held.map_err(SettingError::Storage)?;
    let counter = held.iter().map(|(head, _)| head.stamp.counter).max();
    let stamp = RegisterStamp::next(counter, local, Ulid::generate())?;
    let bytes = build(stamp)?;
    let target = channel.target(owner, stamp.write_id);
    let change = DocumentChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: Ulid::generate(),
            actor: local,
            updated_at_ms: unix_timestamp_millis(),
        },
        kind: DocumentChangeKind::Upsert,
        placement: PlacementRef::NIL,
    };
    publish_setting(ctx, local, &target, &bytes, change).await?;
    Ok(bytes)
}

/// Writes the document row, its revision and the sync outbox upsert in one batch.
async fn publish_setting(
    ctx: &DriverContext,
    local: NodeId,
    target: &DocumentTarget,
    bytes: &[u8],
    change: DocumentChange,
) -> Result<(), SettingError> {
    let storage_error = |error: String| SettingError::Storage(error);
    let outbox = new_identified_record(
        change.current.event_id,
        local,
        target.clone(),
        Vec::new(),
        DocumentOutboxEvent::Upsert {
            bytes: bytes.to_vec(),
            change,
        },
        PlacementRef::NIL,
        false,
    );
    let revision =
        sync_revision_entry(target, &change).map_err(|e| storage_error(e.to_string()))?;
    let outbox = outbox_write_entry(&outbox).map_err(|e| storage_error(e.to_string()))?;
    let row = (
        target.storage_keyspace().to_string(),
        target.storage_key(),
        bytes.to_vec().into(),
    );
    let write = StorageEffect::BatchWrite {
        writes: vec![row, revision, outbox],
        txn_id: None,
    };
    match ctx.storage_handle.send_storage_effect(write).await {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        other => return Err(storage_error(format!("{other:?}"))),
    }
    if let Some(tasks) = ctx.task_handle.as_ref() {
        let _ = aruna_core::handle::Handle::send_effect(tasks, schedule_drain_effect()).await;
    }
    Ok(())
}

/// The register writes of one user and channel on this node, with their heads.
pub(crate) async fn stored_settings(
    storage: &StorageHandle,
    channel: SettingChannel,
    owner: &UserId,
    txn_id: Option<TxnId>,
) -> Result<Vec<(SettingHead, Vec<u8>)>, String> {
    let rows = prefix_rows(storage, channel.keyspace(), owner.to_bytes(), txn_id).await?;
    Ok((rows.into_iter())
        .filter_map(|bytes| match channel.head(&bytes) {
            Ok(head) => Some((head, bytes)),
            Err(error) => {
                warn!(%error, "Skipping an undecodable notification setting");
                None
            }
        })
        .collect())
}

async fn prefix_rows(
    storage: &StorageHandle,
    key_space: &str,
    prefix: Vec<u8>,
    txn_id: Option<TxnId>,
) -> Result<Vec<Vec<u8>>, String> {
    let mut rows = Vec::new();
    let mut start = None;
    loop {
        let page = StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: Some(prefix.clone().into()),
            start: start.take(),
            limit: SETTING_PAGE,
            txn_id,
        };
        match storage.send_storage_effect(page).await {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                rows.extend(values.into_iter().map(|(_, value)| value.to_vec()));
                match next_start_after {
                    Some(after) => start = Some(IterStart::After(after)),
                    None => return Ok(rows),
                }
            }
            other => return Err(format!("{key_space} page: {other:?}")),
        }
    }
}

/// The current setting on this node when it is on. A node that has not received a recent
/// write yet shows the previous one.
pub async fn local_setting(
    ctx: &DriverContext,
    channel: SettingChannel,
    owner: UserId,
) -> Result<Option<Vec<u8>>, SettingError> {
    let settings = stored_settings(&ctx.storage_handle, channel, &owner, None).await;
    let settings = settings.map_err(SettingError::Storage)?;
    let winner = current_setting(settings, |(head, _)| *head);
    Ok(winner.filter(|(head, _)| head.on).map(|(_, bytes)| bytes))
}

/// Shared delays of deliveries while no management node opens a secret, per owner and channel.
#[derive(Debug, Default)]
pub struct Backoffs(Mutex<HashMap<(UserId, SettingChannel), Backoff>>);

#[derive(Clone, Copy, Debug)]
struct Backoff {
    until_ms: u64,
    delay_ms: u64,
}

impl Backoffs {
    /// The end of a running backoff.
    pub fn until(&self, owner: UserId, channel: SettingChannel, now_ms: u64) -> Option<u64> {
        let map = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let until = map.get(&(owner, channel)).map(|backoff| backoff.until_ms);
        until.filter(|until| *until > now_ms)
    }

    /// Records a failed attempt and returns the end of the next delay.
    pub fn fail(&self, owner: UserId, channel: SettingChannel, now_ms: u64) -> u64 {
        let mut map = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let delay_ms = match map.get(&(owner, channel)) {
            Some(backoff) => backoff.delay_ms.saturating_mul(2).min(BACKOFF_CAP_MS),
            None => BACKOFF_START_MS,
        };
        let until_ms = now_ms.saturating_add(delay_ms);
        map.insert((owner, channel), Backoff { until_ms, delay_ms });
        until_ms
    }

    pub fn clear(&self, owner: UserId, channel: SettingChannel) {
        let mut map = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        map.remove(&(owner, channel));
    }
}

/// What a delivery attempt does.
#[derive(Debug)]
pub enum Delivery {
    /// The notice expired, or the current setting on this node is off or missing.
    Skip,
    /// Try again after the delay without spending an attempt.
    Wait(JobError, Duration),
    /// Send with the current setting and its opened secret.
    Send(SettingWinner),
}

/// Decides one delivery from this node's current setting. When it is on, the management nodes
/// are asked in config order to open its secret and the first answer is used. `managers` is
/// `None` when the realm config could not be read, which counts as no answer.
pub async fn delivery_setting(
    ctx: &DriverContext,
    peers: &dyn SettingPeers,
    backoffs: &Backoffs,
    record: &NotificationRecord,
    channel: SettingChannel,
    managers: Option<&[NodeId]>,
    now_ms: u64,
) -> Delivery {
    let expires_ms = record.expires_at_ms();
    if now_ms >= expires_ms {
        return Delivery::Skip;
    }
    let owner = record.recipient;
    let wait = |until_ms: u64, message: &str| {
        let delay = until_ms.min(expires_ms).saturating_sub(now_ms);
        Delivery::Wait(JobError::retryable(message), Duration::from_millis(delay))
    };
    let current = match local_setting(ctx, channel, owner).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return Delivery::Skip,
        Err(error) => {
            warn!(%error, "Reading the notification setting failed");
            return wait(
                backoffs.fail(owner, channel, now_ms),
                "the setting read failed",
            );
        }
    };
    if let Some(until_ms) = backoffs.until(owner, channel, now_ms) {
        return wait(until_ms, "waiting for a management node");
    }
    let (Ok(head), Some(managers)) = (channel.head(&current), managers) else {
        let until_ms = backoffs.fail(owner, channel, now_ms);
        return wait(until_ms, "the realm configuration cannot be read now");
    };
    let target = channel.target(owner, head.id);
    for node in managers {
        match peers.open(*node, target.clone(), current.clone()).await {
            Ok(secret) => {
                backoffs.clear(owner, channel);
                return Delivery::Send(SettingWinner {
                    bytes: current,
                    secret: Some(secret),
                });
            }
            Err(error) => warn!(%node, ?error, "A management node did not open a setting"),
        }
    }
    let until_ms = backoffs.fail(owner, channel, now_ms);
    wait(until_ms, "no management node opened the setting secret")
}

#[cfg(test)]
#[path = "settings_tests.rs"]
mod tests;
