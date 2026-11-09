//! Reads and writes the replicated notification settings of a user through a quorum of the
//! realm's management nodes, and spaces out deliveries while no quorum answers.
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
use aruna_core::keyspaces::TENTATIVE_SETTINGS_KEYSPACE;
use aruna_core::storage_entries::sync_revision_entry;
use aruna_core::structs::execution::job::JobError;
use aruna_core::structs::execution::notification::NotificationRecord;
use aruna_core::structs::execution::setting::{
    SettingChannel, SettingHead, TentativeSetting, current_setting, quorum_size, tentative_prefix,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::TxnId;
use aruna_storage::StorageHandle;
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
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
/// Conflict rounds a write takes before it gives up with an unknown outcome.
const WRITE_ROUNDS: usize = 3;
/// The first delay after a short quorum read; it doubles up to `BACKOFF_CAP_MS`.
pub(crate) const BACKOFF_START_MS: u64 = 30_000;
const BACKOFF_CAP_MS: u64 = 30 * 60_000;

/// One management node's answer to a setting read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettingAnswer {
    /// The highest version the node holds, tentative or confirmed. It only allocates versions.
    pub max_version: u64,
    /// The node's confirmed winner; only confirmed documents decide.
    pub winner: Option<SettingWinner>,
}

/// A confirmed setting with its secret opened when it is on.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// How a management node answered phase one of a write.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoreAnswer {
    Stored,
    /// Another document holds the id or a version at least as high; retry above `max_version`.
    Conflict {
        max_version: u64,
    },
}

/// The combined answers of a quorum of management nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuorumRead {
    pub max_version: u64,
    pub winner: Option<SettingWinner>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SettingError {
    #[error("the realm configuration cannot be read now: {0}")]
    Config(String),
    #[error("too few management nodes answered; the change may still take effect")]
    NoQuorum,
    #[error("the setting could not be built: {0}")]
    Build(String),
    #[error("the setting could not be stored: {0}")]
    Storage(String),
}

/// The management side of the three setting requests, local or over the metadata transport.
#[async_trait::async_trait]
pub trait SettingPeers: Send + Sync {
    async fn read(
        &self,
        node: NodeId,
        owner: UserId,
        channel: SettingChannel,
    ) -> Result<SettingAnswer, MetadataReadError>;

    async fn store(
        &self,
        node: NodeId,
        target: DocumentTarget,
        bytes: Vec<u8>,
        change: DocumentChange,
    ) -> Result<StoreAnswer, MetadataReadError>;

    async fn confirm(&self, node: NodeId, target: DocumentTarget) -> Result<(), MetadataReadError>;
}

/// Serves requests to this node locally and sends the others through the metadata transport.
pub struct MetadataPeers<'a> {
    pub ctx: &'a DriverContext,
    pub local: NodeId,
}

#[async_trait::async_trait]
impl SettingPeers for MetadataPeers<'_> {
    async fn read(
        &self,
        node: NodeId,
        owner: UserId,
        channel: SettingChannel,
    ) -> Result<SettingAnswer, MetadataReadError> {
        let metadata = self.ctx.metadata_handle.as_ref();
        if node == self.local {
            let key = metadata.and_then(|metadata| metadata.realm_seal_key());
            let served = crate::realm_vault::serve_setting(
                self.ctx,
                key.as_deref(),
                self.local,
                owner,
                channel,
            );
            return Box::pin(served).await;
        }
        let metadata = metadata.ok_or(MetadataReadError::Unavailable)?;
        metadata.request_setting(node, owner, channel).await
    }

    async fn store(
        &self,
        node: NodeId,
        target: DocumentTarget,
        bytes: Vec<u8>,
        change: DocumentChange,
    ) -> Result<StoreAnswer, MetadataReadError> {
        if node == self.local {
            let served =
                crate::realm_vault::serve_store(self.ctx, self.local, target, bytes, change);
            return Box::pin(served).await;
        }
        let metadata = (self.ctx.metadata_handle.as_ref()).ok_or(MetadataReadError::Unavailable)?;
        metadata.request_store(node, target, bytes, change).await
    }

    async fn confirm(&self, node: NodeId, target: DocumentTarget) -> Result<(), MetadataReadError> {
        if node == self.local {
            let served = crate::realm_vault::serve_confirm(self.ctx, self.local, target);
            return Box::pin(served).await;
        }
        let metadata = (self.ctx.metadata_handle.as_ref()).ok_or(MetadataReadError::Unavailable)?;
        metadata.request_confirm(node, target).await
    }
}

/// The management nodes of `config`, in config order.
pub fn managers(config: &RealmConfigDocument) -> Vec<NodeId> {
    (config.nodes.iter())
        .filter(|node| node.kind == RealmNodeKind::Management)
        .filter_map(|node| node.node_id.parse::<NodeId>().ok())
        .collect()
}

/// Asks the managers in order until a quorum answered. The largest confirmed winner decides and
/// the largest `max_version` allocates; `None` when fewer than a quorum answered.
pub async fn read_quorum(
    peers: &dyn SettingPeers,
    managers: &[NodeId],
    owner: UserId,
    channel: SettingChannel,
) -> Option<QuorumRead> {
    let needed = quorum_size(managers.len());
    let mut answers = Vec::with_capacity(needed);
    for node in managers {
        match peers.read(*node, owner, channel).await {
            Ok(answer) => answers.push(answer),
            Err(error) => warn!(%node, ?error, "A management node did not answer a setting read"),
        }
        if answers.len() >= needed {
            break;
        }
    }
    if answers.len() < needed {
        return None;
    }
    let max_version = answers.iter().map(|answer| answer.max_version).max();
    let winners = (answers.into_iter())
        .filter_map(|answer| answer.winner)
        .filter_map(|winner| Some((channel.head(&winner.bytes).ok()?, winner)));
    Some(QuorumRead {
        max_version: max_version.unwrap_or(0),
        winner: current_setting(winners, |(head, _)| *head).map(|(_, winner)| winner),
    })
}

/// Writes a new setting document built by `build(version, id)` and returns its bytes. Phase one
/// stores it tentative on the managers; after a store quorum it is published, and the answer
/// waits for a confirm quorum. Every failure after the store quorum is an unknown outcome.
pub async fn write_setting(
    ctx: &DriverContext,
    peers: &dyn SettingPeers,
    local: NodeId,
    managers: &[NodeId],
    channel: SettingChannel,
    owner: UserId,
    build: &(dyn Fn(u64, Ulid) -> Result<Vec<u8>, SettingError> + Sync),
) -> Result<Vec<u8>, SettingError> {
    let needed = quorum_size(managers.len());
    let mut floor = 0;
    for _ in 0..WRITE_ROUNDS {
        let read = read_quorum(peers, managers, owner, channel).await;
        let read = read.ok_or(SettingError::NoQuorum)?;
        let id = Ulid::generate();
        let bytes = build(read.max_version.max(floor) + 1, id)?;
        let target = channel.target(owner, id);
        let change = new_change(local);
        let stores =
            (managers.iter()).map(|node| peers.store(*node, target.clone(), bytes.clone(), change));
        let answers = join_all(stores).await;
        let stored = (answers.iter())
            .filter(|answer| matches!(answer, Ok(StoreAnswer::Stored)))
            .count();
        let conflict = (answers.iter())
            .filter_map(|answer| match answer {
                Ok(StoreAnswer::Conflict { max_version }) => Some(*max_version),
                _ => None,
            })
            .max();
        if stored < needed {
            match conflict {
                Some(max_version) => {
                    floor = floor.max(max_version);
                    continue;
                }
                None => return Err(SettingError::NoQuorum),
            }
        }
        // Publishing after the store quorum is safe: every later quorum read sees this version.
        publish_setting(ctx, local, &target, &bytes, change).await?;
        let confirms = managers
            .iter()
            .map(|node| peers.confirm(*node, target.clone()));
        let confirmed = (join_all(confirms).await.iter())
            .filter(|answer| answer.is_ok())
            .count();
        return match confirmed >= needed {
            true => Ok(bytes),
            false => Err(SettingError::NoQuorum),
        };
    }
    Err(SettingError::NoQuorum)
}

fn new_change(local: NodeId) -> DocumentChange {
    DocumentChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: Ulid::generate(),
            actor: local,
            updated_at_ms: unix_timestamp_millis(),
        },
        kind: DocumentChangeKind::Upsert,
        placement: PlacementRef::NIL,
    }
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

/// The confirmed documents of one user and channel on this node, with their heads.
pub(crate) async fn confirmed_settings(
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

/// The tentative documents of one user and channel on this management node.
pub(crate) async fn tentative_settings(
    storage: &StorageHandle,
    channel: SettingChannel,
    owner: &UserId,
    txn_id: Option<TxnId>,
) -> Result<Vec<(SettingHead, TentativeSetting)>, String> {
    let prefix = tentative_prefix(channel, owner);
    let rows = prefix_rows(storage, TENTATIVE_SETTINGS_KEYSPACE, prefix, txn_id).await?;
    Ok((rows.into_iter())
        .filter_map(|row| {
            let tentative = TentativeSetting::from_bytes(&row).ok()?;
            Some((channel.head(&tentative.bytes).ok()?, tentative))
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

/// The confirmed winner on this node when it is on. Every node holds confirmed documents only,
/// so this shows what this node received so far.
pub async fn local_setting(
    ctx: &DriverContext,
    channel: SettingChannel,
    owner: UserId,
) -> Result<Option<Vec<u8>>, SettingError> {
    let settings = confirmed_settings(&ctx.storage_handle, channel, &owner, None).await;
    let settings = settings.map_err(SettingError::Storage)?;
    let winner = current_setting(settings, |(head, _)| *head);
    Ok(winner.filter(|(head, _)| head.on).map(|(_, bytes)| bytes))
}

/// Shared delays of deliveries after a short quorum read, per owner and channel.
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

    /// Records a short read and returns the end of the next delay.
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

/// What a delivery attempt does after its setting read.
#[derive(Debug)]
pub enum Delivery {
    /// The notice expired, or the confirmed setting is off or missing.
    Skip,
    /// Try again after the delay without spending an attempt.
    Wait(JobError, Duration),
    /// Send with the confirmed winner and its opened secret.
    Send(SettingWinner),
}

/// Decides one delivery from a quorum read. `managers` is `None` when the realm config could
/// not be read, which counts as a short read.
pub async fn delivery_setting(
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
    if let Some(until_ms) = backoffs.until(owner, channel, now_ms) {
        return wait(until_ms, "waiting for a quorum of management nodes");
    }
    let read = match managers {
        Some(managers) => read_quorum(peers, managers, owner, channel).await,
        None => None,
    };
    let Some(read) = read else {
        let until_ms = backoffs.fail(owner, channel, now_ms);
        return wait(
            until_ms,
            "too few management nodes answered the setting read",
        );
    };
    backoffs.clear(owner, channel);
    match read.winner {
        Some(winner) if winner.secret.is_some() => Delivery::Send(winner),
        _ => Delivery::Skip,
    }
}

#[cfg(test)]
#[path = "settings_tests.rs"]
mod tests;
