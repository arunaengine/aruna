//! Writes, reads and deletes the durable rows of task timers, so they survive a node restart.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::time::unix_timestamp_millis;
use std::time::Duration;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::TASK_TIMER_KEYSPACE;
use aruna_core::task::{PersistedTaskTimer, TaskEffect, TaskKey};
use byteview::ByteView;
use tracing::warn;

use crate::StorageHandle;

/// Writes or deletes the durable row a timer effect implies; a timer that re-arms from other
/// state, or lives in memory only, has none.
pub async fn persist_task_effect(
    storage: &StorageHandle,
    effect: &TaskEffect,
) -> Result<(), String> {
    if timer_is_restored(effect) || timer_is_ephemeral(effect) {
        return Ok(());
    }

    match effect {
        TaskEffect::ResetTimer { key, after } => write_timer(storage, key, *after).await,
        TaskEffect::ShortenTimer { key, after } => shorten_timer(storage, key, *after).await,
        TaskEffect::CancelTimer { key } => delete_timer(storage, key).await,
        TaskEffect::ScheduleIdleTimer { .. } | TaskEffect::AbortRunningHandlers { .. } => Ok(()),
    }
}

// Queue-backed tasks below re-arm from durable state, so their timer writes remain
// redundant. The document-sync outbox keeps its retry deadline across restarts.
fn timer_is_restored(effect: &TaskEffect) -> bool {
    let key = match effect {
        TaskEffect::ResetTimer { key, .. } | TaskEffect::ShortenTimer { key, .. } => key,
        _ => return false,
    };
    matches!(
        key,
        TaskKey::DrainMaterializationQueue
            | TaskKey::DrainPruneQueue
            | TaskKey::DrainReplicationQueue
            | TaskKey::DrainRefreshQueue
            | TaskKey::DrainNotificationOutbox
            | TaskKey::PruneNotifications
            | TaskKey::PublishNodeInfo
            | TaskKey::DrainJobQueue
            | TaskKey::PruneJobs
            | TaskKey::DrainMirrorRepair
            | TaskKey::DrainDeviceIntake
            | TaskKey::DrainLinkQueue
            | TaskKey::PruneConnectorReferences
            | TaskKey::MaintainRanges
    )
}

/// Timed locks live in memory only; a restart locks their buckets without them.
fn timer_is_ephemeral(effect: &TaskEffect) -> bool {
    let (TaskEffect::ResetTimer { key, .. }
    | TaskEffect::ShortenTimer { key, .. }
    | TaskEffect::ScheduleIdleTimer { key, .. }
    | TaskEffect::CancelTimer { key }
    | TaskEffect::AbortRunningHandlers { key }) = effect;
    matches!(key, TaskKey::LockBucket { .. })
}

pub async fn delete_persisted_timer(storage: &StorageHandle, key: &TaskKey) {
    if let Err(error) = delete_timer(storage, key).await {
        warn!(error = %error, key = ?key, "Failed to delete persisted task timer");
    }
}

async fn write_timer(
    storage: &StorageHandle,
    key: &TaskKey,
    after: Duration,
) -> Result<(), String> {
    let due_unix_millis = due_at_millis(after)?;
    write_record(
        storage,
        &PersistedTaskTimer {
            key: key.clone(),
            due_unix_millis,
        },
    )
    .await
}

async fn shorten_timer(
    storage: &StorageHandle,
    key: &TaskKey,
    after: Duration,
) -> Result<(), String> {
    let requested_due_at = due_at_millis(after)?;
    match read_timer(storage, key).await? {
        Some(existing) if existing.due_unix_millis <= requested_due_at => Ok(()),
        _ => {
            write_record(
                storage,
                &PersistedTaskTimer {
                    key: key.clone(),
                    due_unix_millis: requested_due_at,
                },
            )
            .await
        }
    }
}

pub async fn read_timer(
    storage: &StorageHandle,
    key: &TaskKey,
) -> Result<Option<PersistedTaskTimer>, String> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            key: task_storage_key(key)?,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| postcard::from_bytes(&bytes).map_err(|error| error.to_string()))
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

async fn write_record(storage: &StorageHandle, record: &PersistedTaskTimer) -> Result<(), String> {
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            key: task_storage_key(&record.key)?,
            value: ByteView::from(
                postcard::to_allocvec(record).map_err(|error| error.to_string())?,
            ),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

async fn delete_timer(storage: &StorageHandle, key: &TaskKey) -> Result<(), String> {
    delete_timer_key(storage, task_storage_key(key)?).await;
    Ok(())
}

/// Deletes one timer row by its row key, such as a row that no longer decodes.
pub async fn delete_timer_key(storage: &StorageHandle, key: ByteView) {
    if let Event::Storage(StorageEvent::Error { error }) = storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            key,
            txn_id: None,
        })
        .await
    {
        warn!(error = %error, "Failed to delete persisted task timer");
    }
}

fn task_storage_key(key: &TaskKey) -> Result<ByteView, String> {
    key.storage_key().map_err(|error| error.to_string())
}

fn due_at_millis(after: Duration) -> Result<u64, String> {
    let after_millis =
        u64::try_from(after.as_millis()).map_err(|_| "timer duration is too large".to_string())?;
    unix_timestamp_millis()
        .checked_add(after_millis)
        .ok_or_else(|| "timer deadline overflow".to_string())
}
