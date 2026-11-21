//! Writes, reads and deletes the durable rows of task timers, so they survive a node restart.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_clock::unix_timestamp_millis;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::LazyLock;
use std::time::Duration;

use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::TASK_TIMER_KEYSPACE;
use aruna_core::task::{PersistedTaskTimer, TaskEffect, TaskKey};
use byteview::ByteView;
use tracing::warn;

use crate::storage::handle::StorageHandle;

/// How many locks the timer keys share, picked by key hash.
const TIMER_LOCK_COUNT: usize = 64;

/// A durable timer change holds its key's lock until the scheduler took it, so the durable and
/// the live changes of one key happen in the same order.
static TIMER_LOCKS: LazyLock<Vec<tokio::sync::Mutex<()>>> = LazyLock::new(|| {
    (0..TIMER_LOCK_COUNT)
        .map(|_| tokio::sync::Mutex::new(()))
        .collect()
});

/// Holds the key of a timer change that has a durable row, until its caller handed the change to
/// the scheduler too; a change without a row holds nothing.
pub async fn hold_change(effect: &TaskEffect) -> Option<tokio::sync::MutexGuard<'static, ()>> {
    let key = match effect {
        TaskEffect::ResetTimer { key, .. }
        | TaskEffect::ShortenTimer { key, .. }
        | TaskEffect::CancelTimer { key } => key,
        _ => return None,
    };
    Some(hold_timer(key).await)
}

/// Holds the change lock of `key` as a timer change does, so a caller can reread its row and hand
/// it to the scheduler before any later change of the key.
pub async fn hold_timer(key: &TaskKey) -> tokio::sync::MutexGuard<'static, ()> {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let index = (hasher.finish() % TIMER_LOCK_COUNT as u64) as usize;
    TIMER_LOCKS[index].lock().await
}

/// The admission of a row no scheduler admission wrote, such as a fixture's.
pub const NO_ADMISSION: u64 = 0;

/// Writes or deletes the durable row a timer effect implies, outside the scheduler: a written row
/// belongs to [`NO_ADMISSION`]. A timer that re-arms from other state, or lives in memory only,
/// has none.
pub async fn persist_task_effect(
    storage: &StorageHandle,
    effect: &TaskEffect,
) -> Result<(), String> {
    if !timer_has_row(effect) {
        return Ok(());
    }
    match effect {
        TaskEffect::ResetTimer { key, after } => {
            let written = write_owned(storage, key, *after, NO_ADMISSION).await;
            written.map_err(|error| error.to_string())
        }
        TaskEffect::ShortenTimer { key, after } => shorten_timer(storage, key, *after).await,
        TaskEffect::CancelTimer { key } => delete_timer(storage, key).await,
        _ => Ok(()),
    }
}

/// Whether a timer change has a durable row to write or delete.
pub fn timer_has_row(effect: &TaskEffect) -> bool {
    let changes_row = matches!(
        effect,
        TaskEffect::ResetTimer { .. }
            | TaskEffect::ShortenTimer { .. }
            | TaskEffect::CancelTimer { .. }
    );
    changes_row && !timer_is_restored(effect) && !timer_is_ephemeral(effect)
}

/// Writes the row of the wake scheduler admission `admission` armed for `key`, due after
/// `after`; only the firing of that admission deletes it.
pub async fn write_owned(
    storage: &StorageHandle,
    key: &TaskKey,
    after: Duration,
    admission: u64,
) -> Result<(), StorageError> {
    let due_unix_millis = due_at_millis(after).map_err(StorageError::WriteError)?;
    let record = PersistedTaskTimer {
        key: key.clone(),
        due_unix_millis,
        admission,
    };
    write_record(storage, &record).await
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
            | TaskKey::PruneFamilies
            | TaskKey::DrainWatchEvents
            | TaskKey::DrainAppliedSync
            | TaskKey::DrainCaptures
    )
}

/// Timed locks live in memory only; a restart locks their buckets without them.
fn timer_is_ephemeral(effect: &TaskEffect) -> bool {
    let (TaskEffect::ResetTimer { key, .. }
    | TaskEffect::ShortenTimer { key, .. }
    | TaskEffect::ScheduleIdleTimer { key, .. }
    | TaskEffect::CancelTimer { key }
    | TaskEffect::AbortRunningHandlers { key }) = effect
    else {
        // A pull claim lives in memory only.
        return true;
    };
    matches!(key, TaskKey::LockBucket { .. })
}

/// Deletes the row of a timer that fired, only if the firing's admission `stamp` owns it: a row a
/// later admission wrote for the key stays, and a firing without a stamp owns no row. Best
/// effort: a row that survives only fires the work once more after a restart.
pub async fn delete_persisted_timer(storage: &StorageHandle, key: &TaskKey, stamp: Option<u64>) {
    let Some(stamp) = stamp else {
        return;
    };
    let _held = hold_timer(key).await;
    let due = match read_timer(storage, key).await {
        Ok(row) => row.is_some_and(|row| row.admission == stamp),
        Err(error) => {
            warn!(error = %error, key = ?key, "Failed to read a fired task timer");
            return;
        }
    };
    if due && let Err(error) = delete_timer(storage, key).await {
        warn!(error = %error, key = ?key, "Failed to delete persisted task timer");
    }
}

async fn shorten_timer(
    storage: &StorageHandle,
    key: &TaskKey,
    after: Duration,
) -> Result<(), String> {
    let requested_due_at = due_at_millis(after)?;
    match read_timer(storage, key)
        .await
        .map_err(|error| error.to_string())?
    {
        Some(existing) if existing.due_unix_millis <= requested_due_at => Ok(()),
        _ => write_owned(storage, key, after, NO_ADMISSION)
            .await
            .map_err(|error| error.to_string()),
    }
}

pub async fn read_timer(
    storage: &StorageHandle,
    key: &TaskKey,
) -> Result<Option<PersistedTaskTimer>, StorageError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            key: task_storage_key(key).map_err(StorageError::ReadError)?,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| postcard::from_bytes(&bytes))
            .transpose()
            .map_err(|error| StorageError::ReadError(error.to_string())),
        Event::Storage(StorageEvent::Error { error }) => Err(error),
        other => Err(StorageError::ReadError(format!(
            "unexpected storage event: {other:?}"
        ))),
    }
}

async fn write_record(
    storage: &StorageHandle,
    record: &PersistedTaskTimer,
) -> Result<(), StorageError> {
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            key: task_storage_key(&record.key).map_err(StorageError::WriteError)?,
            value: ByteView::from(
                postcard::to_allocvec(record)
                    .map_err(|error| StorageError::WriteError(error.to_string()))?,
            ),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error),
        other => Err(StorageError::WriteError(format!(
            "unexpected storage event: {other:?}"
        ))),
    }
}

async fn delete_timer(storage: &StorageHandle, key: &TaskKey) -> Result<(), String> {
    delete_timer_key(storage, task_storage_key(key)?).await
}

/// Deletes one timer row by its row key, such as a row that no longer decodes; a failed delete is
/// the caller's to report, as a surviving row re-arms its timer at the next restart.
pub async fn delete_timer_key(storage: &StorageHandle, key: ByteView) -> Result<(), String> {
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            key,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

/// Deletes a row whose value no longer decodes, under its key's change lock and only while it still
/// does not decode, so a timer a later change wrote for the key stays.
pub async fn delete_malformed(storage: &StorageHandle, key: ByteView) -> Result<(), String> {
    let task_key = postcard::from_bytes::<TaskKey>(&key).ok();
    let _held = match &task_key {
        Some(task_key) => Some(hold_timer(task_key).await),
        None => None,
    };
    let read = StorageEffect::Read {
        key_space: TASK_TIMER_KEYSPACE.to_string(),
        key: key.clone(),
        txn_id: None,
    };
    let value = match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
        other => return Err(format!("unexpected storage event: {other:?}")),
    };
    match value {
        Some(bytes) if postcard::from_bytes::<PersistedTaskTimer>(&bytes).is_err() => {
            delete_timer_key(storage, key).await
        }
        _ => Ok(()),
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
