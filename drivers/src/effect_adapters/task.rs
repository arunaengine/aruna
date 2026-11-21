//! Applies each timer change to its durable row and the task handle, so a restart still sees it
//! and only the firing of the admission that owns a row deletes it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_clock::unix_timestamp_millis;
use aruna_core::events::Event;
use aruna_core::task::{NO_TASK_HANDLE, TaskEffect, TaskEvent, TaskKey};
use aruna_storage::storage::handle::StorageHandle;
use aruna_storage::task_timers::{
    NO_ADMISSION, hold_change, persist_task_effect, read_timer, timer_has_row, write_owned,
};
use aruna_tasks::TaskHandle;
use tracing::warn;

use crate::clock::next_entropy;
use crate::driver::DriverContext;

pub(super) async fn dispatch_task(effect: TaskEffect, context: &DriverContext) -> Event {
    let (storage, task_handle) = (&context.storage_handle, context.task_handle.as_ref());
    let keep_live = kept_in_memory(&effect);
    Box::pin(change_timer(storage, task_handle, effect, keep_live)).await
}

/// Applies one timer change to its row and the scheduler under the key's change lock. Each arming
/// is a new admission owning the row it writes; a shorten that keeps an earlier wake leaves its
/// row. With `keep_live` the scheduler is armed even when the row cannot be written.
pub async fn change_timer(
    storage: &StorageHandle,
    task_handle: Option<&TaskHandle>,
    effect: TaskEffect,
    keep_live: bool,
) -> Event {
    // The row and the scheduler see changes of one key in the same order.
    let held = hold_change(&effect).await;
    if !timer_has_row(&effect) {
        return apply_change(storage, task_handle, effect, keep_live).await;
    }
    let (storage, task_handle) = (storage.clone(), task_handle.cloned());
    let key = task_effect_key(&effect);
    finish_owned(key, async move {
        let _held = held;
        apply_change(&storage, task_handle.as_ref(), effect, keep_live).await
    })
    .await
}

/// Arms a timer's stored due time, which may be earlier, with the admission that owns its row,
/// and the default `effect` only without a row. An unreadable row stays for restoration while
/// the default runs from memory.
pub async fn arm_unless_stored(
    storage: &StorageHandle,
    task_handle: &TaskHandle,
    effect: TaskEffect,
) -> Event {
    let held = hold_change(&effect).await;
    let (storage, task_handle) = (storage.clone(), task_handle.clone());
    let key = task_effect_key(&effect);
    finish_owned(key, async move {
        let _held = held;
        arm_stored(&storage, &task_handle, effect).await
    })
    .await
}

/// [`arm_unless_stored`] once its caller holds the key's change lock.
async fn arm_stored(
    storage: &StorageHandle,
    task_handle: &TaskHandle,
    effect: TaskEffect,
) -> Event {
    if let TaskEffect::ShortenTimer { key, after } = &effect {
        let admission = match read_timer(storage, key).await {
            Ok(Some(row)) => {
                let left = row.due_unix_millis.saturating_sub(unix_timestamp_millis());
                let (after, admission) = (Duration::from_millis(left), row.admission);
                let (event, _) = task_handle.shorten_owned(row.key, after, admission).await;
                return event;
            }
            Ok(None) => None,
            Err(message) => {
                warn!(key = ?key, %message, "Timer kept in memory only");
                Some(next_admission())
            }
        };
        if let Some(admission) = admission {
            let (event, _) = task_handle
                .shorten_owned(key.clone(), *after, admission)
                .await;
            return event;
        }
    }
    apply_change(storage, Some(task_handle), effect, true).await
}

/// [`change_timer`] once its caller holds the key's change lock.
async fn apply_change(
    storage: &StorageHandle,
    task_handle: Option<&TaskHandle>,
    effect: TaskEffect,
    keep_live: bool,
) -> Event {
    let admission = next_admission();
    let key = task_effect_key(&effect);
    let refused = |message| Event::Task(TaskEvent::Error { key, message });
    if let TaskEffect::ShortenTimer { key, after } = &effect
        && timer_has_row(&effect)
    {
        let Some(task_handle) = task_handle else {
            return refused_handle();
        };
        // The row is written before a new wake is armed, so a refused row arms nothing.
        let (event, plan) = task_handle
            .plan_shorten(key.clone(), *after, admission)
            .await;
        let (Event::Task(TaskEvent::TimerScheduled { after, .. }), Some(plan)) = (&event, plan)
        else {
            return event;
        };
        let written = if plan.holder == Some(admission) {
            write_owned(storage, key, *after, admission).await
        } else {
            own_row(storage, key, *after, plan.holder).await
        };
        if let Err(message) = &written {
            warn!(key = ?key, %message, "Timer kept in memory only");
            if !keep_live {
                return refused(message.clone());
            }
        }
        if plan.holder != Some(admission) {
            return event;
        }
        if written.is_err() {
            return task_handle
                .arm_planned(key.clone(), plan, admission)
                .await
                .0;
        }
        let (event, holder) = task_handle
            .claim_shorten(key.clone(), plan, admission)
            .await;
        // The wake that held the key fired since the plan and serves this change, so its run
        // owns the row and deletes it.
        if let Some(holder) = holder.filter(|holder| *holder != admission)
            && let Err(message) = write_owned(storage, key, Duration::ZERO, holder).await
        {
            warn!(key = ?key, %message, "Timer row kept for the next restart");
        }
        return event;
    }
    if timer_has_row(&effect) {
        let written = match &effect {
            TaskEffect::ResetTimer { key, after } => {
                write_owned(storage, key, *after, admission).await
            }
            _ => persist_task_effect(storage, &effect).await,
        };
        if let Err(message) = written {
            if !keep_live {
                return refused(message);
            }
            warn!(key = ?task_effect_key(&effect), %message, "Timer kept in memory only");
        }
    }
    match task_handle {
        Some(task_handle) => task_handle.send_stamped(effect, Some(admission)).await,
        None => refused_handle(),
    }
}

/// Makes sure the wake of admission `holder`, due after `after`, is stored for `key`: a row an
/// earlier refused write left missing or later is written now. An earlier row stays.
async fn own_row(
    storage: &StorageHandle,
    key: &TaskKey,
    after: Duration,
    holder: Option<u64>,
) -> Result<(), String> {
    let holder = holder.unwrap_or(NO_ADMISSION);
    let due = unix_timestamp_millis().saturating_add(after.as_millis() as u64);
    let row = read_timer(storage, key).await?;
    if row.is_some_and(|row| row.admission == holder || row.due_unix_millis < due) {
        return Ok(());
    }
    write_owned(storage, key, after, holder).await
}

/// Runs a timer handoff on its own task, so a caller that stops waiting cannot leave a stored row
/// without its live wake; the caller's own timeout only ends its wait.
async fn finish_owned(
    key: Option<TaskKey>,
    handoff: impl Future<Output = Event> + Send + 'static,
) -> Event {
    tokio::spawn(handoff).await.unwrap_or_else(|error| {
        let message = error.to_string();
        Event::Task(TaskEvent::Error { key, message })
    })
}

/// The error of a timer change on a node without a task handle.
fn refused_handle() -> Event {
    Event::Task(TaskEvent::Error {
        key: None,
        message: NO_TASK_HANDLE.to_string(),
    })
}

/// A new scheduler admission, distinct from every other one and from [`NO_ADMISSION`].
fn next_admission() -> u64 {
    let entropy = next_entropy();
    ((entropy ^ (entropy >> 64)) as u64).max(NO_ADMISSION + 1)
}

/// Whether a change arms an audit record retry, which runs from memory when storage cannot
/// persist it; a failed cancellation of that retry is reported like any other.
fn kept_in_memory(effect: &TaskEffect) -> bool {
    match effect {
        TaskEffect::ResetTimer { key, .. } | TaskEffect::ShortenTimer { key, .. } => {
            matches!(key, TaskKey::RecordAudit { .. })
        }
        _ => false,
    }
}

/// The timer key a control effect names, so a persistence failure still points
/// at the affected timer.
fn task_effect_key(effect: &TaskEffect) -> Option<TaskKey> {
    match effect {
        TaskEffect::ResetTimer { key, .. }
        | TaskEffect::ShortenTimer { key, .. }
        | TaskEffect::ScheduleIdleTimer { key, .. }
        | TaskEffect::CancelTimer { key }
        | TaskEffect::AbortRunningHandlers { key } => Some(key.clone()),
        TaskEffect::Pull(_) => None,
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{kept_in_memory, task_effect_key};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::task::TaskEffect;

    // Only arming an audit retry runs from memory; its failed cancellation is reported.
    #[test]
    fn audit_arming_kept() {
        use aruna_core::structs::storage::key_audit::{
            AuditAction, AuditOutcome, BucketAuditRecord,
        };
        let record = BucketAuditRecord {
            event_id: ulid::Ulid::from_parts(5, 1),
            bucket_id: ulid::Ulid::from_bytes([2; 16]),
            at_ms: 5,
            action: AuditAction::Lock,
            actor: None,
            node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
            generation: Some(1),
            session_id: None,
            intent_id: None,
            sequence: None,
            deadline_ms: None,
            reason: None,
            outcome: AuditOutcome::Intent,
        };
        let key = aruna_core::task::TaskKey::RecordAudit {
            record: Box::new(record),
        };
        let reset = TaskEffect::ResetTimer {
            key: key.clone(),
            after: std::time::Duration::ZERO,
        };
        assert!(kept_in_memory(&reset));
        assert!(!kept_in_memory(&TaskEffect::CancelTimer { key }));
    }

    #[test]
    fn control_effects_key() {
        let key = aruna_core::task::TaskKey::RealmPresence {
            realm_id: RealmId::from_bytes([4u8; 32]),
            node_id: iroh::SecretKey::from_bytes(&[6u8; 32]).public(),
        };
        assert_eq!(
            task_effect_key(&TaskEffect::CancelTimer { key: key.clone() }),
            Some(key.clone())
        );
        assert_eq!(
            task_effect_key(&TaskEffect::AbortRunningHandlers { key: key.clone() }),
            Some(key)
        );
    }
}
