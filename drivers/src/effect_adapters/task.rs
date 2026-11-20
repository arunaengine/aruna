//! Applies each timer change to its durable row and the task handle, so a restart still sees it
//! and only the firing of the admission that owns a row deletes it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::events::Event;
use aruna_core::task::{NO_TASK_HANDLE, TaskEffect, TaskEvent, TaskKey};
use aruna_storage::storage::handle::StorageHandle;
use aruna_storage::task_timers::{
    NO_ADMISSION, hold_change, persist_task_effect, timer_has_row, write_owned,
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
    let _held = hold_change(&effect).await;
    let admission = next_admission();
    let key = task_effect_key(&effect);
    let refused = |message| Event::Task(TaskEvent::Error { key, message });
    if let TaskEffect::ShortenTimer { key, after } = &effect
        && timer_has_row(&effect)
    {
        let Some(task_handle) = task_handle else {
            return refused_handle();
        };
        let (event, armed) = task_handle.shorten_owned(key.clone(), *after, admission).await;
        if armed && let Err(message) = write_owned(storage, key, *after, admission).await {
            warn!(key = ?key, %message, "Timer kept in memory only");
            if !keep_live {
                return refused(message);
            }
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
