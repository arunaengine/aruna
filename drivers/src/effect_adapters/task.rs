//! Persists each timer effect before handing it to the task handle, so a restart still sees it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::task::{NO_TASK_HANDLE, TaskEffect, TaskEvent, TaskKey};
use aruna_storage::task_timers::{hold_change, persist_stamped};

use crate::driver::DriverContext;

pub(super) async fn dispatch_task(effect: TaskEffect, context: &DriverContext) -> Event {
    // The row and the scheduler see changes of one key in the same order.
    let _held = hold_change(&effect).await;
    let kept = kept_in_memory(&effect);
    let stamp = match persist_stamped(&context.storage_handle, &effect).await {
        Ok(stamp) => stamp,
        Err(_) if kept => None,
        Err(message) => {
            return Event::Task(TaskEvent::Error {
                key: task_effect_key(&effect),
                message,
            });
        }
    };
    if let Some(task_handle) = &context.task_handle {
        Box::pin(task_handle.send_stamped(effect, stamp)).await
    } else {
        Event::Task(TaskEvent::Error {
            key: None,
            message: NO_TASK_HANDLE.to_string(),
        })
    }
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
