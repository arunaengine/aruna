//! Task adapter: persist the task effect first, then hand it to the handle.
//! Persistence happens even without a task handle, so a restart still sees the
//! requested control; the missing handle is reported as an explicit task error.

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};

use crate::driver::DriverContext;
use crate::tasks::task_persistence::persist_task_effect;

pub(super) async fn dispatch_task(effect: TaskEffect, context: &DriverContext) -> Event {
    if let Err(message) = persist_task_effect(&context.storage_handle, &effect).await {
        return Event::Task(TaskEvent::Error {
            key: task_effect_key(&effect),
            message,
        });
    }
    if let Some(task_handle) = &context.task_handle {
        Box::pin(task_handle.send_effect(Effect::Task(effect))).await
    } else {
        Event::Task(TaskEvent::Error {
            key: None,
            message: "task handle unavailable".to_string(),
        })
    }
}

/// The timer key a control effect names, so a persistence failure still points
/// at the affected timer.
fn task_effect_key(effect: &TaskEffect) -> Option<TaskKey> {
    match effect {
        TaskEffect::ResetTimer { key, .. }
        | TaskEffect::ShortenTimer { key, .. }
        | TaskEffect::CancelTimer { key }
        | TaskEffect::AbortRunningHandlers { key } => Some(key.clone()),
    }
}

#[cfg(test)]
mod pure_tests {
    use super::task_effect_key;
    use aruna_core::structs::RealmId;
    use aruna_core::task::TaskEffect;

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
