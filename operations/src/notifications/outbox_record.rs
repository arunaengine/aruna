//! Builds notification outbox records and the effect that schedules their drain.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::effects::Effect;
use aruna_core::structs::execution::notification::{NotificationOutboxRecord, NotificationRecord};
use aruna_core::task::{TaskEffect, TaskKey};
use ulid::Ulid;

pub fn new_outbox_record(record: NotificationRecord) -> NotificationOutboxRecord {
    NotificationOutboxRecord {
        outbox_id: Ulid::generate(),
        record,
    }
}

pub fn schedule_drain_effect() -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainNotificationOutbox,
        after: Duration::ZERO,
    })
}
