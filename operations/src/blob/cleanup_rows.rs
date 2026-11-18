//! Schedules the drain of the blob cleanup queue.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::effects::Effect;
use aruna_core::task::{TaskEffect, TaskKey};

pub fn schedule_cleanup_effect() -> Effect {
    Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::DrainCleanupQueue,
        after: Duration::ZERO,
    })
}
