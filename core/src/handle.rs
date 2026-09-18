//! Declares the handle trait that sends one effect to an adapter and awaits its event.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use async_trait::async_trait;

use crate::effects::Effect;
use crate::events::Event;

/// Effect executors return immediate `Event` result values to the operation
/// driver. Durable domain event records are owned by operations/outbox flows;
/// handles should not be treated as the origin of those records.
#[async_trait]
pub trait Handle: Clone + Send + Sync {
    async fn send_effect(&self, effect: Effect) -> Event;
}
