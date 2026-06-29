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
