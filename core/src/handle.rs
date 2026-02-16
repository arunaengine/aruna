use async_trait::async_trait;

use crate::effects::Effect;
use crate::events::Event;

#[async_trait]
pub trait Handle: Clone + Send + Sync {
    async fn send_effect(&self, effect: Effect) -> Event;
}
