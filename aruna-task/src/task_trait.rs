use crate::{error::ArunaTaskError, Task};

#[async_trait::async_trait]
pub trait TaskExecutor: Clone + Send + Sync + 'static {
    async fn execute(&self, task: Task) -> Result<(), ArunaTaskError>;
}
