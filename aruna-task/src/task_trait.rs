use crate::{Task, error::ArunaTaskError};

#[async_trait::async_trait]
pub trait TaskExecutor: Send + Sync {
    async fn execute(&self, task: Task) -> Result<(), ArunaTaskError>;
    fn clone_box(&self) -> Box<dyn TaskExecutor>;
}
// We can now implement Clone manually by forwarding to clone_box.
impl Clone for Box<dyn TaskExecutor> {
    fn clone(&self) -> Box<dyn TaskExecutor + 'static> {
        self.clone_box()
    }
}
