use super::super::*;
use super::*;

pub(super) fn job_id() -> JobId {
    crate::jobs::submit::mint_job_id(
        PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
        BucketId::new(0).unwrap(),
    )
    .unwrap()
}

pub(super) struct RecordingTaskHandler {
    pub(super) seen: mpsc::Sender<TaskKey>,
}

#[async_trait]
impl InboundTaskHandler for RecordingTaskHandler {
    async fn handle_timer(&self, key: TaskKey) {
        let _ = self.seen.send(key).await;
    }
}

pub(super) struct InstalledDrainHandler {
    pub(super) handler: Arc<OperationsTaskHandler>,
    pub(super) completed: mpsc::Sender<()>,
}

#[async_trait]
impl InboundTaskHandler for InstalledDrainHandler {
    async fn handle_timer(&self, key: TaskKey) {
        self.handler.handle_timer(key.clone()).await;
        if key == TaskKey::DrainDocumentSyncOutbox {
            let _ = self.completed.send(()).await;
        }
    }
}

/// Tokio inhibits paused-clock auto-advance while a blocking task is alive.
/// Storage and net answer from their own threads, so without this the clock
/// races ahead of every round trip. `tokio::time::advance` still applies.
pub(super) struct ClockGuard {
    _stop: std::sync::mpsc::Sender<()>,
}

pub(super) fn freeze_clock() -> ClockGuard {
    let (stop, wait) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || {
        let _ = wait.recv();
    });
    ClockGuard { _stop: stop }
}

// Net shutdown drains under a timeout that a still clock never expires.
pub(super) async fn shutdown_net(net: &NetHandle) {
    tokio::time::resume();
    net.shutdown().await;
}

pub(super) struct InstalledHarness {
    pub(super) _dir: tempfile::TempDir,
    pub(super) storage: aruna_storage::StorageHandle,
    pub(super) net: NetHandle,
    pub(super) task_handle: TaskHandle,
    pub(super) context: Arc<DriverContext>,
    pub(super) handler: Arc<OperationsTaskHandler>,
    pub(super) completed: mpsc::Receiver<()>,
}

// The frozen clock only moves on an explicit advance, so waiting here cannot
// outrun the drain; a poll bound would instead depend on machine speed.
pub(super) async fn recv_progress(receiver: &mut mpsc::Receiver<()>) -> bool {
    receiver.recv().await.is_some()
}

pub(super) fn node(seed: u8) -> aruna_core::NodeId {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    iroh::SecretKey::from_bytes(&bytes).public()
}

pub(super) fn outbox_handler() -> (tempfile::TempDir, OperationsTaskHandler, TaskHandle) {
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let task_handle = TaskHandle::new();
    let handler = OperationsTaskHandler::new(
        Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(task_handle.clone()),
            compute_handle: None,
        }),
        JobsRuntime::new(),
    );
    (dir, handler, task_handle)
}

pub(super) async fn scheduled_after(task_handle: &TaskHandle) -> Duration {
    let TaskEvent::TimerScheduled { after, .. } = task_handle
        .schedule_timer_if_idle(TaskKey::DrainDocumentSyncOutbox, Duration::ZERO)
        .await
    else {
        panic!("expected timer schedule event");
    };
    after
}
