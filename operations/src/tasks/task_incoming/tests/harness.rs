use super::super::*;
use super::*;

pub(super) fn job_id() -> JobId {
    crate::jobs::submit::mint_job_id(
        PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
        BucketId::new(0).unwrap(),
    )
    .unwrap()
}

#[test]
fn blocked_batch_waits() {
    // Nothing processed means the due head is blocked, so the next scan
    // waits instead of respinning at the 25ms batch pace.
    let blocked = MetadataMaterializationDrainResult {
        processed: 0,
        has_more_due: true,
        next_due_after: None,
    };
    let progressing = MetadataMaterializationDrainResult {
        processed: 4,
        ..blocked
    };
    assert_eq!(drain_delay(&blocked), METADATA_MATERIALIZATION_RETRY_AFTER);
    assert_eq!(
        drain_delay(&progressing),
        METADATA_MATERIALIZATION_NEXT_BATCH_AFTER
    );
}

#[test]
fn reclaim_retry_climbs() {
    // A failing sweep earns the fast retry, then doubles up to the normal
    // interval so a candidate that always fails cannot hot-loop a full
    // rescan every minute.
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let handler = OperationsTaskHandler::new(
        Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }),
        JobsRuntime::new(),
    );
    let key = TaskKey::DrainBlobReclaimQueue;
    let ladder = |handler: &OperationsTaskHandler| {
        handler.retry_ladder(&key, RECLAIM_SWEEP_RETRY, RECLAIM_SWEEP_AFTER)
    };

    assert_eq!(ladder(&handler), RECLAIM_SWEEP_RETRY);
    assert_eq!(ladder(&handler), RECLAIM_SWEEP_RETRY * 2);
    for _ in 0..8 {
        ladder(&handler);
    }
    assert_eq!(ladder(&handler), RECLAIM_SWEEP_AFTER);

    handler.reset_backoff(&key);
    assert_eq!(ladder(&handler), RECLAIM_SWEEP_RETRY);
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

struct InstalledDrainHandler {
    handler: Arc<OperationsTaskHandler>,
    completed: mpsc::Sender<()>,
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

pub(super) async fn installed_setup() -> InstalledHarness {
    let realm_id = RealmId::from_bytes([46u8; 32]);
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let net = make_net_handle(realm_id, &storage, [46u8; 32]).await;
    tokio::time::pause();
    let target = DocumentSyncTarget::RealmAuthorization { realm_id };
    let topic = target.sync_topic_id(realm_id, &aruna_core::structs::PlacementRef::NIL);
    net.ensure_document_sync_topics(&[topic], Vec::new())
        .expect("shared topic genesis");
    for index in 1..=2u128 {
        let record = crate::sync::document_sync_outbox::new_outbox_record_with_id(
            Ulid::from_parts(1, index),
            node(1),
            target.clone(),
            Vec::new(),
            DocumentSyncOutboxEvent::Upsert {
                bytes: index.to_be_bytes().to_vec(),
                change: change(),
            },
            aruna_core::structs::PlacementRef::NIL,
            true,
        );
        write_outbox_record(&storage, &record).await;
    }

    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    });
    let handler = Arc::new(
        OperationsTaskHandler::new(context.clone(), JobsRuntime::new()).with_outbox_limits(1, 1, 2),
    );
    let (completed_tx, completed) = mpsc::channel(4);
    task_handle
        .set_inbound_handler(Arc::new(InstalledDrainHandler {
            handler: handler.clone(),
            completed: completed_tx,
        }))
        .await;
    InstalledHarness {
        _dir: dir,
        storage,
        net,
        task_handle,
        context,
        handler,
        completed,
    }
}
