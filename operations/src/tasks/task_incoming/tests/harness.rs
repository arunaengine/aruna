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

#[tokio::test]
async fn paused_runtime_waits() {
    // Startup recovery must wait until production has made S3 reachable.
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    });
    let job_id = job_id();
    let record = JobRecord::new(
        job_id,
        JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        },
        UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
        node(7),
        1,
        1,
        None,
    );
    insert_job(&storage, &record).await.expect("insert job");
    assert!(matches!(
        claim_job(&storage, job_id, node(7), 2).await,
        Ok(ClaimOutcome::Claimed(_))
    ));
    let runtime = JobsRuntime::new_paused();

    initialize_task_incoming(context, task_handle, runtime.clone()).await;

    let stored = read_job_record(&storage, job_id, None)
        .await
        .expect("read job")
        .expect("job exists");
    assert_eq!(stored.state, JobState::Claimed);
    assert_eq!(runtime.recover_stale_jobs(&storage).await.unwrap(), 1);
    runtime.start();
    let stored = read_job_record(&storage, job_id, None)
        .await
        .expect("read job")
        .expect("job exists");
    assert_eq!(stored.state, JobState::Queued);
}

#[tokio::test]
async fn start_keeps_job() {
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    });
    let job_id = job_id();
    let record = JobRecord::new(
        job_id,
        JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        },
        UserId::new(Ulid::from_bytes([3u8; 16]), RealmId([2u8; 32])),
        node(7),
        1,
        1,
        None,
    );
    insert_job(&storage, &record).await.expect("insert job");
    assert!(matches!(
        claim_job(&storage, job_id, node(7), 2).await,
        Ok(ClaimOutcome::Claimed(_))
    ));
    let runtime = JobsRuntime::new_paused();

    let queues = initialize_task_holder(
        context,
        task_handle.clone(),
        runtime.clone(),
        RoCrateLimits::default(),
    )
    .await;
    let shutdown = Shutdown::new();
    queues.start(&shutdown).await;

    let stored = read_job_record(&storage, job_id, None)
        .await
        .expect("read job")
        .expect("job exists");
    assert_eq!(stored.state, JobState::Claimed);
    assert!(!runtime.is_started());

    let requested = Duration::from_secs(7200);
    let TaskEvent::TimerScheduled { after, .. } = task_handle
        .schedule_timer_if_idle(TaskKey::DrainJobQueue, requested)
        .await
    else {
        panic!("expected timer schedule event");
    };
    assert_eq!(after, requested, "job queue timer must remain unscheduled");
    assert!(shutdown.drain(Duration::from_secs(30)).await);
}

pub(super) fn target() -> DocumentSyncTarget {
    DocumentSyncTarget::Group {
        group_id: Ulid::from_parts(7, 1),
    }
}

pub(super) fn change() -> DocumentSyncChange {
    DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: Ulid::from_parts(8, 1),
            actor: node(1),
            updated_at_ms: 9,
        },
        kind: DocumentSyncChangeKind::Upsert,
        placement: aruna_core::structs::PlacementRef::NIL,
    }
}

pub(super) async fn read_graph_prune_jobs(
    storage: &aruna_storage::StorageHandle,
) -> Vec<MetadataGraphPruneJobRecord> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 16,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values
            .into_iter()
            .map(|(_, value)| postcard::from_bytes(&value).expect("prune job decodes"))
            .collect(),
        other => panic!("unexpected storage event: {other:?}"),
    }
}

pub(super) async fn write_outbox_record(
    storage: &aruna_storage::StorageHandle,
    record: &DocumentSyncOutboxRecord,
) {
    match storage
        .send_effect(write_outbox_effect(record).expect("outbox effect"))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected outbox write event: {other:?}"),
    }
}

pub(super) async fn make_net_handle(
    realm_id: RealmId,
    storage: &aruna_storage::StorageHandle,
    secret: [u8; 32],
) -> NetHandle {
    NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            secret_key: Some(iroh::SecretKey::from_bytes(&secret)),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .expect("net handle")
}

pub(super) async fn write_realm_config(
    storage: &aruna_storage::StorageHandle,
    realm_id: RealmId,
    config: &RealmConfigDocument,
    node_id: aruna_core::NodeId,
) {
    let actor = Actor {
        node_id,
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    let bytes = config.to_bytes(&actor).expect("config serializes");
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(realm_id.as_bytes().to_vec()),
            value: ByteView::from(bytes),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected realm config write event: {other:?}"),
    }
}
