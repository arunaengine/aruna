use std::sync::Arc;
use std::time::Duration;

use aruna_core::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent, DocumentSyncOutboxRecord,
    DocumentSyncRevision, DocumentSyncTarget,
};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{METADATA_GRAPH_PRUNE_JOB_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::metadata::MetadataGraphPruneJobRecord;
use aruna_core::structs::{Actor, FIRST_GRANTABLE_HANDLE, JobId, RealmConfigDocument, RealmId};
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::types::UserId;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use async_trait::async_trait;
use byteview::ByteView;
use tempfile::tempdir;
use tokio::sync::mpsc;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::jobs::runtime::JobsRuntime;
use crate::sync::document_outbox::write_outbox_effect;
use crate::tasks::incoming::OperationsTaskHandler;
use aruna_tasks::InboundTaskHandler;

pub(crate) fn job_id() -> JobId {
    crate::jobs::submit::mint_job_id(
        PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
        BucketId::new(0).unwrap(),
    )
    .unwrap()
}

pub(crate) struct RecordingTaskHandler {
    pub(crate) seen: mpsc::Sender<TaskKey>,
}

#[async_trait]
impl InboundTaskHandler for RecordingTaskHandler {
    async fn handle_timer(&self, key: TaskKey) {
        let _ = self.seen.send(key).await;
    }
}

pub(crate) struct InstalledDrainHandler {
    pub(crate) handler: Arc<OperationsTaskHandler>,
    pub(crate) completed: mpsc::Sender<()>,
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
pub(crate) struct ClockGuard {
    _stop: std::sync::mpsc::Sender<()>,
}

pub(crate) fn freeze_clock() -> ClockGuard {
    let (stop, wait) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || {
        let _ = wait.recv();
    });
    ClockGuard { _stop: stop }
}

// Net shutdown drains under a timeout that a still clock never expires.
pub(crate) async fn shutdown_net(net: &NetHandle) {
    tokio::time::resume();
    net.shutdown().await;
}

pub(crate) struct InstalledHarness {
    pub(crate) _dir: tempfile::TempDir,
    pub(crate) storage: aruna_storage::StorageHandle,
    pub(crate) net: NetHandle,
    pub(crate) task_handle: TaskHandle,
    pub(crate) context: Arc<DriverContext>,
    pub(crate) handler: Arc<OperationsTaskHandler>,
    pub(crate) completed: mpsc::Receiver<()>,
}

// The frozen clock only moves on an explicit advance, so waiting here cannot
// outrun the drain; a poll bound would instead depend on machine speed.
pub(crate) async fn recv_progress(receiver: &mut mpsc::Receiver<()>) -> bool {
    receiver.recv().await.is_some()
}

pub(crate) fn node(seed: u8) -> aruna_core::NodeId {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    iroh::SecretKey::from_bytes(&bytes).public()
}

pub(crate) fn outbox_handler() -> (tempfile::TempDir, OperationsTaskHandler, TaskHandle) {
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

pub(crate) async fn scheduled_after(task_handle: &TaskHandle) -> Duration {
    let TaskEvent::TimerScheduled { after, .. } = task_handle
        .schedule_idle_timer(TaskKey::DrainDocumentSyncOutbox, Duration::ZERO)
        .await
    else {
        panic!("expected timer schedule event");
    };
    after
}

pub(crate) async fn installed_setup() -> InstalledHarness {
    let realm_id = RealmId::from_bytes([46u8; 32]);
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let net = make_net_handle(realm_id, &storage, [46u8; 32]).await;
    tokio::time::pause();
    let target = DocumentSyncTarget::RealmAuthorization { realm_id };
    let topic = target.sync_topic_id(realm_id, &aruna_core::structs::PlacementRef::NIL);
    net.ensure_sync_topics(&[topic], Vec::new())
        .expect("shared topic genesis");
    for index in 1..=2u128 {
        let record = crate::sync::document_outbox::new_identified_record(
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

pub(crate) fn target() -> DocumentSyncTarget {
    DocumentSyncTarget::Group {
        group_id: Ulid::from_parts(7, 1),
    }
}

pub(crate) fn change() -> DocumentSyncChange {
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

pub(crate) async fn read_graph_jobs(
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

pub(crate) async fn write_outbox_record(
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

pub(crate) async fn make_net_handle(
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

pub(crate) async fn write_realm_config(
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
