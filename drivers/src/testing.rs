//! Builds driver contexts, raw storage and graph calls, a local net config, a frozen clock and an
//! RO-Crate import job record for the tests of the drivers, the operations and the node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::structs::execution::job::{
    ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateSpec, ImportRoCrateTarget, JobId,
    JobPayload, JobRecord, RoCrateLimits,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::task::TaskKey;
use aruna_graph::GraphHandle;
use aruna_net::{DiscoveryMethod, NetConfig, RelayMethod};
use aruna_storage::StorageHandle;
use aruna_tasks::{InboundTaskHandler, TaskHandle};
use byteview::ByteView;
use ulid::Ulid;

use crate::driver::DriverContext;

/// An upload import into bucket `target` under `crate`, owned by a fixed user.
pub fn import_record(job_id: JobId, node: NodeId) -> JobRecord {
    let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
    JobRecord::new(
        job_id,
        JobPayload::ImportRoCrate(ImportRoCrateSpec {
            auth_context: AuthContext {
                user_id: owner,
                realm_id: RealmId([1u8; 32]),
                path_restrictions: None,
                session: None,
            },
            source: ImportRoCrateSource::Upload {
                upload_id: Ulid::from_bytes([3u8; 16]),
            },
            target: ImportRoCrateTarget {
                bucket: "target".to_string(),
                prefix: "crate".to_string(),
            },
            metadata: ImportMetadataTarget {
                group_id: Ulid::from_bytes([4u8; 16]),
                path: "crate".to_string(),
                public: false,
            },
            limits: RoCrateLimits::default(),
            document_id: Ulid::from_bytes([5u8; 16]),
        }),
        owner,
        node,
        1,
        1,
        None,
    )
}

/// A driver context with storage and no other plane.
pub fn test_context(storage: StorageHandle) -> DriverContext {
    DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    }
}

/// A net on loopback without relays or discovery, so a test reaches no outside service. A relay
/// connection's timers also read the real clock and spin forever under paused time.
pub fn local_net() -> NetConfig {
    NetConfig {
        bind_addr: "127.0.0.1:0".parse().expect("loopback address"),
        discovery_method: DiscoveryMethod::None,
        relay_method: RelayMethod::None,
        ..NetConfig::default()
    }
}

/// Tokio inhibits paused-clock auto-advance while a blocking task is alive.
/// Storage and net answer from their own threads, so without this the clock
/// races ahead of every round trip. `tokio::time::advance` still applies.
pub struct ClockGuard {
    _stop: std::sync::mpsc::Sender<()>,
}

pub fn freeze_clock() -> ClockGuard {
    let (stop, wait) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || {
        let _ = wait.recv();
    });
    ClockGuard { _stop: stop }
}

/// A storage driver context with a task plane, shared behind an `Arc`.
pub fn task_context(storage: StorageHandle) -> std::sync::Arc<DriverContext> {
    std::sync::Arc::new(DriverContext {
        task_handle: Some(TaskHandle::new()),
        ..test_context(storage)
    })
}

/// A storage-only driver context on a store opened at `root`.
pub fn path_context(root: &str) -> DriverContext {
    test_context(aruna_storage::FjallStorage::open(root).expect("open the test store"))
}

/// A storage-only driver context on a fresh temporary store.
pub fn temp_context() -> (tempfile::TempDir, DriverContext) {
    let dir = tempfile::tempdir().expect("temp dir");
    let context = path_context(dir.path().to_str().expect("temp path"));
    (dir, context)
}

/// Writes one value outside a transaction; panics on any other answer.
pub async fn write_value(
    storage: &StorageHandle,
    key_space: &str,
    key: impl Into<ByteView>,
    value: impl Into<ByteView>,
) {
    let write = StorageEffect::Write {
        key_space: key_space.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    match storage.send_storage_effect(write).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected storage event: {other:?}"),
    }
}

/// Writes `writes` in one batch outside a transaction; panics on any other answer.
pub async fn write_entries(storage: &StorageHandle, writes: Vec<(String, ByteView, ByteView)>) {
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        other => panic!("unexpected storage event: {other:?}"),
    }
}

/// The stored value of `key`; panics on any answer but a read result.
pub async fn read_value(storage: &StorageHandle, keyspace: &str, key: Vec<u8>) -> Option<ByteView> {
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage
        .send_storage_effect(StorageEffect::Read {
            key_space: keyspace.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected storage read event")
    };
    value
}

/// A task handler that sends each fired timer key to `seen`.
pub struct RecordingTaskHandler {
    pub seen: tokio::sync::mpsc::Sender<TaskKey>,
}

#[async_trait::async_trait]
impl InboundTaskHandler for RecordingTaskHandler {
    async fn handle_timer(&self, key: TaskKey) {
        let _ = self.seen.send(key).await;
    }
}

/// The graph handle a test installed on its context.
pub fn installed_graph_handle(context: &DriverContext) -> &GraphHandle {
    context
        .graph_handle
        .as_ref()
        .expect("graph handle installed")
}

/// Runs one metadata effect on this node's graph store and answers with its event; an effect
/// that names a graph skips the deletion guard here.
pub async fn send_metadata_effect(context: &DriverContext, effect: MetadataEffect) -> Event {
    match &context.graph_handle {
        Some(graph) => Event::Metadata(graph.run(effect).await),
        None => Event::Metadata(MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::HandleMissing,
        }),
    }
}

/// The stored user `user_id`; panics when the read answers anything else.
pub async fn read_user(
    context: &DriverContext,
    user_id: aruna_core::UserId,
) -> aruna_core::structs::identity::user::User {
    let key = user_id.to_bytes().to_vec();
    let value = read_value(
        &context.storage_handle,
        aruna_core::keyspaces::USER_KEYSPACE,
        key,
    )
    .await
    .expect("the user is stored");
    aruna_core::structs::identity::user::User::from_bytes(&value).unwrap()
}
