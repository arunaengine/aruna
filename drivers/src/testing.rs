//! Builds driver contexts, raw storage, graph, bucket and access rows, a local net config, a frozen
//! clock and an RO-Crate import job record for the tests of the drivers, the operations and the node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::egress::EgressPolicy;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::structs::execution::job::{
    ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateSpec, ImportRoCrateTarget, JobId,
    JobPayload, JobRecord, RoCrateLimits,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId,
};
use aruna_core::structs::storage::blob::{Backend, BackendConfig};
use aruna_core::structs::storage::format::Compression;
use aruna_core::task::TaskKey;
use aruna_core::types::GroupId;
use aruna_graph::GraphHandle;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
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

/// `context` with a file system blob backend below `root` on the same storage.
pub async fn with_blob(context: &DriverContext, root: &std::path::Path) -> DriverContext {
    let storage_handle = context.storage_handle.clone();
    let net_handle = NetHandle::new(local_net(), storage_handle.clone())
        .await
        .unwrap();
    let config = BackendConfig {
        backend_type: Backend::FileSystem,
        bucket_prefix: Some("aruna_".to_string()),
        max_bucket_size: Some(100000),
        multipart_bucket: Some("multipart".to_string()),
        root: root.to_str().unwrap().to_string(),
        service_config: HashMap::new(),
        timeouts: Default::default(),
    };
    let policy = EgressPolicy::loopback();
    let blob = BlobHandler::with_egress(config, storage_handle, net_handle, policy).await;
    let blob = blob.unwrap();
    let (http, policy) = blob.repository_client();
    DriverContext {
        blob_handle: Some(blob),
        outbound_handle: Some(aruna_outbound::OutboundHandle::new(http, policy)),
        ..context.clone()
    }
}

/// Stores bucket `bucket` of `group_id`, created by `user_id` with `policies`.
pub async fn seed_bucket(
    context: &DriverContext,
    bucket: &str,
    group_id: GroupId,
    user_id: UserId,
    policies: Vec<aruna_core::structs::placement::policy::PlacementPolicyRef>,
) {
    let info = aruna_core::structs::storage::blob::BucketInfo {
        group_id,
        created_at: std::time::UNIX_EPOCH,
        created_by: user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: policies,
        placement_policy_generation: 1,
        compression: Compression::Off,
        distribution: None,
    };
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: aruna_core::keyspaces::S3_BUCKET_KEYSPACE.to_string(),
            key: bucket.as_bytes().to_vec().into(),
            value: info.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
}

/// The group owner may write everywhere in the group except below `denied`.
pub async fn seed_access(
    context: &DriverContext,
    scope: (RealmId, GroupId, NodeId),
    denied: &str,
) -> AuthContext {
    seed_open_access(context, scope, denied, None, Vec::new()).await
}

/// Like `seed_access`, plus a role that lets everyone read `public` and realm request policies.
pub async fn seed_open_access(
    context: &DriverContext,
    (realm_id, group_id, node_id): (RealmId, GroupId, NodeId),
    denied: &str,
    public: Option<&str>,
    policies: Vec<aruna_core::request_policy::RequestPolicy>,
) -> AuthContext {
    let owner = UserId::local(Ulid::from_parts(5, 5), realm_id);
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
    };
    let mut document = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    for role in document.roles.values_mut() {
        role.permissions
            .insert(denied.to_string(), Permission::DENY);
    }
    if let Some(public) = public {
        let role_id = Ulid::from_parts(6, 6);
        document.roles.insert(
            role_id,
            aruna_core::structs::identity::auth::Role {
                role_id,
                name: "public".to_string(),
                permissions: std::collections::HashMap::from([(
                    public.to_string(),
                    Permission::READ,
                )]),
                assigned_users: std::collections::HashSet::from([UserId::nil(realm_id)]),
            },
        );
    }
    let group = Group {
        display_name: "Lab".to_string(),
        group_id,
        realm_id,
        roles: document.roles.keys().copied().collect(),
        owner,
    };
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 1);
    config.request_policies = policies;
    let realm_doc = RealmAuthorizationDocument::default_realm_doc(realm_id);
    let (realm_key, group_key) = (realm_id.as_bytes().to_vec(), group_id.to_bytes().to_vec());
    let rows = [
        (
            REALM_CONFIG_KEYSPACE,
            realm_key.clone(),
            config.to_bytes(&actor),
        ),
        (AUTH_KEYSPACE, realm_key, realm_doc.to_bytes(&actor)),
        (GROUP_KEYSPACE, group_key.clone(), group.to_bytes(&actor)),
        (AUTH_KEYSPACE, group_key, document.to_bytes(&actor)),
    ];
    let writes = rows
        .into_iter()
        .map(|(space, key, value)| (space.to_string(), key.into(), value.unwrap().into()))
        .collect();
    let effect = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    let event = context.storage_handle.send_storage_effect(effect).await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    AuthContext {
        user_id: owner,
        realm_id,
        path_restrictions: None,
        session: None,
    }
}

/// The wait after the first miss of [`poll_until`].
const POLL_FIRST: std::time::Duration = std::time::Duration::from_millis(10);
/// The longest wait between two asks of [`poll_until`].
const POLL_MOST: std::time::Duration = std::time::Duration::from_millis(500);

/// Asks `check` until it answers `Some`, waiting twice as long after each miss up to half a
/// second. Answers `None` once `cap` has passed: a hang cap for tests, never a speed bound.
pub async fn poll_until<T>(
    cap: std::time::Duration,
    mut check: impl AsyncFnMut() -> Option<T>,
) -> Option<T> {
    let deadline = tokio::time::Instant::now() + cap;
    let mut wait = POLL_FIRST;
    loop {
        if let Some(found) = check().await {
            return Some(found);
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return None;
        }
        tokio::time::sleep(wait.min(deadline - now)).await;
        wait = wait.saturating_mul(2).min(POLL_MOST);
    }
}
