//! Tests the synced folder routes and the bucket route for owner checks, binding, pausing,
//! entries and actions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::routes::device::buckets::{CreateBucketRequest, create_device_bucket};
use crate::routes::device::dto::FolderStateName;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::{EntryState, SyncActionRecord, SyncBase, SyncedFolder};
use aruna_core::task::TaskKey;
use aruna_core::types::{Key, Value};
use aruna_operations::device::sync::repository::{action_entry, base_entry, folder_entry};
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use ed25519_dalek::SigningKey;
use tempfile::TempDir;

struct Fixture {
    _dir: TempDir,
    state: Arc<ServerState>,
    owner: UserId,
    realm_id: RealmId,
}

fn realm_key() -> SigningKey {
    SigningKey::from_bytes(&[7u8; 32])
}

fn realm_id() -> RealmId {
    RealmId::from_bytes(realm_key().verifying_key().to_bytes())
}

async fn seed_row(state: &ServerState, row: (String, Key, Value)) {
    let (key_space, key, value) = row;
    let event = state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn setup_with(capabilities: NodeCapabilities) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    };
    serve(dir, context, capabilities).await
}

/// Serves `context` as the owner's device, with a realm config that names it.
async fn serve(dir: TempDir, context: DriverContext, capabilities: NodeCapabilities) -> Fixture {
    let realm_id = realm_id();
    let node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
    let owner = UserId::local(Ulid::from_bytes([9u8; 16]), realm_id);
    let state = Arc::new(
        ServerState::new(
            Arc::new(context),
            realm_id,
            node_id,
            capabilities,
            false,
            None,
            JobsRuntime::new(),
        )
        .await,
    );
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
    };
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(node_id, RealmNodeKind::User { owner });
    seed_row(
        &state,
        (
            REALM_CONFIG_KEYSPACE.to_string(),
            realm_id.as_bytes().to_vec().into(),
            config.to_bytes(&actor).unwrap().into(),
        ),
    )
    .await;
    Fixture {
        _dir: dir,
        state,
        owner,
        realm_id,
    }
}

async fn setup() -> Fixture {
    setup_with(NodeCapabilities::user_node(realm_id()).unwrap()).await
}

/// Sends every timer key the scheduler fires, so a wake is awaited instead of slept on.
struct Fired(tokio::sync::mpsc::UnboundedSender<TaskKey>);

#[async_trait::async_trait]
impl aruna_tasks::InboundTaskHandler for Fired {
    async fn handle_timer(&self, key: TaskKey) {
        let _ = self.0.send(key);
    }
}

/// A device that reads local directories and reports the timers it fires. No pass runs.
async fn local_setup() -> (Fixture, tokio::sync::mpsc::UnboundedReceiver<TaskKey>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let storage = FjallStorage::open(&path).unwrap();
    let net = aruna_net::NetHandle::new(
        aruna_net::NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            realm_id: realm_id(),
            discovery_method: aruna_net::DiscoveryMethod::None,
            relay_method: aruna_net::RelayMethod::None,
            ..aruna_net::NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .unwrap();
    let backend = aruna_core::structs::storage::blob::BackendConfig {
        backend_type: aruna_core::structs::storage::blob::Backend::FileSystem,
        root: format!("{path}/blobs"),
        service_config: std::collections::HashMap::new(),
        bucket_prefix: Some("aruna-".to_string()),
        max_bucket_size: Some(1_000_000),
        multipart_bucket: Some("multipart".to_string()),
        timeouts: Default::default(),
    };
    let blob = aruna_blob::blob::BlobHandler::new(backend, storage.clone(), net)
        .await
        .unwrap();
    let (sender, fired) = tokio::sync::mpsc::unbounded_channel();
    let task_handle = TaskHandle::new();
    task_handle
        .set_inbound_handler(Arc::new(Fired(sender)))
        .await;
    let context = DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: Some(blob),
        metadata_handle: None,
        task_handle: Some(task_handle),
        compute_handle: None,
    };
    let capabilities = NodeCapabilities::user_node(realm_id()).unwrap();
    (serve(dir, context, capabilities).await, fired)
}

fn owner_auth(fixture: &Fixture) -> AuthContext {
    AuthContext {
        user_id: fixture.owner,
        realm_id: fixture.realm_id,
        path_restrictions: None,
        session: None,
    }
}

fn sample_folder(id: Ulid, fixture: &Fixture, root: &str, state: FolderState) -> SyncedFolder {
    SyncedFolder {
        folder_id: id,
        root: root.to_string(),
        local_bucket: format!("folder-{}", id.to_string().to_lowercase()),
        group_id: Ulid::from_bytes([10u8; 16]),
        remote: RemoteBinding {
            node_id: iroh::SecretKey::from_bytes(&[5u8; 32]).public(),
            bucket: "lab".to_string(),
            prefix: String::new(),
        },
        mode: FolderMode::TwoWay,
        propagate_deletes: true,
        state,
        created_by: fixture.owner,
        created_at_ms: 1,
        last_reconcile_ms: None,
        last_error: None,
        last_error_ms: None,
        observed_files: 0,
        list_cursor: None,
    }
}

fn sample_base() -> SyncBase {
    SyncBase {
        synced: None,
        local_version_id: None,
        synced_at_ms: 0,
        entry: EntryState::InSync,
        pending_at: None,
        local: None,
        remote: None,
    }
}

fn sample_action(folder_id: Ulid, actor: UserId) -> SyncActionRecord {
    SyncActionRecord {
        action_id: Ulid::from_bytes([1u8; 16]),
        folder_id,
        kind: ActionKind::Resolve,
        scope: ActionScope::AllPending,
        actor,
        at_ms: 1,
        before: None,
        after: None,
        outcome: ActionOutcome::Applied,
        trashed_to: None,
        entries: 0,
    }
}

#[tokio::test]
async fn owner_boundary() {
    // The device plane exists only on a user node, and only for its owner.
    let user = setup().await;
    let anonymous = list_synced_folders(State(user.state.clone()), Extension(None))
        .await
        .unwrap_err();
    assert_eq!(anonymous.status_code(), StatusCode::UNAUTHORIZED);

    let stranger = AuthContext {
        user_id: UserId::local(Ulid::from_bytes([1u8; 16]), user.realm_id),
        ..owner_auth(&user)
    };
    let foreign = list_synced_folders(State(user.state.clone()), Extension(Some(stranger)))
        .await
        .unwrap_err();
    assert_eq!(foreign.status_code(), StatusCode::FORBIDDEN);

    let restricted = AuthContext {
        path_restrictions: Some(Vec::new()),
        ..owner_auth(&user)
    };
    let delegated = list_synced_folders(State(user.state.clone()), Extension(Some(restricted)))
        .await
        .unwrap_err();
    assert_eq!(delegated.status_code(), StatusCode::FORBIDDEN);

    let mgmt = setup_with(NodeCapabilities::management_node(realm_key()).unwrap()).await;
    let no_plane = list_synced_folders(
        State(mgmt.state.clone()),
        Extension(Some(owner_auth(&mgmt))),
    )
    .await
    .unwrap_err();
    assert_eq!(no_plane.status_code(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn binds_bad_input() {
    let fixture = setup().await;
    let request = |group_id: &str, node: &str| BindFolderRequest {
        root: "/data".to_string(),
        group_id: group_id.to_string(),
        remote_node_id: node.to_string(),
        remote_bucket: "lab".to_string(),
        create_bucket: false,
        remote_prefix: String::new(),
        mode: None,
        propagate_deletes: None,
    };
    let node = iroh::SecretKey::from_bytes(&[5u8; 32]).public().to_string();
    let bad_group = bind_synced_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Json(request("nope", &node)),
    )
    .await
    .unwrap_err();
    assert_eq!(bad_group.status_code(), StatusCode::BAD_REQUEST);

    let group = Ulid::from_bytes([10u8; 16]).to_string();
    let bad_node = bind_synced_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Json(request(&group, "not-a-node")),
    )
    .await
    .unwrap_err();
    assert_eq!(bad_node.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn lists_folders() {
    let fixture = setup().await;
    let Json(empty) = list_synced_folders(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
    )
    .await
    .unwrap();
    assert!(empty.folders.is_empty());

    let id = Ulid::from_bytes([20u8; 16]);
    seed_row(
        &fixture.state,
        folder_entry(&sample_folder(id, &fixture, "/data", FolderState::Active)).unwrap(),
    )
    .await;
    let Json(listed) = list_synced_folders(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
    )
    .await
    .unwrap();
    assert_eq!(listed.folders.len(), 1);
    assert_eq!(listed.folders[0].folder_id, id.to_string());
}

#[tokio::test]
async fn reads_folder() {
    let fixture = setup().await;
    let bad = get_synced_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path("not-a-ulid".to_string()),
    )
    .await
    .unwrap_err();
    assert_eq!(bad.status_code(), StatusCode::BAD_REQUEST);

    let missing = get_synced_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(Ulid::from_bytes([21u8; 16]).to_string()),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.status_code(), StatusCode::NOT_FOUND);

    let id = Ulid::from_bytes([22u8; 16]);
    seed_row(
        &fixture.state,
        folder_entry(&sample_folder(id, &fixture, "/data", FolderState::Active)).unwrap(),
    )
    .await;
    let Json(view) = get_synced_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(view.folder_id, id.to_string());
}

#[tokio::test]
async fn unbinds_folder() {
    let fixture = setup().await;
    let bad = unbind_synced_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path("bad".to_string()),
    )
    .await
    .unwrap_err();
    assert_eq!(bad.status_code(), StatusCode::BAD_REQUEST);

    let missing = unbind_synced_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(Ulid::from_bytes([23u8; 16]).to_string()),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.status_code(), StatusCode::NOT_FOUND);

    let id = Ulid::from_bytes([24u8; 16]);
    seed_row(
        &fixture.state,
        folder_entry(&sample_folder(id, &fixture, "/data", FolderState::Active)).unwrap(),
    )
    .await;
    let Json(unbound) = unbind_synced_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(unbound.folder_id, id.to_string());
    let Json(deleting) = get_synced_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(deleting.state, FolderStateName::Deleting);
}

#[tokio::test]
async fn toggles_deletes() {
    let fixture = setup().await;
    let id = Ulid::from_bytes([27u8; 16]);
    seed_row(
        &fixture.state,
        folder_entry(&sample_folder(id, &fixture, "/data", FolderState::Active)).unwrap(),
    )
    .await;
    let Json(updated) = update_synced_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
        Json(UpdateFolderRequest {
            propagate_deletes: false,
        }),
    )
    .await
    .unwrap();
    assert!(!updated.propagate_deletes);

    let download = Ulid::from_bytes([28u8; 16]);
    let mut folder = sample_folder(download, &fixture, "/other", FolderState::Active);
    folder.mode = FolderMode::DownloadOnly;
    folder.propagate_deletes = false;
    seed_row(&fixture.state, folder_entry(&folder).unwrap()).await;
    let refused = update_synced_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(download.to_string()),
        Json(UpdateFolderRequest {
            propagate_deletes: true,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(refused.status_code(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn pause_resume() {
    let fixture = setup().await;
    let bad = pause_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path("bad".to_string()),
    )
    .await
    .unwrap_err();
    assert_eq!(bad.status_code(), StatusCode::BAD_REQUEST);

    let id = Ulid::from_bytes([25u8; 16]);
    seed_row(
        &fixture.state,
        folder_entry(&sample_folder(id, &fixture, "/data", FolderState::Active)).unwrap(),
    )
    .await;
    let Json(paused) = pause_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(paused.state, FolderStateName::Paused);

    let Json(resumed) = resume_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(resumed.state, FolderStateName::Active);

    // A folder mid-cleanup cannot be paused back into service.
    let deleting = Ulid::from_bytes([26u8; 16]);
    seed_row(
        &fixture.state,
        folder_entry(&sample_folder(
            deleting,
            &fixture,
            "/other",
            FolderState::Deleting,
        ))
        .unwrap(),
    )
    .await;
    let refused = pause_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(deleting.to_string()),
    )
    .await
    .unwrap_err();
    assert_eq!(refused.status_code(), StatusCode::NOT_FOUND);
}

/// A folder still binding shows as `binding` and cannot be paused or resumed past its bucket.
#[tokio::test]
async fn refuses_binding_pause() {
    let fixture = setup().await;
    let id = Ulid::from_bytes([32u8; 16]);
    let binding = FolderState::Binding {
        create_bucket: true,
    };
    seed_row(
        &fixture.state,
        folder_entry(&sample_folder(id, &fixture, "/data", binding)).unwrap(),
    )
    .await;
    let Json(view) = get_synced_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(view.state, FolderStateName::Binding);
    for paused in [true, false] {
        let call = match paused {
            true => {
                pause_folder(
                    State(fixture.state.clone()),
                    Extension(Some(owner_auth(&fixture))),
                    Path(id.to_string()),
                )
                .await
            }
            false => {
                resume_folder(
                    State(fixture.state.clone()),
                    Extension(Some(owner_auth(&fixture))),
                    Path(id.to_string()),
                )
                .await
            }
        };
        assert_eq!(call.unwrap_err().status_code(), StatusCode::CONFLICT);
    }
}

#[tokio::test]
async fn sync_needs_active() {
    let fixture = setup().await;
    let bad = sync_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path("bad".to_string()),
    )
    .await
    .unwrap_err();
    assert_eq!(bad.status_code(), StatusCode::BAD_REQUEST);

    let missing = sync_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(Ulid::from_bytes([27u8; 16]).to_string()),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.status_code(), StatusCode::NOT_FOUND);

    let id = Ulid::from_bytes([28u8; 16]);
    seed_row(
        &fixture.state,
        folder_entry(&sample_folder(id, &fixture, "/data", FolderState::Paused)).unwrap(),
    )
    .await;
    let paused = sync_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
    )
    .await
    .unwrap_err();
    assert_eq!(paused.status_code(), StatusCode::CONFLICT);
}

/// "Sync now" answers with the folder as it stands and leaves the pass to the timer it wakes.
#[tokio::test]
async fn sync_skips_scan() {
    let (fixture, mut fired) = local_setup().await;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("note.txt"), b"local").unwrap();
    let id = Ulid::from_bytes([29u8; 16]);
    let folder = sample_folder(
        id,
        &fixture,
        root.path().to_str().unwrap(),
        FolderState::Active,
    );
    seed_row(&fixture.state, folder_entry(&folder).unwrap()).await;

    let Json(view) = sync_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(view.counters.observed, 0);
    let offers =
        aruna_operations::staging::offered_directory::list_offers(&fixture.state.get_ctx())
            .await
            .unwrap();
    assert!(offers.is_empty(), "the request must not walk the folder");
    let woken = tokio::time::timeout(std::time::Duration::from_secs(300), async {
        while let Some(key) = fired.recv().await {
            if key == TaskKey::ReconcileSyncedFolders {
                return true;
            }
        }
        false
    })
    .await;
    assert_eq!(woken, Ok(true), "the folder pass must be woken");
}

/// A root that is gone, or runs through a file, is the owner's input error on bind and sync now.
#[tokio::test]
async fn refuses_missing_root() {
    let (fixture, _fired) = local_setup().await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("note.txt"), b"local").unwrap();
    let unusable = |error: &ServerError| {
        matches!(error, ServerError::BadRequestReason(reason)
            if reason.starts_with("the directory is unusable"))
    };
    for (index, root) in [dir.path().join("missing"), dir.path().join("note.txt/sub")]
        .iter()
        .enumerate()
    {
        let root = root.to_string_lossy().to_string();
        let bind = bind_synced_folder(
            State(fixture.state.clone()),
            Extension(Some(owner_auth(&fixture))),
            Json(BindFolderRequest {
                root: root.clone(),
                group_id: Ulid::from_bytes([10u8; 16]).to_string(),
                remote_node_id: iroh::SecretKey::from_bytes(&[5u8; 32]).public().to_string(),
                remote_bucket: "lab".to_string(),
                create_bucket: true,
                remote_prefix: String::new(),
                mode: None,
                propagate_deletes: None,
            }),
        )
        .await
        .unwrap_err();
        assert!(unusable(&bind), "bind of {root}: {bind:?}");

        let id = Ulid::from_bytes([30 + index as u8; 16]);
        let folder = sample_folder(id, &fixture, &root, FolderState::Active);
        seed_row(&fixture.state, folder_entry(&folder).unwrap()).await;
        let sync = sync_folder(
            State(fixture.state.clone()),
            Extension(Some(owner_auth(&fixture))),
            Path(id.to_string()),
        )
        .await
        .unwrap_err();
        assert!(unusable(&sync), "sync of {root}: {sync:?}");
    }
}

/// The bucket route admits only the owner and refuses bad input, this computer and nodes outside
/// the realm before it forwards anything.
#[tokio::test]
async fn bucket_bad_input() {
    let fixture = setup().await;
    let group = Ulid::from_bytes([10u8; 16]).to_string();
    let outsider = iroh::SecretKey::from_bytes(&[5u8; 32]).public().to_string();
    let own = fixture.state.get_node_id().to_string();
    let create = |auth: AuthContext, name: &str, group: &str, node: &str| {
        create_device_bucket(
            State(fixture.state.clone()),
            Extension(Some(auth)),
            Json(CreateBucketRequest {
                name: name.to_string(),
                group_id: group.to_string(),
                node_id: node.to_string(),
            }),
        )
    };
    let stranger = AuthContext {
        user_id: UserId::local(Ulid::from_bytes([1u8; 16]), fixture.realm_id),
        ..owner_auth(&fixture)
    };
    let foreign = create(stranger, "lab-data", &group, &outsider)
        .await
        .unwrap_err();
    assert_eq!(foreign.status_code(), StatusCode::FORBIDDEN);

    for (name, group, node) in [
        ("Lab_Data", group.as_str(), outsider.as_str()),
        ("lab-data", "nope", outsider.as_str()),
        ("lab-data", group.as_str(), "not-a-node"),
    ] {
        let bad = create(owner_auth(&fixture), name, group, node)
            .await
            .unwrap_err();
        assert_eq!(
            bad.status_code(),
            StatusCode::BAD_REQUEST,
            "{name} {group} {node}"
        );
    }
    for node in [&own, &outsider] {
        let refused = create(owner_auth(&fixture), "lab-data", &group, node)
            .await
            .unwrap_err();
        assert_eq!(refused.status_code(), StatusCode::BAD_REQUEST);
        assert_eq!(
            refused.to_string(),
            format!("node {node} is not a realm server")
        );
    }
}

#[tokio::test]
async fn list_entries() {
    let fixture = setup().await;
    let bad = list_folder_entries(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path("bad".to_string()),
        Query(EntryQuery {
            state: None,
            cursor: None,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(bad.status_code(), StatusCode::BAD_REQUEST);

    let id = Ulid::from_bytes([29u8; 16]);
    let bad_cursor = list_folder_entries(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
        Query(EntryQuery {
            state: None,
            cursor: Some("!!!".to_string()),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(bad_cursor.status_code(), StatusCode::BAD_REQUEST);

    seed_row(
        &fixture.state,
        base_entry(id, "notes.txt", &sample_base()).unwrap(),
    )
    .await;
    let Json(page) = list_folder_entries(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
        Query(EntryQuery {
            state: None,
            cursor: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].path, "notes.txt");

    let Json(filtered) = list_folder_entries(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
        Query(EntryQuery {
            state: Some("conflict".to_string()),
            cursor: None,
        }),
    )
    .await
    .unwrap();
    assert!(filtered.entries.is_empty());
}

#[tokio::test]
async fn entry_action_guard() {
    let fixture = setup().await;
    let request = |action, expected| EntryActionRequest { action, expected };
    let bad = act_on_entry(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(("bad".to_string(), "notes.txt".to_string())),
        Json(request(EntryAction::Resolve, None)),
    )
    .await
    .unwrap_err();
    assert_eq!(bad.status_code(), StatusCode::BAD_REQUEST);

    let id = Ulid::from_bytes([30u8; 16]);
    let missing = act_on_entry(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path((id.to_string(), "notes.txt".to_string())),
        Json(request(EntryAction::ReplaceLocal, None)),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.status_code(), StatusCode::PRECONDITION_FAILED);

    let expected = ExpectedBytes {
        fingerprint: "fp".to_string(),
        blake3: "nothex".to_string(),
        remote_version: None,
    };
    let bad_hash = act_on_entry(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path((id.to_string(), "notes.txt".to_string())),
        Json(request(EntryAction::ReplaceLocal, Some(expected))),
    )
    .await
    .unwrap_err();
    assert_eq!(bad_hash.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn folder_action_guard() {
    let fixture = setup().await;
    let request = |action, scope, confirm: &str| FolderActionRequest {
        action,
        scope,
        confirm: confirm.to_string(),
    };
    let bad_id = act_on_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path("bad".to_string()),
        Json(request(
            EntryAction::ReplaceLocal,
            ActionScopeName::AllPending,
            "data",
        )),
    )
    .await
    .unwrap_err();
    assert_eq!(bad_id.status_code(), StatusCode::BAD_REQUEST);

    let id = Ulid::from_bytes([31u8; 16]);
    let wrong_action = act_on_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
        Json(request(
            EntryAction::KeepLocal,
            ActionScopeName::AllPending,
            "data",
        )),
    )
    .await
    .unwrap_err();
    assert_eq!(wrong_action.status_code(), StatusCode::BAD_REQUEST);

    let wrong_scope = act_on_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
        Json(request(
            EntryAction::ReplaceLocal,
            ActionScopeName::Entry,
            "data",
        )),
    )
    .await
    .unwrap_err();
    assert_eq!(wrong_scope.status_code(), StatusCode::BAD_REQUEST);

    let missing = act_on_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
        Json(request(
            EntryAction::ReplaceLocal,
            ActionScopeName::AllPending,
            "data",
        )),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.status_code(), StatusCode::NOT_FOUND);

    seed_row(
        &fixture.state,
        folder_entry(&sample_folder(
            id,
            &fixture,
            "/home/ada/data",
            FolderState::Active,
        ))
        .unwrap(),
    )
    .await;
    let wrong_confirm = act_on_folder(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
        Json(request(
            EntryAction::ReplaceLocal,
            ActionScopeName::AllPending,
            "wrong",
        )),
    )
    .await
    .unwrap_err();
    assert_eq!(wrong_confirm.status_code(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn list_actions() {
    let fixture = setup().await;
    let bad = list_folder_actions(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path("bad".to_string()),
        Query(ActionQuery { cursor: None }),
    )
    .await
    .unwrap_err();
    assert_eq!(bad.status_code(), StatusCode::BAD_REQUEST);

    let id = Ulid::from_bytes([32u8; 16]);
    let bad_cursor = list_folder_actions(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
        Query(ActionQuery {
            cursor: Some("!!!".to_string()),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(bad_cursor.status_code(), StatusCode::BAD_REQUEST);

    seed_row(
        &fixture.state,
        action_entry(&sample_action(id, fixture.owner)).unwrap(),
    )
    .await;
    let Json(page) = list_folder_actions(
        State(fixture.state.clone()),
        Extension(Some(owner_auth(&fixture))),
        Path(id.to_string()),
        Query(ActionQuery { cursor: None }),
    )
    .await
    .unwrap();
    assert_eq!(page.actions.len(), 1);
}
