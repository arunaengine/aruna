use super::*;
use crate::routes::device::dto::FolderStateName;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::{
    Actor, EntryState, NodeCapabilities, RealmConfigDocument, RealmId, RealmNodeKind,
    SyncActionRecord, SyncBase, SyncedFolder,
};
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
    let realm_id = realm_id();
    let node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
    let owner = UserId::local(Ulid::from_bytes([9u8; 16]), realm_id);
    let state = Arc::new(
        ServerState::new(
            Arc::new(DriverContext {
                storage_handle: storage,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: Some(TaskHandle::new()),
                compute_handle: None,
            }),
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
        last_error_at_ms: None,
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
    assert_eq!(unbound.removed, 0);
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
