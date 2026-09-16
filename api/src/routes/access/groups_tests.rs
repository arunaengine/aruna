use super::{
    AddMemberRequest, CreateGroupRequest, DataPathKind, DataPathsQuery, GroupInfoResponse,
    ListGroupsQuery, UpdateGroupRequest, add_group_member, create_group, get_group,
    get_group_usage, list_data_paths, list_group_members, list_groups, run_get_group, update_group,
};
use crate::auth::ValidatedBearer;
use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;
use crate::tests::routes::{seed_realm_auth, test_context, test_state, test_storage};
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keys::generate_signing_key;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    GROUP_KEYSPACE, S3_BUCKET_KEYSPACE, USER_KEYSPACE,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities, Role};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{RealmId, RealmNodeKind};
use aruna_core::structs::identity::user::User;
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion, BucketInfo,
    CurrentVersionPointer, VersionKey, bucket_permission_path, object_permission_path,
};
use aruna_operations::driver::DriverContext;
use aruna_operations::driver::drive;
use aruna_operations::groups::list_groups::ListGroupOperation;
use aruna_storage::storage;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use byteview::ByteView;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::{TempDir, tempdir};
use ulid::Ulid;

async fn store_bytes(state: &ServerState, keyspace: &str, key: Vec<u8>, value: Vec<u8>) {
    match state
        .get_ctx()
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: keyspace.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(value),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected write result: {other:?}"),
    }
}

async fn seed_group(state: &ServerState, owner: UserId) -> Ulid {
    let realm_id = state.get_realm_id();
    let group_id = Ulid::generate();
    let auth_doc = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    let group = Group {
        display_name: "Test".to_string(),
        group_id,
        realm_id,
        roles: auth_doc.roles.keys().copied().collect(),
        owner,
    };
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: owner,
        realm_id,
    };
    store_bytes(
        state,
        GROUP_KEYSPACE,
        group_id.to_bytes().to_vec(),
        group.to_bytes(&actor).unwrap(),
    )
    .await;
    store_bytes(
        state,
        AUTH_KEYSPACE,
        group_id.to_bytes().to_vec(),
        auth_doc.to_bytes(&actor).unwrap(),
    )
    .await;
    group_id
}

async fn store_user(state: &ServerState, user_id: UserId, name: &str) {
    let user = User {
        user_id,
        name: name.to_string(),
        subject_ids: Vec::new(),
        alias_user_ids: Default::default(),
        attributes: Default::default(),
    };
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id,
        realm_id: user_id.realm_id,
    };
    store_bytes(
        state,
        USER_KEYSPACE,
        user_id.to_bytes(),
        user.to_bytes(&actor).unwrap(),
    )
    .await;
}

fn member_auth(user_id: UserId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id: user_id.realm_id,
        path_restrictions: None,
        session: None,
    }
}

async fn setup_state() -> (Arc<ServerState>, TempDir) {
    let (tempdir, storage_handle) = test_storage();
    let driver_ctx = Arc::new(test_context(storage_handle));
    let realm_signing_key = generate_signing_key();
    let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let state = Arc::new(
        test_state(
            driver_ctx,
            realm_id,
            iroh::SecretKey::generate().public(),
            NodeCapabilities::user_node(realm_id).unwrap(),
        )
        .await,
    );

    // The data permission check reads the realm auth doc; seed an empty one
    // so authority comes solely from group roles.
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    seed_realm_auth(&state.get_ctx(), realm_id, &actor).await;
    // Policy loading fails closed without the realm config document, and a
    // node the configuration does not name has no resolvable kind.
    let mut config = aruna_core::structs::identity::realm::RealmConfigDocument::default_for_realm(
        realm_id,
        Vec::new(),
    );
    config.ensure_node(state.get_node_id(), RealmNodeKind::Management);
    store_bytes(
        &state,
        aruna_core::keyspaces::REALM_CONFIG_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        config.to_bytes(&actor).unwrap(),
    )
    .await;

    (state, tempdir)
}

fn foreign_auth() -> AuthContext {
    let realm_id = RealmId::from_bytes([7u8; 32]);
    AuthContext {
        user_id: UserId::local(Ulid::generate(), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    }
}

/// A realm whose initial admin is claimed, so the realm admin path grants.
async fn setup_admin_state() -> (Arc<ServerState>, UserId, TempDir) {
    let tempdir = tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(aruna_tasks::TaskHandle::new()),
        compute_handle: None,
    });
    let realm_signing_key = generate_signing_key();
    let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let node_id = iroh::SecretKey::generate().public();
    let admin = UserId::local(Ulid::generate(), realm_id);
    let actor = Actor {
        node_id,
        user_id: admin,
        realm_id,
    };
    drive(
        aruna_operations::realm::create_realm::CreateRealmOperation::new(
            aruna_operations::realm::create_realm::CreateRealmConfig {
                actor: actor.clone(),
                realm_description: "groups".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            },
        ),
        &driver_ctx,
    )
    .await
    .unwrap();
    drive(
        aruna_operations::realm::claim_admin::ClaimInitialOperation::new(
            aruna_operations::realm::claim_admin::ClaimInitialInput { actor },
        ),
        &driver_ctx,
    )
    .await
    .unwrap();
    let state = Arc::new(
        ServerState::new(
            driver_ctx,
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
            false,
            None,
            aruna_operations::jobs::runtime::JobsRuntime::new(),
        )
        .await,
    );

    (state, admin, tempdir)
}

async fn update_config(
    state: &ServerState,
    mutate: impl FnOnce(&mut aruna_core::structs::identity::realm::RealmConfigDocument),
) {
    let realm_id = state.get_realm_id();
    let mut config = drive(
        aruna_operations::realm::get_config::GetConfigOperation::new(realm_id),
        &state.get_ctx(),
    )
    .await
    .unwrap();
    mutate(&mut config);
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    store_bytes(
        state,
        aruna_core::keyspaces::REALM_CONFIG_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        config.to_bytes(&actor).unwrap(),
    )
    .await;
}

async fn new_group(state: &Arc<ServerState>, user_id: UserId, name: &str) -> ServerResult<()> {
    create_group(
        State(state.clone()),
        Extension(Some(member_auth(user_id))),
        Extension(Some(ValidatedBearer::new_for_test("token"))),
        Json(CreateGroupRequest {
            name: name.to_string(),
        }),
    )
    .await
    .map(|(status, _)| assert_eq!(status, StatusCode::CREATED))
}

#[tokio::test]
async fn device_refuses_add() {
    // A local apply would enqueue an admin record every realm holder rejects,
    // so the device must refuse instead of answering success and diverging.
    let (state, admin, _tempdir) = setup_admin_state().await;
    let node_id = state.get_node_id().to_string();
    update_config(&state, |config| {
        for node in &mut config.nodes {
            if node.node_id == node_id {
                node.kind = RealmNodeKind::User { owner: admin };
            }
        }
    })
    .await;

    let error = add_group_member(
        State(state.clone()),
        Extension(Some(member_auth(admin))),
        Path(Ulid::generate().to_string()),
        Json(AddMemberRequest {
            user_id: admin.to_string(),
            role_ids: None,
        }),
    )
    .await
    .unwrap_err();

    assert!(matches!(error, ServerError::Conflict(reason) if reason.contains("through the realm")));
}

async fn rename(
    state: &Arc<ServerState>,
    auth: Option<AuthContext>,
    group_id: Ulid,
    name: &str,
) -> ServerResult<GroupInfoResponse> {
    update_group(
        State(state.clone()),
        Extension(auth),
        Path(group_id.to_string()),
        Json(UpdateGroupRequest {
            display_name: name.to_string(),
        }),
    )
    .await
    .map(|(status, Json(body))| {
        assert_eq!(status, StatusCode::OK);
        body
    })
}

#[tokio::test]
async fn renames_group() {
    let (state, admin, _tempdir) = setup_admin_state().await;
    let group_id = seed_group(&state, admin).await;

    let renamed = rename(&state, Some(member_auth(admin)), group_id, "  Platform  ")
        .await
        .unwrap();
    assert_eq!(renamed.display_name, "Platform");
    assert_eq!(renamed.group_id, group_id.to_string());

    let stored = run_get_group(&state, Some(member_auth(admin)), &group_id.to_string())
        .await
        .unwrap();
    assert_eq!(stored.display_name, "Platform");
    assert_eq!(stored.roles.len(), renamed.roles.len());
}

#[tokio::test]
async fn renames_foreign_group() {
    // A realm admin renames a group they are not a member of.
    let (state, admin, _tempdir) = setup_admin_state().await;
    let owner = UserId::local(Ulid::generate(), state.get_realm_id());
    let group_id = seed_group(&state, owner).await;

    let renamed = rename(&state, Some(member_auth(admin)), group_id, "Platform")
        .await
        .unwrap();
    assert_eq!(renamed.display_name, "Platform");
}

#[tokio::test]
async fn rename_refuses_stranger() {
    let (state, admin, _tempdir) = setup_admin_state().await;
    let group_id = seed_group(&state, admin).await;
    let stranger = UserId::local(Ulid::generate(), state.get_realm_id());

    let error = rename(&state, Some(member_auth(stranger)), group_id, "Platform")
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::Forbidden));
    assert!(matches!(
        rename(&state, None, group_id, "Platform")
            .await
            .unwrap_err(),
        ServerError::Unauthorized
    ));
    assert!(matches!(
        rename(&state, Some(foreign_auth()), group_id, "Platform")
            .await
            .unwrap_err(),
        ServerError::Forbidden
    ));
}

#[tokio::test]
async fn rename_rejects_name() {
    let (state, admin, _tempdir) = setup_admin_state().await;
    let group_id = seed_group(&state, admin).await;

    for name in ["   ".to_string(), "n".repeat(257)] {
        assert!(matches!(
            rename(&state, Some(member_auth(admin)), group_id, &name)
                .await
                .unwrap_err(),
            ServerError::BadRequest
        ));
    }
}

#[tokio::test]
async fn device_refuses_rename() {
    let (state, admin, _tempdir) = setup_admin_state().await;
    let group_id = seed_group(&state, admin).await;
    let node_id = state.get_node_id().to_string();
    update_config(&state, |config| {
        for node in &mut config.nodes {
            if node.node_id == node_id {
                node.kind = RealmNodeKind::User { owner: admin };
            }
        }
    })
    .await;

    let error = rename(&state, Some(member_auth(admin)), group_id, "Platform")
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::Conflict(reason) if reason.contains("through the realm")));
}

#[tokio::test]
async fn create_caps_name() {
    let (state, admin, _tempdir) = setup_admin_state().await;
    let error = new_group(&state, admin, &"n".repeat(257))
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::BadRequestReason(_)));
}

#[tokio::test]
async fn device_rejects_name() {
    // A user node validates the name itself, so an invalid one answers 400
    // instead of failing at the ingress it would otherwise forward to.
    let (state, admin, _tempdir) = setup_admin_state().await;
    let node_id = state.get_node_id().to_string();
    update_config(&state, |config| {
        for node in &mut config.nodes {
            if node.node_id == node_id {
                node.kind = RealmNodeKind::User { owner: admin };
            }
        }
    })
    .await;

    for name in ["   ".to_string(), "n".repeat(257)] {
        let error = new_group(&state, admin, &name).await.unwrap_err();
        assert!(matches!(error, ServerError::BadRequestReason(_)));
    }
}

#[tokio::test]
async fn device_forwards_create() {
    // A user-kind node never creates locally: without a reachable ingress
    // the create fails as unavailable and no group is stored.
    let (state, admin, _tempdir) = setup_admin_state().await;
    let node_id = state.get_node_id().to_string();
    update_config(&state, |config| {
        for node in &mut config.nodes {
            if node.node_id == node_id {
                node.kind = RealmNodeKind::User { owner: admin };
            }
        }
    })
    .await;

    let error = new_group(&state, admin, "device").await.unwrap_err();
    assert!(matches!(error, ServerError::ServiceUnavailable));
    let groups = drive(ListGroupOperation::new(), &state.get_ctx())
        .await
        .unwrap();
    assert!(groups.is_empty());
}

#[tokio::test]
async fn create_needs_token() {
    // Forwarding carries the caller's own token; without one the device
    // must refuse rather than act under its node identity.
    let (state, admin, _tempdir) = setup_admin_state().await;
    let node_id = state.get_node_id().to_string();
    update_config(&state, |config| {
        for node in &mut config.nodes {
            if node.node_id == node_id {
                node.kind = RealmNodeKind::User { owner: admin };
            }
        }
    })
    .await;

    let error = create_group(
        State(state.clone()),
        Extension(Some(member_auth(admin))),
        Extension(None),
        Json(CreateGroupRequest {
            name: "device".to_string(),
        }),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ServerError::Unauthorized));
}

#[tokio::test]
async fn policy_drops_exemption() {
    // A realm deny policy on the admin path must grade the caller as an
    // ordinary user, without breaking capped self-service creation.
    let (state, admin, _tempdir) = setup_admin_state().await;
    let realm_id = state.get_realm_id();
    update_config(&state, |config| {
        config.quota.groups_per_user = Some(1);
    })
    .await;

    new_group(&state, admin, "first").await.unwrap();
    new_group(&state, admin, "second").await.unwrap();

    let denied = format!("/{realm_id}/admin/groups");
    update_config(&state, |config| {
        config
            .request_policies
            .push(aruna_core::request_policy::RequestPolicy {
                policy_id: Ulid::generate(),
                name: "deny-group-admin".to_string(),
                kind: aruna_core::request_policy::PolicyKind::Deny,
                when: None,
                expression: format!("path == '{denied}'"),
                enabled: true,
            });
    })
    .await;

    let error = new_group(&state, admin, "third").await.unwrap_err();
    assert!(matches!(error, ServerError::Conflict(_)));

    let member = UserId::local(Ulid::generate(), realm_id);
    new_group(&state, member, "mine").await.unwrap();
}

/// Anonymous callers get 401, foreign-realm tokens 403: neither may
/// enumerate the local group or usage directory.
#[tokio::test]
async fn directory_requires_realm() {
    let (state, _tempdir) = setup_state().await;
    let group_id = Ulid::generate().to_string();

    assert!(matches!(
        list_groups(
            State(state.clone()),
            Extension(None),
            Query(ListGroupsQuery::default())
        )
        .await,
        Err(ServerError::Unauthorized)
    ));
    assert!(matches!(
        list_groups(
            State(state.clone()),
            Extension(Some(foreign_auth())),
            Query(ListGroupsQuery::default())
        )
        .await,
        Err(ServerError::Forbidden)
    ));

    for auth in [None, Some(foreign_auth())] {
        let expected = if auth.is_none() {
            ServerError::Unauthorized
        } else {
            ServerError::Forbidden
        };
        for result in [
            get_group(
                State(state.clone()),
                Extension(auth.clone()),
                Path(group_id.clone()),
            )
            .await
            .map(|_| ()),
            get_group_usage(
                State(state.clone()),
                Extension(auth.clone()),
                Path(group_id.clone()),
            )
            .await
            .map(|_| ()),
            list_group_members(
                State(state.clone()),
                Extension(auth.clone()),
                Path(group_id.clone()),
            )
            .await
            .map(|_| ()),
        ] {
            assert_eq!(
                result.unwrap_err().to_string(),
                expected.to_string(),
                "group route leaked to {auth:?}"
            );
        }
    }
}

/// Membership is checked before any group metadata aggregate is read.
#[tokio::test]
async fn nonmember_usage_denied() {
    let (state, _tempdir) = setup_state().await;
    let owner = UserId::local(Ulid::generate(), state.get_realm_id());
    let group_id = seed_group(&state, owner).await;
    let outsider = member_auth(UserId::local(Ulid::generate(), state.get_realm_id()));

    assert!(matches!(
        get_group_usage(
            State(state),
            Extension(Some(outsider)),
            Path(group_id.to_string()),
        )
        .await,
        Err(ServerError::Forbidden)
    ));
}

#[tokio::test]
async fn joins_member_names() {
    let (state, _tempdir) = setup_state().await;
    let owner = UserId::local(Ulid::generate(), state.get_realm_id());
    let group_id = seed_group(&state, owner).await;
    store_user(&state, owner, "Owner").await;

    let (status, Json(body)) = list_group_members(
        State(state.clone()),
        Extension(Some(member_auth(owner))),
        Path(group_id.to_string()),
    )
    .await
    .unwrap();

    assert_eq!(status, axum::http::StatusCode::OK);
    let member = body
        .members
        .iter()
        .find(|member| member.user_id == owner.to_string())
        .unwrap();
    assert_eq!(member.name.as_deref(), Some("Owner"));
}

#[tokio::test]
async fn unresolved_member_none() {
    // A member without a stored user record still lists, with name None.
    let (state, _tempdir) = setup_state().await;
    let owner = UserId::local(Ulid::generate(), state.get_realm_id());
    let group_id = seed_group(&state, owner).await;

    let (status, Json(body)) = list_group_members(
        State(state),
        Extension(Some(member_auth(owner))),
        Path(group_id.to_string()),
    )
    .await
    .unwrap();

    assert_eq!(status, axum::http::StatusCode::OK);
    let member = body
        .members
        .iter()
        .find(|member| member.user_id == owner.to_string())
        .unwrap();
    assert_eq!(member.name, None);
}

async fn seed_bucket(state: &ServerState, bucket: &str, group_id: Ulid) {
    let info = BucketInfo {
        group_id,
        created_at: SystemTime::now(),
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
    };
    store_bytes(
        state,
        S3_BUCKET_KEYSPACE,
        bucket.as_bytes().to_vec(),
        info.to_bytes().unwrap(),
    )
    .await;
}

async fn seed_object(state: &ServerState, bucket: &str, key: &str, owner: UserId, tag: u8) {
    let version_id = Ulid::generate();
    let created_at = UNIX_EPOCH + Duration::from_secs(5);
    let hash = [tag; 32];
    store_bytes(
        state,
        BLOB_HEAD_KEYSPACE,
        BlobHeadKey::new(bucket, key).to_bytes().unwrap(),
        CurrentVersionPointer::new(version_id).to_bytes().unwrap(),
    )
    .await;
    store_bytes(
        state,
        BLOB_VERSIONS_KEYSPACE,
        VersionKey::new(bucket, key, version_id).to_bytes().unwrap(),
        BlobVersion::materialized(hash, BackendRef::node_default(), created_at, owner, None)
            .to_bytes()
            .unwrap(),
    )
    .await;
    store_bytes(
        state,
        BLOB_LOCATIONS_KEYSPACE,
        BlobLocationKey::new(hash, BackendRef::node_default()).to_bytes(),
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "objects".to_string(),
            backend_path: format!("path/{key}"),
            ulid: Ulid::generate(),
            compressed: false,
            encrypted: false,
            created_by: owner,
            created_at,
            staging: false,
            partial: false,
            blob_size: 42,
            hashes: HashMap::new(),
        }
        .to_bytes()
        .unwrap(),
    )
    .await;
}

fn browse(prefix: Option<String>) -> DataPathsQuery {
    DataPathsQuery {
        prefix,
        delimiter: Some("/".to_string()),
        continuation_token: None,
        limit: None,
    }
}

#[tokio::test]
async fn folds_group_buckets() {
    let (state, _tempdir) = setup_state().await;
    let owner = UserId::local(Ulid::generate(), state.get_realm_id());
    let group_id = seed_group(&state, owner).await;
    seed_bucket(&state, "alpha", group_id).await;
    seed_bucket(&state, "beta", group_id).await;
    seed_bucket(&state, "foreign", Ulid::generate()).await;

    let (status, Json(body)) = list_data_paths(
        State(state.clone()),
        Extension(Some(member_auth(owner))),
        Path(group_id.to_string()),
        Query(DataPathsQuery::default()),
    )
    .await
    .unwrap();

    assert_eq!(status, StatusCode::OK);
    let realm_id = state.get_realm_id();
    let node_id = state.get_node_id();
    let mut paths: Vec<_> = body
        .entries
        .iter()
        .map(|entry| {
            assert_eq!(entry.kind, DataPathKind::Folder);
            entry.permission_path.clone()
        })
        .collect();
    paths.sort();
    assert_eq!(
        paths,
        vec![
            bucket_permission_path(realm_id, group_id, node_id, "alpha"),
            bucket_permission_path(realm_id, group_id, node_id, "beta"),
        ]
    );
}

#[tokio::test]
async fn bucket_folds_objects() {
    let (state, _tempdir) = setup_state().await;
    let owner = UserId::local(Ulid::generate(), state.get_realm_id());
    let group_id = seed_group(&state, owner).await;
    seed_bucket(&state, "data", group_id).await;
    for (index, key) in ["a.txt", "dir/1", "dir/2", "z.txt"].iter().enumerate() {
        seed_object(&state, "data", key, owner, index as u8 + 1).await;
    }
    let realm_id = state.get_realm_id();
    let node_id = state.get_node_id();
    let prefix = bucket_permission_path(realm_id, group_id, node_id, "data") + "/";

    let (status, Json(body)) = list_data_paths(
        State(state.clone()),
        Extension(Some(member_auth(owner))),
        Path(group_id.to_string()),
        Query(browse(Some(prefix))),
    )
    .await
    .unwrap();

    assert_eq!(status, StatusCode::OK);
    let folders: Vec<_> = body
        .entries
        .iter()
        .filter(|entry| entry.kind == DataPathKind::Folder)
        .map(|entry| entry.permission_path.clone())
        .collect();
    let mut objects: Vec<_> = body
        .entries
        .iter()
        .filter(|entry| entry.kind == DataPathKind::Object)
        .map(|entry| entry.permission_path.clone())
        .collect();
    objects.sort();

    assert_eq!(
        folders,
        vec![object_permission_path(
            realm_id, group_id, node_id, "data", "dir/"
        )]
    );
    assert_eq!(
        objects,
        vec![
            object_permission_path(realm_id, group_id, node_id, "data", "a.txt"),
            object_permission_path(realm_id, group_id, node_id, "data", "z.txt"),
        ]
    );
    assert!(body.continuation_token.is_none());
}

#[tokio::test]
async fn round_trips_bucket() {
    // A returned bucket folder path must list the bucket's children verbatim.
    let (state, _tempdir) = setup_state().await;
    let owner = UserId::local(Ulid::generate(), state.get_realm_id());
    let group_id = seed_group(&state, owner).await;
    seed_bucket(&state, "data", group_id).await;
    for (index, key) in ["a.txt", "dir/1"].iter().enumerate() {
        seed_object(&state, "data", key, owner, index as u8 + 1).await;
    }
    let realm_id = state.get_realm_id();
    let node_id = state.get_node_id();

    let (_status, Json(listing)) = list_data_paths(
        State(state.clone()),
        Extension(Some(member_auth(owner))),
        Path(group_id.to_string()),
        Query(DataPathsQuery::default()),
    )
    .await
    .unwrap();
    let bucket_path = listing.entries[0].permission_path.clone();
    assert_eq!(
        bucket_path,
        bucket_permission_path(realm_id, group_id, node_id, "data")
    );

    let (_status, Json(body)) = list_data_paths(
        State(state.clone()),
        Extension(Some(member_auth(owner))),
        Path(group_id.to_string()),
        Query(browse(Some(bucket_path.clone()))),
    )
    .await
    .unwrap();

    assert!(
        body.entries
            .iter()
            .all(|entry| entry.permission_path != bucket_path)
    );
    let paths: Vec<_> = body
        .entries
        .iter()
        .map(|entry| entry.permission_path.clone())
        .collect();
    assert!(paths.contains(&object_permission_path(
        realm_id, group_id, node_id, "data", "a.txt"
    )));
    assert!(paths.contains(&object_permission_path(
        realm_id, group_id, node_id, "data", "dir/"
    )));
}

#[tokio::test]
async fn paginates_object_pages() {
    let (state, _tempdir) = setup_state().await;
    let owner = UserId::local(Ulid::generate(), state.get_realm_id());
    let group_id = seed_group(&state, owner).await;
    seed_bucket(&state, "data", group_id).await;
    for (index, key) in ["a", "b", "c", "d"].iter().enumerate() {
        seed_object(&state, "data", key, owner, index as u8 + 1).await;
    }
    let realm_id = state.get_realm_id();
    let node_id = state.get_node_id();
    let prefix = bucket_permission_path(realm_id, group_id, node_id, "data") + "/";

    let mut token = None;
    let mut collected = Vec::new();
    let mut pages = 0;
    loop {
        let (_status, Json(body)) = list_data_paths(
            State(state.clone()),
            Extension(Some(member_auth(owner))),
            Path(group_id.to_string()),
            Query(DataPathsQuery {
                prefix: Some(prefix.clone()),
                delimiter: Some("/".to_string()),
                continuation_token: token.take(),
                limit: Some(2),
            }),
        )
        .await
        .unwrap();
        collected.extend(body.entries.into_iter().map(|entry| entry.permission_path));
        pages += 1;
        assert!(pages <= 5);
        token = body.continuation_token;
        if token.is_none() {
            break;
        }
    }

    collected.sort();
    let expected: Vec<_> = ["a", "b", "c", "d"]
        .iter()
        .map(|key| object_permission_path(realm_id, group_id, node_id, "data", key))
        .collect();
    assert_eq!(collected, expected);
    assert!(pages >= 2);
}

#[tokio::test]
async fn path_matches_helper() {
    let (state, _tempdir) = setup_state().await;
    let owner = UserId::local(Ulid::generate(), state.get_realm_id());
    let group_id = seed_group(&state, owner).await;
    seed_bucket(&state, "data", group_id).await;
    seed_object(&state, "data", "reports/q1.csv", owner, 9).await;
    let realm_id = state.get_realm_id();
    let node_id = state.get_node_id();
    let prefix = bucket_permission_path(realm_id, group_id, node_id, "data") + "/reports/";

    let (_status, Json(body)) = list_data_paths(
        State(state.clone()),
        Extension(Some(member_auth(owner))),
        Path(group_id.to_string()),
        Query(browse(Some(prefix))),
    )
    .await
    .unwrap();

    let object = body
        .entries
        .iter()
        .find(|entry| entry.kind == DataPathKind::Object)
        .unwrap();
    assert_eq!(
        object.permission_path,
        object_permission_path(realm_id, group_id, node_id, "data", "reports/q1.csv")
    );
}

#[tokio::test]
async fn hides_foreign_bucket() {
    // A crafted path naming another group's bucket must not leak its keys.
    let (state, _tempdir) = setup_state().await;
    let owner = UserId::local(Ulid::generate(), state.get_realm_id());
    let group_id = seed_group(&state, owner).await;
    seed_bucket(&state, "secret", Ulid::generate()).await;
    seed_object(&state, "secret", "k", owner, 1).await;
    let realm_id = state.get_realm_id();
    let node_id = state.get_node_id();
    let prefix = bucket_permission_path(realm_id, group_id, node_id, "secret") + "/";

    let (_status, Json(body)) = list_data_paths(
        State(state.clone()),
        Extension(Some(member_auth(owner))),
        Path(group_id.to_string()),
        Query(browse(Some(prefix))),
    )
    .await
    .unwrap();

    assert!(body.entries.is_empty());
}

#[tokio::test]
async fn non_member_forbidden() {
    let (state, _tempdir) = setup_state().await;
    let owner = UserId::local(Ulid::generate(), state.get_realm_id());
    let group_id = seed_group(&state, owner).await;
    let outsider = member_auth(UserId::local(Ulid::generate(), state.get_realm_id()));

    let result = list_data_paths(
        State(state),
        Extension(Some(outsider)),
        Path(group_id.to_string()),
        Query(DataPathsQuery::default()),
    )
    .await;
    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn empty_role_forbidden() {
    // Membership alone is not enough: a role granting no READ is forbidden.
    let (state, _tempdir) = setup_state().await;
    let realm_id = state.get_realm_id();
    let owner = UserId::local(Ulid::generate(), realm_id);
    let group_id = seed_group(&state, owner).await;
    let limited = UserId::local(Ulid::generate(), realm_id);

    let mut auth_doc = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    let role_id = Ulid::generate();
    auth_doc.roles.insert(
        role_id,
        Role {
            role_id,
            name: "empty".to_string(),
            permissions: HashMap::new(),
            assigned_users: HashSet::from([limited]),
        },
    );
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: owner,
        realm_id,
    };
    store_bytes(
        &state,
        AUTH_KEYSPACE,
        group_id.to_bytes().to_vec(),
        auth_doc.to_bytes(&actor).unwrap(),
    )
    .await;

    let result = list_data_paths(
        State(state),
        Extension(Some(member_auth(limited))),
        Path(group_id.to_string()),
        Query(DataPathsQuery::default()),
    )
    .await;
    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn rejects_anonymous_caller() {
    let (state, _tempdir) = setup_state().await;
    let result = list_data_paths(
        State(state),
        Extension(None),
        Path(Ulid::generate().to_string()),
        Query(DataPathsQuery::default()),
    )
    .await;
    assert!(matches!(result, Err(ServerError::Unauthorized)));
}

#[tokio::test]
async fn policy_denies_role() {
    // A group deny policy on the admin path blocks role creation with 403
    // and emits no role, even for the group owner.
    let (state, _tempdir) = setup_state().await;
    let realm_id = state.get_realm_id();
    let owner = UserId::local(Ulid::generate(), realm_id);
    store_user(&state, owner, "Owner").await;
    let group_id = Ulid::generate();

    let mut auth_doc = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    auth_doc.policies = vec![aruna_core::request_policy::RequestPolicy {
        policy_id: Ulid::generate(),
        name: "no-writes".to_string(),
        kind: aruna_core::request_policy::PolicyKind::Deny,
        when: None,
        expression: "permission == 'write'".to_string(),
        enabled: true,
    }];
    let group = Group {
        display_name: "Test".to_string(),
        group_id,
        realm_id,
        roles: auth_doc.roles.keys().copied().collect(),
        owner,
    };
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: owner,
        realm_id,
    };
    store_bytes(
        &state,
        GROUP_KEYSPACE,
        group_id.to_bytes().to_vec(),
        group.to_bytes(&actor).unwrap(),
    )
    .await;
    store_bytes(
        &state,
        AUTH_KEYSPACE,
        group_id.to_bytes().to_vec(),
        auth_doc.to_bytes(&actor).unwrap(),
    )
    .await;

    let result = super::create_group_role(
        State(state.clone()),
        Extension(Some(member_auth(owner))),
        Path(group_id.to_string()),
        Json(super::CreateRoleRequest {
            name: "readers".to_string(),
            permissions: HashMap::from([(
                format!("/{realm_id}/g/{group_id}/data/**"),
                "read".to_string(),
            )]),
            assigned_users: Vec::new(),
            public: false,
        }),
    )
    .await;
    assert!(matches!(result, Err(ServerError::Forbidden)));

    let value = match state
        .get_ctx()
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: AUTH_KEYSPACE.to_string(),
            key: ByteView::from(group_id.to_bytes().to_vec()),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value.unwrap(),
        other => panic!("unexpected read result: {other:?}"),
    };
    let stored = GroupAuthorizationDocument::from_bytes(&value).unwrap();
    assert_eq!(stored.roles.len(), auth_doc.roles.len());
}
