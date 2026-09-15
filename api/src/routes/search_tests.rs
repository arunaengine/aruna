use super::*;
use crate::error::ServerError;
use crate::metadata::{
    CreateMetadataRequest, CreateScaffoldRequest, MetadataQueryMode, ReplaceRoCrateRequest,
};
use crate::routes::metadata::documents::create_metadata_document;
use crate::routes::metadata::rocrate::replace_metadata_rocrate;
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE, USER_KEYSPACE,
};
use aruna_core::request_policy::{PolicyKind, RequestPolicy};
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_core::structs::identity::user::User;
use aruna_operations::driver::DriverContext;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::metadata::materialization_queue::process_materialization_batch;
use aruna_operations::metadata::projector::{drain_projection_queue, replay_event_log};
use aruna_storage::storage;
use aruna_tasks::TaskHandle;
use axum::extract::Path;
use byteview::ByteView;
use ed25519_dalek::SigningKey;
use std::time::SystemTime;
use tempfile::TempDir;
use ulid::Ulid;

struct Fixture {
    _storage_dir: TempDir,
    _metadata_dir: TempDir,
    state: Arc<ServerState>,
    auth: AuthContext,
    actor: Actor,
    realm_id: RealmId,
    groups: [Ulid; 2],
    users: [UserId; 2],
}

fn realm_id(seed: u8) -> RealmId {
    RealmId::from_bytes(
        SigningKey::from_bytes(&[seed; 32])
            .verifying_key()
            .to_bytes(),
    )
}

async fn write_bytes(state: &ServerState, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    let event = state
        .get_ctx()
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(value),
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn seed_group(
    state: &ServerState,
    actor: &Actor,
    group_id: Ulid,
    name: &str,
    policies: Vec<RequestPolicy>,
) {
    let realm = actor.realm_id;
    let mut auth_doc =
        GroupAuthorizationDocument::default_group_doc(actor.user_id, realm, group_id);
    auth_doc.policies = policies;
    let group = Group {
        display_name: name.to_string(),
        group_id,
        realm_id: realm,
        roles: auth_doc.roles.keys().copied().collect(),
        owner: actor.user_id,
    };
    write_bytes(
        state,
        GROUP_KEYSPACE,
        group_id.to_bytes().to_vec(),
        group.to_bytes(actor).unwrap(),
    )
    .await;
    write_bytes(
        state,
        AUTH_KEYSPACE,
        group_id.to_bytes().to_vec(),
        auth_doc.to_bytes(actor).unwrap(),
    )
    .await;
}

async fn seed_bucket(state: &ServerState, actor: &Actor, bucket: &str, group_id: Ulid) {
    write_bytes(
        state,
        S3_BUCKET_KEYSPACE,
        bucket.as_bytes().to_vec(),
        BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: actor.user_id,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        }
        .to_bytes()
        .unwrap(),
    )
    .await;
}

/// Rewrites the realm config with the given realm-scoped request policies,
/// keeping the placement and node entries the fan-out needs.
async fn seed_policies(state: &ServerState, actor: &Actor, policies: Vec<RequestPolicy>) {
    let realm = actor.realm_id;
    let mut config = RealmConfigDocument::default_for_realm(realm, Vec::new());
    config.seed_default_placement();
    config.ensure_node(actor.node_id, RealmNodeKind::Server);
    config.seed_job_control(actor.node_id, 0);
    config.request_policies = policies;
    write_bytes(
        state,
        REALM_CONFIG_KEYSPACE,
        realm.as_bytes().to_vec(),
        config.to_bytes(actor).unwrap(),
    )
    .await;
}

fn deny_policy(expression: &str) -> RequestPolicy {
    RequestPolicy {
        policy_id: Ulid::generate(),
        name: "hide-bucket".to_string(),
        kind: PolicyKind::Deny,
        when: None,
        expression: expression.to_string(),
        enabled: true,
    }
}

async fn seed_user(state: &ServerState, actor: &Actor, user_id: UserId, name: &str) {
    let user = User {
        user_id,
        name: name.to_string(),
        subject_ids: Vec::new(),
        alias_user_ids: Default::default(),
        attributes: Default::default(),
    };
    write_bytes(
        state,
        USER_KEYSPACE,
        user_id.to_storage_key(),
        user.to_bytes(actor).unwrap(),
    )
    .await;
}

async fn setup() -> Fixture {
    let storage_dir = tempfile::tempdir().unwrap();
    let metadata_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
    let realm = realm_id(5);
    let user_id = UserId::local(Ulid::from_bytes([200u8; 16]), realm);
    let actor = Actor {
        node_id,
        user_id,
        realm_id: realm,
    };
    let metadata_handle = MetadataHandle::new(
        metadata_dir.path(),
        node_id,
        storage_handle.clone(),
        None,
        None,
        None,
    )
    .unwrap();
    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: Some(metadata_handle),
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    });
    let state = Arc::new(
        ServerState::new(
            driver_ctx,
            realm,
            node_id,
            NodeCapabilities::user_node(realm).unwrap(),
            false,
            None,
            aruna_operations::jobs::runtime::JobsRuntime::new(),
        )
        .await,
    );

    // The fixture user holds every realm role so the user directory section
    // of a unified search is authorized.
    let mut realm_doc = RealmAuthorizationDocument::default_realm_doc(realm);
    for role in realm_doc.roles.values_mut() {
        role.assigned_users.insert(user_id);
    }
    write_bytes(
        &state,
        AUTH_KEYSPACE,
        realm.as_bytes().to_vec(),
        realm_doc.to_bytes(&actor).unwrap(),
    )
    .await;

    seed_policies(&state, &actor, Vec::new()).await;

    let groups = [Ulid::from_bytes([1u8; 16]), Ulid::from_bytes([2u8; 16])];
    seed_group(&state, &actor, groups[0], "alpha-team", Vec::new()).await;
    seed_group(&state, &actor, groups[1], "alpha-squad", Vec::new()).await;
    seed_bucket(&state, &actor, "alpha-bucket", groups[0]).await;

    let users = [
        UserId::local(Ulid::from_bytes([1u8; 16]), realm),
        UserId::local(Ulid::from_bytes([2u8; 16]), realm),
    ];
    seed_user(&state, &actor, users[0], "beta-anna").await;
    seed_user(&state, &actor, users[1], "beta-bob").await;

    Fixture {
        _storage_dir: storage_dir,
        _metadata_dir: metadata_dir,
        state,
        auth: AuthContext {
            user_id,
            realm_id: realm,
            path_restrictions: None,
            session: None,
        },
        actor,
        realm_id: realm,
        groups,
        users,
    }
}

async fn create_doc(fx: &Fixture, group_id: Ulid, path: &str, name: &str) -> String {
    let (_, Json(response)) = create_metadata_document(
        State(fx.state.clone()),
        Extension(Some(fx.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: group_id.to_string(),
            path: path.to_string(),
            name: name.to_string(),
            description: "desc".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            public: true,
        })),
    )
    .await
    .unwrap();
    response.summary.document_id
}

// Replaces the scaffolded crate with a root dataset that owns one file
// entity, so search sees two subjects under one document.
async fn attach_file(fx: &Fixture, document_id: &str, name: &str) {
    let rocrate = serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                "about": {"@id": format!("https://w3id.org/aruna/{document_id}")}
            },
            {
                "@id": format!("https://w3id.org/aruna/{document_id}"),
                "@type": "Dataset",
                "name": name,
                "description": "desc",
                "datePublished": "2026-01-01",
                "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"},
                "hasPart": [{"@id": "./data/reef.csv"}]
            },
            {
                "@id": "./data/reef.csv",
                "@type": "File",
                "name": format!("{name}-part")
            }
        ]
    });
    let _ = replace_metadata_rocrate(
        State(fx.state.clone()),
        Extension(Some(fx.auth.clone())),
        Extension(None),
        Path(document_id.to_string()),
        Json(ReplaceRoCrateRequest {
            rocrate,
            public: Some(true),
        }),
    )
    .await
    .unwrap();
}

async fn drain_projection(state: &ServerState) {
    let ctx = state.get_ctx();
    let drained = drain_projection_queue(ctx.as_ref()).await.unwrap();
    if drained.markers_examined == 0 {
        replay_event_log(ctx.as_ref()).await.unwrap();
    }
    process_materialization_batch(ctx.as_ref()).await.unwrap();
}

async fn flush_search(state: &ServerState) {
    let ctx = state.get_ctx();
    ctx.metadata_handle
        .as_ref()
        .unwrap()
        .flush_search_updates()
        .await
        .unwrap();
}

fn params(q: &str) -> SearchParams {
    SearchParams {
        q: q.to_string(),
        ..Default::default()
    }
}

async fn search(fx: &Fixture, params: SearchParams) -> ServerResult<SearchResponse> {
    unified_search(
        State(fx.state.clone()),
        Extension(Some(fx.auth.clone())),
        Extension(None),
        Query(params),
    )
    .await
    .map(|(_, Json(body))| body)
}

#[tokio::test]
async fn selects_types() {
    let fx = setup().await;
    let resp = search(
        &fx,
        SearchParams {
            types: Some("groups,users".to_string()),
            ..params("alpha")
        },
    )
    .await
    .unwrap();
    assert!(resp.documents.is_none());
    assert!(resp.buckets.is_none());
    assert!(resp.groups.is_some());
    assert!(resp.users.is_some());
}

async fn bucket_hits(fx: &Fixture, query: &str, limit: usize) -> Vec<String> {
    let (_, Json(section)) = bucket_search(
        State(fx.state.clone()),
        Extension(Some(fx.auth.clone())),
        Extension(None),
        Query(BucketSearchParams {
            q: query.to_string(),
            limit: Some(limit),
        }),
    )
    .await
    .unwrap();
    section.hits.into_iter().map(|hit| hit.bucket).collect()
}

#[tokio::test]
async fn policy_hides_bucket() {
    // A realm deny policy must hide the bucket from the dedicated route and
    // from the buckets section of the unified search alike.
    let fx = setup().await;
    seed_bucket(&fx.state, &fx.actor, "beta-bucket", fx.groups[1]).await;
    seed_policies(
        &fx.state,
        &fx.actor,
        vec![deny_policy("path.endsWith('/alpha-bucket')")],
    )
    .await;

    assert_eq!(bucket_hits(&fx, "bucket", 10).await, ["beta-bucket"]);

    let unified = search(
        &fx,
        SearchParams {
            types: Some("buckets".to_string()),
            ..params("bucket")
        },
    )
    .await
    .unwrap();
    let hits = unified.buckets.unwrap().hits;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].bucket, "beta-bucket");
}

#[tokio::test]
async fn group_hides_bucket() {
    // A group-scoped deny policy hides that group's bucket only.
    let fx = setup().await;
    seed_bucket(&fx.state, &fx.actor, "beta-bucket", fx.groups[1]).await;
    seed_group(
        &fx.state,
        &fx.actor,
        fx.groups[0],
        "alpha-team",
        vec![deny_policy("path.endsWith('/alpha-bucket')")],
    )
    .await;

    assert_eq!(bucket_hits(&fx, "bucket", 10).await, ["beta-bucket"]);
}

#[tokio::test]
async fn policy_fills_page() {
    // A hidden first match must not shorten a page that later matches can
    // still fill.
    let fx = setup().await;
    seed_bucket(&fx.state, &fx.actor, "beta-bucket", fx.groups[1]).await;
    seed_bucket(&fx.state, &fx.actor, "gamma-bucket", fx.groups[1]).await;
    seed_policies(
        &fx.state,
        &fx.actor,
        vec![deny_policy("path.endsWith('/alpha-bucket')")],
    )
    .await;

    assert_eq!(
        bucket_hits(&fx, "bucket", 2).await,
        ["beta-bucket", "gamma-bucket"]
    );
}

#[tokio::test]
async fn searches_buckets() {
    let fx = setup().await;
    let (_, Json(dedicated)) = bucket_search(
        State(fx.state.clone()),
        Extension(Some(fx.auth.clone())),
        Extension(None),
        Query(BucketSearchParams {
            q: "bucket".to_string(),
            limit: Some(10),
        }),
    )
    .await
    .unwrap();
    assert_eq!(dedicated.hits.len(), 1);
    assert_eq!(dedicated.hits[0].bucket, "alpha-bucket");
    assert!(dedicated.hits[0].arn.ends_with(":s3/alpha-bucket"));

    let unified = search(
        &fx,
        SearchParams {
            types: Some("buckets".to_string()),
            ..params("bucket")
        },
    )
    .await
    .unwrap();
    assert!(unified.documents.is_none());
    assert_eq!(unified.buckets.unwrap().hits.len(), 1);
    assert!(unified.groups.is_none());
    assert!(unified.users.is_none());
}

#[tokio::test]
async fn filters_group_hits() {
    // A same-realm caller who is not a group member sees no group hits,
    // while a member still sees both matching groups.
    let fx = setup().await;
    let stranger = AuthContext {
        user_id: UserId::local(Ulid::from_bytes([77u8; 16]), fx.realm_id),
        realm_id: fx.realm_id,
        path_restrictions: None,
        session: None,
    };
    let (_, Json(resp)) = unified_search(
        State(fx.state.clone()),
        Extension(Some(stranger)),
        Extension(None),
        Query(SearchParams {
            types: Some("groups".to_string()),
            ..params("alpha")
        }),
    )
    .await
    .unwrap();
    assert!(resp.groups.unwrap().hits.is_empty());

    let member = search(
        &fx,
        SearchParams {
            types: Some("groups".to_string()),
            ..params("alpha")
        },
    )
    .await
    .unwrap();
    assert_eq!(member.groups.unwrap().hits.len(), 2);
}

#[tokio::test]
async fn rejects_unknown_type() {
    let fx = setup().await;
    let result = search(
        &fx,
        SearchParams {
            types: Some("groups,bogus".to_string()),
            ..params("alpha")
        },
    )
    .await;
    assert!(matches!(result, Err(ServerError::BadRequest)));
}

#[tokio::test]
async fn rejects_short_query() {
    let fx = setup().await;
    let result = search(&fx, params("a")).await;
    assert!(matches!(result, Err(ServerError::BadRequest)));
}

#[tokio::test]
async fn rejects_cursor_multi() {
    // A cursor is only valid when exactly one type is requested.
    let fx = setup().await;
    let result = search(
        &fx,
        SearchParams {
            cursor: Some("token".to_string()),
            ..params("alpha")
        },
    )
    .await;
    assert!(matches!(result, Err(ServerError::BadRequest)));
}

#[tokio::test]
async fn rejects_malformed_cursor() {
    // A garbage single-type cursor is caller input: 400, never a 500.
    let fx = setup().await;
    for section in ["groups", "users"] {
        let result = search(
            &fx,
            SearchParams {
                types: Some(section.to_string()),
                cursor: Some("garbage".to_string()),
                ..params("alpha")
            },
        )
        .await;
        assert!(
            matches!(result, Err(ServerError::BadRequest)),
            "{section} cursor should be rejected"
        );
    }
    let result = search(
        &fx,
        SearchParams {
            types: Some("buckets".to_string()),
            cursor: Some("unsupported".to_string()),
            ..params("alpha")
        },
    )
    .await;
    assert!(matches!(result, Err(ServerError::BadRequest)));
}

#[tokio::test]
async fn pages_groups() {
    let fx = setup().await;
    let first = search(
        &fx,
        SearchParams {
            types: Some("groups".to_string()),
            limit: Some(1),
            ..params("alpha")
        },
    )
    .await
    .unwrap()
    .groups
    .unwrap();
    assert_eq!(first.hits.len(), 1);
    assert_eq!(first.hits[0].group_id, fx.groups[0].to_string());
    let cursor = first.next_cursor.clone().unwrap();

    let second = search(
        &fx,
        SearchParams {
            types: Some("groups".to_string()),
            limit: Some(1),
            cursor: Some(cursor),
            ..params("alpha")
        },
    )
    .await
    .unwrap()
    .groups
    .unwrap();
    assert_eq!(second.hits.len(), 1);
    assert_eq!(second.hits[0].group_id, fx.groups[1].to_string());
    assert!(second.next_cursor.is_none());
}

#[tokio::test]
async fn skips_hidden_group() {
    // A hidden first match at limit 1 must not yield an empty page whose
    // cursor exposes the hidden group's id.
    let fx = setup().await;
    let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
    let hidden_owner = UserId::local(Ulid::from_bytes([240u8; 16]), fx.realm_id);
    let viewer = UserId::local(Ulid::from_bytes([241u8; 16]), fx.realm_id);
    let hidden_actor = Actor {
        node_id,
        user_id: hidden_owner,
        realm_id: fx.realm_id,
    };
    let viewer_actor = Actor {
        node_id,
        user_id: viewer,
        realm_id: fx.realm_id,
    };
    seed_group(
        &fx.state,
        &hidden_actor,
        fx.groups[0],
        "alpha-hidden",
        Vec::new(),
    )
    .await;
    seed_group(
        &fx.state,
        &viewer_actor,
        fx.groups[1],
        "alpha-visible",
        Vec::new(),
    )
    .await;

    let viewer_auth = AuthContext {
        user_id: viewer,
        realm_id: fx.realm_id,
        path_restrictions: None,
        session: None,
    };
    let section = run_groups(&fx.state, &viewer_auth, true, "alpha", 1, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(section.hits.len(), 1);
    assert_eq!(section.hits[0].group_id, fx.groups[1].to_string());
    assert_ne!(
        section.next_cursor.as_deref(),
        Some(fx.groups[0].to_string().as_str())
    );
}

#[tokio::test]
async fn caps_report_truncation() {
    // More hidden matches than the scan cap must report truncation instead of
    // a false completion with an empty page and no cursor.
    let fx = setup().await;
    let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
    let hidden_owner = UserId::local(Ulid::from_bytes([230u8; 16]), fx.realm_id);
    let viewer = UserId::local(Ulid::from_bytes([231u8; 16]), fx.realm_id);
    let hidden_actor = Actor {
        node_id,
        user_id: hidden_owner,
        realm_id: fx.realm_id,
    };
    for index in 0..=MAX_GROUP_ROUNDS as u8 {
        let mut bytes = [16u8; 16];
        bytes[15] = index;
        seed_group(
            &fx.state,
            &hidden_actor,
            Ulid::from_bytes(bytes),
            &format!("alpha-hidden-{index}"),
            Vec::new(),
        )
        .await;
    }

    let viewer_auth = AuthContext {
        user_id: viewer,
        realm_id: fx.realm_id,
        path_restrictions: None,
        session: None,
    };
    let section = run_groups(&fx.state, &viewer_auth, true, "alpha", 1, None)
        .await
        .unwrap()
        .unwrap();
    assert!(section.hits.is_empty());
    assert!(section.next_cursor.is_none());
    assert!(section.truncated);
}

#[tokio::test]
async fn pages_users() {
    let fx = setup().await;
    let first = search(
        &fx,
        SearchParams {
            types: Some("users".to_string()),
            limit: Some(1),
            ..params("beta")
        },
    )
    .await
    .unwrap()
    .users
    .unwrap();
    assert_eq!(first.hits.len(), 1);
    assert_eq!(first.hits[0].user_id, fx.users[0].to_string());
    let cursor = first.next_cursor.clone().unwrap();

    let second = search(
        &fx,
        SearchParams {
            types: Some("users".to_string()),
            limit: Some(1),
            cursor: Some(cursor),
            ..params("beta")
        },
    )
    .await
    .unwrap()
    .users
    .unwrap();
    assert_eq!(second.hits.len(), 1);
    assert_eq!(second.hits[0].user_id, fx.users[1].to_string());
    assert!(second.next_cursor.is_none());
}

#[tokio::test]
async fn documents_include_types() {
    // Root and file entity match as separate subjects of one document, so
    // only subject_types tells a file hit apart from a dataset hit.
    let fx = setup().await;
    let document_id = create_doc(&fx, fx.groups[0], "datasets/reef", "epsilon-reef").await;
    // The crate can only be replaced once its graph is materialized.
    drain_projection(&fx.state).await;
    attach_file(&fx, &document_id, "epsilon-reef").await;
    drain_projection(&fx.state).await;
    flush_search(&fx.state).await;

    let documents = search(
        &fx,
        SearchParams {
            types: Some("documents".to_string()),
            mode: Some(MetadataQueryMode::Local),
            ..params("epsilon")
        },
    )
    .await
    .unwrap()
    .documents
    .unwrap();
    let root = documents
        .hits
        .iter()
        .find(|hit| hit.title == "epsilon-reef")
        .expect("root dataset hit");
    assert_eq!(
        root.subject_types,
        vec!["http://schema.org/Dataset".to_string()]
    );
    let file = documents
        .hits
        .iter()
        .find(|hit| hit.subject_iri.ends_with("reef.csv"))
        .expect("file entity hit");
    assert_eq!(
        file.subject_types,
        vec!["http://schema.org/MediaObject".to_string()]
    );
    assert_eq!(file.document_id, document_id);
}

#[tokio::test]
async fn filters_documents_group() {
    // group_id passes through to metadata search and constrains the hits.
    let fx = setup().await;
    create_doc(&fx, fx.groups[0], "datasets/one", "gamma-one").await;
    create_doc(&fx, fx.groups[1], "datasets/two", "gamma-two").await;
    drain_projection(&fx.state).await;
    flush_search(&fx.state).await;

    let documents = search(
        &fx,
        SearchParams {
            types: Some("documents".to_string()),
            group_id: Some(fx.groups[0].to_string()),
            mode: Some(MetadataQueryMode::Local),
            ..params("gamma")
        },
    )
    .await
    .unwrap()
    .documents
    .unwrap();
    assert!(!documents.hits.is_empty());
    assert!(
        documents
            .hits
            .iter()
            .all(|hit| hit.group_id == fx.groups[0].to_string())
    );
}

#[tokio::test]
async fn requires_auth() {
    let fx = setup().await;
    let unauthenticated = unified_search(
        State(fx.state.clone()),
        Extension(None),
        Extension(None),
        Query(params("alpha")),
    )
    .await;
    assert!(matches!(unauthenticated, Err(ServerError::Unauthorized)));

    let foreign = AuthContext {
        user_id: UserId::local(Ulid::from_bytes([9u8; 16]), realm_id(9)),
        realm_id: realm_id(9),
        path_restrictions: None,
        session: None,
    };
    assert_ne!(foreign.realm_id, fx.realm_id);
    let wrong_realm = unified_search(
        State(fx.state.clone()),
        Extension(Some(foreign)),
        Extension(None),
        Query(params("alpha")),
    )
    .await;
    assert!(matches!(wrong_realm, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn search_requires_auth() {
    let fx = setup().await;
    let result = object_search(
        State(fx.state.clone()),
        Extension(None),
        Extension(None),
        Query(ObjectParams {
            q: "reads".to_string(),
            mode: Some(ObjectMode::Local),
            ..Default::default()
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::Unauthorized)));
}

#[test]
fn search_maps_partiality() {
    let healthy = iroh::SecretKey::from_bytes(&[21u8; 32]).public();
    let failed = iroh::SecretKey::from_bytes(&[22u8; 32]).public();
    let result = ObjectExecution {
        hits: vec![aruna_operations::s3::object::search::ObjectInventoryHit {
            node_id: healthy,
            group_id: Ulid::from_bytes([23u8; 16]),
            bucket: "data".to_string(),
            key: "reads/a.fastq".to_string(),
            content_w3id: Some(format!("https://w3id.org/aruna/data/{}", "01".repeat(32))),
            checksum: None,
            size: Some(42),
            updated_at: Some(SystemTime::UNIX_EPOCH),
        }],
        next_cursor: Some("opaque".to_string()),
        as_of: SystemTime::UNIX_EPOCH,
        partitions: vec![aruna_operations::metadata::api::ObjectPartitionCoverage {
            node_id: healthy,
            observed_at: SystemTime::UNIX_EPOCH,
            truncated: true,
        }],
        fanout_stats: aruna_operations::metadata::api::MetadataFanoutStats {
            nodes_queried: 2,
            nodes_failed: 1,
            failed_partitions: vec![failed],
            discovery_failed: false,
        },
        omitted_partitions: 0,
        complete: false,
    };

    let response = map_search_response(result, ObjectMode::DistributedBestEffort);
    assert_eq!(response.hits.len(), 1);
    assert_eq!(response.coverage.scope, ObjectScope::Realm);
    assert!(!response.coverage.complete);
    assert!(response.coverage.truncated);
    assert_eq!(response.coverage.nodes_failed, 1);
    assert_eq!(
        response.coverage.failed_partitions,
        vec![failed.to_string()]
    );
    assert_eq!(response.coverage.index_freshness.source, "live_heads");
    let encoded = serde_json::to_string(&response).unwrap();
    assert!(!encoded.contains("\"total"));
}

#[test]
fn passes_documents_truncated() {
    // The depth-cap truncation signal must survive the unified mapping.
    let result = MetadataSearchExecution {
        hits: Vec::new(),
        next_cursor: None,
        truncated: true,
        fanout_stats: aruna_operations::metadata::api::MetadataFanoutStats {
            nodes_queried: 1,
            nodes_failed: 0,
            failed_partitions: Vec::new(),
            discovery_failed: false,
        },
    };
    let section = map_documents_section(result);
    assert!(section.truncated);
    assert!(section.next_cursor.is_none());
}

#[tokio::test]
async fn empty_shape() {
    let fx = setup().await;
    let resp = search(
        &fx,
        SearchParams {
            mode: Some(MetadataQueryMode::Local),
            ..params("nomatchquery")
        },
    )
    .await
    .unwrap();
    let documents = resp.documents.unwrap();
    assert!(documents.hits.is_empty());
    assert!(documents.next_cursor.is_none());
    assert!(!documents.truncated);
    let buckets = resp.buckets.unwrap();
    assert!(buckets.hits.is_empty());
    let groups = resp.groups.unwrap();
    assert!(groups.hits.is_empty());
    assert!(groups.next_cursor.is_none());
    let users = resp.users.unwrap();
    assert!(users.hits.is_empty());
    assert!(users.next_cursor.is_none());
}
