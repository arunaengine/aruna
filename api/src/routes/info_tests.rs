use super::{
    GroupCapOverride, InterfaceServicesStatus, InterfaceStatus, NodeCapabilityKind, NodeKindInfo,
    PeerContacts, RealmBinding, RealmBindingScope, RealmConnectionStatus, RealmPlacementOverride,
    RealmPlacementRequest, RealmPlacementStrategy, RealmQuotaConfig, ServiceStatus, UsageResponse,
    get_info, get_realm_info, get_realm_placement, get_usage, map_handle_error,
    map_placement_error, map_quota_error, map_realm_nodes, mutate_realm_placement, presence_nodes,
    set_realm_quota,
};
use crate::error::ServerError;
use crate::openapi::ApiDoc;
use crate::server_state::ServerState;
use crate::tests::routes::{test_context, test_state, test_storage};
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keys::generate_signing_key;
use aruna_core::keyspaces::GROUP_KEYSPACE;
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities};
use aruna_core::structs::placement::placement_record::{DocumentClass, PlacementScope};
use aruna_core::structs::identity::group::Group;
use aruna_core::structs::identity::realm::{QuotaConfig, RealmId};
use aruna_core::structs::storage::usage::UsageCounters;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::placement::allocate_handle::{
    HandleAllocationError, allocate_placement_binding,
};
use aruna_operations::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_operations::realm::get_nodes::RealmPresence;
use aruna_operations::realm::mutate_placement::MutatePlacementError;
use aruna_operations::realm::set_quota::SetQuotaError;
use aruna_storage::storage;
use aruna_tasks::TaskHandle;
use axum::body::Body;
use axum::extract::{FromRequest, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tempfile::{TempDir, tempdir};
use tower::ServiceExt;
use ulid::Ulid;

async fn setup_state() -> (Arc<ServerState>, TempDir) {
    let (tempdir, storage_handle) = test_storage();
    let driver_ctx = Arc::new(test_context(storage_handle));

    let realm_signing_key = generate_signing_key();
    let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let node_id = iroh::SecretKey::generate().public();

    let state = Arc::new(
        test_state(
            driver_ctx,
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
        )
        .await,
    );

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

fn key_set(value: &serde_json::Value) -> std::collections::HashSet<&str> {
    value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect()
}

/// Anonymous and foreign-realm callers keep the flat shape, but every gated
/// value is absent or empty: only health, realm and public interface urls
/// remain, never identity, topology, backend detail or warnings.
#[tokio::test]
async fn anonymous_info_hides() {
    let (state, _tempdir) = setup_state().await;
    state
        .register_rest_interface("0.0.0.0:3000".parse().unwrap())
        .await;

    for auth in [None, Some(foreign_auth())] {
        let (status, Json(response)) = get_info(State(state.clone()), Extension(auth)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(response.node.status, ServiceStatus::Available);
        assert!(response.node.peer_id.is_none());
        assert!(response.node.capabilities.is_none());
        assert!(response.portal.is_none());
        assert!(response.my_addresses.is_empty());
        assert!(response.connections.is_empty());
        assert!(response.warnings.is_empty());
        assert!(response.services.network.is_none());
        assert!(response.services.blob.is_none());
        assert!(response.services.database.is_none());
        assert_eq!(
            response.services.interfaces.rest.url.as_deref(),
            Some("http://127.0.0.1:3000/api/v1"),
            "clients still learn the public api url"
        );
        assert!(response.services.interfaces.rest.bind.is_none());

        let body = serde_json::to_value(&response).unwrap();
        assert_eq!(
            key_set(&body),
            std::collections::HashSet::from([
                "node",
                "api_version",
                "my_addresses",
                "connections",
                "services",
                "warnings",
            ])
        );
        assert_eq!(
            key_set(&body["node"]),
            std::collections::HashSet::from(["status", "realm_id"])
        );
        assert_eq!(
            key_set(&body["services"]),
            std::collections::HashSet::from(["interfaces"])
        );
        assert_eq!(body["api_version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(body["node"]["realm_id"], state.get_realm_id().to_string());
    }
}

/// A realm token unlocks node identity, addresses and peers; backend detail
/// and request metrics stay admin-only.
#[tokio::test]
async fn realm_sees_topology() {
    let (state, _tempdir) = setup_state().await;
    state
        .register_rest_interface("0.0.0.0:3000".parse().unwrap())
        .await;
    state
        .register_s3_interface("0.0.0.0:1337".parse().unwrap(), "127.0.0.1:1337")
        .await;
    let auth = test_auth_context(&state);

    let (status, Json(response)) = get_info(State(state.clone()), Extension(Some(auth))).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(response.node.peer_id, Some(state.get_node_id().to_string()));
    assert_eq!(response.node.capabilities, Some(NodeCapabilityKind::User));
    let network = response.services.network.expect("realm token sees network");
    assert_eq!(network.status, ServiceStatus::Unavailable);
    assert!(
        network.requests.is_none(),
        "request metrics stay admin-only"
    );
    assert_eq!(
        response.services.interfaces,
        InterfaceServicesStatus {
            rest: InterfaceStatus {
                status: ServiceStatus::Available,
                bind: Some("0.0.0.0:3000".to_string()),
                url: Some("http://127.0.0.1:3000/api/v1".to_string()),
            },
            s3: InterfaceStatus {
                status: ServiceStatus::Available,
                bind: Some("0.0.0.0:1337".to_string()),
                url: Some("http://127.0.0.1:1337".to_string()),
            },
            mcp: None,
        }
    );
    assert!(
        response.services.blob.is_none(),
        "backend detail stays admin-only"
    );
    assert!(response.services.database.is_none());
    assert!(response.portal.is_none());
    assert!(response.warnings.is_empty());
}

/// Backend detail and errors need a realm config admin; a plain realm member
/// sees node identity but no backend services.
#[tokio::test]
async fn admin_sees_operations() {
    let (state, realm_id, admin, _tempdir) = setup_management_state().await;
    let member = AuthContext {
        user_id: UserId::local(Ulid::generate(), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    };

    let (_, Json(response)) = get_info(State(state.clone()), Extension(Some(member))).await;
    assert!(
        response.node.peer_id.is_some(),
        "realm member sees node identity"
    );
    assert!(
        response.services.blob.is_none(),
        "plain realm member is not an admin"
    );
    assert!(response.services.database.is_none());
    assert!(response.portal.is_none());

    let (status, Json(response)) =
        get_info(State(state), Extension(Some(admin_auth(realm_id, admin)))).await;

    assert_eq!(status, StatusCode::OK);
    assert!(response.node.peer_id.is_some());
    assert_eq!(
        response.services.blob.expect("admin sees blob").status,
        ServiceStatus::NotConfigured
    );
    assert_eq!(
        response
            .services
            .database
            .expect("admin sees database")
            .status,
        ServiceStatus::Available
    );
    assert_eq!(response.portal.expect("admin sees portal").mode, "disabled");
}

/// `last_error` on a peer connection leaks internal diagnostics, so a realm
/// member sees it redacted while a realm config admin sees it in full.
#[test]
fn peer_error_visibility() {
    let peer = aruna_core::structs::PeerConnectionState {
        node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
        status: aruna_core::structs::PeerConnectionStatus::Unreachable,
        active_addresses: Vec::new(),
        last_error: Some("dial refused: 10.0.0.9:4433".to_string()),
        next_retry_in_secs: Some(5),
    };

    let member = super::map_peer_connection(&peer, false);
    assert!(
        member.last_error.is_none(),
        "non-admin must not see peer errors"
    );
    assert_eq!(member.next_retry_secs, Some(5));
    assert_eq!(member.peer_id, peer.node_id.to_string());

    let admin = super::map_peer_connection(&peer, true);
    assert_eq!(
        admin.last_error.as_deref(),
        Some("dial refused: 10.0.0.9:4433")
    );
}

#[tokio::test]
async fn admin_reports_storage() {
    let (state, realm_id, admin, _tempdir) = setup_management_state().await;

    let _ = state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: "missing".to_string(),
            key: b"key".to_vec().into(),
            txn_id: Some(ulid::Ulid::generate()),
        })
        .await;

    let (status, Json(response)) =
        get_info(State(state), Extension(Some(admin_auth(realm_id, admin)))).await;

    assert_eq!(status, StatusCode::OK);
    let database = response.services.database.expect("admin sees database");
    assert_eq!(database.status, ServiceStatus::Available);
    assert_eq!(
        database.requests.last_error.as_deref(),
        Some("Transaction not found")
    );
    assert!(database.requests.failure_rate > 0.0);
}

#[test]
fn openapi_includes_info() {
    let openapi = ApiDoc::openapi();

    assert!(openapi.paths.paths.contains_key("/system/info"));
}

async fn seed_usage_state(state: &Arc<ServerState>) {
    use aruna_core::keyspaces::{USAGE_NODE_STATS_KEYSPACE, USAGE_STATS_KEYSPACE};
    use aruna_core::structs::storage::usage::{
        NodeUsageSnapshot, global_shard_key, usage_global_key,
    };

    let ctx = state.get_ctx();
    // This node's live local total.
    ctx.storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            key: global_shard_key(0).into(),
            value: aruna_core::structs::storage::usage::UsageCounters {
                buckets: 2,
                ..Default::default()
            }
            .to_bytes()
            .unwrap()
            .into(),
            txn_id: None,
        })
        .await;
    // A remote node's replicated snapshot.
    let remote = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    ctx.storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: USAGE_NODE_STATS_KEYSPACE.to_string(),
            key: usage_global_key(remote).into(),
            value: NodeUsageSnapshot {
                node_id: remote,
                counters: aruna_core::structs::storage::usage::UsageCounters {
                    buckets: 3,
                    ..Default::default()
                },
            }
            .to_bytes()
            .unwrap()
            .into(),
            txn_id: None,
        })
        .await;
}

fn test_auth_context(state: &Arc<ServerState>) -> AuthContext {
    AuthContext {
        user_id: UserId::local(Ulid::generate(), state.get_realm_id()),
        realm_id: state.get_realm_id(),
        path_restrictions: None,
        session: None,
    }
}

/// The usage directory is closed to anonymous and foreign-realm callers.
#[tokio::test]
async fn usage_requires_auth() {
    let (state, _tempdir) = setup_state().await;
    seed_usage_state(&state).await;

    assert!(matches!(
        get_usage(State(state.clone()), Extension(None)).await,
        Err(ServerError::Unauthorized)
    ));
    assert!(matches!(
        get_usage(State(state.clone()), Extension(Some(foreign_auth()))).await,
        Err(ServerError::Forbidden)
    ));

    let auth = test_auth_context(&state);
    let (status, Json(response)) = get_usage(State(state), Extension(Some(auth)))
        .await
        .unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response.buckets, 2, "flat fields report local total");
    assert_eq!(response.realm.buckets, 5, "realm sums local and remote");
    assert_eq!(
        response.metadata_documents, None,
        "a node without a metadata subsystem omits the count"
    );
    let body = serde_json::to_value(&response).unwrap();
    assert!(!body.as_object().unwrap().contains_key("metadata_documents"));
}

/// The reported total covers every live document in the realm, including
/// the private ones the caller holds no role for.
#[tokio::test]
async fn usage_counts_documents() {
    use aruna_core::storage_entries::registry_write_entries;
    use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
    use aruna_core::structs::placement::placement_record::PlacementRef;

    let storage_dir = tempdir().unwrap();
    let metadata_dir = tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let node_id = iroh::SecretKey::generate().public();
    let metadata_handle = aruna_operations::metadata::MetadataHandle::new(
        metadata_dir.path(),
        node_id,
        storage_handle.clone(),
        None,
        None,
        None,
    )
    .unwrap();
    let realm_signing_key = generate_signing_key();
    let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let driver_ctx = Arc::new(DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: Some(metadata_handle),
        task_handle: None,
        compute_handle: None,
    });
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

    let group_id = Ulid::generate();
    let mut writes = Vec::new();
    for (index, public) in [(0u8, true), (1, true), (2, false)] {
        let document_id = Ulid::from_parts(index.into(), index.into());
        let path = format!("datasets/{index}");
        let record = MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: path.clone(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                &path,
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::nil(),
            last_event_id: Ulid::nil(),
        };
        writes.extend(registry_write_entries(&record).unwrap());
    }
    assert!(matches!(
        state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await,
        aruna_core::events::Event::Storage(
            aruna_core::events::StorageEvent::BatchWriteResult { .. }
        )
    ));

    let auth = test_auth_context(&state);
    let (_, Json(response)) = get_usage(State(state), Extension(Some(auth)))
        .await
        .unwrap();

    assert_eq!(response.metadata_documents, Some(3));
}

#[test]
fn quota_warning_unlimited() {
    let group = Ulid::generate();
    let unlimited_group = Ulid::generate();
    let quota = QuotaConfig {
        default_group_quota_bytes: Some(1_000),
        grace_factor_percent: 110,
        warn_threshold_percent: 85,
        group_overrides: vec![aruna_core::structs::identity::realm::GroupQuotaOverride {
            group_id: unlimited_group,
            quota_bytes: None,
            grace_factor_percent: None,
        }],
        ..QuotaConfig::default()
    };

    // Finite default quota, usage below the 850-byte warn threshold.
    let below = super::GroupQuotaStatus::resolve(&quota, &group, 800);
    assert_eq!(below.quota_bytes, Some(1_000));
    assert_eq!(below.ceiling_bytes, Some(1_100));
    assert_eq!(below.warn_threshold_percent, 85);
    assert!(!below.warning);

    // At the threshold the warning fires.
    let at = super::GroupQuotaStatus::resolve(&quota, &group, 850);
    assert!(at.warning);

    // An override with quota_bytes: None is unlimited and never warns.
    let unlimited = super::GroupQuotaStatus::resolve(&quota, &unlimited_group, u64::MAX);
    assert_eq!(unlimited.quota_bytes, None);
    assert_eq!(unlimited.ceiling_bytes, None);
    assert!(!unlimited.warning);
}

#[test]
fn fractional_warn_threshold() {
    let group = Ulid::generate();
    let quota = QuotaConfig {
        default_group_quota_bytes: Some(3),
        warn_threshold_percent: 85,
        ..QuotaConfig::default()
    };

    let below = super::GroupQuotaStatus::resolve(&quota, &group, 2);
    assert!(!below.warning);
    let at = super::GroupQuotaStatus::resolve(&quota, &group, 3);
    assert!(at.warning);

    let tiny_quota = QuotaConfig {
        default_group_quota_bytes: Some(1),
        warn_threshold_percent: 85,
        ..QuotaConfig::default()
    };
    let zero = super::GroupQuotaStatus::resolve(&tiny_quota, &group, 0);
    assert!(!zero.warning);
}

#[test]
fn openapi_includes_quota() {
    let openapi = ApiDoc::openapi();

    assert!(openapi.paths.paths.contains_key("/system/realm/quota"));
}

#[test]
fn quota_conflict_maps() {
    let error = map_quota_error(SetQuotaError::StorageError(
        StorageError::TransactionConflict,
    ));

    assert!(matches!(
        error,
        ServerError::Conflict(message) if message.contains("retry")
    ));
}

#[test]
fn quota_capacity_unavailable() {
    // Cleanup capacity is transient, so it must not read as an internal error.
    let error = map_quota_error(SetQuotaError::StorageError(StorageError::CleanupCapacity));

    assert!(matches!(error, ServerError::ServiceUnavailableReason(_)));
}

async fn setup_management_state() -> (Arc<ServerState>, RealmId, UserId, TempDir) {
    let tempdir = tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    });

    let realm_signing_key = generate_signing_key();
    let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let node_id = iroh::SecretKey::generate().public();

    drive(
        CreateRealmOperation::new(CreateRealmConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            realm_description: "Realm".to_string(),
            oidc_providers: vec![],
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        }),
        &driver_ctx,
    )
    .await
    .unwrap();
    drive(
        ClaimInitialOperation::new(ClaimInitialInput {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
        }),
        &driver_ctx,
    )
    .await
    .unwrap();

    let state = Arc::new(
        ServerState::new(
            driver_ctx,
            realm_id,
            node_id,
            NodeCapabilities::management_node(realm_signing_key).unwrap(),
            false,
            None,
            aruna_operations::jobs::runtime::JobsRuntime::new(),
        )
        .await,
    );

    (state, realm_id, user_id, tempdir)
}

fn admin_auth(realm_id: RealmId, user_id: UserId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    }
}

/// Installs a realm deny policy for one permission path.
async fn deny_path(state: &ServerState, path: &str) {
    let realm_id = state.get_realm_id();
    let mut config = drive(
        aruna_operations::realm::get_config::GetConfigOperation::new(realm_id),
        &state.get_ctx(),
    )
    .await
    .unwrap();
    config
        .request_policies
        .push(aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::generate(),
            name: "deny-path".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: format!("path == '{path}'"),
            enabled: true,
        });
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
            key: realm_id.as_bytes().to_vec().into(),
            value: config.to_bytes(&actor).unwrap().into(),
            txn_id: None,
        })
        .await;
}

#[tokio::test]
async fn policy_blocks_admin() {
    // The config-admin gate must honour realm request policies, not only
    // the RBAC roles the permission check reads.
    let (state, realm_id, admin, _tempdir) = setup_management_state().await;
    let auth = admin_auth(realm_id, admin);
    let _ = get_realm_placement(State(state.clone()), Extension(Some(auth.clone())))
        .await
        .expect("the realm admin sees the placement config");

    deny_path(&state, &format!("/{realm_id}/admin/config")).await;

    assert!(matches!(
        get_realm_placement(State(state.clone()), Extension(Some(auth))).await,
        Err(ServerError::Forbidden)
    ));
}

fn placement_strategy(strategy_id: Ulid) -> RealmPlacementStrategy {
    RealmPlacementStrategy {
        strategy_id: strategy_id.to_string(),
        name: "hot".to_string(),
        replica_count: Some(2),
        distinct_locations: true,
        affinity: Vec::new(),
        shard_count: 64,
    }
}

async fn provision_strategy(state: &ServerState, actor: Actor, strategy_id: Ulid) {
    allocate_placement_binding(
        state.get_ctx().as_ref(),
        actor.clone(),
        PlacementScope::Realm(actor.realm_id),
        DocumentClass::Metadata,
        strategy_id,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn placement_admin_gated() {
    let (state, realm_id, _admin, _tempdir) = setup_management_state().await;
    assert!(matches!(
        get_realm_placement(State(state.clone()), Extension(None)).await,
        Err(ServerError::Unauthorized)
    ));

    let (local_state, _tempdir) = setup_state().await;
    let local_auth = AuthContext {
        user_id: UserId::local(Ulid::generate(), local_state.get_realm_id()),
        realm_id: local_state.get_realm_id(),
        path_restrictions: None,
        session: None,
    };
    assert!(matches!(
        get_realm_placement(State(local_state), Extension(Some(local_auth))).await,
        Err(ServerError::Forbidden)
    ));

    let request = RealmPlacementRequest::RemoveOverride {
        subject: "00".to_string(),
    };
    assert!(matches!(
        mutate_realm_placement(State(state.clone()), Extension(None), Ok(Json(request))).await,
        Err(ServerError::Unauthorized)
    ));

    let stranger = AuthContext {
        user_id: UserId::local(Ulid::generate(), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    };
    assert!(matches!(
        get_realm_placement(State(state), Extension(Some(stranger))).await,
        Err(ServerError::Forbidden)
    ));
}

#[tokio::test]
async fn placement_binding_lifecycle() {
    let (state, realm_id, admin, _tempdir) = setup_management_state().await;
    let auth = admin_auth(realm_id, admin);
    let job_family_strategy_id = drive(
        aruna_operations::realm::get_config::GetConfigOperation::new(realm_id),
        &state.get_ctx(),
    )
    .await
    .unwrap()
    .job_family_strategy_id
    .to_string();
    let (_, Json(initial)) =
        get_realm_placement(State(state.clone()), Extension(Some(auth.clone())))
            .await
            .unwrap();
    assert_eq!(
        serde_json::to_value(&initial).unwrap()["job_family_strategy_id"].as_str(),
        Some(job_family_strategy_id.as_str())
    );
    let initial_default = initial.default_strategy_id.unwrap();
    let strategy_id = Ulid::from_bytes([21; 16]);
    let scope = RealmBindingScope::Realm;
    let node_id = state.get_node_id().to_string();

    let (_status, Json(after_upsert)) = mutate_realm_placement(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Ok(Json(RealmPlacementRequest::UpsertStrategy {
            strategy: placement_strategy(strategy_id),
        })),
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::to_value(after_upsert).unwrap()["job_family_strategy_id"].as_str(),
        Some(job_family_strategy_id.as_str())
    );
    let _ = mutate_realm_placement(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Ok(Json(RealmPlacementRequest::ProvisionMetadataBinding {
            strategy_id: strategy_id.to_string(),
            group_id: None,
        })),
    )
    .await
    .unwrap();

    for request in [
        RealmPlacementRequest::SetDefaultStrategy {
            strategy_id: strategy_id.to_string(),
        },
        RealmPlacementRequest::SetBinding {
            binding: RealmBinding {
                scope: scope.clone(),
                strategy_id: strategy_id.to_string(),
            },
        },
        RealmPlacementRequest::SetOverride {
            placement_override: RealmPlacementOverride {
                subject: "abcd".to_string(),
                pinned: vec![node_id],
                excluded: Vec::new(),
                strategy_id: Some(strategy_id.to_string()),
            },
        },
    ] {
        let (status, _) = mutate_realm_placement(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Ok(Json(request)),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::OK);
    }

    let (_, Json(stored)) =
        get_realm_placement(State(state.clone()), Extension(Some(auth.clone())))
            .await
            .unwrap();
    assert_eq!(stored.default_strategy_id, Some(strategy_id.to_string()));
    assert!(stored.strategies.iter().any(|strategy| {
        strategy.strategy_id == strategy_id.to_string() && strategy.replica_count == Some(2)
    }));
    assert!(stored.bindings.iter().any(|binding| {
        binding.scope == scope && binding.strategy_id == strategy_id.to_string()
    }));
    assert!(stored.overrides.iter().any(|record| {
        record.subject == "abcd" && record.strategy_id == Some(strategy_id.to_string())
    }));

    let error = mutate_realm_placement(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Ok(Json(RealmPlacementRequest::RemoveStrategy {
            strategy_id: strategy_id.to_string(),
        })),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ServerError::Conflict(message) if message.contains("referenced")));

    for request in [
        RealmPlacementRequest::RemoveOverride {
            subject: "abcd".to_string(),
        },
        RealmPlacementRequest::RemoveBinding {
            scope: scope.clone(),
        },
        RealmPlacementRequest::SetDefaultStrategy {
            strategy_id: initial_default,
        },
    ] {
        let (_status, _body) = mutate_realm_placement(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Ok(Json(request)),
        )
        .await
        .unwrap();
    }

    let error = mutate_realm_placement(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Ok(Json(RealmPlacementRequest::RemoveStrategy {
            strategy_id: strategy_id.to_string(),
        })),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ServerError::Conflict(message) if message.contains("referenced")));

    let (_, Json(stored)) = get_realm_placement(State(state), Extension(Some(auth)))
        .await
        .unwrap();
    assert!(
        stored
            .strategies
            .iter()
            .any(|strategy| strategy.strategy_id == strategy_id.to_string())
    );
    assert!(!stored.bindings.iter().any(|binding| binding.scope == scope));
    assert!(
        !stored
            .overrides
            .iter()
            .any(|record| record.subject == "abcd")
    );
}

#[tokio::test]
async fn replication_factor_defaults() {
    let (state, realm_id, admin, _tempdir) = setup_management_state().await;
    let auth = admin_auth(realm_id, admin);
    let strategy_id = Ulid::from_bytes([24; 16]);

    let (_status, _body) = mutate_realm_placement(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Ok(Json(RealmPlacementRequest::UpsertStrategy {
            strategy: placement_strategy(strategy_id),
        })),
    )
    .await
    .unwrap();
    provision_strategy(
        state.as_ref(),
        Actor {
            node_id: state.get_node_id(),
            user_id: admin,
            realm_id,
        },
        strategy_id,
    )
    .await;
    let (_status, _body) = mutate_realm_placement(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Ok(Json(RealmPlacementRequest::SetDefaultStrategy {
            strategy_id: strategy_id.to_string(),
        })),
    )
    .await
    .unwrap();

    let (_, Json(info)) = get_realm_info(State(state.clone()), Extension(Some(auth.clone())))
        .await
        .unwrap();
    assert_eq!(
        info.metadata_replication.default_replication_factor,
        Some(2)
    );

    let mut unbounded = placement_strategy(strategy_id);
    unbounded.replica_count = None;
    let _ = mutate_realm_placement(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Ok(Json(RealmPlacementRequest::UpsertStrategy {
            strategy: unbounded,
        })),
    )
    .await
    .unwrap();

    let (_, Json(info)) = get_realm_info(State(state), Extension(Some(auth)))
        .await
        .unwrap();
    assert_eq!(info.metadata_replication.default_replication_factor, None);
    assert_eq!(
        serde_json::to_value(info.metadata_replication).unwrap()["default_replication_factor"],
        serde_json::Value::Null
    );
}

#[test]
fn replication_schema_unbounded() {
    let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();
    let factor = &openapi["components"]["schemas"]["RealmMetadataReplicationResponse"]["properties"]
        ["default_replication_factor"];
    assert_eq!(
        factor["type"],
        serde_json::json!(["integer", "null"]),
        "default_replication_factor schema must represent finite and unbounded defaults"
    );
}

#[tokio::test]
async fn placement_rejects_invalid() {
    let (state, realm_id, admin, _tempdir) = setup_management_state().await;
    let auth = admin_auth(realm_id, admin);
    let missing = Ulid::from_bytes([22; 16]);
    let mut zero = placement_strategy(missing);
    zero.replica_count = Some(0);

    for request in [
        RealmPlacementRequest::UpsertStrategy { strategy: zero },
        RealmPlacementRequest::SetDefaultStrategy {
            strategy_id: missing.to_string(),
        },
        RealmPlacementRequest::SetBinding {
            binding: RealmBinding {
                scope: RealmBindingScope::Realm,
                strategy_id: missing.to_string(),
            },
        },
        RealmPlacementRequest::SetOverride {
            placement_override: RealmPlacementOverride {
                subject: "00".to_string(),
                pinned: Vec::new(),
                excluded: Vec::new(),
                strategy_id: Some(missing.to_string()),
            },
        },
        RealmPlacementRequest::ProvisionMetadataBinding {
            strategy_id: missing.to_string(),
            group_id: None,
        },
    ] {
        assert!(matches!(
            mutate_realm_placement(
                State(state.clone()),
                Extension(Some(auth.clone())),
                Ok(Json(request))
            )
            .await,
            Err(ServerError::BadRequestReason(_))
        ));
    }

    for request in [
        RealmPlacementRequest::RemoveStrategy {
            strategy_id: "not-a-ulid".to_string(),
        },
        RealmPlacementRequest::RemoveOverride {
            subject: "not-hex".to_string(),
        },
        RealmPlacementRequest::SetOverride {
            placement_override: RealmPlacementOverride {
                subject: "00".to_string(),
                pinned: vec!["not-a-node".to_string()],
                excluded: Vec::new(),
                strategy_id: None,
            },
        },
    ] {
        assert!(matches!(
            mutate_realm_placement(
                State(state.clone()),
                Extension(Some(auth.clone())),
                Ok(Json(request))
            )
            .await,
            Err(ServerError::BadRequestReason(_))
        ));
    }

    let request = axum::http::Request::builder()
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"mutation":"remove_override","subject":"00","extra":true}"#,
        ))
        .unwrap();
    let rejection = Json::<RealmPlacementRequest>::from_request(request, &())
        .await
        .unwrap_err();
    assert!(matches!(
        mutate_realm_placement(State(state), Extension(Some(auth)), Err(rejection)).await,
        Err(ServerError::BadRequestReason(_))
    ));
}

#[tokio::test]
async fn placement_route_registered() {
    let (state, _realm_id, _admin, _tempdir) = setup_management_state().await;
    let response = crate::routes::rest_router(state)
        .oneshot(
            axum::http::Request::builder()
                .uri("/system/realm/placement")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[test]
fn openapi_registers_placement() {
    let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();
    let path = &openapi["paths"]["/system/realm/placement"];
    assert!(path.get("get").is_some());
    assert!(path.get("patch").is_some());
    assert!(
        openapi["components"]["schemas"]
            .get("RealmPlacementMutationRequest")
            .is_some()
    );
    assert!(
        openapi["components"]["schemas"]
            .get("RealmPlacementConfigResponse")
            .is_some()
    );
}

#[test]
fn placement_conflict_maps() {
    let error = map_placement_error(MutatePlacementError::StorageError(
        StorageError::TransactionConflict,
    ));
    assert!(matches!(
        error,
        ServerError::Conflict(message) if message.contains("retry")
    ));
}

#[test]
fn placement_missing_config() {
    assert!(matches!(
        map_placement_error(MutatePlacementError::RealmConfigNotFound),
        ServerError::NotFound
    ));
}

#[test]
fn placement_capacity_unavailable() {
    // Cleanup capacity is transient on both placement paths.
    assert!(matches!(
        map_placement_error(MutatePlacementError::StorageError(
            StorageError::CleanupCapacity
        )),
        ServerError::ServiceUnavailableReason(_)
    ));
    assert!(matches!(
        map_handle_error(HandleAllocationError::Storage(
            StorageError::CleanupCapacity
        )),
        ServerError::ServiceUnavailableReason(_)
    ));
}

#[tokio::test]
async fn quota_requires_auth() {
    let (state, _realm_id, _admin, _tempdir) = setup_management_state().await;
    let body = RealmQuotaConfig::from(QuotaConfig::default());

    let error = set_realm_quota(State(state), Extension(None), Json(body))
        .await
        .unwrap_err();

    assert!(matches!(error, ServerError::Unauthorized));
}

#[tokio::test]
async fn quota_requires_admin() {
    let (state, realm_id, _admin, _tempdir) = setup_management_state().await;
    let stranger = AuthContext {
        user_id: UserId::local(Ulid::generate(), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let body = RealmQuotaConfig::from(QuotaConfig::default());

    let error = set_realm_quota(State(state), Extension(Some(stranger)), Json(body))
        .await
        .unwrap_err();

    assert!(matches!(error, ServerError::Forbidden));
}

#[tokio::test]
async fn admin_sets_quota() {
    let (state, realm_id, admin, _tempdir) = setup_management_state().await;
    let auth = AuthContext {
        user_id: admin,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let mut body = RealmQuotaConfig::from(QuotaConfig::default());
    body.default_group_quota_bytes = Some(4096);
    body.max_devices_per_user = Some(3);
    body.device_requests_per_minute = Some(600);
    body.device_concurrent_pulls = Some(8);

    let (status, Json(stored)) = set_realm_quota(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Json(body),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stored.default_group_quota_bytes, Some(4096));
    assert_eq!(stored.max_devices_per_user, Some(3));
    assert_eq!(stored.device_requests_per_minute, Some(600));
    assert_eq!(stored.device_concurrent_pulls, Some(8));

    let (status, Json(info)) = get_realm_info(State(state), Extension(Some(auth)))
        .await
        .unwrap();
    assert_eq!(status, StatusCode::OK);
    let quota = info.quota.expect("realm token sees quota");
    assert_eq!(quota.default_group_quota_bytes, Some(4096));
    assert_eq!(quota.max_devices_per_user, Some(3));
}

/// Anonymous callers keep what they need to authenticate; realm topology,
/// discovery and quota policy need a token of this realm.
#[tokio::test]
async fn realm_gates_detail() {
    let (state, realm_id, admin, _tempdir) = setup_management_state().await;
    state
        .register_rest_interface("0.0.0.0:3000".parse().unwrap())
        .await;
    let auth = admin_auth(realm_id, admin);
    let mut body = RealmQuotaConfig::from(QuotaConfig::default());
    body.user_group_cap_overrides = vec![GroupCapOverride {
        user_id: admin.to_string(),
        max_groups: Some(1),
    }];
    let _ = set_realm_quota(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Json(body),
    )
    .await
    .unwrap();

    for anonymous in [None, Some(foreign_auth())] {
        let (_status, Json(info)) = get_realm_info(State(state.clone()), Extension(anonymous))
            .await
            .unwrap();
        assert_eq!(info.realm_id, realm_id.to_string());
        assert_eq!(info.description, "Realm");
        assert!(info.nodes.is_empty(), "realm topology is not public");
        assert!(info.is_management_node, "the node kind is public");
        assert_eq!(
            info.management_urls,
            vec!["http://127.0.0.1:3000/api/v1".to_string()],
            "a management node names its own published url"
        );
        assert!(info.quota.is_none(), "quota policy is not public");
        assert!(info.discovery.is_none(), "discovery is not public");
        assert_eq!(
            info.interfaces.rest.url.as_deref(),
            Some("http://127.0.0.1:3000/api/v1"),
            "clients still learn the public api url"
        );
        assert!(
            info.interfaces.rest.bind.is_none(),
            "listen address is not public"
        );
        let body = serde_json::to_value(&info).unwrap();
        assert!(
            body.get("metadata_replication").is_some(),
            "replication policy stays public"
        );
        assert!(
            body.get("detail").is_none(),
            "flat shape has no detail wrapper"
        );
    }

    let (_status, Json(info)) = get_realm_info(State(state), Extension(Some(auth)))
        .await
        .unwrap();
    assert_eq!(info.interfaces.rest.bind.as_deref(), Some("0.0.0.0:3000"));
    assert!(!info.nodes.is_empty());
    assert!(info.discovery.is_some());
    let quota = info.quota.expect("realm token sees quota");
    assert_eq!(quota.user_group_cap_overrides.len(), 1);
    assert_eq!(quota.user_group_cap_overrides[0].user_id, admin.to_string());
}

/// Signed-out callers receive only aggregate overview values. An
/// unavailable metadata count is serialized as null, never as a false zero.
#[tokio::test]
async fn anonymous_realm_overview() {
    let (state, realm_id, admin, _tempdir) = setup_management_state().await;
    let group = Group {
        display_name: "Protected group title".to_string(),
        group_id: Ulid::generate(),
        realm_id,
        roles: Default::default(),
        owner: admin,
    };
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: admin,
        realm_id,
    };
    match state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: GROUP_KEYSPACE.to_string(),
            key: group.group_id.to_bytes().to_vec().into(),
            value: group.to_bytes(&actor).unwrap().into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected group write: {other:?}"),
    }

    let (status, Json(info)) = get_realm_info(State(state), Extension(None)).await.unwrap();
    assert_eq!(status, StatusCode::OK);
    assert!(info.nodes.is_empty());
    assert!(info.discovery.is_none());
    assert!(info.quota.is_none());
    let overview = info.public_overview.as_ref().expect("public overview");
    assert_eq!(overview.live_datasets, None);
    assert_eq!(overview.groups, Some(1));
    assert_eq!(overview.nodes_configured, Some(1));

    let body = serde_json::to_value(&info).unwrap();
    assert_eq!(
        key_set(&body["public_overview"]),
        std::collections::HashSet::from(["live_datasets", "groups", "nodes_configured",])
    );
    assert!(body["public_overview"]["live_datasets"].is_null());
    assert_ne!(body["public_overview"]["live_datasets"], 0);
    assert!(body.get("discovery").is_none());
    assert!(body.get("quota").is_none());
    assert_eq!(body["nodes"], serde_json::json!([]));
    assert!(!body.to_string().contains("Protected group title"));
}

#[tokio::test]
async fn management_urls_follow() {
    // The published document names the url a device follows; without one a management
    // node falls back to its own interface, and a server node lists others but not itself.
    use aruna_core::keyspaces::NODE_INFO_KEYSPACE;
    use aruna_core::structs::storage::node_info::{
        NodeInfoDocument, NodeUrls, NodeUtilization, node_info_key,
    };

    let (state, _realm_id, _admin, _tempdir) = setup_management_state().await;
    state
        .register_rest_interface("0.0.0.0:3000".parse().unwrap())
        .await;
    let node_id = state.get_node_id();
    let document = NodeInfoDocument {
        node_id,
        executors: Vec::new(),
        labels: Default::default(),
        urls: NodeUrls {
            api: Some("https://mgmt.example/api/v1".to_string()),
            s3: None,
        },
        utilization: NodeUtilization {
            storage_bytes_used: 0,
            documents_held: None,
            load_permille: None,
            heartbeat_at_ms: 1_700_000_000_000,
        },
        updated_at_ms: 1_700_000_000_500,
        epoch: aruna_core::structs::storage::node_info::AdvertisementEpoch {
            membership_generation: 1,
            publisher_generation: 1,
            observed_at_ms: 1_700_000_000_500,
        },
        compute_draining: false,
        leaving: false,
        demand: Default::default(),
        reservation: Default::default(),
    };
    state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: NODE_INFO_KEYSPACE.to_string(),
            key: node_info_key(node_id).into(),
            value: document.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;

    let (_status, Json(info)) = get_realm_info(State(state), Extension(None)).await.unwrap();
    assert!(info.is_management_node);
    assert_eq!(
        info.management_urls,
        vec!["https://mgmt.example/api/v1".to_string()]
    );
}

#[tokio::test]
async fn realm_node_details() {
    use aruna_core::keyspaces::NODE_INFO_KEYSPACE;
    use aruna_core::structs::storage::node_info::{
        NodeInfoDocument, NodeUrls, NodeUtilization, node_info_key,
    };

    let (state, realm_id, admin, _tempdir) = setup_management_state().await;
    let node_id = state.get_node_id();

    // The creating node's placement entry is seeded at realm creation with
    // the default location/weight. Publish a node info document for it too.
    let mut docker = aruna_core::compute::ExecutorCapability::new(
        "docker".to_string(),
        aruna_core::structs::placement::placement_policy::PlacementSubject {
            node_id,
            generation: 1,
            location: "eu-west".to_string(),
            labels: std::collections::BTreeMap::new(),
            executor_kind: None,
            local_to_controller: true,
        },
    )
    .expect("subject is valid");
    docker.file_staging = true;
    docker.direct_s3 = true;
    let document = NodeInfoDocument {
        node_id,
        executors: vec![docker],
        labels: std::collections::BTreeMap::from([("tier".to_string(), "hot".to_string())]),
        urls: NodeUrls {
            api: None,
            s3: Some("s3.example".to_string()),
        },
        utilization: NodeUtilization {
            storage_bytes_used: 4_096,
            documents_held: None,
            load_permille: None,
            heartbeat_at_ms: 1_700_000_000_000,
        },
        updated_at_ms: 1_700_000_000_500,
        epoch: aruna_core::structs::storage::node_info::AdvertisementEpoch {
            membership_generation: 1,
            publisher_generation: 1,
            observed_at_ms: 1_700_000_000_500,
        },
        compute_draining: false,
        leaving: false,
        demand: Default::default(),
        reservation: Default::default(),
    };
    state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: NODE_INFO_KEYSPACE.to_string(),
            key: node_info_key(node_id).into(),
            value: document.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;

    let (status, Json(info)) =
        get_realm_info(State(state), Extension(Some(admin_auth(realm_id, admin))))
            .await
            .unwrap();
    assert_eq!(status, StatusCode::OK);
    let node = info
        .nodes
        .iter()
        .find(|node| node.node_id == node_id.to_string())
        .expect("creating node present in realm info");

    let placement = node.placement.as_ref().expect("placement entry present");
    assert_eq!(placement.location, "default");
    assert_eq!(placement.weight, 100);
    assert!(!placement.full);
    assert!(!placement.draining);

    let node_info = node.info.as_ref().expect("node info document present");
    assert_eq!(node_info.executors.len(), 1);
    assert_eq!(node_info.executors[0].kind, "docker");
    assert_eq!(node_info.labels.get("tier"), Some(&"hot".to_string()));
    assert_eq!(node_info.urls.s3.as_deref(), Some("s3.example"));
    assert_eq!(node_info.utilization.storage_bytes_used, 4_096);
    let serialized = serde_json::to_value(node_info).unwrap();
    assert!(serialized["urls"].get("api").is_none());
    assert!(serialized["utilization"].get("documents_held").is_none());
    assert!(serialized["utilization"].get("load_permille").is_none());
}

#[test]
fn node_openapi_optional() {
    let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();
    for (schema, optional_fields) in [
        ("RealmNodeUrlsResponse", &["api", "s3"][..]),
        (
            "RealmNodeUtilizationResponse",
            &["documents_held", "load_permille"][..],
        ),
    ] {
        let schema = &openapi["components"]["schemas"][schema];
        for field in optional_fields {
            assert!(schema["properties"].get(field).is_some());
            assert!(
                !schema["required"]
                    .as_array()
                    .is_some_and(|required| required.iter().any(|value| value == field))
            );
        }
    }
}

#[tokio::test]
async fn quota_invalid_reason() {
    use axum::response::IntoResponse;

    let (state, realm_id, admin, _tempdir) = setup_management_state().await;
    let auth = AuthContext {
        user_id: admin,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let mut body = RealmQuotaConfig::from(QuotaConfig::default());
    body.warn_threshold_percent = 0;

    let error = set_realm_quota(State(state), Extension(Some(auth)), Json(body))
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::BadRequestReason(_)));

    let response = error.into_response();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let parsed: crate::error::ErrorResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        parsed.code.as_deref(),
        Some("Bad request"),
        "machine code stays identical to plain BadRequest"
    );
    assert!(
        parsed.error.contains("warn_threshold_percent"),
        "body must carry the validation reason, got: {}",
        parsed.error
    );
}

#[tokio::test]
async fn rejects_zero_cap() {
    // A zero device quota is refused by the route, not silently stored.
    let (state, realm_id, admin, _tempdir) = setup_management_state().await;
    let auth = AuthContext {
        user_id: admin,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let mut body = RealmQuotaConfig::from(QuotaConfig::default());
    body.max_devices_per_user = Some(0);

    let error = set_realm_quota(State(state), Extension(Some(auth)), Json(body))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        ServerError::BadRequestReason(reason) if reason.contains("max_devices_per_user")
    ));
}

#[tokio::test]
async fn rejects_zero_limits() {
    // Zero would silence an enrolled device; only null is uncapped.
    let (state, realm_id, admin, _tempdir) = setup_management_state().await;
    let auth = AuthContext {
        user_id: admin,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    for body in [
        RealmQuotaConfig {
            device_requests_per_minute: Some(0),
            ..RealmQuotaConfig::from(QuotaConfig::default())
        },
        RealmQuotaConfig {
            device_concurrent_pulls: Some(0),
            ..RealmQuotaConfig::from(QuotaConfig::default())
        },
    ] {
        let error = set_realm_quota(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Json(body),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            ServerError::BadRequestReason(reason) if reason.starts_with("device_")
        ));
    }
}

#[test]
fn stale_stays_configured() {
    // A bounded-stale snapshot may not report a remote peer as connected.
    let local = iroh::SecretKey::from_bytes(&[41u8; 32]).public();
    let remote = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
    let nodes = HashSet::from([remote]);

    let fresh = presence_nodes(RealmPresence::new(nodes.clone(), false), local);
    assert!(fresh.contains(&remote) && fresh.contains(&local));

    let stale = presence_nodes(RealmPresence::new(nodes, true), local);
    assert_eq!(stale, HashSet::from([local]));
}

#[tokio::test]
async fn device_never_connected() {
    // Devices publish no presence, so a presence answer naming one, and
    // this node answering about itself, may still not connect it.
    let (state, realm_id, owner, _tempdir) = setup_management_state().await;
    let mut config = drive(
        aruna_operations::realm::get_config::GetConfigOperation::new(realm_id),
        &state.get_ctx(),
    )
    .await
    .unwrap();
    let device = iroh::SecretKey::from_bytes(&[43u8; 32]).public();
    for node_id in [device, state.get_node_id()] {
        config.nodes.push(aruna_core::structs::identity::realm::RealmNode {
            node_id: node_id.to_string(),
            kind: aruna_core::structs::identity::realm::RealmNodeKind::User { owner },
        });
    }

    let present = HashSet::from([device, state.get_node_id()]);
    let nodes = map_realm_nodes(
        &state,
        &config,
        present,
        BTreeMap::new(),
        &PeerContacts::default(),
        10_000,
    );

    let devices: Vec<_> = nodes
        .iter()
        .filter(|node| node.kind == NodeKindInfo::User)
        .collect();
    assert_eq!(devices.len(), 2);
    for node in devices {
        assert!(!node.present, "a device is never presence-confirmed");
        assert_ne!(node.connection_status, RealmConnectionStatus::Connected);
    }
    let infra = nodes
        .iter()
        .find(|node| node.kind != NodeKindInfo::User)
        .unwrap();
    assert!(infra.present);
    assert_eq!(infra.connection_status, RealmConnectionStatus::Connected);
}

#[tokio::test]
async fn reports_device_seen() {
    // A device is reported from this node's own contact record, and the
    // device's own node is serving the request, so it saw itself now.
    let (state, realm_id, owner, _tempdir) = setup_management_state().await;
    let mut config = drive(
        aruna_operations::realm::get_config::GetConfigOperation::new(realm_id),
        &state.get_ctx(),
    )
    .await
    .unwrap();
    let recent = iroh::SecretKey::from_bytes(&[44u8; 32]).public();
    let stale = iroh::SecretKey::from_bytes(&[45u8; 32]).public();
    for node_id in [recent, stale, state.get_node_id()] {
        config.nodes.push(aruna_core::structs::identity::realm::RealmNode {
            node_id: node_id.to_string(),
            kind: aruna_core::structs::identity::realm::RealmNodeKind::User { owner },
        });
    }
    let now_ms = 1_000_000;
    let window = aruna_operations::metadata::PEER_CONTACT_WINDOW.as_millis() as u64;
    let contacts = PeerContacts::default();
    contacts.note(recent, now_ms - window);
    contacts.note(stale, now_ms - window - 1);

    let nodes = map_realm_nodes(
        &state,
        &config,
        HashSet::new(),
        BTreeMap::new(),
        &contacts,
        now_ms,
    );
    let device = |node_id: aruna_core::NodeId| {
        nodes
            .iter()
            .find(|node| node.node_id == node_id.to_string() && node.kind == NodeKindInfo::User)
            .unwrap()
    };

    assert_eq!(
        device(recent).connection_status,
        RealmConnectionStatus::Seen
    );
    assert_eq!(device(recent).last_seen_ms, Some(now_ms - window));
    assert_eq!(
        device(stale).connection_status,
        RealmConnectionStatus::Unknown
    );
    assert_eq!(device(stale).last_seen_ms, Some(now_ms - window - 1));
    let current = device(state.get_node_id());
    assert_eq!(current.connection_status, RealmConnectionStatus::Seen);
    assert_eq!(current.last_seen_ms, Some(now_ms));
    let infra = nodes
        .iter()
        .find(|node| node.kind != NodeKindInfo::User)
        .unwrap();
    assert_eq!(infra.last_seen_ms, None, "only devices report contact");
}

fn usage_counters() -> UsageCounters {
    UsageCounters {
        buckets: 1,
        objects: 2,
        stored_blobs: 2,
        stored_bytes: 20,
        logical_bytes: 20,
        referenced_bytes: 0,
    }
}

#[test]
fn usage_reports_stored() {
    let body = serde_json::to_value(UsageResponse::new(usage_counters(), usage_counters()))
        .expect("serialized");
    assert_eq!(body["stored_blobs"], 2);
    assert_eq!(body["stored_bytes"], 20);
    assert_eq!(body["realm"]["stored_blobs"], 2);
}

#[test]
fn group_omits_stored() {
    // Physical copies carry no group dimension, so the group scope must omit
    // them instead of reporting the counter row's structural zero.
    let response = UsageResponse::for_group(usage_counters(), usage_counters());
    assert_eq!(response.stored_blobs, None);
    assert_eq!(response.realm.stored_bytes, None);

    let body = serde_json::to_value(&response).expect("serialized");
    let fields = body.as_object().expect("object");
    assert_eq!(fields["objects"], 2);
    assert!(!fields.contains_key("stored_blobs"));
    assert!(!fields.contains_key("stored_bytes"));
    let realm = body["realm"].as_object().expect("object");
    assert!(!realm.contains_key("stored_blobs"));
    assert!(!realm.contains_key("stored_bytes"));
}
