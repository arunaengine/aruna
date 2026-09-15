use super::{
    UserDeviceResponse, evict_device, list_user_devices, preferences_from_attributes,
    revoke_user_device,
};
use crate::error::ServerError;
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::keys::generate_signing_key;
use aruna_core::onboarding::{OnboardingMode, OnboardingPurpose, OnboardingSecretRecord};
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities};
use aruna_core::structs::identity::realm::{RealmId, RealmNodeKind};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::onboarding::create_secret::{CreateSecretInput, CreateSecretOperation};
use aruna_operations::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_operations::realm::ensure_config::{EnsureConfigOperation, EnsureConfigParams};
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use std::sync::Arc;
use tempfile::{TempDir, tempdir};
use ulid::Ulid;

struct Fixture {
    state: Arc<ServerState>,
    owner: UserId,
    other: UserId,
    admin: UserId,
    _dir: TempDir,
}

fn node(seed: u8) -> aruna_core::NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

/// One device and one outstanding enrollment per owner, plus a management
/// node, so every filter the routes apply has something to reject. The realm
/// and its members are produced by the operations enrollment uses.
async fn setup_devices() -> Fixture {
    let dir = tempdir().unwrap();
    let storage_handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
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
    let owner = UserId::local(Ulid::generate(), realm_id);
    let other = UserId::local(Ulid::generate(), realm_id);
    let admin = UserId::local(Ulid::generate(), realm_id);
    let actor = Actor {
        node_id: node(1),
        user_id: UserId::nil(realm_id),
        realm_id,
    };

    drive(
        CreateRealmOperation::new(CreateRealmConfig {
            actor: actor.clone(),
            realm_description: "devices".to_string(),
            oidc_providers: Vec::new(),
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        }),
        driver_ctx.as_ref(),
    )
    .await
    .unwrap();
    for (device, device_owner) in [(node(2), owner), (node(3), other)] {
        drive(
            EnsureConfigOperation::new(EnsureConfigParams {
                actor: actor.clone(),
                target_node_id: device,
                target_node_kind: RealmNodeKind::User {
                    owner: device_owner,
                },
                default_metadata_replication_factor: 3,
                realm_description: String::new(),
                create_if_missing: false,
                reject_kind_mismatch: true,
            }),
            driver_ctx.as_ref(),
        )
        .await
        .unwrap();
    }

    for secret_owner in [owner, other] {
        drive(
            CreateSecretOperation::new(CreateSecretInput {
                record: OnboardingSecretRecord {
                    enrollment_id: Ulid::generate(),
                    secret_hash: Ulid::generate().to_string(),
                    mode: OnboardingMode::User {
                        owner: secret_owner,
                    },
                    purpose: OnboardingPurpose::NodeEnrollment,
                    expires_at: u64::MAX,
                    claimed_node_id: None,
                },
            }),
            driver_ctx.as_ref(),
        )
        .await
        .unwrap();
    }

    drive(
        ClaimInitialOperation::new(ClaimInitialInput {
            actor: Actor {
                node_id: node(1),
                user_id: admin,
                realm_id,
            },
        }),
        driver_ctx.as_ref(),
    )
    .await
    .unwrap();

    let state = Arc::new(
        ServerState::new(
            driver_ctx,
            realm_id,
            node(1),
            NodeCapabilities::management_node(realm_signing_key).unwrap(),
            false,
            None,
            aruna_operations::jobs::runtime::JobsRuntime::new(),
        )
        .await,
    );
    Fixture {
        state,
        owner,
        other,
        admin,
        _dir: dir,
    }
}

fn auth(user_id: UserId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id: user_id.realm_id,
        path_restrictions: None,
        session: None,
    }
}

async fn devices(state: &Arc<ServerState>, owner: UserId) -> Vec<UserDeviceResponse> {
    let (_, Json(listed)) = list_user_devices(State(state.clone()), Extension(Some(auth(owner))))
        .await
        .unwrap();
    listed.devices
}

#[tokio::test]
async fn lists_owned_devices() {
    let fixture = setup_devices().await;
    let listed = devices(&fixture.state, fixture.owner).await;

    assert_eq!(listed.len(), 2);
    let enrolled = listed
        .iter()
        .find(|device| device.status == "enrolled")
        .expect("the enrolled device");
    assert_eq!(
        enrolled.node_id.as_deref(),
        Some(node(2).to_string()).as_deref()
    );
    assert!(enrolled.enrollment_id.is_none());
    let pending = listed
        .iter()
        .find(|device| device.status == "pending")
        .expect("the outstanding enrollment");
    assert_eq!(pending.enrollment_id.as_deref(), Some(pending.id.as_str()));
    assert!(pending.expires_at.is_some());

    let foreign = devices(&fixture.state, fixture.other).await;
    assert!(
        foreign
            .iter()
            .all(|device| device.id != node(2).to_string())
    );
}

#[tokio::test]
async fn revokes_pending_device() {
    let fixture = setup_devices().await;
    let pending = devices(&fixture.state, fixture.owner)
        .await
        .into_iter()
        .find(|device| device.status == "pending")
        .expect("the outstanding enrollment");

    let status = revoke_user_device(
        State(fixture.state.clone()),
        Extension(Some(auth(fixture.owner))),
        Path(pending.id.clone()),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::NO_CONTENT);
    let listed = devices(&fixture.state, fixture.owner).await;
    assert!(listed.iter().all(|device| device.status == "enrolled"));

    let repeated = revoke_user_device(
        State(fixture.state.clone()),
        Extension(Some(auth(fixture.owner))),
        Path(pending.id),
    )
    .await;
    assert!(matches!(repeated, Err(ServerError::NotFound)));

    // Another owner's outstanding enrollment stays untouched.
    assert!(
        devices(&fixture.state, fixture.other)
            .await
            .iter()
            .any(|device| device.status == "pending")
    );
}

#[tokio::test]
async fn rejects_stranger_device() {
    let fixture = setup_devices().await;

    let foreign = revoke_user_device(
        State(fixture.state.clone()),
        Extension(Some(auth(fixture.owner))),
        Path(node(3).to_string()),
    )
    .await;
    assert!(matches!(foreign, Err(ServerError::NotFound)));

    let anonymous = list_user_devices(State(fixture.state), Extension(None)).await;
    assert!(matches!(anonymous, Err(ServerError::Unauthorized)));
}

#[tokio::test]
async fn evicts_enrolled_device() {
    // Eviction drops the membership itself, so the device stops being a
    // realm peer instead of merely losing an unredeemed secret.
    let fixture = setup_devices().await;

    let status = revoke_user_device(
        State(fixture.state.clone()),
        Extension(Some(auth(fixture.owner))),
        Path(node(2).to_string()),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::NO_CONTENT);

    let listed = devices(&fixture.state, fixture.owner).await;
    assert!(listed.iter().all(|device| device.id != node(2).to_string()));

    let repeated = revoke_user_device(
        State(fixture.state.clone()),
        Extension(Some(auth(fixture.owner))),
        Path(node(2).to_string()),
    )
    .await;
    assert!(matches!(repeated, Err(ServerError::NotFound)));

    // Another owner's device keeps its membership.
    assert!(
        devices(&fixture.state, fixture.other)
            .await
            .iter()
            .any(|device| device.id == node(3).to_string())
    );
}

#[tokio::test]
async fn admin_evicts_device() {
    // A realm admin reaches a device it does not own, and the owner path is
    // left as it was for every other device.
    let fixture = setup_devices().await;

    let status = evict_device(
        State(fixture.state.clone()),
        Extension(Some(auth(fixture.admin))),
        Path(node(2).to_string()),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(
        devices(&fixture.state, fixture.owner)
            .await
            .iter()
            .all(|device| device.id != node(2).to_string())
    );

    let status = revoke_user_device(
        State(fixture.state.clone()),
        Extension(Some(auth(fixture.other))),
        Path(node(3).to_string()),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn refuses_non_admin() {
    // Owning a device is not administering the realm: the admin route
    // refuses a plain member and an anonymous caller, and the device stays.
    let fixture = setup_devices().await;

    assert!(matches!(
        evict_device(
            State(fixture.state.clone()),
            Extension(Some(auth(fixture.owner))),
            Path(node(2).to_string()),
        )
        .await,
        Err(ServerError::Forbidden)
    ));
    assert!(matches!(
        evict_device(
            State(fixture.state.clone()),
            Extension(None),
            Path(node(2).to_string()),
        )
        .await,
        Err(ServerError::Unauthorized)
    ));
    assert!(
        devices(&fixture.state, fixture.owner)
            .await
            .iter()
            .any(|device| device.id == node(2).to_string())
    );
}

#[tokio::test]
async fn admin_spares_management() {
    // The route reaches enrolled devices only, so the realm's own management
    // node is not removable through it.
    let fixture = setup_devices().await;

    assert!(matches!(
        evict_device(
            State(fixture.state.clone()),
            Extension(Some(auth(fixture.admin))),
            Path(node(1).to_string()),
        )
        .await,
        Err(ServerError::NotFound)
    ));
}

#[test]
fn decodes_dashboard_scope() {
    // Only the two documented values decode; anything else reads as unset.
    let scope = |value: &str| {
        preferences_from_attributes(&std::collections::HashMap::from([(
            "ui.dashboard_scope".to_string(),
            value.to_string(),
        )]))
        .dashboard_scope
    };
    assert_eq!(scope("personal").as_deref(), Some("personal"));
    assert_eq!(scope(" realm ").as_deref(), Some("realm"));
    assert_eq!(scope("everything"), None);
    assert_eq!(
        preferences_from_attributes(&std::collections::HashMap::new()).dashboard_scope,
        None
    );
}
