use super::{
    ServerError, bootstrap_onboarding, create_onboarding_secret, get_secret_status,
    list_onboarding_secrets, map_finalize_error, revoke_onboarding_secret,
};
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keys::generate_signing_key;
use aruna_core::keyspaces::{ADMIN_DOCUMENT_STATE_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::onboarding::{
    BootstrapOnboardingRequest, CreateSecretRequest, OnboardingMode, OnboardingPurpose,
    OnboardingSecret, OnboardingSecretRecord, OnboardingSecretState, RequestedOnboardingMode,
    issuer_proof_message, node_proof_message,
};
use aruna_core::reducer::AdminDocumentState;
use aruna_core::request_policy::{PolicyKind, RequestPolicy};
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities};
use aruna_core::structs::identity::realm::{
    RealmConfigDocument, RealmDiscoveryConfig, RealmId, RealmNodeKind, StaticRealmEndpoint,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::onboarding::create_secret::{CreateSecretInput, CreateSecretOperation};
use aruna_operations::onboarding::finalize_bootstrap::BootstrapFinalizeError;
use aruna_operations::onboarding::list_secrets::ListSecretsOperation;
use aruna_operations::onboarding::reserve_secret::{ReserveSecretInput, ReserveSecretOperation};
use aruna_operations::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_storage::storage;
use aruna_tasks::TaskHandle;
use axum::Extension;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use base64::Engine;
use crypto_box::{
    PublicKey as TransportPublicKey, SalsaBox, SecretKey as TransportSecretKey, aead::Aead,
};
use ed25519_dalek::{Signer, SigningKey};
use std::sync::Arc;
use tempfile::{TempDir, tempdir};
use ulid::Ulid;

async fn setup_management_state() -> (
    Arc<ServerState>,
    RealmId,
    iroh::PublicKey,
    UserId,
    NetHandle,
    TempDir,
) {
    let tempdir = tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
    let net_handle = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage_handle.clone(),
    )
    .await
    .unwrap();
    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: Some(net_handle.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    });

    let realm_signing_key = generate_signing_key();
    let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let node_id = net_handle.node_id();

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

    (state, realm_id, node_id, user_id, net_handle, tempdir)
}

#[test]
fn declares_dialable_members() {
    // Only configured, sync-eligible members other than the joiner.
    let realm_id = RealmId::from_bytes([9u8; 32]);
    let server = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
    let device = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
    let stranger = iroh::SecretKey::from_bytes(&[3u8; 32]).public();
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(server, RealmNodeKind::Server);
    config.ensure_node(
        device,
        RealmNodeKind::User {
            owner: UserId::nil(realm_id),
        },
    );
    let endpoint = |node: iroh::PublicKey| StaticRealmEndpoint {
        node_id: node.to_string(),
        endpoint_addr: format!("{node};ip:192.0.2.10:4433"),
    };
    config.discovery = RealmDiscoveryConfig::Static {
        endpoints: vec![endpoint(server), endpoint(device), endpoint(stranger)],
    };

    let declared = super::declared_endpoints(&config, &stranger.to_string());
    assert_eq!(declared, vec![endpoint(server)]);
    assert!(super::declared_endpoints(&config, &server.to_string()).is_empty());

    config.discovery = aruna_core::structs::identity::realm::default_discovery_config();
    assert!(super::declared_endpoints(&config, &stranger.to_string()).is_empty());
}

#[test]
fn placement_errors_badrequest() {
    assert!(matches!(
        map_finalize_error(BootstrapFinalizeError::ReservedNodeLabel(String::new())),
        ServerError::ReservedLabel(_)
    ));
    assert!(matches!(
        map_finalize_error(BootstrapFinalizeError::NodeLocationTooLong),
        ServerError::BadRequest
    ));
}

#[tokio::test]
async fn server_secret_consumed() {
    let (state, realm_id, seed_node_id, user_id, net_handle, _tempdir) =
        setup_management_state().await;
    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };

    let (_, Json(created)) = create_onboarding_secret(
        State(state.clone()),
        Extension(Some(auth)),
        Json(CreateSecretRequest {
            seed_url: "http://127.0.0.1:3000".to_string(),
            mode: RequestedOnboardingMode::Server,
            expires_in_seconds: Some(600),
        }),
    )
    .await
    .unwrap();

    let issuer_key = generate_signing_key();
    let issuer_public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(issuer_key.verifying_key().to_bytes());
    let onboarding_secret = created.onboarding_secret;
    let node_proof = SigningKey::from_bytes(&[9u8; 32]);
    let bootstrap_node_id = iroh::SecretKey::from_bytes(&node_proof.to_bytes()).public();
    let node_id = bootstrap_node_id.to_string();
    let node_signature = node_proof
        .sign(&node_proof_message(&onboarding_secret, &node_id, None))
        .to_string();
    let issuer_signature = issuer_key
        .sign(&issuer_proof_message(
            &onboarding_secret,
            &node_id,
            &issuer_public_key,
        ))
        .to_string();

    let (_, Json(bootstrap)) = bootstrap_onboarding(
        State(state.clone()),
        Json(BootstrapOnboardingRequest {
            onboarding_secret,
            node_id,
            node_proof: node_signature,
            transport_public_key: None,
            issuer_public_key: Some(issuer_public_key.clone()),
            issuer_proof: Some(issuer_signature),
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        }),
    )
    .await
    .unwrap();

    assert_eq!(bootstrap.mode, OnboardingMode::Server);
    assert_eq!(bootstrap.realm_id, realm_id.to_string());
    assert_eq!(bootstrap.temporary_bootstrap_endpoint.id, seed_node_id);
    assert!(bootstrap.wrapped_realm_private_key.is_none());
    assert!(bootstrap.delegation_signature.is_some());
    assert!(!bootstrap.onboarding_sync_ticket.is_empty());

    let config = match state
        .get_ctx()
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: byteview::ByteView::from(*realm_id.as_bytes()),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => RealmConfigDocument::from_bytes(&bytes).unwrap(),
        other => panic!("unexpected realm config read result: {other:?}"),
    };
    assert!(config.has_node(bootstrap_node_id));

    let reducer_state = match state
        .get_ctx()
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
            key: reducer_state_key(&AdminDocumentTarget::RealmConfig { realm_id }),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => postcard::from_bytes::<AdminDocumentState>(&bytes).unwrap(),
        other => panic!("unexpected realm config reducer state read result: {other:?}"),
    };
    assert_eq!(
        reducer_state.materialized_config_nodes()[&bootstrap_node_id],
        RealmNodeKind::Server
    );

    net_handle.shutdown().await;
}

#[tokio::test]
async fn mint_binds_owner() {
    // A device secret takes its owner from the credential, not the body.
    let (state, realm_id, _node_id, user_id, net_handle, _tempdir) = setup_management_state().await;
    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };

    let (_, Json(created)) = create_onboarding_secret(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Json(CreateSecretRequest {
            seed_url: "http://127.0.0.1:3000".to_string(),
            mode: RequestedOnboardingMode::User,
            expires_in_seconds: Some(600),
        }),
    )
    .await
    .unwrap();

    assert_eq!(created.mode, RequestedOnboardingMode::User);
    let secret = OnboardingSecret::decode(&created.onboarding_secret).unwrap();
    assert_eq!(secret.mode, OnboardingMode::User { owner: user_id });
    assert_eq!(created.enrollment_id, secret.enrollment_id.to_string());

    let (_, Json(listed)) = list_onboarding_secrets(State(state), Extension(Some(auth)))
        .await
        .unwrap();
    let owner = user_id.to_string();
    assert_eq!(listed.secrets[0].mode, "User");
    assert_eq!(listed.secrets[0].owner.as_deref(), Some(owner.as_str()));
    assert_eq!(listed.secrets[0].enrollment_id, created.enrollment_id);

    net_handle.shutdown().await;
}

#[tokio::test]
async fn policy_denies_enrollment() {
    // A realm deny policy on the enrollment operation stops a device mint
    // without touching the infrastructure modes.
    let (state, realm_id, _node_id, user_id, net_handle, _tempdir) = setup_management_state().await;
    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let deny = RequestPolicy {
        policy_id: Ulid::generate(),
        name: "no-devices".to_string(),
        kind: PolicyKind::Deny,
        when: None,
        expression: format!("operation == '{}'", super::ENROLL_DEVICE_OPERATION),
        enabled: true,
    };
    let mut config = read_realm_config(&state, realm_id).await;
    config.request_policies.push(deny);
    write_realm_config(&state, realm_id, &config).await;

    let request = |mode| CreateSecretRequest {
        seed_url: "http://127.0.0.1:3000".to_string(),
        mode,
        expires_in_seconds: Some(600),
    };
    let denied = create_onboarding_secret(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Json(request(RequestedOnboardingMode::User)),
    )
    .await;
    assert!(matches!(denied, Err(ServerError::Forbidden)));

    let (_, Json(_)) = create_onboarding_secret(
        State(state.clone()),
        Extension(Some(auth)),
        Json(request(RequestedOnboardingMode::Server)),
    )
    .await
    .expect("an infrastructure mint is untouched");

    net_handle.shutdown().await;
}

async fn read_realm_config(state: &Arc<ServerState>, realm_id: RealmId) -> RealmConfigDocument {
    match state
        .get_ctx()
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: byteview::ByteView::from(*realm_id.as_bytes()),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => RealmConfigDocument::from_bytes(&bytes).unwrap(),
        other => panic!("unexpected realm config read result: {other:?}"),
    }
}

async fn write_realm_config(
    state: &Arc<ServerState>,
    realm_id: RealmId,
    config: &RealmConfigDocument,
) {
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    state
        .get_ctx()
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: byteview::ByteView::from(*realm_id.as_bytes()),
            value: byteview::ByteView::from(config.to_bytes(&actor).unwrap()),
            txn_id: None,
        }))
        .await;
}

#[tokio::test]
async fn builds_enroll_url() {
    // The deep link is a contract: aruna://enroll?secret=&seed=&realm=.
    let (state, realm_id, _node_id, user_id, net_handle, _tempdir) = setup_management_state().await;
    state
        .register_rest_public(
            "0.0.0.0:3000".parse().unwrap(),
            Some("https://node.example.test"),
        )
        .await;
    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };

    let (_, Json(created)) = create_onboarding_secret(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Json(CreateSecretRequest {
            seed_url: String::new(),
            mode: RequestedOnboardingMode::User,
            expires_in_seconds: Some(600),
        }),
    )
    .await
    .unwrap();

    let enroll =
        url::Url::parse(&created.enroll_url.expect("device mint carries a deep link")).unwrap();
    assert_eq!(enroll.scheme(), "aruna");
    assert_eq!(enroll.host_str(), Some("enroll"));
    let query = enroll
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(query["secret"], created.onboarding_secret);
    assert_eq!(query["seed"], "https://node.example.test");
    assert_eq!(query["realm"], realm_id.to_string());

    let secret = OnboardingSecret::decode(&created.onboarding_secret).unwrap();
    assert_eq!(
        secret.seed_url, query["seed"],
        "the link names the callback"
    );

    let (_, Json(server)) = create_onboarding_secret(
        State(state),
        Extension(Some(auth)),
        Json(CreateSecretRequest {
            seed_url: "http://127.0.0.1:3000".to_string(),
            mode: RequestedOnboardingMode::Server,
            expires_in_seconds: Some(600),
        }),
    )
    .await
    .unwrap();
    assert!(server.enroll_url.is_none());

    net_handle.shutdown().await;
}

#[tokio::test]
async fn polls_secret_status() {
    // The owner may watch its own secret; a stranger is told nothing.
    let (state, realm_id, _node_id, user_id, net_handle, _tempdir) = setup_management_state().await;
    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };

    let (_, Json(created)) = create_onboarding_secret(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Json(CreateSecretRequest {
            seed_url: "http://127.0.0.1:3000".to_string(),
            mode: RequestedOnboardingMode::User,
            expires_in_seconds: Some(600),
        }),
    )
    .await
    .unwrap();
    let secret = OnboardingSecret::decode(&created.onboarding_secret).unwrap();
    // The id the mint returned is the handle the status route takes.
    let enrollment_id = created.enrollment_id.clone();
    assert_eq!(enrollment_id, secret.enrollment_id.to_string());

    let (_, Json(status)) = get_secret_status(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Path(enrollment_id.clone()),
    )
    .await
    .unwrap();
    assert_eq!(status.status, "pending");
    assert_eq!(status.mode, "User");
    assert_eq!(status.owner.as_deref(), Some(user_id.to_string().as_str()));
    assert!(status.claimed_node_id.is_none());

    let stranger = get_secret_status(
        State(state.clone()),
        Extension(Some(AuthContext {
            user_id: UserId::local(Ulid::generate(), realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        })),
        Path(enrollment_id.clone()),
    )
    .await;
    // A stranger cannot tell a foreign secret from an unknown one.
    assert!(matches!(stranger, Err(ServerError::NotFound)));

    drive(
        ReserveSecretOperation::new(ReserveSecretInput {
            enrollment_id: secret.enrollment_id,
            secret_hash: secret.secret_hash(),
            node_id: "device-a".to_string(),
            now: 1,
            reservation_expires_at: u64::MAX,
            finalizing: true,
        }),
        &state.get_ctx(),
    )
    .await
    .unwrap();

    let (_, Json(status)) = get_secret_status(
        State(state.clone()),
        Extension(Some(auth)),
        Path(enrollment_id),
    )
    .await
    .unwrap();
    assert_eq!(status.status, "claimed");
    assert_eq!(status.claimed_node_id.as_deref(), Some("device-a"));

    let missing = get_secret_status(
        State(state),
        Extension(Some(AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        })),
        Path(Ulid::generate().to_string()),
    )
    .await;
    assert!(matches!(missing, Err(ServerError::NotFound)));

    net_handle.shutdown().await;
}

#[tokio::test]
async fn enrolls_user_device() {
    // A device joins with no issuer key and lands as an owner-bound member.
    let (state, realm_id, _seed, user_id, net_handle, _tempdir) = setup_management_state().await;
    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };

    let (_, Json(created)) = create_onboarding_secret(
        State(state.clone()),
        Extension(Some(auth)),
        Json(CreateSecretRequest {
            seed_url: "http://127.0.0.1:3000".to_string(),
            mode: RequestedOnboardingMode::User,
            expires_in_seconds: Some(600),
        }),
    )
    .await
    .unwrap();

    let device_key = SigningKey::from_bytes(&[23u8; 32]);
    let device_node_id = iroh::SecretKey::from_bytes(&device_key.to_bytes()).public();
    let node_id = device_node_id.to_string();
    let node_proof = device_key
        .sign(&node_proof_message(
            &created.onboarding_secret,
            &node_id,
            None,
        ))
        .to_string();

    let (_, Json(bootstrap)) = bootstrap_onboarding(
        State(state.clone()),
        Json(BootstrapOnboardingRequest {
            onboarding_secret: created.onboarding_secret,
            node_id,
            node_proof,
            transport_public_key: None,
            issuer_public_key: None,
            issuer_proof: None,
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        }),
    )
    .await
    .unwrap();

    assert_eq!(bootstrap.mode, OnboardingMode::User { owner: user_id });
    assert!(bootstrap.wrapped_realm_private_key.is_none());
    assert!(bootstrap.delegation_signature.is_none());

    let config = match state
        .get_ctx()
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: byteview::ByteView::from(*realm_id.as_bytes()),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => RealmConfigDocument::from_bytes(&bytes).unwrap(),
        other => panic!("unexpected realm config read result: {other:?}"),
    };
    let device = config
        .nodes
        .iter()
        .find(|node| node.node_id == device_node_id.to_string())
        .expect("device joined the realm configuration");
    assert_eq!(device.kind, RealmNodeKind::User { owner: user_id });
    assert!(!device.kind.is_sync_eligible());

    net_handle.shutdown().await;
}

#[tokio::test]
async fn mint_rejects_stranger() {
    // Self-service enrollment is still realm-scoped and never anonymous.
    let (state, _realm_id, _node_id, user_id, net_handle, _tempdir) =
        setup_management_state().await;
    let request = || CreateSecretRequest {
        seed_url: "http://127.0.0.1:3000".to_string(),
        mode: RequestedOnboardingMode::User,
        expires_in_seconds: Some(600),
    };

    let anonymous =
        create_onboarding_secret(State(state.clone()), Extension(None), Json(request())).await;
    assert!(matches!(anonymous, Err(ServerError::Unauthorized)));

    let foreign = create_onboarding_secret(
        State(state),
        Extension(Some(AuthContext {
            user_id,
            realm_id: RealmId::from_bytes([31u8; 32]),
            path_restrictions: None,
            session: None,
        })),
        Json(request()),
    )
    .await;
    assert!(matches!(foreign, Err(ServerError::Forbidden)));

    net_handle.shutdown().await;
}

#[tokio::test]
async fn mint_rejects_restricted() {
    // A device carries its owner's whole identity, so a path-restricted
    // token may not enroll one.
    let (state, realm_id, _node_id, user_id, net_handle, _tempdir) = setup_management_state().await;

    let restricted = create_onboarding_secret(
        State(state),
        Extension(Some(AuthContext {
            user_id,
            realm_id,
            path_restrictions: Some(Vec::new()),
            session: None,
        })),
        Json(CreateSecretRequest {
            seed_url: "http://127.0.0.1:3000".to_string(),
            mode: RequestedOnboardingMode::User,
            expires_in_seconds: Some(600),
        }),
    )
    .await;
    assert!(matches!(restricted, Err(ServerError::Forbidden)));

    net_handle.shutdown().await;
}

#[tokio::test]
async fn bootstrap_rejects_secret() {
    // An initial-administrator secret must not onboard a node or be consumed.
    let (state, realm_id, _seed, _user_id, net_handle, _tempdir) = setup_management_state().await;

    let enrollment_id = Ulid::generate();
    let secret = OnboardingSecret {
        seed_url: "http://127.0.0.1:3000".to_string(),
        enrollment_id,
        secret: [11u8; 32],
        mode: OnboardingMode::Server,
        realm_id,
        purpose: OnboardingPurpose::InitialAdministrator,
    };
    drive(
        CreateSecretOperation::new(CreateSecretInput {
            record: OnboardingSecretRecord {
                enrollment_id,
                secret_hash: secret.secret_hash(),
                mode: OnboardingMode::Server,
                purpose: OnboardingPurpose::InitialAdministrator,
                expires_at: u64::MAX,
                claimed_node_id: None,
            },
        }),
        &state.get_ctx(),
    )
    .await
    .unwrap();

    let encoded = secret.encode().unwrap();
    let node_proof = SigningKey::from_bytes(&[9u8; 32]);
    let bootstrap_node_id = iroh::SecretKey::from_bytes(&node_proof.to_bytes()).public();
    let node_id = bootstrap_node_id.to_string();
    let node_signature = node_proof
        .sign(&node_proof_message(&encoded, &node_id, None))
        .to_string();

    let result = bootstrap_onboarding(
        State(state.clone()),
        Json(BootstrapOnboardingRequest {
            onboarding_secret: encoded,
            node_id,
            node_proof: node_signature,
            transport_public_key: None,
            issuer_public_key: None,
            issuer_proof: None,
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        }),
    )
    .await;
    assert!(matches!(result, Err(ServerError::Forbidden)));

    let entries = drive(ListSecretsOperation::new(), &state.get_ctx())
        .await
        .unwrap();
    let entry = entries
        .iter()
        .find(|entry| entry.record.enrollment_id == enrollment_id)
        .expect("secret still present");
    assert!(matches!(entry.state, OnboardingSecretState::Available));

    net_handle.shutdown().await;
}

#[tokio::test]
async fn secrets_list_revoke() {
    let (state, realm_id, _node_id, user_id, net_handle, _tempdir) = setup_management_state().await;
    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };

    let (_, Json(created)) = create_onboarding_secret(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Json(CreateSecretRequest {
            seed_url: "http://127.0.0.1:3000".to_string(),
            mode: RequestedOnboardingMode::Server,
            expires_in_seconds: Some(600),
        }),
    )
    .await
    .unwrap();

    let (_, Json(listed)) =
        list_onboarding_secrets(State(state.clone()), Extension(Some(auth.clone())))
            .await
            .unwrap();
    assert_eq!(listed.secrets.len(), 1);

    assert_eq!(listed.secrets[0].enrollment_id, created.enrollment_id);
    let status = revoke_onboarding_secret(
        State(state.clone()),
        Extension(Some(auth)),
        Path(created.enrollment_id),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, Json(listed)) = list_onboarding_secrets(
        State(state),
        Extension(Some(AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        })),
    )
    .await
    .unwrap();
    assert!(listed.secrets.is_empty());

    net_handle.shutdown().await;
}

#[tokio::test]
async fn secret_pruning_correct() {
    let (state, realm_id, _node_id, user_id, net_handle, _tempdir) = setup_management_state().await;
    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };

    let finalizing_id = Ulid::generate();
    drive(
        CreateSecretOperation::new(CreateSecretInput {
            record: OnboardingSecretRecord {
                enrollment_id: finalizing_id,
                secret_hash: "finalizing".to_string(),
                mode: OnboardingMode::Server,
                purpose: OnboardingPurpose::NodeEnrollment,
                expires_at: 1,
                claimed_node_id: None,
            },
        }),
        &state.get_ctx(),
    )
    .await
    .unwrap();
    drive(
        ReserveSecretOperation::new(ReserveSecretInput {
            enrollment_id: finalizing_id,
            secret_hash: "finalizing".to_string(),
            node_id: "node-a".to_string(),
            now: 1,
            reservation_expires_at: 2,
            finalizing: true,
        }),
        &state.get_ctx(),
    )
    .await
    .unwrap();

    let stale_id = Ulid::generate();
    drive(
        CreateSecretOperation::new(CreateSecretInput {
            record: OnboardingSecretRecord {
                enrollment_id: stale_id,
                secret_hash: "stale".to_string(),
                mode: OnboardingMode::Server,
                purpose: OnboardingPurpose::NodeEnrollment,
                expires_at: 1,
                claimed_node_id: None,
            },
        }),
        &state.get_ctx(),
    )
    .await
    .unwrap();

    let (_, Json(listed)) = list_onboarding_secrets(State(state.clone()), Extension(Some(auth)))
        .await
        .unwrap();
    assert_eq!(listed.secrets.len(), 1);
    assert_eq!(listed.secrets[0].enrollment_id, finalizing_id.to_string());
    assert_eq!(listed.secrets[0].claimed_node_id.as_deref(), Some("node-a"));

    let entries = drive(ListSecretsOperation::new(), &state.get_ctx())
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].record.enrollment_id, finalizing_id);

    net_handle.shutdown().await;
}

#[tokio::test]
async fn invalid_proof_preserves() {
    let (state, realm_id, _seed_node_id, user_id, net_handle, _tempdir) =
        setup_management_state().await;
    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };

    let (_, Json(created)) = create_onboarding_secret(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Json(CreateSecretRequest {
            seed_url: "http://127.0.0.1:3000".to_string(),
            mode: RequestedOnboardingMode::Server,
            expires_in_seconds: Some(600),
        }),
    )
    .await
    .unwrap();

    let node_proof = SigningKey::from_bytes(&[5u8; 32]);
    let joiner_node_id = iroh::SecretKey::from_bytes(&node_proof.to_bytes()).public();
    let joiner_node_id_string = joiner_node_id.to_string();
    let node_signature = node_proof
        .sign(&node_proof_message(
            &created.onboarding_secret,
            &joiner_node_id_string,
            None,
        ))
        .to_string();

    let issuer_key = generate_signing_key();
    let issuer_public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(issuer_key.verifying_key().to_bytes());
    let onboarding_secret = created.onboarding_secret;

    let result = bootstrap_onboarding(
        State(state.clone()),
        Json(BootstrapOnboardingRequest {
            onboarding_secret: onboarding_secret.clone(),
            node_id: joiner_node_id_string.clone(),
            node_proof: node_signature.clone(),
            transport_public_key: None,
            issuer_public_key: Some(issuer_public_key.clone()),
            issuer_proof: Some("invalid-signature".to_string()),
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        }),
    )
    .await;
    assert!(matches!(result, Err(ServerError::Unauthorized)));

    let (_, Json(listed)) = list_onboarding_secrets(State(state.clone()), Extension(Some(auth)))
        .await
        .unwrap();
    assert_eq!(listed.secrets.len(), 1);

    let issuer_signature = issuer_key
        .sign(&issuer_proof_message(
            &onboarding_secret,
            &joiner_node_id_string,
            &issuer_public_key,
        ))
        .to_string();
    let result = bootstrap_onboarding(
        State(state),
        Json(BootstrapOnboardingRequest {
            onboarding_secret,
            node_id: joiner_node_id_string,
            node_proof: node_signature,
            transport_public_key: None,
            issuer_public_key: Some(issuer_public_key),
            issuer_proof: Some(issuer_signature),
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        }),
    )
    .await;
    assert!(result.is_ok());

    net_handle.shutdown().await;
}

#[tokio::test]
async fn bootstrap_wraps_key() {
    let (state, realm_id, _seed_node_id, user_id, net_handle, _tempdir) =
        setup_management_state().await;
    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };

    let (_, Json(created)) = create_onboarding_secret(
        State(state.clone()),
        Extension(Some(auth)),
        Json(CreateSecretRequest {
            seed_url: "http://127.0.0.1:3000".to_string(),
            mode: RequestedOnboardingMode::Management,
            expires_in_seconds: Some(600),
        }),
    )
    .await
    .unwrap();

    let joiner_node_key = SigningKey::from_bytes(&[11u8; 32]);
    let joiner_node_id = iroh::SecretKey::from_bytes(&joiner_node_key.to_bytes()).public();
    let joiner_node_id_string = joiner_node_id.to_string();
    let transport_secret_key = TransportSecretKey::generate(&mut crypto_box::aead::OsRng);
    let transport_public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(transport_secret_key.public_key().as_bytes());
    let node_signature = joiner_node_key
        .sign(&node_proof_message(
            &created.onboarding_secret,
            &joiner_node_id_string,
            Some(&transport_public_key),
        ))
        .to_string();

    let (_, Json(bootstrap)) = bootstrap_onboarding(
        State(state),
        Json(BootstrapOnboardingRequest {
            onboarding_secret: created.onboarding_secret,
            node_id: joiner_node_id_string,
            node_proof: node_signature,
            transport_public_key: Some(transport_public_key),
            issuer_public_key: None,
            issuer_proof: None,
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        }),
    )
    .await
    .unwrap();

    let sender_public_key = TransportPublicKey::from(
        <[u8; 32]>::try_from(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(bootstrap.wrapping_public_key.unwrap())
                .unwrap()
                .as_slice(),
        )
        .unwrap(),
    );
    let cipher = SalsaBox::new(&sender_public_key, &transport_secret_key);
    let nonce_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(bootstrap.wrapped_realm_private_key_nonce.unwrap())
        .unwrap();
    let ciphertext = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(bootstrap.wrapped_realm_private_key.unwrap())
        .unwrap();
    let nonce = crypto_box::Nonce::from(<[u8; 24]>::try_from(nonce_bytes.as_slice()).unwrap());
    let plaintext = cipher.decrypt(&nonce, ciphertext.as_ref()).unwrap();
    let pem = String::from_utf8(plaintext).unwrap();
    assert!(pem.contains("BEGIN PRIVATE KEY"));

    net_handle.shutdown().await;
}
