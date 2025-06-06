use std::sync::Arc;

use aruna_permission::paths::RealmKey;
use aruna_permission::token::{Ed25519KeyPair, OidcTrustConfig, TokenSystem};
use aruna_permission::*;
use aruna_storage::storage::{
    lmdb::{LmdbConfig, LmdbStore},
    store::Store,
};
use ulid::Ulid;

// Include RSA test keys for OIDC providers
const GOOGLE_PUBLIC_KEY: &str = include_str!("../tests/test_google_priv.pem.pub");
const GITHUB_PUBLIC_KEY: &str = include_str!("../tests/test_github_priv.pem.pub");

async fn setup_test_systems() -> (
    TokenSystem,
    PermissionManager,
    Arc<LmdbStore>,
    String,
    Ed25519KeyPair,
) {
    let test_id = Ulid::new().to_string();
    let test_dir = format!("/dev/shm/test_perm_{}", test_id);
    std::fs::create_dir_all(&test_dir).unwrap();

    let config = LmdbConfig {
        path: test_dir.clone(),
        databases: vec![
            aruna_permission::DBNAME,
            aruna_permission::RESOURCE_DB,
            aruna_permission::OIDC_IDENTITIES_DB,
            aruna_permission::IDENTITY_PERMISSIONS_DB,
        ],
    };

    let store = Arc::new(LmdbStore::new(config).unwrap());

    // Create token system for realm A with trusted OIDC issuers
    let realm_a = create_test_realm_key(10);
    let oidc_trust_config = OidcTrustConfig::TrustedIssuers(vec![
        "https://accounts.google.com".to_string(),
        "https://github.com/login/oauth".to_string(),
    ]);
    let mut token_system = TokenSystem::new(realm_a, oidc_trust_config);

    // Add RSA public keys for OIDC verification
    token_system.add_oidc_public_key(
        "https://accounts.google.com".to_string(),
        GOOGLE_PUBLIC_KEY.to_string(),
    );
    token_system.add_oidc_public_key(
        "https://github.com/login/oauth".to_string(),
        GITHUB_PUBLIC_KEY.to_string(),
    );

    // Generate Ed25519 key pair for realm tokens
    let realm_key_pair = Ed25519KeyPair::generate();
    let verifying_key_pem = realm_key_pair.verifying_key_pem().unwrap();
    token_system.add_realm_public_key(realm_a, verifying_key_pem);

    // Create permission manager
    let permission_manager = PermissionManager::new().await.unwrap();

    (
        token_system,
        permission_manager,
        store,
        test_dir,
        realm_key_pair,
    )
}

fn create_test_ulid(suffix: u8) -> Ulid {
    let mut bytes = [0u8; 16];
    bytes[15] = suffix;
    Ulid::from_bytes(bytes)
}

fn create_test_realm_key(suffix: u8) -> RealmKey {
    let mut key = [0u8; 32];
    key[31] = suffix;
    key
}

pub async fn setup_test_store() -> (Arc<LmdbStore>, String) {
    let test_id = Ulid::new().to_string();
    let test_dir = format!("/dev/shm/test_perm_{}", test_id);
    std::fs::create_dir_all(&test_dir).unwrap();

    let config = LmdbConfig {
        path: test_dir.clone(),
        databases: vec![
            aruna_permission::DBNAME,
            aruna_permission::RESOURCE_DB,
            aruna_permission::OIDC_IDENTITIES_DB,
            aruna_permission::IDENTITY_PERMISSIONS_DB,
        ],
    };

    let store = Arc::new(LmdbStore::new(config).unwrap());
    (store, test_dir)
}

pub fn cleanup_test_dir(path: &str) {
    std::fs::remove_dir_all(path).unwrap_or(());
}

#[tokio::test]
async fn test_clean_separation_workflow() {
    let (token_system, permission_manager, store, test_dir, _realm_keys) =
        setup_test_systems().await;
    let mut txn = store.create_txn(true).unwrap();

    // Step 1: Token system handles identity registration from OIDC claims
    let alice_identity = token_system
        .register_user_from_oidc_claims(
            "https://accounts.google.com",
            "alice@example.com",
            store.as_ref(),
            &mut txn,
        )
        .unwrap();

    let bob_identity = token_system
        .register_user_from_oidc_claims(
            "https://github.com/login/oauth",
            "bob_dev",
            store.as_ref(),
            &mut txn,
        )
        .unwrap();

    // Step 2: Permission system handles group creation using identities
    let group_id = create_test_ulid(20);

    permission_manager
        .create_group(
            group_id,
            &alice_identity,
            alice_identity.realm_ulid,
            store.as_ref(),
            &mut txn,
        )
        .await
        .unwrap();

    // Step 3: Add Bob to Alice's group through permission system
    permission_manager
        .add_user(group_id, &bob_identity, "member", store.as_ref(), &mut txn)
        .await
        .unwrap();

    // Step 4: Create resource and test access control
    let resource_id = create_test_ulid(30);
    let resource_path = Path::builder()
        .realm_id(alice_identity.realm_ulid)
        .group_metadata(group_id, resource_id, vec![])
        .build()
        .unwrap();

    permission_manager
        .add_resource(
            ResourceId::Ulid(resource_id),
            &resource_path,
            store.as_ref(),
            &mut txn,
        )
        .unwrap();

    // Verify both users have access to the shared resource
    let alice_access = permission_manager
        .check_permission(
            &alice_identity,
            ResourceId::Ulid(resource_id),
            Action::Write,
            store.clone(),
        )
        .await;
    let bob_access = permission_manager
        .check_permission(
            &bob_identity,
            ResourceId::Ulid(resource_id),
            Action::Write,
            store.clone(),
        )
        .await;

    assert!(alice_access.is_ok());
    assert!(bob_access.is_ok());

    txn.commit().unwrap();
    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_cross_realm_token_handling() {
    let realm_a = create_test_realm_key(10);
    let realm_b = create_test_realm_key(11);

    let oidc_trust_config_a =
        OidcTrustConfig::TrustedIssuers(vec!["https://accounts.google.com".to_string()]);
    let oidc_trust_config_b =
        OidcTrustConfig::TrustedIssuers(vec!["https://accounts.google.com".to_string()]);

    let mut token_system_a = TokenSystem::new(realm_a, oidc_trust_config_a);
    let mut token_system_b = TokenSystem::new(realm_b, oidc_trust_config_b);
    let permission_manager = PermissionManager::new().await.unwrap();

    let (store, test_dir) = setup_test_store().await;
    let mut txn = store.create_txn(true).unwrap();

    // Configure OIDC public keys for both realms
    token_system_a.add_oidc_public_key(
        "https://accounts.google.com".to_string(),
        GOOGLE_PUBLIC_KEY.to_string(),
    );
    token_system_b.add_oidc_public_key(
        "https://accounts.google.com".to_string(),
        GOOGLE_PUBLIC_KEY.to_string(),
    );

    // Generate Ed25519 key pairs for both realms
    let realm_a_keys = Ed25519KeyPair::generate();
    let realm_b_keys = Ed25519KeyPair::generate();

    let realm_a_verifying_pem = realm_a_keys.verifying_key_pem().unwrap();
    let realm_b_verifying_pem = realm_b_keys.verifying_key_pem().unwrap();

    token_system_a.add_realm_public_key(realm_a, realm_a_verifying_pem.clone());
    token_system_b.add_realm_public_key(realm_b, realm_b_verifying_pem.clone());

    // Enable cross-realm verification by sharing public keys
    token_system_a.add_realm_public_key(realm_b, realm_b_verifying_pem);
    token_system_b.add_realm_public_key(realm_a, realm_a_verifying_pem);

    // Same OIDC user gets separate identities in each realm
    let alice_provider = "https://accounts.google.com";
    let alice_sub = "alice@example.com";

    let alice_realm_a = token_system_a
        .register_user_from_oidc_claims(alice_provider, alice_sub, store.as_ref(), &mut txn)
        .unwrap();

    let alice_realm_b = token_system_b
        .register_user_from_oidc_claims(alice_provider, alice_sub, store.as_ref(), &mut txn)
        .unwrap();

    assert_eq!(alice_realm_a.realm_ulid, realm_a);
    assert_eq!(alice_realm_b.realm_ulid, realm_b);

    // Create separate groups and resources in each realm
    let group_a = create_test_ulid(20);
    let group_b = create_test_ulid(21);

    permission_manager
        .create_group(group_a, &alice_realm_a, realm_a, store.as_ref(), &mut txn)
        .await
        .unwrap();
    permission_manager
        .create_group(group_b, &alice_realm_b, realm_b, store.as_ref(), &mut txn)
        .await
        .unwrap();

    // Unify identities across realms at permission level
    permission_manager
        .unify_identities(&alice_realm_a, &alice_realm_b, store.as_ref(), &mut txn)
        .await
        .unwrap();

    let resource_a = create_test_ulid(30);
    let resource_b = create_test_ulid(31);

    let path_a = Path::builder()
        .realm_id(realm_a)
        .group_metadata(group_a, resource_a, vec![])
        .build()
        .unwrap();

    let path_b = Path::builder()
        .realm_id(realm_b)
        .group_metadata(group_b, resource_b, vec![])
        .build()
        .unwrap();

    permission_manager
        .add_resource(ResourceId::Ulid(resource_a), &path_a, store.as_ref(), &mut txn)
        .unwrap();
    permission_manager
        .add_resource(ResourceId::Ulid(resource_b), &path_b, store.as_ref(), &mut txn)
        .unwrap();

    // Verify cross-realm access works after unification
    assert!(
        permission_manager
            .check_permission(
                &alice_realm_a,
                ResourceId::Ulid(resource_b),
                Action::Write,
                store.clone(),
            )
            .await
            .is_ok()
    );
    assert!(
        permission_manager
            .check_permission(
                &alice_realm_b,
                ResourceId::Ulid(resource_a),
                Action::Write,
                store.clone(),
            )
            .await
            .is_ok()
    );

    // Test Aruna realm token generation for both realms
    let realm_a_signing_pem = realm_a_keys.signing_key_pem().unwrap();
    let realm_b_signing_pem = realm_b_keys.signing_key_pem().unwrap();

    let alice_realm_a_token = token_system_a
        .generate_token(&alice_realm_a, &realm_a_signing_pem)
        .unwrap();
    let alice_realm_b_token = token_system_b
        .generate_token(&alice_realm_b, &realm_b_signing_pem)
        .unwrap();

    // Verify tokens can be validated in their respective realms
    let verified_identity_a = token_system_a
        .get_identity(&alice_realm_a_token, store.as_ref(), &mut txn)
        .unwrap();
    let verified_identity_b = token_system_b
        .get_identity(&alice_realm_b_token, store.as_ref(), &mut txn)
        .unwrap();

    assert_eq!(verified_identity_a.user_ulid, alice_realm_a.user_ulid);
    assert_eq!(verified_identity_b.user_ulid, alice_realm_b.user_ulid);

    txn.commit().unwrap();
    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_token_identity_extraction() {
    let realm_ulid = create_test_realm_key(10);
    let oidc_trust_config =
        OidcTrustConfig::TrustedIssuers(vec!["https://login.microsoftonline.com".to_string()]);
    let mut token_system = TokenSystem::new(realm_ulid, oidc_trust_config);

    let (store, test_dir) = setup_test_store().await;
    let mut txn = store.create_txn(true).unwrap();

    // For testing purposes, we'll use the Google key as Microsoft since we don't have a separate one
    token_system.add_oidc_public_key(
        "https://login.microsoftonline.com".to_string(),
        GOOGLE_PUBLIC_KEY.to_string(),
    );

    // Generate Ed25519 key pair for realm tokens
    let realm_keys = Ed25519KeyPair::generate();
    let verifying_pem = realm_keys.verifying_key_pem().unwrap();
    token_system.add_realm_public_key(realm_ulid, verifying_pem);

    // Test 1: First-time OIDC registration creates new identity
    let identity_from_oidc = token_system
        .register_user_from_oidc_claims(
            "https://login.microsoftonline.com",
            "user@company.com",
            store.as_ref(),
            &mut txn,
        )
        .unwrap();

    assert_eq!(identity_from_oidc.realm_ulid, realm_ulid);

    // Test 2: Subsequent login with same OIDC returns existing identity
    let identity_second_login = token_system
        .register_user_from_oidc_claims(
            "https://login.microsoftonline.com",
            "user@company.com",
            store.as_ref(),
            &mut txn,
        )
        .unwrap();

    assert_eq!(
        identity_from_oidc.user_ulid,
        identity_second_login.user_ulid
    );

    // Test 3: Different OIDC subjects create distinct identities
    let different_user_identity = token_system
        .register_user_from_oidc_claims(
            "https://login.microsoftonline.com",
            "different_user",
            store.as_ref(),
            &mut txn,
        )
        .unwrap();

    assert_ne!(
        identity_from_oidc.user_ulid,
        different_user_identity.user_ulid
    );
    assert_eq!(different_user_identity.realm_ulid, realm_ulid);

    // Test 4: Aruna realm token round-trip
    let signing_pem = realm_keys.signing_key_pem().unwrap();
    let aruna_token = token_system
        .generate_token(&identity_from_oidc, &signing_pem)
        .unwrap();

    let parsed_identity = token_system
        .get_identity(&aruna_token, store.as_ref(), &mut txn)
        .unwrap();

    assert_eq!(parsed_identity.user_ulid, identity_from_oidc.user_ulid);
    assert_eq!(parsed_identity.realm_ulid, identity_from_oidc.realm_ulid);

    txn.commit().unwrap();
    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_typical_usage_pattern() {
    let realm_ulid = create_test_realm_key(10);
    let oidc_trust_config =
        OidcTrustConfig::TrustedIssuers(vec!["https://accounts.google.com".to_string()]);
    let mut token_system = TokenSystem::new(realm_ulid, oidc_trust_config);
    let permission_manager = PermissionManager::new().await.unwrap();

    let (store, test_dir) = setup_test_store().await;
    let mut txn = store.create_txn(true).unwrap();

    // Configure OIDC and realm keys
    token_system.add_oidc_public_key(
        "https://accounts.google.com".to_string(),
        GOOGLE_PUBLIC_KEY.to_string(),
    );

    let realm_keys = Ed25519KeyPair::generate();
    let verifying_pem = realm_keys.verifying_key_pem().unwrap();
    token_system.add_realm_public_key(realm_ulid, verifying_pem);

    // === Token System Usage ===

    // 1. User authenticates via OIDC and gets identity
    let user_identity = token_system
        .register_user_from_oidc_claims(
            "https://accounts.google.com",
            "user@example.com",
            store.as_ref(),
            &mut txn,
        )
        .unwrap();

    // === Permission System Usage ===

    // 2. Use identity for permission operations
    let group_id = create_test_ulid(20);
    permission_manager
        .create_group(group_id, &user_identity, realm_ulid, store.as_ref(), &mut txn)
        .await
        .unwrap();

    // 3. Add resources
    let doc_id = create_test_ulid(30);
    let doc_path = Path::builder()
        .realm_id(realm_ulid)
        .group_metadata(group_id, doc_id, vec![])
        .build()
        .unwrap();

    permission_manager
        .add_resource(ResourceId::Ulid(doc_id), &doc_path, store.as_ref(), &mut txn)
        .unwrap();

    // 4. Check permissions
    let access_result = permission_manager
        .check_permission(
            &user_identity,
            ResourceId::Ulid(doc_id),
            Action::Write,
            store.clone(),
        )
        .await;

    assert!(access_result.is_ok());

    // 5. Generate Aruna token for API access
    let signing_pem = realm_keys.signing_key_pem().unwrap();
    let aruna_token = token_system
        .generate_token(&user_identity, &signing_pem)
        .unwrap();

    // 6. Token verification workflow
    let verified_identity = token_system
        .get_identity(&aruna_token, store.as_ref(), &mut txn)
        .unwrap();

    assert_eq!(verified_identity.user_ulid, user_identity.user_ulid);
    assert_eq!(verified_identity.realm_ulid, user_identity.realm_ulid);

    // === Cross-realm scenario ===

    // 7. Same user in different realm
    let other_realm = create_test_realm_key(11);
    let other_oidc_trust_config =
        OidcTrustConfig::TrustedIssuers(vec!["https://accounts.google.com".to_string()]);
    let mut other_token_system = TokenSystem::new(other_realm, other_oidc_trust_config);
    other_token_system.add_oidc_public_key(
        "https://accounts.google.com".to_string(),
        GOOGLE_PUBLIC_KEY.to_string(),
    );

    let other_identity = other_token_system
        .register_user_from_oidc_claims(
            "https://accounts.google.com",
            "user@example.com",
            store.as_ref(),
            &mut txn,
        )
        .unwrap();

    assert_eq!(other_identity.realm_ulid, other_realm);

    // 8. Unify identities across realms for seamless access
    permission_manager
        .unify_identities(&user_identity, &other_identity, store.as_ref(), &mut txn)
        .await
        .unwrap();

    txn.commit().unwrap();
    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_oidc_lookup_operations() {
    let realm_ulid = create_test_realm_key(10);
    let oidc_trust_config = OidcTrustConfig::TrustAll;
    let token_system = TokenSystem::new(realm_ulid, oidc_trust_config);

    let (store, test_dir) = setup_test_store().await;
    let mut txn = store.create_txn(true).unwrap();

    let user_ulid = create_test_ulid(42);
    let provider = "https://auth.example.com";
    let subject = "test_user_123";

    // Initially no OIDC mapping exists
    let lookup_result = token_system
        .get_user_from_oidc(provider, subject, store.as_ref(), &txn)
        .unwrap();
    assert!(lookup_result.is_none());

    // Add OIDC identity mapping
    token_system
        .add_oidc_identity(provider, subject, user_ulid, store.as_ref(), &mut txn)
        .unwrap();

    // Lookup should now return the mapped user ULID
    let lookup_result = token_system
        .get_user_from_oidc(provider, subject, store.as_ref(), &txn)
        .unwrap();
    assert_eq!(lookup_result, Some(user_ulid));

    // Verify isolation: different provider/subject combinations return None
    let different_provider_result = token_system
        .get_user_from_oidc("https://other.com", subject, store.as_ref(), &txn)
        .unwrap();
    assert!(different_provider_result.is_none());

    let different_subject_result = token_system
        .get_user_from_oidc(provider, "other_user", store.as_ref(), &txn)
        .unwrap();
    assert!(different_subject_result.is_none());

    txn.commit().unwrap();
    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_ed25519_key_generation_and_usage() {
    // Test Ed25519 key pair generation
    let key_pair = Ed25519KeyPair::generate();

    let signing_pem = key_pair.signing_key_pem().unwrap();
    let verifying_pem = key_pair.verifying_key_pem().unwrap();

    assert!(signing_pem.contains("BEGIN PRIVATE KEY"));
    assert!(verifying_pem.contains("BEGIN PUBLIC KEY"));

    // Test token generation and validation with Ed25519
    let realm_ulid = create_test_realm_key(1);
    let user_ulid = create_test_ulid(2);
    let mut token_system = TokenSystem::new(realm_ulid, OidcTrustConfig::TrustAll);

    token_system.add_realm_public_key(realm_ulid, verifying_pem);

    let user_identity = UserIdentity::new(user_ulid, realm_ulid);

    // Generate token
    let token = token_system
        .generate_token(&user_identity, &signing_pem)
        .unwrap();

    // Validate token
    let recovered_identity = token_system.validate_realm_token(&token).unwrap();

    assert_eq!(recovered_identity, user_identity);
}
