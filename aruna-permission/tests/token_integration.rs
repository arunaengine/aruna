use aruna_permission::paths::RealmKey;
use aruna_permission::token::{Ed25519KeyPair, TokenSystem};
use aruna_permission::*;
use aruna_storage::storage::{
    lmdb::{LmdbConfig, LmdbStore},
    store::Store,
};
use ulid::Ulid;

async fn setup_test_systems() -> (
    TokenSystem,
    PermissionManager,
    LmdbStore,
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

    let store = LmdbStore::new(config).unwrap();

    // Create token system for realm A with trusted OIDC issuers
    let realm_a = create_test_realm_key(10);
    let issuers = vec![
        aruna_permission::token::Issuer {
            issuer_name: "https://accounts.google.com".to_string(),
            pubkey_url: "".to_string(),
            aud: vec![],
        },
        aruna_permission::token::Issuer {
            issuer_name: "https://github.com/login/oauth".to_string(),
            pubkey_url: "".to_string(),
            aud: vec![],
        },
    ];
    let mut token_system = TokenSystem::new(realm_a, issuers).await.unwrap();

    // Generate Ed25519 key pair for realm tokens
    let realm_key_pair = Ed25519KeyPair::generate();
    let verifying_key_pem = realm_key_pair.verifying_key_pem().unwrap();
    token_system
        .add_realm_public_key(realm_a, verifying_key_pem)
        .unwrap();

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

pub async fn setup_test_store() -> (LmdbStore, String) {
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

    let store = LmdbStore::new(config).unwrap();
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
            &store,
            &mut txn,
        )
        .unwrap();

    let bob_identity = token_system
        .register_user_from_oidc_claims(
            "https://github.com/login/oauth",
            "bob_dev",
            &store,
            &mut txn,
        )
        .unwrap();

    // Step 2: Permission system handles group creation using identities
    let group_id = create_test_ulid(20);

    permission_manager
        .create_group(
            group_id,
            &alice_identity,
            alice_identity.realm_key,
            &store,
            &mut txn,
        )
        .await
        .unwrap();

    // Step 3: Add Bob to Alice's group through permission system
    permission_manager
        .add_user(group_id, &bob_identity, "member", &store, &mut txn)
        .await
        .unwrap();

    // Step 4: Create resource and test access control
    let resource_id = create_test_ulid(30);
    let resource_path = Path::builder()
        .realm_id(alice_identity.realm_key)
        .group_metadata(group_id, resource_id, vec![])
        .build()
        .unwrap();

    permission_manager
        .add_resource(
            ResourceId::Ulid(resource_id),
            &resource_path,
            &store,
            &mut txn,
        )
        .unwrap();

    txn.commit().unwrap();

    // Verify both users have access to the shared resource
    let alice_access = permission_manager
        .check_permission(
            &alice_identity,
            ResourceId::Ulid(resource_id),
            Action::Write,
            &store,
        )
        .await;
    let bob_access = permission_manager
        .check_permission(
            &bob_identity,
            ResourceId::Ulid(resource_id),
            Action::Write,
            &store,
        )
        .await;

    assert!(alice_access.is_ok());
    assert!(bob_access.is_ok());

    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_cross_realm_token_handling() {
    let realm_a = create_test_realm_key(10);
    let realm_b = create_test_realm_key(11);

    let issuers_a = vec![aruna_permission::token::Issuer {
        issuer_name: "https://accounts.google.com".to_string(),
        pubkey_url: "".to_string(),
        aud: vec![],
    }];
    let issuers_b = vec![aruna_permission::token::Issuer {
        issuer_name: "https://accounts.google.com".to_string(),
        pubkey_url: "".to_string(),
        aud: vec![],
    }];
    let mut token_system_a = TokenSystem::new(realm_a, issuers_a).await.unwrap();
    let mut token_system_b = TokenSystem::new(realm_b, issuers_b).await.unwrap();
    let permission_manager = PermissionManager::new().await.unwrap();

    let (store, test_dir) = setup_test_store().await;
    let mut txn = store.create_txn(true).unwrap();

    let realm_a_keys = Ed25519KeyPair::generate();
    let realm_b_keys = Ed25519KeyPair::generate();

    let realm_a_verifying_pem = realm_a_keys.verifying_key_pem().unwrap();
    let realm_b_verifying_pem = realm_b_keys.verifying_key_pem().unwrap();

    token_system_a
        .add_realm_public_key(realm_a, realm_a_verifying_pem.clone())
        .unwrap();
    token_system_b
        .add_realm_public_key(realm_b, realm_b_verifying_pem.clone())
        .unwrap();

    // Enable cross-realm verification by sharing public keys
    token_system_a
        .add_realm_public_key(realm_b, realm_b_verifying_pem)
        .unwrap();
    token_system_b
        .add_realm_public_key(realm_a, realm_a_verifying_pem)
        .unwrap();

    let alice_provider = "https://accounts.google.com";
    let alice_sub = "alice@example.com";

    let alice_realm_a = token_system_a
        .register_user_from_oidc_claims(alice_provider, alice_sub, &store, &mut txn)
        .unwrap();

    let alice_realm_b = token_system_b
        .register_user_from_oidc_claims(alice_provider, alice_sub, &store, &mut txn)
        .unwrap();

    assert_eq!(alice_realm_a.realm_key, realm_a);
    assert_eq!(alice_realm_b.realm_key, realm_b);

    // Create separate groups and resources in each realm
    let group_a = create_test_ulid(20);
    let group_b = create_test_ulid(21);

    permission_manager
        .create_group(group_a, &alice_realm_a, realm_a, &store, &mut txn)
        .await
        .unwrap();
    permission_manager
        .create_group(group_b, &alice_realm_b, realm_b, &store, &mut txn)
        .await
        .unwrap();

    // Unify identities across realms at permission level
    permission_manager
        .unify_identities(&alice_realm_a, &alice_realm_b, &store, &mut txn)
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
        .add_resource(ResourceId::Ulid(resource_a), &path_a, &store, &mut txn)
        .unwrap();
    permission_manager
        .add_resource(ResourceId::Ulid(resource_b), &path_b, &store, &mut txn)
        .unwrap();

    store.commit(txn).unwrap();

    // Verify cross-realm access works after unification
    assert!(
        permission_manager
            .check_permission(
                &alice_realm_a,
                ResourceId::Ulid(resource_b),
                Action::Write,
                &store,
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
                &store,
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
        .validate_realm_token(&alice_realm_a_token)
        .await
        .unwrap();
    let verified_identity_b = token_system_b
        .validate_realm_token(&alice_realm_b_token)
        .await
        .unwrap();

    assert_eq!(verified_identity_a.user_ulid, alice_realm_a.user_ulid);
    assert_eq!(verified_identity_b.user_ulid, alice_realm_b.user_ulid);

    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_token_identity_extraction() {
    let realm_key = create_test_realm_key(10);
    let issuers = vec![aruna_permission::token::Issuer {
        issuer_name: "https://login.microsoftonline.com".to_string(),
        pubkey_url: "".to_string(),
        aud: vec![],
    }];
    let mut token_system = TokenSystem::new(realm_key, issuers).await.unwrap();

    let (store, test_dir) = setup_test_store().await;
    let mut txn = store.create_txn(true).unwrap();

    // Generate Ed25519 key pair for realm tokens
    let realm_keys = Ed25519KeyPair::generate();
    let verifying_pem = realm_keys.verifying_key_pem().unwrap();
    token_system
        .add_realm_public_key(realm_key, verifying_pem)
        .unwrap();

    // Test 1: Register user from OIDC claims
    let identity_from_oidc = token_system
        .register_user_from_oidc_claims(
            "https://login.microsoftonline.com",
            "user@company.com",
            &store,
            &mut txn,
        )
        .unwrap();

    // Test 2: Same OIDC subject should return same identity
    let identity_second_login = token_system
        .register_user_from_oidc_claims(
            "https://login.microsoftonline.com",
            "user@company.com",
            &store,
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
            &store,
            &mut txn,
        )
        .unwrap();

    assert_ne!(
        identity_from_oidc.user_ulid,
        different_user_identity.user_ulid
    );
    assert_eq!(different_user_identity.realm_key, realm_key);

    // Test 4: Aruna realm token round-trip
    let signing_pem = realm_keys.signing_key_pem().unwrap();
    let aruna_token = token_system
        .generate_token(&identity_from_oidc, &signing_pem)
        .unwrap();

    let parsed_identity = token_system
        .validate_realm_token(&aruna_token)
        .await
        .unwrap();

    assert_eq!(parsed_identity.user_ulid, identity_from_oidc.user_ulid);
    assert_eq!(parsed_identity.realm_key, identity_from_oidc.realm_key);

    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_typical_usage_pattern() {
    let realm_key = create_test_realm_key(10);
    let issuers = vec![aruna_permission::token::Issuer {
        issuer_name: "https://accounts.google.com".to_string(),
        pubkey_url: "".to_string(),
        aud: vec![],
    }];
    let mut token_system = TokenSystem::new(realm_key, issuers).await.unwrap();
    let permission_manager = PermissionManager::new().await.unwrap();

    let (store, test_dir) = setup_test_store().await;
    let mut txn = store.create_txn(true).unwrap();

    // Configure realm keys
    let realm_keys = Ed25519KeyPair::generate();
    let verifying_pem = realm_keys.verifying_key_pem().unwrap();
    token_system
        .add_realm_public_key(realm_key, verifying_pem)
        .unwrap();

    // === Token System Usage ===

    // 1. Register user from OIDC token
    let user_identity = token_system
        .register_user_from_oidc_claims(
            "https://accounts.google.com",
            "user@example.com",
            &store,
            &mut txn,
        )
        .unwrap();

    // === Permission System Usage ===

    // 2. Create group
    let group_id = create_test_ulid(20);
    permission_manager
        .create_group(group_id, &user_identity, realm_key, &store, &mut txn)
        .await
        .unwrap();

    // 3. Add resources
    let doc_id = create_test_ulid(30);
    let doc_path = Path::builder()
        .realm_id(realm_key)
        .group_metadata(group_id, doc_id, vec![])
        .build()
        .unwrap();

    permission_manager
        .add_resource(ResourceId::Ulid(doc_id), &doc_path, &store, &mut txn)
        .unwrap();
    store.commit(txn).unwrap();

    // 4. Check permissions
    let access_result = permission_manager
        .check_permission(
            &user_identity,
            ResourceId::Ulid(doc_id),
            Action::Write,
            &store,
        )
        .await;

    assert!(access_result.is_ok());

    // 5. Generate Aruna token for API access
    let signing_pem = realm_keys.signing_key_pem().unwrap();
    let aruna_token = token_system
        .generate_token(&user_identity, &signing_pem)
        .unwrap();

    let mut txn = store.create_txn(true).unwrap();

    // 6. Token verification workflow
    let verified_identity = token_system
        .validate_realm_token(&aruna_token)
        .await
        .unwrap();

    assert_eq!(verified_identity.user_ulid, user_identity.user_ulid);

    // 7. Same user in different realm
    let other_realm = create_test_realm_key(11);
    let other_issuers = vec![aruna_permission::token::Issuer {
        issuer_name: "https://accounts.google.com".to_string(),
        pubkey_url: "".to_string(),
        aud: vec![],
    }];
    let other_token_system = TokenSystem::new(other_realm, other_issuers).await.unwrap();

    let other_identity = other_token_system
        .register_user_from_oidc_claims(
            "https://accounts.google.com",
            "user@example.com",
            &store,
            &mut txn,
        )
        .unwrap();

    // Cross-realm unification
    permission_manager
        .unify_identities(&user_identity, &other_identity, &store, &mut txn)
        .await
        .unwrap();

    txn.commit().unwrap();
    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_oidc_lookup_operations() {
    let realm_key = create_test_realm_key(10);
    let token_system = TokenSystem::new(realm_key, vec![]).await.unwrap();

    let (store, test_dir) = setup_test_store().await;
    let txn = store.create_txn(false).unwrap();

    let user_ulid = create_test_ulid(42);
    let provider = "https://auth.example.com";
    let subject = "test_user_123";

    // Initially no OIDC mapping exists
    let lookup_result = token_system
        .get_user_from_oidc(provider, subject, &store, &txn)
        .unwrap();
    assert!(lookup_result.is_none());

    // Need write transaction to add mapping
    let mut write_txn = store.create_txn(true).unwrap();

    // Add OIDC identity mapping
    token_system
        .add_oidc_identity(provider, subject, user_ulid, &store, &mut write_txn)
        .unwrap();

    // Lookup should now return the mapped user ULID
    let lookup_result = token_system
        .get_user_from_oidc(provider, subject, &store, &write_txn)
        .unwrap();
    assert_eq!(lookup_result, Some(user_ulid));

    // Verify isolation: different provider/subject combinations return None
    let different_provider_result = token_system
        .get_user_from_oidc("https://other.com", subject, &store, &write_txn)
        .unwrap();
    assert!(different_provider_result.is_none());

    let different_subject_result = token_system
        .get_user_from_oidc(provider, "other_user", &store, &write_txn)
        .unwrap();
    assert!(different_subject_result.is_none());

    write_txn.commit().unwrap();
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
    let realm_key = create_test_realm_key(1);
    let mut token_system = TokenSystem::new(realm_key, vec![]).await.unwrap();

    token_system
        .add_realm_public_key(realm_key, verifying_pem)
        .unwrap();

    let user_ulid = create_test_ulid(123);
    let user_identity = UserIdentity::new(user_ulid, realm_key);

    // Generate token
    let token = token_system
        .generate_token(&user_identity, &signing_pem)
        .unwrap();

    // Validate token
    let recovered_identity = token_system.validate_realm_token(&token).await.unwrap();

    assert_eq!(recovered_identity.user_ulid, user_identity.user_ulid);
    assert_eq!(recovered_identity.realm_key, user_identity.realm_key);
}
