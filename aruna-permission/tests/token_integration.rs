use aruna_permission::paths::RealmKey;
use aruna_permission::token::{TokenSystem, derive_realm_key, generate_realm_private_key};
use aruna_permission::*;
use aruna_storage::storage::{
    lmdb::{LmdbConfig, LmdbStore},
    store::Store,
};
use ulid::Ulid;

// NOTE: These integration tests use TokenSystem::generate() instead of TokenSystem::new() with issuers
// to avoid JWKS fetching issues. For tests that need OIDC-like functionality, we manually create
// identities and use add_oidc_identity/get_user_from_oidc directly rather than register_user_from_oidc_claims
// which requires trusted issuers with valid JWKS URLs.

async fn setup_test_systems() -> (
    TokenSystem,
    PermissionManager,
    LmdbStore,
    String,
    [u8; 32], // Changed from Ed25519KeyPair to private key bytes
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

    // Generate private key for realm A
    let realm_a_private_key = generate_realm_private_key();

    // For integration tests, use TokenSystem::generate() to avoid JWKS fetching issues
    let mut token_system = TokenSystem::generate().unwrap();

    // Create permission manager
    let permission_manager = PermissionManager::new().await.unwrap();

    (
        token_system,
        permission_manager,
        store,
        test_dir,
        realm_a_private_key,
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
    let (mut token_system, permission_manager, store, test_dir, _realm_private_key) =
        setup_test_systems().await;
    let mut txn = store.create_txn(true).unwrap();

    // Step 1: Simulate identity registration from OIDC claims
    // Since we're using TokenSystem::generate(), we need to manually create identities
    // and simulate the OIDC mapping without going through the full OIDC verification

    let alice_user_ulid = Ulid::new();
    let bob_user_ulid = Ulid::new();
    let realm_key = token_system.local_realm_key();

    let alice_identity = UserIdentity::new(alice_user_ulid, realm_key);
    let bob_identity = UserIdentity::new(bob_user_ulid, realm_key);

    // Manually add OIDC mappings to simulate registration
    token_system
        .add_oidc_identity(
            "https://accounts.google.com",
            "alice@example.com",
            alice_user_ulid,
            &store,
            &mut txn,
        )
        .unwrap();

    token_system
        .add_oidc_identity(
            "https://github.com/login/oauth",
            "bob_dev",
            bob_user_ulid,
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
    // Generate private keys for both realms
    let realm_a_private_key = generate_realm_private_key();
    let realm_b_private_key = generate_realm_private_key();
    let realm_a = derive_realm_key(&realm_a_private_key);
    let realm_b = derive_realm_key(&realm_b_private_key);

    // Create token systems using generate() to avoid JWKS fetching
    let mut token_system_a = TokenSystem::generate().unwrap();
    let mut token_system_b = TokenSystem::generate().unwrap();
    let permission_manager = PermissionManager::new().await.unwrap();

    let (store, test_dir) = setup_test_store().await;
    let mut txn = store.create_txn(true).unwrap();

    // Enable cross-realm verification by sharing public keys
    token_system_a.add_realm_key(realm_b).unwrap();
    token_system_b.add_realm_key(realm_a).unwrap();

    let alice_provider = "https://accounts.google.com";
    let alice_sub = "alice@example.com";

    // Create identities manually since we're not using full OIDC verification
    let alice_user_ulid = Ulid::new();
    let alice_realm_a = UserIdentity::new(alice_user_ulid, token_system_a.local_realm_key());
    let alice_realm_b = UserIdentity::new(alice_user_ulid, token_system_b.local_realm_key());

    // Add OIDC mappings manually
    token_system_a
        .add_oidc_identity(alice_provider, alice_sub, alice_user_ulid, &store, &mut txn)
        .unwrap();
    token_system_b
        .add_oidc_identity(alice_provider, alice_sub, alice_user_ulid, &store, &mut txn)
        .unwrap();

    assert_eq!(alice_realm_a.realm_key, token_system_a.local_realm_key());
    assert_eq!(alice_realm_b.realm_key, token_system_b.local_realm_key());

    // Create separate groups and resources in each realm
    let group_a = create_test_ulid(20);
    let group_b = create_test_ulid(21);

    permission_manager
        .create_group(
            group_a,
            &alice_realm_a,
            alice_realm_a.realm_key,
            &store,
            &mut txn,
        )
        .await
        .unwrap();
    permission_manager
        .create_group(
            group_b,
            &alice_realm_b,
            alice_realm_b.realm_key,
            &store,
            &mut txn,
        )
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
        .realm_id(alice_realm_a.realm_key)
        .group_metadata(group_a, resource_a, vec![])
        .build()
        .unwrap();

    let path_b = Path::builder()
        .realm_id(alice_realm_b.realm_key)
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
    let alice_realm_a_token = token_system_a.generate_token(&alice_realm_a).unwrap();
    let alice_realm_b_token = token_system_b.generate_token(&alice_realm_b).unwrap();

    // Verify tokens can be validated in their respective realms
    let verified_identity_a = token_system_a
        .validate_realm_token(&alice_realm_a_token)
        .unwrap();
    let verified_identity_b = token_system_b
        .validate_realm_token(&alice_realm_b_token)
        .unwrap();

    assert_eq!(verified_identity_a.user_ulid, alice_realm_a.user_ulid);
    assert_eq!(verified_identity_b.user_ulid, alice_realm_b.user_ulid);

    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_token_identity_extraction() {
    let realm_private_key = generate_realm_private_key();
    let realm_key = derive_realm_key(&realm_private_key);
    let mut token_system = TokenSystem::generate().unwrap();

    let (store, test_dir) = setup_test_store().await;
    let mut txn = store.create_txn(true).unwrap();

    // Test 1: Create user identity and add OIDC mapping manually
    let user_ulid = Ulid::new();
    let identity_from_oidc = UserIdentity::new(user_ulid, token_system.local_realm_key());

    token_system
        .add_oidc_identity(
            "https://login.microsoftonline.com",
            "user@company.com",
            user_ulid,
            &store,
            &mut txn,
        )
        .unwrap();

    // Test 2: Same OIDC subject should return same user ULID
    let lookup_result = token_system
        .get_user_from_oidc(
            "https://login.microsoftonline.com",
            "user@company.com",
            &store,
            &txn,
        )
        .unwrap();

    assert_eq!(lookup_result, Some(user_ulid));
    let identity_second_login =
        UserIdentity::new(lookup_result.unwrap(), token_system.local_realm_key());

    assert_eq!(
        identity_from_oidc.user_ulid,
        identity_second_login.user_ulid
    );

    // Test 3: Different OIDC subjects create distinct identities
    let different_user_ulid = Ulid::new();
    let different_user_identity =
        UserIdentity::new(different_user_ulid, token_system.local_realm_key());

    token_system
        .add_oidc_identity(
            "https://login.microsoftonline.com",
            "different_user",
            different_user_ulid,
            &store,
            &mut txn,
        )
        .unwrap();

    assert_ne!(
        identity_from_oidc.user_ulid,
        different_user_identity.user_ulid
    );
    assert_eq!(
        different_user_identity.realm_key,
        token_system.local_realm_key()
    );

    // Test 4: Aruna realm token round-trip
    let aruna_token = token_system.generate_token(&identity_from_oidc).unwrap();

    let parsed_identity = token_system.validate_realm_token(&aruna_token).unwrap();

    assert_eq!(parsed_identity.user_ulid, identity_from_oidc.user_ulid);
    assert_eq!(parsed_identity.realm_key, identity_from_oidc.realm_key);

    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_typical_usage_pattern() {
    let realm_private_key = generate_realm_private_key();
    let realm_key = derive_realm_key(&realm_private_key);
    let mut token_system = TokenSystem::generate().unwrap();
    let permission_manager = PermissionManager::new().await.unwrap();

    let (store, test_dir) = setup_test_store().await;
    let mut txn = store.create_txn(true).unwrap();

    // === Token System Usage ===

    // 1. Create user identity and simulate OIDC registration
    let user_ulid = Ulid::new();
    let user_identity = UserIdentity::new(user_ulid, token_system.local_realm_key());

    token_system
        .add_oidc_identity(
            "https://accounts.google.com",
            "user@example.com",
            user_ulid,
            &store,
            &mut txn,
        )
        .unwrap();

    // === Permission System Usage ===

    // 2. Create group
    let group_id = create_test_ulid(20);
    permission_manager
        .create_group(
            group_id,
            &user_identity,
            user_identity.realm_key,
            &store,
            &mut txn,
        )
        .await
        .unwrap();

    // 3. Add resources
    let doc_id = create_test_ulid(30);
    let doc_path = Path::builder()
        .realm_id(user_identity.realm_key)
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
    let aruna_token = token_system.generate_token(&user_identity).unwrap();

    let mut txn = store.create_txn(true).unwrap();

    // 6. Token verification workflow
    let verified_identity = token_system.validate_realm_token(&aruna_token).unwrap();

    assert_eq!(verified_identity.user_ulid, user_identity.user_ulid);

    // 7. Same user in different realm
    let mut other_token_system = TokenSystem::generate().unwrap();
    let other_user_ulid = Ulid::new();
    let other_identity = UserIdentity::new(other_user_ulid, other_token_system.local_realm_key());

    other_token_system
        .add_oidc_identity(
            "https://accounts.google.com",
            "user@example.com",
            other_user_ulid,
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
    let realm_private_key = generate_realm_private_key();
    let token_system = TokenSystem::new(&realm_private_key, vec![]).unwrap();

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
async fn test_token_generation_and_usage() {
    // Test token generation and validation
    let realm_private_key = generate_realm_private_key();
    let realm_key = derive_realm_key(&realm_private_key);

    let user_ulid = create_test_ulid(123);
    let user_identity = UserIdentity::new(user_ulid, realm_key);

    let token_system = TokenSystem::new(&realm_private_key, vec![]).unwrap();

    // Generate token
    let token = token_system.generate_token(&user_identity).unwrap();

    // Validate token
    let recovered_identity = token_system.validate_realm_token(&token).unwrap();

    assert_eq!(recovered_identity.user_ulid, user_identity.user_ulid);
    assert_eq!(recovered_identity.realm_key, user_identity.realm_key);
}
