use aruna_permission::paths::RealmKey;
use aruna_permission::{Action, Path, PermissionManager, ResourceId, UserIdentity};
use aruna_storage::storage::{
    lmdb::{LmdbConfig, LmdbStore},
    store::Store,
};
use ulid::Ulid;

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

pub fn create_test_ulid(suffix: u8) -> Ulid {
    let mut bytes = [0u8; 16];
    bytes[15] = suffix;
    Ulid::from_bytes(bytes)
}

pub fn create_test_realm_key(suffix: u8) -> RealmKey {
    let mut key = [0u8; 32];
    key[31] = suffix;
    key
}

#[tokio::test]
async fn test_complete_oidc_workflow() {
    let (store, test_dir) = setup_test_store().await;
    let mut txn = store.create_txn(true).unwrap();
    let manager = PermissionManager::new().await.unwrap();

    // Simulate OIDC login flow
    let realm_id = create_test_realm_key(1);
    let oidc_provider = "google";
    let oidc_sub = "user123@example.com";

    // Step 1: User logs in for the first time (registration)
    let user_ulid = Ulid::new(); // This would be generated during registration
    manager
        .add_oidc_identity(oidc_provider, oidc_sub, user_ulid, &store, &mut txn)
        .unwrap();

    let user_identity = UserIdentity::new(user_ulid, realm_id);

    // Step 2: User creates their first group
    let group_id = Ulid::new();
    manager
        .create_group(group_id, &user_identity, realm_id, &store, &mut txn)
        .await
        .unwrap();

    // Step 3: User adds some resources to their group
    let document_id = Ulid::new();
    let document_path = Path::builder()
        .realm_id(realm_id)
        .group_metadata(group_id, document_id, vec![])
        .build()
        .unwrap();

    manager
        .add_resource(
            ResourceId::Ulid(document_id),
            &document_path,
            &store,
            &mut txn,
        )
        .unwrap();

    // Step 4: User invites another user to the group
    let invited_user_ulid = Ulid::new();
    let invited_identity = UserIdentity::new(invited_user_ulid, realm_id);

    // Add invited user's OIDC identity
    manager
        .add_oidc_identity(
            "github",
            "invited_user",
            invited_user_ulid,
            &store,
            &mut txn,
        )
        .unwrap();

    // Add them to the group
    manager
        .add_user(group_id, &invited_identity, "member", &store, &mut txn)
        .await
        .unwrap();
    store.commit(txn).unwrap();
    let txn = store.create_txn(false).unwrap();

    // Step 5: Test permission checking

    // Original user should have admin access
    let admin_result = manager
        .check_permission(
            &user_identity,
            ResourceId::Ulid(document_id),
            Action::Write,
            &store,
        )
        .await;
    assert!(admin_result.is_ok());

    // Invited user should have member access
    let member_result = manager
        .check_permission(
            &invited_identity,
            ResourceId::Ulid(document_id),
            Action::Write,
            &store,
        )
        .await;
    assert!(member_result.is_ok());

    // Outside user should be denied
    let outsider = UserIdentity::new(Ulid::new(), realm_id);
    let outsider_result = manager
        .check_permission(
            &outsider,
            ResourceId::Ulid(document_id),
            Action::Write,
            &store,
        )
        .await;
    assert!(outsider_result.is_err());

    // Step 6: Simulate subsequent login (OIDC lookup)

    // User logs in again with Google
    let found_user = manager
        .get_user_from_oidc(oidc_provider, oidc_sub, &store, &txn)
        .unwrap();
    assert_eq!(found_user, Some(user_ulid));

    // Reconstruct their identity
    let logged_in_identity = UserIdentity::new(found_user.unwrap(), realm_id);
    assert_eq!(logged_in_identity, user_identity);

    // Step 7: Test cross-realm functionality
    store.commit(txn).unwrap();
    let mut txn = store.create_txn(true).unwrap();

    // Same user joins another realm
    let other_realm_id = create_test_realm_key(2);
    let cross_realm_identity = UserIdentity::new(user_ulid, other_realm_id);

    // Create different permission mapping for other realm
    let other_permission_ulid = Ulid::new();
    manager
        .add_identity_permission(
            &cross_realm_identity,
            other_permission_ulid,
            &store,
            &mut txn,
        )
        .unwrap();

    // Create group in other realm
    let other_group_id = Ulid::new();
    manager
        .create_group(
            other_group_id,
            &cross_realm_identity,
            other_realm_id,
            &store,
            &mut txn,
        )
        .await
        .unwrap();

    txn.commit().unwrap();
    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_permission_unification_scenario() {
    let (store, test_dir) = setup_test_store().await;
    let mut txn = store.create_txn(true).unwrap();
    let manager = PermissionManager::new().await.unwrap();

    let realm_id = create_test_realm_key(1);
    let user_ulid = Ulid::new();

    // Scenario: User has multiple OIDC accounts that should be unified
    manager
        .add_oidc_identity("google", "user@gmail.com", user_ulid, &store, &mut txn)
        .unwrap();
    manager
        .add_oidc_identity("github", "user123", user_ulid, &store, &mut txn)
        .unwrap();
    manager
        .add_oidc_identity("microsoft", "user@company.com", user_ulid, &store, &mut txn)
        .unwrap();

    let user_identity = UserIdentity::new(user_ulid, realm_id);

    // Create a unified permission ID for this user
    let unified_permission_ulid = Ulid::new();
    manager
        .add_identity_permission(&user_identity, unified_permission_ulid, &store, &mut txn)
        .unwrap();

    // User should be able to login with any provider and get same permissions
    let google_lookup = manager
        .get_user_from_oidc("google", "user@gmail.com", &store, &txn)
        .unwrap();
    let github_lookup = manager
        .get_user_from_oidc("github", "user123", &store, &txn)
        .unwrap();
    let microsoft_lookup = manager
        .get_user_from_oidc("microsoft", "user@company.com", &store, &txn)
        .unwrap();

    assert_eq!(google_lookup, Some(user_ulid));
    assert_eq!(github_lookup, Some(user_ulid));
    assert_eq!(microsoft_lookup, Some(user_ulid));

    txn.commit().unwrap();
    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_realm_sovereignty() {
    let (store, test_dir) = setup_test_store().await;
    let mut txn = store.create_txn(true).unwrap();
    let manager = PermissionManager::new().await.unwrap();

    // Two separate realms with their own sovereignty
    let realm_a = create_test_realm_key(1);
    let realm_b = create_test_realm_key(2);

    // Same user ULID in both realms (cross-realm user)
    let user_ulid = Ulid::new();
    let identity_a = UserIdentity::new(user_ulid, realm_a);
    let identity_b = UserIdentity::new(user_ulid, realm_b);

    // Each realm has its own OIDC providers and permissions
    manager
        .add_oidc_identity(
            "realm_a_provider",
            "user@a.com",
            user_ulid,
            &store,
            &mut txn,
        )
        .unwrap();
    manager
        .add_oidc_identity(
            "realm_b_provider",
            "user@b.com",
            user_ulid,
            &store,
            &mut txn,
        )
        .unwrap();

    // Different permission mappings in each realm
    let permission_a = Ulid::new();
    let permission_b = Ulid::new();
    manager
        .add_identity_permission(&identity_a, permission_a, &store, &mut txn)
        .unwrap();
    manager
        .add_identity_permission(&identity_b, permission_b, &store, &mut txn)
        .unwrap();

    // Create groups in each realm
    let group_a = Ulid::new();
    let group_b = Ulid::new();
    manager
        .create_group(group_a, &identity_a, realm_a, &store, &mut txn)
        .await
        .unwrap();
    manager
        .create_group(group_b, &identity_b, realm_b, &store, &mut txn)
        .await
        .unwrap();

    // Create realm-specific resources
    let resource_a = Ulid::new();
    let resource_b = Ulid::new();

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

    manager
        .add_resource(ResourceId::Ulid(resource_a), &path_a, &store, &mut txn)
        .unwrap();
    manager
        .add_resource(ResourceId::Ulid(resource_b), &path_b, &store, &mut txn)
        .unwrap();
    store.commit(txn).unwrap();
    let txn = store.create_txn(false).unwrap();

    // Test realm sovereignty: User should only access resources in their respective realms

    // Identity A should access Resource A
    let result_a_a = manager
        .check_permission(
            &identity_a,
            ResourceId::Ulid(resource_a),
            Action::Write,
            &store,
        )
        .await;
    assert!(result_a_a.is_ok());

    // Identity B should access Resource B
    let result_b_b = manager
        .check_permission(
            &identity_b,
            ResourceId::Ulid(resource_b),
            Action::Write,
            &store,
        )
        .await;
    assert!(result_b_b.is_ok());

    txn.commit().unwrap();
    cleanup_test_dir(&test_dir);
}

#[tokio::test]
async fn test_concurrent_operations() {
    use std::sync::Arc;
    use tokio::task;

    let (store, test_dir) = setup_test_store().await;
    let manager = Arc::new(PermissionManager::new().await.unwrap());

    let realm_id = create_test_realm_key(10);
    let group_id = create_test_ulid(20);
    let admin_identity = UserIdentity::new(create_test_ulid(1), realm_id);

    // Create group first
    let mut txn = store.create_txn(true).unwrap();
    manager
        .create_group(group_id, &admin_identity, realm_id, &store, &mut txn)
        .await
        .unwrap();
    txn.commit().unwrap();

    // Test concurrent read access (permission checking)
    let manager1 = Arc::clone(&manager);
    let manager2 = Arc::clone(&manager);

    let task1 = task::spawn(async move {
        for _ in 0..50 {
            let roles = manager1.get_group_roles(group_id).await;
            assert!(!roles.is_empty());
        }
    });

    let task2 = task::spawn(async move {
        for _ in 0..50 {
            let roles = manager2.get_group_roles(group_id).await;
            assert!(!roles.is_empty());
        }
    });

    // Both tasks should complete successfully
    let (result1, result2) = tokio::join!(task1, task2);
    result1.unwrap();
    result2.unwrap();

    cleanup_test_dir(&test_dir);
}
