use aruna_storage::storage::store::Store;
use blake3::Hash as Blake3Hash;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::Arc;
use ulid::Ulid;

use crate::casbin::DBNAME;
use crate::casbin_helper::{CasbinPolicy, CasbinRole};
use crate::error::{PathError, PermissionError, Result};
use crate::{casbin::Enforcer, paths::Path};

// Database constants
pub const RESOURCE_DB: &str = "resources";
pub const OIDC_IDENTITIES_DB: &str = "oidc_identities";
pub const IDENTITY_PERMISSIONS_DB: &str = "identity_permissions";

/// User identity consisting of user ULID and realm ULID
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UserIdentity {
    pub user_ulid: Ulid,
    pub realm_ulid: Ulid,
}

impl UserIdentity {
    pub fn new(user_ulid: Ulid, realm_ulid: Ulid) -> Self {
        Self {
            user_ulid,
            realm_ulid,
        }
    }
}

impl Display for UserIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.user_ulid, self.realm_ulid)
    }
}

/// Resource identifier that can be either a ULID or Blake3 hash
#[derive(Debug, Clone, PartialEq)]
pub enum ResourceId {
    Ulid(Ulid),
    ContentHash(Blake3Hash),
}

impl Display for ResourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceId::Ulid(ulid) => write!(f, "{}", ulid),
            ResourceId::ContentHash(hash) => write!(
                f,
                "{}",
                base64::Engine::encode(
                    &base64::engine::general_purpose::URL_SAFE_NO_PAD,
                    hash.as_bytes()
                )
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    Read,
    Write,
}

impl Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Action::Read => write!(f, "read"),
            Action::Write => write!(f, "write"),
        }
    }
}

/// Prepare token for creating a group
pub struct CreateGroupPrepare {
    policy: Vec<CasbinPolicy>,
    role: Vec<CasbinRole>,
}

/// Prepare token for adding a user to a role
pub struct AddUserPrepare {
    role: CasbinRole,
}

/// Prepare token for removing a role from a user
pub struct RemoveRolePrepare {
    role: CasbinRole,
}

/// Prepare token for adding a policy to a role
pub struct AddPolicyPrepare {
    policy: CasbinPolicy,
}

/// Higher-level permission manager that builds on top of the base Enforcer
#[derive(Clone)]
pub struct PermissionManager {
    pub enforcer: Arc<RwLock<Enforcer>>,
}

impl PermissionManager {
    pub async fn new() -> Result<Self> {
        let enforcer = Enforcer::new().await?;
        Ok(Self {
            enforcer: Arc::new(RwLock::new(enforcer)),
        })
    }

    /// Resolve user identity to permission ULID
    fn resolve_permission_ulid<'a, S: Store<'a> + 'static>(
        &self,
        user_identity: &UserIdentity,
        store: &'a S,
        txn: &<S as Store<'a>>::Txn,
    ) -> Result<Ulid> {
        // Serialize UserIdentity for storage key
        let key = postcard::to_allocvec(user_identity)?;

        if let Some(permission_bytes) = store
            .get(txn, IDENTITY_PERMISSIONS_DB, &key)
            .map_err(|e| PermissionError::StorageError(e))?
        {
            let permission_ulid: Ulid = postcard::from_bytes(&permission_bytes)?;
            Ok(permission_ulid)
        } else {
            // If no permission mapping exists, create one using the user's ULID
            // This maintains backward compatibility for existing users
            Ok(user_identity.user_ulid)
        }
    }

    /// Add OIDC identity mapping
    pub fn add_oidc_identity<'a, S: Store<'a> + 'static>(
        &self,
        oidc_provider: &str,
        oidc_sub: &str,
        user_ulid: Ulid,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let key = format!("{}:{}", oidc_provider, oidc_sub);
        let value = postcard::to_allocvec(&user_ulid)?;

        store.put(txn, OIDC_IDENTITIES_DB, key.as_bytes(), &value)?;
        Ok(())
    }

    /// Lookup user ULID from OIDC identity
    pub fn get_user_from_oidc<'a, S: Store<'a> + 'static>(
        &self,
        oidc_provider: &str,
        oidc_sub: &str,
        store: &'a S,
        txn: &'a <S as Store<'a>>::Txn,
    ) -> Result<Option<Ulid>> {
        let key = format!("{}:{}", oidc_provider, oidc_sub);

        if let Some(user_bytes) = store
            .get(txn, OIDC_IDENTITIES_DB, key.as_bytes())
            .map_err(|e| PermissionError::StorageError(e))?
        {
            let user_ulid: Ulid = postcard::from_bytes(&user_bytes)?;
            Ok(Some(user_ulid))
        } else {
            Ok(None)
        }
    }

    /// Add identity to permission mapping
    pub fn add_identity_permission<'a, S: Store<'a> + 'static>(
        &self,
        user_identity: &UserIdentity,
        permission_ulid: Ulid,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let key = postcard::to_allocvec(user_identity)?;
        let value = postcard::to_allocvec(&permission_ulid)?;

        store.put(txn, IDENTITY_PERMISSIONS_DB, &key, &value)?;
        Ok(())
    }

    /// Check if a user has permission to access a resource
    pub fn check_permission<'a, S: Store<'a> + 'static>(
        &self,
        user_identity: &UserIdentity,
        resource_id: ResourceId,
        action: Action,
        store: &'a S,
        txn: &'a <S as Store<'a>>::Txn,
    ) -> Result<Path> {
        // Resolve user identity to permission ULID
        let permission_ulid = self.resolve_permission_ulid(user_identity, store, txn)?;

        // Retrieve path from resource mapping
        let key = resource_id.to_string();
        let path_bytes = store
            .get(txn, RESOURCE_DB, key.as_bytes())
            .map_err(|_| PermissionError::ResourceNotFound(resource_id.to_string()))?
            .ok_or_else(|| PermissionError::ResourceNotFound(resource_id.to_string()))?;

        let path = Path::try_from(path_bytes.as_ref())?;

        // Check permission using the enforcer (read lock)
        let allowed = {
            let enforcer = self.enforcer.read();
            enforcer.enforce(
                &permission_ulid.to_string(),
                &path.to_string(),
                &action.to_string(),
            )?
        };

        if allowed {
            Ok(path)
        } else {
            Err(PermissionError::PermissionDenied)
        }
    }

    /// Prepare to create a new group with default admin and member roles
    pub fn create_group_prepare<'a, S: Store<'a> + 'static>(
        &self,
        group_id: Ulid,
        initial_user: &UserIdentity,
        realm_id: Ulid,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<CreateGroupPrepare> {
        // Resolve user identity to permission ULID
        let permission_ulid = self.resolve_permission_ulid(initial_user, store, txn)?;

        let admin_role = format!("{}_admin", group_id);
        let member_role = format!("{}_member", group_id);

        // Create admin role with full group access
        let admin_path = Path::builder()
            .realm_id(realm_id)
            .group_wildcard(group_id)
            .build()?;

        let admin_role_policy = vec![
            admin_role.clone(),
            admin_path.to_string(),
            "write".to_string(),
            "allow".to_string(),
        ];

        store.put(
            txn,
            DBNAME,
            format!("p:{}", admin_role_policy.join(":")).as_bytes(),
            &[],
        )?;

        // Create member role with resources-only access
        let member_path = Path::builder()
            .realm_id(realm_id)
            .group_resources_wildcard(group_id)
            .build()?;

        let member_role_policy = vec![
            member_role.clone(),
            member_path.to_string(),
            "write".to_string(),
            "allow".to_string(),
        ];

        store.put(
            txn,
            DBNAME,
            format!("p:{}", member_role_policy.join(":")).as_bytes(),
            &[],
        )?;

        let member_role_mapping = vec![permission_ulid.to_string(), admin_role];

        store.put(
            txn,
            DBNAME,
            format!("g:{}", member_role_mapping.join(":")).as_bytes(),
            &[],
        )?;

        Ok(CreateGroupPrepare {
            policy: vec![
                CasbinPolicy::new(admin_role_policy)?,
                CasbinPolicy::new(member_role_policy)?,
            ],
            role: vec![CasbinRole::new(member_role_mapping)?],
        })
    }

    /// Commit a group creation operation
    pub async fn create_group_commit(&self, request: CreateGroupPrepare) -> Result<()> {
        let mut enforcer = self.enforcer.write();

        for policy in request.policy {
            enforcer.add_policy(policy).await?;
        }

        for role in request.role {
            enforcer.add_role(role).await?;
        }

        Ok(())
    }

    /// Prepare to add a user to a group role
    pub fn add_user_prepare<'a, S: Store<'a> + 'static>(
        &self,
        group_id: Ulid,
        user_identity: &UserIdentity,
        role_name: &str,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<AddUserPrepare> {
        // Resolve user identity to permission ULID
        let permission_ulid = self.resolve_permission_ulid(user_identity, store, txn)?;

        let full_role = format!("{}_{}", group_id, role_name);

        // Check if role exists by checking if it has any policies
        let policies = {
            let enforcer = self.enforcer.read();
            enforcer.get_policies()
        };

        let role_exists = policies
            .iter()
            .filter_map(|p| p.get(0))
            .any(|r| r == &full_role);

        if !role_exists && role_name != "admin" && role_name != "member" {
            return Err(PermissionError::RoleNotFound(full_role));
        }

        let role_mapping = vec![permission_ulid.to_string(), full_role];

        store.put(
            txn,
            DBNAME,
            format!("g:{}", role_mapping.join(":")).as_bytes(),
            &[],
        )?;

        Ok(AddUserPrepare {
            role: CasbinRole::new(role_mapping)?,
        })
    }

    /// Commit adding a user to a role
    pub async fn add_user_commit(&self, request: AddUserPrepare) -> Result<()> {
        let mut enforcer = self.enforcer.write();
        enforcer.add_role(request.role).await?;
        Ok(())
    }

    /// Prepare to remove a role from a user
    pub fn remove_role_from_user_prepare<'a, S: Store<'a> + 'static>(
        &self,
        group_id: Ulid,
        user_identity: &UserIdentity,
        role_name: &str,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<RemoveRolePrepare> {
        // Resolve user identity to permission ULID
        let permission_ulid = self.resolve_permission_ulid(user_identity, store, txn)?;

        let full_role = format!("{}_{}", group_id, role_name);
        let role_mapping = vec![permission_ulid.to_string(), full_role];

        store.remove(
            txn,
            DBNAME,
            format!("g:{}", role_mapping.join(":")).as_bytes(),
        )?;

        Ok(RemoveRolePrepare {
            role: CasbinRole::new(role_mapping)?,
        })
    }

    /// Commit removing a role from a user
    pub async fn remove_role_from_user_commit(&self, request: RemoveRolePrepare) -> Result<()> {
        let mut enforcer = self.enforcer.write();
        enforcer.remove_role(request.role).await?;
        Ok(())
    }

    /// Prepare to add a policy to a role
    pub fn add_policy_to_role_prepare<'a, S: Store<'a> + 'static>(
        &self,
        group_id: Ulid,
        role_name: &str,
        path: &Path,
        action: &str,
        effect: Option<&str>,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<AddPolicyPrepare> {
        let full_role = format!("{}_{}", group_id, role_name);
        let effect = effect.unwrap_or("allow");

        let policy = vec![
            full_role,
            path.to_string(),
            action.to_string(),
            effect.to_string(),
        ];

        store.put(
            txn,
            DBNAME,
            format!("p:{}", policy.join(":")).as_bytes(),
            &[],
        )?;

        Ok(AddPolicyPrepare {
            policy: CasbinPolicy::new(policy)?,
        })
    }

    /// Commit adding a policy to a role
    pub async fn add_policy_to_role_commit(&self, request: AddPolicyPrepare) -> Result<()> {
        let mut enforcer = self.enforcer.write();
        enforcer.add_policy(request.policy).await?;
        Ok(())
    }

    /// Add a resource mapping from ID to path
    pub fn add_resource<'a, S: Store<'a> + 'static>(
        &self,
        resource_id: ResourceId,
        path: &Path,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        if path.has_wildcards() {
            return Err(PermissionError::PathError(PathError::InvalidAssumption(
                "Resource paths should not contain wildcards".to_string(),
            )));
        }

        let key = resource_id.to_string();
        let path_bytes: Vec<u8> = path.into();

        store.put(txn, RESOURCE_DB, key.as_bytes(), &path_bytes)?;

        Ok(())
    }

    /// Remove a resource mapping
    pub fn remove_resource<'a, S: Store<'a> + 'static>(
        &self,
        resource_id: ResourceId,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let key = resource_id.to_string();
        store.remove(txn, RESOURCE_DB, key.as_bytes())?;
        Ok(())
    }

    /// Convenience method: Create group with both prepare and commit
    pub async fn create_group<'a, S: Store<'a> + 'static>(
        &self,
        group_id: Ulid,
        initial_user: &UserIdentity,
        realm_id: Ulid,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let prepare = self.create_group_prepare(group_id, initial_user, realm_id, store, txn)?;
        self.create_group_commit(prepare).await
    }

    /// Convenience method: Add user with both prepare and commit
    pub async fn add_user<'a, S: Store<'a> + 'static>(
        &self,
        group_id: Ulid,
        user_identity: &UserIdentity,
        role_name: &str,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let prepare = self.add_user_prepare(group_id, user_identity, role_name, store, txn)?;
        self.add_user_commit(prepare).await
    }

    /// Convenience method: Remove role from user with both prepare and commit
    pub async fn remove_role_from_user<'a, S: Store<'a> + 'static>(
        &self,
        group_id: Ulid,
        user_identity: &UserIdentity,
        role_name: &str,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let prepare =
            self.remove_role_from_user_prepare(group_id, user_identity, role_name, store, txn)?;
        self.remove_role_from_user_commit(prepare).await
    }

    /// Convenience method: Add policy to role with both prepare and commit
    pub async fn add_policy_to_role<'a, S: Store<'a> + 'static>(
        &self,
        group_id: Ulid,
        role_name: &str,
        path: &Path,
        action: &str,
        effect: Option<&str>,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let prepare =
            self.add_policy_to_role_prepare(group_id, role_name, path, action, effect, store, txn)?;
        self.add_policy_to_role_commit(prepare).await
    }

    /// Get all roles for a group
    pub fn get_group_roles(&self, group_id: Ulid) -> Vec<String> {
        let prefix = format!("{}_", group_id);
        let policies = {
            let enforcer = self.enforcer.read();
            enforcer.get_policies()
        };

        policies
            .iter()
            .filter_map(|p| p.get(0))
            .filter(|role| role.starts_with(&prefix))
            .map(|role| role.trim_start_matches(&prefix).to_string())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Get all users in a group role (returns permission ULIDs)
    pub fn get_role_users(&self, group_id: Ulid, role_name: &str) -> Vec<String> {
        let full_role = format!("{}_{}", group_id, role_name);
        let enforcer = self.enforcer.read();
        enforcer.get_users_for_role(&full_role)
    }

    /// Get all policies for a role
    pub fn get_role_policies(&self, group_id: Ulid, role_name: &str) -> Vec<Vec<String>> {
        let full_role = format!("{}_{}", group_id, role_name);
        let enforcer = self.enforcer.read();

        enforcer
            .get_policies()
            .into_iter()
            .filter_map(|p| {
                if p.get(0) == Some(&full_role) {
                    Some(p.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Load existing policies from storage
    pub async fn load_policies<'a, S: Store<'a> + 'static>(
        &self,
        store: &'a S,
        txn: &'a <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let mut enforcer = self.enforcer.write();
        enforcer.load_policy(store, txn).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
                crate::DBNAME,
                crate::RESOURCE_DB,
                crate::OIDC_IDENTITIES_DB,
                crate::IDENTITY_PERMISSIONS_DB,
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

    #[tokio::test]
    async fn test_oidc_identity_management() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let manager = PermissionManager::new().await.unwrap();

        let user_ulid = create_test_ulid(1);
        let provider = "google";
        let sub = "user123@example.com";

        // Test adding OIDC identity
        manager
            .add_oidc_identity(provider, sub, user_ulid, &store, &mut txn)
            .unwrap();

        // Test retrieving OIDC identity
        let found_user = manager
            .get_user_from_oidc(provider, sub, &store, &txn)
            .unwrap();
        assert_eq!(found_user, Some(user_ulid));

        // Test non-existent OIDC identity
        let not_found = manager
            .get_user_from_oidc("github", "unknown", &store, &txn)
            .unwrap();
        assert_eq!(not_found, None);

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_identity_permission_mapping() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let manager = PermissionManager::new().await.unwrap();

        let realm_id = create_test_ulid(10);
        let user_ulid = create_test_ulid(1);
        let permission_ulid = create_test_ulid(100);

        let user_identity = UserIdentity::new(user_ulid, realm_id);

        // Test adding identity permission mapping
        manager
            .add_identity_permission(&user_identity, permission_ulid, &store, &mut txn)
            .unwrap();

        // Test resolving permission ULID (internal method)
        let resolved = manager
            .resolve_permission_ulid(&user_identity, &store, &txn)
            .unwrap();
        assert_eq!(resolved, permission_ulid);

        // Test fallback to user ULID when no mapping exists
        let other_user = UserIdentity::new(create_test_ulid(2), realm_id);
        let fallback = manager
            .resolve_permission_ulid(&other_user, &store, &txn)
            .unwrap();
        assert_eq!(fallback, other_user.user_ulid);

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_create_group_with_user_identity() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let manager = PermissionManager::new().await.unwrap();

        let realm_id = create_test_ulid(10);
        let group_id = create_test_ulid(20);
        let admin_user_ulid = create_test_ulid(1);

        let admin_identity = UserIdentity::new(admin_user_ulid, realm_id);

        // Test group creation with UserIdentity
        manager
            .create_group(group_id, &admin_identity, realm_id, &store, &mut txn)
            .await
            .unwrap();

        // Verify group roles were created
        let roles = manager.get_group_roles(group_id);
        assert!(roles.contains(&"admin".to_string()));
        assert!(roles.contains(&"member".to_string()));

        // Verify admin user has admin role (should be their permission ULID)
        let admins = manager.get_role_users(group_id, "admin");
        let expected_permission_ulid = manager
            .resolve_permission_ulid(&admin_identity, &store, &txn)
            .unwrap();
        assert!(admins.contains(&expected_permission_ulid.to_string()));

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_permission_checking_with_identity() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let manager = PermissionManager::new().await.unwrap();

        let realm_id = create_test_ulid(10);
        let group_id = create_test_ulid(20);
        let admin_identity = UserIdentity::new(create_test_ulid(1), realm_id);
        let resource_id = create_test_ulid(30);

        // Create group and add resource
        manager
            .create_group(group_id, &admin_identity, realm_id, &store, &mut txn)
            .await
            .unwrap();

        let resource_path = Path::builder()
            .realm_id(realm_id)
            .group_metadata(group_id, resource_id, vec![])
            .build()
            .unwrap();

        manager
            .add_resource(
                ResourceId::Ulid(resource_id),
                &resource_path,
                &store,
                &mut txn,
            )
            .unwrap();

        // Test permission check
        let result = manager.check_permission(
            &admin_identity,
            ResourceId::Ulid(resource_id),
            Action::Write,
            &store,
            &txn,
        );
        assert!(result.is_ok());

        // Test with non-member user
        let outsider = UserIdentity::new(create_test_ulid(99), realm_id);
        let result = manager.check_permission(
            &outsider,
            ResourceId::Ulid(resource_id),
            Action::Write,
            &store,
            &txn,
        );
        assert!(matches!(result, Err(PermissionError::PermissionDenied)));

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_prepare_commit_workflow() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let manager = PermissionManager::new().await.unwrap();

        let realm_id = create_test_ulid(10);
        let group_id = create_test_ulid(20);
        let admin_identity = UserIdentity::new(create_test_ulid(1), realm_id);

        // Test prepare phase
        let prepare = manager
            .create_group_prepare(group_id, &admin_identity, realm_id, &store, &mut txn)
            .unwrap();

        // Verify Casbin enforcer not updated yet
        assert!(manager.get_group_roles(group_id).is_empty());

        // Test commit phase
        manager.create_group_commit(prepare).await.unwrap();

        // Verify Casbin enforcer was updated
        let roles = manager.get_group_roles(group_id);
        assert!(roles.contains(&"admin".to_string()));
        assert!(roles.contains(&"member".to_string()));

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_user_identity_serialization() {
        let realm_id = create_test_ulid(10);
        let user_ulid = create_test_ulid(1);
        let identity = UserIdentity::new(user_ulid, realm_id);

        // Test postcard serialization (used for storage keys)
        let serialized = postcard::to_allocvec(&identity).unwrap();
        let deserialized: UserIdentity = postcard::from_bytes(&serialized).unwrap();

        assert_eq!(identity, deserialized);
        assert_eq!(identity.user_ulid, deserialized.user_ulid);
        assert_eq!(identity.realm_ulid, deserialized.realm_ulid);
    }

    #[tokio::test]
    async fn test_error_handling() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let manager = PermissionManager::new().await.unwrap();

        let realm_id = create_test_ulid(10);
        let group_id = create_test_ulid(20);
        let admin_identity = UserIdentity::new(create_test_ulid(1), realm_id);
        let user_identity = UserIdentity::new(create_test_ulid(2), realm_id);

        // Create group first
        manager
            .create_group(group_id, &admin_identity, realm_id, &store, &mut txn)
            .await
            .unwrap();

        // Try to add user to non-existent role
        let result = manager
            .add_user(group_id, &user_identity, "nonexistent", &store, &mut txn)
            .await;
        assert!(matches!(result, Err(PermissionError::RoleNotFound(_))));

        // Test resource with wildcards rejected
        let resource_id = create_test_ulid(30);
        let wildcard_path = Path::builder()
            .realm_id(realm_id)
            .group_wildcard(group_id)
            .build()
            .unwrap();

        let result = manager.add_resource(
            ResourceId::Ulid(resource_id),
            &wildcard_path,
            &store,
            &mut txn,
        );
        assert!(matches!(result, Err(PermissionError::PathError(_))));

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }
}
