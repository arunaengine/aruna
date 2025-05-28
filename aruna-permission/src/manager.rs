use aruna_storage::storage::store::Store;
use blake3::Hash as Blake3Hash;
use std::fmt::Display;
use ulid::Ulid;

use crate::casbin::DBNAME;
use crate::casbin_helper::{CasbinPolicy, CasbinRole};
use crate::error::{PermissionError, Result};
use crate::{casbin::Enforcer, paths::Path};

pub const RESOURCE_DB: &str = "resources";

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
pub struct PermissionManager {
    pub enforcer: Enforcer,
}

impl PermissionManager {
    pub async fn new() -> Result<Self> {
        let enforcer = Enforcer::new().await?;
        Ok(Self { enforcer })
    }

    /// Check if a user has permission to access a resource
    pub fn check_permission<'a, S: Store<'a> + 'static>(
        &self,
        resource_id: ResourceId,
        user_id: Ulid,
        action: Action,
        store: &'a S,
        txn: &'a <S as Store<'a>>::Txn,
    ) -> Result<Path> {
        // Retrieve path from resource mapping
        let key = resource_id.to_string();
        let path_bytes = store
            .get(txn, RESOURCE_DB, key.as_bytes())
            .map_err(|_| PermissionError::ResourceNotFound(resource_id.to_string()))?
            .ok_or_else(|| PermissionError::ResourceNotFound(resource_id.to_string()))?;

        let path = Path::try_from(path_bytes.as_ref())?;

        // Check permission
        let allowed =
            self.enforcer
                .enforce(&user_id.to_string(), &path.to_string(), &action.to_string())?;

        if allowed {
            Ok(path)
        } else {
            Err(PermissionError::PermissionDenied)
        }
    }

    /// Prepare to create a new group with default admin and member roles
    pub fn create_group_prepare<'a, S: Store<'a> + 'static>(
        &mut self,
        group_id: Ulid,
        initial_user_id: Ulid,
        realm_id: Ulid,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<CreateGroupPrepare> {
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

        let member_role_mapping = vec![initial_user_id.to_string(), admin_role];

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
    pub async fn create_group_commit(&mut self, request: CreateGroupPrepare) -> Result<()> {
        for policy in request.policy {
            self.enforcer.add_policy(policy).await?;
        }

        for role in request.role {
            self.enforcer.add_role(role).await?;
        }

        Ok(())
    }

    /// Prepare to add a user to a group role
    pub fn add_user_prepare<'a, S: Store<'a> + 'static>(
        &mut self,
        group_id: Ulid,
        user_id: Ulid,
        role_name: &str,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<AddUserPrepare> {
        let full_role = format!("{}_{}", group_id, role_name);

        // Check if role exists by checking if it has any policies
        let policies = self.enforcer.get_policies();
        let role_exists = policies
            .iter()
            .filter_map(|p| p.get(0))
            .any(|r| r == &full_role);

        if !role_exists && role_name != "admin" && role_name != "member" {
            return Err(PermissionError::RoleNotFound(full_role));
        }

        let role_mapping = vec![user_id.to_string(), full_role];

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
    pub async fn add_user_commit(&mut self, request: AddUserPrepare) -> Result<()> {
        self.enforcer.add_role(request.role).await?;
        Ok(())
    }

    /// Prepare to remove a role from a user
    pub fn remove_role_from_user_prepare<'a, S: Store<'a> + 'static>(
        &mut self,
        group_id: Ulid,
        user_id: Ulid,
        role_name: &str,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<RemoveRolePrepare> {
        let full_role = format!("{}_{}", group_id, role_name);
        let role_mapping = vec![user_id.to_string(), full_role];

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
    pub async fn remove_role_from_user_commit(&mut self, request: RemoveRolePrepare) -> Result<()> {
        self.enforcer.remove_role(request.role).await?;
        Ok(())
    }

    /// Prepare to add a policy to a role
    pub fn add_policy_to_role_prepare<'a, S: Store<'a> + 'static>(
        &mut self,
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
    pub async fn add_policy_to_role_commit(&mut self, request: AddPolicyPrepare) -> Result<()> {
        self.enforcer.add_policy(request.policy).await?;
        Ok(())
    }

    /// Add a resource mapping from ID to path
    pub fn add_resource<'a, S: Store<'a> + 'static>(
        &mut self,
        resource_id: ResourceId,
        path: &Path,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let key = resource_id.to_string();
        let path_bytes: Vec<u8> = path.into();

        store.put(txn, RESOURCE_DB, key.as_bytes(), &path_bytes)?;

        Ok(())
    }

    /// Remove a resource mapping
    pub fn remove_resource<'a, S: Store<'a> + 'static>(
        &mut self,
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
        &mut self,
        group_id: Ulid,
        initial_user_id: Ulid,
        realm_id: Ulid,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let prepare = self.create_group_prepare(group_id, initial_user_id, realm_id, store, txn)?;
        self.create_group_commit(prepare).await
    }

    /// Convenience method: Add user with both prepare and commit
    pub async fn add_user<'a, S: Store<'a> + 'static>(
        &mut self,
        group_id: Ulid,
        user_id: Ulid,
        role_name: &str,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let prepare = self.add_user_prepare(group_id, user_id, role_name, store, txn)?;
        self.add_user_commit(prepare).await
    }

    /// Convenience method: Remove role from user with both prepare and commit
    pub async fn remove_role_from_user<'a, 'b: 'a, S: Store<'a> + 'static>(
        &mut self,
        group_id: Ulid,
        user_id: Ulid,
        role_name: &str,
        store: &'a S,
        txn: &'b mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let prepare =
            self.remove_role_from_user_prepare(group_id, user_id, role_name, store, txn)?;
        self.remove_role_from_user_commit(prepare).await
    }

    /// Convenience method: Add policy to role with both prepare and commit
    pub async fn add_policy_to_role<'a, S: Store<'a> + 'static>(
        &mut self,
        group_id: Ulid,
        role_name: &str,
        path: &Path,
        action: &str,
        effect: Option<&str>,
        store: &'a S,
        txn: &'a mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let prepare =
            self.add_policy_to_role_prepare(group_id, role_name, path, action, effect, store, txn)?;
        self.add_policy_to_role_commit(prepare).await
    }

    /// Get all roles for a group
    pub fn get_group_roles(&self, group_id: Ulid) -> Vec<String> {
        let prefix = format!("{}_", group_id);
        let policies = self.enforcer.get_policies();

        policies
            .iter()
            .filter_map(|p| p.get(0))
            .filter(|role| role.starts_with(&prefix))
            .map(|role| role.trim_start_matches(&prefix).to_string())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Get all users in a group role
    pub fn get_role_users(&self, group_id: Ulid, role_name: &str) -> Vec<String> {
        let full_role = format!("{}_{}", group_id, role_name);
        self.enforcer.get_users_for_role(&full_role)
    }

    /// Get all policies for a role
    pub fn get_role_policies(&self, group_id: Ulid, role_name: &str) -> Vec<Vec<String>> {
        let full_role = format!("{}_{}", group_id, role_name);

        self.enforcer
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
        &mut self,
        store: &'a S,
        txn: &'a <S as Store<'a>>::Txn,
    ) -> Result<()> {
        self.enforcer.load_policy(store, txn).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::casbin::DBNAME;
    use aruna_storage::storage::lmdb::{LmdbConfig, LmdbStore};

    async fn setup_test_store() -> (LmdbStore, String) {
        let test_id = ulid::Ulid::new().to_string();
        let test_dir = format!("/dev/shm/test_mgr_{}", test_id);
        std::fs::create_dir_all(&test_dir).unwrap();

        let config = LmdbConfig {
            path: test_dir.clone(),
            databases: vec![DBNAME, RESOURCE_DB],
        };

        let store = LmdbStore::new(config).unwrap();
        (store, test_dir)
    }

    fn cleanup_test_dir(path: &str) {
        std::fs::remove_dir_all(path).unwrap_or(());
    }

    #[tokio::test]
    async fn test_create_group_prepare_commit() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let mut manager = PermissionManager::new().await.unwrap();

        let realm_id = Ulid::new();
        let group_id = Ulid::new();
        let admin_user = Ulid::new();

        // Test prepare phase
        let prepare = manager
            .create_group_prepare(group_id, admin_user, realm_id, &store, &mut txn)
            .unwrap();

        // Verify storage was updated but Casbin enforcer wasn't yet
        assert!(manager.get_group_roles(group_id).is_empty());

        // Test commit phase
        manager.create_group_commit(prepare).await.unwrap();

        // Verify Casbin enforcer was updated
        let roles = manager.get_group_roles(group_id);
        assert!(roles.contains(&"admin".to_string()));
        assert!(roles.contains(&"member".to_string()));

        let admins = manager.get_role_users(group_id, "admin");
        assert!(admins.contains(&admin_user.to_string()));

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_add_user_prepare_commit() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let mut manager = PermissionManager::new().await.unwrap();

        let realm_id = Ulid::new();
        let group_id = Ulid::new();
        let admin_user = Ulid::new();
        let member_user = Ulid::new();

        // Create group first
        manager
            .create_group(group_id, admin_user, realm_id, &store, &mut txn)
            .await
            .unwrap();

        // Test prepare phase for adding member
        let prepare = manager
            .add_user_prepare(group_id, member_user, "member", &store, &mut txn)
            .unwrap();

        // Verify user not yet in role in Casbin
        let members_before = manager.get_role_users(group_id, "member");
        assert!(!members_before.contains(&member_user.to_string()));

        // Test commit phase
        manager.add_user_commit(prepare).await.unwrap();

        // Verify user is now in role
        let members_after = manager.get_role_users(group_id, "member");
        assert!(members_after.contains(&member_user.to_string()));

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_add_user_prepare_invalid_role() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let mut manager = PermissionManager::new().await.unwrap();

        let realm_id = Ulid::new();
        let group_id = Ulid::new();
        let admin_user = Ulid::new();
        let member_user = Ulid::new();

        // Create group first
        manager
            .create_group(group_id, admin_user, realm_id, &store, &mut txn)
            .await
            .unwrap();

        // Test prepare phase with invalid role
        let result =
            manager.add_user_prepare(group_id, member_user, "nonexistent", &store, &mut txn);

        assert!(matches!(result, Err(PermissionError::RoleNotFound(_))));

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_remove_role_prepare_commit() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let mut manager = PermissionManager::new().await.unwrap();

        let realm_id = Ulid::new();
        let group_id = Ulid::new();
        let admin_user = Ulid::new();
        let member_user = Ulid::new();

        // Create group and add member
        manager
            .create_group(group_id, admin_user, realm_id, &store, &mut txn)
            .await
            .unwrap();
        manager
            .add_user(group_id, member_user, "member", &store, &mut txn)
            .await
            .unwrap();

        // Verify member is in role
        let members_before = manager.get_role_users(group_id, "member");
        assert!(members_before.contains(&member_user.to_string()));

        // Test prepare phase for removing role
        let prepare = manager
            .remove_role_from_user_prepare(group_id, member_user, "member", &store, &mut txn)
            .unwrap();

        // Member should still be in role in Casbin before commit
        let members_middle = manager.get_role_users(group_id, "member");
        assert!(members_middle.contains(&member_user.to_string()));

        // Test commit phase
        manager.remove_role_from_user_commit(prepare).await.unwrap();

        // Verify member is no longer in role
        let members_after = manager.get_role_users(group_id, "member");
        assert!(!members_after.contains(&member_user.to_string()));

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_add_policy_prepare_commit() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let mut manager = PermissionManager::new().await.unwrap();

        let realm_id = Ulid::new();
        let group_id = Ulid::new();
        let admin_user = Ulid::new();

        // Create group first
        manager
            .create_group(group_id, admin_user, realm_id, &store, &mut txn)
            .await
            .unwrap();

        // Create a custom path for testing
        let custom_path = Path::builder()
            .realm_id(realm_id)
            .group_metadata_wildcard(group_id)
            .build()
            .unwrap();

        // Test prepare phase for adding policy
        let prepare = manager
            .add_policy_to_role_prepare(
                group_id,
                "custom_role",
                &custom_path,
                "read",
                Some("allow"),
                &store,
                &mut txn,
            )
            .unwrap();

        // Policy should not be in Casbin yet
        let policies_before = manager.get_role_policies(group_id, "custom_role");
        assert!(policies_before.is_empty());

        // Test commit phase
        manager.add_policy_to_role_commit(prepare).await.unwrap();

        // Policy should now be in Casbin
        let policies_after = manager.get_role_policies(group_id, "custom_role");
        assert_eq!(policies_after.len(), 1);

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_add_resource_prepare_commit() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let mut manager = PermissionManager::new().await.unwrap();

        let realm_id = Ulid::new();
        let group_id = Ulid::new();
        let admin_user = Ulid::new();
        let resource_id = Ulid::new();

        // Create group first
        manager
            .create_group(group_id, admin_user, realm_id, &store, &mut txn)
            .await
            .unwrap();

        let resource_path = Path::builder()
            .realm_id(realm_id)
            .group_metadata(group_id, resource_id, vec![])
            .build()
            .unwrap();

        // Test prepare phase
        manager
            .add_resource(
                ResourceId::Ulid(resource_id),
                &resource_path,
                &store,
                &mut txn,
            )
            .unwrap();

        // Verify resource can be accessed after commit
        let result = manager.check_permission(
            ResourceId::Ulid(resource_id),
            admin_user,
            Action::Write,
            &store,
            &txn,
        );
        assert!(result.is_ok());

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_remove_resource_prepare_commit() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let mut manager = PermissionManager::new().await.unwrap();

        let realm_id = Ulid::new();
        let group_id = Ulid::new();
        let admin_user = Ulid::new();
        let resource_id = Ulid::new();

        // Create group and add resource
        manager
            .create_group(group_id, admin_user, realm_id, &store, &mut txn)
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

        // Verify resource exists
        let result_before = manager.check_permission(
            ResourceId::Ulid(resource_id),
            admin_user,
            Action::Write,
            &store,
            &txn,
        );
        assert!(result_before.is_ok());

        // Test prepare phase
        manager
            .remove_resource(ResourceId::Ulid(resource_id), &store, &mut txn)
            .unwrap();

        txn.commit().unwrap();

        // Verify resource is gone after commit
        let txn = store.create_txn(false).unwrap();
        let result_after = manager.check_permission(
            ResourceId::Ulid(resource_id),
            admin_user,
            Action::Write,
            &store,
            &txn,
        );
        assert!(matches!(
            result_after,
            Err(PermissionError::ResourceNotFound(_))
        ));

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_convenience_methods_still_work() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let mut manager = PermissionManager::new().await.unwrap();

        let realm_id = Ulid::new();
        let group_id = Ulid::new();
        let admin_user = Ulid::new();
        let member_user = Ulid::new();

        // Test convenience method for create_group
        manager
            .create_group(group_id, admin_user, realm_id, &store, &mut txn)
            .await
            .unwrap();

        // Test convenience method for add_user
        manager
            .add_user(group_id, member_user, "member", &store, &mut txn)
            .await
            .unwrap();

        // Verify operations worked
        let roles = manager.get_group_roles(group_id);
        assert!(roles.contains(&"admin".to_string()));
        assert!(roles.contains(&"member".to_string()));

        let members = manager.get_role_users(group_id, "member");
        assert!(members.contains(&member_user.to_string()));

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_complex_prepare_commit_workflow() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let mut manager = PermissionManager::new().await.unwrap();

        let realm_id = Ulid::new();
        let group_id = Ulid::new();
        let admin_user = Ulid::new();
        let editor_user = Ulid::new();
        let viewer_user = Ulid::new();

        // Prepare multiple operations but don't commit yet
        let group_prepare = manager
            .create_group_prepare(group_id, admin_user, realm_id, &store, &mut txn)
            .unwrap();

        let editor_prepare = manager
            .add_user_prepare(group_id, editor_user, "member", &store, &mut txn)
            .unwrap();

        let custom_path = Path::builder()
            .realm_id(realm_id)
            .group_metadata_wildcard(group_id)
            .build()
            .unwrap();

        let policy_prepare = manager
            .add_policy_to_role_prepare(
                group_id,
                "viewer",
                &custom_path,
                "read",
                Some("allow"),
                &store,
                &mut txn,
            )
            .unwrap();

        // At this point, storage is updated but Casbin enforcer is not
        assert!(manager.get_group_roles(group_id).is_empty());

        manager
            .add_policy_to_role_commit(policy_prepare)
            .await
            .unwrap();

        let viewer_prepare = manager
            .add_user_prepare(group_id, viewer_user, "viewer", &store, &mut txn)
            .unwrap();

        // Commit all operations
        manager.create_group_commit(group_prepare).await.unwrap();
        manager.add_user_commit(editor_prepare).await.unwrap();
        manager.add_user_commit(viewer_prepare).await.unwrap();

        // Verify all operations took effect
        let roles = manager.get_group_roles(group_id);
        assert!(roles.contains(&"admin".to_string()));
        assert!(roles.contains(&"member".to_string()));
        assert!(roles.contains(&"viewer".to_string()));

        let members = manager.get_role_users(group_id, "member");
        assert!(members.contains(&editor_user.to_string()));

        let viewers = manager.get_role_users(group_id, "viewer");
        assert!(viewers.contains(&viewer_user.to_string()));

        let viewer_policies = manager.get_role_policies(group_id, "viewer");
        assert_eq!(viewer_policies.len(), 1);

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_batch_operations_with_prepare_commit() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let mut manager = PermissionManager::new().await.unwrap();

        let realm_id = Ulid::new();
        let group1_id = Ulid::new();
        let group2_id = Ulid::new();
        let admin1 = Ulid::new();
        let admin2 = Ulid::new();
        let shared_user = Ulid::new();

        // Prepare multiple group creations
        let group1_prepare = manager
            .create_group_prepare(group1_id, admin1, realm_id, &store, &mut txn)
            .unwrap();

        let group2_prepare = manager
            .create_group_prepare(group2_id, admin2, realm_id, &store, &mut txn)
            .unwrap();

        // Prepare adding shared user to both groups
        let user1_prepare = manager
            .add_user_prepare(group1_id, shared_user, "member", &store, &mut txn)
            .unwrap();

        let user2_prepare = manager
            .add_user_prepare(group2_id, shared_user, "member", &store, &mut txn)
            .unwrap();

        // Verify nothing is in Casbin yet
        assert!(manager.get_group_roles(group1_id).is_empty());
        assert!(manager.get_group_roles(group2_id).is_empty());

        // Commit all operations as a batch
        manager.create_group_commit(group1_prepare).await.unwrap();
        manager.create_group_commit(group2_prepare).await.unwrap();
        manager.add_user_commit(user1_prepare).await.unwrap();
        manager.add_user_commit(user2_prepare).await.unwrap();

        // Verify all operations succeeded
        assert!(!manager.get_group_roles(group1_id).is_empty());
        assert!(!manager.get_group_roles(group2_id).is_empty());

        let group1_members = manager.get_role_users(group1_id, "member");
        let group2_members = manager.get_role_users(group2_id, "member");
        assert!(group1_members.contains(&shared_user.to_string()));
        assert!(group2_members.contains(&shared_user.to_string()));

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_error_handling_in_prepare_operations() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let mut manager = PermissionManager::new().await.unwrap();

        let realm_id = Ulid::new();
        let group_id = Ulid::new();
        let admin_user = Ulid::new();
        let member_user = Ulid::new();

        // First create a group
        manager
            .create_group(group_id, admin_user, realm_id, &store, &mut txn)
            .await
            .unwrap();

        // Try to add user to non-existent role
        let result =
            manager.add_user_prepare(group_id, member_user, "nonexistent_role", &store, &mut txn);
        assert!(matches!(result, Err(PermissionError::RoleNotFound(_))));

        // Verify the error didn't affect existing state
        let roles = manager.get_group_roles(group_id);
        assert!(roles.contains(&"admin".to_string()));
        assert!(roles.contains(&"member".to_string()));

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }
}
