use std::fmt::Display;

use aruna_storage::storage::store::Store;
use blake3::Hash as Blake3Hash;
use ulid::Ulid;

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

/// Higher-level permission manager that builds on top of the base Enforcer
pub struct PermissionManager<'a, S: Store<'a> + Send + Sync + 'static> {
    pub enforcer: Enforcer<'a, S>,
}

impl<'a, S: Store<'a>> std::fmt::Debug for PermissionManager<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallStore").finish()
    }
}

impl<'a, S: Store<'a>> PermissionManager<'a, S> {
    pub async fn new(store: &'a S) -> Result<Self> {
        let enforcer = Enforcer::new(store).await?;
        Ok(Self { enforcer })
    }

    /// Check if a user has permission to access a resource
    pub async fn check_permission(
        &self,
        resource_id: ResourceId,
        user_id: Ulid,
        action: &str,
        txn: &<S as Store<'a>>::Txn,
    ) -> Result<Path> {
        // Retrieve path from resource mapping
        let key = resource_id.to_string();
        let path_bytes = self
            .enforcer
            .store
            .get(txn, RESOURCE_DB, key.as_bytes())
            .map_err(|_| PermissionError::ResourceNotFound(resource_id.to_string()))?
            .ok_or_else(|| PermissionError::ResourceNotFound(resource_id.to_string()))?;

        let path = Path::try_from(path_bytes.as_ref())?;

        // Check permission
        let allowed = self
            .enforcer
            .enforce(&user_id.to_string(), &path.to_string(), action)
            .await?;

        if allowed {
            Ok(path)
        } else {
            Err(PermissionError::PermissionDenied)
        }
    }

    /// Create a new group with default admin and member roles
    pub async fn create_group(
        &mut self,
        group_id: Ulid,
        initial_user_id: Ulid,
        realm_id: Ulid,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let admin_role = format!("{}_admin", group_id);
        let member_role = format!("{}_member", group_id);

        // Create admin role with full group access
        let admin_path = Path::builder()
            .realm_id(realm_id)
            .group_wildcard(group_id)
            .build()?;

        self.enforcer
            .add_policy(
                &admin_role,
                &admin_path.to_string(),
                "write",
                Some("allow"),
                txn,
            )
            .await?;

        // Create member role with resources-only access
        let member_path = Path::builder()
            .realm_id(realm_id)
            .group_resources_wildcard(group_id)
            .build()?;

        self.enforcer
            .add_policy(
                &member_role,
                &member_path.to_string(),
                "write",
                Some("allow"),
                txn,
            )
            .await?;

        // Add initial user as admin
        self.enforcer
            .add_role(&initial_user_id.to_string(), &admin_role, txn)
            .await?;

        Ok(())
    }

    /// Add a user to a group role
    pub async fn add_user(
        &mut self,
        group_id: Ulid,
        user_id: Ulid,
        role_name: &str,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let full_role = format!("{}_{}", group_id, role_name);

        // Check if role exists by checking if it has any policies
        let policies = self.enforcer.get_policies().await;
        let role_exists = policies
            .iter()
            .filter_map(|p| p.get(0))
            .any(|r| r == &full_role);

        if !role_exists && role_name != "admin" && role_name != "member" {
            return Err(PermissionError::RoleNotFound(full_role));
        }

        self.enforcer
            .add_role(&user_id.to_string(), &full_role, txn)
            .await?;
        Ok(())
    }

    /// Remove a role from a user
    pub async fn remove_role_from_user(
        &mut self,
        group_id: Ulid,
        user_id: Ulid,
        role_name: &str,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let full_role = format!("{}_{}", group_id, role_name);
        self.enforcer
            .remove_role(&user_id.to_string(), &full_role, txn)
            .await?;
        Ok(())
    }

    /// Add a policy to a role
    pub async fn add_policy_to_role(
        &mut self,
        group_id: Ulid,
        role_name: &str,
        path: &Path,
        action: &str,
        effect: Option<&str>,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let full_role = format!("{}_{}", group_id, role_name);
        self.enforcer
            .add_policy(&full_role, &path.to_string(), action, effect, txn)
            .await?;
        Ok(())
    }

    /// Add a resource mapping from ID to path
    pub async fn add_resource(
        &mut self,
        resource_id: ResourceId,
        path: &Path,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let key = resource_id.to_string();
        let path_bytes: Vec<u8> = path.into();

        self.enforcer
            .store
            .put(txn, RESOURCE_DB, key.as_bytes(), &path_bytes)?;

        Ok(())
    }

    /// Remove a resource mapping
    pub async fn remove_resource(
        &mut self,
        resource_id: ResourceId,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let key = resource_id.to_string();
        self.enforcer
            .store
            .remove(txn, RESOURCE_DB, key.as_bytes())?;
        Ok(())
    }

    /// Get all roles for a group
    pub async fn get_group_roles(&self, group_id: Ulid) -> Vec<String> {
        let prefix = format!("{}_", group_id);
        let policies = self.enforcer.get_policies().await;

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
    pub async fn get_role_users(&self, group_id: Ulid, role_name: &str) -> Vec<String> {
        let full_role = format!("{}_{}", group_id, role_name);
        self.enforcer.get_users_for_role(&full_role).await
    }

    /// Get all policies for a role
    pub async fn get_role_policies(&self, group_id: Ulid, role_name: &str) -> Vec<Vec<String>> {
        let full_role = format!("{}_{}", group_id, role_name);

        self.enforcer
            .get_policies()
            .await
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
    pub async fn load_policies(&mut self, txn: &<S as Store<'a>>::Txn) -> Result<()> {
        self.enforcer.load_policy(txn).await?;
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
    async fn test_comprehensive_group_management() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();

        let mut manager = PermissionManager::new(&store).await.unwrap();

        let realm_id = Ulid::new();
        let group1_id = Ulid::new();
        let group2_id = Ulid::new();
        let admin_user = Ulid::new();
        let member_user = Ulid::new();
        let external_user = Ulid::new();

        // Create first group with admin
        manager
            .create_group(group1_id, admin_user, realm_id, &mut txn)
            .await
            .unwrap();

        // Create second group with different admin
        manager
            .create_group(group2_id, external_user, realm_id, &mut txn)
            .await
            .unwrap();

        // Add member to first group
        manager
            .add_user(group1_id, member_user, "member", &mut txn)
            .await
            .unwrap();

        // Add member to second group
        manager
            .add_user(group2_id, member_user, "member", &mut txn)
            .await
            .unwrap();

        // Verify group roles were created correctly
        let group1_roles = manager.get_group_roles(group1_id).await;
        assert!(group1_roles.contains(&"admin".to_string()));
        assert!(group1_roles.contains(&"member".to_string()));

        let group2_roles = manager.get_group_roles(group2_id).await;
        assert!(group2_roles.contains(&"admin".to_string()));
        assert!(group2_roles.contains(&"member".to_string()));

        // Verify user assignments
        let group1_admins = manager.get_role_users(group1_id, "admin").await;
        assert!(group1_admins.contains(&admin_user.to_string()));
        assert!(!group1_admins.contains(&external_user.to_string()));

        let group1_members = manager.get_role_users(group1_id, "member").await;
        assert!(group1_members.contains(&member_user.to_string()));
        assert!(!group1_members.contains(&admin_user.to_string()));

        // Test removing role from user
        manager
            .remove_role_from_user(group1_id, member_user, "member", &mut txn)
            .await
            .unwrap();

        let group1_members_after = manager.get_role_users(group1_id, "member").await;
        assert!(!group1_members_after.contains(&member_user.to_string()));

        // Test error when adding user to non-existent role
        let result = manager
            .add_user(group1_id, member_user, "nonexistent", &mut txn)
            .await;
        assert!(matches!(result, Err(PermissionError::RoleNotFound(_))));

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_resource_management_and_permissions() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();

        let mut manager = PermissionManager::new(&store).await.unwrap();

        let realm_id = Ulid::new();
        let group_id = Ulid::new();
        let admin_user = Ulid::new();
        let member_user = Ulid::new();
        let outsider_user = Ulid::new();

        // Create group structure
        manager
            .create_group(group_id, admin_user, realm_id, &mut txn)
            .await
            .unwrap();

        manager
            .add_user(group_id, member_user, "member", &mut txn)
            .await
            .unwrap();

        // Create various resource types
        let project_id = Ulid::new();
        let folder_id = Ulid::new();
        let document_id = Ulid::new();
        let data_hash = blake3::hash(b"test data content");

        // Metadata resource path (project with nested folders)
        let metadata_path = Path::builder()
            .realm_id(realm_id)
            .group_metadata(group_id, project_id, vec![folder_id, document_id])
            .build()
            .unwrap();

        // Data resource path
        let data_path = Path::builder()
            .realm_id(realm_id)
            .group_data(group_id, data_hash)
            .build()
            .unwrap();

        // Admin resource path
        let admin_path = Path::builder()
            .realm_id(realm_id)
            .group_admin(group_id)
            .build()
            .unwrap();

        // Add resources to manager
        manager
            .add_resource(ResourceId::Ulid(document_id), &metadata_path, &mut txn)
            .await
            .unwrap();

        manager
            .add_resource(ResourceId::ContentHash(data_hash), &data_path, &mut txn)
            .await
            .unwrap();

        let admin_resource_id = Ulid::new();
        manager
            .add_resource(ResourceId::Ulid(admin_resource_id), &admin_path, &mut txn)
            .await
            .unwrap();

        txn.commit().unwrap();

        // Test permissions with read-only transaction
        let txn = store.create_txn(false).unwrap();

        // Admin should have access to all resources
        let admin_metadata_result = manager
            .check_permission(ResourceId::Ulid(document_id), admin_user, "write", &txn)
            .await;
        assert!(admin_metadata_result.is_ok());

        let admin_data_result = manager
            .check_permission(
                ResourceId::ContentHash(data_hash),
                admin_user,
                "write",
                &txn,
            )
            .await;
        assert!(admin_data_result.is_ok());

        let admin_admin_result = manager
            .check_permission(
                ResourceId::Ulid(admin_resource_id),
                admin_user,
                "write",
                &txn,
            )
            .await;
        assert!(admin_admin_result.is_ok());

        // Member should have access to resources but not admin functions
        let member_metadata_result = manager
            .check_permission(ResourceId::Ulid(document_id), member_user, "write", &txn)
            .await;
        assert!(member_metadata_result.is_ok());

        let member_data_result = manager
            .check_permission(
                ResourceId::ContentHash(data_hash),
                member_user,
                "write",
                &txn,
            )
            .await;
        assert!(member_data_result.is_ok());

        let member_admin_result = manager
            .check_permission(
                ResourceId::Ulid(admin_resource_id),
                member_user,
                "write",
                &txn,
            )
            .await;
        assert!(matches!(
            member_admin_result,
            Err(PermissionError::PermissionDenied)
        ));

        // Outsider should have no access
        let outsider_result = manager
            .check_permission(ResourceId::Ulid(document_id), outsider_user, "read", &txn)
            .await;
        assert!(matches!(
            outsider_result,
            Err(PermissionError::PermissionDenied)
        ));

        // Test non-existent resource
        let nonexistent_result = manager
            .check_permission(ResourceId::Ulid(Ulid::new()), outsider_user, "read", &txn)
            .await;
        assert!(matches!(
            nonexistent_result,
            Err(PermissionError::ResourceNotFound(_))
        ));

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_custom_role_and_policy_management() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();

        let mut manager = PermissionManager::new(&store).await.unwrap();

        let realm_id = Ulid::new();
        let group_id = Ulid::new();
        let admin_user = Ulid::new();
        let editor_user = Ulid::new();
        let viewer_user = Ulid::new();

        // Create group
        manager
            .create_group(group_id, admin_user, realm_id, &mut txn)
            .await
            .unwrap();

        // Create custom "editor" role with specific permissions
        let editor_path = Path::builder()
            .realm_id(realm_id)
            .group_resources_wildcard(group_id)
            .build()
            .unwrap();

        manager
            .add_policy_to_role(
                group_id,
                "editor",
                &editor_path,
                "write",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();

        // Create restricted "viewer" role
        let viewer_metadata_path = Path::builder()
            .realm_id(realm_id)
            .group_metadata_wildcard(group_id)
            .build()
            .unwrap();

        manager
            .add_policy_to_role(
                group_id,
                "viewer",
                &viewer_metadata_path,
                "read",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();

        // Add users to custom roles
        manager
            .add_user(group_id, editor_user, "editor", &mut txn)
            .await
            .unwrap();

        manager
            .add_user(group_id, viewer_user, "viewer", &mut txn)
            .await
            .unwrap();

        // Create test resources
        let project_id = Ulid::new();
        let document_id = Ulid::new();
        let data_hash = blake3::hash(b"editor test data");

        let metadata_resource_path = Path::builder()
            .realm_id(realm_id)
            .group_metadata(group_id, project_id, vec![document_id])
            .build()
            .unwrap();

        let data_resource_path = Path::builder()
            .realm_id(realm_id)
            .group_data(group_id, data_hash)
            .build()
            .unwrap();

        manager
            .add_resource(
                ResourceId::Ulid(document_id),
                &metadata_resource_path,
                &mut txn,
            )
            .await
            .unwrap();

        manager
            .add_resource(
                ResourceId::ContentHash(data_hash),
                &data_resource_path,
                &mut txn,
            )
            .await
            .unwrap();

        txn.commit().unwrap();

        // Test custom role permissions
        let txn = store.create_txn(false).unwrap();

        // Editor should have write access to resources
        let editor_metadata_write = manager
            .check_permission(ResourceId::Ulid(document_id), editor_user, "write", &txn)
            .await;
        assert!(editor_metadata_write.is_ok());

        let editor_data_write = manager
            .check_permission(
                ResourceId::ContentHash(data_hash),
                editor_user,
                "write",
                &txn,
            )
            .await;
        assert!(editor_data_write.is_ok());

        // Viewer should have read access to metadata but not write
        let viewer_metadata_read = manager
            .check_permission(ResourceId::Ulid(document_id), viewer_user, "read", &txn)
            .await;
        viewer_metadata_read.unwrap();

        let viewer_metadata_write = manager
            .check_permission(ResourceId::Ulid(document_id), viewer_user, "write", &txn)
            .await;
        assert!(matches!(
            viewer_metadata_write,
            Err(PermissionError::PermissionDenied)
        ));

        // Viewer should not have access to data resources (only metadata)
        let viewer_data_read = manager
            .check_permission(
                ResourceId::ContentHash(data_hash),
                viewer_user,
                "read",
                &txn,
            )
            .await;
        assert!(matches!(
            viewer_data_read,
            Err(PermissionError::PermissionDenied)
        ));

        // Verify role policies
        let editor_policies = manager.get_role_policies(group_id, "editor").await;
        assert_eq!(editor_policies.len(), 1);

        let viewer_policies = manager.get_role_policies(group_id, "viewer").await;
        assert_eq!(viewer_policies.len(), 1);
        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_hierarchical_permissions_and_wildcards() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();

        let mut manager = PermissionManager::new(&store).await.unwrap();

        let realm_id = Ulid::new();
        let group_id = Ulid::new();
        let admin_user = Ulid::new();
        let project_manager = Ulid::new();
        let developer = Ulid::new();

        // Create group
        manager
            .create_group(group_id, admin_user, realm_id, &mut txn)
            .await
            .unwrap();

        // Create project manager role with project-level wildcard access
        let project_id = Ulid::new();
        let project_wildcard_path = Path::builder()
            .realm_id(realm_id)
            .group_metadata_project_wildcard(group_id, project_id)
            .build()
            .unwrap();

        manager
            .add_policy_to_role(
                group_id,
                "project_manager",
                &project_wildcard_path,
                "write",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();

        // Create developer role with specific folder access
        let folder_id = Ulid::new();
        let specific_path = Path::builder()
            .realm_id(realm_id)
            .group_metadata_specific_path_wildcard(group_id, project_id, vec![folder_id])
            .build()
            .unwrap();

        manager
            .add_policy_to_role(
                group_id,
                "developer",
                &specific_path,
                "write",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();

        // Add users to roles
        manager
            .add_user(group_id, project_manager, "project_manager", &mut txn)
            .await
            .unwrap();

        manager
            .add_user(group_id, developer, "developer", &mut txn)
            .await
            .unwrap();

        // Create test resources at different hierarchy levels
        let doc_in_folder = Ulid::new();
        let doc_in_root = Ulid::new();
        let doc_in_other_folder = Ulid::new();
        let other_folder_id = Ulid::new();

        let folder_doc_path = Path::builder()
            .realm_id(realm_id)
            .group_metadata(group_id, project_id, vec![folder_id, doc_in_folder])
            .build()
            .unwrap();

        let root_doc_path = Path::builder()
            .realm_id(realm_id)
            .group_metadata(group_id, project_id, vec![doc_in_root])
            .build()
            .unwrap();

        let other_folder_doc_path = Path::builder()
            .realm_id(realm_id)
            .group_metadata(
                group_id,
                project_id,
                vec![other_folder_id, doc_in_other_folder],
            )
            .build()
            .unwrap();

        manager
            .add_resource(ResourceId::Ulid(doc_in_folder), &folder_doc_path, &mut txn)
            .await
            .unwrap();

        manager
            .add_resource(ResourceId::Ulid(doc_in_root), &root_doc_path, &mut txn)
            .await
            .unwrap();

        manager
            .add_resource(
                ResourceId::Ulid(doc_in_other_folder),
                &other_folder_doc_path,
                &mut txn,
            )
            .await
            .unwrap();

        txn.commit().unwrap();

        // Test hierarchical permissions
        let txn = store.create_txn(false).unwrap();

        // Admin should have access to everything (via group wildcard)
        assert!(
            manager
                .check_permission(ResourceId::Ulid(doc_in_folder), admin_user, "write", &txn)
                .await
                .is_ok()
        );
        assert!(
            manager
                .check_permission(ResourceId::Ulid(doc_in_root), admin_user, "write", &txn)
                .await
                .is_ok()
        );
        assert!(
            manager
                .check_permission(
                    ResourceId::Ulid(doc_in_other_folder),
                    admin_user,
                    "write",
                    &txn
                )
                .await
                .is_ok()
        );

        // Project manager should have access to all documents in the project
        assert!(
            manager
                .check_permission(
                    ResourceId::Ulid(doc_in_folder),
                    project_manager,
                    "write",
                    &txn
                )
                .await
                .is_ok()
        );
        assert!(
            manager
                .check_permission(
                    ResourceId::Ulid(doc_in_root),
                    project_manager,
                    "write",
                    &txn
                )
                .await
                .is_ok()
        );
        assert!(
            manager
                .check_permission(
                    ResourceId::Ulid(doc_in_other_folder),
                    project_manager,
                    "write",
                    &txn
                )
                .await
                .is_ok()
        );

        // Developer should only have access to documents in their specific folder
        assert!(
            manager
                .check_permission(ResourceId::Ulid(doc_in_folder), developer, "write", &txn)
                .await
                .is_ok()
        );

        assert!(matches!(
            manager
                .check_permission(ResourceId::Ulid(doc_in_root), developer, "write", &txn)
                .await,
            Err(PermissionError::PermissionDenied)
        ));

        assert!(matches!(
            manager
                .check_permission(
                    ResourceId::Ulid(doc_in_other_folder),
                    developer,
                    "write",
                    &txn
                )
                .await,
            Err(PermissionError::PermissionDenied)
        ));

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_cross_group_scenario_and_realm_isolation() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();

        let mut manager = PermissionManager::new(&store).await.unwrap();

        // Multiple realms to test isolation
        let realm1_id = Ulid::new();
        let realm2_id = Ulid::new();

        // Groups in different realms
        let group1_id = Ulid::new();
        let group2_id = Ulid::new();
        let group3_id = Ulid::new(); // Same realm as group1

        let admin1 = Ulid::new();
        let admin2 = Ulid::new();
        let admin3 = Ulid::new();
        let cross_realm_user = Ulid::new();

        // Create groups in different realms
        manager
            .create_group(group1_id, admin1, realm1_id, &mut txn)
            .await
            .unwrap();

        manager
            .create_group(group2_id, admin2, realm2_id, &mut txn)
            .await
            .unwrap();

        manager
            .create_group(group3_id, admin3, realm1_id, &mut txn)
            .await
            .unwrap();

        // Add cross-realm user to multiple groups
        manager
            .add_user(group1_id, cross_realm_user, "member", &mut txn)
            .await
            .unwrap();

        manager
            .add_user(group2_id, cross_realm_user, "member", &mut txn)
            .await
            .unwrap();

        // Create resources in different realms and groups
        let resource1_id = Ulid::new();
        let resource2_id = Ulid::new();
        let resource3_id = Ulid::new();

        let project_id1 = Ulid::new();
        let project_id2 = Ulid::new();
        let project_id3 = Ulid::new();

        let resource1_path = Path::builder()
            .realm_id(realm1_id)
            .group_metadata(group1_id, project_id1, vec![resource1_id])
            .build()
            .unwrap();

        let resource2_path = Path::builder()
            .realm_id(realm2_id)
            .group_metadata(group2_id, project_id2, vec![resource2_id])
            .build()
            .unwrap();

        let resource3_path = Path::builder()
            .realm_id(realm1_id)
            .group_metadata(group3_id, project_id3, vec![resource3_id])
            .build()
            .unwrap();

        manager
            .add_resource(ResourceId::Ulid(resource1_id), &resource1_path, &mut txn)
            .await
            .unwrap();

        manager
            .add_resource(ResourceId::Ulid(resource2_id), &resource2_path, &mut txn)
            .await
            .unwrap();

        manager
            .add_resource(ResourceId::Ulid(resource3_id), &resource3_path, &mut txn)
            .await
            .unwrap();

        txn.commit().unwrap();

        // Test cross-realm access
        let txn = store.create_txn(false).unwrap();

        // Cross-realm user should have access to resources in groups they belong to
        assert!(
            manager
                .check_permission(
                    ResourceId::Ulid(resource1_id),
                    cross_realm_user,
                    "write",
                    &txn
                )
                .await
                .is_ok()
        );

        assert!(
            manager
                .check_permission(
                    ResourceId::Ulid(resource2_id),
                    cross_realm_user,
                    "write",
                    &txn
                )
                .await
                .is_ok()
        );

        // But not to resources in groups they don't belong to
        assert!(matches!(
            manager
                .check_permission(
                    ResourceId::Ulid(resource3_id),
                    cross_realm_user,
                    "write",
                    &txn
                )
                .await,
            Err(PermissionError::PermissionDenied)
        ));

        // Group admins should only access their own group resources
        assert!(
            manager
                .check_permission(ResourceId::Ulid(resource1_id), admin1, "write", &txn)
                .await
                .is_ok()
        );

        assert!(matches!(
            manager
                .check_permission(ResourceId::Ulid(resource2_id), admin1, "write", &txn)
                .await,
            Err(PermissionError::PermissionDenied)
        ));

        assert!(matches!(
            manager
                .check_permission(ResourceId::Ulid(resource3_id), admin1, "write", &txn)
                .await,
            Err(PermissionError::PermissionDenied)
        ));

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_policy_persistence_and_loading() {
        let (store, test_dir) = setup_test_store().await;

        let realm_id = Ulid::new();
        let group_id = Ulid::new();
        let admin_user = Ulid::new();
        let resource_id = Ulid::new();

        // First session: create and persist data
        {
            let mut txn = store.create_txn(true).unwrap();
            let mut manager = PermissionManager::new(&store).await.unwrap();

            manager
                .create_group(group_id, admin_user, realm_id, &mut txn)
                .await
                .unwrap();

            let custom_path = Path::builder()
                .realm_id(realm_id)
                .group_metadata_wildcard(group_id)
                .build()
                .unwrap();

            manager
                .add_policy_to_role(
                    group_id,
                    "custom_role",
                    &custom_path,
                    "read",
                    Some("allow"),
                    &mut txn,
                )
                .await
                .unwrap();

            let resource_path = Path::builder()
                .realm_id(realm_id)
                .group_metadata(group_id, resource_id, vec![])
                .build()
                .unwrap();

            manager
                .add_resource(ResourceId::Ulid(resource_id), &resource_path, &mut txn)
                .await
                .unwrap();

            txn.commit().unwrap();
        }

        // Second session: load persisted data
        {
            let txn = store.create_txn(false).unwrap();
            let mut manager = PermissionManager::new(&store).await.unwrap();

            // Load policies from storage
            manager.load_policies(&txn).await.unwrap();

            // Verify that persisted data is accessible
            let roles = manager.get_group_roles(group_id).await;
            assert!(roles.contains(&"admin".to_string()));
            assert!(roles.contains(&"member".to_string()));

            let admins = manager.get_role_users(group_id, "admin").await;
            assert!(admins.contains(&admin_user.to_string()));

            // Test that permissions still work
            let permission_result = manager
                .check_permission(ResourceId::Ulid(resource_id), admin_user, "write", &txn)
                .await;
            assert!(permission_result.is_ok());

            txn.commit().unwrap();
        }

        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_complex_enterprise_scenario() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();

        let mut manager = PermissionManager::new(&store).await.unwrap();

        // Enterprise setup with multiple departments and projects
        let company_realm = Ulid::new();

        let engineering_group = Ulid::new();
        let marketing_group = Ulid::new();
        let finance_group = Ulid::new();

        let eng_director = Ulid::new();
        let marketing_director = Ulid::new();
        let finance_director = Ulid::new();

        let senior_dev = Ulid::new();
        let junior_dev = Ulid::new();
        let qa_engineer = Ulid::new();
        let product_manager = Ulid::new();
        let marketing_manager = Ulid::new();
        let content_creator = Ulid::new();
        let accountant = Ulid::new();

        // Create department groups
        manager
            .create_group(engineering_group, eng_director, company_realm, &mut txn)
            .await
            .unwrap();

        manager
            .create_group(marketing_group, marketing_director, company_realm, &mut txn)
            .await
            .unwrap();

        manager
            .create_group(finance_group, finance_director, company_realm, &mut txn)
            .await
            .unwrap();

        // Create specialized roles within engineering
        let senior_dev_path = Path::builder()
            .realm_id(company_realm)
            .group_resources_wildcard(engineering_group)
            .build()
            .unwrap();

        let qa_path = Path::builder()
            .realm_id(company_realm)
            .group_metadata_wildcard(engineering_group)
            .build()
            .unwrap();

        manager
            .add_policy_to_role(
                engineering_group,
                "senior_developer",
                &senior_dev_path,
                "write",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();

        manager
            .add_policy_to_role(
                engineering_group,
                "qa_engineer",
                &qa_path,
                "read",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();

        // Junior developers can only work on specific projects
        let junior_project_id = Ulid::new();
        let junior_project_path = Path::builder()
            .realm_id(company_realm)
            .group_metadata_project_wildcard(engineering_group, junior_project_id)
            .build()
            .unwrap();

        manager
            .add_policy_to_role(
                engineering_group,
                "junior_developer",
                &junior_project_path,
                "write",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();

        // Product managers have read access across engineering but write access to specs
        let pm_read_path = Path::builder()
            .realm_id(company_realm)
            .group_metadata_wildcard(engineering_group)
            .build()
            .unwrap();

        manager
            .add_policy_to_role(
                engineering_group,
                "product_manager",
                &pm_read_path,
                "read",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();

        // Marketing roles
        let marketing_content_path = Path::builder()
            .realm_id(company_realm)
            .group_resources_wildcard(marketing_group)
            .build()
            .unwrap();

        manager
            .add_policy_to_role(
                marketing_group,
                "content_creator",
                &marketing_content_path,
                "write",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();

        // Finance has restricted access patterns
        let finance_reports_path = Path::builder()
            .realm_id(company_realm)
            .group_metadata_wildcard(finance_group)
            .build()
            .unwrap();

        manager
            .add_policy_to_role(
                finance_group,
                "accountant",
                &finance_reports_path,
                "write",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();

        // Cross-department collaboration: Product manager can read marketing materials
        let marketing_read_path = Path::builder()
            .realm_id(company_realm)
            .group_metadata_wildcard(marketing_group)
            .build()
            .unwrap();

        manager
            .add_policy_to_role(
                marketing_group,
                "external_reader",
                &marketing_read_path,
                "read",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();

        // Assign users to roles
        manager
            .add_user(engineering_group, senior_dev, "senior_developer", &mut txn)
            .await
            .unwrap();

        manager
            .add_user(engineering_group, junior_dev, "junior_developer", &mut txn)
            .await
            .unwrap();

        manager
            .add_user(engineering_group, qa_engineer, "qa_engineer", &mut txn)
            .await
            .unwrap();

        manager
            .add_user(
                engineering_group,
                product_manager,
                "product_manager",
                &mut txn,
            )
            .await
            .unwrap();

        manager
            .add_user(marketing_group, marketing_manager, "admin", &mut txn)
            .await
            .unwrap();

        manager
            .add_user(
                marketing_group,
                content_creator,
                "content_creator",
                &mut txn,
            )
            .await
            .unwrap();

        manager
            .add_user(finance_group, accountant, "accountant", &mut txn)
            .await
            .unwrap();

        // Cross-department access
        manager
            .add_user(
                marketing_group,
                product_manager,
                "external_reader",
                &mut txn,
            )
            .await
            .unwrap();

        // Create various enterprise resources
        let main_product_id = Ulid::new();
        let mobile_app_id = Ulid::new();
        let campaign_id = Ulid::new();
        let financial_report_id = Ulid::new();

        let codebase_hash = blake3::hash(b"main product codebase");
        let campaign_assets_hash = blake3::hash(b"campaign assets");

        // Engineering resources
        let main_code_path = Path::builder()
            .realm_id(company_realm)
            .group_data(engineering_group, codebase_hash)
            .build()
            .unwrap();

        let main_docs_path = Path::builder()
            .realm_id(company_realm)
            .group_metadata(engineering_group, main_product_id, vec![])
            .build()
            .unwrap();

        let junior_project_path = Path::builder()
            .realm_id(company_realm)
            .group_metadata(engineering_group, junior_project_id, vec![mobile_app_id])
            .build()
            .unwrap();

        // Marketing resources
        let campaign_assets_path = Path::builder()
            .realm_id(company_realm)
            .group_data(marketing_group, campaign_assets_hash)
            .build()
            .unwrap();

        let campaign_docs_path = Path::builder()
            .realm_id(company_realm)
            .group_metadata(marketing_group, campaign_id, vec![])
            .build()
            .unwrap();

        // Finance resources
        let financial_report_path = Path::builder()
            .realm_id(company_realm)
            .group_metadata(finance_group, financial_report_id, vec![])
            .build()
            .unwrap();

        // Add all resources
        manager
            .add_resource(
                ResourceId::ContentHash(codebase_hash),
                &main_code_path,
                &mut txn,
            )
            .await
            .unwrap();

        manager
            .add_resource(ResourceId::Ulid(main_product_id), &main_docs_path, &mut txn)
            .await
            .unwrap();

        manager
            .add_resource(
                ResourceId::Ulid(mobile_app_id),
                &junior_project_path,
                &mut txn,
            )
            .await
            .unwrap();

        manager
            .add_resource(
                ResourceId::ContentHash(campaign_assets_hash),
                &campaign_assets_path,
                &mut txn,
            )
            .await
            .unwrap();

        manager
            .add_resource(ResourceId::Ulid(campaign_id), &campaign_docs_path, &mut txn)
            .await
            .unwrap();

        manager
            .add_resource(
                ResourceId::Ulid(financial_report_id),
                &financial_report_path,
                &mut txn,
            )
            .await
            .unwrap();

        txn.commit().unwrap();

        // Test complex enterprise permissions
        let txn = store.create_txn(false).unwrap();

        // Senior developer should have broad access within engineering
        assert!(
            manager
                .check_permission(
                    ResourceId::ContentHash(codebase_hash),
                    senior_dev,
                    "write",
                    &txn
                )
                .await
                .is_ok()
        );

        assert!(
            manager
                .check_permission(ResourceId::Ulid(main_product_id), senior_dev, "write", &txn)
                .await
                .is_ok()
        );

        assert!(
            manager
                .check_permission(ResourceId::Ulid(mobile_app_id), senior_dev, "write", &txn)
                .await
                .is_ok()
        );

        // Junior developer should only access their assigned project
        assert!(
            manager
                .check_permission(ResourceId::Ulid(mobile_app_id), junior_dev, "write", &txn)
                .await
                .is_ok()
        );

        assert!(matches!(
            manager
                .check_permission(ResourceId::Ulid(main_product_id), junior_dev, "write", &txn)
                .await,
            Err(PermissionError::PermissionDenied)
        ));

        assert!(matches!(
            manager
                .check_permission(
                    ResourceId::ContentHash(codebase_hash),
                    junior_dev,
                    "write",
                    &txn
                )
                .await,
            Err(PermissionError::PermissionDenied)
        ));

        // QA engineer should have read access to engineering metadata
        assert!(
            manager
                .check_permission(ResourceId::Ulid(main_product_id), qa_engineer, "read", &txn)
                .await
                .is_ok()
        );

        assert!(matches!(
            manager
                .check_permission(
                    ResourceId::Ulid(main_product_id),
                    qa_engineer,
                    "write",
                    &txn
                )
                .await,
            Err(PermissionError::PermissionDenied)
        ));

        // Product manager should read engineering and marketing
        assert!(
            manager
                .check_permission(
                    ResourceId::Ulid(main_product_id),
                    product_manager,
                    "read",
                    &txn
                )
                .await
                .is_ok()
        );

        assert!(
            manager
                .check_permission(ResourceId::Ulid(campaign_id), product_manager, "read", &txn)
                .await
                .is_ok()
        );

        assert!(matches!(
            manager
                .check_permission(
                    ResourceId::Ulid(main_product_id),
                    product_manager,
                    "write",
                    &txn
                )
                .await,
            Err(PermissionError::PermissionDenied)
        ));

        // Department isolation: engineering can't access finance
        assert!(matches!(
            manager
                .check_permission(
                    ResourceId::Ulid(financial_report_id),
                    senior_dev,
                    "read",
                    &txn
                )
                .await,
            Err(PermissionError::PermissionDenied)
        ));

        // Finance isolation: accountant can't access engineering
        assert!(matches!(
            manager
                .check_permission(ResourceId::Ulid(main_product_id), accountant, "read", &txn)
                .await,
            Err(PermissionError::PermissionDenied)
        ));

        // But accountant should access finance resources
        assert!(
            manager
                .check_permission(
                    ResourceId::Ulid(financial_report_id),
                    accountant,
                    "write",
                    &txn
                )
                .await
                .is_ok()
        );

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_resource_removal_and_error_scenarios() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();

        let mut manager = PermissionManager::new(&store).await.unwrap();

        let realm_id = Ulid::new();
        let group_id = Ulid::new();
        let admin_user = Ulid::new();
        let resource_id = Ulid::new();

        manager
            .create_group(group_id, admin_user, realm_id, &mut txn)
            .await
            .unwrap();

        let resource_path = Path::builder()
            .realm_id(realm_id)
            .group_metadata(group_id, resource_id, vec![])
            .build()
            .unwrap();

        manager
            .add_resource(ResourceId::Ulid(resource_id), &resource_path, &mut txn)
            .await
            .unwrap();

        // Verify resource exists and is accessible
        assert!(
            manager
                .check_permission(ResourceId::Ulid(resource_id), admin_user, "write", &txn)
                .await
                .is_ok()
        );

        // Remove the resource
        manager
            .remove_resource(ResourceId::Ulid(resource_id), &mut txn)
            .await
            .unwrap();

        txn.commit().unwrap();

        // After removal, resource should not be found
        let txn = store.create_txn(false).unwrap();
        let result = manager
            .check_permission(ResourceId::Ulid(resource_id), admin_user, "write", &txn)
            .await;

        assert!(matches!(result, Err(PermissionError::ResourceNotFound(_))));

        // Test removing non-existent resource (should not error)
        let mut txn = store.create_txn(true).unwrap();
        let non_existent_id = Ulid::new();
        let removal_result = manager
            .remove_resource(ResourceId::Ulid(non_existent_id), &mut txn)
            .await;
        assert!(removal_result.is_ok());

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_allow_deny_precedence_in_manager() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();

        let mut manager = PermissionManager::new(&store).await.unwrap();

        let realm_id = Ulid::new();
        let group_id = Ulid::new();
        let admin_user = Ulid::new();
        let restricted_user = Ulid::new();

        manager
            .create_group(group_id, admin_user, realm_id, &mut txn)
            .await
            .unwrap();

        // Create broad allow policy
        let general_path = Path::builder()
            .realm_id(realm_id)
            .group_resources_wildcard(group_id)
            .build()
            .unwrap();

        manager
            .add_policy_to_role(
                group_id,
                "broad_access",
                &general_path,
                "write",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();

        // Create specific deny policy that should override
        let sensitive_project_id = Ulid::new();
        let sensitive_path = Path::builder()
            .realm_id(realm_id)
            .group_metadata_project_wildcard(group_id, sensitive_project_id)
            .build()
            .unwrap();

        manager
            .add_policy_to_role(
                group_id,
                "broad_access",
                &sensitive_path,
                "write",
                Some("deny"),
                &mut txn,
            )
            .await
            .unwrap();

        manager
            .add_user(group_id, restricted_user, "broad_access", &mut txn)
            .await
            .unwrap();

        // Create test resources
        let normal_resource_id = Ulid::new();
        let sensitive_resource_id = Ulid::new();

        let normal_path = Path::builder()
            .realm_id(realm_id)
            .group_metadata(group_id, Ulid::new(), vec![normal_resource_id])
            .build()
            .unwrap();

        let sensitive_resource_path = Path::builder()
            .realm_id(realm_id)
            .group_metadata(group_id, sensitive_project_id, vec![sensitive_resource_id])
            .build()
            .unwrap();

        manager
            .add_resource(ResourceId::Ulid(normal_resource_id), &normal_path, &mut txn)
            .await
            .unwrap();

        manager
            .add_resource(
                ResourceId::Ulid(sensitive_resource_id),
                &sensitive_resource_path,
                &mut txn,
            )
            .await
            .unwrap();

        txn.commit().unwrap();

        // Test allow/deny precedence
        let txn = store.create_txn(false).unwrap();

        // Should have access to normal resources (allow policy)
        assert!(
            manager
                .check_permission(
                    ResourceId::Ulid(normal_resource_id),
                    restricted_user,
                    "write",
                    &txn
                )
                .await
                .is_ok()
        );

        // Should be denied access to sensitive resources (deny overrides allow)
        assert!(matches!(
            manager
                .check_permission(
                    ResourceId::Ulid(sensitive_resource_id),
                    restricted_user,
                    "write",
                    &txn
                )
                .await,
            Err(PermissionError::PermissionDenied)
        ));

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }
}
