use aruna_storage::storage::store::Store;
use blake3::Hash as Blake3Hash;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::Arc;
use tokio::sync::RwLock;
use ulid::Ulid;

use crate::casbin::DBNAME;
use crate::casbin_helper::{CasbinPolicy, CasbinRole};
use crate::error::{ConversionError, PathError, PermissionError, Result, UnificationError};
use crate::{
    casbin::Enforcer,
    paths::{Path, RealmKey},
};

// Database constants
pub const RESOURCE_DB: &str = "permission_resources";
pub const OIDC_IDENTITIES_DB: &str = "oidc_identities";
pub const IDENTITY_PERMISSIONS_DB: &str = "identity_permissions";

/// User identity consisting of user ULID and realm key
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Ord, PartialOrd)]
pub struct UserIdentity {
    pub user_ulid: Ulid,
    pub realm_key: RealmKey,
}

impl UserIdentity {
    pub fn new(user_ulid: Ulid, realm_key: RealmKey) -> Self {
        Self {
            user_ulid,
            realm_key,
        }
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(self.user_ulid.to_bytes().as_slice());
        buf.extend_from_slice(self.realm_key.as_slice());
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (ulid_bytes, realm_key_bytes) = bytes.split_at(16);
        let sized_ulid: &[u8; 16] = ulid_bytes
            .try_into()
            .map_err(|e| ConversionError::InvalidSliceSize(e))?;
        let realm_key: &[u8; 32] = realm_key_bytes
            .try_into()
            .map_err(|e| ConversionError::InvalidSliceSize(e))?;
        Ok(Self {
            user_ulid: Ulid::from_bytes(*sized_ulid),
            realm_key: *realm_key,
        })
    }

    pub fn from_string(user_id: String) -> Result<Self> {
        let (ulid, realm_key) = user_id.split_once('@').ok_or_else(|| {
            ConversionError::InvalidFormat("Invalid format for UserIdentity".to_string())
        })?;
        let ulid = Ulid::from_string(ulid).map_err(|e| ConversionError::InvalidUlid(e))?;
        let realm_key = hex::decode(realm_key).map_err(|e| ConversionError::InvalidRealmKey(e))?;

        let realm_key: &[u8; 32] = realm_key
            .as_slice()
            .try_into()
            .map_err(|e| ConversionError::InvalidSliceSize(e))?;

        Ok(UserIdentity::new(ulid, *realm_key))
    }
}

impl Display for UserIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.user_ulid, hex::encode(self.realm_key))
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

/// Prepare token for unifying two identities (simplified)
pub struct UnifyIdentitiesPrepare {
    policies_to_add: Vec<CasbinPolicy>,
    roles_to_add: Vec<CasbinRole>,
    policies_to_remove: Vec<CasbinPolicy>,
    roles_to_remove: Vec<CasbinRole>,
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

    /// Resolve user identity to permission ULID (PRIVATE) - UPDATED
    pub fn resolve_permission_ulid<'a, S: Store<'a> + 'static>(
        &self,
        user_identity: &UserIdentity,
        store: &'a S,
        txn: &<S as Store<'a>>::Txn,
    ) -> Result<Ulid> {
        // Serialize UserIdentity for storage key
        let key = postcard::to_allocvec(user_identity)?;

        store
            .get(txn, IDENTITY_PERMISSIONS_DB, &key)
            .map_err(|e| PermissionError::StorageError(e))?
            .map(|value| {
                // Deserialize the permission ULID from the stored value
                postcard::from_bytes(&value).map_err(|e| PermissionError::PostcardError(e))
            })
            .ok_or_else(|| {
                // No fallback - explicit mapping required
                PermissionError::ResourceNotFound(
                    "Permission mapping not found for user identity".to_string(),
                )
            })?
    }

    /// Create a user identity with explicit permission mapping
    pub fn create_user_identity<'a, S: Store<'a> + 'static>(
        &self,
        user_identity: &UserIdentity,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<Ulid> {
        // Generate new permission ULID
        let permission_ulid = Ulid::new();

        // Create explicit mapping
        self.add_identity_permission(user_identity, permission_ulid, store, txn)?;
        println!(
            "CREATE_IDENTITY: 
{}
{}",
            user_identity, permission_ulid
        );

        Ok(permission_ulid)
    }

    /// Create a user identity with explicit permission mapping, or return existing one
    pub fn ensure_user_identity<'a, S: Store<'a> + 'static>(
        &self,
        user_identity: &UserIdentity,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<Ulid> {
        // Try to resolve existing mapping first
        match self.resolve_permission_ulid(user_identity, store, txn) {
            Ok(permission_ulid) => Ok(permission_ulid),
            Err(PermissionError::ResourceNotFound(_)) => {
                // Create new mapping if none exists
                self.create_user_identity(user_identity, store, txn)
            }
            Err(e) => Err(e),
        }
    }

    /// Find all UserIdentity mappings for a permission ULID
    fn get_user_identities_for_permission<'a, S: Store<'a> + 'static>(
        &self,
        permission_ulid: Ulid,
        store: &'a S,
        txn: &<S as Store<'a>>::Txn,
    ) -> Result<Vec<UserIdentity>> {
        let mut identities = Vec::new();

        for (key, value) in store
            .iter_db(txn, IDENTITY_PERMISSIONS_DB)
            .map_err(|e| PermissionError::StorageError(e))?
        {
            let stored_permission_ulid: Ulid = postcard::from_bytes(&value)?;

            if stored_permission_ulid == permission_ulid {
                let user_identity: UserIdentity = postcard::from_bytes(&key)?;
                identities.push(user_identity);
            }
        }

        Ok(identities)
    }

    /// Get all roles for a permission ULID
    pub async fn get_roles_for_permission(&self, permission_ulid: &str) -> Vec<Vec<String>> {
        let enforcer = self.enforcer.read().await;

        // Get all role assignments where this permission ULID is the subject
        enforcer
            .get_groups()
            .into_iter()
            .filter(|role| role.get(0) == Some(&permission_ulid.to_string()))
            .collect()
    }

    /// Get all policies for a permission ULID
    async fn get_policies_for_permission(&self, permission_ulid: &str) -> Vec<Vec<String>> {
        let enforcer = self.enforcer.read().await;

        // Get all policies where this permission ULID is the subject
        enforcer
            .get_policies()
            .into_iter()
            .filter(|policy| policy.get(0) == Some(&permission_ulid.to_string()))
            .collect()
    }

    /// Add OIDC identity mapping (realm-local only)
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

    /// Lookup user ULID from OIDC identity (realm-local only)
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

    /// Prepare to unify two identities (UPDATED - handles all storage)
    pub async fn unify_identities_prepare<'a, S: Store<'a> + 'static>(
        &self,
        identity1: &UserIdentity,
        identity2: &UserIdentity,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<UnifyIdentitiesPrepare> {
        // Validation
        if identity1 == identity2 {
            return Err(PermissionError::from(UnificationError::SelfUnification));
        }

        // Try to resolve both permission ULIDs
        let perm_ulid_1_result = self.resolve_permission_ulid(identity1, store, txn);
        let perm_ulid_2_result = self.resolve_permission_ulid(identity2, store, txn);

        match (&perm_ulid_1_result, &perm_ulid_2_result) {
            (Ok(perm_ulid_1), Ok(perm_ulid_2)) => {
                // Case 3: Both exist - check if already unified, then use complex migration logic
                if perm_ulid_1 == perm_ulid_2 {
                    return Err(PermissionError::from(UnificationError::AlreadyUnified));
                }
                // Continue with complex migration logic below...
            }
            (Ok(perm_ulid_1), Err(_)) => {
                // Case 1: Only identity1 exists - map identity2 to existing perm_ulid_1
                self.add_identity_permission(identity2, perm_ulid_1.clone(), store, txn)?;
                // Return empty migration (no policies/roles to move)
                return Ok(UnifyIdentitiesPrepare {
                    policies_to_add: Vec::new(),
                    roles_to_add: Vec::new(),
                    policies_to_remove: Vec::new(),
                    roles_to_remove: Vec::new(),
                });
            }
            (Err(_), Ok(perm_ulid_2)) => {
                // Case 2: Only identity2 exists - map identity1 to existing perm_ulid_2
                self.add_identity_permission(identity1, perm_ulid_2.clone(), store, txn)?;
                // Return empty migration (no policies/roles to move)
                return Ok(UnifyIdentitiesPrepare {
                    policies_to_add: Vec::new(),
                    roles_to_add: Vec::new(),
                    policies_to_remove: Vec::new(),
                    roles_to_remove: Vec::new(),
                });
            }
            (Err(_), Err(_)) => {
                // Case 4: Neither exists - this should be an error
                return Err(PermissionError::from(UnificationError::IdentityNotFound(
                    "Neither identity has an existing permission mapping".to_string(),
                )));
            }
        }

        // Complex migration logic for Case 3 (both identities exist with different permission ULIDs)
        let Ok(perm_ulid_1) = perm_ulid_1_result else {
            // This should never occur due to earlier checks, but handle gracefully
            return Err(PermissionError::from(UnificationError::IdentityNotFound(
                "Failed to resolve permission ULID for identity1".to_string(),
            )));
        };
        let Ok(perm_ulid_2) = perm_ulid_2_result else {
            // This should never occur due to earlier checks, but handle gracefully
            return Err(PermissionError::from(UnificationError::IdentityNotFound(
                "Failed to resolve permission ULID for identity2".to_string(),
            )));
        };

        // Generate new unified permission ULID
        let new_permission_ulid = Ulid::new();

        // Get all roles and policies for both old permission ULIDs
        let roles_1 = self
            .get_roles_for_permission(&perm_ulid_1.to_string())
            .await;
        let roles_2 = self
            .get_roles_for_permission(&perm_ulid_2.to_string())
            .await;
        let policies_1 = self
            .get_policies_for_permission(&perm_ulid_1.to_string())
            .await;
        let policies_2 = self
            .get_policies_for_permission(&perm_ulid_2.to_string())
            .await;

        // Prepare new roles and policies with unified permission ULID
        let mut roles_to_add = Vec::new();
        let mut policies_to_add = Vec::new();
        let mut roles_to_remove = Vec::new();
        let mut policies_to_remove = Vec::new();

        // Process roles (deduplicate identical ones)
        let mut seen_roles = std::collections::HashSet::new();

        for mut role in roles_1.into_iter().chain(roles_2.into_iter()) {
            // Create new role with unified permission ULID
            let old_role = role.clone();
            role[0] = new_permission_ulid.to_string(); // Replace subject

            let role_key = role[1..].join(":"); // Use role name as dedup key
            if seen_roles.insert(role_key) {
                roles_to_add.push(CasbinRole::new(role.clone())?);
                // UPDATE STORAGE: Add new role to storage
                store.put(txn, DBNAME, format!("g:{}", role.join(":")).as_bytes(), &[])?;
            }

            // Mark old role for removal and UPDATE STORAGE: Remove old role
            roles_to_remove.push(CasbinRole::new(old_role.clone())?);
            store.remove(txn, DBNAME, format!("g:{}", old_role.join(":")).as_bytes())?;
        }

        // Process policies (deduplicate identical ones)
        let mut seen_policies = std::collections::HashSet::new();

        for mut policy in policies_1.into_iter().chain(policies_2.into_iter()) {
            let old_policy = policy.clone();
            policy[0] = new_permission_ulid.to_string(); // Replace subject

            let policy_key = policy[1..].join(":"); // Use obj:act:eft as dedup key
            if seen_policies.insert(policy_key) {
                policies_to_add.push(CasbinPolicy::new(policy.clone())?);
                // UPDATE STORAGE: Add new policy to storage
                store.put(
                    txn,
                    DBNAME,
                    format!("p:{}", policy.join(":")).as_bytes(),
                    &[],
                )?;
            }

            // Mark old policy for removal and UPDATE STORAGE: Remove old policy
            policies_to_remove.push(CasbinPolicy::new(old_policy.clone())?);
            store.remove(
                txn,
                DBNAME,
                format!("p:{}", old_policy.join(":")).as_bytes(),
            )?;
        }

        // Find all UserIdentity mappings to update
        let identities_1 = self.get_user_identities_for_permission(perm_ulid_1, store, txn)?;
        let identities_2 = self.get_user_identities_for_permission(perm_ulid_2, store, txn)?;
        let identity_mappings_to_update: Vec<UserIdentity> = identities_1
            .into_iter()
            .chain(identities_2.into_iter())
            .collect();

        // UPDATE STORAGE: Update all identity mappings to point to new permission ULID
        for identity in &identity_mappings_to_update {
            let key = postcard::to_allocvec(identity)?;
            let value = postcard::to_allocvec(&new_permission_ulid)?;
            store.put(txn, IDENTITY_PERMISSIONS_DB, &key, &value)?;
        }

        Ok(UnifyIdentitiesPrepare {
            policies_to_add,
            roles_to_add,
            policies_to_remove,
            roles_to_remove,
        })
    }

    /// Commit identity unification (UPDATED - only updates in-memory enforcer)
    pub async fn unify_identities_commit(&self, request: UnifyIdentitiesPrepare) -> Result<()> {
        let mut enforcer = self.enforcer.write().await;

        // Remove old roles and policies from in-memory enforcer
        for role in request.roles_to_remove {
            enforcer.remove_role(role).await?;
        }

        for policy in request.policies_to_remove {
            enforcer.remove_policy(policy).await?;
        }

        // Add new unified roles and policies to in-memory enforcer
        for role in request.roles_to_add {
            enforcer.add_role(role).await?;
        }

        for policy in request.policies_to_add {
            enforcer.add_policy(policy).await?;
        }

        Ok(())
    }

    /// Convenience method: Unify identities with both prepare and commit
    pub async fn unify_identities<'a, S: Store<'a> + 'static>(
        &self,
        identity1: &UserIdentity,
        identity2: &UserIdentity,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<()> {
        let prepare = self
            .unify_identities_prepare(identity1, identity2, store, txn)
            .await?;
        self.unify_identities_commit(prepare).await
    }

    /// Check if a user has permission to access a resource
    pub async fn check_permission<S>(
        &self,
        user_identity: &UserIdentity,
        resource_id: ResourceId,
        action: Action,
        store: &S,
    ) -> Result<Path>
    where
        for<'a> S: Store<'a> + 'static,
    {
        println!(
            "
CHECK PERMISSIONS
{}",
            user_identity
        );
        let store = store.clone();
        let manager = self.clone();
        let user_identity = user_identity.clone();
        let (permission_ulid, path) =
            tokio::task::spawn_blocking(move || -> Result<(Ulid, Path)> {
                let txn = store.create_txn(false)?;
                // Resolve user identity to permission ULID (must exist)
                let permission_ulid =
                    manager.resolve_permission_ulid(&user_identity, &store, &txn)?;

                // Retrieve path from resource mapping
                let key = resource_id.to_string();
                let path_bytes = store
                    .get(&txn, RESOURCE_DB, key.as_bytes())
                    .map_err(|_| PermissionError::ResourceNotFound(resource_id.to_string()))?
                    .ok_or_else(|| PermissionError::ResourceNotFound(resource_id.to_string()))?;

                let path = Path::try_from(path_bytes.as_ref())?;
                store.commit(txn)?;
                Ok((permission_ulid, path))
            })
            .await??;

        // Check permission using the enforcer (read lock)
        let allowed = {
            let enforcer = self.enforcer.read().await;
            println!(
                "{}
{}
{}",
                permission_ulid, path, action
            );
            enforcer.enforce(
                &permission_ulid.to_string(),
                &path.to_string(),
                &action.to_string(),
            )?
        };
        println!("{allowed}");

        if allowed {
            Ok(path)
        } else {
            Err(PermissionError::PermissionDenied)
        }
    }

    /// Prepare to create a new group with default admin and member roles (UPDATED)
    pub fn create_group_prepare<'a, S: Store<'a> + 'static>(
        &self,
        group_id: Ulid,
        initial_user: &UserIdentity,
        realm_id: RealmKey,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<CreateGroupPrepare> {
        // Ensure user identity exists with explicit mapping
        let permission_ulid = self.ensure_user_identity(initial_user, store, txn)?;

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

        println!(
            "
CREATE GROUP WITH
{}
{}
{}
",
            admin_path, member_path, permission_ulid
        );

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
        let mut enforcer = self.enforcer.write().await;

        for policy in request.policy {
            enforcer.add_policy(policy).await?;
        }

        for role in request.role {
            enforcer.add_role(role).await?;
        }

        Ok(())
    }

    /// Prepare to add a user to a group role (UPDATED)
    pub fn add_user_prepare<'a, S: Store<'a> + 'static>(
        &self,
        group_id: Ulid,
        user_identity: &UserIdentity,
        role_name: &str,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<AddUserPrepare> {
        // Ensure user identity exists with explicit mapping
        let permission_ulid = self.ensure_user_identity(user_identity, store, txn)?;

        let full_role = format!("{}_{}", group_id, role_name);

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
        let mut enforcer = self.enforcer.write().await;
        enforcer.add_role(request.role).await?;
        Ok(())
    }

    /// Prepare to remove a role from a user (UPDATED)
    pub fn remove_role_from_user_prepare<'a, S: Store<'a> + 'static>(
        &self,
        group_id: Ulid,
        user_identity: &UserIdentity,
        role_name: &str,
        store: &'a S,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> Result<RemoveRolePrepare> {
        // Must resolve existing permission ULID (no creation)
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
        let mut enforcer = self.enforcer.write().await;
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
        let mut enforcer = self.enforcer.write().await;
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
        realm_id: RealmKey,
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
    pub async fn get_group_roles(&self, group_id: Ulid) -> Vec<String> {
        let prefix = format!("{}_", group_id);
        let policies = {
            let enforcer = self.enforcer.read().await;
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
    pub async fn get_role_users(&self, group_id: Ulid, role_name: &str) -> Vec<String> {
        let full_role = format!("{}_{}", group_id, role_name);
        let enforcer = self.enforcer.read().await;
        enforcer.get_users_for_role(&full_role)
    }

    /// Get all policies for a role
    pub async fn get_role_policies(&self, group_id: Ulid, role_name: &str) -> Vec<Vec<String>> {
        let full_role = format!("{}_{}", group_id, role_name);
        let enforcer = self.enforcer.read().await;

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
        let mut enforcer = self.enforcer.write().await;
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

    pub fn create_test_realm_key(suffix: u8) -> RealmKey {
        let mut key = [0u8; 32];
        key[31] = suffix;
        key
    }

    #[tokio::test]
    async fn test_predictable_user_identity_creation() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let manager = PermissionManager::new().await.unwrap();

        let realm_id = create_test_realm_key(10);
        let user_ulid = create_test_ulid(1);
        let identity = UserIdentity::new(user_ulid, realm_id);

        // Initially, no permission mapping should exist
        assert!(
            manager
                .resolve_permission_ulid(&identity, &store, &txn)
                .is_err()
        );

        // Create explicit permission mapping
        let permission_ulid = manager
            .create_user_identity(&identity, &store, &mut txn)
            .unwrap();

        // Now resolution should work
        let resolved = manager
            .resolve_permission_ulid(&identity, &store, &txn)
            .unwrap();
        assert_eq!(resolved, permission_ulid);

        // Ensure method should return existing mapping
        let ensured = manager
            .ensure_user_identity(&identity, &store, &mut txn)
            .unwrap();
        assert_eq!(ensured, permission_ulid);

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
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
    async fn test_basic_identity_unification() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let manager = PermissionManager::new().await.unwrap();

        let realm_id = create_test_realm_key(10);
        let user_ulid_1 = create_test_ulid(1);
        let user_ulid_2 = create_test_ulid(2);

        // Create two separate identities with explicit mappings
        let identity_1 = UserIdentity::new(user_ulid_1, realm_id);
        let identity_2 = UserIdentity::new(user_ulid_2, realm_id);

        // Create groups and permissions for both identities (ensures mappings exist)
        let group_id_1 = create_test_ulid(20);
        let group_id_2 = create_test_ulid(21);

        manager
            .create_group(group_id_1, &identity_1, realm_id, &store, &mut txn)
            .await
            .unwrap();
        manager
            .create_group(group_id_2, &identity_2, realm_id, &store, &mut txn)
            .await
            .unwrap();

        // Verify they have different permission ULIDs initially
        let perm_1 = manager
            .resolve_permission_ulid(&identity_1, &store, &txn)
            .unwrap();
        let perm_2 = manager
            .resolve_permission_ulid(&identity_2, &store, &txn)
            .unwrap();
        assert_ne!(perm_1, perm_2);

        // Unify the identities
        manager
            .unify_identities(&identity_1, &identity_2, &store, &mut txn)
            .await
            .unwrap();

        // Verify they now have the same permission ULID
        let unified_perm_1 = manager
            .resolve_permission_ulid(&identity_1, &store, &txn)
            .unwrap();
        let unified_perm_2 = manager
            .resolve_permission_ulid(&identity_2, &store, &txn)
            .unwrap();
        assert_eq!(unified_perm_1, unified_perm_2);

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_unification_error_cases() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let manager = PermissionManager::new().await.unwrap();

        let realm_id = create_test_realm_key(10);
        let user_ulid = create_test_ulid(1);
        let identity = UserIdentity::new(user_ulid, realm_id);

        // Create explicit mapping
        manager
            .create_user_identity(&identity, &store, &mut txn)
            .unwrap();

        // Test self-unification
        let result = manager
            .unify_identities(&identity, &identity, &store, &mut txn)
            .await;
        assert!(result.is_err());

        // Test non-existent identity
        let nonexistent = UserIdentity::new(create_test_ulid(99), realm_id);
        let result = manager
            .unify_identities(&identity, &nonexistent, &store, &mut txn)
            .await;
        assert!(result.is_ok()); // Should not fail because one mapping exists

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }

    #[tokio::test]
    async fn test_cross_realm_unification() {
        let (store, test_dir) = setup_test_store().await;
        let mut txn = store.create_txn(true).unwrap();
        let manager = PermissionManager::new().await.unwrap();

        let realm_a = create_test_realm_key(10);
        let realm_b = create_test_realm_key(11);
        let user_ulid = create_test_ulid(1);

        // Same user in different realms
        let identity_a = UserIdentity::new(user_ulid, realm_a);
        let identity_b = UserIdentity::new(user_ulid, realm_b);

        // Create groups in each realm (ensures mappings exist)
        let group_a = create_test_ulid(20);
        let group_b = create_test_ulid(21);
        manager
            .create_group(group_a, &identity_a, realm_a, &store, &mut txn)
            .await
            .unwrap();
        manager
            .create_group(group_b, &identity_b, realm_b, &store, &mut txn)
            .await
            .unwrap();

        // Unify cross-realm identities
        manager
            .unify_identities(&identity_a, &identity_b, &store, &mut txn)
            .await
            .unwrap();

        // Verify unified permission access
        let unified_perm_a = manager
            .resolve_permission_ulid(&identity_a, &store, &txn)
            .unwrap();
        let unified_perm_b = manager
            .resolve_permission_ulid(&identity_b, &store, &txn)
            .unwrap();
        assert_eq!(unified_perm_a, unified_perm_b);

        txn.commit().unwrap();
        cleanup_test_dir(&test_dir);
    }
}
