use aruna_storage::storage::store::Store;
use casbin::MemoryAdapter;
use casbin::{CoreApi, DefaultModel, MgmtApi, RbacApi};

pub const MODEL_CONF: &str = r#"
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act, eft

[role_definition]
g = _, _

[policy_effect]
e = some(where (p.eft == allow)) && !some(where (p.eft == deny))

[matchers]
m = g(r.sub, p.sub) && keyMatch2(r.obj, p.obj) && (r.act == p.act || (p.act == "write" && r.act == "read"))
"#;

pub const DBNAME: &str = "casbin";

/// The custom Enforcer struct that integrates the StoreAdapter
pub struct Enforcer<'a, S: Store<'a> + Send + Sync + 'static> {
    pub inner: casbin::Enforcer,
    pub store: &'a S,
}

impl<'a, S: Store<'a>> Enforcer<'a, S> {
    /// Create a new Enforcer with the provided store
    pub async fn new(store: &'a S) -> casbin::Result<Self> {
        // Create the adapter
        let adapter = MemoryAdapter::default();

        // Create the casbin enforcer
        let mut inner =
            casbin::Enforcer::new(DefaultModel::from_str(MODEL_CONF).await?, adapter).await?;

        // Load the policy
        inner.load_policy().await?;

        Ok(Self { inner, store })
    }

    /// Check if a request is permitted
    pub async fn enforce(&self, sub: &str, obj: &str, act: &str) -> casbin::Result<bool> {
        self.inner.enforce((sub, obj, act))
    }

    /// Add a policy rule
    pub async fn add_policy(
        &mut self,
        sub: &str,
        obj: &str,
        act: &str,
        eft: Option<&str>,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> casbin::Result<bool> {
        let mut rule = vec![sub.to_owned(), obj.to_owned(), act.to_owned()];
        if let Some(eft) = eft {
            rule.push(eft.to_owned());
        } else {
            rule.push("allow".to_owned());
        }
        let key = format!("p:{}", rule.join(":"));
        let result = self.inner.add_policy(rule).await;

        self.store
            .put(txn, DBNAME, key.as_bytes(), &[])
            .map_err(|e| casbin::Error::AdapterError(casbin::error::AdapterError(e.into())))?;

        result
    }

    pub async fn load_policy(&mut self, txn: &<S as Store<'a>>::Txn) -> casbin::Result<()> {
        for (key, _) in self
            .store
            .iter_db(txn, DBNAME)
            .map_err(|e| casbin::Error::AdapterError(casbin::error::AdapterError(e.into())))?
        {
            let key = String::from_utf8(key.to_vec()).unwrap();
            if key.starts_with("p:") {
                let rule = key[2..]
                    .split(':')
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                self.inner.add_policy(rule).await?;
            } else if key.starts_with("g:") {
                let rule = key[2..]
                    .split(':')
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                self.inner.add_named_grouping_policy("g", rule).await?;
            }
        }
        self.inner.load_policy().await
    }

    /// Remove a policy rule
    pub async fn remove_policy(
        &mut self,
        sub: &str,
        obj: &str,
        act: &str,
        eft: &str,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> casbin::Result<bool> {
        let rule = vec![
            sub.to_owned(),
            obj.to_owned(),
            act.to_owned(),
            eft.to_owned(),
        ];
        let key = format!("p:{}", rule.join(":"));
        let result = self.inner.remove_policy(rule).await;

        self.store
            .remove(txn, DBNAME, key.as_bytes())
            .map_err(|e| casbin::Error::AdapterError(casbin::error::AdapterError(e.into())))?;

        result
    }

    /// Add a role assignment
    pub async fn add_role(
        &mut self,
        user: &str,
        role: &str,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> casbin::Result<bool> {
        let key = format!("g:{}:{}", user, role);

        let result = self
            .inner
            .add_named_grouping_policy("g", vec![user.to_owned(), role.to_owned()])
            .await;

        self.store
            .put(txn, DBNAME, key.as_bytes(), &[])
            .map_err(|e| casbin::Error::AdapterError(casbin::error::AdapterError(e.into())))?;
        result
    }

    /// Remove a role assignment
    pub async fn remove_role(
        &mut self,
        user: &str,
        role: &str,
        txn: &mut <S as Store<'a>>::Txn,
    ) -> casbin::Result<bool> {
        let key = format!("g:{}:{}", user, role);
        let result = self
            .inner
            .remove_named_grouping_policy("g", vec![user.to_owned(), role.to_owned()])
            .await;

        self.store
            .remove(txn, DBNAME, key.as_bytes())
            .map_err(|e| casbin::Error::AdapterError(casbin::error::AdapterError(e.into())))?;
        result
    }

    /// Get all policies
    pub async fn get_roles(&self) -> Vec<Vec<String>> {
        self.inner.get_grouping_policy()
    }

    /// Get all role assignments
    pub async fn get_groups(&self) -> Vec<Vec<String>> {
        self.inner.get_named_grouping_policy("g")
    }

    /// Get all policies
    pub async fn get_policies(&self) -> Vec<Vec<String>> {
        self.inner.get_policy()
    }

    /// Save all policies to storage
    pub async fn save_policy(&mut self) -> casbin::Result<()> {
        self.inner.save_policy().await
    }

    /// Check if a user has a role
    pub async fn has_role(&self, user: &str, role: &str) -> bool {
        self.inner
            .has_grouping_named_policy("g", vec![user.to_owned(), role.to_owned()])
    }

    /// Get all roles that a user has
    pub async fn get_roles_for_user(&self, user: &str) -> Vec<String> {
        self.inner.get_implicit_roles_for_user(user, None)
    }

    /// Get all users that have a role
    pub async fn get_users_for_role(&self, group: &str) -> Vec<String> {
        self.inner.get_users_for_role(group, None)
    }

    /// Get all permissions for a user
    pub async fn get_permissions_for_user(&self, user: &str) -> Vec<Vec<String>> {
        self.inner.get_implicit_permissions_for_user(user, None)
    }

    /// Get all users that have a permission
    pub async fn get_users_for_permission(&self, obj: &str, act: &str) -> Vec<String> {
        let perm = vec![obj.to_owned(), act.to_owned()];
        self.inner.get_implicit_users_for_permission(perm).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_storage::storage::lmdb::LmdbConfig;
    use aruna_storage::storage::lmdb::LmdbStore;
    use aruna_storage::storage::store::Store;
    use rand::distributions::Alphanumeric;
    use rand::distributions::DistString;
    use std::fs;
    use tokio::test;

    // Helper function to setup a test store
    async fn setup_test_store() -> (LmdbStore, String) {
        // Create a unique test directory for each test
        let test_id = Alphanumeric.sample_string(&mut rand::thread_rng(), 8);
        let test_dir = format!("/dev/shm/test_{}", test_id);

        // Create the directory
        fs::create_dir_all(&test_dir).expect("Failed to create test directory");

        let store_config = LmdbConfig {
            path: test_dir.clone(),
            databases: vec![DBNAME],
        };

        let store = LmdbStore::new(store_config).expect("Failed to create LMDB store");
        (store, test_dir)
    }

    // Helper function to cleanup test resources
    fn cleanup_test_dir(test_dir: &str) {
        fs::remove_dir_all(test_dir).expect("Failed to clean up test directory");
    }

    #[test]
    async fn test_group_membership() {
        let (store, test_dir) = setup_test_store().await;

        let mut txn = store.create_txn(true).unwrap();
        // Create enforcer
        let mut enforcer = Enforcer::new(&store).await.unwrap();

        // Add groups
        enforcer.add_role("alice", "admin", &mut txn).await.unwrap();
        enforcer.add_role("bob", "editor", &mut txn).await.unwrap();

        // Check group membership
        assert!(enforcer.has_role("alice", "admin").await);
        assert!(!enforcer.has_role("alice", "editor").await);
        assert!(enforcer.has_role("bob", "editor").await);
        assert!(!enforcer.has_role("bob", "admin").await);

        assert!(enforcer.get_roles_for_user("alice").await.first().unwrap() == "admin");
        assert!(enforcer.get_users_for_role("admin").await.first().unwrap() == "alice");

        txn.commit().unwrap();

        cleanup_test_dir(&test_dir);
    }
    #[test]
    async fn test_basic_policy_crud() {
        let (store, test_dir) = setup_test_store().await;

        let mut txn = store.create_txn(true).unwrap();
        // Create enforcer
        let mut enforcer = Enforcer::new(&store).await.unwrap();

        // Test adding a policy
        let added = enforcer
            .add_policy("alice", "data1", "read", None, &mut txn)
            .await
            .unwrap();
        assert!(added);

        // Test policy enforcement
        let allowed = enforcer.enforce("alice", "data1", "read").await.unwrap();
        assert!(allowed);

        // Test policy that shouldn't be allowed
        let not_allowed = enforcer.enforce("alice", "data1", "write").await.unwrap();
        assert!(!not_allowed);

        // Test removing a policy
        let removed = enforcer
            .remove_policy("alice", "data1", "read", "allow", &mut txn)
            .await
            .unwrap();
        println!("Removed: {}", removed);
        assert!(removed);

        // Confirm removal
        let not_allowed_after_removal = enforcer.enforce("alice", "data1", "read").await.unwrap();
        assert!(!not_allowed_after_removal);

        txn.commit().unwrap();

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_role_assignments() {
        let (store, test_dir) = setup_test_store().await;

        let mut txn = store.create_txn(true).unwrap();
        // Create enforcer
        let mut enforcer = Enforcer::new(&store).await.unwrap();

        // Setup a role-based policy
        enforcer
            .add_policy("admin", "data", "read", None, &mut txn)
            .await
            .unwrap();
        enforcer
            .add_policy("admin", "data", "write", None, &mut txn)
            .await
            .unwrap();

        // Add alice to admin role
        enforcer.add_role("alice", "admin", &mut txn).await.unwrap();

        // Check if role assignment works
        assert!(enforcer.has_role("alice", "admin").await);

        // Test permissions via role inheritance
        assert!(enforcer.enforce("alice", "data", "read").await.unwrap());
        assert!(enforcer.enforce("alice", "data", "write").await.unwrap());

        // Remove role
        enforcer
            .remove_role("alice", "admin", &mut txn)
            .await
            .unwrap();

        // Permissions should be revoked
        assert!(!enforcer.enforce("alice", "data", "read").await.unwrap());

        txn.commit().unwrap();

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_persistence() {
        let (store, test_dir) = setup_test_store().await;

        // First enforcer instance
        {
            let mut txn = store.create_txn(true).unwrap();
            let mut enforcer = Enforcer::new(&store).await.unwrap();

            enforcer
                .add_policy("alice", "data1", "read", None, &mut txn)
                .await
                .unwrap();
            enforcer
                .add_policy("bob", "data2", "write", None, &mut txn)
                .await
                .unwrap();

            // Save explicitly
            enforcer.save_policy().await.unwrap();

            txn.commit().unwrap();
        }

        // Second enforcer instance - should load the saved policies
        {
            let txn = store.create_txn(false).unwrap();
            let mut enforcer = Enforcer::new(&store).await.unwrap();
            enforcer.load_policy(&txn).await.unwrap();

            // Test persistence
            assert!(enforcer.enforce("alice", "data1", "read").await.unwrap());
            assert!(enforcer.enforce("bob", "data2", "write").await.unwrap());
            assert!(!enforcer.enforce("alice", "data2", "read").await.unwrap());

            txn.commit().unwrap();
        }

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_nested_groups() {
        let (store, test_dir) = setup_test_store().await;

        let mut txn = store.create_txn(true).unwrap();
        // Create enforcer
        let mut enforcer = Enforcer::new(&store).await.unwrap();

        // Set up a role hierarchy
        // editor <- manager <- admin
        // Set permissions for each role
        enforcer
            .add_policy("editor", "articles", "read", None, &mut txn)
            .await
            .unwrap();
        enforcer
            .add_policy("editor", "articles", "edit", None, &mut txn)
            .await
            .unwrap();

        enforcer
            .add_policy("manager", "articles", "publish", None, &mut txn)
            .await
            .unwrap();
        enforcer
            .add_policy("manager", "users", "view", None, &mut txn)
            .await
            .unwrap();

        enforcer
            .add_policy("admin", "users", "edit", None, &mut txn)
            .await
            .unwrap();
        enforcer
            .add_policy("admin", "system", "config", None, &mut txn)
            .await
            .unwrap();

        // Set up the role hierarchy
        enforcer
            .add_role("manager", "editor", &mut txn)
            .await
            .unwrap(); // managers inherit editor permissions
        enforcer
            .add_role("admin", "manager", &mut txn)
            .await
            .unwrap(); // admins inherit manager permissions

        // Assign users to roles
        enforcer
            .add_role("alice", "editor", &mut txn)
            .await
            .unwrap();
        enforcer.add_role("bob", "manager", &mut txn).await.unwrap();
        enforcer
            .add_role("charlie", "admin", &mut txn)
            .await
            .unwrap();

        // Test editor permissions
        assert!(enforcer.enforce("alice", "articles", "read").await.unwrap());
        assert!(enforcer.enforce("alice", "articles", "edit").await.unwrap());
        assert!(
            !enforcer
                .enforce("alice", "articles", "publish")
                .await
                .unwrap()
        );
        assert!(!enforcer.enforce("alice", "users", "view").await.unwrap());

        // Test manager permissions (inherited + direct)
        assert!(enforcer.enforce("bob", "articles", "read").await.unwrap()); // inherited from editor
        assert!(enforcer.enforce("bob", "articles", "edit").await.unwrap()); // inherited from editor
        assert!(
            enforcer
                .enforce("bob", "articles", "publish")
                .await
                .unwrap()
        ); // direct
        assert!(enforcer.enforce("bob", "users", "view").await.unwrap()); // direct
        assert!(!enforcer.enforce("bob", "users", "edit").await.unwrap()); // no access

        // Test admin permissions (inherited through whole chain + direct)
        assert!(
            enforcer
                .enforce("charlie", "articles", "read")
                .await
                .unwrap()
        ); // from editor via manager
        assert!(
            enforcer
                .enforce("charlie", "articles", "edit")
                .await
                .unwrap()
        ); // from editor via manager
        assert!(
            enforcer
                .enforce("charlie", "articles", "publish")
                .await
                .unwrap()
        ); // from manager
        assert!(enforcer.enforce("charlie", "users", "view").await.unwrap()); // from manager
        assert!(enforcer.enforce("charlie", "users", "edit").await.unwrap()); // direct
        assert!(
            enforcer
                .enforce("charlie", "system", "config")
                .await
                .unwrap()
        ); // direct

        // Test role retrieval
        let alice_roles = enforcer.get_roles_for_user("alice").await;
        assert_eq!(alice_roles.len(), 1);
        assert!(alice_roles.contains(&"editor".to_string()));

        let bob_roles = enforcer.get_roles_for_user("bob").await;
        assert_eq!(bob_roles.len(), 2); // manager + inherited editor
        assert!(bob_roles.contains(&"manager".to_string()));
        assert!(bob_roles.contains(&"editor".to_string()));

        let charlie_roles = enforcer.get_roles_for_user("charlie").await;
        assert_eq!(charlie_roles.len(), 3); // admin + inherited manager + inherited editor
        assert!(charlie_roles.contains(&"admin".to_string()));
        assert!(charlie_roles.contains(&"manager".to_string()));
        assert!(charlie_roles.contains(&"editor".to_string()));

        txn.commit().unwrap();

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_hierarchical_wildcards() {
        let (store, test_dir) = setup_test_store().await;

        let mut txn = store.create_txn(true).unwrap();
        // Create enforcer
        let mut enforcer = Enforcer::new(&store).await.unwrap();

        // Define roles
        let realm_admin = "realm_admin";
        let group_admin = "group_admin";
        let project_admin = "project_admin";
        let resource_viewer = "resource_viewer";
        let resource_editor = "resource_editor";

        // Define realms, groups, and resources
        let realm_id = "realm123";
        let group_id = "group456";
        let project_id = "project789";
        let resource_id = "resource101";

        // Setup wildcard permissions at different levels

        // Realm admin can access everything in the realm
        enforcer
            .add_policy(
                realm_admin,
                &format!("/{}", realm_id),
                "read",
                None,
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                realm_admin,
                &format!("/{}/*", realm_id),
                "read",
                None,
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                realm_admin,
                &format!("/{}/*", realm_id),
                "write",
                None,
                &mut txn,
            )
            .await
            .unwrap();

        // Group admin can access everything in their group
        enforcer
            .add_policy(
                group_admin,
                &format!("/{}/groups/{}/", realm_id, group_id),
                "read",
                None,
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                group_admin,
                &format!("/{}/groups/{}/*", realm_id, group_id),
                "read",
                None,
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                group_admin,
                &format!("/{}/groups/{}/*", realm_id, group_id),
                "write",
                None,
                &mut txn,
            )
            .await
            .unwrap();

        // Project admin can access everything in their project
        enforcer
            .add_policy(
                project_admin,
                &format!(
                    "/{}/groups/{}/resources/{}/",
                    realm_id, group_id, project_id
                ),
                "read",
                None,
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                project_admin,
                &format!(
                    "/{}/groups/{}/resources/{}/*",
                    realm_id, group_id, project_id
                ),
                "read",
                None,
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                project_admin,
                &format!(
                    "/{}/groups/{}/resources/{}/*",
                    realm_id, group_id, project_id
                ),
                "write",
                None,
                &mut txn,
            )
            .await
            .unwrap();

        // Resource viewers can only read specific resources
        enforcer
            .add_policy(
                resource_viewer,
                &format!(
                    "/{}/groups/{}/resources/{}/resources/{}/",
                    realm_id, group_id, project_id, resource_id
                ),
                "read",
                None,
                &mut txn,
            )
            .await
            .unwrap();

        // Resource editors can read and write specific resources
        enforcer
            .add_policy(
                resource_editor,
                &format!(
                    "/{}/groups/{}/resources/{}/resources/{}/",
                    realm_id, group_id, project_id, resource_id
                ),
                "read",
                None,
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                resource_editor,
                &format!(
                    "/{}/groups/{}/resources/{}/resources/{}/",
                    realm_id, group_id, project_id, resource_id
                ),
                "write",
                None,
                &mut txn,
            )
            .await
            .unwrap();

        // Assign users to roles
        enforcer
            .add_role("alice", realm_admin, &mut txn)
            .await
            .unwrap();
        enforcer
            .add_role("bob", group_admin, &mut txn)
            .await
            .unwrap();
        enforcer
            .add_role("charlie", project_admin, &mut txn)
            .await
            .unwrap();
        enforcer
            .add_role("dave", resource_viewer, &mut txn)
            .await
            .unwrap();
        enforcer
            .add_role("eve", resource_editor, &mut txn)
            .await
            .unwrap();

        // Test realm admin permissions (alice)

        // Can access realm top level
        assert!(
            enforcer
                .enforce("alice", &format!("/{}/", realm_id), "read")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce("alice", &format!("/{}/", realm_id), "write")
                .await
                .unwrap()
        ); // Via /* wildcard

        // Can access admin functions
        assert!(
            enforcer
                .enforce("alice", &format!("/{}/admin", realm_id), "read")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce("alice", &format!("/{}/admin", realm_id), "write")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce("alice", &format!("/{}/admin/users", realm_id), "read")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce("alice", &format!("/{}/admin/settings", realm_id), "write")
                .await
                .unwrap()
        );

        // Can access group level
        assert!(
            enforcer
                .enforce(
                    "alice",
                    &format!("/{}/groups/{}/", realm_id, group_id),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "alice",
                    &format!("/{}/groups/{}/admin", realm_id, group_id),
                    "write"
                )
                .await
                .unwrap()
        );

        // Can access project level
        assert!(
            enforcer
                .enforce(
                    "alice",
                    &format!(
                        "/{}/groups/{}/resources/{}/",
                        realm_id, group_id, project_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "alice",
                    &format!(
                        "/{}/groups/{}/resources/{}/admin",
                        realm_id, group_id, project_id
                    ),
                    "write"
                )
                .await
                .unwrap()
        );

        // Can access resource level
        assert!(
            enforcer
                .enforce(
                    "alice",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}/",
                        realm_id, group_id, project_id, resource_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "alice",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}/",
                        realm_id, group_id, project_id, resource_id
                    ),
                    "write"
                )
                .await
                .unwrap()
        );

        // Test group admin permissions (bob)

        // Cannot access realm top level admin
        assert!(
            !enforcer
                .enforce("bob", &format!("/{}/admin", realm_id), "read")
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce("bob", &format!("/{}/admin/settings", realm_id), "write")
                .await
                .unwrap()
        );

        // Can access own group
        assert!(
            enforcer
                .enforce(
                    "bob",
                    &format!("/{}/groups/{}/", realm_id, group_id),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "bob",
                    &format!("/{}/groups/{}/admin", realm_id, group_id),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "bob",
                    &format!("/{}/groups/{}/admin/settings", realm_id, group_id),
                    "write"
                )
                .await
                .unwrap()
        );

        // Can access projects in own group
        assert!(
            enforcer
                .enforce(
                    "bob",
                    &format!(
                        "/{}/groups/{}/resources/{}/",
                        realm_id, group_id, project_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "bob",
                    &format!(
                        "/{}/groups/{}/resources/{}/admin",
                        realm_id, group_id, project_id
                    ),
                    "write"
                )
                .await
                .unwrap()
        );

        // Can access resources in own group's projects
        assert!(
            enforcer
                .enforce(
                    "bob",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}/",
                        realm_id, group_id, project_id, resource_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "bob",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}/",
                        realm_id, group_id, project_id, resource_id
                    ),
                    "write"
                )
                .await
                .unwrap()
        );

        // Cannot access other groups
        assert!(
            !enforcer
                .enforce("bob", &format!("/{}/groups/other_group", realm_id), "read")
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "bob",
                    &format!("/{}/groups/other_group/admin", realm_id),
                    "write"
                )
                .await
                .unwrap()
        );

        // Test project admin permissions (charlie)

        // Cannot access realm or group admin
        assert!(
            !enforcer
                .enforce("charlie", &format!("/{}/admin", realm_id), "read")
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "charlie",
                    &format!("/{}/groups/{}/admin", realm_id, group_id),
                    "write"
                )
                .await
                .unwrap()
        );

        // Can access own project
        assert!(
            enforcer
                .enforce(
                    "charlie",
                    &format!(
                        "/{}/groups/{}/resources/{}/",
                        realm_id, group_id, project_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "charlie",
                    &format!(
                        "/{}/groups/{}/resources/{}/admin",
                        realm_id, group_id, project_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "charlie",
                    &format!(
                        "/{}/groups/{}/resources/{}/admin/settings",
                        realm_id, group_id, project_id
                    ),
                    "write"
                )
                .await
                .unwrap()
        );

        // Can access resources in own project
        assert!(
            enforcer
                .enforce(
                    "charlie",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id, project_id, resource_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "charlie",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id, project_id, resource_id
                    ),
                    "write"
                )
                .await
                .unwrap()
        );

        // Cannot access other projects
        assert!(
            !enforcer
                .enforce(
                    "charlie",
                    &format!("/{}/groups/{}/resources/other_project", realm_id, group_id),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "charlie",
                    &format!(
                        "/{}/groups/{}/resources/other_project/admin",
                        realm_id, group_id
                    ),
                    "write"
                )
                .await
                .unwrap()
        );

        // Test resource viewer permissions (dave)

        // Can only read specific resource
        assert!(
            enforcer
                .enforce(
                    "dave",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}/",
                        realm_id, group_id, project_id, resource_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "dave",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}/",
                        realm_id, group_id, project_id, resource_id
                    ),
                    "write"
                )
                .await
                .unwrap()
        );

        // Cannot access admin functions
        assert!(
            !enforcer
                .enforce("dave", &format!("/{}/admin", realm_id), "read")
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "dave",
                    &format!("/{}/groups/{}/admin", realm_id, group_id),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "dave",
                    &format!(
                        "/{}/groups/{}/resources/{}/admin",
                        realm_id, group_id, project_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        );

        // Cannot access other resources
        assert!(
            !enforcer
                .enforce(
                    "dave",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/other_resource",
                        realm_id, group_id, project_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        );

        txn.commit().unwrap();

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_allow_deny_precedence() {
        let (store, test_dir) = setup_test_store().await;

        let mut txn = store.create_txn(true).unwrap();
        // Create enforcer
        let mut enforcer = Enforcer::new(&store).await.unwrap();

        // Setup basic allow policies
        enforcer
            .add_policy("alice", "data1", "read", Some("allow"), &mut txn)
            .await
            .unwrap();
        enforcer
            .add_policy("bob", "data2", "write", Some("allow"), &mut txn)
            .await
            .unwrap();

        // Setup explicit deny policy that overrides an allow
        enforcer
            .add_policy("alice", "data1", "read", Some("deny"), &mut txn)
            .await
            .unwrap();

        // Test basic allow
        assert!(enforcer.enforce("bob", "data2", "write").await.unwrap());

        // Test deny takes precedence over allow
        assert!(!enforcer.enforce("alice", "data1", "read").await.unwrap());

        // Test default deny (no policy)
        assert!(!enforcer.enforce("alice", "data3", "read").await.unwrap());

        txn.commit().unwrap();

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_hierarchical_allow_deny() {
        let (store, test_dir) = setup_test_store().await;

        let mut txn = store.create_txn(true).unwrap();
        // Create enforcer
        let mut enforcer = Enforcer::new(&store).await.unwrap();

        let realm_id = "realm123";
        let group_id = "group456";
        let project_id = "project789";

        // Setup broad allow with wildcards
        enforcer
            .add_policy(
                "admin",
                &format!("/{}/*", realm_id),
                "access",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();

        // Setup specific deny that overrides the wildcard allow
        enforcer
            .add_policy(
                "admin",
                &format!("/{}/restricted", realm_id),
                "access",
                Some("deny"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "admin",
                &format!("/{}/groups/{}/restricted", realm_id, group_id),
                "access",
                Some("deny"),
                &mut txn,
            )
            .await
            .unwrap();

        // Setup specific user with mixed permissions
        enforcer
            .add_policy(
                "user",
                &format!("/{}/groups/{}/*", realm_id, group_id),
                "read",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "user",
                &format!(
                    "/{}/groups/{}/resources/{}/secret",
                    realm_id, group_id, project_id
                ),
                "read",
                Some("deny"),
                &mut txn,
            )
            .await
            .unwrap();

        // Test admin permissions with allow/deny interaction
        assert!(
            enforcer
                .enforce("admin", &format!("/{}/normal", realm_id), "access")
                .await
                .unwrap()
        ); // Allowed by wildcard
        assert!(
            !enforcer
                .enforce("admin", &format!("/{}/restricted", realm_id), "access")
                .await
                .unwrap()
        ); // Explicitly denied
        assert!(
            enforcer
                .enforce(
                    "admin",
                    &format!("/{}/groups/{}/normal", realm_id, group_id),
                    "access"
                )
                .await
                .unwrap()
        ); // Allowed by wildcard
        assert!(
            !enforcer
                .enforce(
                    "admin",
                    &format!("/{}/groups/{}/restricted", realm_id, group_id),
                    "access"
                )
                .await
                .unwrap()
        ); // Explicitly denied

        // Test user permissions with allow/deny interaction
        assert!(
            enforcer
                .enforce(
                    "user",
                    &format!("/{}/groups/{}/normal", realm_id, group_id),
                    "read"
                )
                .await
                .unwrap()
        ); // Allowed by wildcard
        assert!(
            enforcer
                .enforce(
                    "user",
                    &format!(
                        "/{}/groups/{}/resources/{}/normal",
                        realm_id, group_id, project_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        ); // Allowed by wildcard
        assert!(
            !enforcer
                .enforce(
                    "user",
                    &format!(
                        "/{}/groups/{}/resources/{}/secret",
                        realm_id, group_id, project_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        ); // Explicitly denied
        assert!(
            !enforcer
                .enforce("user", &format!("/{}/admin", realm_id), "read")
                .await
                .unwrap()
        ); // Not in allowed path

        txn.commit().unwrap();

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_nested_allow_deny_inheritance() {
        let (store, test_dir) = setup_test_store().await;

        let mut txn = store.create_txn(true).unwrap();
        // Create enforcer
        let mut enforcer = Enforcer::new(&store).await.unwrap();

        let realm_id = "realm123";
        let group_id = "group456";
        let project_id = "project789";

        // Set up role hierarchy
        enforcer.add_role("user", "reader", &mut txn).await.unwrap();
        enforcer
            .add_role("editor", "reader", &mut txn)
            .await
            .unwrap();
        enforcer
            .add_role("admin", "editor", &mut txn)
            .await
            .unwrap();

        // Set up base permissions
        enforcer
            .add_policy(
                "reader",
                &format!("/{}/groups/*/resources/*/resources/*", realm_id),
                "read",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "editor",
                &format!("/{}/groups/*/resources/*/resources/*", realm_id),
                "write",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "admin",
                &format!("/{}/groups/*/resources/*/admin", realm_id),
                "write",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();

        // Set up deny overrides
        enforcer
            .add_policy(
                "reader",
                &format!(
                    "/{}/groups/{}/resources/{}/resources/classified",
                    realm_id, group_id, project_id
                ),
                "read",
                Some("deny"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "editor",
                &format!("/{}/groups/{}/resources/*/locked", realm_id, group_id),
                "write",
                Some("deny"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "admin",
                &format!("/{}/groups/*/security", realm_id),
                "write",
                Some("deny"),
                &mut txn,
            )
            .await
            .unwrap();

        // Test user permissions (inherits reader)
        assert!(
            enforcer
                .enforce(
                    "user",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/normal",
                        realm_id, group_id, project_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        ); // General read allowed
        assert!(
            !enforcer
                .enforce(
                    "user",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/classified",
                        realm_id, group_id, project_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        ); // Classified denied
        assert!(
            !enforcer
                .enforce(
                    "user",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/normal",
                        realm_id, group_id, project_id
                    ),
                    "write"
                )
                .await
                .unwrap()
        ); // No write permission

        // Test editor permissions (inherits reader, adds writer)
        assert!(
            enforcer
                .enforce(
                    "editor",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/normal",
                        realm_id, group_id, project_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        ); // General read allowed
        assert!(
            !enforcer
                .enforce(
                    "editor",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/classified",
                        realm_id, group_id, project_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        ); // Classified denied (from reader)
        assert!(
            enforcer
                .enforce(
                    "editor",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/normal",
                        realm_id, group_id, project_id
                    ),
                    "write"
                )
                .await
                .unwrap()
        ); // Normal write allowed
        assert!(
            !enforcer
                .enforce(
                    "editor",
                    &format!(
                        "/{}/groups/{}/resources/{}/locked",
                        realm_id, group_id, project_id
                    ),
                    "write"
                )
                .await
                .unwrap()
        ); // Locked write denied
        assert!(
            !enforcer
                .enforce(
                    "editor",
                    &format!(
                        "/{}/groups/{}/resources/{}/admin",
                        realm_id, group_id, project_id
                    ),
                    "write"
                )
                .await
                .unwrap()
        ); // No admin permission

        // Test admin permissions (inherits reader, editor, adds admin)
        assert!(
            enforcer
                .enforce(
                    "admin",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/normal",
                        realm_id, group_id, project_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        ); // General read allowed
        assert!(
            !enforcer
                .enforce(
                    "admin",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/classified",
                        realm_id, group_id, project_id
                    ),
                    "read"
                )
                .await
                .unwrap()
        ); // Classified denied (from reader)
        assert!(
            enforcer
                .enforce(
                    "admin",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/normal",
                        realm_id, group_id, project_id
                    ),
                    "write"
                )
                .await
                .unwrap()
        ); // Normal write allowed
        assert!(
            !enforcer
                .enforce(
                    "admin",
                    &format!(
                        "/{}/groups/{}/resources/{}/locked",
                        realm_id, group_id, project_id
                    ),
                    "write"
                )
                .await
                .unwrap()
        ); // Locked write denied (from editor)
        assert!(
            enforcer
                .enforce(
                    "admin",
                    &format!(
                        "/{}/groups/{}/resources/{}/admin",
                        realm_id, group_id, project_id
                    ),
                    "write"
                )
                .await
                .unwrap()
        ); // Admin allowed
        assert!(
            !enforcer
                .enforce(
                    "admin",
                    &format!("/{}/groups/{}/security", realm_id, group_id),
                    "write"
                )
                .await
                .unwrap()
        ); // Security denied specifically for admin

        txn.commit().unwrap();

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_allow_deny_realworld_scenario() {
        let (store, test_dir) = setup_test_store().await;

        let mut txn = store.create_txn(true).unwrap();
        // Create enforcer
        let mut enforcer = Enforcer::new(&store).await.unwrap();

        let realm_id = "org123";
        let group_id1 = "engineering";
        let group_id2 = "marketing";
        let project_id1 = "webapp";
        let project_id2 = "api";
        let project_id3 = "campaign";

        // Set up base organization-wide permissions

        // Organization admin
        enforcer
            .add_policy(
                "org_admin",
                &format!("/{}/*", realm_id),
                "read",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "org_admin",
                &format!("/{}/*", realm_id),
                "write",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "org_admin",
                &format!("/{}/security/keys", realm_id),
                "read",
                Some("deny"),
                &mut txn,
            )
            .await
            .unwrap(); // Even org admin can't read keys

        // Security admin (special role)
        enforcer
            .add_policy(
                "security_admin",
                &format!("/{}/security/*", realm_id),
                "read",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "security_admin",
                &format!("/{}/security/*", realm_id),
                "write",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "security_admin",
                &format!("/{}/admin/audit", realm_id),
                "read",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "security_admin",
                &format!("/{}/groups/*/admin/audit", realm_id),
                "read",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();

        // Engineering permissions
        enforcer
            .add_policy(
                "eng_lead",
                &format!("/{}/groups/{}/*", realm_id, group_id1),
                "read",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "eng_lead",
                &format!("/{}/groups/{}/*", realm_id, group_id1),
                "write",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "eng_lead",
                &format!("/{}/groups/{}/admin/budget", realm_id, group_id1),
                "write",
                Some("deny"),
                &mut txn,
            )
            .await
            .unwrap();

        // Developer permissions (project specific)
        enforcer
            .add_policy(
                "dev_webapp",
                &format!(
                    "/{}/groups/{}/resources/{}/*",
                    realm_id, group_id1, project_id1
                ),
                "read",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "dev_webapp",
                &format!(
                    "/{}/groups/{}/resources/{}/resources/*",
                    realm_id, group_id1, project_id1
                ),
                "write",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "dev_webapp",
                &format!(
                    "/{}/groups/{}/resources/{}/admin/*",
                    realm_id, group_id1, project_id1
                ),
                "write",
                Some("deny"),
                &mut txn,
            )
            .await
            .unwrap();

        enforcer
            .add_policy(
                "dev_api",
                &format!(
                    "/{}/groups/{}/resources/{}/*",
                    realm_id, group_id1, project_id2
                ),
                "read",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "dev_api",
                &format!(
                    "/{}/groups/{}/resources/{}/resources/*",
                    realm_id, group_id1, project_id2
                ),
                "write",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "dev_api",
                &format!(
                    "/{}/groups/{}/resources/{}/admin/*",
                    realm_id, group_id1, project_id2
                ),
                "write",
                Some("deny"),
                &mut txn,
            )
            .await
            .unwrap();

        // Marketing permissions
        enforcer
            .add_policy(
                "marketing_lead",
                &format!("/{}/groups/{}/*", realm_id, group_id2),
                "read",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "marketing_lead",
                &format!("/{}/groups/{}/*", realm_id, group_id2),
                "write",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "marketing_lead",
                &format!("/{}/groups/{}/admin/budget", realm_id, group_id2),
                "write",
                Some("deny"),
                &mut txn,
            )
            .await
            .unwrap();

        // Campaign manager permissions
        enforcer
            .add_policy(
                "campaign_mgr",
                &format!(
                    "/{}/groups/{}/resources/{}/*",
                    realm_id, group_id2, project_id3
                ),
                "read",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "campaign_mgr",
                &format!(
                    "/{}/groups/{}/resources/{}/resources/*",
                    realm_id, group_id2, project_id3
                ),
                "write",
                Some("allow"),
                &mut txn,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "campaign_mgr",
                &format!(
                    "/{}/groups/{}/resources/{}/admin/*",
                    realm_id, group_id2, project_id3
                ),
                "write",
                Some("deny"),
                &mut txn,
            )
            .await
            .unwrap();

        // Team member assignments
        enforcer
            .add_role("alice", "org_admin", &mut txn)
            .await
            .unwrap();
        enforcer
            .add_role("bob", "security_admin", &mut txn)
            .await
            .unwrap();
        enforcer
            .add_role("charlie", "eng_lead", &mut txn)
            .await
            .unwrap();
        enforcer
            .add_role("dave", "dev_webapp", &mut txn)
            .await
            .unwrap();
        enforcer.add_role("eve", "dev_api", &mut txn).await.unwrap();
        enforcer
            .add_role("frank", "marketing_lead", &mut txn)
            .await
            .unwrap();
        enforcer
            .add_role("grace", "campaign_mgr", &mut txn)
            .await
            .unwrap();

        // Special cross-team assignments
        enforcer
            .add_role("eve", "dev_webapp", &mut txn)
            .await
            .unwrap(); // Eve works on both API and webapp
        enforcer
            .add_role("bob", "eng_lead", &mut txn)
            .await
            .unwrap(); // Bob is both security admin and engineering lead

        // Test org admin permissions (alice)
        assert!(
            enforcer
                .enforce("alice", &format!("/{}/admin/users", realm_id), "read")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "alice",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/code",
                        realm_id, group_id1, project_id1
                    ),
                    "write"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce("alice", &format!("/{}/security/keys", realm_id), "read")
                .await
                .unwrap()
        ); // Denied specifically

        // Test security admin permissions (bob)
        assert!(
            enforcer
                .enforce(
                    "bob",
                    &format!("/{}/security/permissions", realm_id),
                    "write"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce("bob", &format!("/{}/admin/audit", realm_id), "read")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "bob",
                    &format!("/{}/groups/{}/admin/audit", realm_id, group_id1),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce("bob", &format!("/{}/admin/users", realm_id), "write")
                .await
                .unwrap()
        ); // Not allowed outside security

        // Test bob's additional eng_lead role
        assert!(
            enforcer
                .enforce(
                    "bob",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/code",
                        realm_id, group_id1, project_id1
                    ),
                    "write"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "bob",
                    &format!("/{}/groups/{}/admin/budget", realm_id, group_id1),
                    "write"
                )
                .await
                .unwrap()
        ); // Denied for eng_lead

        // Test engineering lead permissions (charlie)
        assert!(
            enforcer
                .enforce(
                    "charlie",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/code",
                        realm_id, group_id1, project_id1
                    ),
                    "write"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "charlie",
                    &format!(
                        "/{}/groups/{}/resources/{}/admin/settings",
                        realm_id, group_id1, project_id2
                    ),
                    "write"
                )
                .await
                .unwrap()
        ); // Can admin projects
        assert!(
            !enforcer
                .enforce(
                    "charlie",
                    &format!("/{}/groups/{}/admin/budget", realm_id, group_id1),
                    "write"
                )
                .await
                .unwrap()
        ); // Budget denied
        assert!(
            !enforcer
                .enforce(
                    "charlie",
                    &format!("/{}/groups/{}/resources", realm_id, group_id2),
                    "read"
                )
                .await
                .unwrap()
        ); // Can't access marketing

        // Test developer permissions (dave - webapp only)
        assert!(
            enforcer
                .enforce(
                    "dave",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/code",
                        realm_id, group_id1, project_id1
                    ),
                    "write"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "dave",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/code",
                        realm_id, group_id1, project_id2
                    ),
                    "read"
                )
                .await
                .unwrap()
        ); // Can't access API project
        assert!(
            !enforcer
                .enforce(
                    "dave",
                    &format!(
                        "/{}/groups/{}/resources/{}/admin/settings",
                        realm_id, group_id1, project_id1
                    ),
                    "write"
                )
                .await
                .unwrap()
        ); // Can't admin project

        // Test developer with multiple projects (eve - webapp and api)
        assert!(
            enforcer
                .enforce(
                    "eve",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/code",
                        realm_id, group_id1, project_id1
                    ),
                    "write"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "eve",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/code",
                        realm_id, group_id1, project_id2
                    ),
                    "write"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "eve",
                    &format!(
                        "/{}/groups/{}/resources/{}/admin/settings",
                        realm_id, group_id1, project_id1
                    ),
                    "write"
                )
                .await
                .unwrap()
        ); // Can't admin either project

        // Test marketing permissions (similar structure to engineering)
        assert!(
            enforcer
                .enforce(
                    "frank",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/assets",
                        realm_id, group_id2, project_id3
                    ),
                    "write"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "frank",
                    &format!("/{}/groups/{}/admin/budget", realm_id, group_id2),
                    "write"
                )
                .await
                .unwrap()
        ); // Budget denied

        txn.commit().unwrap();

        cleanup_test_dir(&test_dir);
    }
}
