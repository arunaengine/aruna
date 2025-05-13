#[cfg(test)]
mod tests {
    use aruna_permission::Enforcer;
    use aruna_storage::storage::lmdb::LmdbConfig;
    use aruna_storage::storage::lmdb::LmdbStore;
    use aruna_storage::storage::store::Store;
    use casbin::CoreApi;
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

        let db_name = "casbin_rules";
        let store_config = LmdbConfig {
            path: test_dir.clone(),
            databases: vec![db_name],
        };

        let store = LmdbStore::new(store_config).expect("Failed to create LMDB store");

        (store, test_dir)
    }

    // Helper function to cleanup test resources
    fn cleanup_test_dir(test_dir: &str) {
        fs::remove_dir_all(test_dir).expect("Failed to clean up test directory");
    }

    #[test]
    async fn test_basic_policy_crud() {
        let (store, test_dir) = setup_test_store().await;

        // Create enforcer
        let mut enforcer = Enforcer::new(store, "casbin_rules").await.unwrap();

        // Test adding a policy
        let added = enforcer
            .add_policy("alice", "data1", "read", None)
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
            .remove_policy("alice", "data1", "read", "allow")
            .await
            .unwrap();
        println!("Removed: {}", removed);
        assert!(removed);

        // Confirm removal
        let not_allowed_after_removal = enforcer.enforce("alice", "data1", "read").await.unwrap();
        assert!(!not_allowed_after_removal);

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_role_assignments() {
        let (store, test_dir) = setup_test_store().await;

        // Create enforcer
        let mut enforcer = Enforcer::new(store, "casbin_rules").await.unwrap();

        // Setup a role-based policy
        enforcer
            .add_policy("admin", "data", "read", None)
            .await
            .unwrap();
        enforcer
            .add_policy("admin", "data", "write", None)
            .await
            .unwrap();

        // Add alice to admin role
        enforcer.add_group("alice", "admin").await.unwrap();

        // Check if role assignment works
        assert!(enforcer.has_group("alice", "admin").await);

        // Test permissions via role inheritance
        assert!(enforcer.enforce("alice", "data", "read").await.unwrap());
        assert!(enforcer.enforce("alice", "data", "write").await.unwrap());

        // Remove role
        enforcer.remove_group("alice", "admin").await.unwrap();

        // Permissions should be revoked
        assert!(!enforcer.enforce("alice", "data", "read").await.unwrap());

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_persistence() {
        let (store, test_dir) = setup_test_store().await;

        // First enforcer instance
        {
            let mut enforcer = Enforcer::new(store.clone(), "casbin_rules").await.unwrap();

            enforcer
                .add_policy("alice", "data1", "read", None)
                .await
                .unwrap();
            enforcer
                .add_policy("bob", "data2", "write", None)
                .await
                .unwrap();

            // Save explicitly
            enforcer.save_policy().await.unwrap();
        }

        // Second enforcer instance - should load the saved policies
        {
            let enforcer = Enforcer::new(store, "casbin_rules").await.unwrap();

            // Test persistence
            assert!(enforcer.enforce("alice", "data1", "read").await.unwrap());
            assert!(enforcer.enforce("bob", "data2", "write").await.unwrap());
            assert!(!enforcer.enforce("alice", "data2", "read").await.unwrap());
        }

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_clear_policy() {
        let (store, test_dir) = setup_test_store().await;

        // Create enforcer
        let mut enforcer = Enforcer::new(store, "casbin_rules").await.unwrap();

        // Add policies
        enforcer
            .add_policy("alice", "data1", "read", None)
            .await
            .unwrap();
        enforcer
            .add_policy("bob", "data2", "write", None)
            .await
            .unwrap();

        // Add roles
        enforcer.add_group("alice", "admin").await.unwrap();

        // Clear all policies
        enforcer
            .inner
            .get_mut_adapter()
            .clear_policy()
            .await
            .unwrap();
        enforcer.inner.load_policy().await.unwrap();

        // All policies should be gone
        assert!(!enforcer.enforce("alice", "data1", "read").await.unwrap());
        assert!(!enforcer.enforce("bob", "data2", "write").await.unwrap());

        // Role assignments should be gone too
        assert!(!enforcer.has_group("alice", "admin").await);

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_nested_groups() {
        let (store, test_dir) = setup_test_store().await;

        // Create enforcer
        let mut enforcer = Enforcer::new(store, "casbin_rules").await.unwrap();

        // Set up a role hierarchy
        // editor <- manager <- admin
        // Set permissions for each role
        enforcer
            .add_policy("editor", "articles", "read", None)
            .await
            .unwrap();
        enforcer
            .add_policy("editor", "articles", "edit", None)
            .await
            .unwrap();

        enforcer
            .add_policy("manager", "articles", "publish", None)
            .await
            .unwrap();
        enforcer
            .add_policy("manager", "users", "view", None)
            .await
            .unwrap();

        enforcer
            .add_policy("admin", "users", "edit", None)
            .await
            .unwrap();
        enforcer
            .add_policy("admin", "system", "config", None)
            .await
            .unwrap();

        // Set up the role hierarchy
        enforcer.add_group("manager", "editor").await.unwrap(); // managers inherit editor permissions
        enforcer.add_group("admin", "manager").await.unwrap(); // admins inherit manager permissions

        // Assign users to roles
        enforcer.add_group("alice", "editor").await.unwrap();
        enforcer.add_group("bob", "manager").await.unwrap();
        enforcer.add_group("charlie", "admin").await.unwrap();

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

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_hierarchical_wildcards() {
        let (store, test_dir) = setup_test_store().await;

        // Create enforcer
        let mut enforcer = Enforcer::new(store, "casbin_rules").await.unwrap();

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
            .add_policy(realm_admin, &format!("/{}", realm_id), "read", None)
            .await
            .unwrap();
        enforcer
            .add_policy(realm_admin, &format!("/{}/*", realm_id), "read", None)
            .await
            .unwrap();
        enforcer
            .add_policy(realm_admin, &format!("/{}/*", realm_id), "write", None)
            .await
            .unwrap();

        // Group admin can access everything in their group
        enforcer
            .add_policy(
                group_admin,
                &format!("/{}/groups/{}/", realm_id, group_id),
                "read",
                None,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                group_admin,
                &format!("/{}/groups/{}/*", realm_id, group_id),
                "read",
                None,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                group_admin,
                &format!("/{}/groups/{}/*", realm_id, group_id),
                "write",
                None,
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
            )
            .await
            .unwrap();

        // Assign users to roles
        enforcer.add_group("alice", realm_admin).await.unwrap();
        enforcer.add_group("bob", group_admin).await.unwrap();
        enforcer.add_group("charlie", project_admin).await.unwrap();
        enforcer.add_group("dave", resource_viewer).await.unwrap();
        enforcer.add_group("eve", resource_editor).await.unwrap();

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

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_allow_deny_precedence() {
        let (store, test_dir) = setup_test_store().await;

        // Create enforcer
        let mut enforcer = Enforcer::new(store, "casbin_rules").await.unwrap();

        // Setup basic allow policies
        enforcer
            .add_policy("alice", "data1", "read", Some("allow"))
            .await
            .unwrap();
        enforcer
            .add_policy("bob", "data2", "write", Some("allow"))
            .await
            .unwrap();

        // Setup explicit deny policy that overrides an allow
        enforcer
            .add_policy("alice", "data1", "read", Some("deny"))
            .await
            .unwrap();

        // Test basic allow
        assert!(enforcer.enforce("bob", "data2", "write").await.unwrap());

        // Test deny takes precedence over allow
        assert!(!enforcer.enforce("alice", "data1", "read").await.unwrap());

        // Test default deny (no policy)
        assert!(!enforcer.enforce("alice", "data3", "read").await.unwrap());

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_hierarchical_allow_deny() {
        let (store, test_dir) = setup_test_store().await;

        // Create enforcer
        let mut enforcer = Enforcer::new(store, "casbin_rules").await.unwrap();

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
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "admin",
                &format!("/{}/groups/{}/restricted", realm_id, group_id),
                "access",
                Some("deny"),
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

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_nested_allow_deny_inheritance() {
        let (store, test_dir) = setup_test_store().await;

        // Create enforcer
        let mut enforcer = Enforcer::new(store, "casbin_rules").await.unwrap();

        let realm_id = "realm123";
        let group_id = "group456";
        let project_id = "project789";

        // Set up role hierarchy
        enforcer.add_group("user", "reader").await.unwrap();
        enforcer.add_group("editor", "reader").await.unwrap();
        enforcer.add_group("admin", "editor").await.unwrap();

        // Set up base permissions
        enforcer
            .add_policy(
                "reader",
                &format!("/{}/groups/*/resources/*/resources/*", realm_id),
                "read",
                Some("allow"),
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "editor",
                &format!("/{}/groups/*/resources/*/resources/*", realm_id),
                "write",
                Some("allow"),
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "admin",
                &format!("/{}/groups/*/resources/*/admin", realm_id),
                "write",
                Some("allow"),
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
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "editor",
                &format!("/{}/groups/{}/resources/*/locked", realm_id, group_id),
                "write",
                Some("deny"),
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "admin",
                &format!("/{}/groups/*/security", realm_id),
                "write",
                Some("deny"),
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

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_allow_deny_realworld_scenario() {
        let (store, test_dir) = setup_test_store().await;

        // Create enforcer
        let mut enforcer = Enforcer::new(store, "casbin_rules").await.unwrap();

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
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "org_admin",
                &format!("/{}/*", realm_id),
                "write",
                Some("allow"),
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "org_admin",
                &format!("/{}/security/keys", realm_id),
                "read",
                Some("deny"),
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
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "security_admin",
                &format!("/{}/security/*", realm_id),
                "write",
                Some("allow"),
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "security_admin",
                &format!("/{}/admin/audit", realm_id),
                "read",
                Some("allow"),
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "security_admin",
                &format!("/{}/groups/*/admin/audit", realm_id),
                "read",
                Some("allow"),
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
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "eng_lead",
                &format!("/{}/groups/{}/*", realm_id, group_id1),
                "write",
                Some("allow"),
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "eng_lead",
                &format!("/{}/groups/{}/admin/budget", realm_id, group_id1),
                "write",
                Some("deny"),
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
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "marketing_lead",
                &format!("/{}/groups/{}/*", realm_id, group_id2),
                "write",
                Some("allow"),
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "marketing_lead",
                &format!("/{}/groups/{}/admin/budget", realm_id, group_id2),
                "write",
                Some("deny"),
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
            )
            .await
            .unwrap();

        // Team member assignments
        enforcer.add_group("alice", "org_admin").await.unwrap();
        enforcer.add_group("bob", "security_admin").await.unwrap();
        enforcer.add_group("charlie", "eng_lead").await.unwrap();
        enforcer.add_group("dave", "dev_webapp").await.unwrap();
        enforcer.add_group("eve", "dev_api").await.unwrap();
        enforcer.add_group("frank", "marketing_lead").await.unwrap();
        enforcer.add_group("grace", "campaign_mgr").await.unwrap();

        // Special cross-team assignments
        enforcer.add_group("eve", "dev_webapp").await.unwrap(); // Eve works on both API and webapp
        enforcer.add_group("bob", "eng_lead").await.unwrap(); // Bob is both security admin and engineering lead

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

        cleanup_test_dir(&test_dir);
    }
}
