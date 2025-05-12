#[cfg(test)]
mod tests {
    use aruna_permission::Enforcer;
    use aruna_permission::StoreAdapter;
    use aruna_storage::storage::lmdb::LmdbConfig;
    use aruna_storage::storage::lmdb::LmdbStore;
    use aruna_storage::storage::store::Store;
    use casbin::CoreApi;
    use casbin::Filter;
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
            .remove_policy("alice", "data1", "read")
            .await
            .unwrap();
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
    async fn test_multiple_policies() {
        let (store, test_dir) = setup_test_store().await;

        // Create enforcer
        let mut enforcer = Enforcer::new(store, "casbin_rules").await.unwrap();

        // Add multiple policies
        let policies = vec![
            vec!["alice".to_string(), "data1".to_string(), "read".to_string()],
            vec!["bob".to_string(), "data2".to_string(), "write".to_string()],
            vec![
                "charlie".to_string(),
                "data3".to_string(),
                "read".to_string(),
            ],
        ];

        // Add policies via adapter
        enforcer
            .inner
            .get_mut_adapter()
            .add_policies("p", "p", policies.clone())
            .await
            .unwrap();
        enforcer.inner.load_policy().await.unwrap();

        // Check all policies were added
        let stored_policies = enforcer.get_policies().await;
        assert_eq!(stored_policies.len(), 3);

        println!("Stored policies: {:?}", stored_policies);

        // Check enforcement
        assert!(enforcer.enforce("alice", "data1", "read").await.unwrap());
        assert!(enforcer.enforce("bob", "data2", "write").await.unwrap());
        assert!(enforcer.enforce("charlie", "data3", "read").await.unwrap());

        // Remove multiple policies
        let to_remove = vec![
            vec!["alice".to_string(), "data1".to_string(), "read".to_string()],
            vec![
                "charlie".to_string(),
                "data3".to_string(),
                "read".to_string(),
            ],
        ];

        enforcer
            .inner
            .get_mut_adapter()
            .remove_policies("p", "p", to_remove)
            .await
            .unwrap();
        enforcer.inner.load_policy().await.unwrap();

        // Check removal
        assert!(!enforcer.enforce("alice", "data1", "read").await.unwrap());
        assert!(enforcer.enforce("bob", "data2", "write").await.unwrap());
        assert!(!enforcer.enforce("charlie", "data3", "read").await.unwrap());

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_filtered_policy() {
        let (store, test_dir) = setup_test_store().await;

        // Setup policies first
        {
            let mut enforcer = Enforcer::new(store.clone(), "casbin_rules").await.unwrap();

            // Add various policies
            enforcer
                .add_policy("alice", "data1", "read", None)
                .await
                .unwrap();
            enforcer
                .add_policy("alice", "data1", "write", None)
                .await
                .unwrap();
            enforcer
                .add_policy("alice", "data2", "read", None)
                .await
                .unwrap();
            enforcer
                .add_policy("bob", "data1", "read", None)
                .await
                .unwrap();
            enforcer
                .add_policy("bob", "data2", "write", None)
                .await
                .unwrap();

            enforcer.save_policy().await.unwrap();
        }

        // Test filtered loading
        {
            // Create a new adapter and enforce with filtered policy
            let adapter = StoreAdapter::new(store, "casbin_rules");
            let m = casbin::DefaultModel::default();

            // Create a filter for alice's policies
            let filter = Filter {
                p: vec!["alice"],
                g: vec![],
            };

            // Create an enforcer with the filtered model
            let mut filtered_enforcer = casbin::Enforcer::new(m, adapter).await.unwrap();
            filtered_enforcer
                .load_filtered_policy(filter)
                .await
                .unwrap();

            // Check if only alice's policies were loaded
            assert!(
                filtered_enforcer
                    .enforce(("alice", "data1", "read"))
                    .unwrap()
            );
            assert!(
                filtered_enforcer
                    .enforce(("alice", "data1", "write"))
                    .unwrap()
            );
            assert!(
                filtered_enforcer
                    .enforce(("alice", "data2", "read"))
                    .unwrap()
            );

            // Bob's should not be loaded
            assert!(!filtered_enforcer.enforce(("bob", "data1", "read")).unwrap());
            assert!(
                !filtered_enforcer
                    .enforce(("bob", "data2", "write"))
                    .unwrap()
            );

            // Verify is_filtered flag
            assert!(filtered_enforcer.is_filtered());
        }

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_remove_filtered_policy() {
        let (store, test_dir) = setup_test_store().await;

        // Create enforcer
        let mut enforcer = Enforcer::new(store, "casbin_rules").await.unwrap();

        // Add various policies
        enforcer
            .add_policy("alice", "data1", "read", None)
            .await
            .unwrap();
        enforcer
            .add_policy("alice", "data2", "read", None)
            .await
            .unwrap();
        enforcer
            .add_policy("alice", "data3", "read", None)
            .await
            .unwrap();
        enforcer
            .add_policy("bob", "data1", "read", None)
            .await
            .unwrap();
        enforcer
            .add_policy("bob", "data2", "write", None)
            .await
            .unwrap();

        // Remove filtered policies - all alice's read permissions
        enforcer
            .inner
            .get_mut_adapter()
            .remove_filtered_policy("p", "p", 0, vec!["alice".to_string()])
            .await
            .unwrap();

        enforcer.inner.load_policy().await.unwrap();

        // Alice's policies should be gone
        assert!(!enforcer.enforce("alice", "data1", "read").await.unwrap());
        assert!(!enforcer.enforce("alice", "data2", "read").await.unwrap());
        assert!(!enforcer.enforce("alice", "data3", "read").await.unwrap());

        // Bob's should remain
        assert!(enforcer.enforce("bob", "data1", "read").await.unwrap());
        assert!(enforcer.enforce("bob", "data2", "write").await.unwrap());

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
                &format!("/{}/groups/{}", realm_id, group_id),
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
                &format!("/{}/groups/{}/resources/{}", realm_id, group_id, project_id),
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
                    "/{}/groups/{}/resources/{}/resources/{}",
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
                    "/{}/groups/{}/resources/{}/resources/{}",
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
                    "/{}/groups/{}/resources/{}/resources/{}",
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
                .enforce("alice", &format!("/{}", realm_id), "read")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce("alice", &format!("/{}", realm_id), "write")
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
                    &format!("/{}/groups/{}", realm_id, group_id),
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
                    &format!("/{}/groups/{}/resources/{}", realm_id, group_id, project_id),
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
                    "alice",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}",
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
                .enforce("bob", &format!("/{}/groups/{}", realm_id, group_id), "read")
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
                    &format!("/{}/groups/{}/resources/{}", realm_id, group_id, project_id),
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
                    "bob",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}",
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
                    &format!("/{}/groups/{}/resources/{}", realm_id, group_id, project_id),
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
                        "/{}/groups/{}/resources/{}/resources/{}",
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
                        "/{}/groups/{}/resources/{}/resources/{}",
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
    async fn test_deep_wildcards_with_varying_depth() {
        let (store, test_dir) = setup_test_store().await;

        // Create enforcer
        let mut enforcer = Enforcer::new(store, "casbin_rules").await.unwrap();

        let realm_id = "realm123";
        let group_id = "group456";
        let project_id = "project789";

        // Set up different wildcards with varying depths

        // Top level wildcard
        enforcer
            .add_policy("top_admin", &format!("/{}/*", realm_id), "access", None)
            .await
            .unwrap();

        // Mid-level wildcards
        enforcer
            .add_policy(
                "mid_admin",
                &format!("/{}/groups/{}/*", realm_id, group_id),
                "access",
                None,
            )
            .await
            .unwrap();

        // Deep wildcards
        enforcer
            .add_policy(
                "deep_admin",
                &format!(
                    "/{}/groups/{}/resources/{}/*",
                    realm_id, group_id, project_id
                ),
                "access",
                None,
            )
            .await
            .unwrap();

        // Very specific permissions (no wildcards)
        enforcer
            .add_policy(
                "specific_user",
                &format!(
                    "/{}/groups/{}/resources/{}/resources/resource101",
                    realm_id, group_id, project_id
                ),
                "access",
                None,
            )
            .await
            .unwrap();

        // Partial wildcards (with more specific paths)
        enforcer
            .add_policy(
                "partial_admin",
                &format!("/{}/admin/*", realm_id),
                "access",
                None,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "project_viewer",
                &format!("/{}/groups/{}/resources/*/view", realm_id, group_id),
                "access",
                None,
            )
            .await
            .unwrap();

        // Assign users to roles
        enforcer.add_group("alice", "top_admin").await.unwrap();
        enforcer.add_group("bob", "mid_admin").await.unwrap();
        enforcer.add_group("charlie", "deep_admin").await.unwrap();
        enforcer.add_group("dave", "specific_user").await.unwrap();
        enforcer.add_group("eve", "partial_admin").await.unwrap();
        enforcer.add_group("frank", "project_viewer").await.unwrap();

        // Test top level wildcard (alice)

        // Basic level access
        assert!(
            enforcer
                .enforce("alice", &format!("/{}", realm_id), "access")
                .await
                .unwrap()
        ); // Matches /*

        // Admin level access
        assert!(
            enforcer
                .enforce("alice", &format!("/{}/admin", realm_id), "access")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce("alice", &format!("/{}/admin/users", realm_id), "access")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "alice",
                    &format!("/{}/admin/settings/security", realm_id),
                    "access"
                )
                .await
                .unwrap()
        );

        // Group level access
        assert!(
            enforcer
                .enforce(
                    "alice",
                    &format!("/{}/groups/{}", realm_id, group_id),
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "alice",
                    &format!("/{}/groups/{}/admin", realm_id, group_id),
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "alice",
                    &format!("/{}/groups/{}/users", realm_id, group_id),
                    "access"
                )
                .await
                .unwrap()
        );

        // Project level access
        assert!(
            enforcer
                .enforce(
                    "alice",
                    &format!("/{}/groups/{}/resources/{}", realm_id, group_id, project_id),
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "alice",
                    &format!(
                        "/{}/groups/{}/resources/{}/settings",
                        realm_id, group_id, project_id
                    ),
                    "access"
                )
                .await
                .unwrap()
        );

        // Deep level access
        assert!(
            enforcer
                .enforce(
                    "alice",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/resource101",
                        realm_id, group_id, project_id
                    ),
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "alice",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/resource101/metadata",
                        realm_id, group_id, project_id
                    ),
                    "access"
                )
                .await
                .unwrap()
        );

        // Test mid-level wildcard (bob)

        // Cannot access realm admin
        assert!(
            !enforcer
                .enforce("bob", &format!("/{}/admin", realm_id), "access")
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce("bob", &format!("/{}/admin/users", realm_id), "access")
                .await
                .unwrap()
        );

        // Can access own group
        assert!(
            enforcer
                .enforce(
                    "bob",
                    &format!("/{}/groups/{}", realm_id, group_id),
                    "access"
                )
                .await
                .unwrap()
        ); // Matches /*
        assert!(
            enforcer
                .enforce(
                    "bob",
                    &format!("/{}/groups/{}/admin", realm_id, group_id),
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "bob",
                    &format!("/{}/groups/{}/users", realm_id, group_id),
                    "access"
                )
                .await
                .unwrap()
        );

        // Can access projects in own group
        assert!(
            enforcer
                .enforce(
                    "bob",
                    &format!("/{}/groups/{}/resources/{}", realm_id, group_id, project_id),
                    "access"
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
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "bob",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/resource101",
                        realm_id, group_id, project_id
                    ),
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "bob",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/resource101/metadata",
                        realm_id, group_id, project_id
                    ),
                    "access"
                )
                .await
                .unwrap()
        );

        // Cannot access other groups
        assert!(
            !enforcer
                .enforce(
                    "bob",
                    &format!("/{}/groups/other_group", realm_id),
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "bob",
                    &format!("/{}/groups/other_group/admin", realm_id),
                    "access"
                )
                .await
                .unwrap()
        );

        // Test deep wildcard (charlie)

        // Cannot access realm admin or group admin
        assert!(
            !enforcer
                .enforce("charlie", &format!("/{}/admin", realm_id), "access")
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "charlie",
                    &format!("/{}/groups/{}/admin", realm_id, group_id),
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "charlie",
                    &format!("/{}/groups/{}", realm_id, group_id),
                    "access"
                )
                .await
                .unwrap()
        );

        // Can access specific project
        assert!(
            enforcer
                .enforce(
                    "charlie",
                    &format!("/{}/groups/{}/resources/{}", realm_id, group_id, project_id),
                    "access"
                )
                .await
                .unwrap()
        ); // Matches /*
        assert!(
            enforcer
                .enforce(
                    "charlie",
                    &format!(
                        "/{}/groups/{}/resources/{}/admin",
                        realm_id, group_id, project_id
                    ),
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "charlie",
                    &format!(
                        "/{}/groups/{}/resources/{}/settings",
                        realm_id, group_id, project_id
                    ),
                    "access"
                )
                .await
                .unwrap()
        );

        // Can access resources in specific project
        assert!(
            enforcer
                .enforce(
                    "charlie",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/resource101",
                        realm_id, group_id, project_id
                    ),
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "charlie",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/resource101/metadata",
                        realm_id, group_id, project_id
                    ),
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "charlie",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/different_resource",
                        realm_id, group_id, project_id
                    ),
                    "access"
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
                    "access"
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
                    "access"
                )
                .await
                .unwrap()
        );

        // Test specific permission (dave)

        // Can only access specific resource
        assert!(
            enforcer
                .enforce(
                    "dave",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/resource101",
                        realm_id, group_id, project_id
                    ),
                    "access"
                )
                .await
                .unwrap()
        );

        // Cannot access resource metadata
        assert!(
            !enforcer
                .enforce(
                    "dave",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/resource101/metadata",
                        realm_id, group_id, project_id
                    ),
                    "access"
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
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "dave",
                    &format!("/{}/groups/{}/resources/{}", realm_id, group_id, project_id),
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "dave",
                    &format!("/{}/groups/{}", realm_id, group_id),
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce("dave", &format!("/{}/admin", realm_id), "access")
                .await
                .unwrap()
        );

        // Test partial admin (eve) - only admin section

        // Can access admin section
        assert!(
            enforcer
                .enforce("eve", &format!("/{}/admin", realm_id), "access")
                .await
                .unwrap()
        ); // Matches /*
        assert!(
            enforcer
                .enforce("eve", &format!("/{}/admin/users", realm_id), "access")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce("eve", &format!("/{}/admin/settings", realm_id), "access")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce("eve", &format!("/{}/admin/logs/system", realm_id), "access")
                .await
                .unwrap()
        );

        // Cannot access groups or other sections
        assert!(
            !enforcer
                .enforce("eve", &format!("/{}", realm_id), "access")
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce("eve", &format!("/{}/groups", realm_id), "access")
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "eve",
                    &format!("/{}/groups/{}", realm_id, group_id),
                    "access"
                )
                .await
                .unwrap()
        );

        // Test project viewer (frank) - can view any project

        // Can access view for any project
        assert!(
            enforcer
                .enforce(
                    "frank",
                    &format!(
                        "/{}/groups/{}/resources/{}/view",
                        realm_id, group_id, project_id
                    ),
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "frank",
                    &format!(
                        "/{}/groups/{}/resources/other_project/view",
                        realm_id, group_id
                    ),
                    "access"
                )
                .await
                .unwrap()
        );

        // Cannot access project admin or other paths
        assert!(
            !enforcer
                .enforce(
                    "frank",
                    &format!("/{}/groups/{}/resources/{}", realm_id, group_id, project_id),
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "frank",
                    &format!(
                        "/{}/groups/{}/resources/{}/admin",
                        realm_id, group_id, project_id
                    ),
                    "access"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "frank",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/resource101",
                        realm_id, group_id, project_id
                    ),
                    "access"
                )
                .await
                .unwrap()
        );

        cleanup_test_dir(&test_dir);
    }

    #[test]
    async fn test_complex_hierarchical_permissions() {
        let (store, test_dir) = setup_test_store().await;

        // Create enforcer
        let mut enforcer = Enforcer::new(store, "casbin_rules").await.unwrap();

        // Define realm structure
        let realm_id = "realm123";

        // Define multiple groups
        let group_id1 = "group1";
        let group_id2 = "group2";

        // Define multiple projects per group
        let project_id1 = "project1";
        let project_id2 = "project2";
        let project_id3 = "project3";

        // Define multiple resources per project
        let resource_id1 = "resource1";
        let resource_id2 = "resource2";

        // Set up super admin (can access everything)
        enforcer
            .add_policy("super_admin", "/*", "read", None)
            .await
            .unwrap();
        enforcer
            .add_policy("super_admin", "/*", "write", None)
            .await
            .unwrap();

        // Set up realm admin
        enforcer
            .add_policy("realm_admin", &format!("/{}/*", realm_id), "read", None)
            .await
            .unwrap();
        enforcer
            .add_policy("realm_admin", &format!("/{}/*", realm_id), "write", None)
            .await
            .unwrap();

        // Set up group admins (one for each group)
        enforcer
            .add_policy(
                "group1_admin",
                &format!("/{}/groups/{}/*", realm_id, group_id1),
                "read",
                None,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "group1_admin",
                &format!("/{}/groups/{}/*", realm_id, group_id1),
                "write",
                None,
            )
            .await
            .unwrap();

        enforcer
            .add_policy(
                "group2_admin",
                &format!("/{}/groups/{}/*", realm_id, group_id2),
                "read",
                None,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "group2_admin",
                &format!("/{}/groups/{}/*", realm_id, group_id2),
                "write",
                None,
            )
            .await
            .unwrap();

        // Set up project admins
        enforcer
            .add_policy(
                "project1_admin",
                &format!(
                    "/{}/groups/{}/resources/{}/*",
                    realm_id, group_id1, project_id1
                ),
                "read",
                None,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "project1_admin",
                &format!(
                    "/{}/groups/{}/resources/{}/*",
                    realm_id, group_id1, project_id1
                ),
                "write",
                None,
            )
            .await
            .unwrap();

        enforcer
            .add_policy(
                "project2_admin",
                &format!(
                    "/{}/groups/{}/resources/{}/*",
                    realm_id, group_id1, project_id2
                ),
                "read",
                None,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "project2_admin",
                &format!(
                    "/{}/groups/{}/resources/{}/*",
                    realm_id, group_id1, project_id2
                ),
                "write",
                None,
            )
            .await
            .unwrap();

        enforcer
            .add_policy(
                "project3_admin",
                &format!(
                    "/{}/groups/{}/resources/{}/*",
                    realm_id, group_id2, project_id3
                ),
                "read",
                None,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "project3_admin",
                &format!(
                    "/{}/groups/{}/resources/{}/*",
                    realm_id, group_id2, project_id3
                ),
                "write",
                None,
            )
            .await
            .unwrap();

        // Set up resource viewers and editors
        enforcer
            .add_policy(
                "resource1_viewer",
                &format!(
                    "/{}/groups/{}/resources/{}/resources/{}",
                    realm_id, group_id1, project_id1, resource_id1
                ),
                "read",
                None,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "resource1_editor",
                &format!(
                    "/{}/groups/{}/resources/{}/resources/{}",
                    realm_id, group_id1, project_id1, resource_id1
                ),
                "read",
                None,
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "resource1_editor",
                &format!(
                    "/{}/groups/{}/resources/{}/resources/{}",
                    realm_id, group_id1, project_id1, resource_id1
                ),
                "write",
                None,
            )
            .await
            .unwrap();

        // Set up multi-project viewer
        enforcer
            .add_policy(
                "multi_project_viewer",
                &format!("/{}/groups/{}/resources/*/resources/*", realm_id, group_id1),
                "read",
                None,
            )
            .await
            .unwrap();

        // Assign users
        enforcer.add_group("alice", "super_admin").await.unwrap();
        enforcer.add_group("bob", "realm_admin").await.unwrap();
        enforcer.add_group("charlie", "group1_admin").await.unwrap();
        enforcer.add_group("dave", "group2_admin").await.unwrap();
        enforcer.add_group("eve", "project1_admin").await.unwrap();
        enforcer.add_group("frank", "project2_admin").await.unwrap();
        enforcer.add_group("grace", "project3_admin").await.unwrap();
        enforcer
            .add_group("hannah", "resource1_viewer")
            .await
            .unwrap();
        enforcer.add_group("ian", "resource1_editor").await.unwrap();
        enforcer
            .add_group("julia", "multi_project_viewer")
            .await
            .unwrap();

        // Test super admin (alice) - can access everything
        assert!(
            enforcer
                .enforce("alice", "/any_realm", "read")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce("alice", "/any_realm/admin", "write")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "alice",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}/metadata",
                        realm_id, group_id1, project_id1, resource_id1
                    ),
                    "write"
                )
                .await
                .unwrap()
        );

        // Test realm admin (bob) - can access everything in realm but not outside
        assert!(
            enforcer
                .enforce("bob", &format!("/{}", realm_id), "read")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce("bob", &format!("/{}/admin", realm_id), "write")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "bob",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}/metadata",
                        realm_id, group_id1, project_id1, resource_id1
                    ),
                    "write"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce("bob", "/other_realm", "read")
                .await
                .unwrap()
        );

        // Test group admin (charlie) - can access everything in group1 but not group2
        assert!(
            enforcer
                .enforce(
                    "charlie",
                    &format!("/{}/groups/{}", realm_id, group_id1),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "charlie",
                    &format!("/{}/groups/{}/admin", realm_id, group_id1),
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
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id1, project_id1, resource_id1
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
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id1, project_id2, resource_id2
                    ),
                    "write"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "charlie",
                    &format!("/{}/groups/{}", realm_id, group_id2),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce("charlie", &format!("/{}/admin", realm_id), "read")
                .await
                .unwrap()
        );

        // Test group admin (dave) - can access everything in group2 but not group1
        assert!(
            enforcer
                .enforce(
                    "dave",
                    &format!("/{}/groups/{}", realm_id, group_id2),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "dave",
                    &format!("/{}/groups/{}/admin", realm_id, group_id2),
                    "write"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "dave",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id2, project_id3, resource_id1
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
                    &format!("/{}/groups/{}", realm_id, group_id1),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce("dave", &format!("/{}/admin", realm_id), "read")
                .await
                .unwrap()
        );

        // Test project admin (eve) - can access everything in project1 but not other projects
        assert!(
            enforcer
                .enforce(
                    "eve",
                    &format!(
                        "/{}/groups/{}/resources/{}",
                        realm_id, group_id1, project_id1
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "eve",
                    &format!(
                        "/{}/groups/{}/resources/{}/admin",
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
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id1, project_id1, resource_id1
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
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id1, project_id1, resource_id2
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
                        "/{}/groups/{}/resources/{}",
                        realm_id, group_id1, project_id2
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "eve",
                    &format!(
                        "/{}/groups/{}/resources/{}",
                        realm_id, group_id2, project_id3
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "eve",
                    &format!("/{}/groups/{}", realm_id, group_id1),
                    "read"
                )
                .await
                .unwrap()
        );

        // Test resource viewer (hannah) - can only read resource1
        assert!(
            enforcer
                .enforce(
                    "hannah",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id1, project_id1, resource_id1
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "hannah",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id1, project_id1, resource_id1
                    ),
                    "write"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "hannah",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id1, project_id1, resource_id2
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "hannah",
                    &format!(
                        "/{}/groups/{}/resources/{}",
                        realm_id, group_id1, project_id1
                    ),
                    "read"
                )
                .await
                .unwrap()
        );

        // Test resource editor (ian) - can read and write resource1
        assert!(
            enforcer
                .enforce(
                    "ian",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id1, project_id1, resource_id1
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "ian",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id1, project_id1, resource_id1
                    ),
                    "write"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "ian",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id1, project_id1, resource_id2
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "ian",
                    &format!(
                        "/{}/groups/{}/resources/{}",
                        realm_id, group_id1, project_id1
                    ),
                    "read"
                )
                .await
                .unwrap()
        );

        // Test multi-project viewer (julia) - can read any resource in any project in group1
        assert!(
            enforcer
                .enforce(
                    "julia",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id1, project_id1, resource_id1
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "julia",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id1, project_id1, resource_id2
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "julia",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id1, project_id2, resource_id1
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "julia",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/any_resource",
                        realm_id, group_id1, project_id2
                    ),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "julia",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id1, project_id1, resource_id1
                    ),
                    "write"
                )
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce(
                    "julia",
                    &format!(
                        "/{}/groups/{}/resources/{}/resources/{}",
                        realm_id, group_id2, project_id3, resource_id1
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
        let resource_id = "resource101";

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
    async fn test_complex_allow_deny_wildcards() {
        let (store, test_dir) = setup_test_store().await;

        // Create enforcer
        let mut enforcer = Enforcer::new(store, "casbin_rules").await.unwrap();

        let realm_id = "realm123";
        let group_id = "group456";

        // Setup complex allow/deny permissions for different levels and users

        // Super admin has broad access but with specific denies
        enforcer
            .add_policy("super_admin", "/*", "read", Some("allow"))
            .await
            .unwrap();
        enforcer
            .add_policy("super_admin", "/*", "write", Some("allow"))
            .await
            .unwrap();
        enforcer
            .add_policy("super_admin", "/*/audit_logs", "write", Some("deny"))
            .await
            .unwrap();

        // Realm admin has access to realm with exceptions
        enforcer
            .add_policy(
                "realm_admin",
                &format!("/{}/*", realm_id),
                "read",
                Some("allow"),
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "realm_admin",
                &format!("/{}/*", realm_id),
                "write",
                Some("allow"),
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "realm_admin",
                &format!("/{}/security/*", realm_id),
                "write",
                Some("deny"),
            )
            .await
            .unwrap();

        // Group admin has access to group with exceptions
        enforcer
            .add_policy(
                "group_admin",
                &format!("/{}/groups/{}/*", realm_id, group_id),
                "read",
                Some("allow"),
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "group_admin",
                &format!("/{}/groups/{}/*", realm_id, group_id),
                "write",
                Some("allow"),
            )
            .await
            .unwrap();
        enforcer
            .add_policy(
                "group_admin",
                &format!("/{}/groups/{}/restricted/*", realm_id, group_id),
                "write",
                Some("deny"),
            )
            .await
            .unwrap();

        // Blocked user has only specific allows but broad denies
        enforcer
            .add_policy(
                "blocked_user",
                &format!("/{}/public/*", realm_id),
                "read",
                Some("allow"),
            )
            .await
            .unwrap();
        enforcer
            .add_policy("blocked_user", "/*", "write", Some("deny"))
            .await
            .unwrap();

        // Test super admin permissions
        assert!(
            enforcer
                .enforce("super_admin", "/any_realm/resource", "read")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce("super_admin", "/any_realm/resource", "write")
                .await
                .unwrap()
        );
        assert!(
            !enforcer
                .enforce("super_admin", "/any_realm/audit_logs", "write")
                .await
                .unwrap()
        ); // Denied specifically
        assert!(
            enforcer
                .enforce("super_admin", "/any_realm/audit_logs", "read")
                .await
                .unwrap()
        ); // Reading logs still allowed

        // Test realm admin permissions
        assert!(
            enforcer
                .enforce("realm_admin", &format!("/{}/resource", realm_id), "read")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce("realm_admin", &format!("/{}/resource", realm_id), "write")
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "realm_admin",
                    &format!("/{}/security/users", realm_id),
                    "read"
                )
                .await
                .unwrap()
        ); // Reading security is allowed
        assert!(
            !enforcer
                .enforce(
                    "realm_admin",
                    &format!("/{}/security/permissions", realm_id),
                    "write"
                )
                .await
                .unwrap()
        ); // Writing to security is denied
        assert!(
            !enforcer
                .enforce("realm_admin", "/other_realm/resource", "read")
                .await
                .unwrap()
        ); // Other realms not allowed

        // Test group admin permissions
        assert!(
            enforcer
                .enforce(
                    "group_admin",
                    &format!("/{}/groups/{}/resource", realm_id, group_id),
                    "read"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "group_admin",
                    &format!("/{}/groups/{}/resource", realm_id, group_id),
                    "write"
                )
                .await
                .unwrap()
        );
        assert!(
            enforcer
                .enforce(
                    "group_admin",
                    &format!("/{}/groups/{}/restricted/resource", realm_id, group_id),
                    "read"
                )
                .await
                .unwrap()
        ); // Reading restricted is allowed
        assert!(
            !enforcer
                .enforce(
                    "group_admin",
                    &format!("/{}/groups/{}/restricted/resource", realm_id, group_id),
                    "write"
                )
                .await
                .unwrap()
        ); // Writing to restricted is denied
        assert!(
            !enforcer
                .enforce(
                    "group_admin",
                    &format!("/{}/groups/other_group/resource", realm_id),
                    "read"
                )
                .await
                .unwrap()
        ); // Other groups not allowed

        // Test blocked user permissions
        assert!(
            enforcer
                .enforce(
                    "blocked_user",
                    &format!("/{}/public/resource", realm_id),
                    "read"
                )
                .await
                .unwrap()
        ); // Can read public resources
        assert!(
            !enforcer
                .enforce(
                    "blocked_user",
                    &format!("/{}/public/resource", realm_id),
                    "write"
                )
                .await
                .unwrap()
        ); // Cannot write anything (broad deny)
        assert!(
            !enforcer
                .enforce(
                    "blocked_user",
                    &format!("/{}/private/resource", realm_id),
                    "read"
                )
                .await
                .unwrap()
        ); // Cannot read non-public

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
