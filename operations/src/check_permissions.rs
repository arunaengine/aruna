use aruna_core::errors::AuthorizationError;
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::structs::{AuthContext, Permission};
use aruna_core::types::{Effects, TxnId};

use crate::permission_rules::{PermissionRulesConfig, PermissionRulesOperation};

#[derive(Clone, Debug, PartialEq)]
pub struct CheckPermissionsConfig {
    pub auth_context: AuthContext,
    pub path: String,
    pub required_permission: Permission,
}

/// Decides a single path. Rule collection and evaluation live in
/// `permission_rules`, so read paths that filter many paths at once share
/// exactly these semantics.
#[derive(Debug, PartialEq)]
pub struct CheckPermissionsOperation {
    rules: PermissionRulesOperation,
    path: String,
    required_permission: Permission,
}

impl CheckPermissionsOperation {
    pub fn new(config: CheckPermissionsConfig) -> Self {
        CheckPermissionsOperation {
            rules: PermissionRulesOperation::new(PermissionRulesConfig {
                auth_context: config.auth_context,
                path: config.path.clone(),
            }),
            path: config.path,
            required_permission: config.required_permission,
        }
    }

    pub fn new_with_txn(config: CheckPermissionsConfig, txn_id: TxnId) -> Self {
        CheckPermissionsOperation {
            rules: PermissionRulesOperation::new_with_txn(
                PermissionRulesConfig {
                    auth_context: config.auth_context,
                    path: config.path.clone(),
                },
                txn_id,
            ),
            path: config.path,
            required_permission: config.required_permission,
        }
    }
}

impl Operation for CheckPermissionsOperation {
    type Output = bool;

    type Error = AuthorizationError;

    fn start(&mut self) -> Effects {
        self.rules.start()
    }

    fn step(&mut self, event: Event) -> Effects {
        self.rules.step(event)
    }

    fn is_complete(&self) -> bool {
        self.rules.is_complete()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        let CheckPermissionsOperation {
            rules,
            path,
            required_permission,
        } = self;
        Ok(rules.finalize()?.allows(&path, &required_permission))
    }

    fn abort(&mut self) -> Effects {
        self.rules.abort()
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};

    use aruna_core::UserId;
    use aruna_core::structs::{Actor, AuthContext, Permission, RealmId};
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    use crate::add_group_role::{AddGroupRoleConfig, AddGroupRoleError, AddGroupRoleOperation};
    use crate::add_user_to_group::{AddUserToGroupInput, AddUserToGroupOperation};
    use crate::add_user_to_realm_role::{AddUserToRealmRolesInput, AddUserToRealmRolesOperation};
    use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
    use crate::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use crate::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};

    #[tokio::test]
    pub async fn public_roles_apply_to_everyone_and_are_read_only() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();

        let context = DriverContext {
            storage_handle,
            blob_handle: None,
            net_handle: Some(net_handle.clone()),
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        };

        let realm_id = RealmId([3u8; 32]);
        let admin_id = UserId::local(Ulid::generate(), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let actor = Actor {
            node_id,
            user_id: admin_id,
            realm_id,
        };

        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: actor.clone(),
                realm_description: "Public role test realm".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            &context,
        )
        .await
        .unwrap();
        drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                actor: actor.clone(),
            }),
            &context,
        )
        .await
        .unwrap();

        let (group, _) = drive(
            CreateGroupOperation::new(CreateGroupConfig {
                actor: actor.clone(),
                display_name: "Public group".to_string(),
                owner_cap: None,
            }),
            &context,
        )
        .await
        .unwrap();
        let group_id = group.group_id;

        // A role assigned to the Everyone principal grants READ on the public path.
        drive(
            AddGroupRoleOperation::new(AddGroupRoleConfig {
                auth_context: AuthContext {
                    user_id: admin_id,
                    realm_id,
                    path_restrictions: None,
                    session: None,
                },
                realm_id,
                actor: actor.clone(),
                group_id,
                role: aruna_core::structs::Role {
                    role_id: Ulid::generate(),
                    name: "public-read".to_string(),
                    permissions: HashMap::from([(
                        format!("/{realm_id}/g/{group_id}/data/**"),
                        Permission::READ,
                    )]),
                    assigned_users: HashSet::from([UserId::nil(realm_id)]),
                },
            }),
            &context,
        )
        .await
        .unwrap();

        let data_path = format!("/{realm_id}/g/{group_id}/data/node/bucket/key");
        let check = |auth_context: AuthContext, path: String, permission: Permission| {
            let context = &context;
            async move {
                drive(
                    CheckPermissionsOperation::new(CheckPermissionsConfig {
                        auth_context,
                        path,
                        required_permission: permission,
                    }),
                    context,
                )
                .await
                .unwrap()
            }
        };

        // Anonymous requests (the Everyone principal itself) may read…
        let anonymous = AuthContext::anonymous(realm_id);
        assert!(check(anonymous.clone(), data_path.clone(), Permission::READ).await);
        // …but never write, and never outside the granted path.
        assert!(!check(anonymous.clone(), data_path.clone(), Permission::WRITE).await);
        assert!(
            !check(
                anonymous.clone(),
                format!("/{realm_id}/g/{group_id}/meta/doc"),
                Permission::READ
            )
            .await
        );

        // Authenticated strangers inherit public grants — signed access is
        // never weaker than unsigned access.
        let stranger = AuthContext {
            user_id: UserId::local(Ulid::generate(), realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        };
        assert!(check(stranger.clone(), data_path.clone(), Permission::READ).await);

        for (name, permission) in [
            ("public-write", Permission::WRITE),
            ("public-deny", Permission::DENY),
        ] {
            let result = drive(
                AddGroupRoleOperation::new(AddGroupRoleConfig {
                    auth_context: AuthContext {
                        user_id: admin_id,
                        realm_id,
                        path_restrictions: None,
                        session: None,
                    },
                    realm_id,
                    actor: actor.clone(),
                    group_id,
                    role: aruna_core::structs::Role {
                        role_id: Ulid::generate(),
                        name: name.to_string(),
                        permissions: HashMap::from([(
                            format!("/{realm_id}/g/{group_id}/data/**"),
                            permission,
                        )]),
                        assigned_users: HashSet::from([UserId::nil(realm_id)]),
                    },
                }),
                &context,
            )
            .await;
            assert!(matches!(result, Err(AddGroupRoleError::InvalidPublicRole)));
        }

        net_handle.shutdown().await;
    }

    #[tokio::test]
    pub async fn test_check_permissions() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let task_handle = TaskHandle::new();

        let context = DriverContext {
            storage_handle,
            blob_handle: None,
            net_handle: Some(net_handle.clone()),
            metadata_handle: None,
            task_handle: Some(task_handle),
            compute_handle: None,
        };

        let realm_id = RealmId([0u8; 32]);
        let admin_id = UserId::local(Ulid::generate(), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();

        let realm_config = CreateRealmConfig {
            actor: aruna_core::structs::Actor {
                node_id,
                user_id: admin_id,
                realm_id,
            },
            realm_description: "A description".to_string(),
            oidc_providers: Vec::new(),
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        };

        let realm_operation = CreateRealmOperation::new(realm_config.clone());
        let (_result, realm_auth_doc) = drive(realm_operation, &context).await.unwrap();
        drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                actor: realm_config.actor.clone(),
            }),
            &context,
        )
        .await
        .unwrap();

        let user_id = UserId::local(Ulid::generate(), realm_id);

        let group_config = CreateGroupConfig {
            actor: aruna_core::structs::Actor {
                node_id,
                user_id,
                realm_id,
            },
            display_name: "Test group".to_string(),
            owner_cap: None,
        };

        let group_operation = CreateGroupOperation::new(group_config.clone());
        let (group, group_auth_doc) = drive(group_operation, &context).await.unwrap();
        let group_id = group.group_id;

        //
        // User is in group and has permissions
        //
        let perm_config = CheckPermissionsConfig {
            auth_context: aruna_core::structs::AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            path: format!(
                "/{}/g/{}/meta/{}",
                realm_id,
                group_id.to_string(),
                Ulid::generate().to_string()
            ),
            required_permission: Permission::WRITE,
        };
        let perm_operation = CheckPermissionsOperation::new(perm_config.clone());
        let check_result = drive(perm_operation, &context).await.unwrap();
        assert!(check_result);

        //
        // User is not in group and has no permissions
        //
        let perm_config = CheckPermissionsConfig {
            auth_context: aruna_core::structs::AuthContext {
                user_id: UserId::local(Ulid::generate(), realm_id),
                realm_id,
                path_restrictions: None,
                session: None,
            },
            path: format!(
                "/{}/g/{}/data/{}",
                realm_id,
                group_id.to_string(),
                Ulid::generate().to_string()
            ),
            required_permission: Permission::WRITE,
        };
        let perm_operation = CheckPermissionsOperation::new(perm_config.clone());
        let check_result = drive(perm_operation, &context).await.unwrap();
        assert!(!check_result);

        //
        // Group does not exist
        //
        let perm_config = CheckPermissionsConfig {
            auth_context: aruna_core::structs::AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            path: format!(
                "/{}/g/{}/data/{}",
                realm_id,
                Ulid::generate(),
                Ulid::generate().to_string()
            ),
            required_permission: Permission::WRITE,
        };
        let perm_operation = CheckPermissionsOperation::new(perm_config.clone());
        assert!(drive(perm_operation, &context).await.is_err());

        //
        // User is in group and has not sufficient permissions
        //
        let reader = UserId::local(Ulid::generate(), realm_id);
        let add_user_input = AddUserToGroupInput {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            group_id,
            user_id: reader,
            role_ids: group_auth_doc
                .roles
                .iter()
                .filter_map(|(k, v)| if v.name == "viewer" { Some(*k) } else { None })
                .collect(),
        };

        let add_user_operation = AddUserToGroupOperation::new(add_user_input.clone());
        let _auth_doc = drive(add_user_operation, &context).await.unwrap();

        let mut perm_config = CheckPermissionsConfig {
            auth_context: aruna_core::structs::AuthContext {
                user_id: reader,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            path: format!(
                "/{}/g/{}/meta/{}",
                realm_id,
                group_id.to_string(),
                Ulid::generate().to_string()
            ),
            required_permission: Permission::WRITE,
        };
        let perm_operation = CheckPermissionsOperation::new(perm_config.clone());
        assert!(!drive(perm_operation, &context).await.unwrap());

        //
        // User is in group and has viewer role
        //
        perm_config.required_permission = Permission::READ;
        let perm_operation = CheckPermissionsOperation::new(perm_config.clone());
        assert!(drive(perm_operation, &context).await.unwrap());

        //
        // Test DENY roles
        //
        let denied_user = UserId::local(Ulid::generate(), realm_id);
        let add_role_input = AddGroupRoleConfig {
            auth_context: aruna_core::structs::AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            realm_id,
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            group_id,
            role: aruna_core::structs::Role {
                role_id: Ulid::generate(),
                name: "denied".to_string(),
                permissions: HashMap::from([(
                    format!("/{}/g/{}/**", realm_id, group_id),
                    Permission::DENY,
                )]),
                assigned_users: HashSet::from([denied_user]),
            },
        };

        let add_role_operation = AddGroupRoleOperation::new(add_role_input.clone());
        let _result = drive(add_role_operation, &context).await.unwrap();

        let perm_config = CheckPermissionsConfig {
            auth_context: aruna_core::structs::AuthContext {
                user_id: denied_user,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            path: format!(
                "/{}/g/{}/meta/{}",
                realm_id,
                group_id.to_string(),
                Ulid::generate().to_string()
            ),
            required_permission: Permission::READ,
        };
        let perm_operation = CheckPermissionsOperation::new(perm_config.clone());
        assert!(!drive(perm_operation, &context).await.unwrap());

        //
        // User tries realm operation without realm role
        //
        let perm_config = CheckPermissionsConfig {
            auth_context: aruna_core::structs::AuthContext {
                user_id: denied_user,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            path: format!("/{}/admin/roles/{}", realm_id, Ulid::generate().to_string()),
            required_permission: Permission::READ,
        };
        let perm_operation = CheckPermissionsOperation::new(perm_config.clone());
        assert!(!drive(perm_operation, &context).await.unwrap());

        //
        // Admin tries realm operations
        //
        let perm_config = CheckPermissionsConfig {
            auth_context: aruna_core::structs::AuthContext {
                user_id: admin_id,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            path: format!("/{}/admin/roles/{}", realm_id, Ulid::generate().to_string()),
            required_permission: Permission::WRITE,
        };
        let perm_operation = CheckPermissionsOperation::new(perm_config.clone());
        assert!(drive(perm_operation, &context).await.unwrap());

        //
        // User tries realm operation and has role
        //
        let admin_role = realm_auth_doc
            .roles
            .iter()
            .filter_map(|(id, r)| {
                if r.name == "realm_admin" {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();
        let new_admin = UserId::local(Ulid::generate(), realm_id);

        let add_user_input = AddUserToRealmRolesInput {
            actor: Actor {
                node_id,
                user_id: admin_id,
                realm_id,
            },
            realm_id,
            user_id: new_admin,
            role_ids: admin_role,
        };

        let add_user_operation = AddUserToRealmRolesOperation::new(add_user_input.clone());
        let _auth_doc = drive(add_user_operation, &context).await.unwrap();

        let perm_config = CheckPermissionsConfig {
            auth_context: aruna_core::structs::AuthContext {
                user_id: new_admin,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            path: format!("/{}/admin/roles/{}", realm_id, Ulid::generate().to_string()),
            required_permission: Permission::WRITE,
        };
        let perm_operation = CheckPermissionsOperation::new(perm_config.clone());
        assert!(drive(perm_operation, &context).await.unwrap());

        net_handle.shutdown().await;
    }
}
