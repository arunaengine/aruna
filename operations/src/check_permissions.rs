use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::AuthorizationError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{
    AuthContext, GroupAuthorizationDocument, Permission, RealmAuthorizationDocument, RealmId, Role,
};
use aruna_core::types::{Effects, GroupId, TxnId};
use globset::Glob;
use smallvec::smallvec;
use ulid::Ulid;

#[derive(Clone, Debug, PartialEq)]
pub struct CheckPermissionsConfig {
    pub auth_context: AuthContext,
    pub path: String,
    pub required_permission: Permission,
}

#[derive(Debug, PartialEq)]
pub struct CheckPermissionsOperation {
    config: CheckPermissionsConfig,
    txn_id: Option<TxnId>,
    group_id: Option<Ulid>,
    realm_auth_doc: Option<RealmAuthorizationDocument>,
    group_auth_doc: Option<GroupAuthorizationDocument>,
    output: Option<Result<bool, AuthorizationError>>,
    state: CheckPermissionsState,
}

struct CollectedRole {
    role: Role,
    direct: bool,
    public: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CheckPermissionsState {
    Init,
    StartTransaction,
    GetRealmAuthDoc,
    GetGroupAuthDoc,
    CheckPermissions,
    Finish,
    Error,
}

impl std::fmt::Display for CheckPermissionsState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                CheckPermissionsState::Init => "CheckPermissionsState::Init",
                CheckPermissionsState::StartTransaction =>
                    "CheckPermissionsState::StartTransaction",
                CheckPermissionsState::GetRealmAuthDoc => "CheckPermissionsState::GetRealmAuthDoc",
                CheckPermissionsState::GetGroupAuthDoc => "CheckPermissionsState::GetGroupAuthDoc",
                CheckPermissionsState::CheckPermissions =>
                    "CheckPermissionsState::CheckPermissions",
                CheckPermissionsState::Finish => "CheckPermissionsState::Finish",
                CheckPermissionsState::Error => "CheckPermissionsState::Error",
            }
        )
    }
}

impl CheckPermissionsOperation {
    pub fn new(config: CheckPermissionsConfig) -> Self {
        CheckPermissionsOperation {
            config,
            txn_id: None,
            realm_auth_doc: None,
            group_id: None,
            group_auth_doc: None,
            output: None,
            state: CheckPermissionsState::Init,
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        if let (
            CheckPermissionsState::StartTransaction,
            Event::Storage(StorageEvent::TransactionStarted { txn_id }),
        ) = (self.state, event)
        {
            self.txn_id = Some(txn_id);
            let effects = self.get_realm_auth_doc();
            match effects {
                Ok(effects) => effects,
                Err(err) => self.fail(err),
            }
        } else {
            self.unexpected_event(
                self.state,
                "Event::Storage(StorageEvent::TransactionStart)",
                got,
            )
        }
    }

    fn handle_realm_auth(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        if let (
            CheckPermissionsState::GetRealmAuthDoc,
            Event::Storage(StorageEvent::ReadResult { value, .. }),
        ) = (self.state, event)
        {
            match self.emit_realm_auth_doc(value) {
                Ok(effects) => effects,
                Err(err) => self.fail(err),
            }
        } else {
            self.unexpected_event(self.state, "Event::Storage(StorageEvent::ReadResult)", got)
        }
    }

    fn handle_group_auth(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        if let (
            CheckPermissionsState::GetGroupAuthDoc,
            Event::Storage(StorageEvent::ReadResult { value, .. }),
        ) = (self.state, event)
        {
            match self.emit_group_auth_doc(value) {
                Ok(effects) => effects,
                Err(err) => self.fail(err),
            }
        } else {
            self.unexpected_event(self.state, "Event::Storage(StorageEvent::ReadResult)", got)
        }
    }

    fn handle_check_permissions(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        if let (
            CheckPermissionsState::CheckPermissions,
            Event::Storage(StorageEvent::TransactionCommitted { .. }),
        ) = (self.state, event)
        {
            match self.emit_check_permissions() {
                Ok(effects) => effects,
                Err(err) => self.fail(err),
            }
        } else {
            self.unexpected_event(
                self.state,
                "Event::Storage(StorageEvent::TransactionCommitted)",
                got,
            )
        }
    }

    fn parse_path(path: &str) -> Result<(RealmId, Option<GroupId>), AuthorizationError> {
        let mut levels = path.split("/");
        levels.next();
        let realm = levels
            .next()
            .and_then(|rid| RealmId::from_base64(rid).ok())
            .ok_or_else(|| AuthorizationError::InvalidRealmId)?;

        let separator = levels.next();

        let group = if separator == Some("g") {
            levels.next().and_then(|g| Ulid::from_string(g).ok())
        } else {
            None
        };

        Ok((realm, group))
    }

    fn get_realm_auth_doc(&mut self) -> Result<Effects, AuthorizationError> {
        self.state = CheckPermissionsState::GetRealmAuthDoc;
        let (realm, group) = CheckPermissionsOperation::parse_path(&self.config.path)?;
        self.group_id = group;
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: AUTH_KEYSPACE.to_string(),
            key: (*realm.as_bytes()).into(),
            txn_id: self.txn_id
        })])
    }

    fn emit_realm_auth_doc(
        &mut self,
        value: Option<byteview::ByteView>,
    ) -> Result<Effects, AuthorizationError> {
        self.realm_auth_doc = Some(RealmAuthorizationDocument::from_bytes(
            &value.ok_or_else(|| AuthorizationError::AuthDocNotFound)?,
        )?);

        match self.group_id {
            Some(group) => {
                self.state = CheckPermissionsState::GetGroupAuthDoc;
                Ok(smallvec![Effect::Storage(StorageEffect::Read {
                    txn_id: self.txn_id,
                    key_space: AUTH_KEYSPACE.to_string(),
                    key: group.to_bytes().into(),
                })])
            }
            None => {
                self.state = CheckPermissionsState::CheckPermissions;
                Ok(smallvec![Effect::Storage(
                    StorageEffect::CommitTransaction {
                        txn_id: self
                            .txn_id
                            .ok_or_else(|| AuthorizationError::NoTransactionFound)?
                    }
                )])
            }
        }
    }

    fn emit_group_auth_doc(
        &mut self,
        value: Option<byteview::ByteView>,
    ) -> Result<Effects, AuthorizationError> {
        self.state = CheckPermissionsState::CheckPermissions;
        self.group_auth_doc = Some(GroupAuthorizationDocument::from_bytes(
            &value.ok_or_else(|| AuthorizationError::AuthDocNotFound)?,
        )?);

        Ok(smallvec![Effect::Storage(
            StorageEffect::CommitTransaction {
                txn_id: self
                    .txn_id
                    .ok_or_else(|| AuthorizationError::NoTransactionFound)?
            }
        )])
    }

    fn emit_check_permissions(&mut self) -> Result<Effects, AuthorizationError> {
        self.state = CheckPermissionsState::Finish;
        let roles = self.collect_roles()?;
        let allowed = self.check_permissions(roles)?;
        let allowed = if allowed {
            self.check_path_restrictions()?
        } else {
            false
        };
        self.output = Some(Ok(allowed));
        Ok(smallvec![])
    }

    fn collect_roles(&mut self) -> Result<Vec<CollectedRole>, AuthorizationError> {
        let realm_auth_doc = self
            .realm_auth_doc
            .as_ref()
            .ok_or_else(|| AuthorizationError::AuthDocNotFound)?;
        let realm_id = realm_auth_doc.realm_id;
        let auth_user = self.config.auth_context.user_id;
        let mut roles = realm_auth_doc.roles.clone();
        if let Some(group) = &self.group_auth_doc {
            roles.extend(group.roles.clone());
        }
        Ok(roles
            .into_values()
            .filter_map(|role| {
                // Public roles apply by assigning this realm's exact Everyone
                // principal. Other nil user ids are not public for this realm.
                let public = role.is_public(realm_id);
                let direct = !auth_user.is_nil() && role.assigned_users.contains(&auth_user);
                (public || direct).then_some(CollectedRole {
                    role,
                    direct,
                    public,
                })
            })
            .collect())
    }

    fn check_permissions(&mut self, roles: Vec<CollectedRole>) -> Result<bool, AuthorizationError> {
        let mut allowed = false;
        for CollectedRole {
            role,
            direct,
            public,
        } in roles
        {
            for (path, permission) in role.permissions {
                let glob = Glob::new(&path)?.compile_matcher();
                if glob.is_match(&self.config.path) {
                    if public
                        && permission == Permission::READ
                        && self.config.required_permission == Permission::READ
                    {
                        allowed = true;
                    }

                    if direct {
                        match permission {
                            Permission::DENY => {
                                return Ok(false);
                            }
                            Permission::READ => {
                                if self.config.required_permission == Permission::READ {
                                    allowed = true;
                                }
                            }
                            Permission::WRITE => allowed = true,
                        }
                    }
                }
            }
        }
        Ok(allowed)
    }

    fn check_path_restrictions(&self) -> Result<bool, AuthorizationError> {
        let Some(restrictions) = self.config.auth_context.path_restrictions.as_ref() else {
            return Ok(true);
        };

        let mut allowed = false;
        for restriction in restrictions {
            let glob = Glob::new(&restriction.pattern)?.compile_matcher();
            if glob.is_match(&self.config.path) {
                match restriction.permission {
                    Permission::DENY => return Ok(false),
                    Permission::READ => {
                        if self.config.required_permission == Permission::READ {
                            allowed = true;
                        }
                    }
                    Permission::WRITE => allowed = true,
                }
            }
        }

        Ok(allowed)
    }

    fn fail(&mut self, err: AuthorizationError) -> Effects {
        self.state = CheckPermissionsState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn fail_with_cleanup(&mut self, err: AuthorizationError, cleanup_effects: Effects) -> Effects {
        self.state = CheckPermissionsState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: CheckPermissionsState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            AuthorizationError::UnexpectedEvent {
                state: state.to_string(),
                expected,
                got,
            },
            cleanup_effects,
        )
    }

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }

        Ok(event)
    }
}

impl Operation for CheckPermissionsOperation {
    type Output = bool;

    type Error = AuthorizationError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = CheckPermissionsState::StartTransaction;

        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state {
            CheckPermissionsState::StartTransaction => self.handle_start_transaction(event),
            CheckPermissionsState::GetRealmAuthDoc => self.handle_realm_auth(event),
            CheckPermissionsState::GetGroupAuthDoc => self.handle_group_auth(event),
            CheckPermissionsState::CheckPermissions => self.handle_check_permissions(event),
            CheckPermissionsState::Finish
            | CheckPermissionsState::Init
            | CheckPermissionsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CheckPermissionsState::Finish | CheckPermissionsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or_else(|| AuthorizationError::NotFinished)?
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        match self.txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};

    use aruna_core::UserId;
    use aruna_core::structs::{
        Actor, AuthContext, PathRestriction, Permission, RealmAuthorizationDocument, RealmId, Role,
    };
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use ed25519_dalek::SigningKey;
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
    pub async fn test_path_parsing() {
        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let realm_signing_key: SigningKey = SigningKey::generate(&mut csprng);
        let pubkey = realm_signing_key.verifying_key().to_bytes();
        let realm_id = RealmId::from_bytes(pubkey);
        let group_id = Ulid::new();
        let path = format!("/{}/g/{}", realm_id, group_id.to_string());
        let (parsed_realm_id, parsed_group_id) =
            CheckPermissionsOperation::parse_path(&path).unwrap();

        assert_eq!(realm_id, parsed_realm_id);
        assert_eq!(group_id, parsed_group_id.unwrap());

        let path = format!("/{}/admin", realm_id);
        let (parsed_realm_id, parsed_group_id) =
            CheckPermissionsOperation::parse_path(&path).unwrap();

        assert_eq!(realm_id, parsed_realm_id);
        assert!(parsed_group_id.is_none());

        let path = format!("/abcd/g/{}", Ulid::new().to_string());
        assert!(CheckPermissionsOperation::parse_path(&path).is_err());
    }

    #[test]
    fn path_restrictions_enforce_whitelist_and_required_permission() {
        let realm_id = RealmId([0u8; 32]);
        let group_id = Ulid::new();
        let pattern = format!("/{realm_id}/g/{group_id}/meta/**");
        let mut operation = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: AuthContext {
                user_id: UserId::local(Ulid::new(), realm_id),
                realm_id,
                path_restrictions: Some(vec![PathRestriction {
                    pattern: pattern.clone(),
                    permission: Permission::READ,
                }]),
            },
            path: format!("/{realm_id}/g/{group_id}/meta/document"),
            required_permission: Permission::READ,
        });

        assert!(operation.check_path_restrictions().unwrap());

        operation.config.path = format!("/{realm_id}/g/{group_id}/data/document");
        assert!(!operation.check_path_restrictions().unwrap());

        operation.config.path = format!("/{realm_id}/g/{group_id}/meta/document");
        operation.config.required_permission = Permission::WRITE;
        assert!(!operation.check_path_restrictions().unwrap());

        operation.config.required_permission = Permission::READ;
        operation.config.auth_context.path_restrictions = Some(vec![PathRestriction {
            pattern,
            permission: Permission::DENY,
        }]);
        assert!(!operation.check_path_restrictions().unwrap());
    }

    #[test]
    fn collect_roles_only_treats_same_realm_nil_as_public() {
        let realm_id = RealmId([1u8; 32]);
        let other_realm_id = RealmId([2u8; 32]);
        let group_id = Ulid::new();
        let role = Role {
            role_id: Ulid::new(),
            name: "foreign-nil".to_string(),
            permissions: HashMap::from([(
                format!("/{realm_id}/g/{group_id}/data/**"),
                Permission::READ,
            )]),
            assigned_users: HashSet::from([UserId::nil(other_realm_id)]),
        };
        let mut operation = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: AuthContext::anonymous(realm_id),
            path: format!("/{realm_id}/g/{group_id}/data/object"),
            required_permission: Permission::READ,
        });
        operation.realm_auth_doc = Some(RealmAuthorizationDocument {
            realm_id,
            roles: HashMap::from([(role.role_id, role)]),
            operation_restrictions: HashMap::new(),
        });

        assert!(operation.collect_roles().unwrap().is_empty());
    }

    #[test]
    fn public_grants_are_read_only_when_evaluating_permissions() {
        let realm_id = RealmId([2u8; 32]);
        let group_id = Ulid::new();
        let user_id = UserId::local(Ulid::new(), realm_id);
        let path = format!("/{realm_id}/g/{group_id}/data/object");

        let public_write = Role {
            role_id: Ulid::new(),
            name: "public-write".to_string(),
            permissions: HashMap::from([(path.clone(), Permission::WRITE)]),
            assigned_users: HashSet::from([UserId::nil(realm_id)]),
        };
        let mut write_operation = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: AuthContext::anonymous(realm_id),
            path: path.clone(),
            required_permission: Permission::WRITE,
        });
        assert!(
            !write_operation
                .check_permissions(vec![super::CollectedRole {
                    role: public_write,
                    direct: false,
                    public: true,
                }])
                .unwrap()
        );

        let public_direct_write = Role {
            role_id: Ulid::new(),
            name: "public-direct-write".to_string(),
            permissions: HashMap::from([(path.clone(), Permission::WRITE)]),
            assigned_users: HashSet::from([UserId::nil(realm_id), user_id]),
        };
        assert!(
            write_operation
                .check_permissions(vec![super::CollectedRole {
                    role: public_direct_write,
                    direct: true,
                    public: true,
                }])
                .unwrap()
        );

        let public_deny = Role {
            role_id: Ulid::new(),
            name: "public-deny".to_string(),
            permissions: HashMap::from([(path.clone(), Permission::DENY)]),
            assigned_users: HashSet::from([UserId::nil(realm_id)]),
        };
        let direct_read = Role {
            role_id: Ulid::new(),
            name: "direct-read".to_string(),
            permissions: HashMap::from([(path.clone(), Permission::READ)]),
            assigned_users: HashSet::from([user_id]),
        };
        let public_direct_deny = Role {
            role_id: Ulid::new(),
            name: "public-direct-deny".to_string(),
            permissions: HashMap::from([(path.clone(), Permission::DENY)]),
            assigned_users: HashSet::from([UserId::nil(realm_id), user_id]),
        };
        let mut read_operation = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
            },
            path,
            required_permission: Permission::READ,
        });
        assert!(
            read_operation
                .check_permissions(vec![
                    super::CollectedRole {
                        role: public_deny,
                        direct: false,
                        public: true,
                    },
                    super::CollectedRole {
                        role: direct_read.clone(),
                        direct: true,
                        public: false,
                    },
                ])
                .unwrap()
        );
        assert!(
            !read_operation
                .check_permissions(vec![
                    super::CollectedRole {
                        role: public_direct_deny,
                        direct: true,
                        public: true,
                    },
                    super::CollectedRole {
                        role: direct_read,
                        direct: true,
                        public: false,
                    },
                ])
                .unwrap()
        );
    }

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
        };

        let realm_id = RealmId([3u8; 32]);
        let admin_id = UserId::local(Ulid::new(), realm_id);
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
                },
                realm_id,
                actor: actor.clone(),
                group_id,
                role: aruna_core::structs::Role {
                    role_id: Ulid::new(),
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
            user_id: UserId::local(Ulid::new(), realm_id),
            realm_id,
            path_restrictions: None,
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
                    },
                    realm_id,
                    actor: actor.clone(),
                    group_id,
                    role: aruna_core::structs::Role {
                        role_id: Ulid::new(),
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
        };

        let realm_id = RealmId([0u8; 32]);
        let admin_id = UserId::local(Ulid::new(), realm_id);
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

        let user_id = UserId::local(Ulid::new(), realm_id);

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
            },
            path: format!(
                "/{}/g/{}/meta/{}",
                realm_id,
                group_id.to_string(),
                Ulid::new().to_string()
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
                user_id: UserId::local(Ulid::new(), realm_id),
                realm_id,
                path_restrictions: None,
            },
            path: format!(
                "/{}/g/{}/data/{}",
                realm_id,
                group_id.to_string(),
                Ulid::new().to_string()
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
            },
            path: format!(
                "/{}/g/{}/data/{}",
                realm_id,
                Ulid::new(),
                Ulid::new().to_string()
            ),
            required_permission: Permission::WRITE,
        };
        let perm_operation = CheckPermissionsOperation::new(perm_config.clone());
        assert!(drive(perm_operation, &context).await.is_err());

        //
        // User is in group and has not sufficient permissions
        //
        let reader = UserId::local(Ulid::new(), realm_id);
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
            },
            path: format!(
                "/{}/g/{}/meta/{}",
                realm_id,
                group_id.to_string(),
                Ulid::new().to_string()
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
        let denied_user = UserId::local(Ulid::new(), realm_id);
        let add_role_input = AddGroupRoleConfig {
            auth_context: aruna_core::structs::AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
            },
            realm_id,
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            group_id,
            role: aruna_core::structs::Role {
                role_id: Ulid::new(),
                name: "denied".to_string(),
                permissions: HashMap::from([(
                    format!("{}/g/{}/**", realm_id, group_id),
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
            },
            path: format!(
                "/{}/g/{}/meta/{}",
                realm_id,
                group_id.to_string(),
                Ulid::new().to_string()
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
            },
            path: format!("/{}/admin/roles/{}", realm_id, Ulid::new().to_string()),
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
            },
            path: format!("/{}/admin/roles/{}", realm_id, Ulid::new().to_string()),
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
        let new_admin = UserId::local(Ulid::new(), realm_id);

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
            },
            path: format!("/{}/admin/roles/{}", realm_id, Ulid::new().to_string()),
            required_permission: Permission::WRITE,
        };
        let perm_operation = CheckPermissionsOperation::new(perm_config.clone());
        assert!(drive(perm_operation, &context).await.unwrap());

        net_handle.shutdown().await;
    }
}
