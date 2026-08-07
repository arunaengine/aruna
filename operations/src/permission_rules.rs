use std::collections::{BTreeSet, HashMap};

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::AuthorizationError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::permission_path::{compile_permission_matcher, validate_restriction_limits};
use aruna_core::structs::{
    AuthContext, GroupAuthorizationDocument, MetadataRegistryRecord, PathRestriction, Permission,
    RealmAuthorizationDocument, RealmId, Role,
};
use aruna_core::types::{Effects, GroupId, TxnId};
use globset::GlobMatcher;
use smallvec::smallvec;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};

/// A role that applies to the caller: `direct` when the caller is assigned to
/// it, `public` when the realm's Everyone principal is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectedRole {
    pub role: Role,
    pub direct: bool,
    pub public: bool,
}

#[derive(Clone, Debug)]
struct CompiledRule {
    matcher: GlobMatcher,
    permission: Permission,
    direct: bool,
    public: bool,
}

impl PartialEq for CompiledRule {
    fn eq(&self, other: &Self) -> bool {
        self.matcher.glob() == other.matcher.glob()
            && self.permission == other.permission
            && self.direct == other.direct
            && self.public == other.public
    }
}

#[derive(Clone, Debug)]
struct CompiledRestriction {
    matcher: GlobMatcher,
    permission: Permission,
}

impl PartialEq for CompiledRestriction {
    fn eq(&self, other: &Self) -> bool {
        self.matcher.glob() == other.matcher.glob() && self.permission == other.permission
    }
}

/// The caller's effective permission rules for one realm or group scope.
/// `allows` is the single authorization decision used by every read path, so a
/// caller holding many paths can decide in memory after one collection.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PermissionRules {
    rules: Vec<CompiledRule>,
    restrictions: Option<Vec<CompiledRestriction>>,
}

impl PermissionRules {
    /// Compiles the role patterns once. An unusable pattern fails the whole
    /// collection so callers deny instead of silently dropping a rule.
    pub fn from_roles(
        roles: Vec<CollectedRole>,
        restrictions: Option<&[PathRestriction]>,
    ) -> Result<Self, AuthorizationError> {
        let mut rules = Vec::new();
        for CollectedRole {
            role,
            direct,
            public,
        } in roles
        {
            for (pattern, permission) in role.permissions {
                rules.push(CompiledRule {
                    matcher: compile_permission_matcher(&pattern)?,
                    permission,
                    direct,
                    public,
                });
            }
        }

        let restrictions = restrictions
            .map(|restrictions| {
                // Defense in depth: an over-limit restriction set fails the
                // collection (deny), mirroring the issuance-time rejection.
                validate_restriction_limits(restrictions)?;
                restrictions
                    .iter()
                    .map(|restriction| {
                        Ok(CompiledRestriction {
                            matcher: compile_permission_matcher(&restriction.pattern)?,
                            permission: restriction.permission.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>, AuthorizationError>>()
            })
            .transpose()?;

        Ok(Self {
            rules,
            restrictions,
        })
    }

    /// A matching direct DENY denies outright, a public role only ever grants
    /// READ, and token path restrictions act as a whitelist on top.
    pub fn allows(&self, path: &str, required: &Permission) -> bool {
        let mut allowed = false;
        for rule in &self.rules {
            if !rule.matcher.is_match(path) {
                continue;
            }
            if rule.public && rule.permission == Permission::READ && *required == Permission::READ {
                allowed = true;
            }
            if rule.direct {
                match rule.permission {
                    Permission::DENY => return false,
                    Permission::READ => {
                        if *required == Permission::READ {
                            allowed = true;
                        }
                    }
                    Permission::WRITE => allowed = true,
                }
            }
        }

        allowed && self.restrictions_allow(path, required)
    }

    fn restrictions_allow(&self, path: &str, required: &Permission) -> bool {
        let Some(restrictions) = self.restrictions.as_ref() else {
            return true;
        };

        let mut allowed = false;
        for restriction in restrictions {
            if !restriction.matcher.is_match(path) {
                continue;
            }
            match restriction.permission {
                Permission::DENY => return false,
                Permission::READ => {
                    if *required == Permission::READ {
                        allowed = true;
                    }
                }
                Permission::WRITE => allowed = true,
            }
        }
        allowed
    }
}

/// The caller's rules for a set of groups, collected once per request: every
/// candidate path is then decided in memory instead of driving one permission
/// check per path.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GroupPermissionRules {
    realm_id: Option<RealmId>,
    groups: HashMap<GroupId, PermissionRules>,
}

impl GroupPermissionRules {
    /// One rules drive per distinct group. Anonymous callers hold no rules, and
    /// a group whose collection fails keeps none, so its records stay hidden.
    pub async fn collect(
        context: &DriverContext,
        auth_context: Option<&AuthContext>,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Self {
        let Some(auth_context) = auth_context else {
            return Self::default();
        };

        let mut groups = HashMap::new();
        for group_id in group_ids.into_iter().collect::<BTreeSet<_>>() {
            let Ok(rules) = drive(
                PermissionRulesOperation::new(PermissionRulesConfig {
                    auth_context: auth_context.clone(),
                    path: format!("/{}/g/{group_id}", auth_context.realm_id),
                }),
                context,
            )
            .await
            else {
                continue;
            };
            groups.insert(group_id, rules);
        }

        Self {
            realm_id: Some(auth_context.realm_id),
            groups,
        }
    }

    pub fn from_groups(
        realm_id: Option<RealmId>,
        groups: HashMap<GroupId, PermissionRules>,
    ) -> Self {
        Self { realm_id, groups }
    }

    /// Mirrors `can_read_record`: public records need no authorization, every
    /// other record is decided on its own permission path. Records outside the
    /// caller's realm stay hidden, which is stricter than a cross-realm check.
    pub fn record_visible(&self, record: &MetadataRegistryRecord) -> bool {
        record.public
            || (self.realm_id == Some(record.realm_id)
                && self.allows(record.group_id, &record.permission_path, &Permission::READ))
    }

    pub fn allows(&self, group_id: GroupId, path: &str, required: &Permission) -> bool {
        self.groups
            .get(&group_id)
            .is_some_and(|rules| rules.allows(path, required))
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PermissionRulesConfig {
    pub auth_context: AuthContext,
    /// Selects the authorization documents to read: only the realm and the
    /// optional group segment matter, the remaining path is ignored.
    pub path: String,
}

/// Reads the realm and group authorization documents once and returns the
/// caller's rules, so bulk read paths pay O(scopes) storage reads instead of
/// one permission check per candidate path.
#[derive(Debug, PartialEq)]
pub struct PermissionRulesOperation {
    config: PermissionRulesConfig,
    txn_id: Option<TxnId>,
    external_txn: bool,
    group_id: Option<GroupId>,
    realm_auth_doc: Option<RealmAuthorizationDocument>,
    group_auth_doc: Option<GroupAuthorizationDocument>,
    output: Option<Result<PermissionRules, AuthorizationError>>,
    state: PermissionRulesState,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum PermissionRulesState {
    Init,
    StartTransaction,
    GetRealmAuthDoc,
    GetGroupAuthDoc,
    CollectRules,
    Finish,
    Error,
}

impl std::fmt::Display for PermissionRulesState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                PermissionRulesState::Init => "PermissionRulesState::Init",
                PermissionRulesState::StartTransaction => "PermissionRulesState::StartTransaction",
                PermissionRulesState::GetRealmAuthDoc => "PermissionRulesState::GetRealmAuthDoc",
                PermissionRulesState::GetGroupAuthDoc => "PermissionRulesState::GetGroupAuthDoc",
                PermissionRulesState::CollectRules => "PermissionRulesState::CollectRules",
                PermissionRulesState::Finish => "PermissionRulesState::Finish",
                PermissionRulesState::Error => "PermissionRulesState::Error",
            }
        )
    }
}

impl PermissionRulesOperation {
    pub fn new(config: PermissionRulesConfig) -> Self {
        PermissionRulesOperation {
            config,
            txn_id: None,
            external_txn: false,
            group_id: None,
            realm_auth_doc: None,
            group_auth_doc: None,
            output: None,
            state: PermissionRulesState::Init,
        }
    }

    pub fn new_with_txn(config: PermissionRulesConfig, txn_id: TxnId) -> Self {
        PermissionRulesOperation {
            config,
            txn_id: Some(txn_id),
            external_txn: true,
            group_id: None,
            realm_auth_doc: None,
            group_auth_doc: None,
            output: None,
            state: PermissionRulesState::Init,
        }
    }

    fn handle_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        if let (
            PermissionRulesState::StartTransaction,
            Event::Storage(StorageEvent::TransactionStarted { txn_id }),
        ) = (self.state, event)
        {
            self.txn_id = Some(txn_id);
            match self.read_realm_doc() {
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
            PermissionRulesState::GetRealmAuthDoc,
            Event::Storage(StorageEvent::ReadResult { value, .. }),
        ) = (self.state, event)
        {
            match self.store_realm_doc(value) {
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
            PermissionRulesState::GetGroupAuthDoc,
            Event::Storage(StorageEvent::ReadResult { value, .. }),
        ) = (self.state, event)
        {
            match self.store_group_doc(value) {
                Ok(effects) => effects,
                Err(err) => self.fail(err),
            }
        } else {
            self.unexpected_event(self.state, "Event::Storage(StorageEvent::ReadResult)", got)
        }
    }

    fn handle_commit(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        if let (
            PermissionRulesState::CollectRules,
            Event::Storage(StorageEvent::TransactionCommitted { .. }),
        ) = (self.state, event)
        {
            match self.emit_rules() {
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

    fn read_realm_doc(&mut self) -> Result<Effects, AuthorizationError> {
        self.state = PermissionRulesState::GetRealmAuthDoc;
        let (realm, group) = PermissionRulesOperation::parse_path(&self.config.path)?;
        self.group_id = group;
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: AUTH_KEYSPACE.to_string(),
            key: (*realm.as_bytes()).into(),
            txn_id: self.txn_id
        })])
    }

    fn store_realm_doc(
        &mut self,
        value: Option<byteview::ByteView>,
    ) -> Result<Effects, AuthorizationError> {
        self.realm_auth_doc = Some(RealmAuthorizationDocument::from_bytes(
            &value.ok_or_else(|| AuthorizationError::AuthDocNotFound)?,
        )?);

        match self.group_id {
            Some(group) => {
                self.state = PermissionRulesState::GetGroupAuthDoc;
                Ok(smallvec![Effect::Storage(StorageEffect::Read {
                    txn_id: self.txn_id,
                    key_space: AUTH_KEYSPACE.to_string(),
                    key: group.to_bytes().into(),
                })])
            }
            None => {
                if self.external_txn {
                    self.emit_rules()
                } else {
                    self.state = PermissionRulesState::CollectRules;
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
    }

    fn store_group_doc(
        &mut self,
        value: Option<byteview::ByteView>,
    ) -> Result<Effects, AuthorizationError> {
        self.group_auth_doc = Some(GroupAuthorizationDocument::from_bytes(
            &value.ok_or_else(|| AuthorizationError::AuthDocNotFound)?,
        )?);

        if self.external_txn {
            self.emit_rules()
        } else {
            self.state = PermissionRulesState::CollectRules;
            Ok(smallvec![Effect::Storage(
                StorageEffect::CommitTransaction {
                    txn_id: self
                        .txn_id
                        .ok_or_else(|| AuthorizationError::NoTransactionFound)?
                }
            )])
        }
    }

    fn emit_rules(&mut self) -> Result<Effects, AuthorizationError> {
        self.state = PermissionRulesState::Finish;
        let roles = self.collect_roles()?;
        self.output = Some(Ok(PermissionRules::from_roles(
            roles,
            self.config.auth_context.path_restrictions.as_deref(),
        )?));
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

    fn fail(&mut self, err: AuthorizationError) -> Effects {
        self.state = PermissionRulesState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn fail_with_cleanup(&mut self, err: AuthorizationError, cleanup_effects: Effects) -> Effects {
        self.state = PermissionRulesState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: PermissionRulesState,
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

    fn fail_on_storage(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }

        Ok(event)
    }
}

impl Operation for PermissionRulesOperation {
    type Output = PermissionRules;

    type Error = AuthorizationError;

    fn start(&mut self) -> Effects {
        if self.external_txn {
            return match self.read_realm_doc() {
                Ok(effects) => effects,
                Err(err) => self.fail(err),
            };
        }
        self.state = PermissionRulesState::StartTransaction;

        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state {
            PermissionRulesState::StartTransaction => self.handle_transaction(event),
            PermissionRulesState::GetRealmAuthDoc => self.handle_realm_auth(event),
            PermissionRulesState::GetGroupAuthDoc => self.handle_group_auth(event),
            PermissionRulesState::CollectRules => self.handle_commit(event),
            PermissionRulesState::Finish
            | PermissionRulesState::Init
            | PermissionRulesState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            PermissionRulesState::Finish | PermissionRulesState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or_else(|| AuthorizationError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        if self.external_txn {
            return smallvec![];
        }
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
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        AuthContext, GroupAuthorizationDocument, MetadataRegistryRecord, PathRestriction,
        Permission, RealmAuthorizationDocument, RealmId, Role,
    };
    use ulid::Ulid;

    use super::{CollectedRole, PermissionRules, PermissionRulesConfig, PermissionRulesOperation};

    fn role(permissions: HashMap<String, Permission>, assigned: HashSet<UserId>) -> Role {
        Role {
            role_id: Ulid::generate(),
            name: "test".to_string(),
            permissions,
            assigned_users: assigned,
        }
    }

    fn direct_rules(permissions: HashMap<String, Permission>) -> PermissionRules {
        PermissionRules::from_roles(
            vec![CollectedRole {
                role: role(permissions, HashSet::new()),
                direct: true,
                public: false,
            }],
            None,
        )
        .expect("patterns compile")
    }

    #[test]
    fn parses_scope_path() {
        let realm_id = RealmId([4u8; 32]);
        let group_id = Ulid::generate();
        let (parsed_realm, parsed_group) =
            PermissionRulesOperation::parse_path(&format!("/{realm_id}/g/{group_id}"))
                .expect("group path parses");
        assert_eq!(parsed_realm, realm_id);
        assert_eq!(parsed_group, Some(group_id));

        let (parsed_realm, parsed_group) =
            PermissionRulesOperation::parse_path(&format!("/{realm_id}/admin"))
                .expect("realm path parses");
        assert_eq!(parsed_realm, realm_id);
        assert!(parsed_group.is_none());

        assert!(PermissionRulesOperation::parse_path("/abcd/g/nope").is_err());
    }

    #[test]
    fn deny_wins() {
        // A per-document DENY must beat the group-wide READ it overlaps.
        let realm_id = RealmId([5u8; 32]);
        let group_id = Ulid::generate();
        let secret = MetadataRegistryRecord::permission_path_for(
            &realm_id,
            group_id,
            "datasets/secret",
            Ulid::generate(),
        );
        let sibling = MetadataRegistryRecord::permission_path_for(
            &realm_id,
            group_id,
            "datasets/open",
            Ulid::generate(),
        );
        let rules = direct_rules(HashMap::from([
            (
                format!("/{realm_id}/g/{group_id}/meta/**"),
                Permission::READ,
            ),
            (secret.clone(), Permission::DENY),
        ]));

        assert!(!rules.allows(&secret, &Permission::READ));
        assert!(rules.allows(&sibling, &Permission::READ));
    }

    #[test]
    fn narrow_grant_matches() {
        // A grant on one document must not open the rest of the group.
        let realm_id = RealmId([6u8; 32]);
        let group_id = Ulid::generate();
        let granted = MetadataRegistryRecord::permission_path_for(
            &realm_id,
            group_id,
            "datasets/shared",
            Ulid::generate(),
        );
        let other = MetadataRegistryRecord::permission_path_for(
            &realm_id,
            group_id,
            "datasets/private",
            Ulid::generate(),
        );
        let rules = direct_rules(HashMap::from([(granted.clone(), Permission::READ)]));

        assert!(rules.allows(&granted, &Permission::READ));
        assert!(!rules.allows(&other, &Permission::READ));
        assert!(!rules.allows(&granted, &Permission::WRITE));
    }

    #[test]
    fn star_stays_bounded() {
        // A single-segment grant must not silently become recursive: `*` and
        // `?` never cross `/`, only `**` spans segments.
        let realm_id = RealmId([11u8; 32]);
        let group_id = Ulid::generate();
        let rules = direct_rules(HashMap::from([(
            format!("/{realm_id}/g/{group_id}/*"),
            Permission::READ,
        )]));

        assert!(rules.allows(&format!("/{realm_id}/g/{group_id}/data"), &Permission::READ));
        assert!(!rules.allows(
            &format!("/{realm_id}/g/{group_id}/data/node/bucket/key"),
            &Permission::READ
        ));
    }

    #[test]
    fn malformed_fails_collection() {
        // An uncompilable pattern denies the whole collection, never grants.
        let realm_id = RealmId([12u8; 32]);
        let group_id = Ulid::generate();
        assert!(
            PermissionRules::from_roles(
                vec![CollectedRole {
                    role: role(
                        HashMap::from([(format!("/{realm_id}/g/{group_id}/["), Permission::READ)]),
                        HashSet::new(),
                    ),
                    direct: true,
                    public: false,
                }],
                None,
            )
            .is_err()
        );
    }

    #[test]
    fn oversized_restrictions_deny() {
        let realm_id = RealmId([13u8; 32]);
        let group_id = Ulid::generate();
        let pattern = format!("/{realm_id}/g/{group_id}/meta/**");
        let restrictions = vec![
            PathRestriction {
                pattern: pattern.clone(),
                permission: Permission::READ,
            };
            aruna_core::permission_path::MAX_TOKEN_RESTRICTIONS + 1
        ];
        assert!(
            PermissionRules::from_roles(
                vec![CollectedRole {
                    role: role(HashMap::from([(pattern, Permission::READ)]), HashSet::new()),
                    direct: true,
                    public: false,
                }],
                Some(&restrictions),
            )
            .is_err()
        );
    }

    #[test]
    fn restrictions_gate_access() {
        let realm_id = RealmId([7u8; 32]);
        let group_id = Ulid::generate();
        let pattern = format!("/{realm_id}/g/{group_id}/meta/**");
        let path = format!("/{realm_id}/g/{group_id}/meta/document");
        let granted = HashMap::from([(format!("/{realm_id}/g/{group_id}/**"), Permission::WRITE)]);
        let restricted = |permission: Permission| {
            PermissionRules::from_roles(
                vec![CollectedRole {
                    role: role(granted.clone(), HashSet::new()),
                    direct: true,
                    public: false,
                }],
                Some(&[PathRestriction {
                    pattern: pattern.clone(),
                    permission,
                }]),
            )
            .expect("patterns compile")
        };

        let read_only = restricted(Permission::READ);
        assert!(read_only.allows(&path, &Permission::READ));
        assert!(!read_only.allows(&path, &Permission::WRITE));
        assert!(!read_only.allows(
            &format!("/{realm_id}/g/{group_id}/data/x"),
            &Permission::READ
        ));
        assert!(!restricted(Permission::DENY).allows(&path, &Permission::READ));
    }

    #[test]
    fn public_grants_read() {
        // Public roles grant READ to everyone but never WRITE, and a direct
        // DENY still wins over a public grant.
        let realm_id = RealmId([8u8; 32]);
        let group_id = Ulid::generate();
        let path = format!("/{realm_id}/g/{group_id}/data/object");
        let public = |permission: Permission| {
            PermissionRules::from_roles(
                vec![CollectedRole {
                    role: role(
                        HashMap::from([(path.clone(), permission)]),
                        HashSet::from([UserId::nil(realm_id)]),
                    ),
                    direct: false,
                    public: true,
                }],
                None,
            )
            .expect("patterns compile")
        };

        assert!(public(Permission::READ).allows(&path, &Permission::READ));
        assert!(!public(Permission::WRITE).allows(&path, &Permission::WRITE));
        assert!(!public(Permission::READ).allows(&path, &Permission::WRITE));

        let denied = PermissionRules::from_roles(
            vec![
                CollectedRole {
                    role: role(
                        HashMap::from([(path.clone(), Permission::READ)]),
                        HashSet::from([UserId::nil(realm_id)]),
                    ),
                    direct: false,
                    public: true,
                },
                CollectedRole {
                    role: role(
                        HashMap::from([(path.clone(), Permission::DENY)]),
                        HashSet::new(),
                    ),
                    direct: true,
                    public: false,
                },
            ],
            None,
        )
        .expect("patterns compile");
        assert!(!denied.allows(&path, &Permission::READ));
    }

    #[test]
    fn skips_foreign_nil() {
        // Only this realm's Everyone principal makes a role public.
        let realm_id = RealmId([9u8; 32]);
        let other_realm = RealmId([10u8; 32]);
        let group_id = Ulid::generate();
        let mut operation = PermissionRulesOperation::new(PermissionRulesConfig {
            auth_context: AuthContext::anonymous(realm_id),
            path: format!("/{realm_id}/g/{group_id}"),
        });
        let foreign = role(
            HashMap::from([(
                format!("/{realm_id}/g/{group_id}/data/**"),
                Permission::READ,
            )]),
            HashSet::from([UserId::nil(other_realm)]),
        );
        operation.realm_auth_doc = Some(RealmAuthorizationDocument {
            realm_id,
            roles: HashMap::from([(foreign.role_id, foreign)]),
            operation_restrictions: HashMap::new(),
        });

        assert!(
            operation
                .collect_roles()
                .expect("roles collected")
                .is_empty()
        );
    }

    #[test]
    fn reuses_parent_txn() {
        let realm_id = RealmId([14u8; 32]);
        let group_id = Ulid::from(15u128);
        let txn_id = Ulid::from(16u128);
        let mut operation = PermissionRulesOperation::new_with_txn(
            PermissionRulesConfig {
                auth_context: AuthContext::anonymous(realm_id),
                path: format!("/{realm_id}/g/{group_id}"),
            },
            txn_id,
        );

        let effects = operation.start();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read {
                key_space,
                txn_id: Some(read_txn),
                ..
            })] if key_space == AUTH_KEYSPACE && *read_txn == txn_id
        ));

        let realm = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(postcard::to_allocvec(&realm).unwrap().into()),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read {
                key_space,
                txn_id: Some(read_txn),
                ..
            })] if key_space == AUTH_KEYSPACE && *read_txn == txn_id
        ));

        let group = GroupAuthorizationDocument::new_default_group_doc(
            UserId::nil(realm_id),
            realm_id,
            group_id,
        );
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(postcard::to_allocvec(&group).unwrap().into()),
        }));
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert!(operation.finalize().is_ok());

        let mut failed = PermissionRulesOperation::new_with_txn(
            PermissionRulesConfig {
                auth_context: AuthContext::anonymous(realm_id),
                path: format!("/{realm_id}/g/{group_id}"),
            },
            txn_id,
        );
        failed.start();
        let effects = failed.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        }));
        assert!(effects.is_empty());
        assert!(failed.is_complete());
        assert!(failed.finalize().is_err());
    }
}
