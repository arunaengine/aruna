//! Collects the roles and restrictions of a caller into path rules that many paths reuse.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::permission_path::{
    compile_permission_matcher, restrictions_cover_subtree, validate_restriction_limits,
};
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission, Role};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{GroupId, Key, KeySpace, Value};
use globset::GlobMatcher;
use ulid::Ulid;

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

    /// Patterns of the roles the caller holds directly, for callers that derive
    /// reachable subtrees instead of deciding one concrete path.
    pub fn direct_patterns(&self) -> Vec<(String, Permission)> {
        self.rules
            .iter()
            .filter(|rule| rule.direct)
            .map(|rule| {
                (
                    rule.matcher.glob().glob().to_string(),
                    rule.permission.clone(),
                )
            })
            .collect()
    }

    pub fn admits_scope(
        &self,
        root: &str,
        scope: &aruna_core::structs::storage::abe_access::KeyScope,
    ) -> bool {
        use aruna_core::structs::storage::abe_access::KeyScope;
        if scope.validate().is_err() {
            return false;
        }
        match scope {
            KeyScope::Exact(key) => self.allows(&format!("{root}/{key}"), &Permission::READ),
            KeyScope::Writes(writes) => (writes.iter())
                .all(|(key, _)| self.allows(&format!("{root}/{key}"), &Permission::READ)),
            KeyScope::Subtree(prefix) => self.reads_below(&format!("{root}/{prefix}")),
        }
    }

    /// Whether every path that starts with `path` is readable: a role grant and, for a scoped
    /// token, a restriction cover all of them, and no deny of either could reach one.
    pub fn reads_below(&self, path: &str) -> bool {
        let patterns: Vec<_> = (self.rules.iter())
            .filter(|rule| rule.direct || rule.public && rule.permission == Permission::READ)
            .map(|rule| (rule.matcher.glob().glob(), &rule.permission))
            .collect();
        if !covers_below(&patterns, path) {
            return false;
        }
        match &self.restrictions {
            None => true,
            Some(rules) => covers_below(
                &(rules.iter())
                    .map(|rule| (rule.matcher.glob().glob(), &rule.permission))
                    .collect::<Vec<_>>(),
                path,
            ),
        }
    }

    /// Like [`Self::reads_below`] for `required` access: a WRITE needs WRITE grants on the whole
    /// subtree, and no deny of a role or a scoped token may reach into it.
    pub fn holds_below(&self, path: &str, required: &Permission) -> bool {
        if *required == Permission::READ {
            return self.reads_below(path);
        }
        let held = |permission: &Permission| *permission != Permission::READ;
        let patterns: Vec<_> = (self.rules.iter())
            .filter(|rule| rule.direct && held(&rule.permission))
            .map(|rule| (rule.matcher.glob().glob(), &rule.permission))
            .collect();
        let restrictions = self.restrictions.as_ref().map(|rules| {
            (rules.iter())
                .filter(|rule| held(&rule.permission))
                .map(|rule| (rule.matcher.glob().glob(), &rule.permission))
                .collect::<Vec<_>>()
        });
        covers_below(&patterns, path)
            && restrictions.is_none_or(|restrictions| covers_below(&restrictions, path))
    }

    fn restrictions_allow(&self, path: &str, required: &Permission) -> bool {
        let Some(restrictions) = self.restrictions.as_ref() else {
            return true;
        };
        if let Some(root) = path.strip_suffix("/**") {
            let patterns = restrictions
                .iter()
                .map(|restriction| (restriction.matcher.glob().glob(), &restriction.permission));
            return restrictions_cover_subtree(patterns, root, required);
        }

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

/// Whether `patterns` grant every path that starts with `path` and no deny could reach one. A
/// grant ends in `**` below a base of whole segments, its wildcards matched against `path`.
fn covers_below(patterns: &[(&str, &Permission)], path: &str) -> bool {
    let mut allowed = false;
    for (pattern, permission) in patterns {
        if **permission == Permission::DENY {
            let literal =
                (pattern.split(['*', '?', '[', ']', '{', '}', '\\']).next()).unwrap_or_default();
            if literal.starts_with(path) || path.starts_with(literal) {
                return false;
            }
        } else if let Some(base) = pattern.strip_suffix("**")
            && (base.is_empty() || base.ends_with('/'))
            && base_matches(base, path)
        {
            allowed = true;
        }
    }
    allowed
}

/// Whether the glob `base`, whole segments that end in `/`, matches as many leading segments
/// of `path`, so everything below them matches `base` followed by `**`.
fn base_matches(base: &str, path: &str) -> bool {
    let depth = base.matches('/').count();
    let Some(end) = depth.checked_sub(1) else {
        return true;
    };
    let Some((at, _)) = path.match_indices('/').nth(end) else {
        return false;
    };
    compile_permission_matcher(base).is_ok_and(|matcher| matcher.is_match(&path[..=at]))
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
/// one permission check per candidate path. Both documents come from one batch read.
#[derive(Debug, PartialEq)]
pub struct PermissionRulesOperation {
    config: PermissionRulesConfig,
}

impl PermissionRulesOperation {
    pub fn new(config: PermissionRulesConfig) -> Self {
        PermissionRulesOperation { config }
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
}

/// The authorization rows a rule collection for `path` reads: the realm document, then the
/// group document when the path names a group.
pub(crate) fn rule_reads(path: &str) -> Result<Vec<(KeySpace, Key)>, AuthorizationError> {
    let (realm, group) = PermissionRulesOperation::parse_path(path)?;
    let mut reads = vec![(
        AUTH_KEYSPACE.to_string(),
        Key::from(realm.as_bytes().to_vec()),
    )];
    if let Some(group) = group {
        reads.push((
            AUTH_KEYSPACE.to_string(),
            Key::from(group.to_bytes().to_vec()),
        ));
    }
    Ok(reads)
}

/// The caller's rules from the rows [`rule_reads`] named, in that order. A missing document
/// fails the collection.
pub(crate) fn read_rules(
    auth_context: &AuthContext,
    values: Vec<Option<Value>>,
) -> Result<PermissionRules, AuthorizationError> {
    let mut values = values.into_iter();
    let realm = values
        .next()
        .flatten()
        .ok_or(AuthorizationError::DocNotFound)?;
    let realm = RealmAuthorizationDocument::from_bytes(&realm)?;
    let group = values
        .next()
        .map(|group| {
            let group = group.ok_or(AuthorizationError::DocNotFound)?;
            GroupAuthorizationDocument::from_bytes(&group).map_err(AuthorizationError::from)
        })
        .transpose()?;
    let roles = collect_roles(auth_context, &realm, group.as_ref());
    PermissionRules::from_roles(roles, auth_context.path_restrictions.as_deref())
}

fn collect_roles(
    auth_context: &AuthContext,
    realm_auth_doc: &RealmAuthorizationDocument,
    group_auth_doc: Option<&GroupAuthorizationDocument>,
) -> Vec<CollectedRole> {
    let realm_id = realm_auth_doc.realm_id;
    let auth_user = auth_context.user_id;
    let mut roles = realm_auth_doc.roles.clone();
    if let Some(group) = group_auth_doc {
        roles.extend(group.roles.clone());
    }
    roles
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
        .collect()
}

impl Operation for PermissionRulesOperation {
    type Output = PermissionRules;
    type Error = AuthorizationError;

    fn first(&mut self) -> Result<Next<PermissionRules>, AuthorizationError> {
        Ok(Next::Call(IoCall::Read(rule_reads(&self.config.path)?)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PermissionRules>, AuthorizationError> {
        let IoResult::Values(values) = result else {
            return Err(AuthorizationError::UnexpectedEvent {
                state: "PermissionRules".to_string(),
                expected: "IoResult::Values",
                got: result.label().to_string(),
            });
        };
        read_rules(&self.config.auth_context, values).map(Next::Done)
    }
}

#[cfg(test)]
mod pure_tests {
    use std::collections::{HashMap, HashSet};

    use aruna_core::UserId;
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::errors::AuthorizationError;
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission, Role};
    use aruna_core::structs::identity::group::GroupAuthorizationDocument;
    use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId};
    use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
    use ulid::Ulid;

    use super::{
        CollectedRole, PermissionRules, PermissionRulesConfig, PermissionRulesOperation,
        collect_roles,
    };

    fn role(permissions: HashMap<String, Permission>, assigned: HashSet<UserId>) -> Role {
        Role {
            role_id: Ulid::from_parts(1, 1),
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
        let group_id = Ulid::from_parts(2, 2);
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
        let group_id = Ulid::from_parts(3, 3);
        let secret = MetadataRegistryRecord::permission_path_for(
            &realm_id,
            group_id,
            "datasets/secret",
            Ulid::from_parts(4, 4),
        );
        let sibling = MetadataRegistryRecord::permission_path_for(
            &realm_id,
            group_id,
            "datasets/open",
            Ulid::from_parts(5, 5),
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
        let group_id = Ulid::from_parts(6, 6);
        let granted = MetadataRegistryRecord::permission_path_for(
            &realm_id,
            group_id,
            "datasets/shared",
            Ulid::from_parts(7, 7),
        );
        let other = MetadataRegistryRecord::permission_path_for(
            &realm_id,
            group_id,
            "datasets/private",
            Ulid::from_parts(8, 8),
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
        let group_id = Ulid::from_parts(9, 9);
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
        let group_id = Ulid::from_parts(10, 10);
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
        let group_id = Ulid::from_parts(11, 11);
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
        let group_id = Ulid::from_parts(12, 12);
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
    fn any_node_restriction() {
        let data = "/realm/g/group/data";
        let restricted = PermissionRules::from_roles(
            vec![CollectedRole {
                role: role(
                    HashMap::from([(format!("{data}/**"), Permission::WRITE)]),
                    HashSet::new(),
                ),
                direct: true,
                public: false,
            }],
            Some(&[PathRestriction {
                pattern: format!("{data}/*/bucket/**"),
                permission: Permission::READ,
            }]),
        )
        .expect("patterns compile");
        // Every member node grants the same scope, also for a whole subtree.
        for node in ["node-1", "node-2"] {
            assert!(restricted.allows(&format!("{data}/{node}/bucket/key"), &Permission::READ));
            assert!(restricted.allows(&format!("{data}/{node}/bucket/**"), &Permission::READ));
            assert!(!restricted.allows(&format!("{data}/{node}/other/key"), &Permission::READ));
        }
    }

    #[test]
    fn public_grants_read() {
        // Public roles grant READ to everyone but never WRITE, and a direct
        // DENY still wins over a public grant.
        let realm_id = RealmId([8u8; 32]);
        let group_id = Ulid::from_parts(13, 13);
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
    fn node_glob_covers() {
        // The distributed bucket grant names every node by a wildcard segment; it covers a
        // prefix of the bucket on any node, while a deny below the prefix still hides it.
        let root = "/realm/g/group/data";
        let any_node = format!("{root}/*/bucket/**");
        let rules = direct_rules(HashMap::from([(any_node.clone(), Permission::READ)]));
        assert!(rules.reads_below(&format!("{root}/node/bucket/raw/")));
        assert!(rules.reads_below(&format!("{root}/node/bucket/raw")));
        assert!(!rules.reads_below(&format!("{root}/node/other/raw/")));
        let denied = direct_rules(HashMap::from([
            (any_node, Permission::READ),
            (format!("{root}/node/bucket/raw/x/**"), Permission::DENY),
        ]));
        assert!(!denied.reads_below(&format!("{root}/node/bucket/raw/")));
    }

    #[test]
    fn write_below_subtree() {
        let meta = "/realm/g/group/meta";
        let write = direct_rules(HashMap::from([(format!("{meta}/**"), Permission::WRITE)]));
        assert!(write.holds_below(&format!("{meta}/"), &Permission::WRITE));
        assert!(write.holds_below(&format!("{meta}/runs/"), &Permission::READ));
        let read = direct_rules(HashMap::from([(format!("{meta}/**"), Permission::READ)]));
        assert!(!read.holds_below(&format!("{meta}/"), &Permission::WRITE));
        assert!(read.holds_below(&format!("{meta}/"), &Permission::READ));
        let denied = direct_rules(HashMap::from([
            (format!("{meta}/**"), Permission::WRITE),
            (format!("{meta}/secret/**"), Permission::DENY),
        ]));
        assert!(!denied.holds_below(&format!("{meta}/"), &Permission::WRITE));
        assert!(denied.holds_below(&format!("{meta}/runs/"), &Permission::WRITE));
        let restricted = PermissionRules::from_roles(
            vec![CollectedRole {
                role: role(
                    HashMap::from([(format!("{meta}/**"), Permission::WRITE)]),
                    HashSet::new(),
                ),
                direct: true,
                public: false,
            }],
            Some(&[PathRestriction {
                pattern: format!("{meta}/**"),
                permission: Permission::READ,
            }]),
        )
        .expect("patterns compile");
        assert!(!restricted.holds_below(&format!("{meta}/"), &Permission::WRITE));
    }

    #[test]
    fn subtree_needs_separator() {
        use aruna_core::structs::storage::abe_access::KeyScope;
        let root = "/realm/g/group/data/node/bucket";
        let narrow = format!("{root}/foo**");
        let scope = KeyScope::Subtree("foo/".into());
        let rules = direct_rules(HashMap::from([(narrow.clone(), Permission::READ)]));
        assert!(!rules.allows(&format!("{root}/foo/file"), &Permission::READ));
        assert!(!rules.admits_scope(root, &scope));
        let restricted = PermissionRules::from_roles(
            vec![CollectedRole {
                role: role(
                    HashMap::from([(format!("{root}/**"), Permission::READ)]),
                    HashSet::new(),
                ),
                direct: true,
                public: false,
            }],
            Some(&[PathRestriction {
                pattern: narrow,
                permission: Permission::READ,
            }]),
        )
        .expect("patterns compile");
        assert!(!restricted.allows(&format!("{root}/foo/file"), &Permission::READ));
        assert!(!restricted.admits_scope(root, &scope));
    }

    #[test]
    fn patterns_exclude_public() {
        let realm_id = RealmId([19u8; 32]);
        let direct_path = format!("/{realm_id}/g/**");
        let public_path = format!("/{realm_id}/public/**");
        let rules = PermissionRules::from_roles(
            vec![
                CollectedRole {
                    role: role(
                        HashMap::from([(direct_path.clone(), Permission::READ)]),
                        HashSet::new(),
                    ),
                    direct: true,
                    public: false,
                },
                CollectedRole {
                    role: role(
                        HashMap::from([(public_path, Permission::READ)]),
                        HashSet::from([UserId::nil(realm_id)]),
                    ),
                    direct: false,
                    public: true,
                },
            ],
            None,
        )
        .expect("patterns compile");

        assert_eq!(
            rules.direct_patterns(),
            vec![(direct_path, Permission::READ)]
        );
    }

    #[test]
    fn skips_foreign_nil() {
        // Only this realm's Everyone principal makes a role public.
        let realm_id = RealmId([9u8; 32]);
        let other_realm = RealmId([10u8; 32]);
        let group_id = Ulid::from_parts(14, 14);
        let foreign = role(
            HashMap::from([(
                format!("/{realm_id}/g/{group_id}/data/**"),
                Permission::READ,
            )]),
            HashSet::from([UserId::nil(other_realm)]),
        );
        let realm = RealmAuthorizationDocument {
            realm_id,
            roles: HashMap::from([(foreign.role_id, foreign)]),
            operation_restrictions: HashMap::new(),
        };

        assert!(collect_roles(&AuthContext::anonymous(realm_id), &realm, None).is_empty());
    }

    #[test]
    fn reads_both_documents() {
        // One batch read names the realm and the group document; a missing one fails.
        let realm_id = RealmId([14u8; 32]);
        let group_id = Ulid::from(15u128);
        let config = || PermissionRulesConfig {
            auth_context: AuthContext::anonymous(realm_id),
            path: format!("/{realm_id}/g/{group_id}"),
        };
        let mut operation = PermissionRulesOperation::new(config());
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Read(reads)))
                if reads.len() == 2 && reads.iter().all(|(space, _)| space == AUTH_KEYSPACE)
        ));
        let realm = RealmAuthorizationDocument::default_realm_doc(realm_id);
        let group = GroupAuthorizationDocument::default_group_doc(
            UserId::nil(realm_id),
            realm_id,
            group_id,
        );
        let rows = vec![
            Some(postcard::to_allocvec(&realm).unwrap().into()),
            Some(postcard::to_allocvec(&group).unwrap().into()),
        ];
        assert!(matches!(
            operation.next(IoResult::Values(rows)),
            Ok(Next::Done(_))
        ));

        let mut failed = PermissionRulesOperation::new(config());
        failed.first().unwrap();
        let realm = Some(postcard::to_allocvec(&realm).unwrap().into());
        assert_eq!(
            failed.next(IoResult::Values(vec![realm, None])),
            Err(AuthorizationError::DocNotFound)
        );
        assert!(matches!(
            failed.next(IoResult::Done),
            Err(AuthorizationError::UnexpectedEvent { .. })
        ));
    }
}
