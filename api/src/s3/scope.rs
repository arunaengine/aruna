//! Read scopes for callers whose roles reach only a part of a bucket: the
//! access hook resolves them once and the listing handlers narrow their page to
//! them, while every concrete object path stays an ordinary permission check.

use aruna_core::errors::AuthorizationError;
use aruna_core::permission_path::readable_roots;
use aruna_core::structs::{AuthContext, PathRestriction, Permission, UserAccess};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::permission_rules::{
    PermissionRules, PermissionRulesConfig, PermissionRulesOperation,
};
use s3s::{S3Result, s3_error};

/// Navigable key prefixes and the rules deciding each concrete key's access.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct SubpathScope {
    prefixes: Vec<String>,
    root: String,
    rules: PermissionRules,
}

impl SubpathScope {
    /// Turns absolute permission roots into key prefixes relative to `root`.
    fn from_rules(
        rules: PermissionRules,
        restrictions: Option<&[PathRestriction]>,
        root: &str,
    ) -> Self {
        let roots = readable_roots(&rules.direct_patterns(), restrictions, root);
        let inside = format!("{root}/");
        let prefixes = roots
            .into_iter()
            .filter_map(|candidate| {
                if candidate == root {
                    Some(String::new())
                } else {
                    candidate.strip_prefix(&inside).map(str::to_string)
                }
            })
            .collect();
        Self {
            prefixes,
            root: root.to_string(),
            rules,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.prefixes.is_empty()
    }

    /// Applies the ordinary permission decision, including denies and restrictions.
    pub(crate) fn allows_key(&self, key: &str) -> bool {
        self.rules
            .allows(&format!("{}/{key}", self.root), &Permission::READ)
    }

    /// Whether a listing prefix is an ancestor of, equal to, or inside an
    /// allowed prefix. A request prefix without such an overlap is refused.
    pub(crate) fn allows_prefix(&self, prefix: &str) -> bool {
        self.prefixes.iter().any(|allowed| {
            allowed.is_empty()
                || allowed.starts_with(prefix)
                || prefix == allowed
                || prefix.starts_with(&format!("{allowed}/"))
        })
    }
}

/// Resolves the subtrees at or below `root` the caller reaches through its
/// realm and group roles, narrowed by the credential's own restrictions. A
/// missing realm or group document grants nothing instead of failing.
pub(crate) async fn resolve_scope(
    context: &DriverContext,
    user_access: &UserAccess,
    root: &str,
) -> S3Result<SubpathScope> {
    let auth_context = AuthContext {
        user_id: user_access.user_identity,
        realm_id: user_access.user_identity.realm_id,
        path_restrictions: user_access.path_restrictions.clone(),
        session: None,
    };
    let rules = match drive(
        PermissionRulesOperation::new(PermissionRulesConfig {
            auth_context,
            path: root.to_string(),
        }),
        context,
    )
    .await
    {
        Ok(rules) => rules,
        Err(
            AuthorizationError::AuthDocNotFound
            | AuthorizationError::GroupNotFound
            | AuthorizationError::InvalidRealmId
            | AuthorizationError::InvalidGroupId,
        ) => return Ok(SubpathScope::default()),
        Err(error) => return Err(s3_error!(InternalError, "{}", error.to_string())),
    };
    Ok(SubpathScope::from_rules(
        rules,
        user_access.path_restrictions.as_deref(),
        root,
    ))
}

#[cfg(test)]
mod tests {
    use aruna_core::structs::{PathRestriction, Permission, Role};
    use aruna_operations::permission_rules::{CollectedRole, PermissionRules};

    use super::SubpathScope;

    fn test_scope(
        patterns: &[(&str, Permission)],
        restrictions: Option<&[PathRestriction]>,
    ) -> SubpathScope {
        let root = "/realm/g/group/data/node/study";
        let rules = PermissionRules::from_roles(
            vec![CollectedRole {
                role: Role {
                    role_id: Default::default(),
                    name: "reader".to_string(),
                    permissions: patterns
                        .iter()
                        .map(|(key, permission)| (format!("{root}/{key}"), permission.clone()))
                        .collect(),
                    assigned_users: Default::default(),
                },
                direct: true,
                public: false,
            }],
            restrictions,
        )
        .unwrap();
        SubpathScope::from_rules(rules, restrictions, root)
    }

    #[test]
    fn allows_scoped_keys() {
        let scope = test_scope(&[("imaging/**", Permission::READ)], None);
        assert!(!scope.is_empty());
        assert!(scope.allows_key("imaging/scan.tif"));
        assert!(!scope.allows_key("imaging"));
        assert!(!scope.allows_key("imaging-2/scan.tif"));
        assert!(!scope.allows_key("sequencing/reads.fastq"));
    }

    #[test]
    fn keeps_scope_prefixes() {
        let scope = test_scope(&[("imaging/**", Permission::READ)], None);
        assert!(scope.allows_prefix(""));
        assert!(scope.allows_prefix("imaging/"));
        assert!(scope.allows_prefix("imaging/2026/"));
        assert!(!scope.allows_prefix("sequencing/"));
    }

    #[test]
    fn exact_stays_exact() {
        let scope = test_scope(&[("imaging", Permission::READ)], None);
        assert!(scope.allows_key("imaging"));
        assert!(!scope.allows_key("imaging/private/key"));
    }

    #[test]
    fn denies_filter_keys() {
        let scope = test_scope(
            &[
                ("**", Permission::READ),
                ("imaging/private/**", Permission::DENY),
                ("imaging/*/secret*", Permission::DENY),
            ],
            None,
        );
        assert!(scope.allows_prefix(""));
        assert!(scope.allows_key("sequencing/reads.fastq"));
        assert!(!scope.allows_key("imaging/private/key"));
        assert!(!scope.allows_key("imaging/public/secret.txt"));
    }

    #[test]
    fn restrictions_filter_keys() {
        let restrictions = [
            PathRestriction {
                pattern: "/realm/g/group/data/node/study/imaging/**".to_string(),
                permission: Permission::READ,
            },
            PathRestriction {
                pattern: "/realm/g/group/data/node/study/imaging/*/secret*".to_string(),
                permission: Permission::DENY,
            },
        ];
        let scope = test_scope(&[("**", Permission::READ)], Some(&restrictions));
        assert!(scope.allows_key("imaging/public/key"));
        assert!(!scope.allows_key("imaging/public/secret.txt"));
        assert!(!scope.allows_key("sequencing/reads.fastq"));
    }
}
