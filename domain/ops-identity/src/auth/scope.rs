//! Resolves the key prefixes a caller may read when a role reaches only part of a bucket.
//! Listing frontends narrow their page to those prefixes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0
// Every concrete object path still goes through an ordinary permission check.

use crate::auth::permission_rules::PermissionRules;
use aruna_core::errors::AuthorizationError;
use aruna_core::permission_path::readable_roots;
use aruna_core::structs::identity::auth::{PathRestriction, Permission};

/// Navigable key prefixes and the rules deciding each concrete key's access.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SubpathScope {
    prefixes: Vec<String>,
    root: String,
    rules: PermissionRules,
}

impl SubpathScope {
    /// Turns absolute permission roots into key prefixes relative to `root`.
    pub fn from_rules(
        rules: PermissionRules,
        restrictions: Option<&[PathRestriction]>,
        root: &str,
    ) -> Self {
        let inside = format!("{root}/");
        let prefixes = readable_roots(&rules.direct_patterns(), restrictions, root)
            .into_iter()
            .filter_map(|allowed| {
                if allowed == root {
                    Some(String::new())
                } else {
                    allowed.strip_prefix(&inside).map(str::to_string)
                }
            })
            .collect();
        Self {
            prefixes,
            root: root.to_string(),
            rules,
        }
    }

    /// The scope of the rules `rules_operation` collected below `root`. A missing realm or
    /// group document grants nothing instead of failing; any other error is returned.
    pub fn resolve(
        rules: Result<PermissionRules, AuthorizationError>,
        restrictions: Option<&[PathRestriction]>,
        root: &str,
    ) -> Result<Self, AuthorizationError> {
        match rules {
            Ok(rules) => Ok(Self::from_rules(rules, restrictions, root)),
            Err(
                AuthorizationError::DocNotFound
                | AuthorizationError::GroupNotFound
                | AuthorizationError::InvalidGroupId
                | AuthorizationError::InvalidRealmId,
            ) => Ok(Self::default()),
            Err(error) => Err(error),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.prefixes.is_empty()
    }

    /// Applies the ordinary permission decision, including denies and restrictions.
    pub fn allows_key(&self, key: &str) -> bool {
        self.rules
            .allows(&format!("{}/{key}", self.root), &Permission::READ)
    }

    /// Whether a listing prefix starts, equals, or lies inside an allowed prefix,
    /// so `imag` starts `imaging`. A request prefix without such an overlap is refused.
    pub fn allows_prefix(&self, prefix: &str) -> bool {
        self.prefixes.iter().any(|allowed| {
            allowed.is_empty()
                || allowed.starts_with(prefix)
                || prefix == allowed
                || prefix.starts_with(&format!("{allowed}/"))
        })
    }

    pub fn overlaps(&self, path: &str) -> bool {
        if path == self.root {
            return !self.is_empty();
        }
        path.strip_prefix(&format!("{}/", self.root))
            .is_some_and(|prefix| self.allows_prefix(prefix))
    }
}

#[cfg(test)]
mod tests {
    use super::SubpathScope;
    use crate::auth::permission_rules::{CollectedRole, PermissionRules};
    use aruna_core::errors::AuthorizationError;
    use aruna_core::structs::identity::auth::{PathRestriction, Permission, Role};

    const ROOT: &str = "/realm/g/group/data/node/study";

    fn test_rules(
        patterns: &[(&str, Permission)],
        restrictions: Option<&[PathRestriction]>,
    ) -> PermissionRules {
        PermissionRules::from_roles(
            vec![CollectedRole {
                role: Role {
                    role_id: Default::default(),
                    name: "reader".to_string(),
                    permissions: patterns
                        .iter()
                        .map(|(key, permission)| (format!("{ROOT}/{key}"), permission.clone()))
                        .collect(),
                    assigned_users: Default::default(),
                },
                direct: true,
                public: false,
            }],
            restrictions,
        )
        .unwrap()
    }

    fn test_scope(
        patterns: &[(&str, Permission)],
        restrictions: Option<&[PathRestriction]>,
    ) -> SubpathScope {
        SubpathScope::from_rules(test_rules(patterns, restrictions), restrictions, ROOT)
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
        assert!(scope.allows_prefix("imag"));
        assert!(!scope.allows_prefix("imag/"));
        assert!(!scope.allows_prefix("imagery/"));
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
                pattern: format!("{ROOT}/imaging/**"),
                permission: Permission::READ,
            },
            PathRestriction {
                pattern: format!("{ROOT}/imaging/*/secret*"),
                permission: Permission::DENY,
            },
        ];
        let scope = test_scope(&[("**", Permission::READ)], Some(&restrictions));
        assert!(scope.allows_key("imaging/public/key"));
        assert!(!scope.allows_key("imaging/public/secret.txt"));
        assert!(!scope.allows_key("sequencing/reads.fastq"));
    }

    #[test]
    fn overlaps_stay_contained() {
        // `team/` never includes `team-old/`, and the root overlaps only a nonempty scope.
        let scope = test_scope(&[("team/**", Permission::READ)], None);
        assert!(scope.overlaps(ROOT));
        assert!(scope.overlaps(&format!("{ROOT}/team")));
        assert!(scope.overlaps(&format!("{ROOT}/team/inner")));
        assert!(!scope.overlaps(&format!("{ROOT}/team-old")));
        assert!(!scope.overlaps("/realm/g/other/data/node/study/team"));
        assert!(!SubpathScope::default().overlaps(""));
    }

    #[test]
    fn missing_grants_nothing() {
        for missing in [
            AuthorizationError::DocNotFound,
            AuthorizationError::GroupNotFound,
            AuthorizationError::InvalidGroupId,
            AuthorizationError::InvalidRealmId,
        ] {
            let scope = SubpathScope::resolve(Err(missing), None, ROOT).unwrap();
            assert!(scope.is_empty());
        }
        let failed = SubpathScope::resolve(Err(AuthorizationError::NotFinished), None, ROOT);
        assert_eq!(failed, Err(AuthorizationError::NotFinished));
        let rules = test_rules(&[("imaging/**", Permission::READ)], None);
        let scope = SubpathScope::resolve(Ok(rules), None, ROOT).unwrap();
        assert!(scope.allows_key("imaging/scan.tif"));
    }
}
