use crate::structs::{PathRestriction, Permission};
use globset::GlobMatcher;
use thiserror::Error;

/// Maximum number of path restrictions a token or credential may carry.
pub const MAX_TOKEN_RESTRICTIONS: usize = 50;
/// Maximum byte length of a single restriction pattern.
pub const MAX_RESTRICTION_PATTERN_BYTES: usize = 512;
/// Maximum combined byte length of all restriction patterns.
pub const MAX_RESTRICTIONS_TOTAL_BYTES: usize = 16 * 1024;

/// Compiles a permission path pattern with separator-anchored wildcards: `*` and
/// `?` never cross `/`, only `**` spans segments, so a pattern scoped to one
/// subtree cannot leak into a deeper namespace (globset default lets `*` cross).
pub fn compile_permission_matcher(pattern: &str) -> Result<GlobMatcher, globset::Error> {
    Ok(globset::GlobBuilder::new(pattern)
        .literal_separator(true)
        .build()?
        .compile_matcher())
}

/// Whether a permission path pattern matches a concrete path, treating a
/// pattern that fails to compile as non-matching (fail-closed).
pub fn permission_pattern_matches(pattern: &str, path: &str) -> bool {
    compile_permission_matcher(pattern)
        .map(|matcher| matcher.is_match(path))
        .unwrap_or(false)
}

/// The path subtree a group's roles may grant on.
pub fn role_subtree_root(
    realm_id: impl std::fmt::Display,
    group_id: impl std::fmt::Display,
) -> String {
    format!("/{realm_id}/g/{group_id}")
}

/// A group-role permission pattern may only grant on its own group subtree: it
/// must compile and be the subtree root or literally prefixed by it, so
/// wildcards cannot reach another group, realm, or admin namespace.
pub fn role_path_confined(pattern: &str, subtree_root: &str) -> bool {
    if compile_permission_matcher(pattern).is_err() {
        return false;
    }
    pattern == subtree_root || pattern.starts_with(&format!("{subtree_root}/"))
}

/// Whether a concrete path is a root itself or lies below it.
pub fn path_within(path: &str, root: &str) -> bool {
    path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

/// The literal subtree a pattern grants on: the pattern itself when it holds no
/// wildcard, the parent of a trailing `/**`, and nothing otherwise, so an
/// unsupported wildcard never widens a derived scope.
fn pattern_root(pattern: &str) -> Option<&str> {
    if let Some(root) = pattern.strip_suffix("/**") {
        return (!root.is_empty() && !root.contains(['*', '?', '[', ']', '{', '}']))
            .then_some(root);
    }
    (!pattern.contains(['*', '?', '[', ']', '{', '}'])).then_some(pattern)
}

/// Splits patterns into the allowed and denied roots at or below `root`. A
/// pattern that covers `root` itself contributes `root`.
fn split_roots<'a>(
    patterns: impl IntoIterator<Item = (&'a str, &'a Permission)>,
    root: &str,
) -> (Vec<String>, Vec<String>) {
    let mut allowed = Vec::new();
    let mut denied = Vec::new();
    for (pattern, permission) in patterns {
        let covered = if permission_pattern_matches(pattern, root) {
            Some(root.to_string())
        } else {
            pattern_root(pattern)
                .filter(|candidate| path_within(candidate, root))
                .map(str::to_string)
        };
        let Some(covered) = covered else {
            continue;
        };
        match permission {
            Permission::DENY => denied.push(covered),
            _ => allowed.push(covered),
        }
    }
    (allowed, denied)
}

/// The subtrees at or below `root` a caller may reach, from its role patterns
/// and narrowed by a credential's restrictions. Whether a concrete path inside
/// one is readable stays with the ordinary permission check.
pub fn readable_roots(
    granted: &[(String, Permission)],
    restrictions: Option<&[PathRestriction]>,
    root: &str,
) -> Vec<String> {
    let (mut allowed, mut denied) = split_roots(
        granted
            .iter()
            .map(|(pattern, permission)| (pattern.as_str(), permission)),
        root,
    );
    if let Some(restrictions) = restrictions {
        let (restricted, restricted_denied) = split_roots(
            restrictions
                .iter()
                .map(|restriction| (restriction.pattern.as_str(), &restriction.permission)),
            root,
        );
        allowed = narrow_roots(&allowed, &restricted);
        denied.extend(restricted_denied);
    }
    allowed.retain(|candidate| !denied.iter().any(|deny| path_within(candidate, deny)));
    allowed.sort();
    allowed.dedup();
    allowed
}

/// Keeps the deeper root of every overlapping pair, so a narrowing set can only
/// shrink the reachable subtrees.
fn narrow_roots(granted: &[String], narrowing: &[String]) -> Vec<String> {
    let mut kept = Vec::new();
    for candidate in granted {
        for other in narrowing {
            if path_within(candidate, other) {
                kept.push(candidate.clone());
            } else if path_within(other, candidate) {
                kept.push(other.clone());
            }
        }
    }
    kept
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RestrictionLimitError {
    #[error("too many path restrictions ({count})")]
    TooManyRestrictions { count: usize },
    #[error("path restriction pattern too long ({bytes} bytes)")]
    PatternTooLong { bytes: usize },
    #[error("path restrictions total size too large ({bytes} bytes)")]
    TotalTooLarge { bytes: usize },
}

/// Fail-closed size limits on token and credential path restrictions, enforced
/// at issuance and validation so an oversized restriction set cannot exhaust the
/// permission evaluator.
pub fn validate_restriction_limits(
    restrictions: &[PathRestriction],
) -> Result<(), RestrictionLimitError> {
    if restrictions.len() > MAX_TOKEN_RESTRICTIONS {
        return Err(RestrictionLimitError::TooManyRestrictions {
            count: restrictions.len(),
        });
    }
    let mut total = 0usize;
    for restriction in restrictions {
        let bytes = restriction.pattern.len();
        if bytes > MAX_RESTRICTION_PATTERN_BYTES {
            return Err(RestrictionLimitError::PatternTooLong { bytes });
        }
        total = total.saturating_add(bytes);
    }
    if total > MAX_RESTRICTIONS_TOTAL_BYTES {
        return Err(RestrictionLimitError::TotalTooLarge { bytes: total });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_RESTRICTION_PATTERN_BYTES, MAX_TOKEN_RESTRICTIONS, RestrictionLimitError,
        permission_pattern_matches, readable_roots, validate_restriction_limits,
    };
    use crate::structs::{PathRestriction, Permission};

    #[test]
    fn single_star_bounded() {
        // A single-segment wildcard must stay within one path segment.
        assert!(permission_pattern_matches("/realm/*", "/realm/group"));
        assert!(!permission_pattern_matches(
            "/realm/*",
            "/realm/g/abc/meta/doc"
        ));
        assert!(!permission_pattern_matches(
            "/realm/*/meta",
            "/realm/g/abc/meta"
        ));
    }

    #[test]
    fn double_star_spans() {
        assert!(permission_pattern_matches(
            "/realm/**",
            "/realm/g/abc/meta/doc"
        ));
        assert!(permission_pattern_matches(
            "/realm/g/abc/data/**",
            "/realm/g/abc/data/node/bucket/key"
        ));
        assert!(!permission_pattern_matches(
            "/realm/g/abc/data/**",
            "/realm/g/abc/meta/doc"
        ));
    }

    #[test]
    fn matches_boundaries() {
        assert!(permission_pattern_matches("/realm/g/abc", "/realm/g/abc"));
        assert!(!permission_pattern_matches("/realm/g/abc", "/realm/g/abcd"));
        // `**` at the tail also matches the empty suffix of the anchored prefix.
        assert!(permission_pattern_matches(
            "/realm/g/abc/**",
            "/realm/g/abc/x"
        ));
    }

    #[test]
    fn matches_bare_prefix() {
        // A group-wide metadata watch derives the path with a trailing slash.
        assert!(permission_pattern_matches(
            "/realm/g/abc/meta/**",
            "/realm/g/abc/meta/"
        ));
        assert!(permission_pattern_matches(
            "/realm/g/abc/**",
            "/realm/g/abc/meta/"
        ));
    }

    #[test]
    fn malformed_never_matches() {
        assert!(!permission_pattern_matches("/realm/[", "/realm/anything"));
    }

    #[test]
    fn rejects_excess_restrictions() {
        let restriction = PathRestriction {
            pattern: "/realm/g/abc/data/**".to_string(),
            permission: Permission::READ,
        };
        let within = vec![restriction.clone(); MAX_TOKEN_RESTRICTIONS];
        assert!(validate_restriction_limits(&within).is_ok());

        let too_many = vec![restriction.clone(); MAX_TOKEN_RESTRICTIONS + 1];
        assert_eq!(
            validate_restriction_limits(&too_many),
            Err(RestrictionLimitError::TooManyRestrictions {
                count: MAX_TOKEN_RESTRICTIONS + 1
            })
        );

        let long = vec![PathRestriction {
            pattern: "a".repeat(MAX_RESTRICTION_PATTERN_BYTES + 1),
            permission: Permission::READ,
        }];
        assert_eq!(
            validate_restriction_limits(&long),
            Err(RestrictionLimitError::PatternTooLong {
                bytes: MAX_RESTRICTION_PATTERN_BYTES + 1
            })
        );
    }

    fn granted(patterns: &[(&str, Permission)]) -> Vec<(String, Permission)> {
        patterns
            .iter()
            .map(|(pattern, permission)| (pattern.to_string(), permission.clone()))
            .collect()
    }

    const BUCKET: &str = "/realm/g/group/data/node/study";

    #[test]
    fn pattern_covers_root() {
        let granted = granted(&[("/realm/g/group/data/**", Permission::READ)]);
        assert_eq!(readable_roots(&granted, None, BUCKET), vec![BUCKET]);
    }

    #[test]
    fn pattern_yields_subpath() {
        let granted = granted(&[
            (
                "/realm/g/group/data/node/study/imaging/**",
                Permission::READ,
            ),
            ("/realm/g/group/meta/**", Permission::WRITE),
        ]);
        assert_eq!(
            readable_roots(&granted, None, BUCKET),
            vec![format!("{BUCKET}/imaging")]
        );
    }

    #[test]
    fn unrelated_yields_nothing() {
        let granted = granted(&[("/realm/g/other/data/**", Permission::WRITE)]);
        assert!(readable_roots(&granted, None, BUCKET).is_empty());
    }

    #[test]
    fn deny_removes_root() {
        let granted = granted(&[
            (
                "/realm/g/group/data/node/study/imaging/**",
                Permission::READ,
            ),
            ("/realm/g/group/data/node/study/**", Permission::DENY),
        ]);
        assert!(readable_roots(&granted, None, BUCKET).is_empty());
    }

    #[test]
    fn restrictions_narrow_roots() {
        // A credential restricted deeper than the role keeps only its own scope.
        let granted = granted(&[("/realm/g/group/data/**", Permission::READ)]);
        let restrictions = vec![PathRestriction {
            pattern: format!("{BUCKET}/imaging/**"),
            permission: Permission::READ,
        }];
        assert_eq!(
            readable_roots(&granted, Some(&restrictions), BUCKET),
            vec![format!("{BUCKET}/imaging")]
        );

        let elsewhere = vec![PathRestriction {
            pattern: "/realm/g/group/data/node/other/**".to_string(),
            permission: Permission::READ,
        }];
        assert!(readable_roots(&granted, Some(&elsewhere), BUCKET).is_empty());
    }

    #[test]
    fn wildcard_grants_nothing() {
        let granted = granted(&[("/realm/g/group/data/node/study/*/imaging", Permission::READ)]);
        assert!(readable_roots(&granted, None, BUCKET).is_empty());
    }
}
