use crate::structs::PathRestriction;
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
        permission_pattern_matches, validate_restriction_limits,
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
}
