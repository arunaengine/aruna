//! Read scopes for callers whose roles reach only a part of a bucket: the
//! access hook resolves them once and the listing handlers narrow their page to
//! them, while every concrete object path stays an ordinary permission check.

use aruna_core::permission_path::readable_roots;
use aruna_core::structs::UserAccess;
use aruna_core::types::GroupId;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_group::{GetGroupConfig, GetGroupError, GetGroupOperation};
use s3s::{S3Result, s3_error};

/// Key prefixes inside one bucket the caller may read. An empty prefix stands
/// for the whole bucket, and an empty scope for no access at all.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SubpathScope {
    prefixes: Vec<String>,
}

impl SubpathScope {
    /// Turns absolute permission roots into key prefixes relative to `root`.
    fn from_roots(roots: Vec<String>, root: &str) -> Self {
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
        Self { prefixes }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.prefixes.is_empty()
    }

    /// Whether the caller may read the whole bucket, which needs no filtering.
    pub(crate) fn covers_all(&self) -> bool {
        self.prefixes.iter().any(String::is_empty)
    }

    /// Whether a key lies inside an allowed prefix.
    pub(crate) fn allows_key(&self, key: &str) -> bool {
        self.prefixes.iter().any(|prefix| {
            prefix.is_empty() || key == prefix || key.starts_with(&format!("{prefix}/"))
        })
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

/// Resolves the subtrees at or below `root` the caller reaches through its group
/// roles, narrowed by the credential's own restrictions. A group without an
/// authorization document grants nothing instead of failing the request.
pub(crate) async fn resolve_scope(
    context: &DriverContext,
    user_access: &UserAccess,
    group_id: GroupId,
    root: &str,
) -> S3Result<SubpathScope> {
    let authorization =
        match drive(GetGroupOperation::new(GetGroupConfig { group_id }), context).await {
            Ok((_, authorization)) => authorization,
            Err(GetGroupError::GroupNotFound | GetGroupError::AuthDocNotFound) => {
                return Ok(SubpathScope::default());
            }
            Err(error) => {
                return Err(s3_error!(InternalError, "{}", error.to_string()));
            }
        };

    let granted = authorization.user_permissions(user_access.user_identity);
    Ok(SubpathScope::from_roots(
        readable_roots(&granted, user_access.path_restrictions.as_deref(), root),
        root,
    ))
}

#[cfg(test)]
mod tests {
    use super::SubpathScope;

    fn imaging_scope() -> SubpathScope {
        SubpathScope::from_roots(
            vec!["/realm/g/group/data/node/study/imaging".to_string()],
            "/realm/g/group/data/node/study",
        )
    }

    #[test]
    fn allows_scoped_keys() {
        let scope = imaging_scope();
        assert!(!scope.is_empty());
        assert!(!scope.covers_all());
        assert!(scope.allows_key("imaging/scan.tif"));
        assert!(scope.allows_key("imaging"));
        assert!(!scope.allows_key("imaging-2/scan.tif"));
        assert!(!scope.allows_key("sequencing/reads.fastq"));
    }

    #[test]
    fn keeps_scope_prefixes() {
        let scope = imaging_scope();
        assert!(scope.allows_prefix(""));
        assert!(scope.allows_prefix("imaging/"));
        assert!(scope.allows_prefix("imaging/2026/"));
        assert!(!scope.allows_prefix("sequencing/"));
    }

    #[test]
    fn root_covers_all() {
        let scope = SubpathScope::from_roots(
            vec!["/realm/g/group/data/node/study".to_string()],
            "/realm/g/group/data/node/study",
        );
        assert!(scope.covers_all());
        assert!(scope.allows_key("sequencing/reads.fastq"));
    }
}
