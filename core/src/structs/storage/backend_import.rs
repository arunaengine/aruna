//! Defines backend import rules, the rows of imported objects and the import job records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::UserId;
use crate::errors::ConversionError;
use crate::id::NodeId;
use crate::structs::execution::job::JobId;
use crate::structs::identity::auth::AuthContext;
use crate::structs::storage::group_backend::path_bucket;
use crate::types::{GroupId, Key};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use ulid::Ulid;

/// Failures one import run keeps for its report; later ones are only counted.
pub const MAX_IMPORT_ERRORS: usize = 100;

/// Write pattern of a rule that names none: a unique file per version.
pub const DEFAULT_PATTERN: &str = "{dir}{id}_{name}";

/// Write pattern of a provider bucket linked to a bucket: the bucket key, unchanged.
pub const PLAIN_PATTERN: &str = "{dir}{name}";

/// Registers the objects under `backend_prefix` of a group backend in `bucket` under
/// `key_prefix`. The bytes stay where they are and Aruna never deletes them.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BackendImport {
    pub import_id: Ulid,
    pub group_id: GroupId,
    pub backend_id: Ulid,
    pub backend_prefix: String,
    pub bucket: String,
    pub key_prefix: String,
    /// Cron expression evaluated in UTC; `None` runs only on request.
    pub schedule: Option<String>,
    pub created_by: UserId,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub last_job: Option<JobId>,
    pub last_started_ms: Option<u64>,
    /// Group backend that new writes go to; `None` is `backend_id`.
    pub write_backend_id: Option<Ulid>,
    /// Folder that new writes go to; `None` is `backend_prefix`.
    pub write_prefix: Option<String>,
    /// Path of a new write below the write folder, see [`expand_pattern`].
    pub write_pattern: String,
}

impl BackendImport {
    pub fn key(&self) -> Key {
        import_key(self.group_id, self.import_id)
    }

    /// The bucket key of a backend key; `None` outside `backend_prefix`.
    pub fn aruna_key(&self, backend_key: &str) -> Option<String> {
        let rest = backend_key.strip_prefix(&self.backend_prefix)?;
        Some(format!("{}{rest}", self.key_prefix))
    }

    pub fn write_backend(&self) -> Ulid {
        self.write_backend_id.unwrap_or(self.backend_id)
    }

    pub fn write_folder(&self) -> &str {
        self.write_prefix.as_deref().unwrap_or(&self.backend_prefix)
    }

    /// Whether this rule reads or writes `bucket` on `backend_id`, a storage without a bucket.
    pub fn uses_bucket(&self, backend_id: Ulid, bucket: &str) -> bool {
        let names = |folder: &str| path_bucket(folder) == Some(bucket);
        (self.backend_id == backend_id && names(&self.backend_prefix))
            || (self.write_backend() == backend_id && names(self.write_folder()))
    }

    /// The provider bucket this rule links whole to its whole bucket, with plain keys.
    pub fn mapped_bucket(&self) -> Option<&str> {
        let bucket = path_bucket(&self.backend_prefix)?;
        (self.key_prefix.is_empty()
            && self.backend_prefix.len() == bucket.len() + 1
            && self.write_backend() == self.backend_id
            && self.write_folder() == self.backend_prefix
            && self.write_pattern == PLAIN_PATTERN)
            .then_some(bucket)
    }

    /// Without `{id}` a write replaces the file of the same name.
    pub fn same_name(&self) -> bool {
        !self.write_pattern.contains("{id}")
    }

    /// The backend path of a write of `key` as version `id`; `None` outside `key_prefix`,
    /// for a key without a file name except a folder key, and for a path that is not plain.
    pub fn plain_path(&self, key: &str, id: Ulid) -> Option<String> {
        let rest = key.strip_prefix(&self.key_prefix)?;
        let (dir, name) = match rest.rfind('/') {
            Some(slash) => rest.split_at(slash + 1),
            None => ("", rest),
        };
        // A folder key, like `reports/`, keeps its exact name under the plain pattern only.
        if name.is_empty() {
            let path = format!("{}{rest}", self.write_folder());
            path.strip_suffix('/').filter(|folder| plain_safe(folder))?;
            let plain = self.write_pattern == PLAIN_PATTERN && !rest.is_empty();
            return plain.then_some(path);
        }
        let expanded = expand_pattern(&self.write_pattern, dir, name, id)?;
        let path = format!("{}{expanded}", self.write_folder());
        plain_safe(&path).then_some(path)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Replaces `{dir}`, `{name}` and `{id}` in one pass, so a name holding a placeholder stays
/// as it is. Any other brace is invalid.
pub fn expand_pattern(pattern: &str, dir: &str, name: &str, id: Ulid) -> Option<String> {
    let mut expanded = String::new();
    let mut rest = pattern;
    while let Some(start) = rest.find(['{', '}']) {
        expanded.push_str(&rest[..start]);
        let tail = &rest[start..];
        let (value, length) = if tail.starts_with("{dir}") {
            (dir.to_string(), 5)
        } else if tail.starts_with("{name}") {
            (name.to_string(), 6)
        } else if tail.starts_with("{id}") {
            (id.to_string(), 4)
        } else {
            return None;
        };
        expanded.push_str(&value);
        rest = &tail[length..];
    }
    expanded.push_str(rest);
    Some(expanded)
}

/// A pattern needs `{name}` and must give a plain relative path in and below the folder.
pub fn valid_pattern(pattern: &str) -> bool {
    pattern.contains("{name}")
        && ["", "dir/"].into_iter().all(|dir| {
            expand_pattern(pattern, dir, "name", Ulid::nil()).is_some_and(|path| plain_safe(&path))
        })
}

/// Relative, without empty, `.` or `..` parts and without control characters. OpenDAL
/// trims the ends of a path, so a path with outer whitespace would name another file.
fn plain_safe(path: &str) -> bool {
    path.trim() == path
        && path.split('/').all(|part| {
            !part.is_empty() && part != "." && part != ".." && !part.chars().any(char::is_control)
        })
}

/// Where a same-name write of version `id` goes when a file without versions holds `path`.
pub fn tagged_path(path: &str, id: Ulid) -> String {
    format!("{path}~{id}")
}

/// Rules are keyed by group first, so one group's rules are one prefix scan.
pub fn import_key(group_id: GroupId, import_id: Ulid) -> Key {
    let mut key = group_id.to_bytes().to_vec();
    key.extend_from_slice(&import_id.to_bytes());
    key.into()
}

pub fn object_key(import_id: Ulid, backend_key: &str) -> Key {
    let mut key = import_id.to_bytes().to_vec();
    key.extend_from_slice(backend_key.as_bytes());
    key.into()
}

/// Backend versions are keyed by the ulid of the location they belong to.
pub fn version_key(location: Ulid) -> Key {
    location.to_bytes().to_vec().into()
}

pub fn object_prefix(import_id: Ulid) -> Key {
    import_id.to_bytes().to_vec().into()
}

/// A write to a fixed path in the tenant's folders; `ulid` names its location.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlainPath {
    pub path: String,
    pub ulid: Ulid,
    /// Fails the write with `PathTaken` if a file is already at the path.
    pub if_not_exists: bool,
}

/// What a removal mark does after the rows: delete this provider bucket while it is empty.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProviderCleanup {
    pub backend_id: Ulid,
    pub bucket: String,
}

impl ProviderCleanup {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    /// `None` for the empty value of a mark without a provider bucket.
    pub fn from_mark(value: &[u8]) -> Result<Option<Self>, ConversionError> {
        match value.is_empty() {
            true => Ok(None),
            false => Ok(Some(postcard::from_bytes(value)?)),
        }
    }
}

/// One object of a backend listing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExternalObject {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
    pub modified_ms: Option<u64>,
}

/// What a run recorded for one backend key. It commits with the version it names, so a
/// failed import leaves the previous row in place.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ImportedObject {
    pub version_id: Ulid,
    pub etag: Option<String>,
    pub size: u64,
    pub modified_ms: Option<u64>,
    /// The bucket key of a version Aruna wrote at this backend key.
    pub key: Option<String>,
}

impl ImportedObject {
    pub fn new(version_id: Ulid, object: &ExternalObject) -> Self {
        Self {
            version_id,
            etag: object.etag.clone(),
            size: object.size,
            modified_ms: object.modified_ms,
            key: None,
        }
    }

    /// A version Aruna wrote itself. Without the backend's ETag its first listing only records
    /// its ETag and time; with it, listings compare like any imported object.
    pub fn written(version_id: Ulid, key: String, size: u64, etag: Option<String>) -> Self {
        Self {
            version_id,
            etag,
            size,
            modified_ms: None,
            key: Some(key),
        }
    }

    /// Aruna wrote it and no listing has seen it since.
    pub fn unlisted(&self) -> bool {
        self.key.is_some() && self.etag.is_none() && self.modified_ms.is_none()
    }

    /// The first listing of Aruna's own write only records its ETag and time; `None` when
    /// the row is no such write or the listed object has another size.
    pub fn first_listing(&self, object: &ExternalObject) -> Option<Self> {
        (self.unlisted() && self.size == object.size).then(|| Self {
            etag: object.etag.clone(),
            modified_ms: object.modified_ms,
            ..self.clone()
        })
    }

    /// Without an ETag the modification time decides whether the object changed. Quotes
    /// around an ETag do not count, as write answers and listings differ there.
    pub fn matches(&self, object: &ExternalObject) -> bool {
        let unquoted = |etag: Option<&str>| etag.map(|etag| etag.trim_matches('"').to_string());
        self.size == object.size
            && unquoted(self.etag.as_deref()) == unquoted(object.etag.as_deref())
            && (object.etag.is_some() || self.modified_ms == object.modified_ms)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// One run of an import rule. The rule is read when the run starts, so a removed
/// rule stops the run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendImportSpec {
    pub auth_context: AuthContext,
    pub node_id: NodeId,
    pub group_id: GroupId,
    pub import_id: Ulid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ImportFailure {
    pub key: String,
    pub error: String,
}

/// Progress of one run; `cursor` is the last backend key the run finished.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct BackendImportCheckpoint {
    pub cursor: Option<String>,
    pub imported: u64,
    pub skipped: u64,
    pub failed: u64,
    pub errors: Vec<ImportFailure>,
}

impl BackendImportCheckpoint {
    pub fn fail(&mut self, key: &str, error: String) {
        self.failed = self.failed.saturating_add(1);
        if self.errors.len() < MAX_IMPORT_ERRORS {
            self.errors.push(ImportFailure {
                key: key.to_string(),
                error,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(etag: Option<&str>, modified_ms: Option<u64>) -> ExternalObject {
        ExternalObject {
            key: "raw/a.csv".to_string(),
            size: 4,
            etag: etag.map(str::to_string),
            modified_ms,
        }
    }

    fn rule(pattern: &str) -> BackendImport {
        BackendImport {
            import_id: Ulid::nil(),
            group_id: Ulid::nil(),
            backend_id: Ulid::nil(),
            backend_prefix: "raw/".to_string(),
            bucket: "bucket".to_string(),
            key_prefix: "imported/".to_string(),
            schedule: None,
            created_by: UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: pattern.to_string(),
        }
    }

    #[test]
    fn maps_backend_keys() {
        let rule = rule(DEFAULT_PATTERN);
        assert_eq!(
            rule.aruna_key("raw/x/a.csv").as_deref(),
            Some("imported/x/a.csv")
        );
        assert_eq!(rule.aruna_key("other/a.csv"), None);
    }

    #[test]
    fn expands_write_patterns() {
        let id = Ulid::from_parts(1, 2);
        let path = |pattern: &str, key: &str| rule(pattern).plain_path(key, id);
        assert_eq!(
            path(DEFAULT_PATTERN, "imported/x/a.csv"),
            Some(format!("raw/x/{id}_a.csv"))
        );
        assert_eq!(
            path("{dir}{id}/{name}", "imported/a.csv"),
            Some(format!("raw/{id}/a.csv"))
        );
        assert_eq!(
            path("{dir}{name}", "imported/x/y/a.csv").as_deref(),
            Some("raw/x/y/a.csv")
        );
        // A placeholder inside a name is kept as written.
        assert_eq!(
            path("{dir}{name}", "imported/{id}.csv").as_deref(),
            Some("raw/{id}.csv")
        );
        for key in [
            "imported/",
            "imported/x/",
            "other/a.csv",
            "imported/x//a.csv",
        ] {
            assert_eq!(path(DEFAULT_PATTERN, key), None, "{key}");
        }
        // Under the plain pattern a folder key keeps its exact name.
        assert_eq!(
            path(PLAIN_PATTERN, "imported/x/").as_deref(),
            Some("raw/x/")
        );
        for key in ["imported/", "imported/x//"] {
            assert_eq!(path(PLAIN_PATTERN, key), None, "{key}");
        }
        assert_eq!(path("{dir}{name}", "imported/../a.csv"), None);
        // A trimmed path would name another file.
        assert_eq!(path("{dir}{name}", "imported/a.csv "), None);
        assert!(!valid_pattern("{name} "));

        let mut moved = rule("{name}");
        moved.write_prefix = Some("new/".to_string());
        assert_eq!(
            moved.plain_path("imported/x/a.csv", id).as_deref(),
            Some("new/a.csv")
        );
        assert!(!rule(DEFAULT_PATTERN).same_name());
        assert!(moved.same_name());
    }

    #[test]
    fn checks_write_patterns() {
        for pattern in [DEFAULT_PATTERN, "{dir}{id}/{name}", "{dir}{name}", "{name}"] {
            assert!(valid_pattern(pattern), "{pattern}");
        }
        for pattern in [
            "{dir}{id}",
            "{dir}/{name}",
            "{dir}{other}{name}",
            "../{name}",
            "{name}/",
        ] {
            assert!(!valid_pattern(pattern), "{pattern}");
        }
    }

    #[test]
    fn detects_changed_objects() {
        let row = ImportedObject::new(Ulid::nil(), &object(Some("e1"), Some(1)));
        assert!(row.matches(&object(Some("e1"), Some(2))));
        assert!(!row.matches(&object(Some("e2"), Some(1))));
        let untagged = ImportedObject::new(Ulid::nil(), &object(None, Some(1)));
        assert!(untagged.matches(&object(None, Some(1))));
        assert!(!untagged.matches(&object(None, Some(2))));
    }

    #[test]
    fn notes_own_writes() {
        let written = ImportedObject::written(Ulid::nil(), "in/a.csv".to_string(), 4, None);
        let listed = written.first_listing(&object(Some("e1"), Some(5))).unwrap();
        assert_eq!(listed.key.as_deref(), Some("in/a.csv"));
        assert!(listed.matches(&object(Some("e1"), Some(5))));
        // Once listed, a later outside edit is a change like any other.
        assert_eq!(listed.first_listing(&object(Some("e2"), Some(6))), None);
        assert!(!listed.matches(&object(Some("e2"), Some(6))));
        // A different size means someone replaced the file before the first listing.
        let resized = ExternalObject {
            size: 9,
            ..object(Some("e1"), Some(5))
        };
        assert_eq!(written.first_listing(&resized), None);
        assert!(!ImportedObject::new(Ulid::nil(), &object(None, None)).unlisted());
        // A write that kept the backend's ETag compares by it, quotes aside.
        let tagged = ImportedObject::written(Ulid::nil(), "in/a.csv".into(), 4, Some("e1".into()));
        assert_eq!(tagged.first_listing(&object(Some("\"e1\""), Some(5))), None);
        assert!(tagged.matches(&object(Some("\"e1\""), Some(5))));
        assert!(!tagged.matches(&object(Some("\"e2\""), Some(5))));
    }

    #[test]
    fn finds_used_buckets() {
        let mut rule = rule(DEFAULT_PATTERN);
        rule.backend_prefix = "lab-raw/2024/".to_string();
        assert!(rule.uses_bucket(Ulid::nil(), "lab-raw"));
        assert!(!rule.uses_bucket(Ulid::nil(), "lab"));
        assert!(!rule.uses_bucket(Ulid::from_parts(1, 1), "lab-raw"));
        rule.write_prefix = Some("lab-new/".to_string());
        assert!(rule.uses_bucket(Ulid::nil(), "lab-new"));
    }

    #[test]
    fn caps_kept_errors() {
        let mut checkpoint = BackendImportCheckpoint::default();
        for _ in 0..=MAX_IMPORT_ERRORS {
            checkpoint.fail("key", "broken".to_string());
        }
        assert_eq!(checkpoint.failed, MAX_IMPORT_ERRORS as u64 + 1);
        assert_eq!(checkpoint.errors.len(), MAX_IMPORT_ERRORS);
    }
}
