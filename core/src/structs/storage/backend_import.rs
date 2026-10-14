//! Defines backend import rules, the rows of imported objects and the import job records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::UserId;
use crate::errors::ConversionError;
use crate::id::NodeId;
use crate::structs::execution::job::JobId;
use crate::structs::identity::auth::AuthContext;
use crate::types::{GroupId, Key};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use ulid::Ulid;

/// Failures one import run keeps for its report; later ones are only counted.
pub const MAX_IMPORT_ERRORS: usize = 100;

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

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
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

pub fn object_prefix(import_id: Ulid) -> Key {
    import_id.to_bytes().to_vec().into()
}

/// One object of a backend listing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExternalObject {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
    pub modified_ms: Option<u64>,
}

/// What a run recorded for one backend key. It is written before the version, so a
/// failed run retries the same version id.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ImportedObject {
    pub version_id: Ulid,
    pub etag: Option<String>,
    pub size: u64,
    pub modified_ms: Option<u64>,
}

impl ImportedObject {
    pub fn new(version_id: Ulid, object: &ExternalObject) -> Self {
        Self {
            version_id,
            etag: object.etag.clone(),
            size: object.size,
            modified_ms: object.modified_ms,
        }
    }

    /// Without an ETag the modification time decides whether the object changed.
    pub fn matches(&self, object: &ExternalObject) -> bool {
        self.size == object.size
            && self.etag == object.etag
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

    #[test]
    fn maps_backend_keys() {
        let rule = BackendImport {
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
        };
        assert_eq!(
            rule.aruna_key("raw/x/a.csv").as_deref(),
            Some("imported/x/a.csv")
        );
        assert_eq!(rule.aruna_key("other/a.csv"), None);
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
    fn caps_kept_errors() {
        let mut checkpoint = BackendImportCheckpoint::default();
        for _ in 0..=MAX_IMPORT_ERRORS {
            checkpoint.fail("key", "broken".to_string());
        }
        assert_eq!(checkpoint.failed, MAX_IMPORT_ERRORS as u64 + 1);
        assert_eq!(checkpoint.errors.len(), MAX_IMPORT_ERRORS);
    }
}
