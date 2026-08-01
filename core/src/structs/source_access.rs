use super::source_connector::{SourceConnector, SourceConnectorKind};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResolvedSourceAccess {
    OpenDal {
        kind: SourceConnectorKind,
        config: HashMap<String, String>,
        path: String,
        version: Option<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedSourceConnector {
    pub connector: SourceConnector,
    pub secret_fingerprint: Option<[u8; 16]>,
    pub access: ResolvedSourceAccess,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceMetadata {
    pub content_length: u64,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<SystemTime>,
    #[serde(skip)]
    pub source_version: Option<String>,
}

impl SourceMetadata {
    /// Canonical fingerprint of the drift-identifying fields of a source
    /// observation. Two observations with the same fingerprint are the same
    /// representation, so a resolving access MUST NOT create a successor
    /// version. Provider version and the complete-read BLAKE3 are folded in by
    /// the verified reference cache (#375); this is the HEAD-derived signal.
    pub fn observation_fingerprint(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.content_length.to_le_bytes());
        write_fingerprint_field(&mut hasher, self.content_type.as_deref());
        write_fingerprint_field(&mut hasher, self.etag.as_deref());
        match self
            .last_modified
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        {
            Some(since_epoch) => {
                hasher.update(&[1]);
                hasher.update(&since_epoch.as_millis().to_le_bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        }
        *hasher.finalize().as_bytes()
    }
}

/// Length-prefixed canonical encoding of an optional string field so distinct
/// fields cannot collide across boundaries.
fn write_fingerprint_field(hasher: &mut blake3::Hasher, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hasher.update(&(value.len() as u64).to_le_bytes());
            hasher.update(value.as_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceEntryKind {
    File,
    Directory,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceEntry {
    pub name: String,
    pub path: String,
    pub kind: SourceEntryKind,
    pub size: Option<u64>,
    pub modified: Option<SystemTime>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn metadata() -> SourceMetadata {
        SourceMetadata {
            content_length: 42,
            content_type: Some("text/plain".to_string()),
            etag: Some("abc".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1000)),
            source_version: None,
        }
    }

    #[test]
    fn fingerprint_is_stable_and_field_sensitive() {
        let base = metadata();
        assert_eq!(base.observation_fingerprint(), base.observation_fingerprint());

        let mut longer = base.clone();
        longer.content_length = 43;
        assert_ne!(base.observation_fingerprint(), longer.observation_fingerprint());

        let mut retagged = base.clone();
        retagged.etag = Some("def".to_string());
        assert_ne!(base.observation_fingerprint(), retagged.observation_fingerprint());

        let mut touched = base.clone();
        touched.last_modified = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(2000));
        assert_ne!(base.observation_fingerprint(), touched.observation_fingerprint());
    }

    // source_version is transient (not persisted), so it must not perturb the
    // fingerprint or a stored observation would spuriously read as drifted.
    #[test]
    fn fingerprint_ignores_transient_source_version() {
        let base = metadata();
        let mut versioned = base.clone();
        versioned.source_version = Some("v7".to_string());
        assert_eq!(
            base.observation_fingerprint(),
            versioned.observation_fingerprint()
        );
    }
}
