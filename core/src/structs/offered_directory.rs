use crate::errors::ConversionError;
use crate::types::UserId;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use ulid::Ulid;

/// Descriptor key naming the offered bucket a local-directory source belongs to.
/// The registration it names is device-local, so the root path never travels.
pub const OFFERED_DIRECTORY_BUCKET: &str = "offered_bucket";

/// Access key carrying the offered root. It is filled in on the owning device
/// while the source is resolved and is never part of a stored descriptor.
pub const OFFERED_DIRECTORY_ROOT: &str = "offered_root";

/// One directory a device offers as a read-only bucket, keyed by that bucket.
/// The record stays device-local: it is the only place the root path exists.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OfferedDirectory {
    pub bucket: String,
    pub group_id: Ulid,
    /// Root as the owner registered it; every access re-resolves it and refuses
    /// anything that leaves the resolved directory.
    pub root: String,
    pub created_at: SystemTime,
    pub created_by: UserId,
}

impl OfferedDirectory {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Weak identity of a local file: size and modification time. Listing and stat
/// must derive it the same way, or every read would look like drift.
pub fn weak_fingerprint(size: u64, modified: Option<SystemTime>) -> String {
    let millis = modified
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|since_epoch| since_epoch.as_millis())
        .unwrap_or_default();
    format!("{size:x}-{millis:x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::RealmId;
    use std::time::Duration;

    fn record() -> OfferedDirectory {
        OfferedDirectory {
            bucket: "photos".to_string(),
            group_id: Ulid::from_bytes([3u8; 16]),
            root: "/home/owner/photos".to_string(),
            created_at: UNIX_EPOCH + Duration::from_secs(10),
            created_by: UserId::new(Ulid::from_bytes([4u8; 16]), RealmId::from_bytes([5u8; 32])),
        }
    }

    #[test]
    fn record_roundtrip() {
        let stored = record();
        assert_eq!(
            OfferedDirectory::from_bytes(&stored.to_bytes().unwrap()).unwrap(),
            stored
        );
    }

    // Size and mtime both have to move the fingerprint, or a same-size rewrite
    // would be served under the identity of the bytes it replaced.
    #[test]
    fn fingerprint_tracks_stat() {
        let base = weak_fingerprint(10, Some(UNIX_EPOCH + Duration::from_secs(1)));
        assert_eq!(
            base,
            weak_fingerprint(10, Some(UNIX_EPOCH + Duration::from_secs(1)))
        );
        assert_ne!(
            base,
            weak_fingerprint(11, Some(UNIX_EPOCH + Duration::from_secs(1)))
        );
        assert_ne!(
            base,
            weak_fingerprint(10, Some(UNIX_EPOCH + Duration::from_secs(2)))
        );
        assert_ne!(base, weak_fingerprint(10, None));
    }
}
