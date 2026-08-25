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

/// Rendered in place of a stat field the filesystem did not report. A
/// fingerprint carrying one is incomplete and can never stand in for a hash.
const UNKNOWN_FIELD: &str = "?";

/// The stat facts one weak fingerprint is built from. Size and mtime alone are
/// forgeable: a rewrite can restore both. The change time and the inode cannot
/// be set by a user tool, so a rewritten file always moves one of them.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FileStat {
    pub size: u64,
    /// Modification time in nanoseconds, at whatever resolution the filesystem
    /// keeps: a same-millisecond rewrite still moves it where it has one.
    pub modified_ns: Option<u128>,
    /// Inode change time in nanoseconds.
    pub changed_ns: Option<u128>,
    pub inode: Option<u64>,
}

impl FileStat {
    /// Every stat fact of one local file. The inode and change time are read
    /// where the platform has them and are absent otherwise, which makes the
    /// fingerprint incomplete rather than wrong.
    pub fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        Self {
            size: metadata.len(),
            modified_ns: nanos_since_epoch(metadata.modified().ok()),
            changed_ns: changed_nanos(metadata),
            inode: inode_of(metadata),
        }
    }

    /// What a listing that reports nothing but size and mtime can say. The
    /// fingerprint it produces is incomplete on purpose.
    pub fn partial(size: u64, modified: Option<SystemTime>) -> Self {
        Self {
            size,
            modified_ns: nanos_since_epoch(modified),
            ..Self::default()
        }
    }
}

#[cfg(unix)]
fn changed_nanos(metadata: &std::fs::Metadata) -> Option<u128> {
    use std::os::unix::fs::MetadataExt;
    let seconds = u128::try_from(metadata.ctime()).ok()?;
    let nanos = u128::try_from(metadata.ctime_nsec()).ok()?;
    Some(seconds.saturating_mul(1_000_000_000).saturating_add(nanos))
}

#[cfg(not(unix))]
fn changed_nanos(_metadata: &std::fs::Metadata) -> Option<u128> {
    None
}

#[cfg(unix)]
fn inode_of(metadata: &std::fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    Some(metadata.ino())
}

#[cfg(not(unix))]
fn inode_of(_metadata: &std::fs::Metadata) -> Option<u64> {
    None
}

fn nanos_since_epoch(time: Option<SystemTime>) -> Option<u128> {
    time.and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|since_epoch| since_epoch.as_nanos())
}

/// Weak identity of a local file. Listing and stat must derive it the same way,
/// or every read would look like drift.
pub fn weak_fingerprint(stat: &FileStat) -> String {
    let size = stat.size;
    let modified = field(stat.modified_ns);
    let changed = field(stat.changed_ns);
    let inode = field(stat.inode.map(u128::from));
    format!("{size:x}-{modified}-{changed}-{inode}")
}

fn field(value: Option<u128>) -> String {
    match value {
        Some(value) => format!("{value:x}"),
        None => UNKNOWN_FIELD.to_string(),
    }
}

/// Whether a fingerprint carries every stat fact. Only a complete one may stand
/// in for reading the file: an unknown field says the filesystem did not answer,
/// which is exactly when the bytes have to be hashed instead.
pub fn fingerprint_complete(fingerprint: &str) -> bool {
    let parts: Vec<&str> = fingerprint.split('-').collect();
    parts.len() == 4
        && parts
            .iter()
            .all(|part| !part.is_empty() && *part != UNKNOWN_FIELD)
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

    fn stat() -> FileStat {
        FileStat {
            size: 10,
            modified_ns: Some(1_000_000_000),
            changed_ns: Some(2_000_000_000),
            inode: Some(42),
        }
    }

    // Every stat fact has to move the fingerprint. Size and mtime alone are not
    // enough: a rewrite can restore both, and the change time cannot be, so a
    // file that changed under a preserved mtime must still look different.
    #[test]
    fn fingerprint_tracks_stat() {
        let base = weak_fingerprint(&stat());
        assert_eq!(base, weak_fingerprint(&stat()));
        for changed in [
            FileStat { size: 11, ..stat() },
            FileStat {
                modified_ns: Some(1_000_000_001),
                ..stat()
            },
            FileStat {
                changed_ns: Some(2_000_000_001),
                ..stat()
            },
            FileStat {
                inode: Some(43),
                ..stat()
            },
        ] {
            assert_ne!(base, weak_fingerprint(&changed));
        }
    }

    // A fingerprint the filesystem could not fill in must never authorise
    // reusing a recorded hash; the file has to be read instead.
    #[test]
    fn fingerprint_reports_gaps() {
        assert!(fingerprint_complete(&weak_fingerprint(&stat())));
        assert!(!fingerprint_complete(&weak_fingerprint(
            &FileStat::partial(10, Some(UNIX_EPOCH + Duration::from_secs(1)))
        )));
        assert!(!fingerprint_complete(&weak_fingerprint(
            &FileStat::default()
        )));
        assert!(!fingerprint_complete("a-b"));
        assert!(!fingerprint_complete(""));
    }
}
