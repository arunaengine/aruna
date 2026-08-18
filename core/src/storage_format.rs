//! Identity of the one on-disk format this build supports.
//!
//! Every Aruna-owned database root carries a single marker row. A root that
//! carries a different identity or epoch belongs to another format generation
//! and is rejected before any record is decoded; there is no migration.

use crate::errors::ConversionError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Realm-independent identity of the owning format, so an unrelated fjall root
/// pointed at Aruna is refused instead of adopted.
pub const STORAGE_FORMAT_ID: &str = "aruna-storage";
/// Current storage-format epoch. Bump it whenever stored records change shape.
pub const STORAGE_FORMAT_EPOCH: u32 = 1;
/// The single row inside [`crate::keyspaces::STORAGE_FORMAT_KEYSPACE`].
pub const STORAGE_FORMAT_KEY: &[u8] = b"epoch";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum StorageFormatError {
    #[error(
        "storage root holds format `{id}` epoch {epoch}, this build requires `{STORAGE_FORMAT_ID}` epoch {STORAGE_FORMAT_EPOCH}"
    )]
    Mismatch { id: String, epoch: u32 },
    #[error("storage root holds an undecodable format marker")]
    Undecodable,
    #[error("storage root holds records but no format marker")]
    Unmarked,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageFormatMarker {
    pub id: String,
    pub epoch: u32,
}

impl Default for StorageFormatMarker {
    fn default() -> Self {
        Self {
            id: STORAGE_FORMAT_ID.to_string(),
            epoch: STORAGE_FORMAT_EPOCH,
        }
    }
}

impl StorageFormatMarker {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    /// Accepts the stored marker of a root this build may open. `None` means a
    /// fresh root that the caller initializes with [`Self::default`].
    pub fn verify(stored: Option<&[u8]>) -> Result<Option<Self>, StorageFormatError> {
        let Some(bytes) = stored else {
            return Ok(None);
        };
        let marker: Self =
            postcard::from_bytes(bytes).map_err(|_| StorageFormatError::Undecodable)?;
        if marker.id != STORAGE_FORMAT_ID || marker.epoch != STORAGE_FORMAT_EPOCH {
            return Err(StorageFormatError::Mismatch {
                id: marker.id,
                epoch: marker.epoch,
            });
        }
        Ok(Some(marker))
    }
}

#[cfg(test)]
mod tests {
    use super::{STORAGE_FORMAT_EPOCH, StorageFormatError, StorageFormatMarker};

    #[test]
    fn accepts_current_marker() {
        let current = StorageFormatMarker::default();
        assert_eq!(
            StorageFormatMarker::verify(Some(&current.to_bytes().unwrap())),
            Ok(Some(current))
        );
    }

    #[test]
    fn reports_fresh_root() {
        assert_eq!(StorageFormatMarker::verify(None), Ok(None));
    }

    #[test]
    fn rejects_other_epoch() {
        // An older root must fail here, before any record decode is attempted.
        let older = StorageFormatMarker {
            epoch: STORAGE_FORMAT_EPOCH - 1,
            ..StorageFormatMarker::default()
        };
        assert!(matches!(
            StorageFormatMarker::verify(Some(&older.to_bytes().unwrap())),
            Err(StorageFormatError::Mismatch { .. })
        ));
    }

    #[test]
    fn rejects_foreign_id() {
        let foreign = StorageFormatMarker {
            id: "other-store".to_string(),
            ..StorageFormatMarker::default()
        };
        assert!(matches!(
            StorageFormatMarker::verify(Some(&foreign.to_bytes().unwrap())),
            Err(StorageFormatError::Mismatch { .. })
        ));
    }

    #[test]
    fn rejects_garbage_marker() {
        assert_eq!(
            StorageFormatMarker::verify(Some(&[0xff, 0xff, 0xff])),
            Err(StorageFormatError::Undecodable)
        );
    }
}
