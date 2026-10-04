//! Describes how one stored copy keeps its bytes on a backend: its layout and encryption.
//! Also defines the compression setting a write applies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::errors::ConversionError;
use crate::structs::storage::encryption::BucketKeyRef;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// How one copy stores its bytes. Its raw default encodes like the two `false`
/// flags it replaced, so older location rows decode unchanged. Only valid pairs of layout and
/// encryption encode or decode.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StoredFormat {
    pub layout: StoredLayout,
    pub encryption: StoredEncryption,
}

/// The encoded shape of a stored format.
#[derive(Serialize, Deserialize)]
#[serde(rename = "StoredFormat")]
struct FormatParts<L, E> {
    layout: L,
    encryption: E,
}

impl Serialize for StoredFormat {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.checked().map_err(serde::ser::Error::custom)?;
        let parts = FormatParts {
            layout: &self.layout,
            encryption: &self.encryption,
        };
        parts.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StoredFormat {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let parts = FormatParts::<StoredLayout, StoredEncryption>::deserialize(deserializer)?;
        let format = Self {
            layout: parts.layout,
            encryption: parts.encryption,
        };
        format.checked().map_err(serde::de::Error::custom)?;
        Ok(format)
    }
}

/// Arrangement of the stored bytes.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum StoredLayout {
    /// The original bytes, unchanged.
    #[default]
    Raw,
    /// The zstd seekable format: 1 MiB zstd frames, then a seek table in a skippable frame.
    Frames(Box<FrameLayout>),
    /// A Pithos 1.1 archive with one encrypted file.
    Pithos(Box<PithosLayout>),
}

/// Record of a framed copy. The index hash covers the frame digests and seek table at its end.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FrameLayout {
    /// The zstd level the frames were written with.
    pub level: u8,
    /// Frames are at most 1 MiB; a multipart part may end with a shorter one.
    pub frames: u32,
    pub stored_size: u64,
    pub index_hash: [u8; 32],
}

/// Record of a Pithos copy. The digest covers every directory of the archive, so a changed
/// archive fails when it is opened, before anything is decrypted.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PithosLayout {
    pub stored_size: u64,
    pub metadata_digest: [u8; 32],
}

/// Compression a write applies: off, or zstd with a level.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum Compression {
    #[default]
    Off,
    Zstd {
        level: u8,
    },
}

impl Compression {
    /// Highest zstd level a bucket may use.
    pub const MAX_LEVEL: u8 = 22;

    /// Rejects zstd levels outside 1 to 22.
    pub fn checked(self) -> Result<Self, ConversionError> {
        match self {
            Self::Zstd { level } if level == 0 || level > Self::MAX_LEVEL => Err(
                ConversionError::FromStrError(format!("zstd level {level} is not in 1..=22")),
            ),
            _ => Ok(self),
        }
    }
}

/// Encryption of the stored bytes.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum StoredEncryption {
    #[default]
    None,
    /// The piece keys of a Pithos archive are granted to this bucket key generation.
    Pithos(Box<BucketKeyRef>),
}

impl StoredFormat {
    /// A Pithos archive of a bucket key generation.
    pub fn pithos(layout: PithosLayout, key: BucketKeyRef) -> Self {
        Self {
            layout: StoredLayout::Pithos(Box::new(layout)),
            encryption: StoredEncryption::Pithos(Box::new(key)),
        }
    }

    /// Raw bytes and frames are plain; a Pithos archive always names its key generation.
    pub fn checked(&self) -> Result<(), ConversionError> {
        match (&self.layout, &self.encryption) {
            (StoredLayout::Raw | StoredLayout::Frames(_), StoredEncryption::None)
            | (StoredLayout::Pithos(_), StoredEncryption::Pithos(_)) => Ok(()),
            _ => Err(ConversionError::InvalidLength(
                "stored layout and encryption do not match".to_string(),
            )),
        }
    }

    /// The key generation an encrypted copy needs; `None` for plain copies.
    pub fn bucket_key(&self) -> Option<BucketKeyRef> {
        match &self.encryption {
            StoredEncryption::Pithos(key) => Some(**key),
            StoredEncryption::None => None,
        }
    }

    /// The class this copy shares physical bytes within.
    pub fn encoding(&self) -> EncodingClass {
        match &self.layout {
            StoredLayout::Raw => EncodingClass::Raw,
            StoredLayout::Frames(layout) => EncodingClass::Zstd {
                level: layout.level,
            },
            StoredLayout::Pithos(layout) => EncodingClass::Pithos {
                digest: layout.metadata_digest,
            },
        }
    }
}

/// Copies of one hash on one backend share bytes only within one class, so
/// buckets with different settings never share a physical copy.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum EncodingClass {
    Raw,
    Zstd {
        level: u8,
    },
    /// Encrypted copies are never shared: the archive's metadata digest is unique to each copy.
    Pithos {
        digest: [u8; 32],
    },
}

/// Tag of the zstd class in keys; backend keys start with `n` or `g` instead.
const ZSTD_TAG: u8 = b'z';
/// Tag of the Pithos class in keys, followed by the 32-byte metadata digest.
const PITHOS_TAG: u8 = b'p';

impl EncodingClass {
    /// Bytes placed between hash and backend in a location key. Raw adds none,
    /// so raw keys keep their shape; backend keys start with `n:` or `g:`.
    pub fn key_bytes(&self) -> Vec<u8> {
        match self {
            Self::Raw => Vec::new(),
            Self::Zstd { level } => vec![ZSTD_TAG, *level],
            Self::Pithos { digest } => [&[PITHOS_TAG], &digest[..]].concat(),
        }
    }

    /// Reads a class written by `key_bytes`.
    pub fn from_key_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        match bytes {
            [] => Ok(Self::Raw),
            [ZSTD_TAG, level] => Ok(Self::Zstd { level: *level }),
            [PITHOS_TAG, digest @ ..] if digest.len() == 32 => Ok(Self::Pithos {
                digest: digest.try_into()?,
            }),
            _ => Err(ConversionError::InvalidLength(
                "unknown encoding class in key".to_string(),
            )),
        }
    }

    /// Splits the class from the backend part of a location key.
    pub fn split_key(bytes: &[u8]) -> Result<(Self, &[u8]), ConversionError> {
        match bytes.first() {
            Some(b'n' | b'g') => Ok((Self::Raw, bytes)),
            Some(&ZSTD_TAG) if bytes.len() > 2 => Ok((Self::Zstd { level: bytes[1] }, &bytes[2..])),
            Some(&PITHOS_TAG) if bytes.len() > 33 => Ok((
                Self::Pithos {
                    digest: bytes[1..33].try_into()?,
                },
                &bytes[33..],
            )),
            _ => Err(ConversionError::InvalidLength(
                "unknown encoding class in location key".to_string(),
            )),
        }
    }
}

/// The class a write with this compression stores its copy in.
impl From<Compression> for EncodingClass {
    fn from(compression: Compression) -> Self {
        match compression {
            Compression::Off => Self::Raw,
            Compression::Zstd { level } => Self::Zstd { level },
        }
    }
}

/// This node's progress re-encoding one bucket's versions to `target`.
/// Keyed by bucket name; a later setting change replaces it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompressionMigration {
    pub target: Compression,
    /// Last version key handled; the next batch resumes after it.
    pub cursor: Option<Vec<u8>>,
    /// Versions moved to `target` over all passes.
    pub migrated: u64,
    /// Versions the current pass did not need to change.
    pub skipped: u64,
    /// Versions the current or last pass could not move.
    pub failed: u64,
    /// Passes started again because the pass before them had failures.
    pub retries: u32,
    /// Set while a retry pass waits; it starts at this time.
    pub retry_at_ms: Option<u64>,
    pub started_at_ms: u64,
    pub finished_at_ms: Option<u64>,
}

impl CompressionMigration {
    pub fn new(target: Compression, started_at_ms: u64) -> Self {
        Self {
            target,
            cursor: None,
            migrated: 0,
            skipped: 0,
            failed: 0,
            retries: 0,
            retry_at_ms: None,
            started_at_ms,
            finished_at_ms: None,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Compression, EncodingClass, FormatParts, PithosLayout, StoredEncryption, StoredFormat,
        StoredLayout,
    };
    use crate::structs::storage::blob::{BackendRef, BlobLocationKey};
    use crate::structs::storage::cleanup::ReclaimCandidateKey;
    use crate::structs::storage::encryption::BucketKeyRef;

    #[test]
    fn classes_split_keys() {
        let zstd = EncodingClass::Zstd { level: 3 };
        let raw = BlobLocationKey::new([2; 32], EncodingClass::Raw, BackendRef::node_default());
        let packed = BlobLocationKey::new([2; 32], zstd, BackendRef::node_default());

        assert_ne!(raw.to_bytes(), packed.to_bytes());
        assert_eq!(
            BlobLocationKey::from_bytes(&packed.to_bytes()).unwrap(),
            packed
        );
        let other = BlobLocationKey::new([2; 32], EncodingClass::Zstd { level: 4 }, raw.backend);
        assert_ne!(other.to_bytes(), packed.to_bytes());
        let first = EncodingClass::Pithos { digest: [7; 32] };
        let sealed = BlobLocationKey::new([2; 32], first, BackendRef::node_default());
        assert_eq!(
            BlobLocationKey::from_bytes(&sealed.to_bytes()).unwrap(),
            sealed
        );
        let second = BlobLocationKey::new(
            [2; 32],
            EncodingClass::Pithos { digest: [8; 32] },
            sealed.backend.clone(),
        );
        assert_ne!(second.to_bytes(), sealed.to_bytes());
        for class in [zstd, first] {
            let candidate = ReclaimCandidateKey::new(BackendRef::node_default(), class, [5; 32]);
            assert_eq!(
                ReclaimCandidateKey::from_bytes(&candidate.to_bytes()).unwrap(),
                candidate
            );
        }
    }

    #[test]
    fn levels_are_checked() {
        assert!(Compression::Zstd { level: 0 }.checked().is_err());
        assert!(Compression::Zstd { level: 23 }.checked().is_err());
        assert!(Compression::Zstd { level: 19 }.checked().is_ok());
        assert!(Compression::Off.checked().is_ok());
    }

    #[test]
    fn pairs_are_checked() {
        let key = BucketKeyRef::new(ulid::Ulid::from_bytes([1; 16]), 2);
        let layout = PithosLayout {
            stored_size: 9,
            metadata_digest: [3; 32],
        };
        let sealed = StoredFormat::pithos(layout.clone(), key);
        let bytes = postcard::to_allocvec(&sealed).unwrap();
        assert_eq!(
            postcard::from_bytes::<StoredFormat>(&bytes).unwrap(),
            sealed
        );
        assert_eq!(sealed.bucket_key(), Some(key));

        let unkeyed = StoredFormat {
            layout: StoredLayout::Pithos(Box::new(layout)),
            encryption: StoredEncryption::None,
        };
        let plain_keyed = StoredFormat {
            layout: StoredLayout::Raw,
            encryption: StoredEncryption::Pithos(Box::new(key)),
        };
        for format in [unkeyed, plain_keyed] {
            assert!(postcard::to_allocvec(&format).is_err());
            let parts = FormatParts {
                layout: &format.layout,
                encryption: &format.encryption,
            };
            let bytes = postcard::to_allocvec(&parts).unwrap();
            assert!(postcard::from_bytes::<StoredFormat>(&bytes).is_err());
        }
    }

    #[test]
    fn raw_matches_flags() {
        let flags = postcard::to_allocvec(&(false, false)).unwrap();
        let format = postcard::to_allocvec(&StoredFormat::default()).unwrap();

        assert_eq!(format, flags);
    }
}
