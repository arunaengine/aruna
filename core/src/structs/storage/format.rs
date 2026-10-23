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
    /// Frames of one compressed part in a provider upload; completion adds the seek table.
    FramedPart(Box<PartLayout>),
    /// The original bytes at a tenant path; the location id gives the copy its own location row.
    Plain(ulid::Ulid),
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

/// Frames one part sent to a provider upload, then padding that lifts it to the 5 MiB minimum.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartLayout {
    pub level: u8,
    pub frames: Vec<PartFrame>,
    /// Length of the skippable frame after the frames; zero for none.
    pub padding: u32,
}

/// Stored length, original length and BLAKE3 of the stored bytes of one frame.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartFrame {
    pub stored: u32,
    pub original: u32,
    pub digest: [u8; 32],
}

/// Record of a Pithos copy. The digest covers every directory of the archive, so a changed
/// archive fails when it is opened, before anything is decrypted.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PithosLayout {
    pub stored_size: u64,
    pub metadata_digest: [u8; 32],
    pub storage_generation: u64,
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

    /// The zstd level Pithos applies: its nearest supported level, the lower on a tie.
    pub fn pithos_level(self) -> Option<u8> {
        const PITHOS_ZSTD: [u8; 7] = [1, 4, 8, 11, 15, 18, 22];
        let Self::Zstd { level } = self else {
            return None;
        };
        PITHOS_ZSTD
            .into_iter()
            .min_by_key(|zstd| zstd.abs_diff(level))
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
            (
                StoredLayout::Raw
                | StoredLayout::Frames(_)
                | StoredLayout::FramedPart(_)
                | StoredLayout::Plain(_),
                StoredEncryption::None,
            )
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
            StoredLayout::FramedPart(layout) => EncodingClass::Zstd {
                level: layout.level,
            },
            StoredLayout::Pithos(layout) => EncodingClass::Pithos {
                digest: layout.metadata_digest,
            },
            StoredLayout::Plain(location) => EncodingClass::Plain {
                location: *location,
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
    /// Raw bytes at a tenant path are never shared: each copy has its own location row.
    Plain {
        location: ulid::Ulid,
    },
}

/// Tag of the zstd class in keys; backend keys start with `n` or `g` instead.
const ZSTD_TAG: u8 = b'z';
/// Tag of the Pithos class in keys, followed by the 32-byte metadata digest.
const PITHOS_TAG: u8 = b'p';
/// Tag of the plain class in keys, followed by the 16-byte location id.
const PLAIN_TAG: u8 = b'l';

impl EncodingClass {
    /// Bytes placed between hash and backend in a location key. Raw adds none,
    /// so raw keys keep their shape; backend keys start with `n:` or `g:`.
    pub fn key_bytes(&self) -> Vec<u8> {
        match self {
            Self::Raw => Vec::new(),
            Self::Zstd { level } => vec![ZSTD_TAG, *level],
            Self::Pithos { digest } => [&[PITHOS_TAG], &digest[..]].concat(),
            Self::Plain { location } => [&[PLAIN_TAG], &location.to_bytes()[..]].concat(),
        }
    }

    /// The compression setting this class stores: a plain copy holds raw bytes.
    pub fn setting(self) -> Self {
        match self {
            Self::Plain { .. } => Self::Raw,
            other => other,
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
            [PLAIN_TAG, location @ ..] if location.len() == 16 => Ok(Self::Plain {
                location: ulid::Ulid::from_bytes(location.try_into()?),
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
            Some(&PLAIN_TAG) if bytes.len() > 17 => Ok((
                Self::Plain {
                    location: ulid::Ulid::from_bytes(bytes[1..17].try_into()?),
                },
                &bytes[17..],
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

/// This node's progress re-encoding one bucket's versions to `target`, or moving them.
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
    /// Also moves each version to the storage the bucket's rules choose for a new write now.
    pub moving: bool,
    /// A move handles only keys under this prefix; empty handles the whole bucket.
    pub prefix: String,
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
            moving: false,
            prefix: String::new(),
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Version rows a migration run finished after its first unfinished row, already counted in
/// its progress. The next run of the same pass that resumes at `cursor` passes over them.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MigrationAhead {
    pub started_at_ms: u64,
    pub retries: u32,
    pub cursor: Option<Vec<u8>>,
    pub rows: Vec<Vec<u8>>,
}

impl MigrationAhead {
    /// The rows, only for the pass of `record` that resumes at its cursor.
    pub fn rows_for(self, record: &CompressionMigration) -> Vec<Vec<u8>> {
        let same = (self.started_at_ms, self.retries) == (record.started_at_ms, record.retries);
        match same && self.cursor == record.cursor {
            true => self.rows,
            false => Vec::new(),
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
        // Plain copies of the same bytes on one backend never share a row.
        let plain = |seed| EncodingClass::Plain {
            location: ulid::Ulid::from_parts(seed, seed.into()),
        };
        let mine = BlobLocationKey::new([2; 32], plain(1), BackendRef::node_default());
        let theirs = BlobLocationKey::new([2; 32], plain(2), BackendRef::node_default());
        assert_ne!(mine.to_bytes(), theirs.to_bytes());
        assert_eq!(BlobLocationKey::from_bytes(&mine.to_bytes()).unwrap(), mine);
        assert_eq!(plain(1).setting(), EncodingClass::Raw);
        for class in [zstd, first, plain(3)] {
            let candidate = ReclaimCandidateKey::new(BackendRef::node_default(), class, [5; 32]);
            assert_eq!(
                ReclaimCandidateKey::from_bytes(&candidate.to_bytes()).unwrap(),
                candidate
            );
        }
    }

    #[test]
    fn pithos_nearest_level() {
        let effective = |level| Compression::Zstd { level }.pithos_level();
        assert_eq!(effective(3), Some(4));
        assert_eq!(effective(6), Some(4));
        assert_eq!(effective(13), Some(11));
        assert_eq!(effective(22), Some(22));
        assert_eq!(Compression::Off.pithos_level(), None);
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
            storage_generation: 4,
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
