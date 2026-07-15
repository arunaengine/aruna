use crate::errors::ConversionError;
use crate::structs::BackendLocation;
use crate::structs::checksum::{ChecksumAlgorithm, HASH_MD5};
use crate::types::{GroupId, UserId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MultipartChecksumType {
    FullObject,
    Composite,
}

impl MultipartChecksumType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FullObject => "FULL_OBJECT",
            Self::Composite => "COMPOSITE",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MultipartUploadChecksumHint {
    pub algorithm: Option<ChecksumAlgorithm>,
    pub checksum_type: MultipartChecksumType,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MultipartUploadStatus {
    Open,
    Completing,
    Aborting,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MultipartUpload {
    pub upload_id: Ulid,
    pub bucket: String,
    pub key: String,
    pub group_id: GroupId,
    pub created_by: UserId,
    pub created_at: SystemTime,
    pub status: MultipartUploadStatus,
    pub checksum_hint: Option<MultipartUploadChecksumHint>,
}

impl MultipartUpload {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MultipartUploadPartKey {
    pub upload_id: Ulid,
    pub part_number: u16,
}

impl MultipartUploadPartKey {
    pub fn new(upload_id: Ulid, part_number: u16) -> Self {
        Self {
            upload_id,
            part_number,
        }
    }

    pub fn prefix(upload_id: Ulid) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&upload_id)?)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MultipartUploadPart {
    pub part_number: u16,
    pub location: BackendLocation,
    pub created_at: SystemTime,
}

impl MultipartUploadPart {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn etag(&self) -> Option<&[u8]> {
        self.location.hashes.get(HASH_MD5).map(Vec::as_slice)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MultipartObjectMetadataKey {
    Summary { version_id: Ulid },
    Part { version_id: Ulid, part_number: u16 },
}

impl MultipartObjectMetadataKey {
    pub fn summary(version_id: Ulid) -> Self {
        Self::Summary { version_id }
    }

    pub fn part(version_id: Ulid, part_number: u16) -> Self {
        Self::Part {
            version_id,
            part_number,
        }
    }

    pub fn part_prefix(version_id: Ulid) -> Result<Vec<u8>, ConversionError> {
        let mut prefix = Self::Part {
            version_id,
            part_number: 0,
        }
        .to_bytes()?;
        let part_number_len = postcard::to_allocvec(&0u16)?.len();
        prefix.truncate(prefix.len().saturating_sub(part_number_len));
        Ok(prefix)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MultipartObjectSummary {
    pub checksum_type: MultipartChecksumType,
    pub part_count: usize,
    #[serde(skip)]
    pub composite_hashes: HashMap<String, Vec<u8>>,
}

impl MultipartObjectSummary {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        #[derive(Serialize)]
        struct StoredSummary<'a> {
            checksum_type: MultipartChecksumType,
            part_count: usize,
            composite_hashes: &'a HashMap<String, Vec<u8>>,
        }

        Ok(postcard::to_allocvec(&StoredSummary {
            checksum_type: self.checksum_type,
            part_count: self.part_count,
            composite_hashes: &self.composite_hashes,
        })?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        #[derive(Deserialize)]
        struct StoredSummary {
            checksum_type: MultipartChecksumType,
            part_count: usize,
            composite_hashes: HashMap<String, Vec<u8>>,
        }

        match postcard::from_bytes::<StoredSummary>(bytes) {
            Ok(summary) => Ok(Self {
                checksum_type: summary.checksum_type,
                part_count: summary.part_count,
                composite_hashes: summary.composite_hashes,
            }),
            Err(_) => Ok(postcard::from_bytes(bytes)?),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MultipartObjectPart {
    pub part_number: u16,
    pub size: u64,
    pub hashes: HashMap<String, Vec<u8>>,
}

impl MultipartObjectPart {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[cfg(test)]
mod test {
    use super::{MultipartChecksumType, MultipartObjectMetadataKey, MultipartObjectSummary};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use ulid::Ulid;

    // Prefix keeps the whole version id, spans all part numbers, excludes the summary.
    #[test]
    fn prefix_covers_version() {
        let version_id = Ulid::r#gen();
        let prefix = MultipartObjectMetadataKey::part_prefix(version_id).unwrap();

        // The prefix must be the part key with the fixed part-number suffix stripped.
        let zero_part = MultipartObjectMetadataKey::part(version_id, 0)
            .to_bytes()
            .unwrap();
        assert_eq!(prefix.as_slice(), &zero_part[..zero_part.len() - 1]);

        // Every part key for this version shares the prefix, across part numbers.
        for part_number in [0u16, 1, 127, 128, 255, 256, 65535] {
            let part_key = MultipartObjectMetadataKey::part(version_id, part_number)
                .to_bytes()
                .unwrap();
            assert!(
                part_key.starts_with(&prefix),
                "part {part_number} key does not start with prefix",
            );
        }

        // The summary key must not be captured by the part prefix.
        let summary_key = MultipartObjectMetadataKey::summary(version_id)
            .to_bytes()
            .unwrap();
        assert!(!summary_key.starts_with(&prefix));

        // A different version must not be captured by this version's prefix.
        let other_key = MultipartObjectMetadataKey::part(Ulid::r#gen(), 0)
            .to_bytes()
            .unwrap();
        assert!(!other_key.starts_with(&prefix));
    }

    // Two versions differing only in the last ULID byte must not share a part scan.
    #[test]
    fn prefix_isolates_versions() {
        let mut first_bytes = [7u8; 16];
        let mut second_bytes = [7u8; 16];
        first_bytes[15] = 0xAA;
        second_bytes[15] = 0xAB;
        let first = Ulid::from_bytes(first_bytes);
        let second = Ulid::from_bytes(second_bytes);

        let first_prefix = MultipartObjectMetadataKey::part_prefix(first).unwrap();
        let second_prefix = MultipartObjectMetadataKey::part_prefix(second).unwrap();
        assert_ne!(first_prefix, second_prefix);

        let second_part = MultipartObjectMetadataKey::part(second, 1)
            .to_bytes()
            .unwrap();
        assert!(!second_part.starts_with(&first_prefix));
        let second_summary = MultipartObjectMetadataKey::summary(second)
            .to_bytes()
            .unwrap();
        assert!(!second_summary.starts_with(&first_prefix));
    }

    #[test]
    fn prefix_spans_parts() {
        let version_id = Ulid::from_bytes([3u8; 16]);
        let prefix = MultipartObjectMetadataKey::part_prefix(version_id).unwrap();
        for part_number in [0u16, 1, 127, 128, 255, 256, 65535] {
            let key = MultipartObjectMetadataKey::part(version_id, part_number)
                .to_bytes()
                .unwrap();
            assert!(
                key.starts_with(&prefix),
                "part {part_number} missing prefix"
            );
        }
        let summary = MultipartObjectMetadataKey::summary(version_id)
            .to_bytes()
            .unwrap();
        assert!(!summary.starts_with(&prefix));
    }

    #[test]
    fn key_round_trips() {
        let version_id = Ulid::from_bytes([9u8; 16]);
        let summary = MultipartObjectMetadataKey::summary(version_id);
        assert_eq!(
            MultipartObjectMetadataKey::from_bytes(&summary.to_bytes().unwrap()).unwrap(),
            summary
        );
        for part_number in [0u16, 1, 65535] {
            let part = MultipartObjectMetadataKey::part(version_id, part_number);
            assert_eq!(
                MultipartObjectMetadataKey::from_bytes(&part.to_bytes().unwrap()).unwrap(),
                part
            );
        }
        assert!(MultipartObjectMetadataKey::from_bytes(&[]).is_err());
        assert!(MultipartObjectMetadataKey::from_bytes(&[9u8; 5]).is_err());
    }

    #[test]
    fn keys_keep_legacy() {
        let version_id = Ulid::from_bytes([9u8; 16]);
        for key in [
            MultipartObjectMetadataKey::summary(version_id),
            MultipartObjectMetadataKey::part(version_id, 256),
        ] {
            let legacy_bytes = postcard::to_allocvec(&key).unwrap();
            assert_eq!(key.to_bytes().unwrap(), legacy_bytes);
            assert_eq!(
                MultipartObjectMetadataKey::from_bytes(&legacy_bytes).unwrap(),
                key
            );
        }
    }

    #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
    struct LegacySummary {
        checksum_type: MultipartChecksumType,
        part_count: usize,
    }

    #[test]
    fn decodes_legacy_summary() {
        let legacy = LegacySummary {
            checksum_type: MultipartChecksumType::Composite,
            part_count: 3,
        };

        assert_eq!(
            MultipartObjectSummary::from_bytes(&postcard::to_allocvec(&legacy).unwrap()).unwrap(),
            MultipartObjectSummary {
                checksum_type: MultipartChecksumType::Composite,
                part_count: 3,
                composite_hashes: HashMap::new(),
            }
        );
    }

    #[test]
    fn retains_summary_hashes() {
        let summary = MultipartObjectSummary {
            checksum_type: MultipartChecksumType::Composite,
            part_count: 2,
            composite_hashes: HashMap::from([("sha256".to_string(), vec![7u8; 32])]),
        };
        let legacy = LegacySummary {
            checksum_type: summary.checksum_type,
            part_count: summary.part_count,
        };

        assert_eq!(
            postcard::to_allocvec(&summary).unwrap(),
            postcard::to_allocvec(&legacy).unwrap()
        );
        assert_eq!(
            postcard::from_bytes::<LegacySummary>(&postcard::to_allocvec(&summary).unwrap())
                .unwrap(),
            legacy
        );
        assert_eq!(
            MultipartObjectSummary::from_bytes(&summary.to_bytes().unwrap()).unwrap(),
            summary
        );
    }
}
