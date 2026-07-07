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
        // part_number is postcard varint-encoded, so a zero part number occupies
        // a single byte, not size_of::<u16>(). Strip exactly what it contributes
        // to keep the full version_id in the prefix.
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
    pub composite_hashes: HashMap<String, Vec<u8>>,
}

impl MultipartObjectSummary {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
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
    use super::MultipartObjectMetadataKey;
    use ulid::Ulid;

    #[test]
    fn part_prefix_covers_full_version_id_and_excludes_summary() {
        let version_id = Ulid::new();
        let prefix = MultipartObjectMetadataKey::part_prefix(version_id).unwrap();

        // The prefix must be the part key with only the zero part number stripped.
        let zero_part = MultipartObjectMetadataKey::part(version_id, 0)
            .to_bytes()
            .unwrap();
        assert_eq!(prefix.as_slice(), &zero_part[..zero_part.len() - 1]);

        // Every part key for this version shares the prefix, across varint widths.
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
        let other_key = MultipartObjectMetadataKey::part(Ulid::new(), 0)
            .to_bytes()
            .unwrap();
        assert!(!other_key.starts_with(&prefix));
    }
}
