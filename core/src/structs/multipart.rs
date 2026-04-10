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
        prefix.truncate(prefix.len().saturating_sub(std::mem::size_of::<u16>()));
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
