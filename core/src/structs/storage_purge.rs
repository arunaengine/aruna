use crate::NodeId;
use crate::errors::ConversionError;
use crate::structs::{AuthContext, JobId};
use crate::types::GroupId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoragePurgeScope {
    File { bucket: String, key: String },
    Prefix { bucket: String, prefix: String },
    Bucket { bucket: String },
}

impl StoragePurgeScope {
    pub fn bucket(&self) -> &str {
        match self {
            Self::File { bucket, .. } | Self::Prefix { bucket, .. } | Self::Bucket { bucket } => {
                bucket
            }
        }
    }

    pub fn matches_key(&self, bucket: &str, key: &str) -> bool {
        if self.bucket() != bucket {
            return false;
        }
        match self {
            Self::File { key: target, .. } => key == target,
            Self::Prefix { prefix, .. } => key.starts_with(prefix),
            Self::Bucket { .. } => true,
        }
    }

    pub fn list_prefix(&self) -> Option<&str> {
        match self {
            Self::File { key, .. } => Some(key),
            Self::Prefix { prefix, .. } => Some(prefix),
            Self::Bucket { .. } => None,
        }
    }

    pub fn is_bucket(&self) -> bool {
        matches!(self, Self::Bucket { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePurgeSpec {
    pub scope: StoragePurgeScope,
    pub group_id: GroupId,
    pub auth_context: AuthContext,
    pub node_id: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePurgeResult {
    pub scope: StoragePurgeScope,
    pub versions_removed: u64,
    pub multipart_uploads_removed: u64,
    pub batches_completed: u64,
    pub bucket_deleted: bool,
    pub emptiness_proven: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePurgeCheckpoint {
    pub initial_versions: u64,
    pub initial_multipart_uploads: u64,
    pub batches_completed: u64,
}

impl StoragePurgeCheckpoint {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePurgeFence {
    pub job_id: JobId,
    pub scope: StoragePurgeScope,
}

impl StoragePurgeFence {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}
