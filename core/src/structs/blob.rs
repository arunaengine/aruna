use crate::errors::{BlobError, ConversionError};
use crate::structs::RealmId;
use crate::types::UserId;
use byteview::ByteView;
use core::fmt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::SystemTime;
use ulid::Ulid;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum Backend {
    #[default]
    S3,
    HTTP,
    Postgres,
    FileSystem,
}

impl FromStr for Backend {
    type Err = ConversionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "s3" => Ok(Backend::S3),
            "http" => Ok(Backend::HTTP),
            "postgres" => Ok(Backend::Postgres),
            "filesystem" => Ok(Backend::FileSystem),
            _ => Err(ConversionError::FromStrError(format!(
                "unknown backend {}",
                s
            ))),
        }
    }
}

impl Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Backend::S3 => write!(f, "s3"),
            Backend::HTTP => write!(f, "http"),
            Backend::Postgres => write!(f, "postgres"),
            Backend::FileSystem => write!(f, "filesystem"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BackendConfig {
    pub backend_type: Backend,
    pub root: String,
    pub service_config: HashMap<String, String>,
    pub bucket_prefix: Option<String>,
    pub max_bucket_size: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct BackendBucket {
    pub name: String,
    pub load: u64,
}

impl TryFrom<(ByteView, ByteView)> for BackendBucket {
    type Error = ConversionError;

    fn try_from(value: (ByteView, ByteView)) -> Result<Self, Self::Error> {
        let (bucket, load) = value;

        Ok(BackendBucket {
            name: String::from_utf8(bucket.to_vec())?,
            load: u64::from_le_bytes(load.as_ref().try_into()?),
        })
    }
}

impl From<(String, u64)> for BackendBucket {
    fn from((name, size): (String, u64)) -> Self {
        Self { name, load: size }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BackendLocation {
    pub root: String,
    pub storage_bucket: String,
    pub object_bucket: String, // S3 bucket from request
    pub object_key: String,    // S3 key from request
    pub ulid: Ulid,
    pub compressed: bool,
    pub encrypted: bool,
}

impl Display for BackendLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path = PathBuf::from(&self.root)
            .join(&self.storage_bucket)
            .join(&self.object_bucket)
            .join(format!("{}_{}", self.object_key, self.ulid));
        write!(
            f,
            "{}",
            path.into_os_string()
                .into_string()
                .map_err(|_| fmt::Error)?
        )
    }
}

impl BackendLocation {
    pub fn get_full_path(&self) -> Result<String, ConversionError> {
        PathBuf::from(&self.root)
            .join(&self.storage_bucket)
            .join(&self.object_bucket)
            .join(format!("{}_{}", self.object_key, self.ulid))
            .into_os_string()
            .into_string()
            .map_err(|_| ConversionError::OsStringError)
    }

    pub fn get_storage_path(&self) -> Result<String, BlobError> {
        Ok(PathBuf::from(&self.storage_bucket)
            .join(&self.object_bucket)
            .join(format!("{}_{}", self.object_key, self.ulid))
            .into_os_string()
            .into_string()
            .map_err(|_| ConversionError::OsStringError)?)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum NegotiationResult {
    Accepted(Ulid),
    Rejected(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BlobInfo {
    pub location: BackendLocation,
    pub created_by: UserId,
    pub created_at: SystemTime,
    pub staging: bool,
    pub partial: bool,
    pub blob_size: u64,
    pub hashes: HashMap<String, Vec<u8>>, // Raw bytes that can be encoded as needed
}

impl BlobInfo {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn get_location(&self) -> &BackendLocation {
        &self.location
    }

    pub fn get_blake3(&self) -> Option<&Vec<u8>> {
        self.hashes.get("blake3")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserIdentity {
    pub user_id: UserId,
    pub realm_key: RealmId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserAccess {
    pub user_id: UserIdentity,
    pub group_id: Ulid,
    pub secret: String,
    //pub expiry: SystemTime
    //filter: todo!()
}

impl UserAccess {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}
