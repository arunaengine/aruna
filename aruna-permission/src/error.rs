use std::array::TryFromSliceError;

use casbin::error::AdapterError;
use thiserror::Error;
use tracing::error;

/// Custom error type for path operations.
#[derive(Error, Debug)]
pub enum PathError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Invalid ULID: {0}")]
    InvalidUlid(#[from] ulid::DecodeError),

    #[error("Invalid Blake3 hash: {0}")]
    InvalidHash(String),

    #[error("Invalid base64: {0}")]
    InvalidBase64(#[from] base64::DecodeError),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Building error: {0}")]
    BuildError(String),

    #[error("Invalid assumption: {0}")]
    InvalidAssumption(String),
}

/// Error types for unification operations
#[derive(Error, Debug)]
pub enum UnificationError {
    #[error("Identity not found: {0}")]
    IdentityNotFound(String),
    #[error("Identities already unified")]
    AlreadyUnified,
    #[error("Cannot unify identity with itself")]
    SelfUnification,
}


/// Error types for unification operations
#[derive(Error, Debug)]
pub enum ConversionError {
    #[error("Invalid size for slice: {0}")]
    InvalidSliceSize(#[from] TryFromSliceError),
    #[error("Invalid format: {0}")]
    InvalidFormat(String),
    #[error("Invalid ulid: {0}")]
    InvalidUlid(#[from] ulid::DecodeError),
    #[error("Invalid realm_key: {0}")]
    InvalidRealmKey(#[from] hex::FromHexError),
}


#[derive(Error, Debug)]
pub enum ArunaPermissionHandlerError {
    #[error("Failed to convert permission: {0}")]
    ConversionError(#[from] postcard::Error),
    #[error("Storage error: {0}")]
    StorageError(#[from] aruna_storage::error::ArunaStorageError),
    #[error("Casbin helper error: {0}")]
    CasbinHelperError(String),
}

impl From<ArunaPermissionHandlerError> for casbin::Error {
    fn from(err: ArunaPermissionHandlerError) -> Self {
        error!("Error converting to casbin error: {}", err);
        casbin::Error::AdapterError(AdapterError(Box::new(err)))
    }
}

#[derive(Error, Debug)]
pub enum PermissionError {
    #[error("Permission denied for resource")]
    PermissionDenied,
    #[error("Resource not found: {0}")]
    ResourceNotFound(String),
    #[error("Role not found: {0}")]
    RoleNotFound(String),
    #[error("Storage error: {0}")]
    StorageError(#[from] aruna_storage::error::ArunaStorageError),
    #[error("Casbin error: {0}")]
    CasbinError(#[from] casbin::Error),
    #[error("Path error: {0}")]
    PathError(#[from] PathError),
    #[error("Postcard error: {0}")]
    PostcardError(#[from] postcard::Error),
    #[error("Aruna permission handler error: {0}")]
    ArunaPermissionHandlerError(#[from] ArunaPermissionHandlerError),
    #[error("Unification error: {0}")]
    UnificationError(#[from] UnificationError),
    #[error("Conversion error: {0}")]
    ConversionError(#[from] ConversionError),
}

pub type Result<T> = std::result::Result<T, PermissionError>;
