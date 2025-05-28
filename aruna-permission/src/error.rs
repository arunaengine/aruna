use casbin::error::AdapterError;
use thiserror::Error;
use tracing::error;

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
    PathError(#[from] crate::paths::PathError),
    #[error("Postcard error: {0}")]
    PostcardError(#[from] postcard::Error),
    #[error("Aruna permission handler error: {0}")]
    ArunaPermissionHandlerError(#[from] ArunaPermissionHandlerError),
}

pub type Result<T> = std::result::Result<T, PermissionError>;
