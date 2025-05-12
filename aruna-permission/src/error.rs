use casbin::error::AdapterError;
use thiserror::Error;
use tracing::error;

#[derive(Error, Debug)]
pub enum ArunaPermissionHandlerError {
    #[error("Failed to convert permission: {0}")]
    ConversionError(#[from] postcard::Error),
    #[error("Storage error: {0}")]
    StorageError(#[from] aruna_storage::error::ArunaStorageError),
}

impl From<ArunaPermissionHandlerError> for casbin::Error {
    fn from(err: ArunaPermissionHandlerError) -> Self {
        error!("Error converting to casbin error: {}", err);
        casbin::Error::AdapterError(AdapterError(Box::new(err)))
    }
}
