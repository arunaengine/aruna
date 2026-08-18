use aruna_core::errors::ConversionError;
use aruna_core::storage_format::StorageFormatError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageLibError {
    #[error(transparent)]
    FjallError(#[from] fjall::Error),
    #[error(transparent)]
    Format(#[from] StorageFormatError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("Crossfire error: {0}")]
    CrossfireRecvError(#[from] crossfire::RecvError),
    #[error("Crossfire send error")]
    CrossfireSendError,
}
