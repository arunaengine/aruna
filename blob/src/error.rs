use thiserror::Error;

#[derive(Debug, Error)]
pub enum BlobLibError {
    #[error("Conversion failed: {0}")]
    ConversionError(String),
    #[error(transparent)]
    OpenDalError(#[from] opendal::Error),
}
