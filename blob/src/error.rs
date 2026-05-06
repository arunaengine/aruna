use thiserror::Error;

#[derive(Debug, Error)]
pub enum BlobLibError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    PostcardError(#[from] postcard::Error),
    #[error(transparent)]
    OpenDalError(#[from] opendal::Error),
}
