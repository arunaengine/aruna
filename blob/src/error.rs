use thiserror::Error;

#[derive(Debug, Error)]
pub enum BlobLibError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    PostcardError(#[from] postcard::Error),
    #[error(transparent)]
    OpenDalError(#[from] opendal::Error),
    #[error(transparent)]
    IrohWriteError(#[from] iroh_quinn::WriteError),
    #[error(transparent)]
    IrohReadExactError(#[from] iroh_quinn::ReadExactError),
}
