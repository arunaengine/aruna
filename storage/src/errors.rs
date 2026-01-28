use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageLibError {
    #[error(transparent)]
    FjallError(#[from] fjall::Error),
    #[error("Crossfire error: {0}")]
    CrossfireRecvError(#[from] crossfire::RecvError),
    #[error("Crossfire send error")]
    CrossfireSendError,
}
