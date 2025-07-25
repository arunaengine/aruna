use aruna_storage::error::ArunaStorageError;

#[derive(thiserror::Error, Debug)]
pub enum ArunaTaskError {
    #[error("Storage error: {0}")]
    StorageError(#[from] ArunaStorageError),
    #[error("Tokio join error: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),
    #[error("Postcard error: {0}")]
    PostcardError(#[from] postcard::Error),
    #[error("Execution error: {0}")]
    ExecutionError(String),
}
