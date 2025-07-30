use std::sync::Arc;

use aruna_storage::error::ArunaStorageError;
use tokio::sync::Notify;

use crate::Task;

#[derive(thiserror::Error, Debug)]
pub enum ArunaTaskError {
    #[error("Storage error: {0}")]
    StorageError(#[from] ArunaStorageError),
    #[error("Tokio join error: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),
    #[error("Tokio sender error: {0}")]
    SenderError(#[from] tokio::sync::mpsc::error::SendError<(Task, Option<Arc<Notify>>)>),
    #[error("Postcard error: {0}")]
    PostcardError(#[from] postcard::Error),
    #[error("Execution error: {0}")]
    ExecutionError(String),
}
