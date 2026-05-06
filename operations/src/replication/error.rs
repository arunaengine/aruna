use aruna_core::errors::ConversionError;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum ReplicationError {
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("The specified key does not exist")]
    NoSuchKey,
    #[error("Connection to receiving node is missing")]
    ConnectionMissing,
    #[error("Blob is missing blake3 hash")]
    HashMissing,
    #[error("Integrity check failed: {0}")]
    IntegrityCheckFailed(String),
    #[error("Transaction id missing")]
    TransactionMissing,
    #[error("Replication rejected: {0}")]
    ReplicationRejected(String),
    #[error("Replication failed")]
    ReplicationFailed,
}
