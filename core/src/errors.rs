use thiserror::Error;

#[derive(Debug, Error)]
pub enum BlobError {
    #[error("Channel closed")]
    ChannelClosed,
    #[error(transparent)]
    OpenDalError(#[from] opendal::Error),
    #[error("Operator creation failed")]
    OperatorCreationFailed,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Key not found")]
    KeyNotFound,
    #[error("Transaction conflict")]
    TransactionConflict,
    #[error("Transaction not found")]
    TransactionNotFound,
    #[error("Keyspace error")]
    KeyspaceError,
    #[error("Read error")]
    ReadError,
    #[error("Write error")]
    WriteError,
    #[error("Delete error")]
    DeleteError,
    #[error("Channel closed")]
    ChannelClosed,
    #[error("Invalid effect type")]
    InvalidEffect,
}

#[derive(Debug, Error)]
pub enum DhtError {
    #[error("Key not found")]
    KeyNotFound,
    #[error("Store failed: {0}")]
    StoreFailed(String),
    #[error("Storage full")]
    StorageFull,
    #[error("Other: {0}")]
    Other(String),
}

#[derive(Debug, Error)]
pub enum GossipError {
    #[error("Already subscribed")]
    AlreadySubscribed,
    #[error("Not subscribed")]
    NotSubscribed,
    #[error("Broadcast failed: {0}")]
    BroadcastFailed(String),
    #[error("Other: {0}")]
    Other(String),
}

#[derive(Debug, Error)]
pub enum StreamError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    #[error("Stream closed")]
    StreamClosed,
    #[error("Other: {0}")]
    Other(String),
}

#[derive(Debug, Error)]
pub enum ConversionError {
    #[error(transparent)]
    PostcardError(#[from] postcard::Error),
}

#[derive(Debug, Error)]
pub enum ParseRealmIdError {
    #[error("Parsing error {0}")]
    ParsingError(String),
}
