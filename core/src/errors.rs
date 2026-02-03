use thiserror::Error;

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
}

#[derive(Debug, Error)]
pub enum ConversionError {
    #[error(transparent)]
    PostcardError(#[from] postcard::Error)
}

#[derive(Debug, Error)]
pub enum ParseRealmIdError {
    #[error("Parsing error {0}")]
    ParsingError(String),
}
