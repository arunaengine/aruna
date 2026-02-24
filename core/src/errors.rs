use std::array::TryFromSliceError;

use automerge::AutomergeError;
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
    UlidDecodeError(#[from] ulid::DecodeError),
    #[error(transparent)]
    Base64DecodeError(#[from] base64::DecodeError),
    #[error("`{0}`")]
    InvalidLength(String),
    #[error("Invalid UserId")]
    InvalidUserId,
    #[error(transparent)]
    PostcardError(#[from] postcard::Error),
    #[error(transparent)]
    PublicKeyError(#[from] ed25519_dalek::ed25519::Error),
    #[error(transparent)]
    FromSliceError(#[from] TryFromSliceError),
    #[error(transparent)]
    PublicKeyConversionError(#[from] ed25519_dalek::pkcs8::spki::Error),
    #[error(transparent)]
    PrivateKeyConversionError(#[from] ed25519_dalek::pkcs8::Error),
    #[error("Invalid string `{0}` for Operation")]
    InvalidOperationConversion(String),
    #[error(transparent)]
    ReconcileError(#[from] autosurgeon::ReconcileError),
    #[error(transparent)]
    HydrateError(#[from] autosurgeon::HydrateError),
    #[error(transparent)]
    AutomergeError(#[from] AutomergeError),
}
