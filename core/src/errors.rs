use crate::structs::SourceConnectorKind;
use automerge::AutomergeError;
use std::array::TryFromSliceError;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum AuthorizationError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    GlobError(#[from] globset::Error),
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("Invalid realm id")]
    InvalidRealmId,
    #[error("Invalid group id")]
    InvalidGroupId,
    #[error("No group found")]
    GroupNotFound,
    #[error("Authorization document not found")]
    AuthDocNotFound,
    #[error("Creating Group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

#[derive(Debug, Error, PartialEq)]
pub enum BlobError {
    #[error("Channel closed")]
    ChannelClosed,
    #[error("Failed to send message")]
    SendError,
    #[error("Invalid effect type")]
    InvalidEffect,
    #[error("Blob handle missing")]
    HandleMissing,
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("failed to create bucket: {0}")]
    MakeBucketError(String),
    #[error("Operator creation failed: {0}")]
    OperatorCreationFailed(String),
    #[error("Outboard creation failed: {0}")]
    OutboardCreationFailed(String),
    #[error("Failed to open connection: {0}")]
    ConnectionFailed(String),
    #[error("Write error: {0}")]
    WriteError(String),
    #[error("Read error: {0}")]
    ReadError(String),
    #[error("Delete error: {0}")]
    DeleteError(String),
    #[error("Integrity check failed: {0}")]
    IntegrityCheckFailed(String),
    #[error("Replication rejected: {0}")]
    ReplicationRejected(String),
    #[error("Replication failed: {0}")]
    ReplicationFailed(String),
}

#[derive(Debug, Error, PartialEq)]
pub enum StagingSourceError {
    #[error("Channel closed")]
    ChannelClosed,
    #[error("Invalid effect type")]
    InvalidEffect,
    #[error("Staging source handle missing")]
    HandleMissing,
    #[error("Staging source not found")]
    NotFound,
    #[error("Unsupported staging source kind `{0}`")]
    UnsupportedKind(String),
    #[error("Operator creation failed: {0}")]
    OperatorCreationFailed(String),
    #[error("Stat error: {0}")]
    StatError(String),
    #[error("Read error: {0}")]
    ReadError(String),
}

#[derive(Debug, Error, PartialEq)]
pub enum SourceConnectorResolutionError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Connector not found")]
    NotFound,
    #[error("Connector kind `{0}` is not supported in Phase 3")]
    UnsupportedConnectorKind(SourceConnectorKind),
    #[error("Source path must be relative to connector root")]
    InvalidSourcePath,
    #[error("Source connector resolution failed")]
    ResolveFailed,
}

#[derive(Debug, Error, PartialEq)]
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

#[derive(Debug, Error, PartialEq)]
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

#[derive(Debug, Error, PartialEq)]
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

#[derive(Debug, Error, PartialEq)]
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
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error("Failed to convert from str: {0}")]
    FromStrError(String),
    #[error("Failed to convert OsString to String")]
    OsStringError,
    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),
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
    #[error("RO-Crate conversion error: {0}")]
    RoCrateError(String),
}

impl PartialEq for ConversionError {
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
    }
}
