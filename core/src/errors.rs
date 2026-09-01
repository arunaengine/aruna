use crate::structs::{BackendLocation, SourceConnectorKind};
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
    #[error(transparent)]
    RestrictionLimit(#[from] crate::permission_path::RestrictionLimitError),
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
#[allow(clippy::large_enum_variant)]
pub enum BlobError {
    #[error("Channel closed")]
    ChannelClosed,
    #[error("Cleanup is unsupported by the backend")]
    CleanupUnsupported,
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
    /// A stored record names a backend this node does not have registered.
    #[error("unknown storage backend {0}")]
    UnknownBackend(String),
    #[error("Outboard creation failed: {0}")]
    OutboardCreationFailed(String),
    #[error("Failed to open connection: {0}")]
    ConnectionFailed(String),
    /// Server-side write fault: backend writer, close, or operator failure.
    #[error("Write error: {0}")]
    WriteError(String),
    /// Client-sourced body stream fault; the request, not the node, is at fault.
    #[error("Stream failed: {0}")]
    StreamFailed(String),
    /// The output location must be handed to durable cleanup before releasing capacity.
    #[error("Write cleanup failed for {location:?}: {message}")]
    WriteCleanup {
        location: BackendLocation,
        message: String,
    },
    #[error("Blob exceeds the {limit} byte limit")]
    SizeLimitExceeded { limit: u64 },
    #[error("Read error: {0}")]
    ReadError(String),
    #[error("List error: {0}")]
    ListError(String),
    #[error("Delete error: {0}")]
    DeleteError(String),
    #[error("Integrity check failed: {0}")]
    IntegrityCheckFailed(String),
    #[error("Replication rejected: {0}")]
    ReplicationRejected(String),
    #[error("Replication failed: {0}")]
    ReplicationFailed(String),
    #[error("Blob storage is sealed for shutdown")]
    Sealed,
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
    #[error("Access denied")]
    AccessDenied,
    #[error("Unsupported staging source kind `{0}`")]
    UnsupportedKind(String),
    #[error("Operator creation failed: {0}")]
    OperatorCreationFailed(String),
    #[error("Check error: {0}")]
    CheckError(String),
    #[error("Stat error: {0}")]
    StatError(String),
    #[error("List error: {0}")]
    ListError(String),
    #[error("Read error: {0}")]
    ReadError(String),
    /// The source changed while it was being read, so the bytes are not one
    /// representation. Retryable, and never an identity.
    #[error("Source changed during the read")]
    SourceUnstable,
    #[error(transparent)]
    EgressDenied(#[from] crate::egress::EgressError),
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

#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum StorageError {
    #[error("Key not found")]
    KeyNotFound,
    #[error("Transaction conflict")]
    TransactionConflict,
    /// A commit that neither succeeded nor was refused. Its writes may still
    /// become durable, so a caller must not undo what the commit would own.
    #[error("Commit failed")]
    CommitFailed,
    #[error("Transaction not found")]
    TransactionNotFound,
    /// Transaction-cleanup tracking is full, so the effect was never enqueued.
    /// Distinct from a conflict: re-running the same effect only spins.
    #[error("Transaction cleanup capacity exhausted")]
    CleanupCapacity,
    #[error("Keyspace error: {0}")]
    KeyspaceError(String),
    #[error("Read error: {0}")]
    ReadError(String),
    #[error("Write error: {0}")]
    WriteError(String),
    #[error("Delete error")]
    DeleteError,
    #[error("Persist error: {0}")]
    PersistError(String),
    #[error("Channel closed")]
    ChannelClosed,
    #[error("Queue full")]
    QueueFull,
    #[error("Timeout")]
    Timeout,
    #[error("Invalid effect type")]
    InvalidEffect,
    #[error("Storage is sealed for shutdown")]
    Sealed,
}

impl StorageError {
    /// Whether a failed `CommitTransaction` proves the transaction's writes
    /// were discarded: refused by the conflict check, or never handed to the
    /// storage actor at all. Every other failure leaves the commit either
    /// already applied or unknown, so its records may exist.
    pub fn proves_no_commit(&self) -> bool {
        matches!(
            self,
            Self::TransactionConflict | Self::QueueFull | Self::CleanupCapacity
        )
    }
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
    #[error("Invalid session claim")]
    InvalidSessionClaim,
    #[error(transparent)]
    PostcardError(#[from] postcard::Error),
    #[error(transparent)]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error("Failed to convert from str: {0}")]
    FromStrError(String),
    #[error("Failed to convert OsString to String")]
    OsStringError,
    #[error("Unsafe path: {0}")]
    UnsafePath(String),
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
    #[error("RO-Crate conversion error: {0}")]
    RoCrateError(String),
    #[error(transparent)]
    PlacementPolicyError(#[from] crate::structs::PlacementPolicyError),
    #[error(transparent)]
    AdvertisementError(#[from] crate::compute::AdvertisementError),
    #[error("policy refs must be sorted and deduplicated")]
    NonCanonicalPolicyRefs,
    /// A monotonic head generation must never wrap: a wrapped pointer would
    /// compare equal to an older one and silently win the convergent order.
    #[error("object head generation is exhausted")]
    HeadGenerationExhausted,
}

impl PartialEq for ConversionError {
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
    }
}
