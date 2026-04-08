use crate::explorer::ExplorerError;
use crate::storage::SnapshotError;
use aruna::config::SetupError;
use aruna_operations::create_token::CreateTokenError;
use aruna_storage::errors::StorageLibError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CliError {
    #[error(transparent)]
    UlidConversion(#[from] ulid::DecodeError),
    #[error(transparent)]
    CreateTokenError(#[from] CreateTokenError),
    #[error(transparent)]
    JwtTokenError(#[from] jsonwebtoken::errors::Error),
    #[error(transparent)]
    Base64DecodeError(#[from] base64::DecodeError),
    #[error("Cannot convert Vec into Slice")]
    IntoSliceError,
    #[error(transparent)]
    Ed25519Error(#[from] ed25519_dalek::ed25519::Error),
    #[error(transparent)]
    PCKSError(#[from] ed25519_dalek::pkcs8::Error),
    #[error(transparent)]
    SPKIError(#[from] ed25519_dalek::pkcs8::spki::Error),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error(transparent)]
    SetupError(#[from] SetupError),
    #[error(transparent)]
    FjallError(#[from] fjall::Error),
    #[error(transparent)]
    StorageError(#[from] StorageLibError),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    SnapshotError(#[from] SnapshotError),
    #[error(transparent)]
    ExplorerError(#[from] ExplorerError),
}
