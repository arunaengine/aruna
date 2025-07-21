use aruna_metadata::error::ArunaMetadataError;
use aruna_permission::PermissionError;
use iroh::KeyParsingError;
use std::
    num::ParseIntError
;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ArunaError {
    #[error("ArunaMetadataError: {0}")]
    MetadataError(#[from] ArunaMetadataError),
    #[error("DotenvyError: {0}")]
    DotenvyError(#[from] dotenvy::Error),
    #[error("DataError: {0}")]
    DataError(#[from] anyhow::Error),
    #[error("KeyParsingError: {0}")]
    KeyParsingError(#[from] KeyParsingError),
    #[error("ParseIntError: {0}")]
    ParseIntError(#[from] ParseIntError),
    #[error("SigningKeyError: {0}")]
    SigningKeyError(#[from] ed25519_dalek::pkcs8::Error),
    #[error("PermissionError: {0}")]
    PermissionError(#[from] PermissionError),
    #[error("SerdeError: {0}")]
    SerdeError(#[from] serde_json::error::Error),
    #[error("IpV4ParsingError: {0}")]
    IpV4ParsingError(#[from] std::net::AddrParseError),
}
