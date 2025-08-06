use aruna_data::error::ArunaDataError;
use aruna_metadata::error::ArunaMetadataError;
use aruna_permission::PermissionError;
use aruna_task::error::ArunaTaskError;
use iroh::KeyParsingError;
use std::{net::AddrParseError, num::ParseIntError};
use thiserror::Error;
use tokio::task::JoinError;

#[derive(Debug, Error)]
pub enum ArunaError {
    #[error("ArunaMetadataError: {0}")]
    MetadataError(#[from] ArunaMetadataError),
    #[error("ArunaNetworkError: {0}")]
    NetworkError(#[from] anyhow::Error),
    #[error("DotenvyError: {0}")]
    DotenvyError(#[from] dotenvy::Error),
    #[error("DataError: {0}")]
    DataError(#[from] ArunaDataError),
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
    IpV4ParsingError(#[from] AddrParseError),
    #[error("TaskError: {0}")]
    TaskError(#[from] ArunaTaskError),
    #[error("TokioJoinError: {0}")]
    JoinError(#[from] JoinError),
}
