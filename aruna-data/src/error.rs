use std::{array::TryFromSliceError, num::ParseIntError, str::ParseBoolError};

use axum::Json;
use axum::http::StatusCode;
use iroh::{
    KeyParsingError,
    endpoint::{ReadExactError, WriteError},
};
use s3s::{S3Error, s3_error};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::task::JoinError;
use utoipa::IntoResponses;

#[macro_export]
macro_rules! logerr {
    () => {
        |e| {
            tracing::error!("Error: {:?}", e);
            e
        }
    };
}

#[derive(Debug, Error, IntoResponses, Clone, Serialize, Deserialize, PartialEq)]
#[allow(dead_code)]
pub enum ArunaDataError {
    // 400 Bad Request
    #[response(status = 400)]
    #[error("Invalid parameter {name}: {error}")]
    InvalidParameter { name: String, error: String },
    // 400 Bad Request
    #[response(status = 400)]
    #[error("Parameter {name} not specified: {error}")]
    ParameterNotSpecified { name: String, error: String },
    // 409 Conflict
    #[response(status = 409)]
    #[error("Conflict parameter {name}: {error}")]
    ConflictParameter { name: String, error: String },
    // 403 Forbidden
    #[response(status = 403)]
    #[error("Not allowed: {0}")]
    Forbidden(String),
    // 401 Unauthorized: If no valid credentials are provided
    #[response(status = 401)]
    #[error("Unauthorized")]
    Unauthorized,
    // 404 Not Found
    #[response(status = 404)]
    #[error("Not found: {0}")]
    NotFound(String),
    // 500 Internal Server Error
    #[response(status = 500)]
    #[error("Serialization error {0}")]
    SerializationError(String),
    #[response(status = 500)]
    #[error("I/O error: {0}")]
    //IoError(#[from] std::io::Error),
    IoError(String),
    #[response(status = 500)]
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[response(status = 500)]
    #[error("Poisend lock error")]
    PoisonedLockError,
    #[response(status = 500)]
    #[error("Conversion failed from: {from} to {to}")]
    ConversionError { from: String, to: String },
    #[response(status = 500)]
    #[error("Transaction failure: {0}")]
    TransactionFailure(String),
    #[response(status = 500)]
    #[error("Server error: {0}")]
    ServerError(String),
    #[response(status = 500)]
    #[error("Config error: {0}")]
    ConfigError(String),
}

impl ArunaDataError {
    pub fn into_axum_tuple(self) -> (StatusCode, Json<String>) {
        match self {
            err @ ArunaDataError::InvalidParameter { .. } => {
                (StatusCode::BAD_REQUEST, Json(err.to_string()))
            }
            err @ ArunaDataError::ParameterNotSpecified { .. } => {
                (StatusCode::BAD_REQUEST, Json(err.to_string()))
            }
            err @ ArunaDataError::ConflictParameter { .. } => {
                (StatusCode::CONFLICT, Json(err.to_string()))
            }
            ArunaDataError::Forbidden(message) => (StatusCode::FORBIDDEN, Json(message)),
            ArunaDataError::Unauthorized => {
                (StatusCode::UNAUTHORIZED, Json("Unauthorized".to_string()))
            }
            err @ ArunaDataError::NotFound(_) => (StatusCode::NOT_FOUND, Json(err.to_string())),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json("Internal server error".to_string()),
            ),
        }
    }
}
impl From<std::io::Error> for ArunaDataError {
    fn from(e: std::io::Error) -> Self {
        ArunaDataError::IoError(e.to_string())
    }
}

impl From<aruna_realm::error::RealmError> for ArunaDataError {
    fn from(e: aruna_realm::error::RealmError) -> Self {
        ArunaDataError::ServerError(e.to_string())
    }
}

impl From<aruna_permission::error::PermissionError> for ArunaDataError {
    fn from(e: aruna_permission::error::PermissionError) -> Self {
        ArunaDataError::Forbidden(e.to_string())
    }
}

impl From<dotenvy::Error> for ArunaDataError {
    fn from(e: dotenvy::Error) -> Self {
        ArunaDataError::ServerError(e.to_string())
    }
}

impl From<ed25519_dalek::pkcs8::Error> for ArunaDataError {
    fn from(_e: ed25519_dalek::pkcs8::Error) -> Self {
        ArunaDataError::ConversionError {
            from: "pkcs8 formatted String".to_string(),
            to: "SingingKey".to_string(),
        }
    }
}

impl From<KeyParsingError> for ArunaDataError {
    fn from(_e: KeyParsingError) -> Self {
        ArunaDataError::ConversionError {
            from: "String".to_string(),
            to: "SecretKey".to_string(),
        }
    }
}

impl From<ParseIntError> for ArunaDataError {
    fn from(_e: ParseIntError) -> Self {
        ArunaDataError::ConversionError {
            from: "String".to_string(),
            to: "Int".to_string(),
        }
    }
}

impl From<ParseBoolError> for ArunaDataError {
    fn from(_e: ParseBoolError) -> Self {
        ArunaDataError::ConversionError {
            from: "String".to_string(),
            to: "Bool".to_string(),
        }
    }
}

impl From<ulid::DecodeError> for ArunaDataError {
    fn from(e: ulid::DecodeError) -> Self {
        ArunaDataError::InvalidParameter {
            name: "Ulid".to_string(),
            error: e.to_string(),
        }
    }
}

impl From<aruna_permission::error::PathError> for ArunaDataError {
    fn from(e: aruna_permission::error::PathError) -> Self {
        ArunaDataError::InvalidParameter {
            name: "path".to_string(),
            error: e.to_string(),
        }
    }
}

impl From<aruna_storage::error::ArunaStorageError> for ArunaDataError {
    fn from(e: aruna_storage::error::ArunaStorageError) -> Self {
        ArunaDataError::DatabaseError(e.to_string())
    }
}

impl From<postcard::Error> for ArunaDataError {
    fn from(e: postcard::Error) -> Self {
        ArunaDataError::SerializationError(e.to_string())
    }
}

impl From<bao_tree::io::EncodeError> for ArunaDataError {
    fn from(e: bao_tree::io::EncodeError) -> Self {
        ArunaDataError::SerializationError(e.to_string())
    }
}

impl From<TryFromSliceError> for ArunaDataError {
    fn from(e: TryFromSliceError) -> Self {
        ArunaDataError::SerializationError(e.to_string())
    }
}

impl From<fancy_regex::Error> for ArunaDataError {
    fn from(e: fancy_regex::Error) -> Self {
        ArunaDataError::SerializationError(e.to_string())
    }
}

impl From<opendal::Error> for ArunaDataError {
    fn from(e: opendal::Error) -> Self {
        ArunaDataError::ServerError(e.to_string())
    }
}

impl From<JoinError> for ArunaDataError {
    fn from(e: JoinError) -> Self {
        ArunaDataError::ServerError(e.to_string())
    }
}

impl From<WriteError> for ArunaDataError {
    fn from(e: WriteError) -> Self {
        ArunaDataError::ServerError(e.to_string())
    }
}

impl From<ReadExactError> for ArunaDataError {
    fn from(e: ReadExactError) -> Self {
        ArunaDataError::ServerError(e.to_string())
    }
}

impl From<ArunaDataError> for S3Error {
    fn from(e: ArunaDataError) -> Self {
        match e {
            ArunaDataError::Unauthorized => s3_error!(UnauthorizedAccess, "Unauthorized"),
            // should probably be matched against bucket/key/tags/...
            ArunaDataError::NotFound(e) => s3_error!(NoSuchResource, "{e}"),
            ArunaDataError::Forbidden(e) => s3_error!(UnauthorizedAccess, "{e}"),
            ArunaDataError::InvalidParameter { name, error } => {
                s3_error!(InvalidRequest, "Invalid parameter {name}: {error}")
            }
            ArunaDataError::ParameterNotSpecified { name, error } => {
                s3_error!(InvalidRequest, "Parameter {name} not specified: {error}")
            }
            ArunaDataError::ConflictParameter { name, error } => {
                s3_error!(InvalidRequest, "Confliction parameter {name}: {error}")
            }
            _ => s3_error!(InternalError, "{e}"),
        }
    }
}
