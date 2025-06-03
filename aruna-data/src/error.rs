use axum::Json;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::IntoResponses;

#[macro_export]
macro_rules! logerr {
    () => {
        |e| {
            tracing::error!("Error: {:?}", e);
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
    #[error("Deserialize error {0}")]
    //DeserializeError(#[from] bincode::Error),
    DeserializeError(String),
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

impl From<ulid::DecodeError> for ArunaDataError {
    fn from(e: ulid::DecodeError) -> Self {
        ArunaDataError::InvalidParameter {
            name: "Ulid".to_string(),
            error: e.to_string(),
        }
    }
}

impl From<aruna_permission::paths::PathError> for ArunaDataError {
    fn from(e: aruna_permission::paths::PathError) -> Self {
        ArunaDataError::InvalidParameter {
            name: "path".to_string(),
            error: e.to_string(),
        }
    }
}
