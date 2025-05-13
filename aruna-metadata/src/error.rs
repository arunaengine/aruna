use axum::Json;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use tantivy::TantivyError;
use tantivy::query::QueryParserError;
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
pub enum ArunaMetadataError {
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
    #[error("Consensus error: {0}")]
    //ConsensusError(#[from] synevi::SyneviError),
    ConsensusError(String),
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
    #[response(status = 500)]
    #[error("Graph error: {0}")]
    GraphError(String),
    #[response(status = 500)]
    #[error("Tantivy error: {0}")]
    TantivyError(String),
    #[response(status = 500)]
    #[error("Network error: {0}")]
    NetworkError(String),
}

impl ArunaMetadataError {
    pub fn into_axum_tuple(self) -> (axum::http::StatusCode, Json<String>) {
        match self {
            err @ ArunaMetadataError::InvalidParameter { .. } => {
                (StatusCode::BAD_REQUEST, Json(err.to_string()))
            }
            err @ ArunaMetadataError::ParameterNotSpecified { .. } => {
                (StatusCode::BAD_REQUEST, Json(err.to_string()))
            }
            err @ ArunaMetadataError::ConflictParameter { .. } => {
                (StatusCode::CONFLICT, Json(err.to_string()))
            }
            ArunaMetadataError::Forbidden(message) => (StatusCode::FORBIDDEN, Json(message)),
            ArunaMetadataError::Unauthorized => {
                (StatusCode::UNAUTHORIZED, Json("Unauthorized".to_string()))
            }
            err @ ArunaMetadataError::NotFound(_) => (StatusCode::NOT_FOUND, Json(err.to_string())),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json("Internal server error".to_string()),
            ),
        }
    }
}
impl From<std::io::Error> for ArunaMetadataError {
    fn from(e: std::io::Error) -> Self {
        ArunaMetadataError::IoError(e.to_string())
    }
}

impl From<serde_json::Error> for ArunaMetadataError {
    fn from(e: serde_json::Error) -> Self {
        ArunaMetadataError::ConversionError {
            from: "serde_json::Error".to_string(),
            to: e.to_string(),
        }
    }
}
impl From<TantivyError> for ArunaMetadataError {
    fn from(e: TantivyError) -> Self {
        ArunaMetadataError::TantivyError(e.to_string())
    }
}

impl From<QueryParserError> for ArunaMetadataError {
    fn from(e: QueryParserError) -> Self {
        ArunaMetadataError::TantivyError(e.to_string())
    }
}
impl From<automerge::AutomergeError> for ArunaMetadataError {
    fn from(e: automerge::AutomergeError) -> Self {
        ArunaMetadataError::DatabaseError(e.to_string())
    }
}

impl From<autosurgeon::HydrateError> for ArunaMetadataError {
    fn from(e: autosurgeon::HydrateError) -> Self {
        ArunaMetadataError::DatabaseError(e.to_string())
    }
}

impl From<autosurgeon::ReconcileError> for ArunaMetadataError {
    fn from(e: autosurgeon::ReconcileError) -> Self {
        ArunaMetadataError::DatabaseError(e.to_string())
    }
}

impl From<anyhow::Error> for ArunaMetadataError {
    fn from(e: anyhow::Error) -> Self {
        ArunaMetadataError::NetworkError(e.to_string())
    }
}

impl From<postcard::Error> for ArunaMetadataError {
    fn from(e: postcard::Error) -> Self {
        ArunaMetadataError::ConversionError {
            from: e.to_string(),
            to: "&[u8]".to_string(),
        }
    }
}
impl From<aruna_storage::error::ArunaStorageError> for ArunaMetadataError {
    fn from(e: aruna_storage::error::ArunaStorageError) -> Self {
        ArunaMetadataError::DatabaseError(e.to_string())
    }
}
impl From<aruna_realm::error::RealmError> for ArunaMetadataError {
    fn from(e: aruna_realm::error::RealmError) -> Self {
        ArunaMetadataError::NetworkError(e.to_string())
    }
}
