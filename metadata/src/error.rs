use axum::Json;
use axum::http::StatusCode;
use serde::Serialize;
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

#[derive(Debug, Error, IntoResponses, Clone, Serialize)]
#[allow(dead_code)]
pub enum ArunaError {
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
    #[error("Database: {0} does not exist")]
    DatabaseDoesNotExist(&'static str),
    #[response(status = 500)]
    #[error("I/O error: {0}")]
    //IoError(#[from] std::io::Error),
    IoError(String),
    #[response(status = 500)]
    #[error("Database error: {0}")]
    //DatabaseError(#[from] heed::Error),
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
    #[error("Graph error: {0}")]
    TantivyError(String),
}

impl ArunaError {
    pub fn into_axum_tuple(self) -> (axum::http::StatusCode, Json<String>) {
        match self {
            err @ ArunaError::InvalidParameter { .. } => {
                (StatusCode::BAD_REQUEST, Json(err.to_string()))
            }
            err @ ArunaError::ParameterNotSpecified { .. } => {
                (StatusCode::BAD_REQUEST, Json(err.to_string()))
            }
            err @ ArunaError::ConflictParameter { .. } => {
                (StatusCode::CONFLICT, Json(err.to_string()))
            }
            ArunaError::Forbidden(message) => (StatusCode::FORBIDDEN, Json(message)),
            ArunaError::Unauthorized => {
                (StatusCode::UNAUTHORIZED, Json("Unauthorized".to_string()))
            }
            err @ ArunaError::NotFound(_) => (StatusCode::NOT_FOUND, Json(err.to_string())),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json("Internal server error".to_string()),
            ),
        }
    }
}
impl From<std::io::Error> for ArunaError {
    fn from(e: std::io::Error) -> Self {
        ArunaError::IoError(e.to_string())
    }
}

impl From<heed::Error> for ArunaError {
    fn from(e: heed::Error) -> Self {
        ArunaError::DatabaseError(e.to_string())
    }
}
impl From<serde_json::Error> for ArunaError {
    fn from(e: serde_json::Error) -> Self {
        ArunaError::ConversionError {
            from: "serde_json::Error".to_string(),
            to: e.to_string(),
        }
    }
}
impl From<TantivyError> for ArunaError {
    fn from(e: TantivyError) -> Self {
        ArunaError::TantivyError(e.to_string())
    }
}

impl From<QueryParserError> for ArunaError {
    fn from(e: QueryParserError) -> Self {
        ArunaError::TantivyError(e.to_string())
    }
}

impl From<fjall::Error> for ArunaError {
    fn from(e: fjall::Error) -> Self {
        ArunaError::DatabaseError(e.to_string())
    }
}

impl From<redb::Error> for ArunaError {
    fn from(e: redb::Error) -> Self {
        ArunaError::DatabaseError(e.to_string())
    }
}

impl From<redb::DatabaseError> for ArunaError {
    fn from(e: redb::DatabaseError) -> Self {
        ArunaError::DatabaseError(e.to_string())
    }
}

impl From<redb::TransactionError> for ArunaError {
    fn from(e: redb::TransactionError) -> Self {
        ArunaError::DatabaseError(e.to_string())
    }
}

impl From<redb::TableError> for ArunaError {
    fn from(e: redb::TableError) -> Self {
        ArunaError::DatabaseError(e.to_string())
    }
}

impl From<redb::StorageError> for ArunaError {
    fn from(e: redb::StorageError) -> Self {
        ArunaError::DatabaseError(e.to_string())
    }
}

impl From<redb::CommitError> for ArunaError {
    fn from(e: redb::CommitError) -> Self {
        ArunaError::DatabaseError(e.to_string())
    }
}

impl From<automerge::AutomergeError> for ArunaError {
    fn from(e: automerge::AutomergeError) -> Self {
        ArunaError::DatabaseError(e.to_string())
    }
}

impl From<autosurgeon::HydrateError> for ArunaError {
    fn from(e: autosurgeon::HydrateError) -> Self {
        ArunaError::DatabaseError(e.to_string())
    }
}

impl From<autosurgeon::ReconcileError> for ArunaError {
    fn from(e: autosurgeon::ReconcileError) -> Self {
        ArunaError::DatabaseError(e.to_string())
    }
}
