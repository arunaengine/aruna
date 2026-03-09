use std::array::TryFromSliceError;

use aruna_core::errors::ConversionError;
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Debug, Error)]
pub enum S3ServerError {
    #[error(transparent)]
    DomainError(#[from] s3s::host::DomainError),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("Unimplemented")]
    Unimplemented,
    #[error("Unauthorized")]
    Unauthorized,
    #[error("{0}")]
    InternalError(String),
    #[error("Bad request")]
    BadRequest,
}

#[derive(Debug, Error)]
pub enum TokenError {
    #[error("Realm is not trusted")]
    RealmNotTrusted,
    #[error("TokenBlacklisted")]
    TokenBlacklisted,
    #[error("Invalid issuer key")]
    InvalidIssuerKey,
    #[error(transparent)]
    PublicKeyError(#[from] ed25519_dalek::ed25519::Error),
    #[error("Token expired")]
    Expired,
    #[error("Invalid server token")]
    InvalidServerToken,
    #[error(transparent)]
    FromSliceError(#[from] TryFromSliceError),
    #[error(transparent)]
    PublicKeyConversionError(#[from] ed25519_dalek::pkcs8::spki::Error),
    #[error(transparent)]
    PrivateKeyConversionError(#[from] ed25519_dalek::pkcs8::Error),
    #[error(transparent)]
    JWTError(#[from] jsonwebtoken::errors::Error),
    #[error(transparent)]
    Base64Error(#[from] base64::DecodeError),

}

/// Standard error response for API endpoints.
///
/// All API endpoints return this structure for error responses.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ErrorResponse {
    /// Error message describing what went wrong.
    pub error: String,
    /// Optional error code for programmatic handling.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Optional additional details about the error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

impl ErrorResponse {
    /// Create a simple error response with just a message.
    #[inline]
    #[must_use]
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            code: None,
            details: None,
        }
    }

    /// Create an error response with a code.
    #[inline]
    #[must_use]
    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    /// Create an error response with details.
    #[inline]
    #[must_use]
    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }
}

impl<E: std::fmt::Display> From<E> for ErrorResponse {
    fn from(e: E) -> Self {
        Self::new(e.to_string())
    }
}

/// Result type alias for handlers.
pub type ServerResult<T> = Result<T, ServerError>;

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let code = self.error_code();
        let message = self.to_string();

        let body = ErrorResponse::new(&message).with_code(code);

        (status, Json(body)).into_response()
    }
}

impl ServerError {
    fn status_code(&self) -> StatusCode {
        match self {
            ServerError::Unimplemented => StatusCode::NOT_IMPLEMENTED,
            ServerError::Unauthorized => StatusCode::UNAUTHORIZED,
            ServerError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ServerError::BadRequest => StatusCode::BAD_REQUEST,
        }
    }

    fn error_code(&self) -> String {
        match self {
            ServerError::Unimplemented => "Not implemented".to_string(),
            ServerError::Unauthorized => "Not authorized".to_string(),
            ServerError::InternalError(msg) => msg.clone(),
            ServerError::BadRequest => "Bad request".to_string(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ServerSetupError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Runtime error: `{0}`")]
    Runtime(String),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
}
