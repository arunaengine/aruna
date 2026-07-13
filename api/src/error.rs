use std::array::TryFromSliceError;

use aruna_core::errors::ConversionError;
use aruna_operations::auth::ArunaBearerTokenError;
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
    #[error("Not found")]
    NotFound,
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Forbidden")]
    Forbidden,
    #[error("{0}")]
    InternalError(String),
    #[error("{0}")]
    Conflict(String),
    #[error("Bad request")]
    BadRequest,
    #[error("{0}")]
    BadRequestReason(String),
    #[error("Bad gateway")]
    BadGateway,
    #[error("Service unavailable")]
    ServiceUnavailable,
    #[error("{0}")]
    ServiceUnavailableReason(String),
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
    #[error("Error decoding AuthContext")]
    AuthContextConversion(#[from] ConversionError),
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

impl From<ArunaBearerTokenError> for TokenError {
    fn from(error: ArunaBearerTokenError) -> Self {
        match error {
            ArunaBearerTokenError::RealmNotTrusted => Self::RealmNotTrusted,
            ArunaBearerTokenError::TokenRevoked => Self::TokenBlacklisted,
            ArunaBearerTokenError::InvalidIssuerKey => Self::InvalidIssuerKey,
            ArunaBearerTokenError::Expired => Self::Expired,
            ArunaBearerTokenError::InvalidServerToken => Self::InvalidServerToken,
            ArunaBearerTokenError::AuthContextConversion(error) => {
                Self::AuthContextConversion(error)
            }
            ArunaBearerTokenError::PublicKeyError(error) => Self::PublicKeyError(error),
            ArunaBearerTokenError::FromSliceError(error) => Self::FromSliceError(error),
            ArunaBearerTokenError::PublicKeyConversionError(error) => {
                Self::PublicKeyConversionError(error)
            }
            ArunaBearerTokenError::JwtError(error) => Self::JWTError(error),
            ArunaBearerTokenError::Base64Error(error) => Self::Base64Error(error),
        }
    }
}

#[derive(Debug, Error)]
pub enum OidcError {
    #[error("OIDC is not configured")]
    NotConfigured,
    #[error("OIDC provider not found")]
    ProviderNotFound,
    #[error("unsupported OIDC signing algorithm")]
    UnsupportedAlgorithm,
    #[error("OIDC key id is missing")]
    MissingKeyId,
    #[error("OIDC signing key not found")]
    SigningKeyNotFound,
    #[error("OIDC token subject is missing")]
    MissingSubject,
    #[error("OIDC configuration error: {0}")]
    Internal(String),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    Jwt(#[from] jsonwebtoken::errors::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
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
        let message = self.public_message();

        let body = ErrorResponse::new(&message).with_code(code);

        let mut response = (status, Json(body)).into_response();
        if matches!(
            self,
            ServerError::ServiceUnavailable | ServerError::ServiceUnavailableReason(_)
        ) {
            response.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("1"),
            );
        }
        response
    }
}

impl ServerError {
    fn status_code(&self) -> StatusCode {
        match self {
            ServerError::Unimplemented => StatusCode::NOT_IMPLEMENTED,
            ServerError::NotFound => StatusCode::NOT_FOUND,
            ServerError::Unauthorized => StatusCode::UNAUTHORIZED,
            ServerError::Forbidden => StatusCode::FORBIDDEN,
            ServerError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ServerError::Conflict(_) => StatusCode::CONFLICT,
            ServerError::BadRequest | ServerError::BadRequestReason(_) => StatusCode::BAD_REQUEST,
            ServerError::BadGateway => StatusCode::BAD_GATEWAY,
            ServerError::ServiceUnavailable | ServerError::ServiceUnavailableReason(_) => {
                StatusCode::SERVICE_UNAVAILABLE
            }
        }
    }

    fn error_code(&self) -> String {
        match self {
            ServerError::Unimplemented => "Not implemented".to_string(),
            ServerError::NotFound => "Not found".to_string(),
            ServerError::Unauthorized => "Not authorized".to_string(),
            ServerError::Forbidden => "Forbidden".to_string(),
            ServerError::InternalError(_) => "Internal error".to_string(),
            ServerError::Conflict(_) => "Conflict".to_string(),
            ServerError::BadRequest | ServerError::BadRequestReason(_) => "Bad request".to_string(),
            ServerError::BadGateway => "Bad gateway".to_string(),
            ServerError::ServiceUnavailable | ServerError::ServiceUnavailableReason(_) => {
                "Service unavailable".to_string()
            }
        }
    }

    fn public_message(&self) -> String {
        match self {
            ServerError::InternalError(_) => "Internal server error".to_string(),
            ServerError::BadGateway => "Bad gateway".to_string(),
            _ => self.to_string(),
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
