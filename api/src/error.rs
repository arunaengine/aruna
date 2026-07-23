use std::array::TryFromSliceError;

use aruna_core::errors::ConversionError;
use aruna_core::metadata::MetadataValidationViolation;
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
    #[error("{0}")]
    PayloadTooLarge(String),
    #[error("Bad request")]
    BadRequest,
    #[error("{0}")]
    BadRequestReason(String),
    #[error("{0}")]
    BadRequestMessage(String),
    #[error("Metadata validation failed")]
    MetadataValidation(Vec<MetadataValidationViolation>),
    #[error("Bad gateway")]
    BadGateway,
    #[error("{0}")]
    BadGatewayReason(String),
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
    /// Structured metadata validation failures.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub violations: Option<Vec<ValidationViolationResponse>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ValidationViolationResponse {
    pub code: String,
    pub message: String,
    pub pointer: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity_id: Option<String>,
}

impl From<MetadataValidationViolation> for ValidationViolationResponse {
    fn from(violation: MetadataValidationViolation) -> Self {
        Self {
            code: violation.code,
            message: violation.message,
            pointer: violation.pointer,
            entity_id: violation.entity_id,
        }
    }
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
            violations: None,
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

    #[inline]
    #[must_use]
    pub fn with_violations(mut self, violations: Vec<ValidationViolationResponse>) -> Self {
        self.violations = Some(violations);
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

        let mut body = ErrorResponse::new(&message).with_code(code);
        if let ServerError::MetadataValidation(violations) = &self {
            body = body.with_violations(violations.iter().cloned().map(Into::into).collect());
        }

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
            ServerError::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            ServerError::BadRequest
            | ServerError::BadRequestReason(_)
            | ServerError::BadRequestMessage(_)
            | ServerError::MetadataValidation(_) => StatusCode::BAD_REQUEST,
            ServerError::BadGateway | ServerError::BadGatewayReason(_) => StatusCode::BAD_GATEWAY,
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
            ServerError::PayloadTooLarge(_) => "Payload too large".to_string(),
            ServerError::BadRequest
            | ServerError::BadRequestReason(_)
            | ServerError::BadRequestMessage(_) => "Bad request".to_string(),
            ServerError::MetadataValidation(_) => "Validation failed".to_string(),
            ServerError::BadGateway | ServerError::BadGatewayReason(_) => "Bad gateway".to_string(),
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

#[cfg(test)]
mod tests {
    use super::{ErrorResponse, ServerError};
    use aruna_core::metadata::MetadataValidationViolation;
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    #[tokio::test]
    async fn validation_is_structured() {
        let response = ServerError::MetadataValidation(vec![MetadataValidationViolation {
            code: "missing_root_data_entity".to_string(),
            message: "missing root".to_string(),
            pointer: "/@graph".to_string(),
            entity_id: Some("./".to_string()),
        }])
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: ErrorResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let violations = body.violations.unwrap();
        assert_eq!(violations[0].code, "missing_root_data_entity");
        assert_eq!(violations[0].pointer, "/@graph");
    }
}
