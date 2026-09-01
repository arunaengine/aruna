use std::array::TryFromSliceError;

use aruna_core::errors::ConversionError;
use aruna_core::metadata::{MetadataProfileValidationFinding, MetadataValidationViolation};
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
    #[error("Feature disabled")]
    FeatureDisabled(&'static str),
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Forbidden")]
    Forbidden,
    #[error("{0}")]
    InternalError(String),
    #[error("{0}")]
    Conflict(String),
    /// The bytes a caller acted on are no longer the bytes on disk. The local
    /// data is preserved and the attempt is refused, never applied blindly.
    #[error("{0}")]
    PreconditionFailed(String),
    #[error("{0}")]
    JobPlanConflict(String),
    /// Standing compute quota refused a new admission; the typed reason is
    /// carried in the body so a client can act on the exact dimension.
    #[error("{0}")]
    ComputeQuotaDenied(aruna_core::compute_quota::QuotaDenied),
    #[error("{0}")]
    PayloadTooLarge(String),
    #[error("Bad request")]
    BadRequest,
    /// A write surface named a label the owning node derives for itself.
    #[error("label `{0}` is reserved and derived by the owning node")]
    ReservedLabel(String),
    #[error("{0}")]
    BadRequestReason(String),
    #[error("{0}")]
    BadRequestMessage(String),
    #[error("Metadata validation failed")]
    MetadataValidation(Vec<MetadataValidationViolation>),
    #[error("Metadata Profile validation failed")]
    MetadataProfileValidation(Vec<MetadataProfileValidationFinding>),
    #[error("Bad gateway")]
    BadGateway,
    #[error("{0}")]
    BadGatewayReason(String),
    #[error("Service unavailable")]
    ServiceUnavailable,
    #[error("{0}")]
    ServiceUnavailableReason(String),
    /// A management-only route reached a node that is not one, and no management
    /// node answered the relay.
    #[error("No management node is reachable")]
    NoManagementNode,
    /// A relayed request failed after the management node could already have
    /// applied it, so it must not be re-sent to another node.
    #[error("Relaying to a management node failed")]
    RelayFailed,
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
    #[error("Token lifetime exceeds the revocable maximum")]
    LifetimeTooLong,
    #[error("Revocation state is unavailable")]
    RevocationUnavailable,
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
            ArunaBearerTokenError::LifetimeTooLong => Self::LifetimeTooLong,
            ArunaBearerTokenError::RevocationUnavailable => Self::RevocationUnavailable,
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
#[schema(example = json!({"error": "Not found", "code": "Not found"}))]
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
    /// Structured Profile validation findings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub findings: Option<Vec<ProfileValidationFindingResponse>>,
    /// The exact standing-quota refusal behind a 409, when one caused it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota: Option<QuotaDeniedResponse>,
}

/// Why a standing compute quota refused a new admission, with the numbers the
/// decision used. Group totals are approximate across partitions.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QuotaDeniedResponse {
    /// `job` for a per-job ceiling, `group` for the group's standing cap.
    pub scope: String,
    /// `jobs`, `cpu_cores`, `ram_bytes`, `disk_bytes` or `walltime_ms`.
    pub dimension: String,
    pub observed: u64,
    pub requested: u64,
    pub limit: u64,
}

impl From<aruna_core::compute_quota::QuotaDenied> for QuotaDeniedResponse {
    fn from(denied: aruna_core::compute_quota::QuotaDenied) -> Self {
        use aruna_core::compute_quota::{QuotaDimension, QuotaScope};
        Self {
            scope: match denied.scope {
                QuotaScope::Job => "job",
                QuotaScope::Group => "group",
            }
            .to_string(),
            dimension: match denied.dimension {
                QuotaDimension::Jobs => "jobs",
                QuotaDimension::CpuCores => "cpu_cores",
                QuotaDimension::RamBytes => "ram_bytes",
                QuotaDimension::DiskBytes => "disk_bytes",
                QuotaDimension::WalltimeMs => "walltime_ms",
            }
            .to_string(),
            observed: denied.observed,
            requested: denied.requested,
            limit: denied.limit,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ValidationViolationResponse {
    pub code: String,
    pub message: String,
    pub pointer: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProfileValidationFindingResponse {
    pub code: String,
    pub severity: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub focus_node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub rule: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_revision: Option<String>,
    pub completeness: String,
}

impl From<MetadataProfileValidationFinding> for ProfileValidationFindingResponse {
    fn from(finding: MetadataProfileValidationFinding) -> Self {
        Self {
            code: finding.code,
            severity: format!("{:?}", finding.severity).to_lowercase(),
            focus_node: finding.focus_node,
            path: finding.path,
            rule: finding.rule,
            message: finding.message,
            profile_revision: finding
                .profile_revision
                .map(|revision| revision.to_string()),
            completeness: format!("{:?}", finding.completeness).to_lowercase(),
        }
    }
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
            findings: None,
            quota: None,
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

    #[inline]
    #[must_use]
    pub fn with_findings(mut self, findings: Vec<ProfileValidationFindingResponse>) -> Self {
        self.findings = Some(findings);
        self
    }

    #[inline]
    #[must_use]
    pub fn with_quota(mut self, quota: QuotaDeniedResponse) -> Self {
        self.quota = Some(quota);
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
        let body = self.response_body();

        let mut response = (status, Json(body)).into_response();
        if matches!(
            &self,
            ServerError::ServiceUnavailable
                | ServerError::ServiceUnavailableReason(_)
                | ServerError::NoManagementNode
        ) || matches!(&self, ServerError::MetadataProfileValidation(findings) if profile_validation_unavailable(findings))
        {
            response.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("1"),
            );
        }
        response
    }
}

impl ServerError {
    pub(crate) fn response_body(&self) -> ErrorResponse {
        let mut body = ErrorResponse::new(self.public_message()).with_code(self.error_code());
        if let ServerError::MetadataValidation(violations) = self {
            body = body.with_violations(violations.iter().cloned().map(Into::into).collect());
        }
        if let ServerError::MetadataProfileValidation(findings) = self {
            body = body.with_findings(findings.iter().cloned().map(Into::into).collect());
        }
        if let ServerError::ComputeQuotaDenied(denied) = self {
            body = body.with_quota((*denied).into());
        }
        body
    }

    pub(crate) fn status_code(&self) -> StatusCode {
        match self {
            ServerError::Unimplemented => StatusCode::NOT_IMPLEMENTED,
            ServerError::NotFound | ServerError::FeatureDisabled(_) => StatusCode::NOT_FOUND,
            ServerError::Unauthorized => StatusCode::UNAUTHORIZED,
            ServerError::Forbidden => StatusCode::FORBIDDEN,
            ServerError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ServerError::Conflict(_)
            | ServerError::JobPlanConflict(_)
            | ServerError::ComputeQuotaDenied(_) => StatusCode::CONFLICT,
            ServerError::PreconditionFailed(_) => StatusCode::PRECONDITION_FAILED,
            ServerError::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            ServerError::BadRequest
            | ServerError::ReservedLabel(_)
            | ServerError::BadRequestReason(_)
            | ServerError::BadRequestMessage(_)
            | ServerError::MetadataValidation(_) => StatusCode::BAD_REQUEST,
            ServerError::MetadataProfileValidation(findings) => {
                if profile_validation_unavailable(findings) {
                    StatusCode::SERVICE_UNAVAILABLE
                } else {
                    StatusCode::BAD_REQUEST
                }
            }
            ServerError::BadGateway
            | ServerError::BadGatewayReason(_)
            | ServerError::RelayFailed => StatusCode::BAD_GATEWAY,
            ServerError::ServiceUnavailable
            | ServerError::ServiceUnavailableReason(_)
            | ServerError::NoManagementNode => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    fn error_code(&self) -> String {
        match self {
            ServerError::Unimplemented => "Not implemented".to_string(),
            ServerError::NotFound => "Not found".to_string(),
            ServerError::FeatureDisabled(code) => (*code).to_string(),
            ServerError::Unauthorized => "Not authorized".to_string(),
            ServerError::Forbidden => "Forbidden".to_string(),
            ServerError::InternalError(_) => "Internal error".to_string(),
            ServerError::Conflict(_) => "Conflict".to_string(),
            ServerError::JobPlanConflict(_) => "JobPlanConflict".to_string(),
            ServerError::ComputeQuotaDenied(_) => "compute_quota_denied".to_string(),
            ServerError::PreconditionFailed(_) => "Precondition failed".to_string(),
            ServerError::PayloadTooLarge(_) => "Payload too large".to_string(),
            ServerError::ReservedLabel(_) => "reserved_label".to_string(),
            ServerError::BadRequest
            | ServerError::BadRequestReason(_)
            | ServerError::BadRequestMessage(_) => "Bad request".to_string(),
            ServerError::MetadataValidation(_) => "Validation failed".to_string(),
            ServerError::MetadataProfileValidation(findings) => findings.first().map_or_else(
                || "profile_validation_failed".to_string(),
                |finding| finding.code.clone(),
            ),
            ServerError::BadGateway | ServerError::BadGatewayReason(_) => "Bad gateway".to_string(),
            ServerError::ServiceUnavailable | ServerError::ServiceUnavailableReason(_) => {
                "Service unavailable".to_string()
            }
            ServerError::NoManagementNode => "no_management_node".to_string(),
            ServerError::RelayFailed => "relay_failed".to_string(),
        }
    }

    pub(crate) fn public_message(&self) -> String {
        match self {
            ServerError::InternalError(_) => "Internal server error".to_string(),
            ServerError::BadGateway => "Bad gateway".to_string(),
            _ => self.to_string(),
        }
    }
}

fn profile_validation_unavailable(findings: &[MetadataProfileValidationFinding]) -> bool {
    findings.iter().any(|finding| {
        matches!(
            finding.code.as_str(),
            "profile_unavailable" | "validator_unavailable"
        )
    })
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
    use aruna_core::metadata::{
        MetadataProfileValidationCompleteness, MetadataProfileValidationFinding,
        MetadataProfileValidationSeverity, MetadataValidationViolation,
    };
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

    #[tokio::test]
    async fn profile_validation_is_structured_and_unavailability_is_retryable() {
        let finding = |code: &str| MetadataProfileValidationFinding {
            code: code.to_string(),
            severity: MetadataProfileValidationSeverity::Violation,
            focus_node: Some("https://example.test/dataset".to_string()),
            path: Some("http://schema.org/identifier".to_string()),
            rule: "http://www.w3.org/ns/shacl#minCount".to_string(),
            message: "identifier is required".to_string(),
            profile_revision: None,
            completeness: MetadataProfileValidationCompleteness::Complete,
        };
        let rejected =
            ServerError::MetadataProfileValidation(vec![finding("constraint_violation")])
                .into_response();
        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
        let body: ErrorResponse =
            serde_json::from_slice(&to_bytes(rejected.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body.code.as_deref(), Some("constraint_violation"));
        let findings = body.findings.expect("structured findings are returned");
        assert_eq!(findings[0].severity, "violation");
        assert_eq!(findings[0].completeness, "complete");

        let unavailable =
            ServerError::MetadataProfileValidation(vec![finding("validator_unavailable")])
                .into_response();
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            unavailable.headers().get(axum::http::header::RETRY_AFTER),
            Some(&axum::http::HeaderValue::from_static("1"))
        );

        // An exhausted validation budget is permanent, not a retryable outage.
        for code in ["validation_limit", "unsupported_constraint"] {
            let permanent =
                ServerError::MetadataProfileValidation(vec![finding(code)]).into_response();
            assert_eq!(permanent.status(), StatusCode::BAD_REQUEST);
            assert!(
                permanent
                    .headers()
                    .get(axum::http::header::RETRY_AFTER)
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn plan_conflict_typed() {
        let response =
            ServerError::JobPlanConflict("idempotency key conflict".to_string()).into_response();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: ErrorResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body.code.as_deref(), Some("JobPlanConflict"));
    }

    #[tokio::test]
    async fn quota_denial_typed() {
        // The refusal must carry the exact dimension and numbers, not prose.
        use aruna_core::compute_quota::{QuotaDenied, QuotaDimension, QuotaScope};

        let response = ServerError::ComputeQuotaDenied(QuotaDenied {
            scope: QuotaScope::Group,
            dimension: QuotaDimension::CpuCores,
            observed: 30,
            requested: 8,
            limit: 32,
        })
        .into_response();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: ErrorResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body.code.as_deref(), Some("compute_quota_denied"));
        let quota = body.quota.expect("the typed reason is carried in the body");
        assert_eq!(quota.scope, "group");
        assert_eq!(quota.dimension, "cpu_cores");
        assert_eq!((quota.observed, quota.requested, quota.limit), (30, 8, 32));
    }
}
