//! Turns API, S3 and local failures into short messages that say what to do next.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::Deserialize;
use std::fmt;

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("{0}")]
    Api(Box<ApiError>),
    #[error("not signed in: run `aruna-cli login --api <url>` first")]
    NotSignedIn,
    #[error("{0}")]
    Usage(String),
    #[error("{0}")]
    Routing(String),
    #[error("S3: {0}")]
    S3(String),
    #[error("checksum mismatch for {path}: expected BLAKE3 {expected}, got {actual}")]
    Checksum {
        path: String,
        expected: String,
        actual: String,
    },
    #[error("cannot reach the API: {0}")]
    Http(#[from] reqwest::Error),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
}

/// The error body every REST route answers with.
#[derive(Debug, Default, Deserialize)]
pub struct ErrorBody {
    #[serde(default)]
    pub error: String,
    pub code: Option<String>,
    pub details: Option<String>,
    pub violations: Option<Vec<Violation>>,
    pub findings: Option<Vec<Finding>>,
    pub omitted_findings: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, serde::Serialize)]
pub struct Violation {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub pointer: String,
    pub entity_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, serde::Serialize)]
pub struct Finding {
    pub code: String,
    #[serde(default)]
    pub severity: String,
    pub focus_node: Option<String>,
    pub path: Option<String>,
    #[serde(default)]
    pub message: String,
}

/// A refused REST request with the parts of its answer a user needs.
#[derive(Debug, thiserror::Error)]
pub struct ApiError {
    pub status: u16,
    pub body: ErrorBody,
    pub path: String,
}

impl ApiError {
    /// Builds the error from a status and the raw body, which may not be JSON.
    pub fn from_body(status: u16, path: &str, text: &str) -> Self {
        let body = serde_json::from_str::<ErrorBody>(text).unwrap_or_else(|_| ErrorBody {
            error: text.trim().chars().take(300).collect(),
            ..ErrorBody::default()
        });
        Self {
            status,
            body,
            path: path.to_string(),
        }
    }

    fn hint(&self) -> &'static str {
        match self.status {
            400 | 422 => "the request was rejected",
            401 => "not signed in or the token expired; run `aruna-cli login` again",
            403 => "access denied",
            404 => "not found",
            409 => "conflict",
            412 => "changed meanwhile; read it again and retry",
            413 => "too large for this node",
            429 => "rate limited; wait a moment and retry",
            501 => "not supported by this node",
            502..=504 => "a node is unreachable or busy; try again",
            _ => "request failed",
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (HTTP {}", self.hint(), self.status)?;
        if let Some(code) = &self.body.code {
            write!(f, ", {code}")?;
        }
        write!(f, ")")?;
        if !self.path.is_empty() {
            write!(f, " at {}", self.path)?;
        }
        if !self.body.error.is_empty() {
            write!(f, ": {}", self.body.error)?;
        }
        if let Some(details) = &self.body.details {
            write!(f, "\n  {details}")?;
        }
        for violation in self.body.violations.iter().flatten() {
            write!(f, "\n  - {} {}", violation.code, violation.message)?;
            if !violation.pointer.is_empty() {
                write!(f, " at {}", violation.pointer)?;
            }
        }
        for finding in self.body.findings.iter().flatten() {
            let focus = finding.focus_node.as_deref().unwrap_or("");
            write!(
                f,
                "\n  - {} {}: {}",
                finding.severity, focus, finding.message
            )?;
        }
        if let Some(omitted) = self.body.omitted_findings.filter(|count| *count > 0) {
            write!(f, "\n  ... and {omitted} more findings")?;
        }
        Ok(())
    }
}

impl From<ApiError> for CliError {
    fn from(error: ApiError) -> Self {
        Self::Api(Box::new(error))
    }
}

impl CliError {
    pub fn usage(message: impl Into<String>) -> Self {
        Self::Usage(message.into())
    }

    pub fn routing(message: impl Into<String>) -> Self {
        Self::Routing(message.into())
    }
}

/// Keeps the code and message of an S3 failure and drops the SDK's wrapping.
pub fn s3_error<E>(error: aws_sdk_s3::error::SdkError<E>) -> CliError
where
    E: aws_sdk_s3::error::ProvideErrorMetadata + std::error::Error + 'static,
{
    let status = error.raw_response().map(|raw| raw.status().as_u16());
    let message = match error.as_service_error() {
        Some(service) => format!(
            "{}: {}",
            service.code().unwrap_or("Error"),
            service.message().unwrap_or("no message")
        ),
        None => match &error {
            aws_sdk_s3::error::SdkError::DispatchFailure(failure) => {
                format!("cannot reach the S3 endpoint: {failure:?}")
            }
            other => other.to_string(),
        },
    };
    match status {
        Some(status) => CliError::S3(format!("{message} (HTTP {status})")),
        None => CliError::S3(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_status_hint() {
        let error = ApiError::from_body(401, "/api/v1/x", r#"{"error":"Unauthorized"}"#);
        let text = error.to_string();
        assert!(text.contains("run `aruna-cli login` again"), "{text}");
        assert!(text.contains("HTTP 401) at /api/v1/x"), "{text}");
    }

    #[test]
    fn lists_validation_details() {
        let body = r#"{"error":"invalid crate","code":"invalid_rocrate",
            "violations":[{"code":"missing_root","message":"no root dataset","pointer":"/@graph"}],
            "findings":[{"code":"min_count","severity":"violation","focus_node":"./",
                "rule":"r","message":"name is required","completeness":"complete"}],
            "omitted_findings":2}"#;
        let text = ApiError::from_body(400, "", body).to_string();
        assert!(text.contains("invalid_rocrate"), "{text}");
        assert!(
            text.contains("missing_root no root dataset at /@graph"),
            "{text}"
        );
        assert!(text.contains("violation ./: name is required"), "{text}");
        assert!(text.contains("2 more findings"), "{text}");
    }

    #[test]
    fn keeps_plain_body() {
        let text = ApiError::from_body(503, "", "upstream down\n").to_string();
        assert!(text.starts_with("a node is unreachable"), "{text}");
        assert!(text.ends_with(": upstream down"), "{text}");
    }
}
