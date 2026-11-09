//! Maps host errors to plugin errors and plugin errors to the run outcome the host records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt;

use crate::bindings::aruna::plugin::types::{ErrorKind, Failure, FailureKind, HostError};

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Why a host call or a handler failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The install has no grant for this call. Nothing wider is tried instead.
    NotGranted {
        permission: String,
        resource: String,
    },
    /// The host refused or could not run the call.
    Host { kind: ErrorKind, message: String },
    /// The run's input does not fit the handler.
    Input(String),
    /// The handler gave up; the run fails without a retry.
    Failed(String),
}

impl Error {
    pub fn failed(message: impl Into<String>) -> Self {
        Self::Failed(message.into())
    }

    /// Converts a host error, naming the permission and resource of a missing grant.
    pub(crate) fn host(error: HostError, permission: &str, resource: &str) -> Self {
        match error.kind {
            ErrorKind::NotGranted => Self::NotGranted {
                permission: permission.to_string(),
                resource: resource.to_string(),
            },
            kind => Self::Host {
                kind,
                message: error.message,
            },
        }
    }

    /// The run outcome: only an unavailable host is worth a retry.
    pub(crate) fn failure(self) -> Failure {
        let kind = match &self {
            Self::Host {
                kind: ErrorKind::Unavailable,
                ..
            } => FailureKind::Retryable,
            Self::Input(_) => FailureKind::InvalidInput,
            _ => FailureKind::Permanent,
        };
        Failure {
            kind,
            message: self.to_string(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotGranted {
                permission,
                resource,
            } => write!(
                f,
                "permission `{permission}` is not granted for `{resource}`"
            ),
            Self::Host { kind, message } => write!(f, "host call failed ({kind:?}): {message}"),
            Self::Input(message) => write!(f, "invalid input: {message}"),
            Self::Failed(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::Failed(format!("JSON: {error}"))
    }
}
