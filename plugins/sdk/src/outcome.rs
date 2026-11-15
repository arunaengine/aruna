//! Errors of host calls and handlers, and the run outcome each one ends in.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt;

use crate::bindings::aruna::plugin::types::{ErrorKind, Failure, FailureKind, HostError};

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Why a call or a handler failed. Uncaught, `Unavailable` and `Retry` end the run as retried,
/// `InvalidInput` as invalid input, and every other one as failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The install may not read or write what the call names.
    NotAllowed(String),
    NotFound(String),
    /// Something changed since it was read.
    Conflict(String),
    LimitReached(String),
    /// The call's arguments are not valid.
    InvalidCall(String),
    /// The host cannot answer now; the run is retried later.
    Unavailable(String),
    /// A change may have happened without its answer; a person decides.
    UnknownOutcome(String),
    /// A service answered with a status outside 200 to 299.
    Service {
        status: u16,
        message: String,
    },
    /// The run's input does not fit the plugin.
    InvalidInput(String),
    /// The plugin gave up.
    Fail(String),
    /// The plugin asks for the run to be tried again later.
    Retry(String),
}

/// Ends the run as failed, without a retry.
pub fn fail(message: impl Into<String>) -> Error {
    Error::Fail(message.into())
}

/// Ends the run as retried later.
pub fn retry(message: impl Into<String>) -> Error {
    Error::Retry(message.into())
}

impl Error {
    /// The run outcome the host records for an uncaught error.
    pub(crate) fn failure(self) -> Failure {
        let kind = match &self {
            Self::Unavailable(_) | Self::Retry(_) => FailureKind::Retryable,
            Self::InvalidInput(_) => FailureKind::InvalidInput,
            _ => FailureKind::Permanent,
        };
        Failure {
            kind,
            message: self.to_string(),
        }
    }
}

impl From<HostError> for Error {
    fn from(error: HostError) -> Self {
        let message = error.message;
        match error.kind {
            ErrorKind::NotGranted | ErrorKind::Denied => Self::NotAllowed(message),
            ErrorKind::NotFound => Self::NotFound(message),
            ErrorKind::Conflict => Self::Conflict(message),
            ErrorKind::Limit => Self::LimitReached(message),
            ErrorKind::Invalid => Self::InvalidCall(message),
            ErrorKind::Unavailable => Self::Unavailable(message),
            ErrorKind::UnknownOutcome => Self::UnknownOutcome(message),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::Fail(format!("JSON: {error}"))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAllowed(message) => write!(f, "not allowed: {message}"),
            Self::NotFound(message) => write!(f, "not found: {message}"),
            Self::Conflict(message) => write!(f, "conflict: {message}"),
            Self::LimitReached(message) => write!(f, "limit reached: {message}"),
            Self::InvalidCall(message) => write!(f, "invalid call: {message}"),
            Self::Unavailable(message) => write!(f, "unavailable: {message}"),
            Self::UnknownOutcome(message) => write!(f, "unknown outcome: {message}"),
            Self::Service { status, message } => write!(f, "service answered {status}: {message}"),
            Self::InvalidInput(message) => write!(f, "invalid input: {message}"),
            Self::Fail(message) | Self::Retry(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}
