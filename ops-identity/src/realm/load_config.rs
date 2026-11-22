//! Reports a realm config read that failed, for callers that go on without the config.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use tracing::{debug, warn};

use crate::document_repository::GetConfigError;

/// Logs why the realm config is missing. A realm not stored yet is a normal state.
pub fn config_unavailable(error: &GetConfigError) {
    match error {
        GetConfigError::DocumentNotFound => debug!("realm config is not stored here yet"),
        error => warn!(%error, "realm config unavailable"),
    }
}
