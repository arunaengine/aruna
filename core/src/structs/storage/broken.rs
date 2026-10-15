//! Records of a bucket check that finds current versions whose stored copy cannot be served.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::errors::ConversionError;
use crate::structs::storage::blob::BlobVersion;
use serde::{Deserialize, Serialize};

/// Why a version's copy cannot be served.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BrokenReason {
    /// No location row names the copy.
    MissingRecord,
    /// The group storage holding the copy was removed or disabled.
    StorageRemoved,
    /// The backend has no object at the copy's path.
    MissingBlob,
}

/// A broken version, keyed by its version key; `version` is the record the check saw.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BrokenVersion {
    pub reason: BrokenReason,
    pub version: BlobVersion,
    pub found_at_ms: u64,
}

impl BrokenVersion {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// This node's progress checking one bucket's current versions. Keyed by bucket name.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageCheck {
    /// Last head key handled; the next batch resumes after it.
    pub cursor: Option<Vec<u8>>,
    pub checked: u64,
    pub broken: u64,
    /// Versions whose backend could not answer; a later check retries them.
    pub failed: u64,
    pub started_at_ms: u64,
    pub finished_at_ms: Option<u64>,
}

impl StorageCheck {
    pub fn new(started_at_ms: u64) -> Self {
        Self {
            started_at_ms,
            ..Self::default()
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}
