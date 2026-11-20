//! The saved count of how many current versions of a bucket carry its placement rules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::errors::ConversionError;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// This node's count of one bucket's current versions against the bucket rules. Keyed by bucket
/// name; in a distributed bucket it counts only the ranges this node writes.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CoverageCount {
    /// Last head key counted; the next page resumes after it.
    pub cursor: Option<Vec<u8>>,
    /// The rules generation every counted page compared against.
    pub generation: u64,
    pub observed: u64,
    /// Delete markers, which carry no rules.
    pub deleted: u64,
    /// Versions that carry none or only some of the rules.
    pub gaps: u64,
    pub registered: u64,
    pub quarantined: u64,
    pub absent: u64,
    pub reference_only: u64,
    pub started_at_ms: u64,
    /// Set once every current version was counted.
    pub finished_at_ms: Option<u64>,
    /// The start this count belongs to; every coordinator a distributed start reached holds it.
    pub run: Option<Ulid>,
}

impl CoverageCount {
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
