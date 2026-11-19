//! Holds the object key search input, hit, page and error types.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::error::PolicyEnforcementError;
use aruna_core::calls::IoError;
use aruna_core::errors::ConversionError;
use aruna_core::id::NodeId;
use aruna_core::search::ObjectKeyMatch;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use std::time::SystemTime;
use thiserror::Error;

pub const SEARCH_MAX_LIMIT: usize = 100;

#[derive(Clone, Debug, PartialEq)]
pub struct SearchObjectsInput {
    pub auth: AuthContext,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub query: String,
    pub key_match: ObjectKeyMatch,
    pub bucket: Option<String>,
    pub limit: usize,
    pub start_after: Option<Vec<u8>>,
    pub as_of: SystemTime,
}

#[derive(Debug, Error, PartialEq)]
pub enum SearchObjectsError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Policy(#[from] PolicyEnforcementError),
    #[error("object inventory storage failed: {0}")]
    Storage(String),
    #[error("object inventory scan budget exhausted")]
    Unavailable,
    #[error("the search position is no longer valid")]
    Position,
}

impl From<IoError> for SearchObjectsError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.to_string())
    }
}
