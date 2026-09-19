//! One stored login session of a user, with its kind, token hash, expiry and revoked flag.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::UserId;
use crate::errors::ConversionError;
use crate::structs::identity::auth::SessionKind;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserSession {
    pub sid: String,
    pub user_id: UserId,
    pub kind: SessionKind,
    pub label: Option<String>,
    pub created_at: u64,
    pub expires_at: u64,
    pub token_hash: String,
    pub revoked: bool,
}

impl UserSession {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}
