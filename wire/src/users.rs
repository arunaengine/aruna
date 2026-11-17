//! User registration and token answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RegisterUserRequest {
    pub onboarding_secret: Option<String>,
    /// Create a new account although an account with this email or name exists.
    #[serde(default)]
    pub create_new: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RegisterUserResponse {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GetTokenResponse {
    pub token: String,
}
