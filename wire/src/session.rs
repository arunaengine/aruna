//! S3 session requests and answers, and how early a client refreshes a session.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::structs::identity::s3_session::SESSION_REFRESH_WINDOW;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// A client refreshes a session once this much time is left: the node's refresh window less
/// one minute, so a client clock that runs early still asks inside the node's window.
pub const CLIENT_REFRESH_WINDOW: Duration =
    Duration::from_secs(SESSION_REFRESH_WINDOW.as_secs() - 60);

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(as = CreateS3SessionRequest)]
pub struct S3SessionRequest {
    pub group_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(as = S3SessionGroupResponse)]
pub struct SessionGroupResponse {
    pub id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(as = S3SessionIssuerResponse)]
pub struct SessionIssuerResponse {
    pub node_id: String,
    pub s3_endpoint: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(as = S3SessionRestrictionResponse)]
pub struct SessionRestrictionResponse {
    pub pattern: String,
    pub permission: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(as = S3SessionResponse)]
pub struct SessionTokenResponse {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub expires_at: String,
    pub group: SessionGroupResponse,
    pub restrictions: Vec<SessionRestrictionResponse>,
    pub issuer_node: SessionIssuerResponse,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(as = S3SessionSummaryResponse)]
pub struct SessionSummaryResponse {
    pub access_key_id: String,
    pub created_at: Option<String>,
    pub expires_at: String,
    pub last_used_at: Option<String>,
    pub group: SessionGroupResponse,
    pub restrictions: Vec<SessionRestrictionResponse>,
    pub issuer_node: SessionIssuerResponse,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(as = ListS3SessionsResponse)]
pub struct S3SessionsResponse {
    pub sessions: Vec<SessionSummaryResponse>,
}
