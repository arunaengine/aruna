//! Request and answer shapes the named secret routes of every scope share, and their mapping.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod node;

use std::collections::BTreeMap;
use std::fmt;

use aruna_core::compute::Secret;
use aruna_core::secrets::{SecretFields, SecretKind, SecretName, SecretRecord, UseRule};
use aruna_operations::secrets::{ChangeError, SecretChange};
use axum::http::HeaderMap;
use axum::http::header::IF_MATCH;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::error::{ServerError, ServerResult};

/// Rows per page when the caller sets no limit.
const DEFAULT_LIMIT: usize = 50;
/// The most rows one page holds.
const MAX_LIMIT: usize = 200;

#[derive(Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateSecretRequest {
    /// 1 to 64 of `a-z`, `0-9`, `.`, `_` and `-`, starting with a letter or digit.
    pub name: String,
    /// `bearer`, `basic`, `header`, `hmac` or `fields`.
    pub kind: String,
    #[serde(default)]
    pub description: String,
    /// Settings that are not secret, such as `header` for the `header` and `hmac` kinds.
    #[serde(default)]
    pub settings: BTreeMap<String, String>,
    /// The field values the kind needs. Stored sealed and never returned.
    pub fields: BTreeMap<String, String>,
    /// Use rules: which plugin install may use the secret, how and toward where.
    #[serde(default)]
    #[schema(value_type = Vec<Object>)]
    pub uses: Vec<UseRule>,
    /// Lets the owner read the value back. Only user secrets accept it, and only at create.
    #[serde(default)]
    pub readable: bool,
}

impl fmt::Debug for CreateSecretRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateSecretRequest")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("fields", &self.fields.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ReplaceSecretRequest {
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub settings: BTreeMap<String, String>,
    #[serde(default)]
    #[schema(value_type = Vec<Object>)]
    pub uses: Vec<UseRule>,
}

#[derive(Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RotateSecretRequest {
    /// The new field values, with the same names the kind needs. Never returned.
    pub fields: BTreeMap<String, String>,
}

impl fmt::Debug for RotateSecretRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RotateSecretRequest")
            .field("fields", &self.fields.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// A secret as its managers see it. Field values are never part of it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SecretView {
    pub name: String,
    pub kind: String,
    pub description: String,
    pub settings: BTreeMap<String, String>,
    /// Names of the stored fields.
    pub fields: Vec<String>,
    #[schema(value_type = Vec<Object>)]
    pub uses: Vec<UseRule>,
    pub readable: bool,
    /// A user or group secret whose info or value has not reached this node yet; not usable.
    #[serde(default)]
    pub incomplete: bool,
    /// Pass it as `If-Match` to replace, rotate or delete only this state.
    pub revision: String,
    pub created_by: String,
    pub created_at_ms: u64,
    pub updated_by: String,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct SecretPage {
    pub secrets: Vec<SecretView>,
    /// Pass it as `cursor` for the next page; absent on the last page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct SecretListQuery {
    /// The `next_cursor` of the previous page.
    pub cursor: Option<String>,
    /// Rows per page, 1 to 200, default 50.
    pub limit: Option<usize>,
}

impl SecretListQuery {
    fn page(&self) -> ServerResult<(Option<SecretName>, usize)> {
        let cursor = self.cursor.as_deref().map(parse_name).transpose()?;
        Ok((
            cursor,
            self.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT),
        ))
    }
}

pub(crate) fn parse_name(name: &str) -> ServerResult<SecretName> {
    SecretName::try_from(name.to_string())
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))
}

fn secret_fields(fields: BTreeMap<String, String>) -> SecretFields {
    SecretFields::new(
        (fields.into_iter())
            .map(|(name, value)| (name, Secret::new(value)))
            .collect(),
    )
}

/// The `If-Match` revision, without quotes or a weak marker.
pub(crate) fn expected(headers: &HeaderMap) -> ServerResult<Option<String>> {
    let Some(value) = headers.get(IF_MATCH) else {
        return Ok(None);
    };
    let value = (value.to_str())
        .map_err(|_| ServerError::BadRequestReason("If-Match is not text".to_string()))?;
    let value = value.trim().trim_start_matches("W/").trim_matches('"');
    Ok(Some(value.to_string()))
}

/// The create change; `readable` is refused here, since node and realm secrets never read back.
pub(crate) fn create_change(request: CreateSecretRequest) -> ServerResult<SecretChange> {
    if request.readable {
        return Err(ServerError::BadRequestReason(
            "only user secrets can be readable".to_string(),
        ));
    }
    let kind = SecretKind::parse(&request.kind)
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    Ok(SecretChange::Create {
        name: parse_name(&request.name)?,
        kind,
        description: request.description,
        settings: request.settings,
        uses: request.uses,
        fields: secret_fields(request.fields),
    })
}

pub(crate) fn replace_change(
    name: SecretName,
    request: ReplaceSecretRequest,
    expected: Option<String>,
) -> SecretChange {
    SecretChange::Replace {
        name,
        description: request.description,
        settings: request.settings,
        uses: request.uses,
        expected,
    }
}

pub(crate) fn rotate_change(
    name: SecretName,
    request: RotateSecretRequest,
    expected: Option<String>,
) -> SecretChange {
    SecretChange::Rotate {
        name,
        fields: secret_fields(request.fields),
        expected,
    }
}

pub(crate) fn view(info: SecretRecord) -> SecretView {
    SecretView {
        revision: info.revision_token(),
        name: info.name.to_string(),
        kind: info.kind.as_str().to_string(),
        description: info.description,
        settings: info.settings,
        fields: info.fields,
        uses: info.uses,
        readable: false,
        incomplete: false,
        created_by: info.created_by.to_string(),
        created_at_ms: info.created_at_ms,
        updated_by: info.updated_by.to_string(),
        updated_at_ms: info.updated_at_ms,
    }
}

pub(crate) fn page(infos: Vec<SecretRecord>, more: bool) -> SecretPage {
    let next_cursor = (more.then(|| infos.last().map(|info| info.name.to_string()))).flatten();
    SecretPage {
        secrets: infos.into_iter().map(view).collect(),
        next_cursor,
    }
}

pub(crate) fn change_error(error: ChangeError) -> ServerError {
    match error {
        ChangeError::Exists => ServerError::Conflict(error.to_string()),
        ChangeError::NotFound => ServerError::NotFound,
        ChangeError::Stale => ServerError::PreconditionFailed(error.to_string()),
        ChangeError::Invalid(error) => ServerError::BadRequestReason(error.to_string()),
    }
}

