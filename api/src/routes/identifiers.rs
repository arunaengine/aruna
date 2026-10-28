//! Identifier rules and the links they find: request and response shapes, and the route that
//! tries rules on a sample without storing them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::identifier::{
    CompiledParser, Identifier, IdentifierParser, IdentifierRule, IdentifierTarget,
};
use axum::extract::State;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::require_realm_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi()]
pub struct IdentifiersApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(IdentifiersApiDoc::openapi()).routes(routes!(test_rules))
}

/// What a rule reads: the object key or the recorded source URL.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum IdentifierTargetBody {
    Key,
    SourceUrl,
}

/// One identifier rule of a bucket.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct IdentifierRuleBody {
    /// Shown to users, for example `BioSample`.
    pub name: String,
    /// A Rust regex with one capture group named `id`.
    pub pattern: String,
    pub target: IdentifierTargetBody,
    /// The landing page with `{id}`; it must give an http or https URL.
    pub url_template: String,
}

/// The identifier rules of a bucket, applied in order.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct IdentifierParserBody {
    pub rules: Vec<IdentifierRuleBody>,
}

impl From<IdentifierTargetBody> for IdentifierTarget {
    fn from(target: IdentifierTargetBody) -> Self {
        match target {
            IdentifierTargetBody::Key => Self::Key,
            IdentifierTargetBody::SourceUrl => Self::SourceUrl,
        }
    }
}

impl From<IdentifierTarget> for IdentifierTargetBody {
    fn from(target: IdentifierTarget) -> Self {
        match target {
            IdentifierTarget::Key => Self::Key,
            IdentifierTarget::SourceUrl => Self::SourceUrl,
        }
    }
}

impl From<IdentifierParserBody> for IdentifierParser {
    fn from(body: IdentifierParserBody) -> Self {
        let rules = body.rules.into_iter().map(|rule| IdentifierRule {
            name: rule.name,
            pattern: rule.pattern,
            target: rule.target.into(),
            url_template: rule.url_template,
        });
        Self {
            rules: rules.collect(),
        }
    }
}

impl From<IdentifierParser> for IdentifierParserBody {
    fn from(parser: IdentifierParser) -> Self {
        let rules = parser.rules.into_iter().map(|rule| IdentifierRuleBody {
            name: rule.name,
            pattern: rule.pattern,
            target: rule.target.into(),
            url_template: rule.url_template,
        });
        Self {
            rules: rules.collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ParserTestRequest {
    pub parser: IdentifierParserBody,
    /// An object key to try the key rules on.
    #[serde(default)]
    pub key: String,
    /// A source URL to try the source URL rules on.
    pub source_url: Option<String>,
}

/// One identifier found in a key or source URL.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct IdentifierView {
    pub name: String,
    pub value: String,
    pub url: String,
    pub target: IdentifierTargetBody,
}

impl From<Identifier> for IdentifierView {
    fn from(found: Identifier) -> Self {
        Self {
            name: found.name,
            value: found.value,
            url: found.url,
            target: found.target.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ParserTestResponse {
    pub identifiers: Vec<IdentifierView>,
}

#[utoipa::path(
    post,
    path = "/data/identifier-rules/test",
    tag = "data/storage",
    summary = "Try identifier rules",
    description = r#"Applies identifier rules to a sample key and source URL and returns the links they find.

**Authentication**: realm bearer token.

**Behavior**
- Stores nothing.
- Uses the same regex engine as origin reads, so the result matches what file details show.
- Returns 400 with the first invalid rule.

**Limits**
- One set of rules per call."#,
    request_body(content = ParserTestRequest, example = json!({
        "parser": {"rules": [{
            "name": "BioSample",
            "pattern": "(?P<id>SAM(?:N|EA|D)\\d+)",
            "target": "key",
            "url_template": "https://www.ebi.ac.uk/biosamples/samples/{id}"
        }]},
        "key": "SAMN02138552/contigs.fna.gz",
        "source_url": null
    })),
    responses(
        (status = 200, description = "Identifiers found", body = ParserTestResponse, example = json!({
            "identifiers": [{
                "name": "BioSample",
                "value": "SAMN02138552",
                "url": "https://www.ebi.ac.uk/biosamples/samples/SAMN02138552",
                "target": "key"
            }]
        })),
        (status = 400, description = "Invalid rule", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn test_rules(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<ParserTestRequest>,
) -> ServerResult<Json<ParserTestResponse>> {
    require_realm_auth(&state, auth)?;
    let parser = CompiledParser::compile(&request.parser.into())
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    let found = parser.find(&request.key, request.source_url.as_deref());
    Ok(Json(ParserTestResponse {
        identifiers: found.into_iter().map(Into::into).collect(),
    }))
}

#[cfg(test)]
#[path = "identifiers_tests.rs"]
mod tests;
