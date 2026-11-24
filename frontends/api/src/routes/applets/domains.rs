//! Adds, checks and removes the custom domains of an applet.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::applet::hosts::custom_domain;
use aruna_core::applet::record::Applet;
use aruna_ops_work::applets::change::{AppletChange, ChangeAppletOperation};
use aruna_ops_work::applets::domains::VerifyDomainOperation;
use aruna_ops_work::applets::{AppletAccessOperation, Need};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use serde::Deserialize;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::wire::{DomainBody, domain_body};
use super::{applet_error, applet_id, hosting, require_feature};
use crate::auth::UnrestrictedAuth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(list_domains, add_domain))
        .routes(routes!(remove_domain))
        .routes(routes!(verify_domain))
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(example = json!({"host": "bakta.lab.example.org"}))]
pub struct DomainRequest {
    /// A subdomain of three labels or more outside this node's applet domain.
    pub host: String,
}

fn domain_of(applet: &Applet, host: &str) -> ServerResult<DomainBody> {
    (applet.domains.iter())
        .find(|domain| domain.host == host)
        .map(domain_body)
        .ok_or(ServerError::NotFound)
}

#[utoipa::path(
    get,
    path = "/applets/{applet_id}/domains",
    tag = "applets",
    summary = "List an applet's custom domains",
    description = r#"Lists the custom domains of an applet with their check state and the TXT value each must publish.

**Authentication**: realm bearer token without path restrictions, with READ on the group's data
on the applet's host node."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    responses(
        (status = 200, description = "The custom domains", body = Vec<DomainBody>,
            example = json!([{"host": "bakta.lab.example.org", "state": "active",
            "txt_value": "aruna-applet=9f86d081884c7d659a2feaa0c55ad015",
            "checked_at": "2026-11-24T08:00:00+00:00"}])),
        (status = 404, description = "No such applet, or no READ", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_domains(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
) -> ServerResult<Json<Vec<DomainBody>>> {
    let read = AppletAccessOperation::new(&auth, applet_id(&id)?, Need::Read);
    let applet = drive(read, &state.get_ctx()).await.map_err(applet_error)?;
    Ok(Json(applet.domains.iter().map(domain_body).collect()))
}

#[utoipa::path(
    post,
    path = "/applets/{applet_id}/domains",
    tag = "applets",
    summary = "Add a custom domain",
    description = r#"Adds a custom domain that waits for its DNS check.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's data
on the applet's host node.

**Behavior**
- Publish the answered `txt_value` as the TXT record `_aruna-verify.<host>` and point the host
  at the applet host with a CNAME (or flattened A and AAAA records), then ask for a check.
- The domain serves only once a check passes; two failed checks in a row suspend it."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body = DomainRequest,
    responses(
        (status = 201, description = "The pending domain", body = DomainBody,
            example = json!({"host": "bakta.lab.example.org", "state": "pending",
            "txt_value": "aruna-applet=9f86d081884c7d659a2feaa0c55ad015",
            "checked_at": "2026-11-24T08:00:00+00:00"})),
        (status = 400, description = "Not a subdomain outside the applet domain", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group's data", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 409, description = "Already added, custom domains or applets off, or hosted elsewhere", body = ErrorResponse),
        (status = 503, description = "This node hosts no applets", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn add_domain(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<DomainRequest>,
) -> ServerResult<(StatusCode, Json<DomainBody>)> {
    let settings = hosting(&state)?;
    if !settings.custom_domains {
        return Err(ServerError::Conflict(
            "this node does not serve custom domains".to_string(),
        ));
    }
    let host = custom_domain(&body.host, &settings.applet_domain)
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    let read = AppletAccessOperation::new(&auth, applet_id(&id)?, Need::Write);
    let record = drive(read, &state.get_ctx()).await.map_err(applet_error)?;
    require_feature(&state, record.group_id).await?;
    let nonce = hex::encode(rand::random::<[u8; 16]>());
    let change = AppletChange::AddDomain {
        host: host.clone(),
        nonce,
    };
    let key = (record.applet_id, None);
    let write = ChangeAppletOperation::new(&auth, key, change, state.get_node_id());
    let applet = drive(write, &state.get_ctx()).await.map_err(applet_error)?;
    Ok((StatusCode::CREATED, Json(domain_of(&applet, &host)?)))
}

#[utoipa::path(
    delete,
    path = "/applets/{applet_id}/domains/{domain}",
    tag = "applets",
    summary = "Remove a custom domain",
    description = r#"Removes a custom domain; it stops serving at once.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's data
on the applet's host node."#,
    params(
        ("applet_id" = String, Path, description = "Applet id; an unparseable id is 404"),
        ("domain" = String, Path, description = "The custom domain")
    ),
    responses(
        (status = 204, description = "The domain is gone"),
        (status = 403, description = "No WRITE on the group's data", body = ErrorResponse),
        (status = 404, description = "No such applet or domain", body = ErrorResponse),
        (status = 409, description = "The applet is hosted elsewhere", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn remove_domain(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path((id, domain)): Path<(String, String)>,
) -> ServerResult<StatusCode> {
    let change = AppletChange::RemoveDomain {
        host: domain.trim().trim_end_matches('.').to_ascii_lowercase(),
    };
    let key = (applet_id(&id)?, None);
    let write = ChangeAppletOperation::new(&auth, key, change, state.get_node_id());
    drive(write, &state.get_ctx()).await.map_err(applet_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/applets/{applet_id}/domains/{domain}/verify",
    tag = "applets",
    summary = "Check a custom domain now",
    description = r#"Checks a custom domain in DNS at once and records the result.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's data
on the applet's host node.

**Behavior**
- The check passes when `_aruna-verify.<host>` holds the domain's TXT value and the host
  resolves to at least one address of the applet host. A domain another applet serves fails.
- A pass makes the domain active; two failures in a row suspend an active domain. The node
  repeats the check on its own."#,
    params(
        ("applet_id" = String, Path, description = "Applet id; an unparseable id is 404"),
        ("domain" = String, Path, description = "The custom domain")
    ),
    responses(
        (status = 200, description = "The domain after the check", body = DomainBody,
            example = json!({"host": "bakta.lab.example.org", "state": "active",
            "txt_value": "aruna-applet=9f86d081884c7d659a2feaa0c55ad015",
            "checked_at": "2026-11-24T08:00:00+00:00"})),
        (status = 403, description = "No WRITE on the group's data", body = ErrorResponse),
        (status = 404, description = "No such applet or domain", body = ErrorResponse),
        (status = 500, description = "DNS could not be asked", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn verify_domain(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path((id, domain)): Path<(String, String)>,
) -> ServerResult<Json<DomainBody>> {
    let host = domain.trim().trim_end_matches('.').to_ascii_lowercase();
    let check = VerifyDomainOperation::new(Some(&auth), (applet_id(&id)?, &host));
    let applet = drive(check, &state.get_ctx()).await.map_err(applet_error)?;
    Ok(Json(domain_of(&applet, &host)?))
}
