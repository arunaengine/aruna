//! Lists, creates and reads applets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_ops_work::applets::create::{CreateAppletOperation, Hosting, NewApplet};
use aruna_ops_work::applets::reads::ListAppletsOperation;
use aruna_ops_work::applets::{AppletAccessOperation, Need};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use ulid::Ulid;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::wire::{AppletBody, AppletPageBody, AppletQuery, CreateAppletRequest};
use super::{
    applet_error, applet_id, effective_hosting, group_id, limit, require_feature, serving,
};
use crate::auth::{RealmAuth, UnrestrictedAuth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(list_applets, create_applet))
        .routes(routes!(get_applet))
}

#[utoipa::path(
    get,
    path = "/applets",
    tag = "applets",
    summary = "List the applets of a group",
    description = r#"Lists the applets of one group, paged by applet id.

**Authentication**: realm bearer token with READ on the group's data on this node.

**Behavior**
- Every node that holds the group's applet records answers; changes go to the applet's
  `host_node`.
- `serving` is what the edge serves now: a live applet whose feature is switched off shows
  `feature_paused` and comes back by itself when the feature is on again."#,
    params(AppletQuery),
    responses(
        (status = 200, description = "One page of applets", body = AppletPageBody, example = json!({
            "items": [{
                "applet_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "group_id": "01JABCDEF0123456789ABCDEFG",
                "name": "Bakta web", "slug": "bakta", "host": "bakta.apps.example.net",
                "repository": null, "host_node": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
                "lifecycle": "private", "access": "private", "serving": "gone", "live_version": null,
                "versions": [], "grants": {"read": [], "references": [], "start": null, "submissions": null,
                    "limits": {"cpu_cores": null, "ram_bytes": null, "walltime_ms": null, "upload_bytes": null,
                    "visitor_hourly": null, "visitor_daily": null, "applet_daily": null}},
                "embedding": {"enabled": false, "origins": []}, "guests": null, "domains": [],
                "pending": null, "account": null, "created_by": "01K8W1A0000000000000000000",
                "created_at": "2026-11-24T08:00:00+00:00", "revision": 1
            }],
            "next_cursor": null
        })),
        (status = 400, description = "A malformed group id or cursor", body = ErrorResponse),
        (status = 404, description = "The caller cannot read the group's data", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_applets(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Query(query): Query<AppletQuery>,
) -> ServerResult<Json<AppletPageBody>> {
    let group = group_id(&query.group_id)?;
    let after = (query.cursor.as_deref())
        .map(Ulid::from_string)
        .transpose()
        .map_err(|_| ServerError::BadRequestReason("cursor is not a ULID".to_string()))?;
    let scope = (group, state.get_node_id());
    let list = ListAppletsOperation::new(&auth, scope, after, limit(query.limit));
    let page = drive(list, &state.get_ctx()).await.map_err(applet_error)?;
    let mut items = Vec::with_capacity(page.applets.len());
    for applet in &page.applets {
        items.push(AppletBody::new(applet, serving(&state, applet).await?));
    }
    Ok(Json(AppletPageBody {
        items,
        next_cursor: page.next.map(|id| id.to_string()),
    }))
}

#[utoipa::path(
    post,
    path = "/applets",
    tag = "applets",
    summary = "Create an applet",
    description = r#"Creates a private applet in a group, hosted on this node at `<slug>.<applet domain>`.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's data on
this node.

**Behavior**
- The applet starts private, without versions, grants, embedding or custom domains.
- `repository` names a metadata document of the group whose Git repository holds the source.
- This node decides about the applet from now on and keeps the files of its versions.

**Errors**
- 409 when the host is taken or applets are switched off for the group.
- 503 when this node hosts no applets."#,
    request_body(content = CreateAppletRequest, example = json!({
        "group_id": "01JABCDEF0123456789ABCDEFG", "name": "Bakta web", "slug": "bakta",
        "repository": "01K8W1C0000000000000000000"
    })),
    responses(
        (status = 201, description = "The new private applet", body = AppletBody, example = json!({
            "applet_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "group_id": "01JABCDEF0123456789ABCDEFG",
            "name": "Bakta web", "slug": "bakta", "host": "bakta.apps.example.net",
            "repository": "01K8W1C0000000000000000000",
            "host_node": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
            "lifecycle": "private", "access": "private", "serving": "gone", "live_version": null,
            "versions": [], "grants": {"read": [], "references": [], "start": null, "submissions": null,
                "limits": {"cpu_cores": null, "ram_bytes": null, "walltime_ms": null, "upload_bytes": null,
                "visitor_hourly": null, "visitor_daily": null, "applet_daily": null}},
            "embedding": {"enabled": false, "origins": []}, "guests": null, "domains": [],
            "pending": null, "account": null, "created_by": "01K8W1A0000000000000000000",
            "created_at": "2026-11-24T08:00:00+00:00", "revision": 1
        })),
        (status = 400, description = "A malformed group id, name, slug or repository", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group's data, or a path-restricted token", body = ErrorResponse),
        (status = 409, description = "The host is taken, or applets are off for the group", body = ErrorResponse),
        (status = 503, description = "This node hosts no applets", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_applet(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Json(request): Json<CreateAppletRequest>,
) -> ServerResult<(StatusCode, Json<AppletBody>)> {
    let group = group_id(&request.group_id)?;
    let repository = (request.repository.as_deref())
        .map(Ulid::from_string)
        .transpose()
        .map_err(|_| ServerError::BadRequestReason("repository is not a ULID".to_string()))?;
    let applet_domain = effective_hosting(&state).await?.applet_domain.clone();
    require_feature(&state, group).await?;
    let request = NewApplet {
        group_id: group,
        name: request.name,
        slug: request.slug,
        repository,
    };
    let hosting = Hosting {
        node: state.get_node_id(),
        applet_domain,
        visitor_salt: rand::random(),
    };
    let create = CreateAppletOperation::new(&auth, request, hosting);
    let applet = drive(create, &state.get_ctx())
        .await
        .map_err(applet_error)?;
    let body = AppletBody::new(&applet, serving(&state, &applet).await?);
    Ok((StatusCode::CREATED, Json(body)))
}

#[utoipa::path(
    get,
    path = "/applets/{applet_id}",
    tag = "applets",
    summary = "Read an applet",
    description = r#"Returns one applet with its versions, approved grants, hosts and pending approval.

**Authentication**: realm bearer token with READ on the group's data on the applet's host node.

**Behavior**
- `revision` rises with every change; send it back as `expected_revision` so a change made
  meanwhile is never overwritten."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    responses(
        (status = 200, description = "The applet", body = AppletBody, example = json!({
            "applet_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "group_id": "01JABCDEF0123456789ABCDEFG",
            "name": "Bakta web", "slug": "bakta", "host": "bakta.apps.example.net", "repository": null,
            "host_node": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
            "lifecycle": "live", "access": "public", "serving": "live", "live_version": 1,
            "versions": [{"version": 1, "commit": "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
                "state": "approved", "created_by": "01K8W1A0000000000000000000",
                "created_at": "2026-11-24T08:00:00+00:00", "approved_by": "01K8W1A0000000000000000001"}],
            "grants": {"read": [], "references": [], "start": null, "submissions": null,
                "limits": {"cpu_cores": null, "ram_bytes": null, "walltime_ms": null, "upload_bytes": null,
                "visitor_hourly": null, "visitor_daily": null, "applet_daily": null}},
            "embedding": {"enabled": false, "origins": []}, "guests": null, "domains": [],
            "pending": null, "account": null, "created_by": "01K8W1A0000000000000000000",
            "created_at": "2026-11-24T08:00:00+00:00", "revision": 3
        })),
        (status = 404, description = "No such applet, or the caller cannot read its group's data", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_applet(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(id): Path<String>,
) -> ServerResult<Json<AppletBody>> {
    let read = AppletAccessOperation::new(&auth, applet_id(&id)?, Need::Read);
    let applet = drive(read, &state.get_ctx()).await.map_err(applet_error)?;
    Ok(Json(AppletBody::new(
        &applet,
        serving(&state, &applet).await?,
    )))
}
