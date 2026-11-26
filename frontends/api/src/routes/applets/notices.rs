//! The notice feed of the stateless applet edge: which hosts to drop from its decision cache.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_clock::unix_timestamp_millis;
use aruna_core::applet::notices::{Cursor, NOTICE_KEEP_MS, notice_start, policy_mark};
use aruna_core::structs::identity::features::Feature;
use aruna_ops_work::applets::feed::FeedOperation;
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::edge::edge_auth;
use super::{applet_error, realm_config};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

/// Notices one answer carries at most; the cursor continues after them.
const PAGE: usize = 500;
/// How long the feed waits for a notice when the edge names no wait.
const DEFAULT_WAIT_S: u64 = 20;
const MAX_WAIT_S: u64 = 25;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new().routes(routes!(notices))
}

#[derive(Clone, Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct NoticeQuery {
    /// The `cursor` of the previous answer; absent on the first call.
    pub after: Option<String>,
    /// Seconds to wait for a notice when none is there yet: 20 when absent, at most 25.
    pub wait_s: Option<u64>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AppletNotice)]
pub struct NoticeBody {
    pub applet_id: String,
    /// Drop every cached decision for these hosts.
    pub hosts: Vec<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AppletNotices, example = json!({
    "cursor": "02000001936f2a1c00.9f86d081884c7d65", "reset": false,
    "notices": [{"applet_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2",
        "hosts": ["bakta.apps.example.net", "bakta.lab.example.org"]}]
}))]
pub struct NoticesBody {
    /// Send it back as `after`; it also carries the feature policy the edge decided under.
    pub cursor: String,
    /// Drop every cached decision: the feature policy changed, or the edge fell too far behind.
    pub reset: bool,
    pub notices: Vec<NoticeBody>,
}

/// A digest of everything outside the applet records that changes host decisions.
async fn policy(state: &ServerState) -> ServerResult<u64> {
    let config = realm_config(state).await?;
    let mode = config.features.get(&Feature::Applets);
    let disabled = state.disabled_features().contains(&Feature::Applets);
    let hosting = state.applet_settings().hosting.is_some();
    policy_mark(&(mode, disabled, hosting))
        .map_err(|error| ServerError::InternalError(error.into()))
}

#[utoipa::path(
    get,
    path = "/edge/v1/notices",
    tag = "applets",
    summary = "Follow the changes that invalidate edge decisions",
    description = r#"Answers the hosts whose cached host decisions the edge must drop, after its last cursor.

**Authentication**: the configured edge key in `X-Aruna-Edge-Key`; no user token.

**Behavior**
- An approval, rollback, pause, resume, takedown, new version, embedding or guest change, and a
  custom domain that becomes active, is suspended or is removed each name the applet's hosts.
- `reset` is true on the first call, after a change of the applets feature policy or of this
  node's hosting, and when the cursor is older than one hour; drop the whole cache then.
- Without a notice the call waits up to `wait_s` seconds, then answers an empty list."#,
    params(NoticeQuery),
    responses(
        (status = 200, description = "The notices and the next cursor", body = NoticesBody),
        (status = 401, description = "No valid edge key", body = ErrorResponse),
        (status = 503, description = "This node hosts no applets", body = ErrorResponse)
    )
)]
pub async fn notices(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Query(query): Query<NoticeQuery>,
) -> ServerResult<Json<NoticesBody>> {
    edge_auth(&state, &headers)?;
    let policy = policy(&state).await?;
    let now_ms = unix_timestamp_millis();
    let held = (query.after.as_deref())
        .and_then(Cursor::parse)
        .filter(|cursor| cursor.policy == policy)
        .filter(|cursor| cursor.at_ms().saturating_add(NOTICE_KEEP_MS) >= now_ms);
    let Some(held) = held else {
        let cursor = Cursor {
            after: notice_start(now_ms),
            policy,
        };
        return Ok(Json(NoticesBody {
            cursor: cursor.encode(),
            reset: true,
            notices: Vec::new(),
        }));
    };
    let wait = Duration::from_secs(query.wait_s.unwrap_or(DEFAULT_WAIT_S).min(MAX_WAIT_S));
    let feed = FeedOperation::new(held.after.clone(), PAGE, wait);
    let page = drive(feed, &state.get_ctx()).await.map_err(applet_error)?;
    let cursor = Cursor {
        after: page.last.unwrap_or(held.after),
        policy,
    };
    let notices = (page.notices.into_iter())
        .map(|notice| NoticeBody {
            applet_id: notice.applet_id.to_string(),
            hosts: notice.hosts,
        })
        .collect();
    Ok(Json(NoticesBody {
        cursor: cursor.encode(),
        reset: false,
        notices,
    }))
}
