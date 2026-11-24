//! Serves applets: records and their lifecycle, and the settings of their hosting node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod builds;
pub mod domains;
pub mod edge;
pub mod lifecycle;
pub mod node;
pub mod notices;
pub mod preview;
pub mod records;
pub mod review;
pub mod signin;
pub mod source;
pub mod wire;

use std::sync::Arc;

use aruna_core::applet::record::{Applet, Serving};
use aruna_core::applet::settings::HostingSettings;
use aruna_core::structs::identity::features::{Feature, resolve};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::GroupId;
use aruna_ops_identity::document_repository::GetConfigOperation;
use aruna_ops_work::applets::AppletError;
use aruna_runner::driver::drive;
use ulid::Ulid;
use utoipa_axum::router::OpenApiRouter;

use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;

/// Items of a page when the caller names no limit.
const DEFAULT_LIMIT: usize = 50;
/// The most items one page holds.
const MAX_LIMIT: usize = 500;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    (records::router())
        .merge(lifecycle::router())
        .merge(review::router())
        .merge(source::router())
        .merge(builds::router())
        .merge(domains::router())
        .merge(edge::router())
        .merge(preview::router())
        .merge(notices::router())
        .merge(signin::router())
        .merge(node::router())
}

pub(crate) fn applet_error(error: AppletError) -> ServerError {
    match error {
        AppletError::NotFound => ServerError::NotFound,
        AppletError::Forbidden => ServerError::Forbidden,
        AppletError::Invalid(reason) => ServerError::BadRequestReason(reason),
        AppletError::Conflict(reason) => ServerError::Conflict(reason),
        AppletError::Limited(reason) => ServerError::Refused(
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            "applet_limited",
            reason,
        ),
        AppletError::Elsewhere(node) => ServerError::Conflict(format!(
            "the applet is hosted on node {node}; send changes there"
        )),
        AppletError::Storage(reason) => ServerError::InternalError(reason.into()),
    }
}

pub(crate) fn applet_id(text: &str) -> ServerResult<Ulid> {
    Ulid::from_string(text).map_err(|_| ServerError::NotFound)
}

pub(crate) fn group_id(text: &str) -> ServerResult<GroupId> {
    Ulid::from_string(text)
        .map_err(|_| ServerError::BadRequestReason("group_id is not a ULID".to_string()))
}

/// The hosting settings of this node; a node that hosts no applets refuses the call.
pub(crate) fn hosting(state: &ServerState) -> ServerResult<&HostingSettings> {
    (state.applet_settings().hosting.as_ref()).ok_or_else(|| {
        ServerError::ServiceUnavailableReason("this node hosts no applets".to_string())
    })
}

pub(super) async fn realm_config(state: &ServerState) -> ServerResult<RealmConfigDocument> {
    drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::ServiceUnavailableReason(error.to_string()))
}

/// Whether the applets feature is on for `group_id` on this node.
pub(crate) async fn realm_feature(state: &ServerState, group_id: GroupId) -> ServerResult<bool> {
    let config = realm_config(state).await?;
    Ok(resolve(
        &config,
        &state.disabled_features(),
        Feature::Applets,
        Some(&group_id),
    )
    .on)
}

/// Refuses work on applets of `group_id` while the applets feature is off for it.
pub(crate) async fn require_feature(state: &ServerState, group_id: GroupId) -> ServerResult<()> {
    crate::features::require(state, Feature::Applets, Some(group_id)).await
}

/// What the edge serves for `applet` now, from the feature state and this node's hosting.
pub(crate) async fn serving(state: &ServerState, applet: &Applet) -> ServerResult<Serving> {
    let feature = realm_feature(state, applet.group_id).await?;
    Ok(applet.serving(feature, state.applet_settings().hosting.is_some()))
}

fn limit(asked: Option<usize>) -> usize {
    asked.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}
