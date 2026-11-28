//! Serves workflow automations and automation runs over REST/OpenAPI under `/compute/`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod events;
pub mod lifecycle;
pub mod records;
#[cfg(test)]
mod route_tests;
#[cfg(test)]
mod run_tests;
pub mod run_wire;
pub mod runs;
pub mod save;
pub mod spec_wire;
pub mod stage_wire;
pub mod wire;

use std::sync::Arc;

use aruna_ops_work::automations::AutoError;
use ulid::Ulid;
use utoipa_axum::router::OpenApiRouter;

use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 500;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    records::router()
        .merge(save::router())
        .merge(lifecycle::router())
        .merge(events::router())
        .merge(runs::router())
}

pub(crate) fn limit(asked: Option<usize>) -> usize {
    asked.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

pub(crate) fn path_id(text: &str) -> ServerResult<Ulid> {
    Ulid::from_string(text).map_err(|_| ServerError::NotFound)
}

pub(crate) fn param_id(text: &str, name: &str) -> ServerResult<Ulid> {
    Ulid::from_string(text)
        .map_err(|_| ServerError::BadRequestReason(format!("{name} is not a ULID")))
}

pub(crate) fn auto_error(error: AutoError) -> ServerError {
    match error {
        AutoError::NotFound => ServerError::NotFound,
        AutoError::Forbidden => ServerError::Forbidden,
        AutoError::FeatureOff(feature, state) => ServerError::FeatureOff(feature, state),
        AutoError::Invalid(msg) => ServerError::BadRequestReason(msg),
        AutoError::Conflict(msg) | AutoError::Capacity(msg) => ServerError::Conflict(msg),
        AutoError::Limited {
            reason,
            retry_after,
        } => ServerError::Limited {
            code: "automation_limited",
            message: reason,
            retry_after,
        },
        AutoError::Unavailable(msg) => ServerError::ServiceUnavailableReason(msg),
        AutoError::Storage(msg) => ServerError::InternalError(msg.into()),
    }
}
