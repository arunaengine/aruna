//! Relays a REST write that reached a member of a distributed bucket to the coordinator of the
//! key's range, and a read to a holder, with the caller's bearer token (D12). The target checks
//! the token and the caller's permissions again on its own path; an unreachable coordinator
//! fails the write.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::LazyLock;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::distribution::WriteRoute;
use aruna_operations::distribution::delegate::node_urls;
use aruna_operations::distribution::guard::route_write;
use aruna_operations::distribution::status::node_reach;
use axum::http::{HeaderMap, StatusCode, header};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::warn;
use ulid::Ulid;

use crate::auth::ValidatedBearer;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::management_relay::RELAY_HOP_HEADER;
use crate::routes::storage::distributed::map_guard_error;
use crate::server::state::ServerState;

const API_PREFIX: &str = "/api/v1";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .build()
        .unwrap_or_default()
});

/// The coordinator a REST write goes to when this node does not coordinate the key's range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RestForward {
    pub set_id: Ulid,
    pub range_id: Ulid,
    pub node_id: NodeId,
}

/// The coordinator a REST write to `key` goes to, or `None` when this node writes it. A request
/// another node already relayed is never relayed again.
pub(crate) async fn rest_target(
    state: &ServerState,
    bucket: &BucketInfo,
    key: &str,
    headers: &HeaderMap,
) -> ServerResult<Option<RestForward>> {
    let Some(binding) = bucket.distribution.as_ref() else {
        return Ok(None);
    };
    if headers.contains_key(RELAY_HOP_HEADER) {
        return Ok(None);
    }
    match route_write(state.get_ctx().as_ref(), binding, key).await {
        Ok(WriteRoute::Local { .. }) => Ok(None),
        Ok(WriteRoute::Forward {
            range_id, node_id, ..
        }) => Ok(Some(RestForward {
            set_id: binding.set_id,
            range_id,
            node_id,
        })),
        Err(error) => Err(map_guard_error(error)),
    }
}

/// `reach` says since when this node has not heard from the coordinator.
fn unreachable(forward: &RestForward, reach: &str) -> ServerError {
    ServerError::ServiceUnavailableReason(format!(
        "range {} of distributed bucket {} is coordinated by node {}, which is {reach}; \
         the bucket is degraded and writes to this range fail until the coordinator returns",
        forward.range_id, forward.set_id, forward.node_id
    ))
}

/// The coordinator's refusal as this node's answer.
fn coordinator_error(forward: &RestForward, status: StatusCode, body: &str) -> ServerError {
    let message = serde_json::from_str::<ErrorResponse>(body)
        .map(|error| error.error)
        .unwrap_or_default();
    match status {
        StatusCode::BAD_REQUEST => ServerError::BadRequestMessage(message),
        StatusCode::UNAUTHORIZED => ServerError::Unauthorized,
        StatusCode::FORBIDDEN => ServerError::Forbidden,
        StatusCode::NOT_FOUND => ServerError::NotFound,
        StatusCode::CONFLICT => ServerError::Conflict(message),
        StatusCode::SERVICE_UNAVAILABLE => ServerError::ServiceUnavailableReason(message),
        _ => ServerError::BadGatewayReason(format!(
            "coordinator node {} answered {status}: {message}",
            forward.node_id
        )),
    }
}

/// The url of `path` on a node's published api base, which may already carry the nest.
fn relay_url(base: &str, path: &str) -> String {
    let base = base.trim_end_matches('/');
    match base.ends_with(API_PREFIX) {
        true => format!("{base}{path}"),
        false => format!("{base}{API_PREFIX}{path}"),
    }
}

/// Sends the same JSON request to `path` on the coordinator and decodes its answer.
pub(crate) async fn relay_json<T: DeserializeOwned>(
    state: &ServerState,
    forward: &RestForward,
    path: &str,
    bearer: Option<&ValidatedBearer>,
    body: &impl Serialize,
) -> ServerResult<(StatusCode, T)> {
    let bearer = bearer.ok_or(ServerError::Unauthorized)?;
    let urls = node_urls(state.get_ctx().as_ref(), forward.node_id).await;
    let Some(base) = urls.and_then(|urls| urls.api) else {
        return Err(unreachable(
            forward,
            &node_reach(state.get_ctx().as_ref(), forward.node_id).hint(),
        ));
    };
    let response = CLIENT
        .post(relay_url(&base, path))
        .header(header::AUTHORIZATION, format!("Bearer {}", bearer.as_str()))
        .header(RELAY_HOP_HEADER, state.get_node_id().to_string())
        .json(body)
        .send()
        .await;
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            warn!(
                node_id = %forward.node_id,
                connect = error.is_connect(),
                error = %error,
                "A distributed bucket coordinator did not take a relayed write"
            );
            return Err(unreachable(
                forward,
                &node_reach(state.get_ctx().as_ref(), forward.node_id).hint(),
            ));
        }
    };
    answer(forward, response).await
}

/// A node's successful answer decoded, or its refusal as this node's answer.
async fn answer<T: DeserializeOwned>(
    forward: &RestForward,
    response: reqwest::Response,
) -> ServerResult<(StatusCode, T)> {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(coordinator_error(forward, status, &body));
    }
    let answer = serde_json::from_str(&body).map_err(|error| {
        ServerError::BadGatewayReason(format!("the coordinator answer does not decode: {error}"))
    })?;
    Ok((status, answer))
}

/// Sends a GET of `path` to the forward's node with the caller's bearer token and decodes its
/// answer; `None` when the node did not answer, so the caller may ask another holder.
pub(crate) async fn relay_get<T: DeserializeOwned>(
    state: &ServerState,
    forward: &RestForward,
    path: &str,
    bearer: Option<&ValidatedBearer>,
) -> ServerResult<Option<T>> {
    let bearer = bearer.ok_or(ServerError::Unauthorized)?;
    let urls = node_urls(state.get_ctx().as_ref(), forward.node_id).await;
    let Some(base) = urls.and_then(|urls| urls.api) else {
        return Ok(None);
    };
    let response = CLIENT
        .get(relay_url(&base, path))
        .header(header::AUTHORIZATION, format!("Bearer {}", bearer.as_str()))
        .header(RELAY_HOP_HEADER, state.get_node_id().to_string())
        .send()
        .await;
    match response {
        Ok(response) => Ok(Some(answer(forward, response).await?.1)),
        Err(error) => {
            warn!(
                node_id = %forward.node_id,
                connect = error.is_connect(),
                error = %error,
                "A distributed bucket holder did not take a relayed read"
            );
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn forward() -> RestForward {
        RestForward {
            set_id: Ulid::from_bytes([1; 16]),
            range_id: Ulid::from_bytes([2; 16]),
            node_id: iroh::SecretKey::from_bytes(&[3; 32]).public(),
        }
    }

    #[test]
    fn urls_keep_nest() {
        assert_eq!(
            relay_url("https://a.example/", "/data/staging"),
            "https://a.example/api/v1/data/staging"
        );
        assert_eq!(
            relay_url("https://a.example/api/v1", "/data/staging"),
            "https://a.example/api/v1/data/staging"
        );
    }

    #[test]
    fn refusals_keep_status() {
        let body = r#"{"error":"bucket changed"}"#;
        assert!(matches!(
            coordinator_error(&forward(), StatusCode::CONFLICT, body),
            ServerError::Conflict(message) if message == "bucket changed"
        ));
        assert!(matches!(
            coordinator_error(&forward(), StatusCode::FORBIDDEN, body),
            ServerError::Forbidden
        ));
        let ServerError::ServiceUnavailableReason(message) = unreachable(&forward(), "unreachable")
        else {
            panic!("an unreachable coordinator answers 503");
        };
        assert!(message.contains("degraded"));
    }
}
