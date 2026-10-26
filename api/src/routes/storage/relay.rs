//! Relays a REST write on a distributed bucket member to the key range's coordinator, and a read
//! to a holder, with the caller's bearer token (D12). An unreachable coordinator fails the write.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The target checks the token and the caller's permissions again on its own path.

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
/// The distributed bucket a relayed request is meant for; its target refuses another bucket.
pub(crate) const RELAY_SET_HEADER: &str = "x-aruna-relay-set";
/// Bytes of a node's answer read at most; a longer answer fails instead of filling memory.
pub(crate) const MAX_ANSWER_BYTES: usize = 16 * 1024 * 1024;

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

/// Refuses a relayed request when the local bucket of the name is not the member of the
/// distributed bucket the relaying node meant, for example after it was deleted and recreated.
pub(crate) fn relayed_set(bucket: &BucketInfo, headers: &HeaderMap) -> ServerResult<()> {
    let named = headers.get(RELAY_SET_HEADER).map(|value| value.to_str());
    let bound = bucket.distribution.as_ref().map(|binding| binding.set_id);
    let relayed = headers.contains_key(RELAY_HOP_HEADER);
    match (named, bound) {
        (None, None) => Ok(()),
        (None, Some(_)) if !relayed => Ok(()),
        (Some(Ok(named)), Some(set_id)) if named == set_id.to_string() => Ok(()),
        _ => Err(ServerError::Conflict(
            "the bucket of this name is not the member of the distributed bucket the \
             relaying node named"
                .into(),
        )),
    }
}

/// The body of a node's answer, at most [`MAX_ANSWER_BYTES`]; a broken body is an error.
pub(crate) async fn capped_body(mut response: reqwest::Response) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
        if body.len() + chunk.len() > MAX_ANSWER_BYTES {
            return Err(format!("the answer exceeds {MAX_ANSWER_BYTES} bytes"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// The coordinator a REST write to `key` goes to, or `None` when this node writes it. A request
/// another node already relayed is never relayed again.
pub(crate) async fn rest_target(
    state: &ServerState,
    bucket: &BucketInfo,
    key: &str,
    headers: &HeaderMap,
) -> ServerResult<Option<RestForward>> {
    relayed_set(bucket, headers)?;
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
pub(crate) fn relay_url(base: &str, path: &str) -> String {
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
        .header(RELAY_SET_HEADER, forward.set_id.to_string())
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
    let body = match capped_body(response).await {
        Ok(body) => body,
        Err(_) if !status.is_success() => Vec::new(),
        Err(error) => {
            return Err(ServerError::BadGatewayReason(format!(
                "node {} answered incompletely: {error}",
                forward.node_id
            )));
        }
    };
    let body = String::from_utf8_lossy(&body);
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
        .header(RELAY_SET_HEADER, forward.set_id.to_string())
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

    #[tokio::test]
    async fn answers_are_capped() {
        let answer = |body: Vec<u8>| reqwest::Response::from(http::Response::new(body));
        assert_eq!(capped_body(answer(b"{}".to_vec())).await.unwrap(), b"{}");
        let oversized = vec![b'x'; MAX_ANSWER_BYTES + 1];
        assert!(capped_body(answer(oversized)).await.is_err());
    }

    #[test]
    fn relays_name_set() {
        let set_id = Ulid::from_bytes([1; 16]);
        use aruna_core::structs::storage::distribution::{
            DistributionBinding, DistributionRevision,
        };
        let bucket = |distribution| BucketInfo {
            group_id: Ulid::from_bytes([3; 16]),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
            distribution,
        };
        let member = bucket(Some(DistributionBinding {
            set_id,
            member_id: Ulid::from_bytes([2; 16]),
            incarnation: 5,
            revision: DistributionRevision {
                number: 1,
                digest: [0; 32],
            },
        }));
        let mut relayed = HeaderMap::new();
        relayed.insert(RELAY_HOP_HEADER, "node".parse().unwrap());
        // A relay that names no set, or another one, reaches no member bucket.
        assert!(relayed_set(&member, &relayed).is_err());
        relayed.insert(RELAY_SET_HEADER, set_id.to_string().parse().unwrap());
        assert!(relayed_set(&member, &relayed).is_ok());
        // A recreated ordinary bucket of the name refuses the relay meant for the set.
        assert!(relayed_set(&bucket(None), &relayed).is_err());
        assert!(relayed_set(&member, &HeaderMap::new()).is_ok());
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
        for (status, mapped) in [
            (StatusCode::BAD_REQUEST, "400"),
            (StatusCode::UNAUTHORIZED, "401"),
            (StatusCode::NOT_FOUND, "404"),
            (StatusCode::SERVICE_UNAVAILABLE, "503"),
            (StatusCode::INTERNAL_SERVER_ERROR, "502"),
        ] {
            let name = match coordinator_error(&forward(), status, body) {
                ServerError::BadRequestMessage(message) if message == "bucket changed" => "400",
                ServerError::Unauthorized => "401",
                ServerError::NotFound => "404",
                ServerError::ServiceUnavailableReason(_) => "503",
                ServerError::BadGatewayReason(_) => "502",
                other => panic!("unexpected mapping {other:?}"),
            };
            assert_eq!(name, mapped);
        }
        let ServerError::ServiceUnavailableReason(message) = unreachable(&forward(), "unreachable")
        else {
            panic!("an unreachable coordinator answers 503");
        };
        assert!(message.contains("degraded"));
    }
}
