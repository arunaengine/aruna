//! Relays a REST write on a distributed bucket member to the key range's coordinator, and a read
//! to a holder, with the caller's bearer token (D12). An unreachable coordinator fails the write.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The target checks the token and the caller's permissions again on its own path.

use aruna_operations::driver::drive;

use aruna_core::NodeId;
use aruna_core::compute::{SecretBytes, SharedSecret};
use aruna_core::member_read::MemberBody;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_operations::distribution::status::node_reach;
use aruna_operations::s3::object::member_read::{
    MemberFailure, MemberRead, MemberReadOperation, MemberRefusal, MemberTextOperation,
};
use aruna_operations::s3::object::member_rest::{
    RestAnswer, RestAsk, RestCall, RestCallOperation, rest_url,
};
use aruna_operations::s3::object::member_write::WriteTargetOperation;
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use serde::Serialize;
use serde::de::DeserializeOwned;
use ulid::Ulid;

use crate::auth::ValidatedBearer;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::lookup::map_guard_error;
use crate::routes::management_relay::RELAY_HOP_HEADER;
use crate::server::state::ServerState;

/// The distributed bucket a relayed request is meant for; its target refuses another bucket.
pub(crate) const RELAY_SET_HEADER: &str = "x-aruna-relay-set";
/// The node a node-directed read is meant for; the target refuses a header naming another node.
pub(crate) const RELAY_NODE_HEADER: &str = "x-aruna-relay-node";
/// Bytes of a node's answer read at most; a longer answer fails instead of filling memory.
pub(crate) const MAX_ANSWER_BYTES: usize = 16 * 1024 * 1024;

/// The coordinator a REST write goes to when this node does not coordinate the key's range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RestForward {
    pub set_id: Ulid,
    pub range_id: Ulid,
    pub node_id: NodeId,
}

/// One REST read of a key that another member may have to answer.
pub(crate) struct RestRead<'a> {
    pub info: &'a BucketInfo,
    pub key: &'a str,
    pub headers: &'a HeaderMap,
    pub path: &'a str,
    pub bearer: Option<&'a ValidatedBearer>,
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
    let relayed = headers.contains_key(RELAY_HOP_HEADER);
    let target = WriteTargetOperation::new(binding, key, false, relayed);
    let target = drive(target, state.get_ctx().as_ref()).await;
    Ok(target.map_err(map_guard_error)?.map(|target| RestForward {
        set_id: target.set_id,
        range_id: target.range_id,
        node_id: target.node_id,
    }))
}

/// `route` with the query `pairs`, for a holder.
pub(crate) fn query_path(route: &str, pairs: &[(&str, &str)]) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query.extend_pairs(pairs);
    format!("{route}?{}", query.finish())
}

/// 404 from a copy that answered for range `range_id` while its coordinator could not.
pub(crate) fn stale_miss(range_id: Ulid) -> ServerError {
    ServerError::Refused(
        StatusCode::NOT_FOUND,
        "stale_not_found",
        format!(
            "not found on a copy of range {range_id}, which answered because the range's \
             coordinator could not; the copy may be behind, so the object may exist"
        ),
    )
}

fn bearer_secret(bearer: Option<&ValidatedBearer>) -> Option<SharedSecret> {
    bearer.map(|bearer| SharedSecret::new(SecretBytes::new(bearer.as_str().as_bytes().to_vec())))
}

/// The headers of `sent` that can travel as text.
fn sent_pairs(sent: &HeaderMap) -> Vec<(String, String)> {
    (sent.iter())
        .filter_map(|(name, value)| {
            Some((name.as_str().to_string(), value.to_str().ok()?.to_string()))
        })
        .collect()
}

/// A read's failure as this node's answer.
fn read_failure(failure: MemberFailure) -> ServerError {
    match failure.refusal {
        MemberRefusal::Refused {
            node_id,
            status,
            message,
            ..
        } => {
            let status = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
            refused(node_id, status, message)
        }
        MemberRefusal::NoHolder { set_id, range_id } => ServerError::ServiceUnavailableReason(
            format!("no holder of range {range_id} of distributed bucket {set_id} answered"),
        ),
        MemberRefusal::Route(error) => map_guard_error(error),
        other => ServerError::InternalError(format!("{other:?}").into()),
    }
}

/// Runs a read another member may answer; `None` when this node answers it.
async fn read_member(
    state: &ServerState,
    read: MemberReadOperation,
) -> ServerResult<Option<MemberBody>> {
    match drive(read, state.get_ctx().as_ref()).await {
        Ok(MemberRead::Local(_)) => Ok(None),
        Ok(MemberRead::Remote { body, .. }) => Ok(Some(body)),
        Err(failure) => Err(read_failure(failure)),
    }
}

fn rest_read(
    state: &ServerState,
    read: &RestRead<'_>,
    sent: &HeaderMap,
    stream: bool,
) -> Option<MemberReadOperation> {
    let binding = read.info.distribution.clone()?;
    if read.headers.contains_key(RELAY_HOP_HEADER) {
        return None;
    }
    let ask = RestAsk {
        local: state.get_node_id(),
        path: read.path.to_string(),
        bearer: bearer_secret(read.bearer),
        headers: sent_pairs(sent),
        stream,
    };
    Some(MemberReadOperation::rest(binding, read.key, ask))
}

/// The answer of the member that keeps `read`'s key, or `None` when this node answers it. A
/// relayed read stays here.
pub(crate) async fn holder_json<T: DeserializeOwned>(
    state: &ServerState,
    read: &RestRead<'_>,
) -> ServerResult<Option<T>> {
    relayed_set(read.info, read.headers)?;
    let Some(operation) = rest_read(state, read, &HeaderMap::new(), false) else {
        return Ok(None);
    };
    decoded_read(state, operation).await
}

/// The coordinator's answer after a local copy missed the key, as a copy may lag behind it;
/// `None` when this node coordinates the range or the read was relayed here.
pub(crate) async fn missed_json<T: DeserializeOwned>(
    state: &ServerState,
    read: &RestRead<'_>,
) -> ServerResult<Option<T>> {
    let Some(operation) = rest_read(state, read, &HeaderMap::new(), false) else {
        return Ok(None);
    };
    decoded_read(state, operation.after_miss(false)).await
}

async fn decoded_read<T: DeserializeOwned>(
    state: &ServerState,
    operation: MemberReadOperation,
) -> ServerResult<Option<T>> {
    match read_member(state, operation).await? {
        Some(body) => Ok(Some(decoded(state, body).await?.1)),
        None => Ok(None),
    }
}

/// Streams the answer of the member that keeps `read`'s key with `sent` headers, keeping its
/// status, content headers and body; `None` when this node answers it.
pub(crate) async fn holder_stream(
    state: &ServerState,
    read: &RestRead<'_>,
    sent: &HeaderMap,
) -> ServerResult<Option<Response>> {
    relayed_set(read.info, read.headers)?;
    let Some(operation) = rest_read(state, read, sent, true) else {
        return Ok(None);
    };
    streamed_read(state, operation).await
}

/// Like [`holder_stream`], at the coordinator after a local copy missed the key.
pub(crate) async fn missed_stream(
    state: &ServerState,
    read: &RestRead<'_>,
    sent: &HeaderMap,
) -> ServerResult<Option<Response>> {
    let Some(operation) = rest_read(state, read, sent, true) else {
        return Ok(None);
    };
    streamed_read(state, operation.after_miss(false)).await
}

async fn streamed_read(
    state: &ServerState,
    operation: MemberReadOperation,
) -> ServerResult<Option<Response>> {
    let Some(answer) = read_member(state, operation).await? else {
        return Ok(None);
    };
    let status = StatusCode::from_u16(answer.status).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut response = Response::builder().status(status);
    for name in [
        header::CONTENT_TYPE,
        header::CONTENT_LENGTH,
        header::CONTENT_RANGE,
        header::ACCEPT_RANGES,
    ] {
        if let Some(value) = answer.header(name.as_str()) {
            response = response.header(name, value);
        }
    }
    response
        .body(Body::from_stream(answer.stream.0))
        .map(Some)
        .map_err(|error| ServerError::InternalError(error.into()))
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
    refused(forward.node_id, status, message)
}

fn refused(node_id: NodeId, status: StatusCode, message: String) -> ServerError {
    match status {
        StatusCode::BAD_REQUEST => ServerError::BadRequestMessage(message),
        StatusCode::UNAUTHORIZED => ServerError::Unauthorized,
        StatusCode::FORBIDDEN => ServerError::Forbidden,
        StatusCode::NOT_FOUND => ServerError::NotFound,
        StatusCode::CONFLICT => ServerError::Conflict(message),
        StatusCode::SERVICE_UNAVAILABLE => ServerError::ServiceUnavailableReason(message),
        _ => ServerError::BadGatewayReason(format!(
            "coordinator node {node_id} answered {status}: {message}"
        )),
    }
}

/// The url of `path` on a node's published api base, which may already carry the nest.
pub(crate) fn relay_url(base: &str, path: &str) -> String {
    rest_url(base, path)
}

async fn call_node(state: &ServerState, call: RestCall) -> ServerResult<RestAnswer> {
    (drive(RestCallOperation::new(call), state.get_ctx().as_ref()).await)
        .map_err(|error| ServerError::InternalError(error.into()))
}

/// A node's successful answer decoded, with its status.
async fn decoded<T: DeserializeOwned>(
    state: &ServerState,
    body: MemberBody,
) -> ServerResult<(StatusCode, T)> {
    let status = StatusCode::from_u16(body.status).unwrap_or(StatusCode::OK);
    let text = drive(MemberTextOperation::of_body(body), state.get_ctx().as_ref()).await;
    let text = text.map_err(|refusal| {
        ServerError::BadGatewayReason(format!("a node answered incompletely: {refusal:?}"))
    })?;
    let answer = serde_json::from_str(&text).map_err(|error| {
        ServerError::BadGatewayReason(format!("the coordinator answer does not decode: {error}"))
    })?;
    Ok((status, answer))
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
    let json =
        serde_json::to_vec(body).map_err(|error| ServerError::InternalError(error.into()))?;
    let call = RestCall {
        node_id: forward.node_id,
        path: path.to_string(),
        bearer: bearer_secret(Some(bearer)),
        relay: Some((state.get_node_id(), forward.set_id)),
        headers: Vec::new(),
        json: Some(json),
        timed: false,
        any_status: false,
    };
    match call_node(state, call).await? {
        RestAnswer::Body(body) => decoded(state, body).await,
        RestAnswer::Refused { status, body } => Err(refusal_of(forward, status, &body)),
        RestAnswer::Silent => Err(unreachable(
            forward,
            &node_reach(&state.get_ctx().heard(), forward.node_id).hint(),
        )),
    }
}

fn refusal_of(forward: &RestForward, status: u16, body: &str) -> ServerError {
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    coordinator_error(forward, status, body)
}

/// Sends a GET of `path` with `sent` headers to the forward's node with the caller's bearer
/// token and decodes its answer; `None` when the node did not answer, so the caller may ask
/// another holder.
pub(crate) async fn relay_get<T: DeserializeOwned>(
    state: &ServerState,
    forward: &RestForward,
    path: &str,
    bearer: Option<&ValidatedBearer>,
    sent: HeaderMap,
) -> ServerResult<Option<T>> {
    let call = RestCall {
        node_id: forward.node_id,
        path: path.to_string(),
        bearer: bearer_secret(bearer),
        relay: Some((state.get_node_id(), forward.set_id)),
        headers: sent_pairs(&sent),
        json: None,
        timed: true,
        any_status: false,
    };
    match call_node(state, call).await? {
        RestAnswer::Body(body) => Ok(Some(decoded(state, body).await?.1)),
        RestAnswer::Refused { status, body } => Err(refusal_of(forward, status, &body)),
        RestAnswer::Silent => Ok(None),
    }
}

/// Asks `node_id` itself for the JSON answer of `path` before `deadline`, marked with the node
/// header so the target answers for its own records and never directs the read on.
pub(crate) async fn node_read<T: DeserializeOwned>(
    state: &ServerState,
    node_id: NodeId,
    path: &str,
    bearer: Option<&ValidatedBearer>,
    deadline: tokio::time::Instant,
) -> ServerResult<T> {
    let silent = || ServerError::ServiceUnavailableReason(format!("node {node_id} did not answer"));
    let forward = RestForward {
        set_id: Ulid::nil(),
        range_id: Ulid::nil(),
        node_id,
    };
    let call = RestCall {
        node_id,
        path: path.to_string(),
        bearer: bearer_secret(bearer),
        relay: None,
        headers: vec![(RELAY_NODE_HEADER.to_string(), node_id.to_string())],
        json: None,
        timed: true,
        any_status: false,
    };
    // The deadline covers the answer's body too, not only the connection.
    let read = async {
        match call_node(state, call).await? {
            RestAnswer::Body(body) => Ok(decoded(state, body).await?.1),
            RestAnswer::Refused { status, body } => Err(refusal_of(&forward, status, &body)),
            RestAnswer::Silent => Err(silent()),
        }
    };
    tokio::time::timeout_at(deadline, read)
        .await
        .unwrap_or_else(|_| Err(silent()))
}

/// Whether a node-directed read reached its target: `Ok(true)` when it names this node,
/// `Ok(false)` when the request carries no node header, and a refusal for another node.
pub(crate) fn node_directed(state: &ServerState, headers: &HeaderMap) -> ServerResult<bool> {
    let Some(named) = headers.get(RELAY_NODE_HEADER) else {
        return Ok(false);
    };
    if named.to_str().ok() == Some(state.get_node_id().to_string().as_str()) {
        return Ok(true);
    }
    Err(ServerError::Conflict(
        "the read was directed at another node".to_string(),
    ))
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

#[cfg(test)]
#[path = "reads_tests.rs"]
mod reads;
