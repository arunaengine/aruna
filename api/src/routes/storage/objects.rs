//! Lists a bucket's objects over REST; a distributed bucket is merged across its key ranges in
//! key order, each range listed locally or by a holder through a bearer token relay (D12).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::distribution::KeyRange;
use aruna_operations::distribution::guard::{copies_interval, lists_interval};
use aruna_operations::s3::listing::common_prefix_of;
use aruna_operations::s3::object::list::ListedObject;
use aruna_operations::s3::object::pages::{
    self, ListRequest, ObjectPage, PageError, PageObject, PageScope, RangeCall, RangeRelay, Resume,
    encode_resume, listing_path,
};
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ValidatedBearer, ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::mcp::data::{ObjectOutput, object_output};
use crate::routes::management_relay::RELAY_HOP_HEADER;
use crate::routes::storage::deletion::bucket_info;
use crate::routes::storage::distributed::map_guard_error;
use crate::routes::storage::relay::{RestForward, relay_get, relayed_set};
use crate::server::state::ServerState;

/// Entries one page returns by default and at most.
const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;

#[derive(OpenApi)]
#[openapi()]
pub struct ObjectsDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(ObjectsDoc::openapi()).routes(routes!(list_objects))
}

#[derive(Debug, Clone, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ObjectListQuery {
    /// Only keys that start with this prefix.
    pub prefix: Option<String>,
    /// Groups keys by the part up to the next delimiter after the prefix, usually `/`.
    pub delimiter: Option<String>,
    /// The `next_cursor` of the previous page.
    pub cursor: Option<String>,
    /// Entries per page, 1 to 1000; defaults to 100.
    pub limit: Option<usize>,
    /// Starts after this key or common prefix.
    pub start_after: Option<String>,
    /// Inclusive lower key of one range; only for requests another member relays.
    pub lower: Option<String>,
    /// Exclusive upper key of one range; only for requests another member relays.
    pub upper: Option<String>,
    /// Lists this member's copy of the named range while its coordinator cannot; only for
    /// requests another member relays.
    pub copy: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, ToSchema)]
pub struct ObjectListResponse {
    pub bucket: String,
    pub objects: Vec<ObjectOutput>,
    /// Common prefixes the delimiter grouped keys into.
    pub prefixes: Vec<String>,
    /// Present while more entries follow.
    pub next_cursor: Option<String>,
    /// Ranges a copy listed because their coordinator could not answer; the copy may be
    /// behind, so their entries may be missing recent changes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stale_ranges: Vec<String>,
}

/// The relayed listing of one range on a holder, or with `copy` on a copy of it.
fn range_path(
    request: &ListRequest,
    cursor: Option<&str>,
    (range, copy): (&KeyRange, bool),
) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query.append_pair("limit", &request.limit.to_string());
    for (name, value) in [
        ("prefix", request.prefix.as_deref()),
        ("delimiter", request.delimiter.as_deref()),
        ("cursor", cursor),
        ("lower", Some(range.lower.as_str())),
        ("upper", range.upper.as_deref()),
        ("copy", copy.then_some("true")),
    ] {
        if let Some(value) = value {
            query.append_pair(name, value);
        }
    }
    format!(
        "/data/buckets/{}/objects?{}",
        request.bucket,
        query.finish()
    )
}

/// Lists the ranges other members keep with the caller's bearer token.
pub(crate) struct BearerRelay<'a> {
    pub state: &'a ServerState,
    pub bearer: Option<&'a ValidatedBearer>,
}

impl RangeRelay for BearerRelay<'_> {
    type Object = ObjectOutput;
    type Error = ServerError;

    async fn list_range(
        &self,
        call: RangeCall<'_>,
    ) -> ServerResult<Option<ObjectPage<ObjectOutput>>> {
        let path = range_path(call.request, call.cursor, (call.range, call.copy));
        let forward = RestForward {
            set_id: call.set_id,
            range_id: call.range.range_id,
            node_id: call.node_id,
        };
        let page: Option<ObjectListResponse> =
            relay_get(self.state, &forward, &path, self.bearer, HeaderMap::new()).await?;
        Ok(page.map(|page| ObjectPage {
            objects: page.objects,
            prefixes: page.prefixes,
            next_cursor: page.next_cursor,
            stale_ranges: page.stale_ranges,
            unavailable_ranges: Vec::new(),
        }))
    }

    fn is_fault(error: &ServerError) -> bool {
        error.status_code().is_server_error()
    }
}

impl From<&ListedObject> for ObjectOutput {
    fn from(object: &ListedObject) -> Self {
        object_output(object)
    }
}

impl PageObject for ObjectOutput {
    fn key(&self) -> &str {
        &self.key
    }
}

pub(crate) fn page_error(error: PageError<ServerError>) -> ServerError {
    match error {
        PageError::Cursor => ServerError::BadRequestReason(error.to_string()),
        PageError::Unanswered { .. } => ServerError::ServiceUnavailableReason(error.to_string()),
        PageError::Guard(error) => map_guard_error(error),
        PageError::Internal(error) => ServerError::InternalError(error),
        PageError::Relay(error) => error,
    }
}

fn listed(bucket: String, page: ObjectPage<ObjectOutput>) -> ObjectListResponse {
    ObjectListResponse {
        bucket,
        objects: page.objects,
        prefixes: page.prefixes,
        next_cursor: page.next_cursor,
        stale_ranges: page.stale_ranges,
    }
}

/// One page of `info`'s objects on this node: local for an ordinary bucket, merged across the
/// key ranges of a distributed one. With a caller, only what that caller may read shows.
pub(crate) async fn object_page(
    state: &ServerState,
    info: &BucketInfo,
    (bearer, caller): (Option<&ValidatedBearer>, Option<pages::Caller<'_>>),
    request: ListRequest,
) -> ServerResult<ObjectListResponse> {
    let relay = BearerRelay { state, bearer };
    let context = state.get_ctx();
    let scope = PageScope {
        context: context.as_ref(),
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        relay: &relay,
    };
    let bucket = request.bucket.clone();
    let page = pages::object_page(&scope, info, caller, request).await;
    Ok(listed(bucket, page.map_err(page_error)?))
}

/// The range bounds a relayed request names, when this member lists every key inside them.
async fn relayed_bounds(
    state: &ServerState,
    info: &BucketInfo,
    headers: &HeaderMap,
    query: &ObjectListQuery,
) -> ServerResult<Option<(String, Option<String>)>> {
    let Some(lower) = query.lower.clone() else {
        return match query.upper.is_some() {
            true => Err(ServerError::BadRequestReason("upper needs lower".into())),
            false => Ok(None),
        };
    };
    let Some(binding) = info.distribution.as_ref() else {
        return Err(ServerError::BadRequestReason(
            "range bounds apply only to distributed buckets".into(),
        ));
    };
    relayed_set(info, headers)?;
    if !headers.contains_key(RELAY_HOP_HEADER) {
        return Err(ServerError::BadRequestReason(
            "range bounds are only accepted from another member".into(),
        ));
    }
    let upper = query.upper.clone();
    let context = state.get_ctx();
    let held = match lists_interval(context.as_ref(), binding, &lower, upper.as_deref()).await {
        Ok(false) if query.copy == Some(true) => {
            copies_interval(context.as_ref(), binding, &lower, upper.as_deref()).await
        }
        held => held,
    };
    match held {
        Ok(true) => Ok(Some((lower, upper))),
        Ok(false) => Err(ServerError::ServiceUnavailableReason(
            "this member does not list the named key range".into(),
        )),
        Err(error) => Err(map_guard_error(error)),
    }
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/objects",
    tag = "data/storage",
    summary = "List a bucket's objects",
    description = r#"Returns one page of a bucket's current objects in key order, with size, ETag, last change, content type and content address.

**Authentication**: realm bearer token with READ on the bucket, or on the prefix it lists.
Without a token, READ granted by public roles applies, and a refusal answers 401.

**Behavior**
- `prefix` narrows the keys; `delimiter` groups keys below the prefix into `prefixes`.
- Follow `next_cursor` for the next page; it is absent on the last page.
- A distributed bucket is listed across its key ranges: ranges this node keeps no records of are
  listed by one of their holders, which checks the caller's token again on its own path. The
  merged page visits a bounded number of ranges; a range no holder answers fails the page with
  503 instead of returning an incomplete listing.
- A range is listed by its coordinator. While the coordinator cannot answer, a ready copy
  lists it instead and `stale_ranges` names the range: the copy may be behind, so recent
  changes may be missing.
- `lower`, `upper` and `copy` name one key range and are accepted only on requests another
  member of the distributed bucket relays."#,
    params(
        ("bucket" = String, Path, description = "Bucket name as used by the S3 surface"),
        ObjectListQuery
    ),
    responses(
        (
            status = 200,
            description = "One page of objects",
            body = ObjectListResponse,
            example = json!({
                "bucket": "research-raw",
                "objects": [{
                    "key": "runs/2026/sample.fastq.gz",
                    "version_id": "01JMVER0123456789ABCDEFGHJ",
                    "size": 1048576,
                    "etag": "9b2cf535f27731c974343645a3985328",
                    "last_modified": "2026-10-24T10:00:00+00:00",
                    "content_type": "application/gzip",
                    "referenced": false,
                    "content_id": "https://w3id.org/aruna/data/af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
                }],
                "prefixes": ["runs/2026/raw/"],
                "next_cursor": "AAtydW5zLzIwMjYvAA"
            })
        ),
        (status = 400, description = "Invalid cursor or range bounds", body = ErrorResponse),
        (status = 401, description = "Invalid bearer token, or no public READ without one", body = ErrorResponse),
        (status = 403, description = "No READ on the bucket or the listed prefix", body = ErrorResponse),
        (status = 404, description = "No such bucket", body = ErrorResponse),
        (status = 503, description = "A key range of a distributed bucket had no reachable holder", body = ErrorResponse)
    ),
    security(("bearer_auth" = []), ())
)]
pub async fn list_objects(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    headers: HeaderMap,
    Path(bucket): Path<String>,
    Query(query): Query<ObjectListQuery>,
) -> ServerResult<Json<ObjectListResponse>> {
    // Signed out, only public roles grant READ, and a refusal hides whether the bucket exists.
    let signed_out = auth.is_none();
    let auth = auth.unwrap_or_else(|| AuthContext::anonymous(state.get_realm_id()));
    let auth = require_realm_auth(&state, Some(auth))?;
    let hidden = |error| match error {
        ServerError::NotFound | ServerError::Forbidden if signed_out => ServerError::Unauthorized,
        error => error,
    };
    let expiry = bearer.as_ref().map_or(0, ValidatedBearer::expires_at_secs);
    let info = bucket_info(&state, &bucket).await.map_err(hidden)?;
    let (realm_id, node_id) = (state.get_realm_id(), state.get_node_id());
    let ids = (realm_id, info.group_id, node_id);
    let path = listing_path(ids, &bucket, query.prefix.as_deref());
    ensure_permission(&state, &auth, path, Permission::READ)
        .await
        .map_err(hidden)?;
    // A start key resumes like a cursor after that key, past the whole common prefix it names.
    let start = (query.start_after.clone().filter(|key| !key.is_empty()))
        .map(|key| {
            let (prefix, delimiter) = (query.prefix.as_deref(), query.delimiter.as_deref());
            let group = (key.starts_with(prefix.unwrap_or_default()))
                .then(|| common_prefix_of(&key, prefix, delimiter))
                .flatten();
            let key = group.clone().unwrap_or(key);
            encode_resume(&Resume::After { key, group })
        })
        .transpose()
        .map_err(page_error)?;
    let request = ListRequest {
        bucket,
        prefix: query.prefix.clone(),
        delimiter: query.delimiter.clone(),
        cursor: query.cursor.clone().or(start),
        limit: query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT),
        partial: false,
    };
    let caller = Some((&auth, expiry));
    if let Some(bounds) = relayed_bounds(&state, &info, &headers, &query).await? {
        let relay = BearerRelay {
            state: &state,
            bearer: bearer.as_ref(),
        };
        let context = state.get_ctx();
        let scope = PageScope {
            context: context.as_ref(),
            realm_id,
            node_id,
            relay: &relay,
        };
        let page = pages::range_page(&scope, (&info, caller), &request, query.start_after, bounds);
        let bucket = request.bucket.clone();
        return Ok(Json(listed(bucket, page.await.map_err(page_error)?)));
    }
    let page = object_page(&state, &info, (bearer.as_ref(), caller), request).await?;
    Ok(Json(page))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_names_bounds() {
        let request = ListRequest {
            bucket: "b".to_string(),
            prefix: Some("a b/".to_string()),
            delimiter: None,
            cursor: None,
            limit: 5,
            partial: false,
        };
        let range = KeyRange {
            range_id: ulid::Ulid::from_bytes([1; 16]),
            lower: "m".to_string(),
            upper: None,
            lineage: None,
            coordinator: ulid::Ulid::from_bytes([2; 16]),
            epoch: 1,
            replicas: Vec::new(),
        };
        assert_eq!(
            range_path(&request, Some("q&r"), (&range, false)),
            "/data/buckets/b/objects?limit=5&prefix=a+b%2F&cursor=q%26r&lower=m"
        );
        assert_eq!(
            range_path(&request, None, (&range, true)),
            "/data/buckets/b/objects?limit=5&prefix=a+b%2F&lower=m&copy=true"
        );
    }
}

#[cfg(test)]
#[path = "objects_tests.rs"]
mod route_tests;
