//! Lists a bucket's objects over REST; a distributed bucket is merged across its key ranges in
//! key order, each range listed locally or by a holder through a bearer token relay (D12).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_operations::driver::drive;
use std::sync::Arc;

use aruna_core::compute::{SecretBytes, SharedSecret};
use aruna_core::outbound::{RangeEntry, RangeRefusal};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_operations::distribution::routing::{copies_interval, lists_interval};
use aruna_operations::s3::listing::common_prefix_of;
use aruna_operations::s3::object::list::ListedObject;
use aruna_operations::s3::object::pages::{
    self, ListRequest, ObjectPage, ObjectPageOperation, PageError, PageNode, PageObject, Resume,
    encode_resume, listing_path,
};
use aruna_operations::s3::object::range_list::MemberAccess;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ValidatedBearer, ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::lookup::bucket_info;
use crate::lookup::map_guard_error;
use crate::lookup::{ObjectOutput, object_output};
use crate::routes::management_relay::RELAY_HOP_HEADER;
use crate::routes::storage::relay::relayed_set;
use crate::server::state::ServerState;

/// Entries one page returns by default and at most.
const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new().routes(routes!(list_objects))
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

impl From<&ListedObject> for ObjectOutput {
    fn from(object: &ListedObject) -> Self {
        object_output(object)
    }
}

/// An entry another member listed; its time reads like a time listed here.
impl From<RangeEntry> for ObjectOutput {
    fn from(entry: RangeEntry) -> Self {
        Self {
            key: entry.key,
            version_id: entry.version_id,
            size: entry.size,
            etag: entry.etag,
            last_modified: (entry.last_modified)
                .and_then(|time| chrono::DateTime::parse_from_rfc3339(&time).ok())
                .map(|time| time.with_timezone(&chrono::Utc).to_rfc3339()),
            content_type: entry.content_type,
            referenced: entry.referenced,
            content_id: entry.content_id,
            link: None,
        }
    }
}

impl PageObject for ObjectOutput {
    fn key(&self) -> &str {
        &self.key
    }
}

/// A holder's refusal as this node's answer.
fn refusal_error(refusal: RangeRefusal) -> ServerError {
    let RangeRefusal {
        node_id,
        status,
        message,
        ..
    } = refusal;
    match status {
        400 => ServerError::BadRequestMessage(message),
        401 => ServerError::Unauthorized,
        403 => ServerError::Forbidden,
        404 => ServerError::NotFound,
        409 => ServerError::Conflict(message),
        503 => ServerError::ServiceUnavailableReason(message),
        _ => ServerError::BadGatewayReason(format!(
            "coordinator node {node_id} answered {status}: {message}"
        )),
    }
}

pub(crate) fn page_error(error: PageError) -> ServerError {
    match error {
        PageError::Cursor => ServerError::BadRequestReason(error.to_string()),
        PageError::Unanswered { .. } => ServerError::ServiceUnavailableReason(error.to_string()),
        PageError::Guard(error) => map_guard_error(error),
        PageError::Internal(error) => ServerError::InternalError(error.into()),
        PageError::Relay(refusal) => refusal_error(refusal),
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

/// This node as a listing needs it.
pub(crate) fn page_node(state: &ServerState) -> PageNode {
    PageNode {
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        ids: state.get_ctx().node_and_realm(),
    }
}

/// The caller's bearer token, which other members check themselves.
pub(crate) fn bearer_access(bearer: Option<&ValidatedBearer>) -> MemberAccess {
    let secret = |bearer: &ValidatedBearer| {
        SharedSecret::new(SecretBytes::new(bearer.as_str().as_bytes().to_vec()))
    };
    MemberAccess::Bearer(bearer.map(secret))
}

/// One page of `info`'s objects on this node: local for an ordinary bucket, merged across the
/// key ranges of a distributed one. With a caller, only what that caller may read shows.
pub(crate) async fn object_page(
    state: &ServerState,
    info: &BucketInfo,
    (bearer, caller): (Option<&ValidatedBearer>, Option<pages::Caller<'_>>),
    request: ListRequest,
) -> ServerResult<ObjectListResponse> {
    let bucket = request.bucket.clone();
    let access = bearer_access(bearer);
    let node = page_node(state);
    let list = ObjectPageOperation::<ObjectOutput>::new(node, access, info, caller, request);
    let page = drive(list, &state.get_ctx()).await;
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
    let held = match drive(
        lists_interval(binding, &lower, upper.as_deref()),
        context.as_ref(),
    )
    .await
    {
        Ok(false) if query.copy == Some(true) => {
            drive(
                copies_interval(binding, &lower, upper.as_deref()),
                context.as_ref(),
            )
            .await
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
        let bucket = request.bucket.clone();
        let access = bearer_access(bearer.as_ref());
        let node = page_node(&state);
        let list = ObjectPageOperation::<ObjectOutput>::new(node, access, &info, caller, request);
        let list = list.for_range(query.start_after, bounds);
        let page = drive(list, &state.get_ctx()).await;
        return Ok(Json(listed(bucket, page.map_err(page_error)?)));
    }
    let page = object_page(&state, &info, (bearer.as_ref(), caller), request).await?;
    Ok(Json(page))
}

#[cfg(test)]
#[path = "objects_tests.rs"]
mod route_tests;
