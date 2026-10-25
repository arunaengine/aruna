//! Lists a bucket's objects over REST. A distributed bucket is merged across its key ranges:
//! each range is listed from this member's records or by a holder of the range through a
//! relay with the caller's bearer token (D12), in key order and with a bounded fan-out.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BucketInfo, bucket_permission_path, object_permission_path,
};
use aruna_core::structs::storage::distribution::{DistributionBinding, KeyRange};
use aruna_operations::auth::permission_rules::permission_rules;
use aruna_operations::distribution::guard::{bound_view, lists_interval};
use aruna_operations::driver::drive;
use aruna_operations::s3::object::list::{
    ListBucketInput, ListBucketOperation, ListContinuationToken,
};
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::{Extension, Json};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
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
/// Ranges one merged page visits at most, so its fan-out stays bounded.
const MAX_PAGE_RANGES: usize = 64;
/// Bytes of keys and prefixes after which a merged page visits no further range.
const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;

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
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, ToSchema)]
pub struct ObjectListResponse {
    pub bucket: String,
    pub objects: Vec<ObjectOutput>,
    /// Common prefixes the delimiter grouped keys into.
    pub prefixes: Vec<String>,
    /// Present while more entries follow.
    pub next_cursor: Option<String>,
}

/// Where a merged page continues: after a listed key or common prefix, or at the first key of
/// a range when a page visited its bound number of ranges before any entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum Resume {
    After { key: String, group: Option<String> },
    From(String),
}

fn invalid_cursor() -> ServerError {
    ServerError::BadRequestReason("the cursor is not a next_cursor of this listing".into())
}

fn encode_resume(resume: &Resume) -> ServerResult<String> {
    let bytes = postcard::to_allocvec(resume)
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_resume(cursor: &str) -> ServerResult<Resume> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| invalid_cursor())?;
    postcard::from_bytes(&bytes).map_err(|_| invalid_cursor())
}

/// The token that resumes a local listing after `key`, or past the whole `group`.
fn resume_token(
    bucket: &str,
    key: &str,
    group: Option<&str>,
) -> ServerResult<ListContinuationToken> {
    let last_key = BlobHeadKey::object_prefix(bucket, group.unwrap_or(key))
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok(ListContinuationToken {
        last_key,
        last_common_prefix: group.map(str::to_string),
    })
}

/// One listing request as the merge needs it.
#[derive(Clone, Debug)]
pub(crate) struct ListRequest {
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub cursor: Option<String>,
    pub limit: usize,
}

/// One page of `info`'s objects on this node: local for an ordinary bucket, merged across the
/// key ranges of a distributed one.
pub(crate) async fn object_page(
    state: &ServerState,
    info: &BucketInfo,
    bearer: Option<&ValidatedBearer>,
    request: ListRequest,
) -> ServerResult<ObjectListResponse> {
    match info.distribution.as_ref() {
        Some(binding) => merged_page(state, info, binding, bearer, request).await,
        None => {
            let token = local_token(&request)?;
            local_part(state, info, &request, (token, None), None).await
        }
    }
}

/// The local listing position a cursor of an unmerged listing names.
fn local_token(request: &ListRequest) -> ServerResult<Option<ListContinuationToken>> {
    match request.cursor.as_deref().map(decode_resume).transpose()? {
        Some(Resume::After { key, group }) => {
            Ok(Some(resume_token(&request.bucket, &key, group.as_deref())?))
        }
        Some(Resume::From(_)) => Err(invalid_cursor()),
        None => Ok(None),
    }
}

/// One page of `info`'s objects from this node's records, inside `bounds` when given.
async fn local_part(
    state: &ServerState,
    info: &BucketInfo,
    request: &ListRequest,
    (token, start_after): (Option<ListContinuationToken>, Option<String>),
    bounds: Option<(String, Option<String>)>,
) -> ServerResult<ObjectListResponse> {
    let input = ListBucketInput {
        bucket: request.bucket.clone(),
        group_id: info.group_id,
        continuation_token: token,
        max_keys: Some(request.limit),
        prefix: request.prefix.clone().filter(|prefix| !prefix.is_empty()),
        delimiter: request
            .delimiter
            .clone()
            .filter(|delimiter| !delimiter.is_empty()),
        start_after,
    };
    let operation = match bounds {
        Some((lower, upper)) => ListBucketOperation::new(input).with_bounds(lower, upper),
        None => ListBucketOperation::new(input),
    };
    let result = drive(operation, &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok(ObjectListResponse {
        bucket: request.bucket.clone(),
        objects: result.objects.iter().map(object_output).collect(),
        prefixes: result.common_prefixes,
        next_cursor: result
            .continuation_token
            .map(|token| {
                let key = BlobHeadKey::from_bytes(&token.last_key)
                    .map_err(|error| ServerError::InternalError(error.to_string()))?
                    .key;
                let group = token.last_common_prefix;
                encode_resume(&Resume::After { key, group })
            })
            .transpose()?,
    })
}

/// The relayed listing of one range on a holder.
fn range_path(request: &ListRequest, cursor: Option<&str>, range: &KeyRange) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query.append_pair("limit", &request.limit.to_string());
    for (name, value) in [
        ("prefix", request.prefix.as_deref()),
        ("delimiter", request.delimiter.as_deref()),
        ("cursor", cursor),
        ("lower", Some(range.lower.as_str())),
        ("upper", range.upper.as_deref()),
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

/// One range's page from a holder of the range; only a range nobody answers ends the page.
async fn remote_part(
    state: &ServerState,
    (binding, holders): (&DistributionBinding, Vec<aruna_core::NodeId>),
    range: &KeyRange,
    bearer: Option<&ValidatedBearer>,
    (request, cursor): (&ListRequest, Option<String>),
) -> ServerResult<ObjectListResponse> {
    let path = range_path(request, cursor.as_deref(), range);
    for node_id in holders {
        let forward = RestForward {
            set_id: binding.set_id,
            range_id: range.range_id,
            node_id,
        };
        if let Some(page) = relay_get(state, &forward, &path, bearer).await? {
            return Ok(page);
        }
    }
    Err(ServerError::ServiceUnavailableReason(format!(
        "no holder of range {} of distributed bucket {} answered, so the listing would be \
         incomplete; retry later",
        range.range_id, binding.set_id
    )))
}

/// One merged page over every range of a distributed bucket under the prefix.
async fn merged_page(
    state: &ServerState,
    info: &BucketInfo,
    binding: &DistributionBinding,
    bearer: Option<&ValidatedBearer>,
    request: ListRequest,
) -> ServerResult<ObjectListResponse> {
    let context = state.get_ctx();
    let view = bound_view(context.as_ref(), binding)
        .await
        .map_err(map_guard_error)?;
    let snapshot = view.snapshot();
    let resume = request.cursor.as_deref().map(decode_resume).transpose()?;
    let (after, from) = match &resume {
        Some(Resume::After { key, group }) => (Some((key.clone(), group.clone())), None),
        Some(Resume::From(lower)) => (None, Some(lower.clone())),
        None => (None, None),
    };
    let prefix = request.prefix.clone().unwrap_or_default();
    let mut page = ObjectListResponse {
        bucket: request.bucket.clone(),
        ..Default::default()
    };
    let mut last_group = after.as_ref().and_then(|(_, group)| group.clone());
    let mut last_key = after.as_ref().map(|(key, _)| key.clone());
    let ranges = snapshot.listed_ranges(&prefix).into_iter().filter(|range| {
        let past = after.as_ref().is_none_or(|(key, group)| {
            let entry = group.as_deref().unwrap_or(key);
            range.upper.as_deref().is_none_or(|upper| entry < upper)
        });
        past && from
            .as_deref()
            .is_none_or(|from| range.lower.as_str() >= from)
    });
    let mut first = true;
    let mut bytes = 0usize;
    for (visited, range) in ranges.enumerate() {
        let listed = page.objects.len() + page.prefixes.len();
        let remaining = request.limit.saturating_sub(listed);
        if remaining == 0 || visited == MAX_PAGE_RANGES || bytes >= MAX_PAGE_BYTES {
            // A page that listed nothing continues at this range, never at its own start.
            let next = match last_key.as_ref().filter(|_| listed > 0) {
                Some(key) => Resume::After {
                    key: key.clone(),
                    group: last_group.clone().filter(|group| group >= key),
                },
                None => Resume::From(range.lower.clone()),
            };
            page.next_cursor = Some(encode_resume(&next)?);
            return Ok(page);
        }
        let start = match (first, &after) {
            (true, Some((key, group))) => Some((key.clone(), group.clone())),
            _ => None,
        };
        first = false;
        let sub = ListRequest {
            limit: remaining,
            cursor: None,
            ..request.clone()
        };
        let part = match snapshot.lists_locally(binding.member_id, range) {
            true => {
                let token = (start.as_ref())
                    .map(|(key, group)| resume_token(&request.bucket, key, group.as_deref()))
                    .transpose()?;
                let bounds = Some((range.lower.clone(), range.upper.clone()));
                local_part(state, info, &sub, (token, None), bounds).await?
            }
            false => {
                // The full position, so a common prefix is skipped as a whole.
                let cursor = (start
                    .map(|(key, group)| encode_resume(&Resume::After { key, group })))
                .transpose()?;
                let holders = (snapshot.record_holders(range).into_iter())
                    .filter(|member| member.member_id != binding.member_id)
                    .map(|member| member.node_id)
                    .collect();
                remote_part(state, (binding, holders), range, bearer, (&sub, cursor)).await?
            }
        };
        let keys = (part.objects.iter().map(|object| &object.key)).chain(part.prefixes.iter());
        bytes += keys.map(String::len).sum::<usize>();
        for group in part.prefixes {
            if last_group.as_deref() == Some(group.as_str()) {
                continue;
            }
            if last_key.as_deref().is_none_or(|key| group.as_str() > key) {
                last_key = Some(group.clone());
            }
            last_group = Some(group.clone());
            page.prefixes.push(group);
        }
        if let Some(object) = part.objects.last()
            && last_key
                .as_deref()
                .is_none_or(|key| object.key.as_str() > key)
        {
            last_key = Some(object.key.clone());
        }
        page.objects.extend(part.objects);
        // The range's own position, which also moves on when nothing in it was shown.
        if part.next_cursor.is_some() {
            page.next_cursor = part.next_cursor;
            break;
        }
    }
    Ok(page)
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
    match lists_interval(state.get_ctx().as_ref(), binding, &lower, upper.as_deref()).await {
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

**Behavior**
- `prefix` narrows the keys; `delimiter` groups keys below the prefix into `prefixes`.
- Follow `next_cursor` for the next page; it is absent on the last page.
- A distributed bucket is listed across its key ranges: ranges this node keeps no records of are
  listed by one of their holders, which checks the caller's token again on its own path. The
  merged page visits a bounded number of ranges; a range no holder answers fails the page with
  503 instead of returning an incomplete listing.
- `lower` and `upper` name one key range and are accepted only on requests another member of
  the distributed bucket relays."#,
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
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No READ on the bucket or the listed prefix", body = ErrorResponse),
        (status = 404, description = "No such bucket", body = ErrorResponse),
        (status = 503, description = "A key range of a distributed bucket had no reachable holder", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_objects(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    headers: HeaderMap,
    Path(bucket): Path<String>,
    Query(query): Query<ObjectListQuery>,
) -> ServerResult<Json<ObjectListResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let info = bucket_info(&state, &bucket).await?;
    let (realm_id, node_id) = (state.get_realm_id(), state.get_node_id());
    // READ on the browsed prefix suffices, as for the group's data paths.
    let path = match query.prefix.as_deref().filter(|prefix| !prefix.is_empty()) {
        Some(prefix) => object_permission_path(realm_id, info.group_id, node_id, &bucket, prefix),
        None => bucket_permission_path(realm_id, info.group_id, node_id, &bucket),
    };
    ensure_permission(&state, &auth, path, Permission::READ).await?;
    let request = ListRequest {
        bucket,
        prefix: query.prefix.clone(),
        delimiter: query.delimiter.clone(),
        cursor: query.cursor.clone(),
        limit: query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT),
    };
    if let Some(bounds) = relayed_bounds(&state, &info, &headers, &query).await? {
        let token = local_token(&request)?;
        let page = local_part(
            &state,
            &info,
            &request,
            (token, query.start_after),
            Some(bounds),
        );
        return Ok(Json(
            readable_page(&state, &auth, &info, page.await?).await?,
        ));
    }
    let page = object_page(&state, &info, bearer.as_ref(), request).await?;
    Ok(Json(readable_page(&state, &auth, &info, page).await?))
}

/// The page without the keys and prefixes the caller may not read: READ on a listed prefix
/// covers the literal prefix only, and denies or restrictions below it still apply.
async fn readable_page(
    state: &ServerState,
    auth: &AuthContext,
    info: &BucketInfo,
    mut page: ObjectListResponse,
) -> ServerResult<ObjectListResponse> {
    let (realm_id, node_id) = (state.get_realm_id(), state.get_node_id());
    let root = bucket_permission_path(realm_id, info.group_id, node_id, &page.bucket);
    let rules = permission_rules(&state.get_ctx(), auth, &root)
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let path =
        |key: &str| object_permission_path(realm_id, info.group_id, node_id, &page.bucket, key);
    let readable = |key: &str| rules.allows(&path(key), &Permission::READ);
    page.objects.retain(|object| readable(&object.key));
    page.prefixes.retain(|prefix| readable(prefix));
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_round_trips() {
        let after = Resume::After {
            key: "a/b".to_string(),
            group: Some("a/".to_string()),
        };
        let encoded = encode_resume(&after).unwrap();
        assert_eq!(decode_resume(&encoded).unwrap(), after);
        let from = Resume::From("m".to_string());
        assert_eq!(decode_resume(&encode_resume(&from).unwrap()).unwrap(), from);
        assert!(decode_resume("not a cursor").is_err());
    }

    #[test]
    fn range_path_names_bounds() {
        let request = ListRequest {
            bucket: "b".to_string(),
            prefix: Some("a b/".to_string()),
            delimiter: None,
            cursor: None,
            limit: 5,
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
            range_path(&request, Some("q&r"), &range),
            "/data/buckets/b/objects?limit=5&prefix=a+b%2F&cursor=q%26r&lower=m"
        );
    }

    #[test]
    fn cursor_skips_group() {
        // A holder resumes past the whole common prefix, not just after its name.
        let request = ListRequest {
            bucket: "b".to_string(),
            prefix: None,
            delimiter: Some("/".to_string()),
            cursor: Some(
                encode_resume(&Resume::After {
                    key: "d/".to_string(),
                    group: Some("d/".to_string()),
                })
                .unwrap(),
            ),
            limit: 5,
        };
        let token = local_token(&request).unwrap().unwrap();
        assert_eq!(token.last_common_prefix.as_deref(), Some("d/"));
        let from = ListRequest {
            cursor: Some(encode_resume(&Resume::From("m".to_string())).unwrap()),
            ..request
        };
        assert!(local_token(&from).is_err());
    }

    #[test]
    fn group_resumes_past_it() {
        let token = resume_token("bucket", "d/x", Some("d/")).unwrap();
        assert_eq!(token.last_common_prefix.as_deref(), Some("d/"));
        let head = BlobHeadKey::from_bytes(&token.last_key).unwrap();
        assert_eq!(head.key, "d/");
    }
}
