//! Serves the S3 object listings: version 1 and 2 listings and the version listing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::forward::list::{decode_bound, encode_bound};
use super::listing::{ObjectListingPage, marker_continuation_token, next_marker_for};
use super::view::InputView;
use super::{ArunaS3Service, URL_ENCODE_SET, forward, link};
use crate::s3::auth::DelegatedRequest;
use crate::s3::scope::SubpathScope;
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess, object_permission_path};
use aruna_ops_data::s3::object::list::{ListBucketInput, ListBucketOperation};
use aruna_ops_data::s3::object::member::merge::MergeResume as Resume;
use aruna_ops_data::s3::object::versions::{ListVersionsInput, ListVersionsOperation};
use aruna_ops_data::s3::object::visible::{HiddenCursor, is_sealed};
use aruna_ops_identity::auth::request_authorization::BucketAccessOperation;
use aruna_runner::driver::drive;
use aruna_runner::position::open_cursor;
use percent_encoding::utf8_percent_encode;
use s3s::dto::{
    EncodingType, ListObjectVersionsInput, ListObjectVersionsOutput, ListObjectsInput,
    ListObjectsOutput, ListObjectsV2Input, ListObjectsV2Output, Owner,
};
use s3s::{S3Request, S3Response, S3Result, s3_error};
use tracing::{debug, error};

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn list_objects_v2(
    adapter: &ArunaS3Service,
    req: S3Request<ListObjectsV2Input>,
) -> S3Result<S3Response<ListObjectsV2Output>> {
    if let Some(view) = req.extensions.get::<InputView>().copied() {
        return adapter.view_list(view, req.input).await;
    }
    debug!(
        bucket = %req.input.bucket,
        prefix = ?req.input.prefix,
        max_keys = ?req.input.max_keys,
        "Received LIST OBJECTS V2 Request"
    );

    let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    let bucket_info = req.extensions.get::<BucketInfo>().cloned();
    let bounds = adapter
        .delegated_listing(&req.extensions, bucket_info.as_ref(), req.uri.query())
        .await?;
    // A distributed bucket merges every range under the prefix into one page.
    let merged = (bucket_info.as_ref())
        .and_then(|bucket_info| bucket_info.distribution.clone())
        .filter(|_| bounds.is_none());
    let requested_continuation_token = req.input.continuation_token.clone();
    let query = (req.input.prefix.as_deref(), req.input.delimiter.as_deref());
    let requested = requested_continuation_token.as_deref();
    // A sealed token resumes a page that stopped among entries the caller may not see.
    let hidden = match requested.filter(|token| is_sealed(token)) {
        Some(token) => Some(
            open_cursor::<HiddenCursor>(token)
                .ok_or_else(|| s3_error!(InvalidArgument, "Invalid continuation token"))?,
        ),
        None => None,
    };
    let plain = requested.filter(|_| hidden.is_none());
    let (continuation_token, from) = match &merged {
        Some(binding) => match decode_bound(binding.set_id, query, plain)? {
            Some(Resume::After(token)) => (Some(token), None),
            Some(other) => (None, Some(other)),
            None => (None, None),
        },
        None => (ArunaS3Service::decode_list_token(plain)?, None),
    };
    let max_keys = match req.input.max_keys {
        None => ListBucketOperation::DEFAULT_MAX_KEYS,
        Some(max_keys) => usize::try_from(max_keys)
            .map_err(|_| s3_error!(InvalidArgument, "max-keys must be non-negative"))?
            .min(ListBucketOperation::DEFAULT_MAX_KEYS),
    };
    let bucket = req.input.bucket.clone();
    let prefix = req.input.prefix.clone();
    let delimiter = req.input.delimiter.clone();
    let start_after = req.input.start_after.clone();

    let group_id = bucket_info
        .as_ref()
        .map(|bucket_info| bucket_info.group_id)
        .unwrap_or(user_access.group_id);
    let scope = req.extensions.get::<SubpathScope>().cloned();
    if let Some(scope) = &scope {
        let requested = object_permission_path(
            adapter.realm_id,
            group_id,
            adapter.node_id,
            &bucket,
            prefix.as_deref().unwrap_or_default(),
        );
        if !scope.overlaps(&requested) {
            return Err(s3_error!(AccessDenied, "Permission denied"));
        }
    }
    // A share link's listing shows each key only as that share session may read it.
    let shared =
        (req.extensions.get::<DelegatedRequest>()).and_then(|delegated| delegated.share.as_ref());
    let access = match shared {
        Some(auth) => Some(
            drive(
                BucketAccessOperation::new(
                    auth,
                    (adapter.realm_id, group_id, adapter.node_id),
                    &bucket,
                ),
                &adapter.state,
            )
            .await
            .map_err(|error| s3_error!(InternalError, "{error}"))?,
        ),
        None => None,
    };

    let owner = req.input.fetch_owner.unwrap_or(false).then(|| Owner {
        display_name: None,
        id: Some(group_id.to_string()),
    });
    let url_encoded = req
        .input
        .encoding_type
        .as_ref()
        .is_some_and(|encoding_type| encoding_type.as_str() == EncodingType::URL);
    let encode_field = |value: String| -> String {
        if url_encoded {
            utf8_percent_encode(&value, URL_ENCODE_SET).to_string()
        } else {
            value
        }
    };

    let input = ListBucketInput {
        bucket: bucket.clone(),
        group_id,
        continuation_token,
        max_keys: Some(max_keys),
        prefix: prefix.clone(),
        delimiter: delimiter.clone(),
        start_after: start_after.clone(),
    };
    let (page, next, stale) = match &merged {
        Some(binding) => {
            let shape = (owner, url_encoded, scope.as_ref());
            adapter
                .merged_objects(&user_access, binding, input, shape, (from, true))
                .await?
        }
        None => {
            let page = adapter
                .run_object_listing(
                    input,
                    (owner, Some(&user_access)),
                    url_encoded,
                    (scope.as_ref(), access),
                    (bounds, hidden),
                )
                .await?;
            (page, None, Vec::new())
        }
    };

    let ObjectListingPage {
        contents,
        common_prefixes,
        continuation_token: result_token,
        sealed,
    } = page;
    let key_count = contents.len() + common_prefixes.len();
    let next_continuation_token = match &merged {
        Some(binding) => {
            let query = (prefix.as_deref(), delimiter.as_deref());
            let resume = (result_token.map(Resume::After)).or(next);
            encode_bound(binding.set_id, query, resume.as_ref())?
        }
        None => match sealed {
            Some(sealed) => Some(sealed),
            None => ArunaS3Service::encode_list_token(result_token.as_ref())?,
        },
    };
    let is_truncated = next_continuation_token.is_some();

    let mut response = S3Response::new(ListObjectsV2Output {
        name: Some(bucket),
        prefix: prefix.map(encode_field),
        max_keys: Some(i32::try_from(max_keys).unwrap_or(i32::MAX)),
        key_count: Some(i32::try_from(key_count).unwrap_or(i32::MAX)),
        continuation_token: requested_continuation_token,
        is_truncated: Some(is_truncated),
        next_continuation_token,
        contents: Some(contents),
        common_prefixes: Some(common_prefixes),
        delimiter: delimiter.map(encode_field),
        encoding_type: req.input.encoding_type,
        start_after: start_after.map(encode_field),
        ..Default::default()
    });
    forward::reads::mark_stale(&mut response.headers, &stale);
    Ok(response)
}

#[tracing::instrument(err, skip(adapter, req))]
#[allow(clippy::blocks_in_conditions)]
pub(super) async fn list_objects(
    adapter: &ArunaS3Service,
    req: S3Request<ListObjectsInput>,
) -> S3Result<S3Response<ListObjectsOutput>> {
    debug!(
        bucket = %req.input.bucket,
        prefix = ?req.input.prefix,
        max_keys = ?req.input.max_keys,
        "Received LIST OBJECTS Request"
    );

    let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    let bucket_info = req.extensions.get::<BucketInfo>().cloned();
    let max_keys = match req.input.max_keys {
        None => ListBucketOperation::DEFAULT_MAX_KEYS,
        Some(max_keys) => usize::try_from(max_keys)
            .map_err(|_| s3_error!(InvalidArgument, "max-keys must be non-negative"))?
            .min(ListBucketOperation::DEFAULT_MAX_KEYS),
    };
    let bucket = req.input.bucket.clone();
    let prefix = req.input.prefix.clone();
    let delimiter = req.input.delimiter.clone();
    let marker = req.input.marker.clone();
    let merged = (bucket_info.as_ref()).and_then(|bucket_info| bucket_info.distribution.clone());

    let group_id = bucket_info
        .as_ref()
        .map(|bucket_info| bucket_info.group_id)
        .unwrap_or(user_access.group_id);
    let scope = req.extensions.get::<SubpathScope>().cloned();
    if let Some(scope) = &scope {
        let requested = object_permission_path(
            adapter.realm_id,
            group_id,
            adapter.node_id,
            &bucket,
            prefix.as_deref().unwrap_or_default(),
        );
        if !scope.overlaps(&requested) {
            return Err(s3_error!(AccessDenied, "Permission denied"));
        }
    }

    // A sealed marker resumes among hidden entries; any other marker is a key.
    let hidden = match marker.as_deref().filter(|marker| is_sealed(marker)) {
        Some(marker) => Some(
            open_cursor::<HiddenCursor>(marker)
                .ok_or_else(|| s3_error!(InvalidArgument, "Invalid marker"))?,
        ),
        None => None,
    };
    // In a distributed bucket a marker may be the sealed position this listing issued; any
    // other token, such as a ListObjectsV2 one, stays a plain key.
    let query = (prefix.as_deref(), delimiter.as_deref());
    let resumed = (merged.as_ref().zip(marker.as_deref()))
        .and_then(|(binding, marker)| decode_bound(binding.set_id, query, Some(marker)).ok())
        .flatten()
        .filter(|resume| matches!(resume, Resume::Sealed { .. }));
    let key_marker = marker
        .as_deref()
        .filter(|_| hidden.is_none() && resumed.is_none());
    let continuation_token =
        marker_continuation_token(&bucket, key_marker, prefix.as_deref(), delimiter.as_deref())?;
    let start_after = continuation_token
        .is_none()
        .then(|| key_marker.map(str::to_string))
        .flatten();

    let owner = Some(Owner {
        display_name: None,
        id: Some(group_id.to_string()),
    });
    let url_encoded = req
        .input
        .encoding_type
        .as_ref()
        .is_some_and(|encoding_type| encoding_type.as_str() == EncodingType::URL);
    let encode_field = |value: String| -> String {
        if url_encoded {
            utf8_percent_encode(&value, URL_ENCODE_SET).to_string()
        } else {
            value
        }
    };

    let input = ListBucketInput {
        bucket: bucket.clone(),
        group_id,
        continuation_token,
        max_keys: Some(max_keys),
        prefix: prefix.clone(),
        delimiter: delimiter.clone(),
        start_after,
    };
    let (page, stale) = match &merged {
        // A marker cannot name a range start, so a legacy listing visits every range.
        Some(binding) => {
            let shape = (owner, url_encoded, scope.as_ref());
            let (mut page, next, stale) =
                (adapter.merged_objects(&user_access, binding, input, shape, (resumed, false)))
                    .await?;
            // A range that stopped among hidden entries goes on from this marker.
            if let Some(next) = next {
                page.sealed = encode_bound(binding.set_id, query, Some(&next))?;
            }
            (page, stale)
        }
        None => {
            let page = adapter
                .run_object_listing(
                    input,
                    (owner, Some(&user_access)),
                    url_encoded,
                    (scope.as_ref(), None),
                    (None, hidden),
                )
                .await?;
            (page, Vec::new())
        }
    };

    let ObjectListingPage {
        contents,
        common_prefixes,
        continuation_token: result_token,
        sealed,
    } = page;
    let is_truncated = result_token.is_some() || sealed.is_some();
    let next_marker = match sealed {
        Some(sealed) => Some(sealed),
        None => next_marker_for(
            delimiter.as_deref(),
            result_token.as_ref(),
            contents.is_empty(),
        )
        .map(encode_field),
    };

    let mut response = S3Response::new(ListObjectsOutput {
        name: Some(bucket),
        prefix: prefix.map(encode_field),
        marker: marker.map(encode_field),
        max_keys: Some(i32::try_from(max_keys).unwrap_or(i32::MAX)),
        is_truncated: Some(is_truncated),
        next_marker,
        contents: Some(contents),
        common_prefixes: Some(common_prefixes),
        delimiter: delimiter.map(encode_field),
        encoding_type: req.input.encoding_type,
        ..Default::default()
    });
    forward::reads::mark_stale(&mut response.headers, &stale);
    Ok(response)
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn list_object_versions(
    adapter: &ArunaS3Service,
    req: S3Request<ListObjectVersionsInput>,
) -> S3Result<S3Response<ListObjectVersionsOutput>> {
    debug!(
        bucket = %req.input.bucket,
        prefix = ?req.input.prefix,
        "Received LIST OBJECT VERSIONS Request"
    );

    let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    let bucket_info = req.extensions.get::<BucketInfo>().cloned();
    let bucket = req.input.bucket.clone();
    let prefix = req.input.prefix.clone();
    let delimiter = req.input.delimiter.clone();
    let key_marker = req.input.key_marker.clone();
    let requested_version_marker = req.input.version_id_marker.clone();
    let bounds = adapter
        .delegated_listing(&req.extensions, bucket_info.as_ref(), req.uri.query())
        .await?;
    let version_id_marker = match requested_version_marker.as_deref() {
        None => None,
        Some(marker) => Some(
            ulid::Ulid::from_string(marker)
                .map_err(|_| s3_error!(InvalidArgument, "Invalid version-id-marker"))?,
        ),
    };
    let max_keys = match req.input.max_keys {
        None => ListVersionsOperation::DEFAULT_MAX_KEYS,
        Some(max_keys) => usize::try_from(max_keys)
            .map_err(|_| s3_error!(InvalidArgument, "max-keys must be non-negative"))?
            .min(ListVersionsOperation::DEFAULT_MAX_KEYS),
    };
    let group_id = bucket_info
        .as_ref()
        .map(|bucket_info| bucket_info.group_id)
        .unwrap_or(user_access.group_id);
    // A distributed bucket seeks in its ordered inventory, range by range.
    if let Some(binding) = bucket_info.and_then(|bucket_info| bucket_info.distribution) {
        let output = match bounds {
            Some(bounds) => {
                let shape = (group_id, max_keys, version_id_marker);
                adapter
                    .range_versions(&binding, req.input, bounds, shape)
                    .await?
            }
            None => {
                adapter
                    .merged_versions(&user_access, &binding, req.input, (group_id, max_keys))
                    .await?
            }
        };
        return Ok(S3Response::new(output));
    }

    // A sealed key marker resumes among versions the caller may not see.
    let hidden = match key_marker.as_deref().filter(|marker| is_sealed(marker)) {
        Some(marker) => Some(
            open_cursor::<link::VersionCursor>(marker)
                .ok_or_else(|| s3_error!(InvalidArgument, "Invalid key-marker"))?,
        ),
        None => None,
    };
    let (key_marker, version_id_marker) = match hidden {
        Some(_) => (None, None),
        None => (key_marker, version_id_marker),
    };
    let input = ListVersionsInput {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        delimiter: delimiter.clone(),
        key_marker,
        version_id_marker,
        max_keys: Some(max_keys),
    };
    let result = adapter
        .visible_versions(&user_access, input, hidden)
        .await?;

    Ok(adapter.list_versions_output(req.input, group_id, max_keys, result))
}
