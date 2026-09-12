//! Object range, listing and pagination helpers for the S3 adapter. The trait
//! implementation stays in `service`.

use super::{ArunaS3Service, S3_URL_ENCODE_SET};
use crate::s3::error::IntoS3Error;
use crate::s3::scope::SubpathScope;
use aruna_core::permission_path::permission_pattern_matches;
use aruna_core::structs::{BlobHeadKey, PathRestriction, Permission};
use aruna_operations::driver::drive;
use aruna_operations::s3::copy_object::CopySourceConditions;
use aruna_operations::s3::get_object::ObjectRangeRequest;
use aruna_operations::s3::list_objects::{
    ListObjectsV2ContinuationToken, ListObjectsV2Input as LOV2I, ListObjectsV2Operation,
};
use aruna_operations::s3::listing::common_prefix_of;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use percent_encoding::utf8_percent_encode;
use s3s::dto::{CommonPrefix, ETagCondition, Object, Owner, Timestamp, TimestampFormat};
use s3s::{S3Result, s3_error};
use std::time::SystemTime;

pub(super) fn object_range_request(range: s3s::dto::Range) -> ObjectRangeRequest {
    match range {
        s3s::dto::Range::Int { first, last } => match last {
            Some(end) => ObjectRangeRequest::StartEnd { start: first, end },
            None => ObjectRangeRequest::Start { start: first },
        },
        s3s::dto::Range::Suffix { length } => ObjectRangeRequest::Suffix { length },
    }
}

fn etag_condition_value(condition: &ETagCondition) -> String {
    match condition {
        ETagCondition::Any => "*".to_string(),
        ETagCondition::ETag(etag) => etag.value().to_string(),
    }
}

pub(super) fn timestamp_system_time(timestamp: &Timestamp) -> S3Result<SystemTime> {
    let mut rendered = Vec::new();
    timestamp
        .format(TimestampFormat::DateTime, &mut rendered)
        .map_err(|_| s3_error!(InvalidArgument, "Invalid timestamp"))?;
    let rendered =
        String::from_utf8(rendered).map_err(|_| s3_error!(InvalidArgument, "Invalid timestamp"))?;
    let parsed = chrono::DateTime::parse_from_rfc3339(&rendered)
        .map_err(|_| s3_error!(InvalidArgument, "Invalid timestamp"))?;
    Ok(parsed.with_timezone(&chrono::Utc).into())
}

pub(super) fn copy_source_conditions(
    if_match: Option<&ETagCondition>,
    if_none_match: Option<&ETagCondition>,
    if_modified_since: Option<&Timestamp>,
    if_unmodified_since: Option<&Timestamp>,
) -> S3Result<CopySourceConditions> {
    Ok(CopySourceConditions {
        if_match: if_match.map(etag_condition_value),
        if_none_match: if_none_match.map(etag_condition_value),
        if_modified_since: if_modified_since.map(timestamp_system_time).transpose()?,
        if_unmodified_since: if_unmodified_since.map(timestamp_system_time).transpose()?,
    })
}

/// Whether a credential's restrictions leave any allowed scope at or below one
/// bucket: either a restriction covers the bucket node itself, or it is scoped
/// to a path inside that bucket. Unrestricted credentials reach every bucket.
pub(super) fn restrictions_reach(
    restrictions: Option<&[PathRestriction]>,
    bucket_path: &str,
) -> bool {
    let Some(restrictions) = restrictions else {
        return true;
    };
    let inside = format!("{bucket_path}/");
    restrictions.iter().any(|restriction| {
        restriction.permission != Permission::DENY
            && (restriction.pattern.starts_with(&inside)
                || permission_pattern_matches(&restriction.pattern, bucket_path))
    })
}

/// One page of a shared object listing before protocol-specific mapping.
pub(super) struct ObjectListingPage {
    pub(super) contents: Vec<Object>,
    pub(super) common_prefixes: Vec<CommonPrefix>,
    pub(super) continuation_token: Option<ListObjectsV2ContinuationToken>,
}

/// Resumes a delimited ListObjects page past the group the marker collapses
/// into, because such a marker names an already returned common prefix.
pub(super) fn marker_continuation_token(
    bucket: &str,
    marker: Option<&str>,
    prefix: Option<&str>,
    delimiter: Option<&str>,
) -> S3Result<Option<ListObjectsV2ContinuationToken>> {
    let Some(marker) = marker.filter(|marker| !marker.is_empty()) else {
        return Ok(None);
    };
    let Some(group) = common_prefix_of(marker, prefix, delimiter) else {
        return Ok(None);
    };
    let last_key = BlobHeadKey::object_prefix(bucket, marker)
        .map_err(|_| s3_error!(InvalidArgument, "Invalid marker"))?;
    Ok(Some(ListObjectsV2ContinuationToken {
        last_key,
        last_common_prefix: Some(group),
    }))
}

/// V1 `NextMarker`. A delimited page may end on a common prefix, and a page whose keys
/// were all filtered out has no trailing `<Key>` to resume from, so both dead-end without
/// one. An undelimited page with contents needs none: the client resumes from its last key.
pub(super) fn next_marker_for(
    delimiter: Option<&str>,
    token: Option<&ListObjectsV2ContinuationToken>,
    contents_empty: bool,
) -> Option<String> {
    let token = token?;
    if delimiter.is_some() || contents_empty {
        next_marker_of(token)
    } else {
        None
    }
}

/// Names the last entry of a truncated page, preferring the common prefix the
/// page stopped inside.
pub(super) fn next_marker_of(token: &ListObjectsV2ContinuationToken) -> Option<String> {
    if let Some(group) = token.last_common_prefix.clone() {
        return Some(group);
    }
    BlobHeadKey::from_bytes(&token.last_key)
        .ok()
        .map(|head| head.key)
}

impl ArunaS3Service {
    pub(super) fn decode_list_token(
        token: Option<&str>,
    ) -> S3Result<Option<ListObjectsV2ContinuationToken>> {
        token
            .map(|token| {
                let decoded = STANDARD
                    .decode(token)
                    .map_err(|_| s3_error!(InvalidArgument, "Invalid continuation token"))?;
                ListObjectsV2ContinuationToken::from_bytes(&decoded)
                    .map_err(|_| s3_error!(InvalidArgument, "Invalid continuation token"))
            })
            .transpose()
    }

    pub(super) fn encode_list_token(
        token: Option<&ListObjectsV2ContinuationToken>,
    ) -> S3Result<Option<String>> {
        token
            .map(|token| {
                token
                    .to_bytes()
                    .map(|bytes| STANDARD.encode(bytes))
                    .map_err(|err| s3_error!(InternalError, "{}", err.to_string()))
            })
            .transpose()
    }

    async fn readable_prefix(
        &self,
        input: &LOV2I,
        prefix: String,
        scope: &SubpathScope,
        remaining_pages: &mut usize,
    ) -> S3Result<bool> {
        let mut input = LOV2I {
            prefix: Some(prefix),
            delimiter: None,
            continuation_token: None,
            start_after: None,
            max_keys: None,
            ..input.clone()
        };
        loop {
            consume_scope_page(remaining_pages)?;
            let result = drive(ListObjectsV2Operation::new(input.clone()), &self.state)
                .await
                .and_then(|result| result.transpose())
                .map_err(IntoS3Error::into_s3_error)?
                .ok_or_else(|| s3_error!(InternalError, "Failed to list objects"))?;
            if result
                .objects
                .iter()
                .any(|object| scope.allows_key(&object.head.key))
            {
                return Ok(true);
            }
            let Some(token) = result.continuation_token else {
                return Ok(false);
            };
            input.continuation_token = Some(token);
        }
    }

    /// Runs one listing page shared by ListObjects and ListObjectsV2.
    pub(super) async fn run_object_listing(
        &self,
        mut input: LOV2I,
        owner: Option<Owner>,
        url_encoded: bool,
        scope: Option<&SubpathScope>,
    ) -> S3Result<ObjectListingPage> {
        let mut remaining_pages = 100;
        if scope.is_some() {
            input.max_keys = Some(
                input
                    .max_keys
                    .unwrap_or(ListObjectsV2Operation::DEFAULT_MAX_KEYS)
                    .min(remaining_pages - 1),
            );
        }
        let result = loop {
            consume_scope_page(&mut remaining_pages)?;
            let mut result = drive(ListObjectsV2Operation::new(input.clone()), &self.state)
                .await
                .and_then(|result| result.transpose())
                .map_err(IntoS3Error::into_s3_error)?
                .ok_or_else(|| s3_error!(InternalError, "Failed to list objects"))?;

            if let Some(scope) = scope {
                result
                    .objects
                    .retain(|object| scope.allows_key(&object.head.key));
                let mut prefixes = Vec::new();
                for prefix in result.common_prefixes {
                    if scope.allows_prefix(&prefix)
                        && self
                            .readable_prefix(&input, prefix.clone(), scope, &mut remaining_pages)
                            .await?
                    {
                        prefixes.push(prefix);
                    }
                }
                result.common_prefixes = prefixes;
            }

            if !result.objects.is_empty()
                || !result.common_prefixes.is_empty()
                || result.continuation_token.is_none()
            {
                break result;
            }
            input.continuation_token = result.continuation_token;
            input.start_after = None;
        };

        let encode_field = |value: String| -> String {
            if url_encoded {
                utf8_percent_encode(&value, S3_URL_ENCODE_SET).to_string()
            } else {
                value
            }
        };

        let scoped_token = if scope.is_some() && result.continuation_token.is_some() {
            scoped_marker(
                &input.bucket,
                result.objects.last().map(|object| object.head.key.as_str()),
                result.common_prefixes.last().map(String::as_str),
            )?
        } else {
            None
        };
        let contents: Vec<Object> = result
            .objects
            .into_iter()
            .map(|object| {
                let response_fields = self.build_response_fields(
                    object.location.as_ref(),
                    None,
                    None,
                    object.source_metadata.as_ref(),
                    object.last_refresh,
                    object.version_created_at,
                );
                Object {
                    e_tag: response_fields.e_tag,
                    key: Some(encode_field(object.head.key)),
                    last_modified: response_fields.last_modified,
                    owner: owner.clone(),
                    size: response_fields.content_length,
                    ..Default::default()
                }
            })
            .collect();
        let common_prefixes: Vec<CommonPrefix> = result
            .common_prefixes
            .into_iter()
            .map(|prefix| CommonPrefix {
                prefix: Some(encode_field(prefix)),
            })
            .collect();

        Ok(ObjectListingPage {
            contents,
            common_prefixes,
            continuation_token: scope
                .map(|_| scoped_token)
                .unwrap_or(result.continuation_token),
        })
    }
}

fn consume_scope_page(remaining_pages: &mut usize) -> S3Result<()> {
    *remaining_pages = remaining_pages.checked_sub(1).ok_or_else(|| {
        s3_error!(
            SlowDown,
            "Listing scan limit reached; request a narrower prefix"
        )
    })?;
    Ok(())
}

pub(super) fn scoped_marker(
    bucket: &str,
    last_key: Option<&str>,
    last_prefix: Option<&str>,
) -> S3Result<Option<ListObjectsV2ContinuationToken>> {
    let group = last_prefix.filter(|prefix| last_key.is_none_or(|key| *prefix > key));
    let Some(entry) = group.or(last_key) else {
        return Ok(None);
    };
    let last_key = BlobHeadKey::object_prefix(bucket, entry)
        .map_err(|_| s3_error!(InternalError, "Invalid listing marker"))?;
    Ok(Some(ListObjectsV2ContinuationToken {
        last_key,
        last_common_prefix: group.map(str::to_string),
    }))
}

#[cfg(test)]
mod tests {
    use super::{consume_scope_page, scoped_marker};
    use aruna_core::structs::BlobHeadKey;

    #[test]
    fn scoped_scan_bounded() {
        let mut remaining = 100;
        for _ in 0..100 {
            consume_scope_page(&mut remaining).unwrap();
        }
        let error = consume_scope_page(&mut remaining).unwrap_err();
        assert_eq!(error.code(), &s3s::S3ErrorCode::SlowDown);
    }

    #[test]
    fn prefix_pages_progress() {
        let mut remaining = 100;
        for expected in (97..100).rev() {
            consume_scope_page(&mut remaining).unwrap();
            assert_eq!(remaining, expected);
        }
    }

    #[test]
    fn prefix_token_precedence() {
        let token = scoped_marker("study", Some("imaging/a"), Some("imaging/b/"))
            .unwrap()
            .unwrap();
        assert_eq!(token.last_common_prefix.as_deref(), Some("imaging/b/"));
        assert_eq!(
            BlobHeadKey::from_bytes(&token.last_key).unwrap().key,
            "imaging/b/"
        );

        let token = scoped_marker("study", Some("imaging/z"), Some("imaging/b/"))
            .unwrap()
            .unwrap();
        assert_eq!(token.last_common_prefix, None);
        assert_eq!(
            BlobHeadKey::from_bytes(&token.last_key).unwrap().key,
            "imaging/z"
        );
    }
}
