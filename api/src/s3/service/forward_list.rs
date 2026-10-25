//! Lists a distributed bucket across its key ranges: each range under the prefix is listed
//! from this member's records or by a holder of the range, in key order, and merged into one
//! bounded page. Common prefixes that span a range bound appear once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::blob::{BlobHeadKey, BucketInfo, UserAccess};
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionSnapshot, KeyRange,
};
use aruna_operations::distribution::guard::{bound_view, lists_interval};
use aruna_operations::s3::object::list::{ListBucketInput, ListContinuationToken};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use http::Method;
use percent_encoding::{percent_decode_str, utf8_percent_encode};
use s3s::dto::{CommonPrefix, ETag, Object, Owner, Timestamp, TimestampFormat};
use s3s::{S3Error, S3ErrorCode, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::forward::{Forward, ForwardBody, ForwardRequest, answer_text, xml_field};
use super::listing::{ObjectListingPage, scoped_marker};
use super::{ArunaS3Service, URL_ENCODE_SET};
use crate::s3::auth::DelegatedRequest;
use crate::s3::error::guard_error;
use crate::s3::scope::SubpathScope;

/// The query parameters a forwarded listing names its key range with.
const LOWER_PARAM: &str = "aruna-lower";
const UPPER_PARAM: &str = "aruna-upper";
const TOKEN_DOMAIN: &[u8] = b"aruna-distributed-listing-v1";

/// A continuation token of a merged listing, bound to its set and query.
#[derive(Debug, Serialize, Deserialize)]
struct BoundToken {
    set_id: Ulid,
    query: [u8; 32],
    position: Resume,
}

/// Where a merged listing continues: after a listed entry, or at the first key of a range when
/// a page visited its bound number of ranges without an entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) enum Resume {
    After(ListContinuationToken),
    From(String),
}

/// Ranges one merged page visits at most, so its fan-out stays bounded.
pub(super) const MAX_RANGES_PER_PAGE: usize = 64;
/// Bytes of keys and prefixes after which a merged page visits no further range.
pub(super) const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;

fn query_digest(prefix: Option<&str>, delimiter: Option<&str>) -> [u8; 32] {
    let bytes = postcard::to_allocvec(&(prefix, delimiter)).unwrap_or_default();
    let mut hasher = blake3::Hasher::new();
    hasher.update(TOKEN_DOMAIN);
    hasher.update(&bytes);
    *hasher.finalize().as_bytes()
}

/// The token a merged page ends with.
pub(super) fn encode_bound(
    set_id: Ulid,
    (prefix, delimiter): (Option<&str>, Option<&str>),
    position: Option<&Resume>,
) -> S3Result<Option<String>> {
    position
        .map(|position| {
            let token = BoundToken {
                set_id,
                query: query_digest(prefix, delimiter),
                position: position.clone(),
            };
            postcard::to_allocvec(&token)
                .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
                .map_err(|error| s3_error!(InternalError, "{error}"))
        })
        .transpose()
}

/// The position a merged token resumes at; a token of another set or query is refused.
pub(super) fn decode_bound(
    set_id: Ulid,
    (prefix, delimiter): (Option<&str>, Option<&str>),
    token: Option<&str>,
) -> S3Result<Option<Resume>> {
    let invalid = || s3_error!(InvalidArgument, "Invalid continuation token");
    token
        .map(|token| {
            let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid())?;
            let token: BoundToken = postcard::from_bytes(&bytes).map_err(|_| invalid())?;
            match token.set_id == set_id && token.query == query_digest(prefix, delimiter) {
                true => Ok(token.position),
                false => Err(invalid()),
            }
        })
        .transpose()
}

/// The range bounds a forwarded listing names.
fn named_bounds(query: Option<&str>) -> Option<(String, Option<String>)> {
    let (mut lower, mut upper) = (None, None);
    for (name, value) in url::form_urlencoded::parse(query?.as_bytes()) {
        match name.as_ref() {
            LOWER_PARAM => lower = Some(value.into_owned()),
            UPPER_PARAM => upper = Some(value.into_owned()),
            _ => {}
        }
    }
    Some((lower?, upper))
}

/// The query of a listing a holder answers for one range.
pub(super) fn range_query(base: &[(&str, String)], range: &KeyRange) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in base {
        query.append_pair(name, value);
    }
    query.append_pair(LOWER_PARAM, &range.lower);
    if let Some(upper) = &range.upper {
        query.append_pair(UPPER_PARAM, upper);
    }
    query.finish()
}

/// No holder of a range answered, so the listing cannot be complete.
pub(super) fn incomplete(set_id: Ulid, range: &KeyRange) -> S3Error {
    let mut error = S3Error::with_message(
        S3ErrorCode::Custom("ListingIncomplete".into()),
        format!(
            "No holder of range {} of distributed bucket {set_id} answered, so the listing \
             would be incomplete. Retry later.",
            range.range_id
        ),
    );
    error.set_status_code(http::StatusCode::SERVICE_UNAVAILABLE);
    error
}

/// Whether another holder may answer instead: the node was unreachable or could not serve
/// the range from its own view, for example while it no longer holds it or catches up.
pub(super) fn unreachable(error: &S3Error) -> bool {
    error
        .status_code()
        .is_none_or(|status| status.is_server_error())
}

/// The XML elements `tag` of a listing body.
pub(super) fn elements<'a>(body: &'a str, tag: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    body.split(open.as_str())
        .skip(1)
        .filter_map(|part| part.split(close.as_str()).next())
        .collect()
}

pub(super) fn timestamp(text: Option<String>) -> Option<Timestamp> {
    text.and_then(|text| Timestamp::parse(TimestampFormat::DateTime, &text).ok())
}

pub(super) fn e_tag(text: Option<String>) -> Option<ETag> {
    text.and_then(|text| ETag::parse_http_header(text.as_bytes()).ok())
}

/// Where a holder resumes the first listed range: at the full position of the request, so a
/// common prefix is skipped as a whole and not listed again after its name.
fn remote_start(input: &ListBucketInput, first: bool) -> S3Result<Option<(&'static str, String)>> {
    Ok(match (first, &input.continuation_token) {
        (true, Some(token)) => ArunaS3Service::encode_list_token(Some(token))?
            .map(|token| ("continuation-token", token)),
        (true, None) => (input.start_after.clone())
            .filter(|start| !start.is_empty())
            .map(|start| ("start-after", start)),
        (false, _) => None,
    })
}

/// Narrows a holder's page to what this node's own scope of the caller allows, as a local
/// page is narrowed.
fn scope_part(part: &mut ObjectListingPage, scope: &SubpathScope, url_encoded: bool) {
    let raw = |value: Option<&String>| {
        ArunaS3Service::raw(value.map(String::as_str).unwrap_or_default(), url_encoded)
    };
    (part.contents).retain(|object| scope.allows_key(&raw(object.key.as_ref())));
    (part.common_prefixes).retain(|group| scope.allows_prefix(&raw(group.prefix.as_ref())));
}

impl ArunaS3Service {
    pub(super) fn encoded(&self, value: String, url_encoded: bool) -> String {
        match url_encoded {
            true => utf8_percent_encode(&value, URL_ENCODE_SET).to_string(),
            false => value,
        }
    }

    pub(super) fn raw(value: &str, url_encoded: bool) -> String {
        match url_encoded {
            true => percent_decode_str(value).decode_utf8_lossy().into_owned(),
            false => value.to_string(),
        }
    }

    /// The bounds a forwarded listing names, when this member keeps every range inside them.
    pub(super) async fn delegated_listing(
        &self,
        extensions: &http::Extensions,
        bucket: Option<&BucketInfo>,
        query: Option<&str>,
    ) -> S3Result<Option<(String, Option<String>)>> {
        if extensions.get::<DelegatedRequest>().is_none() {
            return Ok(None);
        }
        let binding = bucket
            .and_then(|bucket| bucket.distribution.as_ref())
            .ok_or_else(|| s3_error!(AccessDenied, "Delegation refused: not a member"))?;
        let (lower, upper) = named_bounds(query)
            .ok_or_else(|| s3_error!(AccessDenied, "Delegation refused: no key range named"))?;
        match lists_interval(self.state.as_ref(), binding, &lower, upper.as_deref()).await {
            Ok(true) => Ok(Some((lower, upper))),
            Ok(false) => {
                let mut error = S3Error::with_message(
                    S3ErrorCode::Custom("RangeNotHeld".into()),
                    "This member does not answer listings of the named key range.".to_string(),
                );
                error.set_status_code(http::StatusCode::SERVICE_UNAVAILABLE);
                Err(error)
            }
            Err(error) => Err(guard_error(&error)),
        }
    }

    pub(super) async fn snapshot(
        &self,
        binding: &DistributionBinding,
    ) -> S3Result<DistributionSnapshot> {
        bound_view(self.state.as_ref(), binding)
            .await
            .map(|view| view.head.snapshot)
            .map_err(|error| guard_error(&error))
    }

    /// Asks the holders of `range` in turn; only a range nobody answers ends the listing.
    pub(super) async fn ask_holders(
        &self,
        user: &UserAccess,
        (binding, snapshot): (&DistributionBinding, &DistributionSnapshot),
        range: &KeyRange,
        (operation, bucket, query): (&'static str, &str, String),
    ) -> S3Result<String> {
        let holders = snapshot.record_holders(range);
        let others = holders
            .into_iter()
            .filter(|member| member.member_id != binding.member_id);
        for member in others {
            let forward = Forward {
                set_id: binding.set_id,
                range_id: range.range_id,
                node_id: member.node_id,
            };
            let request = ForwardRequest {
                method: Method::GET,
                operation,
                bucket: bucket.to_string(),
                key: String::new(),
                query: Some(query.clone()),
                headers: Vec::new(),
                body: ForwardBody::Empty,
            };
            let answer = match self.forward_write(user, &forward, request).await {
                Ok(response) => answer_text(response).await,
                Err(error) => Err(error),
            };
            match answer {
                Ok(body) => return Ok(body),
                Err(error) if unreachable(&error) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(incomplete(binding.set_id, range))
    }

    /// One range's object page from a holder, keys mapped as this request encodes them.
    async fn remote_objects(
        &self,
        user: &UserAccess,
        context: (&DistributionBinding, &DistributionSnapshot),
        range: &KeyRange,
        (input, start, remaining): (&ListBucketInput, Option<(&'static str, String)>, usize),
        (owner, url_encoded): (Option<&Owner>, bool),
    ) -> S3Result<ObjectListingPage> {
        let mut base = vec![
            ("list-type", "2".to_string()),
            ("max-keys", remaining.to_string()),
        ];
        base.extend(input.prefix.clone().map(|prefix| ("prefix", prefix)));
        base.extend(
            input
                .delimiter
                .clone()
                .map(|delimiter| ("delimiter", delimiter)),
        );
        base.extend(start);
        let query = range_query(&base, range);
        let request = ("ListObjectsV2", input.bucket.as_str(), query);
        let body = self.ask_holders(user, context, range, request).await?;
        let contents = (elements(&body, "Contents").into_iter())
            .filter_map(|entry| {
                Some(Object {
                    key: Some(self.encoded(xml_field(entry, "Key")?, url_encoded)),
                    last_modified: timestamp(xml_field(entry, "LastModified")),
                    e_tag: e_tag(xml_field(entry, "ETag")),
                    size: xml_field(entry, "Size").and_then(|size| size.parse().ok()),
                    owner: owner.cloned(),
                    ..Default::default()
                })
            })
            .collect();
        let common_prefixes = (elements(&body, "CommonPrefixes").into_iter())
            .filter_map(|entry| xml_field(entry, "Prefix"))
            .map(|prefix| CommonPrefix {
                prefix: Some(self.encoded(prefix, url_encoded)),
            })
            .collect();
        let continuation_token = match xml_field(&body, "NextContinuationToken") {
            Some(token) => Self::decode_list_token(Some(&token))?,
            None => None,
        };
        Ok(ObjectListingPage {
            contents,
            common_prefixes,
            continuation_token,
        })
    }

    /// One merged object page over every range under the prefix, at most `max_keys` entries.
    pub(super) async fn merged_objects(
        &self,
        user: &UserAccess,
        binding: &DistributionBinding,
        input: ListBucketInput,
        (owner, url_encoded, scope): (Option<Owner>, bool, Option<&SubpathScope>),
        (from, bounded): (Option<String>, bool),
    ) -> S3Result<(ObjectListingPage, Option<String>)> {
        let snapshot = self.snapshot(binding).await?;
        let max = input
            .max_keys
            .unwrap_or(aruna_operations::s3::object::list::ListBucketOperation::DEFAULT_MAX_KEYS);
        let prefix = input.prefix.clone().unwrap_or_default();
        let resume = match &input.continuation_token {
            Some(token) => Some((
                BlobHeadKey::from_bytes(&token.last_key)
                    .map_err(|_| s3_error!(InvalidArgument, "Invalid continuation token"))?
                    .key,
                token.last_common_prefix.clone(),
            )),
            None => (input.start_after.clone())
                .filter(|start| !start.is_empty())
                .map(|start| (start, None)),
        };
        let mut merged = ObjectListingPage {
            contents: Vec::new(),
            common_prefixes: Vec::new(),
            continuation_token: None,
        };
        let mut last_prefix = resume.as_ref().and_then(|(_, group)| group.clone());
        // Only entries of this page move its token on; a resumed prefix would repeat it.
        let mut page_prefix: Option<String> = None;
        let mut last_key: Option<String> = None;
        let ranges = snapshot.listed_ranges(&prefix);
        let ranges = ranges.into_iter().filter(|range| {
            let after = resume.as_ref().is_none_or(|(key, _)| {
                range
                    .upper
                    .as_deref()
                    .is_none_or(|upper| key.as_str() < upper)
            });
            after
                && from
                    .as_deref()
                    .is_none_or(|from| range.lower.as_str() >= from)
        });
        let mut first = true;
        let mut bytes = 0usize;
        for (visited, range) in ranges.enumerate() {
            let remaining =
                max.saturating_sub(merged.contents.len() + merged.common_prefixes.len());
            let full = bounded && (visited == MAX_RANGES_PER_PAGE || bytes >= MAX_PAGE_BYTES);
            if remaining == 0 || full {
                merged.continuation_token =
                    scoped_marker(&input.bucket, last_key.as_deref(), page_prefix.as_deref())?;
                // Nothing listed yet: the next page starts at this range.
                let next = merged
                    .continuation_token
                    .is_none()
                    .then(|| range.lower.clone());
                return Ok((merged, next));
            }
            let part = match snapshot.lists_locally(binding.member_id, range) {
                true => {
                    let mut sub = input.clone();
                    sub.max_keys = Some(remaining);
                    if !first {
                        (sub.continuation_token, sub.start_after) = (None, None);
                    }
                    let bounds = Some((range.lower.clone(), range.upper.clone()));
                    self.run_object_listing(sub, owner.clone(), url_encoded, scope, bounds)
                        .await?
                }
                false => {
                    let start = remote_start(&input, first)?;
                    let query = (&input, start, remaining);
                    let context = (binding, &snapshot);
                    let mut part = self
                        .remote_objects(user, context, range, query, (owner.as_ref(), url_encoded))
                        .await?;
                    if let Some(scope) = scope {
                        scope_part(&mut part, scope, url_encoded);
                    }
                    part
                }
            };
            first = false;
            let keys = part
                .contents
                .iter()
                .filter_map(|object| object.key.as_ref());
            let groups = part
                .common_prefixes
                .iter()
                .filter_map(|group| group.prefix.as_ref());
            bytes += keys.chain(groups).map(String::len).sum::<usize>();
            for prefix in part.common_prefixes {
                let raw = Self::raw(prefix.prefix.as_deref().unwrap_or_default(), url_encoded);
                if last_prefix.as_deref() == Some(raw.as_str()) {
                    continue;
                }
                last_prefix = Some(raw.clone());
                page_prefix = Some(raw);
                merged.common_prefixes.push(prefix);
            }
            if let Some(object) = part.contents.last() {
                last_key = object.key.as_deref().map(|key| Self::raw(key, url_encoded));
            }
            merged.contents.extend(part.contents);
            if part.continuation_token.is_some() {
                merged.continuation_token = part.continuation_token;
                break;
            }
        }
        Ok((merged, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_bind_query() {
        let set = Ulid::from_bytes([1; 16]);
        let position = ListContinuationToken {
            last_key: b"k".to_vec(),
            last_common_prefix: Some("a/".to_string()),
        };
        let query = (Some("a"), Some("/"));
        let resume = Resume::After(position);
        let token = encode_bound(set, query, Some(&resume)).unwrap();
        let back = decode_bound(set, query, token.as_deref()).unwrap();
        assert_eq!(back, Some(resume));
        let from = Resume::From("m".to_string());
        let token = encode_bound(set, query, Some(&from)).unwrap();
        assert_eq!(
            decode_bound(set, query, token.as_deref()).unwrap(),
            Some(from)
        );
        assert!(decode_bound(set, (Some("b"), Some("/")), token.as_deref()).is_err());
        let other = Ulid::from_bytes([2; 16]);
        assert!(decode_bound(other, query, token.as_deref()).is_err());
    }

    #[test]
    fn bounds_round_trip() {
        let range = KeyRange {
            range_id: Ulid::from_bytes([1; 16]),
            lower: "a b".to_string(),
            upper: Some("m&n".to_string()),
            lineage: None,
            coordinator: Ulid::from_bytes([2; 16]),
            epoch: 1,
            replicas: Vec::new(),
        };
        let query = range_query(&[("list-type", "2".to_string())], &range);
        assert_eq!(
            named_bounds(Some(&query)),
            Some(("a b".to_string(), Some("m&n".to_string())))
        );
        assert_eq!(named_bounds(Some("list-type=2")), None);
    }

    #[test]
    fn holder_resumes_group() {
        let token = ListContinuationToken {
            last_key: BlobHeadKey::object_prefix("spire", "d/").unwrap(),
            last_common_prefix: Some("d/".to_string()),
        };
        let input = ListBucketInput {
            bucket: "spire".to_string(),
            group_id: Ulid::from_bytes([1; 16]),
            continuation_token: Some(token.clone()),
            max_keys: Some(10),
            prefix: None,
            delimiter: Some("/".to_string()),
            start_after: None,
        };
        let (name, value) = remote_start(&input, true).unwrap().unwrap();
        assert_eq!(name, "continuation-token");
        let sent = ArunaS3Service::decode_list_token(Some(&value)).unwrap();
        assert_eq!(sent, Some(token));
        assert_eq!(remote_start(&input, false).unwrap(), None);
    }

    #[test]
    fn next_holder_answers() {
        let mut not_held = S3Error::with_message(S3ErrorCode::Custom("RangeNotHeld".into()), "");
        not_held.set_status_code(http::StatusCode::SERVICE_UNAVAILABLE);
        assert!(unreachable(&not_held));
        assert!(!unreachable(&s3_error!(AccessDenied, "denied")));
    }

    #[test]
    fn holder_page_scoped() {
        let mut part = ObjectListingPage {
            contents: vec![Object {
                key: Some("secret".to_string()),
                ..Default::default()
            }],
            common_prefixes: vec![CommonPrefix {
                prefix: Some("private/".to_string()),
            }],
            continuation_token: None,
        };
        // A scope without grants hides what the holder listed under its own rules.
        scope_part(&mut part, &SubpathScope::default(), false);
        assert!(part.contents.is_empty() && part.common_prefixes.is_empty());
    }

    #[test]
    fn listing_bodies_parse() {
        let body = "<ListBucketResult><Contents><Key>a&amp;b</Key><Size>3</Size></Contents>\
                    <CommonPrefixes><Prefix>d/</Prefix></CommonPrefixes></ListBucketResult>";
        let contents = elements(body, "Contents");
        assert_eq!(xml_field(contents[0], "Key").as_deref(), Some("a&b"));
        let prefixes = elements(body, "CommonPrefixes");
        assert_eq!(xml_field(prefixes[0], "Prefix").as_deref(), Some("d/"));
    }
}
