//! Lists a distributed bucket across its key ranges into one bounded page in key order, each
//! range from local records or by a holder; prefixes that span a range bound appear once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::blob::{BlobHeadKey, BucketInfo, UserAccess};
use aruna_core::structs::storage::distribution::fence::LocalFence;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionSnapshot, KeyRange,
};
use aruna_operations::distribution::guard::{
    bound_view, copies_interval, lists_interval, local_fence,
};
use aruna_operations::s3::object::list::{ListBucketInput, ListContinuationToken};
use aruna_operations::s3::object::visible::{HiddenCursor, is_sealed, open_cursor};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use http::Method;
use percent_encoding::{percent_decode_str, utf8_percent_encode};
use s3s::dto::{CommonPrefix, ETag, Object, Owner, Timestamp, TimestampFormat};
use s3s::{S3Error, S3ErrorCode, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::{Forward, ForwardBody, ForwardRequest, answer_text, xml_field};
use crate::s3::auth::DelegatedRequest;
use crate::s3::error::guard_error;
use crate::s3::scope::SubpathScope;
use crate::s3::service::listing::{ObjectListingPage, scoped_marker};
use crate::s3::service::{ArunaS3Service, URL_ENCODE_SET};

/// The query parameters a forwarded listing names its key range with.
const LOWER_PARAM: &str = "aruna-lower";
const UPPER_PARAM: &str = "aruna-upper";
/// Asks a copy to list its records while the range's coordinator cannot.
const COPY_PARAM: &str = "aruna-copy";
const TOKEN_DOMAIN: &[u8] = b"aruna-distributed-listing-v1";

/// A continuation token of a merged listing, bound to its set and query.
#[derive(Debug, Serialize, Deserialize)]
struct BoundToken {
    set_id: Ulid,
    query: [u8; 32],
    position: Resume,
}

/// Where a merged listing continues: after a listed entry, or at the first key of a range when
/// a page visited its bound number of ranges without an entry, or inside the range starting at
/// `lower`, from the sealed cursor of the member that stopped there among hidden entries.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(in crate::s3::service) enum Resume {
    After(ListContinuationToken),
    From(String),
    Sealed {
        lower: String,
        node: aruna_core::NodeId,
        copy: bool,
        cursor: String,
        group: Option<String>,
    },
}

/// Ranges one merged page visits at most, so its fan-out stays bounded.
pub(in crate::s3::service) const MAX_PAGE_RANGES: usize = 64;
/// Bytes of keys and prefixes after which a merged page visits no further range.
pub(in crate::s3::service) const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;

fn query_digest(prefix: Option<&str>, delimiter: Option<&str>) -> [u8; 32] {
    let bytes = postcard::to_allocvec(&(prefix, delimiter)).unwrap_or_default();
    let mut hasher = blake3::Hasher::new();
    hasher.update(TOKEN_DOMAIN);
    hasher.update(&bytes);
    *hasher.finalize().as_bytes()
}

/// The token a merged page ends with.
pub(in crate::s3::service) fn encode_bound(
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
pub(in crate::s3::service) fn decode_bound(
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

/// Whether a forwarded listing asks for a copy's answer.
fn named_copy(query: Option<&str>) -> bool {
    let pairs = url::form_urlencoded::parse(query.unwrap_or_default().as_bytes());
    pairs.into_iter().any(|(name, _)| name == COPY_PARAM)
}

/// The query of a listing a holder answers for one range.
pub(in crate::s3::service) fn range_query(base: &[(&str, String)], range: &KeyRange) -> String {
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
pub(in crate::s3::service) fn incomplete(set_id: Ulid, range: &KeyRange) -> S3Error {
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
pub(in crate::s3::service) fn unreachable(error: &S3Error) -> bool {
    error
        .status_code()
        .is_none_or(|status| status.is_server_error())
}

/// The XML elements `tag` of a listing body.
pub(in crate::s3::service) fn elements<'a>(body: &'a str, tag: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    body.split(open.as_str())
        .skip(1)
        .filter_map(|part| part.split(close.as_str()).next())
        .collect()
}

pub(in crate::s3::service) fn timestamp(text: Option<String>) -> Option<Timestamp> {
    text.and_then(|text| Timestamp::parse(TimestampFormat::DateTime, &text).ok())
}

pub(in crate::s3::service) fn e_tag(text: Option<String>) -> Option<ETag> {
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
    pub(in crate::s3::service) fn encoded(&self, value: String, url_encoded: bool) -> String {
        match url_encoded {
            true => utf8_percent_encode(&value, URL_ENCODE_SET).to_string(),
            false => value,
        }
    }

    pub(in crate::s3::service) fn raw(value: &str, url_encoded: bool) -> String {
        match url_encoded {
            true => percent_decode_str(value).decode_utf8_lossy().into_owned(),
            false => value.to_string(),
        }
    }

    /// The bounds a forwarded listing names, when this member keeps every range inside them.
    pub(in crate::s3::service) async fn delegated_listing(
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
        let state = self.state.as_ref();
        let held = match lists_interval(state, binding, &lower, upper.as_deref()).await {
            Ok(false) if named_copy(query) => {
                copies_interval(state, binding, &lower, upper.as_deref()).await
            }
            held => held,
        };
        match held {
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

    pub(in crate::s3::service) async fn snapshot(
        &self,
        binding: &DistributionBinding,
    ) -> S3Result<DistributionSnapshot> {
        bound_view(self.state.as_ref(), binding)
            .await
            .map(|view| view.head.snapshot)
            .map_err(|error| guard_error(&error))
    }

    pub(in crate::s3::service) async fn fence(
        &self,
        binding: &DistributionBinding,
    ) -> S3Result<LocalFence> {
        local_fence(self.state.as_ref(), binding.set_id)
            .await
            .map_err(|error| guard_error(&error))
    }

    /// Asks the member that lists `range`, or with `copies` its ready copies, which answer
    /// while the coordinator cannot; a range none answers ends the listing.
    pub(in crate::s3::service) async fn ask_holders(
        &self,
        user: &UserAccess,
        context: (&DistributionBinding, &DistributionSnapshot),
        (range, copies): (&KeyRange, bool),
        request: (&'static str, &str, String),
    ) -> S3Result<String> {
        let asked = (range, copies, None);
        let answer = self.ask_members(user, context, asked, request).await;
        answer.map(|(body, _)| body)
    }

    /// Like `ask_holders`, from the member `only` names when it does, with the member that
    /// answered.
    async fn ask_members(
        &self,
        user: &UserAccess,
        (binding, snapshot): (&DistributionBinding, &DistributionSnapshot),
        (range, copies, only): (&KeyRange, bool, Option<aruna_core::NodeId>),
        (operation, bucket, query): (&'static str, &str, String),
    ) -> S3Result<(String, aruna_core::NodeId)> {
        let (holders, query) = match copies {
            true => (
                snapshot.copy_holders(range),
                format!("{query}&{COPY_PARAM}=1"),
            ),
            false => (snapshot.list_holder(range).into_iter().collect(), query),
        };
        let others = (holders.into_iter())
            .filter(|member| member.member_id != binding.member_id)
            .filter(|member| only.is_none_or(|node| member.node_id == node));
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
            let answer = match self.forward_read(user, &forward, request).await {
                Ok(response) => answer_text(response).await,
                Err(error) => Err(error),
            };
            match answer {
                Ok(body) => return Ok((body, member.node_id)),
                Err(error) if unreachable(&error) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(incomplete(binding.set_id, range))
    }

    /// One range's object page from a holder, keys mapped as this request encodes them, with
    /// the member that listed it.
    async fn remote_objects(
        &self,
        user: &UserAccess,
        context: (&DistributionBinding, &DistributionSnapshot),
        asked: (&KeyRange, bool, Option<aruna_core::NodeId>),
        (input, start, remaining): (&ListBucketInput, Option<(&'static str, String)>, usize),
        (owner, url_encoded): (Option<&Owner>, bool),
    ) -> S3Result<(ObjectListingPage, aruna_core::NodeId)> {
        let range = asked.0;
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
        let (body, lister) = (self.ask_members(user, context, asked, request)).await?;
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
        // A member that stopped among hidden entries answers with its own sealed cursor.
        let (continuation_token, sealed) = match xml_field(&body, "NextContinuationToken") {
            Some(token) if is_sealed(&token) => (None, Some(token)),
            Some(token) => (Self::decode_list_token(Some(&token))?, None),
            None => (None, None),
        };
        let page = ObjectListingPage {
            contents,
            common_prefixes,
            continuation_token,
            sealed,
        };
        Ok((page, lister))
    }

    /// One merged object page over every range under the prefix, at most `max_keys` entries,
    /// and the ranges a copy listed because their coordinator could not.
    pub(in crate::s3::service) async fn merged_objects(
        &self,
        user: &UserAccess,
        binding: &DistributionBinding,
        input: ListBucketInput,
        (owner, url_encoded, scope): (Option<Owner>, bool, Option<&SubpathScope>),
        (from, bounded): (Option<Resume>, bool),
    ) -> S3Result<(ObjectListingPage, Option<Resume>, Vec<Ulid>)> {
        let snapshot = self.snapshot(binding).await?;
        let fence = self.fence(binding).await?;
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
            sealed: None,
        };
        let mut last_prefix = resume.as_ref().and_then(|(_, group)| group.clone());
        // A sealed position resumes its range at the member that sealed it.
        let (from, mut resumed) = match from {
            Some(Resume::From(lower)) => (Some(lower), None),
            Some(Resume::Sealed {
                lower,
                node,
                copy,
                cursor,
                group,
            }) => {
                last_prefix = group;
                (Some(lower), Some((node, copy, cursor)))
            }
            _ => (None, None),
        };
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
        let mut stale = Vec::new();
        for (visited, range) in ranges.enumerate() {
            let remaining =
                max.saturating_sub(merged.contents.len() + merged.common_prefixes.len());
            let full = bounded && (visited == MAX_PAGE_RANGES || bytes >= MAX_PAGE_BYTES);
            if remaining == 0 || full {
                merged.continuation_token =
                    scoped_marker(&input.bucket, last_key.as_deref(), page_prefix.as_deref())?;
                // Nothing listed yet: the next page starts at this range.
                let next = (merged.continuation_token.is_none())
                    .then(|| Resume::From(range.lower.clone()));
                return Ok((merged, next, stale));
            }
            let (context, shape) = ((binding, &snapshot), (owner.as_ref(), url_encoded));
            let resumed = resumed.take();
            let local = match &resumed {
                Some((node, ..)) => *node == self.node_id,
                None => snapshot.lists_locally(binding.member_id, range, &fence),
            };
            let remote = match (local, &resumed) {
                (true, _) => None,
                (false, Some((node, copy, cursor))) => {
                    let start = Some(("continuation-token", cursor.clone()));
                    let query = (&input, start, remaining);
                    let asked = (range, *copy, Some(*node));
                    let part = self
                        .remote_objects(user, context, asked, query, shape)
                        .await?;
                    Some((part, *copy))
                }
                (false, None) => {
                    let query = (&input, remote_start(&input, first)?, remaining);
                    let asked = (range, false, None);
                    match self
                        .remote_objects(user, context, asked, query.clone(), shape)
                        .await
                    {
                        Ok(part) => Some((part, false)),
                        // The coordinator cannot answer, so this member's copy or another lists.
                        Err(error) if unreachable(&error) => {
                            stale.push(range.range_id);
                            let here = (snapshot.record_holders(range).iter())
                                .any(|member| member.member_id == binding.member_id);
                            match here {
                                true => None,
                                false => Some((
                                    (self.remote_objects(
                                        user,
                                        context,
                                        (range, true, None),
                                        query,
                                        shape,
                                    ))
                                    .await?,
                                    true,
                                )),
                            }
                        }
                        Err(error) => return Err(error),
                    }
                }
            };
            let (part, lister, copy) = match remote {
                Some(((mut part, lister), copy)) => {
                    if let Some(scope) = scope {
                        scope_part(&mut part, scope, url_encoded);
                    }
                    (part, lister, copy)
                }
                None => {
                    let mut sub = input.clone();
                    sub.max_keys = Some(remaining);
                    if !first || resumed.is_some() {
                        (sub.continuation_token, sub.start_after) = (None, None);
                    }
                    let hidden = match &resumed {
                        Some((_, _, cursor)) => {
                            Some(open_cursor::<HiddenCursor>(cursor).ok_or_else(|| {
                                s3_error!(InvalidArgument, "Invalid continuation token")
                            })?)
                        }
                        None => None,
                    };
                    let bounds = Some((range.lower.clone(), range.upper.clone()));
                    let reader = (owner.clone(), Some(user));
                    let part =
                        self.run_object_listing(sub, reader, url_encoded, scope, (bounds, hidden));
                    // Listing here instead of the coordinator lists this member's copy.
                    let copy = resumed.as_ref().map_or(!local, |(_, copy, _)| *copy);
                    (part.await?, self.node_id, copy)
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
            // A range that stopped among hidden entries goes on from its member's cursor.
            if let Some(cursor) = part.sealed {
                let next = Resume::Sealed {
                    lower: range.lower.clone(),
                    node: lister,
                    copy,
                    cursor,
                    group: last_prefix,
                };
                return Ok((merged, Some(next), stale));
            }
            if part.continuation_token.is_some() {
                merged.continuation_token = part.continuation_token;
                break;
            }
        }
        Ok((merged, None, stale))
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
            sealed: None,
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
