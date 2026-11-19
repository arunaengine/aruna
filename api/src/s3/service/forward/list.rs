//! Lists a distributed bucket across its key ranges into one bounded page in key order, each
//! range from local records or by a holder; prefixes that span a range bound appear once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::blob::{BucketInfo, UserAccess};
use aruna_core::structs::storage::distribution::DistributionBinding;
use aruna_operations::distribution::routing::{copies_interval, lists_interval};
use aruna_operations::driver::drive;
use aruna_operations::s3::object::list::ListBucketInput;
pub(in crate::s3::service) use aruna_operations::s3::object::member_merge::MergeResume as Resume;
use aruna_operations::s3::object::member_merge::{
    MergeCaller, MergeError, MergeShows, MergedObject, MergedObjectsOperation,
};
use aruna_operations::s3::object::visible::VisibleError;
use percent_encoding::{percent_decode_str, utf8_percent_encode};
use s3s::dto::{CommonPrefix, ETag, Object, Owner, Timestamp, TimestampFormat};
use s3s::{S3Error, S3ErrorCode, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::reads::delegated_caller;
use crate::s3::auth::DelegatedRequest;
use crate::s3::error::{IntoS3Error, guard_error};
use crate::s3::scope::SubpathScope;
use crate::s3::service::link::link_error;
use crate::s3::service::listing::{ObjectListingPage, ScopedKeys};
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
            aruna_core::cursor::encode_value(&token)
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
            let token: BoundToken =
                aruna_core::cursor::decode_value(token).map_err(|_| invalid())?;
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

/// No holder of a range answered, so the listing cannot be complete.
pub(in crate::s3::service) fn incomplete(set_id: Ulid, range_id: Ulid) -> S3Error {
    let mut error = S3Error::with_message(
        S3ErrorCode::Custom("ListingIncomplete".into()),
        format!(
            "No holder of range {range_id} of distributed bucket {set_id} answered, so the listing \
             would be incomplete. Retry later."
        ),
    );
    error.set_status_code(http::StatusCode::SERVICE_UNAVAILABLE);
    error
}

pub(in crate::s3::service) fn timestamp(text: Option<String>) -> Option<Timestamp> {
    text.and_then(|text| Timestamp::parse(TimestampFormat::DateTime, &text).ok())
}

pub(in crate::s3::service) fn e_tag(text: Option<String>) -> Option<ETag> {
    text.and_then(|text| ETag::parse_http_header(text.as_bytes()).ok())
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
        let held = match drive(lists_interval(binding, &lower, upper.as_deref()), state).await {
            Ok(false) if named_copy(query) => {
                drive(copies_interval(binding, &lower, upper.as_deref()), state).await
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

    /// Who a merged listing asks other members for.
    pub(in crate::s3::service) fn merge_caller(
        &self,
        user: &UserAccess,
        binding: &DistributionBinding,
    ) -> MergeCaller {
        MergeCaller {
            ids: self.state.node_and_realm(),
            caller: delegated_caller(user),
            binding: binding.clone(),
        }
    }

    /// A merged listing's failure as this node's answer.
    pub(in crate::s3::service) fn merge_error(&self, error: MergeError) -> S3Error {
        match error {
            MergeError::Route(error) => guard_error(&error),
            MergeError::Member(refusal) => self.member_error(refusal.into()),
            MergeError::Incomplete { set_id, range_id } => incomplete(set_id, range_id),
            MergeError::Token => s3_error!(InvalidArgument, "Invalid continuation token"),
            MergeError::VersionMarker => s3_error!(InvalidArgument, "Invalid version-id-marker"),
            MergeError::Listing(VisibleError::List(error)) => error.into_s3_error(),
            MergeError::Listing(VisibleError::Shows(error)) => link_error(error),
            MergeError::Listing(VisibleError::Cursor) => {
                s3_error!(InternalError, "The listing cursor was not sealed")
            }
            MergeError::Listing(VisibleError::Io(error)) => s3_error!(InternalError, "{error}"),
            MergeError::Failed(reason) => s3_error!(InternalError, "{reason}"),
        }
    }

    /// One merged object page over every range under the prefix, at most `max_keys` entries,
    /// and the ranges a copy listed because their coordinator could not.
    pub(in crate::s3::service) async fn merged_objects(
        &self,
        user: &UserAccess,
        binding: &DistributionBinding,
        input: ListBucketInput,
        (owner, url_encoded, scope): (Option<Owner>, bool, Option<&SubpathScope>),
        resume: (Option<Resume>, bool),
    ) -> S3Result<(ObjectListingPage, Option<Resume>, Vec<Ulid>)> {
        let shows = MergeShows {
            keys: ScopedKeys(scope.cloned()),
            caller: self.listing_caller(Some(user), &input.bucket).await,
            realm_id: self.realm_id,
            node: self.link_node(),
        };
        let from = self.merge_caller(user, binding);
        let merge = MergedObjectsOperation::new(from, shows, input, resume);
        let merged =
            (drive(merge, self.state.as_ref()).await).map_err(|error| self.merge_error(error))?;
        let contents = (merged.objects.into_iter())
            .map(|object| match object {
                MergedObject::Local(object) => self.listed_object(*object, &owner, url_encoded),
                MergedObject::Remote(object) => Object {
                    key: Some(self.encoded(object.key, url_encoded)),
                    last_modified: timestamp(object.last_modified),
                    e_tag: e_tag(object.etag),
                    size: object.size,
                    owner: owner.clone(),
                    ..Default::default()
                },
            })
            .collect();
        let common_prefixes = (merged.prefixes.into_iter())
            .map(|prefix| CommonPrefix {
                prefix: Some(self.encoded(prefix, url_encoded)),
            })
            .collect();
        let page = ObjectListingPage {
            contents,
            common_prefixes,
            continuation_token: merged.token,
            sealed: None,
        };
        Ok((page, merged.next, merged.stale))
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
        let contents = xml_elements(body, "Contents");
        assert_eq!(xml_field(contents[0], "Key").as_deref(), Some("a&b"));
        let prefixes = xml_elements(body, "CommonPrefixes");
        assert_eq!(xml_field(prefixes[0], "Prefix").as_deref(), Some("d/"));
    }
}
