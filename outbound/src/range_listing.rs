//! Lists one key range at another member of a distributed bucket: its REST listing with the
//! caller's bearer, or its S3 listing under a delegation this node signs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::outbound::RangeRefusal;
use aruna_core::outbound::{RangeAccess, RangeAnswer, RangeEntry, RangeListing, RangePage};
use aruna_core::structs::storage::distribution::delegation::{
    DELEGATION_HEADER, Delegation, Payload, SignedDelegation, request_digest,
};
use reqwest::{Response, Url};
use serde::Deserialize;
use tracing::warn;

use crate::OutboundHandle;

pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a member may take to start its answer.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(30);
/// Bytes of a member's answer read at most; a longer answer fails instead of filling memory.
const MAX_ANSWER_BYTES: usize = 16 * 1024 * 1024;
const API_PREFIX: &str = "/api/v1";
const RELAY_HOP_HEADER: &str = "x-aruna-relay-hop";
const RELAY_SET_HEADER: &str = "x-aruna-relay-set";
const LOWER_PARAM: &str = "aruna-lower";
const UPPER_PARAM: &str = "aruna-upper";
const COPY_PARAM: &str = "aruna-copy";
const ACCEPT_ANY: &str = "*/*";

/// Signs a delegation with this node's key; `None` when this node cannot.
pub trait ListingSigner: Send + Sync {
    fn sign(&self, delegation: Delegation) -> Option<SignedDelegation>;
}

/// A signer for a backend run without a node key.
pub(crate) struct NoSigner;

impl ListingSigner for NoSigner {
    fn sign(&self, _: Delegation) -> Option<SignedDelegation> {
        None
    }
}

#[derive(Deserialize)]
struct RestPage {
    #[serde(default)]
    objects: Vec<RangeEntry>,
    #[serde(default)]
    prefixes: Vec<String>,
    next_cursor: Option<String>,
    #[serde(default)]
    stale_ranges: Vec<String>,
}

#[derive(Default, Deserialize)]
struct RestError {
    #[serde(default)]
    error: String,
}

/// The url of the member's REST listing of the range.
fn rest_url(listing: &RangeListing) -> Option<Url> {
    let base = listing.base_url.trim_end_matches('/');
    let nest = if base.ends_with(API_PREFIX) {
        ""
    } else {
        API_PREFIX
    };
    let path = format!("{base}{nest}/data/buckets/{}/objects", listing.bucket);
    let mut url = Url::parse(&path).ok()?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("limit", &listing.limit.to_string());
        let range = &listing.range;
        for (name, value) in [
            ("prefix", listing.prefix.as_deref()),
            ("delimiter", listing.delimiter.as_deref()),
            ("cursor", listing.position.as_deref()),
            ("lower", Some(range.lower.as_str())),
            ("upper", range.upper.as_deref()),
            ("copy", listing.copy.then_some("true")),
        ] {
            if let Some(value) = value {
                query.append_pair(name, value);
            }
        }
    }
    Some(url)
}

/// The url of the member's S3 listing of the range.
fn s3_url(listing: &RangeListing) -> Option<Url> {
    let base = listing.base_url.trim_end_matches('/');
    let mut url = Url::parse(&format!("{base}/{}/", listing.bucket)).ok()?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("list-type", "2");
        query.append_pair("max-keys", &listing.limit.to_string());
        let named = |value: &Option<String>| value.clone().filter(|value| !value.is_empty());
        if let Some(prefix) = named(&listing.prefix) {
            query.append_pair("prefix", &prefix);
        }
        if let Some(delimiter) = named(&listing.delimiter) {
            query.append_pair("delimiter", &delimiter);
        }
        if let Some(token) = &listing.position {
            query.append_pair("continuation-token", token);
        }
        query.append_pair(LOWER_PARAM, &listing.range.lower);
        if let Some(upper) = &listing.range.upper {
            query.append_pair(UPPER_PARAM, upper);
        }
        if listing.copy {
            query.append_pair(COPY_PARAM, "1");
        }
    }
    Some(url)
}

/// The delegation for exactly this listing, signed by this node.
fn signed_header(
    listing: &RangeListing,
    delegation: &Delegation,
    query: Option<&str>,
    signer: &dyn ListingSigner,
) -> Option<String> {
    let accept = [("accept", ACCEPT_ANY)];
    let bound = Delegation {
        operation: "ListObjectsV2".to_string(),
        bucket: listing.bucket.clone(),
        key: String::new(),
        copy_source: None,
        request_digest: request_digest("GET", &listing.bucket, "", query, &accept),
        payload: Payload::Empty,
        ..delegation.clone()
    };
    signer.sign(bound)?.encode().ok()
}

impl OutboundHandle {
    /// Lists one range at its member. Member addresses come from the realm's own records, so
    /// this client passes no egress screen.
    pub async fn list_range(
        &self,
        listing: RangeListing,
        signer: &dyn ListingSigner,
    ) -> RangeAnswer {
        let client = &self.inner.members;
        let request = match &listing.access {
            RangeAccess::Bearer(token) => {
                let Some(url) = rest_url(&listing) else {
                    return RangeAnswer::Silent;
                };
                let mut request = client
                    .get(url)
                    .header(RELAY_HOP_HEADER, listing.local_node.to_string())
                    .header(RELAY_SET_HEADER, listing.set_id.to_string());
                if let Some(token) = token {
                    let Ok(token) = std::str::from_utf8(token.bytes().expose()) else {
                        return RangeAnswer::Silent;
                    };
                    request = request.bearer_auth(token);
                }
                request
            }
            RangeAccess::Delegated(delegation) => {
                let Some(url) = s3_url(&listing) else {
                    return RangeAnswer::Silent;
                };
                let header = signed_header(&listing, delegation, url.query(), signer);
                let Some(header) = header else {
                    return RangeAnswer::Silent;
                };
                client
                    .get(url)
                    .header(reqwest::header::ACCEPT, ACCEPT_ANY)
                    .header(DELEGATION_HEADER, header)
            }
        };
        let response = match tokio::time::timeout(ANSWER_TIMEOUT, request.send()).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                let connect = error.is_connect();
                let error = error.without_url();
                warn!(node_id = %listing.node_id, connect, error = %error, "A member did not take a range listing");
                return RangeAnswer::Silent;
            }
            Err(_) => {
                warn!(node_id = %listing.node_id, "A member did not start answering a range listing in time");
                return RangeAnswer::Silent;
            }
        };
        let status = response.status();
        let body = capped_body(response).await;
        match listing.access {
            RangeAccess::Bearer(_) => rest_answer(&listing, status, body),
            RangeAccess::Delegated(_) => s3_answer(&listing, status, body),
        }
    }
}

/// The body of an answer, at most [`MAX_ANSWER_BYTES`]; a broken body is an error.
async fn capped_body(mut response: Response) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| error.without_url().to_string())?
    {
        if body.len() + chunk.len() > MAX_ANSWER_BYTES {
            return Err(format!("the answer exceeds {MAX_ANSWER_BYTES} bytes"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn refused(
    listing: &RangeListing,
    status: u16,
    code: Option<String>,
    message: String,
) -> RangeAnswer {
    RangeAnswer::Refused(RangeRefusal {
        node_id: listing.node_id,
        status,
        code,
        message,
    })
}

fn rest_answer(
    listing: &RangeListing,
    status: reqwest::StatusCode,
    body: Result<Vec<u8>, String>,
) -> RangeAnswer {
    if !status.is_success() {
        let body = body.unwrap_or_default();
        let error = serde_json::from_slice::<RestError>(&body).unwrap_or_default();
        return refused(listing, status.as_u16(), None, error.error);
    }
    let body = match body {
        Ok(body) => body,
        Err(error) => {
            let message = format!("node {} answered incompletely: {error}", listing.node_id);
            return refused(listing, 502, None, message);
        }
    };
    match serde_json::from_slice::<RestPage>(&body) {
        Ok(page) => RangeAnswer::Page(RangePage {
            objects: page.objects,
            prefixes: page.prefixes,
            next: page.next_cursor,
            stale_ranges: page.stale_ranges,
        }),
        Err(error) => {
            let message = format!("the member's answer does not decode: {error}");
            refused(listing, 502, None, message)
        }
    }
}

fn s3_answer(
    listing: &RangeListing,
    status: reqwest::StatusCode,
    body: Result<Vec<u8>, String>,
) -> RangeAnswer {
    if !status.is_success() {
        let body = String::from_utf8_lossy(body.as_deref().unwrap_or_default()).into_owned();
        let message = xml_field(&body, "Message").unwrap_or_default();
        return refused(listing, status.as_u16(), xml_field(&body, "Code"), message);
    }
    let body = match body {
        Ok(body) => body,
        Err(error) => {
            return refused(
                listing,
                503,
                None,
                format!("a member's answer broke off: {error}"),
            );
        }
    };
    let Ok(body) = String::from_utf8(body) else {
        return refused(
            listing,
            503,
            None,
            "a member answered with invalid text".to_string(),
        );
    };
    RangeAnswer::Page(s3_page(&body))
}

/// A member's S3 listing body as one page.
fn s3_page(body: &str) -> RangePage {
    let objects = (elements(body, "Contents").into_iter())
        .filter_map(|entry| {
            Some(RangeEntry {
                key: xml_field(entry, "Key")?,
                size: xml_field(entry, "Size").and_then(|size| size.parse().ok()),
                last_modified: xml_field(entry, "LastModified"),
                etag: xml_field(entry, "ETag"),
                ..RangeEntry::default()
            })
        })
        .collect();
    let prefixes = (elements(body, "CommonPrefixes").into_iter())
        .filter_map(|entry| xml_field(entry, "Prefix"))
        .collect();
    RangePage {
        objects,
        prefixes,
        next: xml_field(body, "NextContinuationToken"),
        stale_ranges: Vec::new(),
    }
}

/// The XML elements `tag` of a listing body.
fn elements<'a>(body: &'a str, tag: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    body.split(open.as_str())
        .skip(1)
        .filter_map(|part| part.split(close.as_str()).next())
        .collect()
}

/// The text between `<tag>` and `</tag>`, with basic entities decoded.
fn xml_field(body: &str, tag: &str) -> Option<String> {
    let start = body.find(&format!("<{tag}>"))? + tag.len() + 2;
    let end = start + body.get(start..)?.find(&format!("</{tag}>"))?;
    let text = body.get(start..end)?;
    Some(
        text.replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'")
            .replace("&amp;", "&"),
    )
}

#[cfg(test)]
#[path = "range_tests.rs"]
mod tests;
