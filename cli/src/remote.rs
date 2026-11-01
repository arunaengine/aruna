//! Parses `bucket/key` paths, finds the node that serves a bucket, and lists its objects.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::client::Api;
use crate::error::CliError;
use serde::{Deserialize, Serialize};

/// The prefix of an object's content address; the rest is its BLAKE3 digest in hex.
const CONTENT_PREFIX: &str = "https://w3id.org/aruna/data/";

/// A bucket and a key or prefix inside it, written `bucket/key` or `s3://bucket/key`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemotePath {
    pub bucket: String,
    pub key: String,
}

impl RemotePath {
    pub fn parse(raw: &str) -> Result<Self, CliError> {
        let trimmed = raw.strip_prefix("s3://").unwrap_or(raw);
        let (bucket, key) = trimmed.split_once('/').unwrap_or((trimmed, ""));
        if bucket.is_empty() || bucket.contains(['\\', ' ']) {
            return Err(CliError::usage(format!(
                "{raw} is not a remote path; write bucket/key"
            )));
        }
        Ok(Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
    }

    /// The key as a listing prefix: empty, or ending with `/`.
    pub fn folder(&self) -> String {
        match self.key.trim_end_matches('/') {
            "" => String::new(),
            key => format!("{key}/"),
        }
    }
}

/// Whether the argument names remote data without doubt.
pub fn is_explicit(raw: &str) -> bool {
    raw.starts_with("s3://")
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BucketHit {
    pub bucket: String,
    pub node_id: String,
    pub group_id: String,
    pub group_name: Option<String>,
    #[serde(default)]
    pub created_at: String,
    pub set_id: Option<String>,
    #[serde(default)]
    pub public: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct BucketPage {
    #[serde(default)]
    pub hits: Vec<BucketHit>,
    #[serde(default)]
    pub nodes_queried: usize,
    #[serde(default)]
    pub nodes_failed: usize,
    #[serde(default)]
    pub failed_nodes: Vec<String>,
    pub next_cursor: Option<String>,
}

/// The node and group that serve one bucket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Placement {
    pub bucket: String,
    pub node_id: String,
    pub group_id: String,
    pub set_id: Option<String>,
}

/// Picks the node for a bucket like the portal does: the named node, else the signed-in node,
/// else the only host, else any member of one distributed bucket. Several hosts need `--node`.
pub fn choose_node(
    bucket: &str,
    page: &BucketPage,
    local: Option<&str>,
    wanted: Option<&str>,
) -> Result<Placement, CliError> {
    let hits: Vec<&BucketHit> = page
        .hits
        .iter()
        .filter(|hit| hit.bucket == bucket)
        .collect();
    let pick = |hit: &BucketHit| Placement {
        bucket: hit.bucket.clone(),
        node_id: hit.node_id.clone(),
        group_id: hit.group_id.clone(),
        set_id: hit.set_id.clone(),
    };
    if hits.is_empty() {
        let partial = match page.nodes_failed {
            0 => String::new(),
            failed => format!(
                "; {failed} of {} nodes did not answer, so it may be on one of them",
                page.nodes_queried
            ),
        };
        return Err(CliError::routing(format!(
            "bucket {bucket} was not found or is not readable{partial}"
        )));
    }
    if let Some(wanted) = wanted {
        return match hits.iter().find(|hit| hit.node_id == wanted) {
            Some(hit) => Ok(pick(hit)),
            None => Err(CliError::routing(format!(
                "bucket {bucket} is not on node {wanted}; it is on {}",
                node_list(&hits)
            ))),
        };
    }
    if let Some(hit) = hits.iter().find(|hit| Some(hit.node_id.as_str()) == local) {
        return Ok(pick(hit));
    }
    let first = hits[0];
    let one_set = first.set_id.is_some() && hits.iter().all(|hit| hit.set_id == first.set_id);
    if hits.len() == 1 || one_set {
        return Ok(pick(first));
    }
    Err(CliError::routing(format!(
        "bucket {bucket} exists on several nodes ({}); choose one with --node",
        node_list(&hits)
    )))
}

fn node_list(hits: &[&BucketHit]) -> String {
    let nodes: Vec<&str> = hits.iter().map(|hit| hit.node_id.as_str()).collect();
    nodes.join(", ")
}

/// Searches the realm for a bucket and returns where it is served and that node's API.
pub async fn locate(
    api: &Api,
    bucket: &str,
    wanted: Option<&str>,
) -> Result<(Placement, Api), CliError> {
    let page: BucketPage = api
        .get(
            "search/buckets",
            &[("q", bucket.to_string()), ("limit", "50".to_string())],
        )
        .await?;
    let placement = choose_node(bucket, &page, api.node_id.as_deref(), wanted)?;
    let node_api = api.for_node(&placement.node_id).await?;
    Ok((placement, node_api))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ObjectEntry {
    pub key: String,
    pub size: Option<u64>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_type: Option<String>,
    pub content_id: Option<String>,
}

impl ObjectEntry {
    /// The BLAKE3 digest of the content, when the node has read the bytes.
    pub fn blake3(&self) -> Option<&str> {
        self.content_id.as_deref()?.strip_prefix(CONTENT_PREFIX)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ObjectPage {
    #[serde(default)]
    pub objects: Vec<ObjectEntry>,
    #[serde(default)]
    pub prefixes: Vec<String>,
    pub next_cursor: Option<String>,
    #[serde(default)]
    pub stale_ranges: Vec<String>,
}

pub async fn list_page(
    api: &Api,
    bucket: &str,
    prefix: &str,
    delimiter: bool,
    cursor: Option<String>,
) -> Result<ObjectPage, CliError> {
    let mut query = vec![("limit", "1000".to_string())];
    if !prefix.is_empty() {
        query.push(("prefix", prefix.to_string()));
    }
    if delimiter {
        query.push(("delimiter", "/".to_string()));
    }
    if let Some(cursor) = cursor {
        query.push(("cursor", cursor));
    }
    api.get(&format!("data/buckets/{bucket}/objects"), &query)
        .await
}

/// Every object below `prefix`, following all pages; stale ranges are reported once.
pub async fn list_all(api: &Api, bucket: &str, prefix: &str) -> Result<Vec<ObjectEntry>, CliError> {
    let mut objects = Vec::new();
    let mut cursor = None;
    loop {
        let page = list_page(api, bucket, prefix, false, cursor).await?;
        if !page.stale_ranges.is_empty() {
            eprintln!(
                "warning: {} key ranges were listed from a copy that may be behind",
                page.stale_ranges.len()
            );
        }
        objects.extend(page.objects);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(objects),
        }
    }
}

/// The current entry of exactly `key`, if the bucket has one.
pub async fn find_object(
    api: &Api,
    bucket: &str,
    key: &str,
) -> Result<Option<ObjectEntry>, CliError> {
    let page = list_page(api, bucket, key, false, None).await?;
    Ok(page.objects.into_iter().find(|entry| entry.key == key))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(node: &str, set: Option<&str>) -> BucketHit {
        BucketHit {
            bucket: "data".into(),
            node_id: node.into(),
            group_id: "g".into(),
            group_name: None,
            created_at: String::new(),
            set_id: set.map(Into::into),
            public: false,
        }
    }

    fn page(hits: Vec<BucketHit>) -> BucketPage {
        BucketPage {
            hits,
            nodes_queried: 3,
            ..BucketPage::default()
        }
    }

    #[test]
    fn parses_remote_paths() {
        let path = RemotePath::parse("s3://data/raw/a.txt").unwrap();
        assert_eq!(
            (path.bucket.as_str(), path.key.as_str()),
            ("data", "raw/a.txt")
        );
        assert_eq!(RemotePath::parse("data").unwrap().folder(), "");
        assert_eq!(RemotePath::parse("data/raw").unwrap().folder(), "raw/");
        assert_eq!(RemotePath::parse("data/raw/").unwrap().folder(), "raw/");
        assert!(RemotePath::parse("/abs/path").is_err());
    }

    #[test]
    fn prefers_local_node() {
        let page = page(vec![hit("a", None), hit("b", None)]);
        let placement = choose_node("data", &page, Some("b"), None).unwrap();
        assert_eq!(placement.node_id, "b");
    }

    #[test]
    fn refuses_ambiguous_name() {
        let page = page(vec![hit("a", None), hit("b", None)]);
        let error = choose_node("data", &page, Some("c"), None).unwrap_err();
        assert!(error.to_string().contains("--node"), "{error}");
        let chosen = choose_node("data", &page, Some("c"), Some("a")).unwrap();
        assert_eq!(chosen.node_id, "a");
        assert!(choose_node("data", &page, None, Some("z")).is_err());
    }

    #[test]
    fn routes_distributed_member() {
        let page = page(vec![hit("a", Some("set")), hit("b", Some("set"))]);
        let placement = choose_node("data", &page, Some("c"), None).unwrap();
        assert_eq!(placement.node_id, "a");
        assert_eq!(placement.set_id.as_deref(), Some("set"));
    }

    #[test]
    fn reports_partial_search() {
        let mut page = page(vec![hit("a", None)]);
        page.hits[0].bucket = "other".into();
        page.nodes_failed = 1;
        let error = choose_node("data", &page, None, None).unwrap_err();
        assert!(error.to_string().contains("1 of 3 nodes"), "{error}");
    }

    #[test]
    fn reads_content_digest() {
        let entry = ObjectEntry {
            key: "k".into(),
            size: Some(1),
            etag: None,
            last_modified: None,
            content_type: None,
            content_id: Some(format!("{CONTENT_PREFIX}abc")),
        };
        assert_eq!(entry.blake3(), Some("abc"));
    }
}
