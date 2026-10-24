//! Finds the datasets that link Aruna data: one identifier, or a file through its version, its
//! content and the folders that contain its key. Every metadata node answers from its own
//! postings within one budget; results merge by document id and say whether they are complete.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::structs::storage::blob::{BlobHeadKey, CurrentVersionPointer, VersionKey};
use aruna_core::structs::storage::data_link::{DataLink, LinkScope};

use aruna_core::structs::storage::data_link::LinkPosting;

use super::{
    ApiQueryMode, AuthContext, BTreeMap, BlobVersion, Deserialize, DriverContext, Event,
    MetadataApiError, MetadataFanoutOperation, MetadataFanoutScope, MetadataNodeCall,
    MetadataReadError, NodeId, RealmId, Serialize, Span, StorageEffect, StorageEvent, Ulid,
    can_read_record, fanout_bearer, load_live_record, map_read_error, metadata_node_call,
    reference_document_title, run_metadata_fanout,
};
use crate::metadata::data_links::{
    LinkBudget, key_prefixes, read_postings, set_postings, unsettled_documents, unverified_versions,
};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

/// Documents one page returns at most.
pub const LINKS_MAX_LIMIT: usize = 100;
const NODE_SEEKS: usize = 1100;
const NODE_ROWS: usize = 4096;
const UNSETTLED_CAP: usize = 1024;

/// One identifier, or the prefixes of a key a bucket's prefix postings are probed for.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkQuery {
    Exact(DataLink),
    KeyPrefixes { scope: LinkScope, key: String },
}

/// What one metadata node is asked: the queries, the page position and size.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkNodeRequest {
    pub queries: Vec<LinkQuery>,
    /// Only postings of linked parts (Dataset-typed `hasPart` of a root) count.
    pub parts_only: bool,
    /// Documents after this id; documents sort by id on every node.
    pub after: Option<Ulid>,
    pub limit: usize,
}

/// One link of a document to one of the queried identifiers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinkHit {
    pub id: String,
    pub part: bool,
    pub subject_iris: Vec<String>,
}

/// One readable document that links the target.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinkDocument {
    pub document_id: Ulid,
    pub group_id: String,
    pub document_path: String,
    pub graph_iri: String,
    pub title: Option<String>,
    pub links: Vec<LinkHit>,
}

/// One node's answer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkNodePage {
    pub documents: Vec<LinkDocument>,
    /// More documents may follow the last one of this page.
    pub more: bool,
    /// The budget ran out before every queried identifier was read.
    pub cut: bool,
    /// A matching document exists that the caller may not read.
    pub hidden: bool,
    pub pending_materializations: usize,
    pub failed_materializations: usize,
}

/// The merged answer of every metadata node.
#[derive(Clone, Debug, Default)]
pub struct LinkPage {
    pub documents: Vec<LinkDocument>,
    pub next_after: Option<Ulid>,
    /// Every node answered with every posting read and no materialization unsettled.
    pub complete: bool,
    pub hidden: bool,
    pub nodes_failed: usize,
    pub pending_materializations: usize,
    pub failed_materializations: usize,
}

/// Answers a link lookup from this node's postings within one shared budget.
pub async fn links_local(
    context: &DriverContext,
    realm_id: RealmId,
    auth: Option<&AuthContext>,
    request: LinkNodeRequest,
) -> Result<LinkNodePage, MetadataApiError> {
    let internal = |error: crate::metadata::data_links::DataLinkError| {
        MetadataApiError::Internal(error.to_string())
    };
    let storage = &context.storage_handle;
    let limit = request.limit.clamp(1, LINKS_MAX_LIMIT);
    let mut budget = LinkBudget {
        seeks: NODE_SEEKS,
        rows: NODE_ROWS,
    };
    let mut page = LinkNodePage::default();
    let mut links = Vec::new();
    for query in request.queries {
        match query {
            LinkQuery::Exact(link) => links.push(link),
            LinkQuery::KeyPrefixes { scope, key } => {
                let (found, complete) = key_prefixes(storage, &scope, &key, &mut budget)
                    .await
                    .map_err(internal)?;
                page.cut |= !complete;
                links.extend(found);
            }
        }
    }
    let mut matches = BTreeMap::<Ulid, Vec<(DataLink, LinkPosting)>>::new();
    for link in links {
        let (postings, more) =
            read_postings(storage, &link.key(), request.after, limit, &mut budget)
                .await
                .map_err(internal)?;
        page.more |= more;
        for (document_id, posting) in postings {
            if request.parts_only && !posting.part {
                continue;
            }
            matches
                .entry(document_id)
                .or_default()
                .push((link.clone(), posting));
        }
    }
    page.cut |= budget.exhausted();
    if matches.len() > limit {
        page.more = true;
    }
    let documents: BTreeMap<_, _> = matches.into_iter().take(limit).collect();
    describe(context, realm_id, auth, documents, &mut page).await?;
    let (pending, failed) = unsettled_documents(storage, UNSETTLED_CAP)
        .await
        .map_err(internal)?;
    page.pending_materializations = pending;
    page.failed_materializations = failed;
    Ok(page)
}

/// Adds the readable documents of `matches` to `page`: postings of an older document event
/// are dropped, unreadable documents only set `hidden`.
async fn describe(
    context: &DriverContext,
    realm_id: RealmId,
    auth: Option<&AuthContext>,
    matches: BTreeMap<Ulid, Vec<(DataLink, LinkPosting)>>,
    page: &mut LinkNodePage,
) -> Result<(), MetadataApiError> {
    for (document_id, postings) in matches {
        let record = match load_live_record(context, document_id).await {
            Ok(record) => record,
            Err(MetadataApiError::NotFound) => continue,
            Err(error) => return Err(error),
        };
        let current: Vec<_> = postings
            .into_iter()
            .filter(|(_, posting)| posting.cursor == record.last_event_id)
            .collect();
        if current.is_empty() {
            continue;
        }
        if !can_read_record(context, realm_id, auth, &record).await? {
            page.hidden = true;
            continue;
        }
        page.documents.push(LinkDocument {
            document_id,
            group_id: record.group_id.to_string(),
            document_path: record.document_path.clone(),
            graph_iri: record.graph_iri.clone(),
            title: reference_document_title(context, &record).await,
            links: current
                .into_iter()
                .map(|(link, posting)| LinkHit {
                    id: link.to_id(),
                    part: posting.part,
                    subject_iris: posting.subjects,
                })
                .collect(),
        });
    }
    Ok(())
}

/// One node's page of the documents that link a distributed set: its prefix links and its
/// verified version links, in posting key order. For the set's deletion preview.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetPreviewPage {
    pub page: LinkNodePage,
    /// The posting key to continue after; `None` once both ranges are read.
    pub next: Option<Vec<u8>>,
    /// Version links on this node that no member could verify, counted up to a cap; while
    /// any exist the preview is incomplete.
    pub unverified: usize,
}

/// Reads one page of the postings that name `set_id` on this node.
pub async fn set_preview_local(
    context: &DriverContext,
    realm_id: RealmId,
    auth: Option<&AuthContext>,
    set_id: Ulid,
    after: Option<Vec<u8>>,
    limit: usize,
) -> Result<SetPreviewPage, MetadataApiError> {
    let internal = |error: crate::metadata::data_links::DataLinkError| {
        MetadataApiError::Internal(error.to_string())
    };
    let storage = &context.storage_handle;
    let scope = LinkScope::Set { realm_id, set_id };
    let limit = limit.clamp(1, LINKS_MAX_LIMIT);
    let (rows, next) = set_postings(storage, &scope, set_id, after, limit)
        .await
        .map_err(internal)?;
    let mut matches = BTreeMap::<Ulid, Vec<(DataLink, LinkPosting)>>::new();
    let scope_key = scope.key();
    for (key, document_id, posting) in rows {
        let length = key
            .starts_with(&scope_key)
            .then(|| scope.length_of(&key))
            .flatten();
        let link = match length {
            Some(length) => {
                let start = scope_key.len() + 2;
                let prefix = key
                    .get(start..start + usize::from(length))
                    .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                    .unwrap_or_default();
                scope.link(&prefix)
            }
            None => DataLink::SetVersion {
                realm_id,
                set_id,
                version_id: version_of(&key).unwrap_or_default(),
            },
        };
        matches
            .entry(document_id)
            .or_default()
            .push((link, posting));
    }
    let mut preview = SetPreviewPage {
        next,
        ..SetPreviewPage::default()
    };
    preview.page.more = preview.next.is_some();
    describe(context, realm_id, auth, matches, &mut preview.page).await?;
    preview.unverified = unverified_versions(storage, UNSETTLED_CAP)
        .await
        .map_err(internal)?;
    let (pending, failed) = unsettled_documents(storage, UNSETTLED_CAP)
        .await
        .map_err(internal)?;
    preview.page.pending_materializations = pending;
    preview.page.failed_materializations = failed;
    Ok(preview)
}

/// The version id of a set-version posting key: the 16 bytes before the document.
fn version_of(key: &[u8]) -> Option<Ulid> {
    let end = key.len().checked_sub(16)?;
    let bytes: [u8; 16] = key.get(end.checked_sub(16)?..end)?.try_into().ok()?;
    Some(Ulid::from_bytes(bytes))
}

/// Asks every metadata node and merges their pages by document id.
pub async fn links_distributed(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    auth: Option<AuthContext>,
    bearer_token: Option<String>,
    request: LinkNodeRequest,
) -> Result<LinkPage, MetadataApiError> {
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let limit = request.limit.clamp(1, LINKS_MAX_LIMIT);
    let local_call: MetadataNodeCall<LinkNodePage> = metadata_node_call(
        (context.clone(), auth, realm_id, request.clone()),
        |(context, auth, realm_id, request), _| async move {
            links_local(&context, realm_id, auth.as_ref(), request)
                .await
                .map_err(crate::forward::transport::read_error)
        },
    );
    let remote_call: MetadataNodeCall<LinkNodePage> = metadata_node_call(
        (handle, fanout_bearer(bearer_token.as_deref()), request),
        |(handle, auth_token, request), node_id| async move {
            handle
                .request_data_links(node_id, auth_token, request)
                .await
        },
    );
    let (parts, stats) = run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        MetadataFanoutScope::new(Some(ApiQueryMode::Distributed), None, true),
        MetadataFanoutOperation::IdentifierLookup,
        local_call,
        remote_call,
        record_links_result,
        map_read_error,
    )
    .await?;
    let failed = stats.nodes_failed + usize::from(stats.discovery_failed);
    Ok(merge_pages(
        parts.into_iter().map(|(_, page)| page),
        limit,
        failed,
    ))
}

/// Merges node pages: one entry per document, the first `limit` documents by id.
pub fn merge_pages(
    pages: impl IntoIterator<Item = LinkNodePage>,
    limit: usize,
    nodes_failed: usize,
) -> LinkPage {
    let mut merged = BTreeMap::<Ulid, LinkDocument>::new();
    let mut page = LinkPage {
        nodes_failed,
        ..LinkPage::default()
    };
    let mut more = false;
    let mut cut = false;
    for node in pages {
        more |= node.more;
        cut |= node.cut;
        page.hidden |= node.hidden;
        page.pending_materializations += node.pending_materializations;
        page.failed_materializations += node.failed_materializations;
        for document in node.documents {
            let entry = merged
                .entry(document.document_id)
                .or_insert_with(|| LinkDocument {
                    links: Vec::new(),
                    ..document.clone()
                });
            for link in document.links {
                if !entry.links.contains(&link) {
                    entry.links.push(link);
                }
            }
        }
    }
    more |= merged.len() > limit;
    page.documents = merged.into_values().take(limit).collect();
    page.next_after = more
        .then(|| page.documents.last().map(|document| document.document_id))
        .flatten();
    page.complete = !more
        && !cut
        && nodes_failed == 0
        && page.pending_materializations == 0
        && page.failed_materializations == 0;
    page
}

fn record_links_result(span: &Span, result: &Result<LinkNodePage, MetadataReadError>) {
    match result {
        Ok(page) => {
            span.record("result", "ok");
            span.record("hit_count", page.documents.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

/// The queries for a file of a local bucket: its version and content, and the prefixes of
/// its key. A distributed bucket is named by its set, an ordinary one by this node.
pub async fn file_queries(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    bucket: &str,
    key: &str,
    version_id: Option<Ulid>,
) -> Result<Vec<LinkQuery>, MetadataApiError> {
    let info = crate::driver::drive(GetBucketOperation::new(bucket.to_string()), context)
        .await
        .map_err(|error| match error {
            GetBucketError::NotFound => MetadataApiError::NotFound,
            other => MetadataApiError::Internal(other.to_string()),
        })?;
    let version_id = match version_id {
        Some(version_id) => Some(version_id),
        None => read_head(context, bucket, key).await?,
    };
    let version = match version_id {
        Some(version_id) => read_version(context, bucket, key, version_id).await?,
        None => None,
    };
    let scope = match &info.distribution {
        Some(binding) => LinkScope::Set {
            realm_id,
            set_id: binding.set_id,
        },
        None => LinkScope::Node {
            realm_id,
            node_id,
            bucket: bucket.to_string(),
        },
    };
    let mut queries = vec![LinkQuery::KeyPrefixes {
        scope,
        key: key.to_string(),
    }];
    if let (Some(_), Some(version_id)) = (&info.distribution, version_id) {
        queries.push(LinkQuery::Exact(DataLink::Version {
            realm_id,
            version_id,
        }));
    }
    if let Some(hash) = version
        .as_ref()
        .and_then(|version| version.blob_hash().copied())
    {
        queries.push(LinkQuery::Exact(DataLink::Hash(hash)));
    }
    Ok(queries)
}

async fn read_head(
    context: &DriverContext,
    bucket: &str,
    key: &str,
) -> Result<Option<Ulid>, MetadataApiError> {
    let head = BlobHeadKey::new(bucket, key)
        .to_bytes()
        .map_err(|_| MetadataApiError::BadRequest)?;
    Ok(read_row(context, BLOB_HEAD_KEYSPACE, head)
        .await?
        .map(|value| CurrentVersionPointer::from_bytes(&value))
        .transpose()
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?
        .map(|pointer| pointer.version_id))
}

async fn read_version(
    context: &DriverContext,
    bucket: &str,
    key: &str,
    version_id: Ulid,
) -> Result<Option<BlobVersion>, MetadataApiError> {
    let row = VersionKey::new(bucket, key, version_id)
        .to_bytes()
        .map_err(|_| MetadataApiError::BadRequest)?;
    read_row(context, BLOB_VERSIONS_KEYSPACE, row)
        .await?
        .map(|value| BlobVersion::from_bytes(&value))
        .transpose()
        .map_err(|error| MetadataApiError::Internal(error.to_string()))
}

async fn read_row(
    context: &DriverContext,
    key_space: &str,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, MetadataApiError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            Ok(value.map(|value| value.to_vec()))
        }
        Event::Storage(StorageEvent::Error { error }) => {
            Err(MetadataApiError::Internal(error.to_string()))
        }
        other => Err(MetadataApiError::Internal(format!("{other:?}"))),
    }
}

#[cfg(test)]
#[path = "links_tests.rs"]
mod tests;
