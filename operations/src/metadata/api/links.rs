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
    LinkBudget, key_prefixes, read_postings, set_postings, truncated_documents,
    unsettled_documents, unverified_versions,
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
    /// Every matching document up to this id was examined, readable or not, and more may
    /// follow it; `None` once nothing follows.
    pub next: Option<Ulid>,
    /// The budget ran out before every queried identifier was read.
    pub cut: bool,
    /// A matching document exists that the caller may not read.
    pub hidden: bool,
    pub pending_materializations: usize,
    pub failed_materializations: usize,
    /// Documents whose links were cut at the projection limit; any of them may match.
    pub truncated_documents: usize,
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
    pub truncated_documents: usize,
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
    // Each identifier is read up to its last returned document; beyond the smallest such end
    // another identifier may still hold unread documents.
    let mut end: Option<Ulid> = None;
    for link in links {
        let (postings, more) =
            read_postings(storage, &link.key(), request.after, limit, &mut budget)
                .await
                .map_err(internal)?;
        if more {
            match postings.last() {
                Some((last, _)) => end = Some(end.map_or(*last, |end| end.min(*last))),
                None => page.cut = true,
            }
        }
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
    if let Some((nth, _)) = matches.iter().nth(limit - 1)
        && matches.len() > limit
    {
        end = Some(end.map_or(*nth, |end| end.min(*nth)));
    }
    page.next = end;
    let documents: BTreeMap<_, _> = match end {
        Some(end) => matches.into_iter().filter(|(id, _)| *id <= end).collect(),
        None => matches,
    };
    describe(context, realm_id, auth, documents, &mut page).await?;
    let (pending, failed) = unsettled_documents(storage, UNSETTLED_CAP)
        .await
        .map_err(internal)?;
    page.pending_materializations = pending;
    page.failed_materializations = failed;
    page.truncated_documents = truncated_documents(storage, UNSETTLED_CAP)
        .await
        .map_err(internal)?;
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
    describe(context, realm_id, auth, matches, &mut preview.page).await?;
    preview.unverified = unverified_versions(storage, UNSETTLED_CAP)
        .await
        .map_err(internal)?;
    let (pending, failed) = unsettled_documents(storage, UNSETTLED_CAP)
        .await
        .map_err(internal)?;
    preview.page.pending_materializations = pending;
    preview.page.failed_materializations = failed;
    preview.page.truncated_documents = truncated_documents(storage, UNSETTLED_CAP)
        .await
        .map_err(internal)?;
    Ok(preview)
}

/// A set's deletion preview over every metadata node: each node's first page of the postings
/// that name the set, merged by document id.
#[derive(Clone, Debug, Default)]
pub struct SetPreview {
    pub documents: Vec<LinkDocument>,
    /// A node holds more postings of the set than its first page.
    pub more: bool,
    pub hidden: bool,
    pub nodes_queried: usize,
    pub nodes_failed: usize,
    pub unverified: usize,
    pub pending_materializations: usize,
    pub failed_materializations: usize,
    pub truncated_documents: usize,
}

impl SetPreview {
    /// Every node answered its whole set with nothing unsettled, unverified or truncated.
    pub fn complete(&self) -> bool {
        !self.more
            && self.nodes_failed == 0
            && self.unverified == 0
            && self.pending_materializations == 0
            && self.failed_materializations == 0
            && self.truncated_documents == 0
    }
}

/// Asks every metadata node for its first page of the documents that link `set_id`.
pub async fn set_preview_distributed(
    context: &DriverContext,
    (realm_id, local_node_id): (RealmId, NodeId),
    auth: Option<AuthContext>,
    bearer_token: Option<String>,
    (set_id, limit): (Ulid, usize),
) -> Result<SetPreview, MetadataApiError> {
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let limit = limit.clamp(1, LINKS_MAX_LIMIT);
    let local_call: MetadataNodeCall<SetPreviewPage> = metadata_node_call(
        (context.clone(), auth, realm_id),
        move |(context, auth, realm_id), _| async move {
            set_preview_local(&context, realm_id, auth.as_ref(), set_id, None, limit)
                .await
                .map_err(crate::forward::transport::read_error)
        },
    );
    let remote_call: MetadataNodeCall<SetPreviewPage> = metadata_node_call(
        (handle, fanout_bearer(bearer_token.as_deref())),
        move |(handle, auth_token), node_id| async move {
            (handle.request_set_preview(node_id, auth_token, (set_id, limit))).await
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
        record_preview_result,
        map_read_error,
    )
    .await?;
    let nodes = (
        stats.nodes_queried,
        stats.nodes_failed + usize::from(stats.discovery_failed),
    );
    Ok(merge_previews(
        parts.into_iter().map(|(_, page)| page),
        nodes,
        limit,
    ))
}

/// Merges node previews by document id, keeping the first `limit` documents.
pub fn merge_previews(
    pages: impl IntoIterator<Item = SetPreviewPage>,
    (nodes_queried, nodes_failed): (usize, usize),
    limit: usize,
) -> SetPreview {
    let mut preview = SetPreview {
        nodes_queried,
        nodes_failed,
        ..SetPreview::default()
    };
    let mut merged = BTreeMap::<Ulid, LinkDocument>::new();
    for node in pages {
        preview.more |= node.next.is_some();
        preview.hidden |= node.page.hidden;
        preview.unverified += node.unverified;
        preview.pending_materializations += node.page.pending_materializations;
        preview.failed_materializations += node.page.failed_materializations;
        preview.truncated_documents += node.page.truncated_documents;
        for document in node.page.documents {
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
    preview.more |= merged.len() > limit;
    preview.documents = merged.into_values().take(limit).collect();
    preview
}

fn record_preview_result(span: &Span, result: &Result<SetPreviewPage, MetadataReadError>) {
    let outcome = match result {
        Ok(_) => "ok",
        Err(_) => "error",
    };
    span.record("result", outcome);
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
    // Every node examined everything up to its own end; together, up to the smallest one.
    let mut end: Option<Ulid> = None;
    let mut cut = false;
    for node in pages {
        if let Some(next) = node.next {
            end = Some(end.map_or(next, |end| end.min(next)));
        }
        cut |= node.cut;
        page.hidden |= node.hidden;
        page.pending_materializations += node.pending_materializations;
        page.failed_materializations += node.failed_materializations;
        page.truncated_documents += node.truncated_documents;
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
    if let Some(end) = end {
        merged.retain(|id, _| *id <= end);
    }
    if merged.len() > limit {
        end = merged.keys().nth(limit.saturating_sub(1)).copied();
    }
    page.documents = merged.into_values().take(limit).collect();
    // Hidden documents count as examined, so a page of only hidden ones still moves on.
    page.next_after = end;
    page.complete = end.is_none()
        && !cut
        && nodes_failed == 0
        && page.pending_materializations == 0
        && page.failed_materializations == 0
        && page.truncated_documents == 0;
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

/// Where a linked part's files are browsed: the bucket name, its prefix, and whether this
/// node holds the bucket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartTarget {
    pub link: DataLink,
    pub bucket: Option<String>,
    pub prefix: String,
    /// The node of an ordinary bucket; a distributed bucket is browsed on any member.
    pub node_id: Option<NodeId>,
    pub held_here: bool,
}

/// Resolves a bucket or prefix identifier to the bucket that holds its files. A set this node
/// has no record of yet resolves without a bucket name.
pub async fn resolve_part(
    context: &DriverContext,
    node_id: NodeId,
    link: DataLink,
) -> Result<PartTarget, MetadataApiError> {
    let Some((scope, prefix)) = link.scope() else {
        return Err(MetadataApiError::BadRequest);
    };
    let prefix = prefix.to_string();
    match scope {
        LinkScope::Node {
            node_id: owner,
            bucket,
            ..
        } => {
            let held_here = owner == node_id
                && crate::driver::drive(GetBucketOperation::new(bucket.clone()), context)
                    .await
                    .is_ok_and(|info| info.distribution.is_none());
            Ok(PartTarget {
                link,
                bucket: Some(bucket),
                prefix,
                node_id: Some(owner),
                held_here,
            })
        }
        LinkScope::Set { set_id, .. } => {
            let record = read_row(
                context,
                aruna_core::keyspaces::DISTRIBUTION_KEYSPACE,
                set_id.to_bytes().to_vec(),
            )
            .await?
            .and_then(|value| {
                aruna_core::structs::storage::distribution::DistributionView::from_bytes(&value)
                    .ok()
            });
            let bucket = record.map(|view| view.snapshot().config.bucket.clone());
            let held_here = match &bucket {
                Some(name) => crate::driver::drive(GetBucketOperation::new(name.clone()), context)
                    .await
                    .is_ok_and(|info| {
                        info.distribution
                            .is_some_and(|binding| binding.set_id == set_id)
                    }),
                None => false,
            };
            Ok(PartTarget {
                link,
                bucket,
                prefix,
                node_id: None,
                held_here,
            })
        }
    }
}

/// At most this many datasets a file names as derived `isPartOf`; the paged listing has all.
pub const PART_OF_LIMIT: usize = 10;

/// The datasets whose live linked parts contain a file: its derived `isPartOf`, computed now
/// and never stored per file.
#[derive(Clone, Debug, Default)]
pub struct PartOf {
    pub datasets: Vec<LinkDocument>,
    /// More datasets exist than this answer names.
    pub more: bool,
    pub complete: bool,
    pub hidden: bool,
}

/// The derived `isPartOf` of a file of a local bucket, as `auth` may see it.
#[allow(clippy::too_many_arguments)]
pub async fn part_of(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    auth: Option<AuthContext>,
    bearer_token: Option<String>,
    bucket: &str,
    key: &str,
) -> Result<PartOf, MetadataApiError> {
    let info = crate::driver::drive(GetBucketOperation::new(bucket.to_string()), context)
        .await
        .map_err(|error| match error {
            GetBucketError::NotFound => MetadataApiError::NotFound,
            other => MetadataApiError::Internal(other.to_string()),
        })?;
    let page = links_distributed(
        context,
        realm_id,
        node_id,
        auth,
        bearer_token,
        LinkNodeRequest {
            queries: vec![LinkQuery::KeyPrefixes {
                scope: bucket_scope(realm_id, node_id, bucket, &info),
                key: key.to_string(),
            }],
            parts_only: true,
            after: None,
            limit: PART_OF_LIMIT,
        },
    )
    .await?;
    Ok(PartOf {
        more: page.next_after.is_some(),
        complete: page.complete,
        hidden: page.hidden,
        datasets: page.documents,
    })
}

/// A distributed bucket is named by its set, an ordinary one by this node and its name.
fn bucket_scope(
    realm_id: RealmId,
    node_id: NodeId,
    bucket: &str,
    info: &aruna_core::structs::storage::blob::BucketInfo,
) -> LinkScope {
    match &info.distribution {
        Some(binding) => LinkScope::Set {
            realm_id,
            set_id: binding.set_id,
        },
        None => LinkScope::Node {
            realm_id,
            node_id,
            bucket: bucket.to_string(),
        },
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
    let scope = bucket_scope(realm_id, node_id, bucket, &info);
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
