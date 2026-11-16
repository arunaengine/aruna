//! Finds the datasets that link Aruna data: one identifier, or a file through its version, its
//! content and the folders that contain its key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Every metadata node answers from its own postings within one budget; results merge by
// document id and say whether they are complete.

use super::links_types::{
    LINKS_MAX_LIMIT, LinkDocument, LinkHit, LinkNodePage, LinkNodeRequest, LinkPage, LinkQuery,
    PART_OF_LIMIT, PartOf, PartTarget, SetPreview, SetPreviewPage, merge_pages, merge_previews,
};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::structs::storage::blob::{
    BlobHeadKey, CurrentVersionPointer, VersionKey, bucket_permission_path, object_permission_path,
};
use aruna_core::structs::storage::data::link::{DataLink, LinkScope};

use aruna_core::structs::storage::data::link::LinkPosting;
use aruna_core::types::GroupId;

use super::{
    ApiQueryMode, AuthContext, BTreeMap, BlobVersion, DriverContext, Event, MetadataApiError,
    MetadataFanoutOperation, MetadataFanoutScope, MetadataNodeCall, MetadataReadError, NodeId,
    RealmId, Span, StorageEffect, StorageEvent, Ulid, can_read_record, fanout_bearer,
    load_live_record, map_read_error, metadata_node_call, reference_document_title,
    run_metadata_fanout,
};
use crate::metadata::data_links::{
    LinkBudget, key_prefixes, read_postings, set_postings, truncated_documents,
    unsettled_documents, unverified_versions,
};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

const NODE_SEEKS: usize = 1100;
const NODE_ROWS: usize = 4096;
const UNSETTLED_CAP: usize = 1024;

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
    let confirm = request.confirm;
    describe(context, (realm_id, auth, confirm), documents, &mut page).await?;
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
/// are dropped, unreadable documents only set `hidden`, and `confirmed` for `confirm`.
async fn describe(
    context: &DriverContext,
    (realm_id, auth, confirm): (RealmId, Option<&AuthContext>, Option<Ulid>),
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
            page.confirmed |= confirm == Some(document_id);
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
    describe(context, (realm_id, auth, None), matches, &mut preview.page).await?;
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
            realm_id,
            node_id: owner,
            bucket,
        } => {
            let local = match owner == node_id {
                true => crate::driver::drive(GetBucketOperation::new(bucket.clone()), context)
                    .await
                    .ok()
                    .filter(|info| info.distribution.is_none()),
                false => None,
            };
            let read_paths = local
                .iter()
                .map(|info| read_path(realm_id, info.group_id, owner, &bucket, &prefix))
                .collect();
            Ok(PartTarget {
                link,
                held_here: local.is_some(),
                bucket: Some(bucket),
                prefix,
                node_id: Some(owner),
                read_paths,
            })
        }
        LinkScope::Set { realm_id, set_id } => {
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
            let config = record.map(|view| view.snapshot().config.clone());
            let read_paths = config.iter().flat_map(|config| {
                config.members.iter().map(|member| {
                    let group = config.owner_group;
                    read_path(realm_id, group, member.node_id, &config.bucket, &prefix)
                })
            });
            let read_paths = read_paths.collect();
            let bucket = config.map(|config| config.bucket);
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
                read_paths,
            })
        }
    }
}

/// The path whose READ lists `prefix` of a bucket, or the whole bucket.
fn read_path(realm: RealmId, group: GroupId, node: NodeId, bucket: &str, prefix: &str) -> String {
    match prefix.is_empty() {
        true => bucket_permission_path(realm, group, node, bucket),
        false => object_permission_path(realm, group, node, bucket, prefix),
    }
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
            confirm: None,
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
