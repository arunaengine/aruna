//! Finds the datasets that link Aruna data: one identifier, or a file through its version, its
//! content and the folders that contain its key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Every metadata node answers from its own postings within one budget; results merge by
// document id and say whether they are complete.

pub mod local;
pub mod types;

use self::local::{LinksLocalOperation, SetPreviewOperation};

use self::types::{
    LINKS_MAX_LIMIT, LinkNodePage, LinkNodeRequest, LinkPage, LinkQuery, PART_OF_LIMIT, PartOf,
    PartTarget, SetPreview, SetPreviewPage, merge_pages, merge_previews,
};
use aruna_core::structs::storage::blob::{bucket_permission_path, object_permission_path};
use aruna_core::structs::storage::data::link::{DataLink, LinkScope};
use aruna_core::types::GroupId;

use super::fanout::run_metadata_fanout;
use super::{
    ApiQueryMode, AuthContext, Event, MetadataApiError, MetadataFanoutOperation,
    MetadataFanoutScope, MetadataNodeCall, MetadataReadError, NodeId, RealmId, Span, StorageEffect,
    StorageEvent, Ulid, fanout_bearer, map_read_error, metadata_node_call,
};
use crate::driver::DriverContext;
use crate::metadata::object_version::{ObjectVersion, ObjectVersionError, ObjectVersionOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

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
            let preview = SetPreviewOperation::new((realm_id, auth), set_id, None, limit);
            crate::driver::drive(preview, &context)
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
            let links = LinksLocalOperation::new(realm_id, auth, request);
            crate::driver::drive(links, &context)
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
    let ObjectVersion {
        version_id,
        version,
    } = crate::driver::drive(
        ObjectVersionOperation::new(bucket, key, version_id),
        context,
    )
    .await
    .map_err(|error| match error {
        ObjectVersionError::Key(_) => MetadataApiError::BadRequest,
        other => MetadataApiError::Internal(other.to_string()),
    })?;
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
        other => Err(MetadataApiError::Internal(other.label().to_string())),
    }
}

#[cfg(test)]
mod tests;
