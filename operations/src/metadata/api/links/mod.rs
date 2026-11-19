//! Finds the datasets that link Aruna data: one identifier, or a file through its version, its
//! content and the folders that contain its key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Every metadata node answers from its own postings within one budget; results merge by
// document id and say whether they are complete.

pub mod local;
pub mod types;

use std::time::Duration;

use self::local::{LinksLocalOperation, SetPreviewOperation};

use self::types::{
    LINKS_MAX_LIMIT, LinkNodePage, LinkNodeRequest, LinkPage, LinkQuery, PART_OF_LIMIT, PartOf,
    PartTarget, SetPreview, SetPreviewPage, merge_pages, merge_previews,
};
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::keyspaces::DISTRIBUTION_KEYSPACE;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::storage::blob::{
    BucketInfo, bucket_permission_path, object_permission_path,
};
use aruna_core::structs::storage::data::link::{DataLink, LinkScope};
use aruna_core::structs::storage::distribution::DistributionView;
use aruna_core::types::GroupId;

use super::node_fanout::{FanoutParts, NodeCalls, NodeFanoutOperation, peer_answer, peer_request};
use super::{
    ApiQueryMode, AuthContext, Event, MetadataApiError, MetadataFanoutOperation,
    MetadataFanoutScope, MetadataReadError, NodeId, RealmId, Span, Ulid, fanout_bearer,
};
use crate::forward::transport::read_error;
use crate::metadata::object_version::{ObjectVersion, ObjectVersionError, ObjectVersionOperation};
use crate::metadata::protocol::MetadataTransportMessage;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

/// The output of a local link child; its API error reads as the node's read error.
fn local_part<T: 'static>(event: Event) -> Result<T, MetadataReadError> {
    let Event::Child(outcome) = event else {
        return Err(MetadataReadError::Unavailable);
    };
    let part = outcome.take::<T, MetadataApiError>();
    part.ok_or(MetadataReadError::Unavailable)?
        .map_err(read_error)
}

fn child_effect<O>(operation: O) -> Effect
where
    O: Operation + Send + 'static,
    O::Output: std::fmt::Debug + Send + PartialEq + 'static,
    O::Error: std::fmt::Debug + Send + PartialEq + 'static,
{
    Effect::SubOperation(boxed_suboperation(operation, |result| {
        Event::Child(ChildOutcome::new(result))
    }))
}

/// How the set preview asks each node for its first page of the set's postings.
#[derive(Debug, PartialEq)]
struct PreviewCalls {
    realm_id: RealmId,
    auth: Option<AuthContext>,
    auth_token: Option<AuthToken>,
    set_id: Ulid,
    limit: usize,
}

impl NodeCalls for PreviewCalls {
    type Part = SetPreviewPage;

    fn local(&self, _node: NodeId) -> Effect {
        let caller = (self.realm_id, self.auth.clone());
        child_effect(SetPreviewOperation::new(
            caller,
            self.set_id,
            None,
            self.limit,
        ))
    }

    fn remote(&self, node: NodeId, timeout: Duration) -> Effect {
        let message = MetadataTransportMessage::SetPreview {
            auth_token: self.auth_token.clone(),
            set_id: self.set_id,
            limit: self.limit,
        };
        peer_request(node, &message, timeout)
    }

    fn read(
        &self,
        node: NodeId,
        local: bool,
        event: Event,
    ) -> Result<SetPreviewPage, MetadataReadError> {
        if local {
            return local_part(event);
        }
        match peer_answer(node, event)? {
            MetadataTransportMessage::SetPreviewResults { result } => result.map(|page| *page),
            _ => Err(MetadataReadError::Unavailable),
        }
    }

    fn record(&self, span: &Span, result: &Result<SetPreviewPage, MetadataReadError>) {
        let outcome = match result {
            Ok(_) => "ok",
            Err(_) => "error",
        };
        span.record("result", outcome);
    }
}

/// Every metadata node's first page of the documents that link `set_id`, merged.
#[derive(Debug, PartialEq)]
pub struct SetPreviewFanout {
    realm_id: RealmId,
    local_node: NodeId,
    calls: Option<PreviewCalls>,
}

/// Asks every metadata node for its first page of the documents that link `set_id`.
pub fn set_preview_distributed(
    (realm_id, local_node): (RealmId, NodeId),
    auth: Option<AuthContext>,
    bearer_token: Option<String>,
    (set_id, limit): (Ulid, usize),
) -> SetPreviewFanout {
    let calls = PreviewCalls {
        realm_id,
        auth,
        auth_token: fanout_bearer(bearer_token.as_deref()),
        set_id,
        limit: limit.clamp(1, LINKS_MAX_LIMIT),
    };
    SetPreviewFanout {
        realm_id,
        local_node,
        calls: Some(calls),
    }
}

fn distributed_scope() -> MetadataFanoutScope {
    MetadataFanoutScope::new(Some(ApiQueryMode::Distributed), None, true)
}

impl Operation for SetPreviewFanout {
    type Output = SetPreview;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<SetPreview>, MetadataApiError> {
        let calls = (self.calls.take())
            .ok_or_else(|| MetadataApiError::Internal("preview already started".to_string()))?;
        let fanout = NodeFanoutOperation::new(
            (self.realm_id, self.local_node),
            distributed_scope(),
            MetadataFanoutOperation::IdentifierLookup,
            calls,
        );
        Ok(Next::Call(IoCall::child(fanout)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SetPreview>, MetadataApiError> {
        let (parts, stats) = result.child::<FanoutParts<SetPreviewPage>, MetadataApiError>()??;
        let nodes = (
            stats.nodes_queried,
            stats.nodes_failed + usize::from(stats.discovery_failed),
        );
        let limit = self
            .calls
            .as_ref()
            .map_or(LINKS_MAX_LIMIT, |calls| calls.limit);
        let pages = parts.into_iter().map(|(_, page)| page);
        Ok(Next::Done(merge_previews(pages, nodes, limit)))
    }
}

/// How the links fanout asks each node's reverse index.
#[derive(Debug, PartialEq)]
struct LinkCalls {
    realm_id: RealmId,
    auth: Option<AuthContext>,
    auth_token: Option<AuthToken>,
    request: LinkNodeRequest,
}

impl NodeCalls for LinkCalls {
    type Part = LinkNodePage;

    fn local(&self, _node: NodeId) -> Effect {
        let request = self.request.clone();
        child_effect(LinksLocalOperation::new(
            self.realm_id,
            self.auth.clone(),
            request,
        ))
    }

    fn remote(&self, node: NodeId, timeout: Duration) -> Effect {
        let message = MetadataTransportMessage::DataLinks {
            auth_token: self.auth_token.clone(),
            request: Box::new(self.request.clone()),
        };
        peer_request(node, &message, timeout)
    }

    fn read(
        &self,
        node: NodeId,
        local: bool,
        event: Event,
    ) -> Result<LinkNodePage, MetadataReadError> {
        if local {
            return local_part(event);
        }
        match peer_answer(node, event)? {
            MetadataTransportMessage::DataLinkResults { result } => result.map(|page| *page),
            _ => Err(MetadataReadError::Unavailable),
        }
    }

    fn record(&self, span: &Span, result: &Result<LinkNodePage, MetadataReadError>) {
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
}

/// Every metadata node's page of the documents that link the request's identifiers, merged
/// by document id.
#[derive(Debug, PartialEq)]
pub struct LinksFanout {
    realm_id: RealmId,
    local_node: NodeId,
    limit: usize,
    calls: Option<LinkCalls>,
}

/// Asks every metadata node and merges their pages by document id.
pub fn links_distributed(
    (realm_id, local_node): (RealmId, NodeId),
    auth: Option<AuthContext>,
    bearer_token: Option<String>,
    request: LinkNodeRequest,
) -> LinksFanout {
    let limit = request.limit.clamp(1, LINKS_MAX_LIMIT);
    let calls = LinkCalls {
        realm_id,
        auth,
        auth_token: fanout_bearer(bearer_token.as_deref()),
        request,
    };
    LinksFanout {
        realm_id,
        local_node,
        limit,
        calls: Some(calls),
    }
}

impl Operation for LinksFanout {
    type Output = LinkPage;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<LinkPage>, MetadataApiError> {
        let calls = (self.calls.take())
            .ok_or_else(|| MetadataApiError::Internal("links already started".to_string()))?;
        let fanout = NodeFanoutOperation::new(
            (self.realm_id, self.local_node),
            distributed_scope(),
            MetadataFanoutOperation::IdentifierLookup,
            calls,
        );
        Ok(Next::Call(IoCall::child(fanout)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<LinkPage>, MetadataApiError> {
        let (parts, stats) = result.child::<FanoutParts<LinkNodePage>, MetadataApiError>()??;
        let failed = stats.nodes_failed + usize::from(stats.discovery_failed);
        let pages = parts.into_iter().map(|(_, page)| page);
        Ok(Next::Done(merge_pages(pages, self.limit, failed)))
    }
}

fn bucket_error(error: GetBucketError) -> MetadataApiError {
    match error {
        GetBucketError::NotFound => MetadataApiError::NotFound,
        other => MetadataApiError::Internal(other.to_string()),
    }
}

#[derive(Debug, PartialEq)]
enum ResolveStage {
    Start,
    /// The bucket of an ordinary link this node owns.
    Owned {
        realm_id: RealmId,
        owner: NodeId,
        bucket: String,
    },
    /// The set record of a distributed link.
    Set {
        realm_id: RealmId,
        set_id: Ulid,
    },
    /// Whether this node holds the set's bucket.
    Held(Box<PartTarget>, Ulid),
}

/// Resolves a bucket or prefix identifier to the bucket that holds its files. A set this node
/// has no record of yet resolves without a bucket name.
#[derive(Debug, PartialEq)]
pub struct ResolvePartOperation {
    node_id: NodeId,
    link: DataLink,
    prefix: String,
    stage: ResolveStage,
}

/// The bucket that holds the files `link` names, as seen from `node_id`.
pub fn resolve_part(node_id: NodeId, link: DataLink) -> ResolvePartOperation {
    ResolvePartOperation {
        node_id,
        link,
        prefix: String::new(),
        stage: ResolveStage::Start,
    }
}

impl ResolvePartOperation {
    fn owned(
        &self,
        (realm_id, owner): (RealmId, NodeId),
        bucket: String,
        local: Option<BucketInfo>,
    ) -> PartTarget {
        let read_paths = (local.iter())
            .map(|info| read_path(realm_id, info.group_id, owner, &bucket, &self.prefix))
            .collect();
        PartTarget {
            link: self.link.clone(),
            held_here: local.is_some(),
            bucket: Some(bucket),
            prefix: self.prefix.clone(),
            node_id: Some(owner),
            read_paths,
        }
    }

    fn set(
        &mut self,
        (realm_id, set_id): (RealmId, Ulid),
        value: Option<Vec<u8>>,
    ) -> Result<Next<PartTarget>, MetadataApiError> {
        let record = value.and_then(|value| DistributionView::from_bytes(&value).ok());
        let config = record.map(|view| view.snapshot().config.clone());
        let read_paths = config.iter().flat_map(|config| {
            config.members.iter().map(|member| {
                let group = config.owner_group;
                read_path(
                    realm_id,
                    group,
                    member.node_id,
                    &config.bucket,
                    &self.prefix,
                )
            })
        });
        let target = PartTarget {
            link: self.link.clone(),
            read_paths: read_paths.collect(),
            bucket: config.map(|config| config.bucket),
            prefix: self.prefix.clone(),
            node_id: None,
            held_here: false,
        };
        let Some(bucket) = target.bucket.clone() else {
            return Ok(Next::Done(target));
        };
        self.stage = ResolveStage::Held(Box::new(target), set_id);
        Ok(Next::Call(IoCall::child(GetBucketOperation::new(bucket))))
    }
}

impl Operation for ResolvePartOperation {
    type Output = PartTarget;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<PartTarget>, MetadataApiError> {
        let Some((scope, prefix)) = self.link.scope() else {
            return Err(MetadataApiError::BadRequest);
        };
        self.prefix = prefix.to_string();
        match scope {
            LinkScope::Node {
                realm_id,
                node_id: owner,
                bucket,
            } => {
                if owner != self.node_id {
                    return Ok(Next::Done(self.owned((realm_id, owner), bucket, None)));
                }
                let read = GetBucketOperation::new(bucket.clone());
                self.stage = ResolveStage::Owned {
                    realm_id,
                    owner,
                    bucket,
                };
                Ok(Next::Call(IoCall::child(read)))
            }
            LinkScope::Set { realm_id, set_id } => {
                let row = (
                    DISTRIBUTION_KEYSPACE.to_string(),
                    set_id.to_bytes().to_vec().into(),
                );
                self.stage = ResolveStage::Set { realm_id, set_id };
                Ok(Next::Call(IoCall::Read(vec![row])))
            }
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PartTarget>, MetadataApiError> {
        match std::mem::replace(&mut self.stage, ResolveStage::Start) {
            ResolveStage::Start => Err(MetadataApiError::Internal(
                "resolve not started".to_string(),
            )),
            ResolveStage::Owned {
                realm_id,
                owner,
                bucket,
            } => {
                let info = result.child::<BucketInfo, GetBucketError>()?.ok();
                let local = info.filter(|info| info.distribution.is_none());
                Ok(Next::Done(self.owned((realm_id, owner), bucket, local)))
            }
            ResolveStage::Set { realm_id, set_id } => {
                let value = (result.value())
                    .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
                self.set((realm_id, set_id), value.map(|value| value.to_vec()))
            }
            ResolveStage::Held(mut target, set_id) => {
                let info = result.child::<BucketInfo, GetBucketError>()?;
                target.held_here = info.is_ok_and(|info| {
                    (info.distribution).is_some_and(|binding| binding.set_id == set_id)
                });
                Ok(Next::Done(*target))
            }
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
#[derive(Debug, PartialEq)]
pub struct PartOfOperation {
    realm_id: RealmId,
    node_id: NodeId,
    caller: Option<(Option<AuthContext>, Option<String>)>,
    bucket: String,
    key: String,
}

/// The datasets whose live linked parts contain `key` of the local `bucket`.
pub fn part_of(
    (realm_id, node_id): (RealmId, NodeId),
    (auth, bearer_token): (Option<AuthContext>, Option<String>),
    bucket: &str,
    key: &str,
) -> PartOfOperation {
    PartOfOperation {
        realm_id,
        node_id,
        caller: Some((auth, bearer_token)),
        bucket: bucket.to_string(),
        key: key.to_string(),
    }
}

impl Operation for PartOfOperation {
    type Output = PartOf;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<PartOf>, MetadataApiError> {
        Ok(Next::Call(IoCall::child(GetBucketOperation::new(
            self.bucket.clone(),
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PartOf>, MetadataApiError> {
        let Some((auth, bearer_token)) = self.caller.take() else {
            let page = result.child::<LinkPage, MetadataApiError>()??;
            return Ok(Next::Done(PartOf {
                more: page.next_after.is_some(),
                complete: page.complete,
                hidden: page.hidden,
                datasets: page.documents,
            }));
        };
        let info = result
            .child::<BucketInfo, GetBucketError>()?
            .map_err(bucket_error)?;
        let request = LinkNodeRequest {
            queries: vec![LinkQuery::KeyPrefixes {
                scope: bucket_scope(self.realm_id, self.node_id, &self.bucket, &info),
                key: self.key.clone(),
            }],
            parts_only: true,
            after: None,
            limit: PART_OF_LIMIT,
            confirm: None,
        };
        let node = (self.realm_id, self.node_id);
        let links = links_distributed(node, auth, bearer_token, request);
        Ok(Next::Call(IoCall::child(links)))
    }
}

/// A distributed bucket is named by its set, an ordinary one by this node and its name.
fn bucket_scope(realm_id: RealmId, node_id: NodeId, bucket: &str, info: &BucketInfo) -> LinkScope {
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
#[derive(Debug, PartialEq)]
pub struct FileQueriesOperation {
    realm_id: RealmId,
    node_id: NodeId,
    bucket: String,
    key: String,
    version_id: Option<Ulid>,
    info: Option<BucketInfo>,
}

/// The link queries of `key` in the local `bucket`, at `version_id` or its current version.
pub fn file_queries(
    (realm_id, node_id): (RealmId, NodeId),
    bucket: &str,
    key: &str,
    version_id: Option<Ulid>,
) -> FileQueriesOperation {
    FileQueriesOperation {
        realm_id,
        node_id,
        bucket: bucket.to_string(),
        key: key.to_string(),
        version_id,
        info: None,
    }
}

impl Operation for FileQueriesOperation {
    type Output = Vec<LinkQuery>;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<Vec<LinkQuery>>, MetadataApiError> {
        Ok(Next::Call(IoCall::child(GetBucketOperation::new(
            self.bucket.clone(),
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<LinkQuery>>, MetadataApiError> {
        let Some(info) = self.info.take() else {
            let info = result
                .child::<BucketInfo, GetBucketError>()?
                .map_err(bucket_error)?;
            self.info = Some(info);
            let version = ObjectVersionOperation::new(&self.bucket, &self.key, self.version_id);
            return Ok(Next::Call(IoCall::child(version)));
        };
        let ObjectVersion {
            version_id,
            version,
        } = (result.child::<ObjectVersion, ObjectVersionError>()?).map_err(
            |error| match error {
                ObjectVersionError::Key(_) => MetadataApiError::BadRequest,
                other => MetadataApiError::Internal(other.to_string()),
            },
        )?;
        let scope = bucket_scope(self.realm_id, self.node_id, &self.bucket, &info);
        let mut queries = vec![LinkQuery::KeyPrefixes {
            scope,
            key: self.key.clone(),
        }];
        if let (Some(_), Some(version_id)) = (&info.distribution, version_id) {
            queries.push(LinkQuery::Exact(DataLink::Version {
                realm_id: self.realm_id,
                version_id,
            }));
        }
        let hash = version
            .as_ref()
            .and_then(|version| version.blob_hash().copied());
        if let Some(hash) = hash {
            queries.push(LinkQuery::Exact(DataLink::Hash(hash)));
        }
        Ok(Next::Done(queries))
    }
}

#[cfg(test)]
mod call_tests;
#[cfg(test)]
mod tests;
