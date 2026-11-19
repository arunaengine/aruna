//! Searches the metadata graphs of the realm's nodes and pages the merged hits with a signed
//! cursor.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::cursor::SEARCH_CURSOR_CONTEXT;
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::entity_patch::DCTERMS_CONFORMS_IRI;
use aruna_core::events::{Event, NetEvent};
use aruna_core::metadata::{AuthToken, MetadataEffect, MetadataEvent, MetadataSearchHit};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::rows::event;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::GroupId;
use tracing::Span;

use super::distributed::record_search_node;
use super::fanout_types::{
    MetadataFanoutOperation, MetadataFanoutScope, MetadataFanoutStats, RealmNodeDiscovery,
    fanout_bearer,
};
use super::node_fanout::{
    FanoutParts, NodeCalls, NodeFanoutOperation, discovery_answer, discovery_call,
};
use super::path_types::{deduplicate_fanout_nodes, select_fanout_nodes};
use super::search_types::{MetadataSearchExecution, MetadataSearchRequest};
use super::{
    ApiQueryMode, DISTRIBUTED_QUERY_DEADLINE, MetadataApiError, MetadataReadError, map_read_error,
};
use crate::forward::fetch::{peer_call, peer_reply};
use crate::metadata::profile::validation::equivalent_profile_iris;
use crate::metadata::protocol::{MetadataTransportMessage, encode_message, metadata_read_error};
use crate::metadata::search_cursor::{
    MAX_PAGINATION_DEPTH, NodeSearchResult, SEARCH_MAX_PAGE, SEARCH_PAGE_SIZE, SearchCursor,
    SearchCursorError, SearchWatermark, blank_signature, merge_search_hits, paginate,
    query_fingerprint, resume_fetch_limit,
};

/// Where one node's search runs.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum SearchTarget {
    /// This node's graphs, under the caller's own authority.
    Local(Option<AuthContext>),
    /// A peer, under the forwarded credential.
    Remote {
        node: NodeId,
        auth_token: Option<AuthToken>,
    },
}

/// What one node searches for.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct SearchAsk {
    pub(super) graph_iris: Option<Vec<String>>,
    pub(super) query: String,
    pub(super) group_id: Option<GroupId>,
    /// The profile IRIs one `conformsTo` filter stands for; empty without a filter.
    pub(super) profiles: Vec<String>,
}

/// One node's best hits, at most `limit`, with the limit it asked for. A filtered search asks
/// once per equivalent profile IRI and keeps the best hits of all of them.
#[derive(Debug, PartialEq)]
pub(super) struct NodeSearchOperation {
    target: SearchTarget,
    ask: SearchAsk,
    limit: usize,
    deadline_ms: u64,
    now_ms: u64,
    asked: usize,
    hits: Vec<MetadataSearchHit>,
}

impl NodeSearchOperation {
    pub(super) fn new(
        target: SearchTarget,
        ask: SearchAsk,
        limit: usize,
        deadline_ms: u64,
    ) -> Self {
        Self {
            target,
            ask,
            limit,
            deadline_ms,
            now_ms: 0,
            asked: 0,
            hits: Vec::new(),
        }
    }

    fn call(&self) -> Result<IoCall, MetadataReadError> {
        let profile = self.ask.profiles.get(self.asked).cloned();
        let (graph_iris, query) = (self.ask.graph_iris.clone(), self.ask.query.clone());
        let (limit, group_id) = (self.limit, self.ask.group_id);
        let (node, auth_token) = match &self.target {
            SearchTarget::Local(auth) => {
                let effect = MetadataEffect::SearchGraphs {
                    auth_context: auth.clone(),
                    graph_iris,
                    query,
                    limit,
                    group_id,
                    conforms_to: profile,
                };
                return Ok(IoCall::Effect(Box::new(Effect::Metadata(effect))));
            }
            SearchTarget::Remote { node, auth_token } => (*node, auth_token.clone()),
        };
        let message = match profile {
            Some(object_iri) => MetadataTransportMessage::FilteredSearchGraphs {
                auth_token,
                graph_iris,
                query,
                limit,
                predicate_iri: DCTERMS_CONFORMS_IRI.to_string(),
                object_iri,
                group_id,
            },
            None => MetadataTransportMessage::SearchGraphs {
                auth_token,
                graph_iris,
                query,
                limit,
                group_id,
            },
        };
        let frame = encode_message(&message).map_err(|_| MetadataReadError::Unavailable)?;
        let wait = Duration::from_millis(self.deadline_ms.saturating_sub(self.now_ms));
        Ok(peer_call(node, frame, wait))
    }

    fn answer(&self, result: IoResult) -> Result<Vec<MetadataSearchHit>, MetadataReadError> {
        match &self.target {
            SearchTarget::Local(_) => match event(result)? {
                Event::Metadata(MetadataEvent::SearchResult { hits }) => Ok(hits),
                Event::Metadata(MetadataEvent::Error { error, .. }) => {
                    Err(metadata_read_error(error))
                }
                _ => Err(MetadataReadError::Unavailable),
            },
            SearchTarget::Remote { node, .. } => match peer_reply(*node, result)? {
                Ok(MetadataTransportMessage::SearchResults { result }) => result,
                _ => Err(MetadataReadError::Unavailable),
            },
        }
    }
}

type NodeHits = (Vec<MetadataSearchHit>, usize);

impl Operation for NodeSearchOperation {
    type Output = NodeHits;
    type Error = MetadataReadError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<NodeHits>, MetadataReadError> {
        Ok(Next::Call(self.call()?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<NodeHits>, MetadataReadError> {
        let hits = self.answer(result)?;
        if self.ask.profiles.is_empty() {
            return Ok(Next::Done((hits, self.limit)));
        }
        self.hits.extend(hits);
        self.asked += 1;
        if self.asked < self.ask.profiles.len() {
            return Ok(Next::Call(self.call()?));
        }
        let hits = merge_search_hits(std::mem::take(&mut self.hits));
        Ok(Next::Done((
            hits.into_iter().take(self.limit).collect(),
            self.limit,
        )))
    }
}

/// How the search fanout asks each node: deeper for nodes a cursor resumes.
#[derive(Debug, PartialEq)]
struct SearchCalls {
    auth: Option<AuthContext>,
    auth_token: Option<AuthToken>,
    ask: SearchAsk,
    resume: HashMap<NodeId, u32>,
    page_size: usize,
    deadline_ms: u64,
}

impl SearchCalls {
    fn search(&self, node: NodeId, target: SearchTarget) -> Effect {
        let limit = resume_fetch_limit(&self.resume, node, self.page_size, MAX_PAGINATION_DEPTH);
        let search = NodeSearchOperation::new(target, self.ask.clone(), limit, self.deadline_ms);
        Effect::SubOperation(boxed_suboperation(search, |result| {
            Event::Child(ChildOutcome::new(result))
        }))
    }
}

impl NodeCalls for SearchCalls {
    type Part = NodeHits;

    fn local(&self, node: NodeId) -> Effect {
        self.search(node, SearchTarget::Local(self.auth.clone()))
    }

    // Each node's searches ask within what is left of the search deadline.
    fn remote(&self, node: NodeId, _timeout: Duration) -> Effect {
        let auth_token = self.auth_token.clone();
        self.search(node, SearchTarget::Remote { node, auth_token })
    }

    fn read(
        &self,
        _node: NodeId,
        _local: bool,
        event: Event,
    ) -> Result<NodeHits, MetadataReadError> {
        match event {
            Event::Child(outcome) => outcome
                .take::<NodeHits, MetadataReadError>()
                .ok_or(MetadataReadError::Unavailable)?,
            _ => Err(MetadataReadError::Unavailable),
        }
    }

    fn record(&self, span: &Span, result: &Result<NodeHits, MetadataReadError>) {
        record_search_node(span, result);
    }
}

/// A page waiting for its cursor signature.
#[derive(Debug, PartialEq)]
struct Unsigned {
    cursor: SearchCursor,
    hits: Vec<MetadataSearchHit>,
    truncated: bool,
    stats: MetadataFanoutStats,
}

#[derive(Debug, PartialEq)]
enum SearchStage {
    Start,
    Discovery,
    Fanout,
    Sign(Box<Unsigned>),
}

/// One page of a metadata search across the realm, or on this node in local mode. A cursor
/// resumes each node where the last page left it, so the search keeps the nodes it resumes.
#[derive(Debug, PartialEq)]
pub struct MetadataSearchOperation {
    realm_id: RealmId,
    local_node: NodeId,
    request: MetadataSearchRequest,
    fingerprint: [u8; 32],
    page_size: usize,
    watermark: Option<SearchWatermark>,
    resume: HashMap<NodeId, u32>,
    deadline_ms: Option<u64>,
    now_ms: u64,
    stage: SearchStage,
}

/// The search a caller asked for, across the realm or on this node as its mode says.
pub fn search_metadata(
    realm_id: RealmId,
    local_node: NodeId,
    request: MetadataSearchRequest,
) -> MetadataSearchOperation {
    MetadataSearchOperation {
        realm_id,
        local_node,
        request,
        fingerprint: [0; 32],
        page_size: SEARCH_PAGE_SIZE,
        watermark: None,
        resume: HashMap::new(),
        deadline_ms: None,
        now_ms: 0,
        stage: SearchStage::Start,
    }
}

impl MetadataSearchOperation {
    fn mode(&self) -> ApiQueryMode {
        self.request.mode.unwrap_or(ApiQueryMode::Distributed)
    }

    fn deadline(&self) -> u64 {
        self.deadline_ms.unwrap_or(self.now_ms)
    }

    fn remaining(&self) -> Duration {
        Duration::from_millis(self.deadline().saturating_sub(self.now_ms))
    }

    fn checked(&mut self) -> Result<(), MetadataApiError> {
        let request = &mut self.request;
        if request.query.trim().is_empty() && request.conforms_to.is_none() {
            return Err(MetadataApiError::BadRequest);
        }
        if (request.conforms_to.as_deref()).is_some_and(|iri| oxrdf::NamedNode::new(iri).is_err()) {
            return Err(MetadataApiError::BadRequest);
        }
        if let Some(iri) = request.conforms_to.take() {
            request.conforms_to = equivalent_profile_iris(&iri).into_iter().next();
        }
        let limit = request.limit.unwrap_or(SEARCH_PAGE_SIZE);
        self.page_size = limit.clamp(1, SEARCH_MAX_PAGE);
        self.fingerprint = query_fingerprint(
            &request.query,
            request.graph_iris.as_deref(),
            request.mode,
            request.conforms_to.as_deref(),
            request.group_id,
        );
        Ok(())
    }

    /// Reads the cursor that `signers` may have signed, then asks the nodes.
    fn resumed(
        &mut self,
        signers: Vec<NodeId>,
        discovery: Option<RealmNodeDiscovery>,
    ) -> Result<Next<MetadataSearchExecution>, MetadataApiError> {
        let raw = self.request.cursor.as_deref().unwrap_or_default();
        let asked = (
            self.request.target_nodes.as_deref(),
            self.mode(),
            self.local_node,
        );
        let resumed = resume_cursor((raw, &signers), self.fingerprint, asked, discovery)?;
        self.watermark = Some(resumed.watermark);
        self.resume = resumed.resume;
        self.fanout(Some(resumed.nodes), resumed.discovery_failed)
    }

    fn fanout(
        &mut self,
        targets: Option<Vec<NodeId>>,
        discovery_failed: bool,
    ) -> Result<Next<MetadataSearchExecution>, MetadataApiError> {
        let request = &self.request;
        let profiles =
            (request.conforms_to.as_deref()).map_or_else(Vec::new, equivalent_profile_iris);
        let calls = SearchCalls {
            auth: request.auth.clone(),
            auth_token: fanout_bearer(request.bearer_token.as_deref()),
            ask: SearchAsk {
                graph_iris: request.graph_iris.clone(),
                query: request.query.clone(),
                group_id: request.group_id,
                profiles,
            },
            resume: self.resume.clone(),
            page_size: self.page_size,
            deadline_ms: self.deadline(),
        };
        let scope = MetadataFanoutScope::new(request.mode, targets, true)
            .with_subject(self.fingerprint)
            .with_discovery_failed(discovery_failed);
        let fanout = NodeFanoutOperation::new(
            (self.realm_id, self.local_node),
            scope,
            MetadataFanoutOperation::Search,
            calls,
            map_read_error,
        )
        .until(self.deadline());
        self.stage = SearchStage::Fanout;
        Ok(Next::Call(IoCall::child(fanout)))
    }

    fn paged(
        &mut self,
        (parts, stats): FanoutParts<NodeHits>,
    ) -> Result<Next<MetadataSearchExecution>, MetadataApiError> {
        let results = (parts.into_iter())
            .map(|(node_id, (hits, requested))| NodeSearchResult {
                node_id,
                saturated: hits.len() >= requested,
                hits,
            })
            .collect();
        let watermark = self.watermark.take();
        let page = paginate(results, watermark, self.page_size, MAX_PAGINATION_DEPTH);
        let Some(next) = page.next else {
            return Ok(Next::Done(MetadataSearchExecution {
                hits: page.hits,
                next_cursor: None,
                truncated: page.truncated,
                fanout_stats: stats,
            }));
        };
        let signer = self.local_node;
        let cursor = SearchCursor::new_signed(
            self.fingerprint,
            next.watermark,
            next.resume,
            signer,
            blank_signature,
        )
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
        let bytes = (cursor.signing_bytes(SEARCH_CURSOR_CONTEXT))
            .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
        self.stage = SearchStage::Sign(Box::new(Unsigned {
            cursor,
            hits: page.hits,
            truncated: page.truncated,
            stats,
        }));
        let sign = Effect::Net(NetEffect::CursorSign(bytes));
        Ok(Next::Call(IoCall::Effect(Box::new(sign))))
    }
}

impl Operation for MetadataSearchOperation {
    type Output = MetadataSearchExecution;
    type Error = MetadataApiError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
        let budget = DISTRIBUTED_QUERY_DEADLINE.as_millis() as u64;
        self.deadline_ms
            .get_or_insert(now_ms.saturating_add(budget));
    }

    fn first(&mut self) -> Result<Next<MetadataSearchExecution>, MetadataApiError> {
        self.checked()?;
        if self.request.cursor.is_none() {
            let targets = self.request.target_nodes.take();
            return self.fanout(targets, false);
        }
        // Check the full realm because capped fan-out may omit the cursor's signer.
        let mut signers = match (self.mode(), self.request.target_nodes.clone()) {
            (ApiQueryMode::Local, _) => return self.resumed(vec![self.local_node], None),
            (ApiQueryMode::Distributed, Some(nodes)) => nodes,
            (ApiQueryMode::Distributed, None) => {
                self.stage = SearchStage::Discovery;
                let wait = self.remaining();
                return Ok(Next::Call(discovery_call(
                    self.realm_id,
                    self.local_node,
                    wait,
                )));
            }
        };
        signers.push(self.local_node);
        self.resumed(signers, None)
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<MetadataSearchExecution>, MetadataApiError> {
        match std::mem::replace(&mut self.stage, SearchStage::Start) {
            SearchStage::Start => Err(MetadataApiError::Internal("search not started".to_string())),
            SearchStage::Discovery => {
                let discovery = discovery_answer(result, self.local_node);
                let (signers, discovery) =
                    cursor_signers(discovery, self.local_node, &self.fingerprint);
                self.resumed(signers, Some(discovery))
            }
            SearchStage::Fanout => {
                let parts = result.child::<FanoutParts<NodeHits>, MetadataApiError>()??;
                self.paged(parts)
            }
            SearchStage::Sign(unsigned) => {
                let Ok(Event::Net(NetEvent::CursorSigned(Some(signature)))) = event(result) else {
                    return Err(MetadataApiError::Internal(
                        "net handle unavailable for search cursor signing".to_string(),
                    ));
                };
                let Unsigned {
                    mut cursor,
                    hits,
                    truncated,
                    stats,
                } = *unsigned;
                cursor.signature = signature;
                let next_cursor = (cursor.encode())
                    .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
                Ok(Next::Done(MetadataSearchExecution {
                    hits,
                    next_cursor: Some(next_cursor),
                    truncated,
                    fanout_stats: stats,
                }))
            }
        }
    }
}

/// What a signed search cursor resumes: its watermark, each node's position, and the nodes to
/// ask, which keep every node it resumes.
pub(super) struct Resumed {
    pub(super) watermark: SearchWatermark,
    pub(super) resume: HashMap<NodeId, u32>,
    pub(super) nodes: Vec<NodeId>,
    pub(super) discovery_failed: bool,
}

/// Reads `raw`, signed by one of `signers` for `fingerprint`, and picks the nodes to ask: the
/// requested ones, this node alone in local mode, or the discovered ones.
pub(super) fn resume_cursor(
    (raw, signers): (&str, &[NodeId]),
    fingerprint: [u8; 32],
    (requested, mode, local_node): (Option<&[NodeId]>, ApiQueryMode, NodeId),
    discovery: Option<RealmNodeDiscovery>,
) -> Result<Resumed, MetadataApiError> {
    let cursor = SearchCursor::decode(raw, signers)
        .map_err(|error| MetadataApiError::InvalidCursor(error.to_string()))?;
    if cursor.fingerprint != fingerprint {
        return Err(MetadataApiError::InvalidCursor(
            SearchCursorError::QueryMismatch.to_string(),
        ));
    }
    let resume = cursor.resume_positions();
    // Keep resumed nodes in the bounded selection so remaining hits are not silently
    // discarded when discovery changes.
    let mut nodes = match (requested, mode) {
        (Some(nodes), _) => select_fanout_nodes(nodes, local_node, &fingerprint),
        (None, ApiQueryMode::Local) => vec![local_node],
        (None, ApiQueryMode::Distributed) => (discovery.as_ref())
            .map_or_else(|| vec![local_node], |discovery| discovery.nodes.clone()),
    };
    for node_id in resume.keys() {
        if !nodes.contains(node_id) {
            nodes.push(*node_id);
        }
    }
    Ok(Resumed {
        watermark: cursor.payload.watermark.clone(),
        resume,
        nodes: deduplicate_fanout_nodes(nodes),
        discovery_failed: discovery.is_some_and(|discovery| discovery.failed),
    })
}

/// The nodes that may have signed a cursor, the whole realm since a capped fanout may omit the
/// signer, and the bounded selection of the discovered nodes to ask.
pub(super) fn cursor_signers(
    discovery: RealmNodeDiscovery,
    local_node: NodeId,
    fingerprint: &[u8; 32],
) -> (Vec<NodeId>, RealmNodeDiscovery) {
    let mut signers = discovery.nodes.clone();
    signers.push(local_node);
    let nodes = select_fanout_nodes(&discovery.nodes, local_node, fingerprint);
    let selected = RealmNodeDiscovery {
        nodes,
        failed: discovery.failed,
    };
    (signers, selected)
}

#[cfg(test)]
#[path = "search_fanout_tests.rs"]
mod call_tests;
