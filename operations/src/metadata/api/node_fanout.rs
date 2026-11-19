//! Finds the realm's nodes and asks each of them, this node through its own call, under one
//! deadline, keeping the answers of the nodes that replied.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm, MetadataRequestEffect, NetEffect};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use tracing::{Span, warn};

use super::fanout::{ensure_query_mode, fanout_node_span};
use super::fanout_types::{
    MetadataFanoutOperation, MetadataFanoutScope, MetadataFanoutStats, RealmNodeDiscovery,
};
use super::path_types::{deduplicate_fanout_nodes, select_fanout_nodes};
use super::preflight_types::authorized_realm_nodes;
use super::{
    ApiQueryMode, DISTRIBUTED_QUERY_DEADLINE, MetadataApiError, MetadataReadError,
    QUERY_FANOUT_LIMIT, QUERY_MAX_NODES, map_read_error,
};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::fetch::peer_reply;
use crate::metadata::protocol::{MetadataTransportMessage, encode_message};
use crate::realm::get_nodes::{
    GetNodesError, GetNodesOperation, REALM_DISCOVERY_TIMEOUT, RealmPresence,
};

fn child_member<O>(operation: O) -> Effect
where
    O: Operation + Send + 'static,
    O::Output: std::fmt::Debug + Send + PartialEq + 'static,
    O::Error: std::fmt::Debug + Send + PartialEq + 'static,
{
    Effect::SubOperation(boxed_suboperation(operation, |result| {
        Event::Child(ChildOutcome::new(result))
    }))
}

/// The child result a group member answered with, when it has these types.
pub(super) fn member_child<T: 'static, E: 'static>(member: GroupResult) -> Option<Result<T, E>> {
    let GroupResult::Done(event) = member else {
        return None;
    };
    match *event {
        Event::Child(outcome) => outcome.take(),
        _ => None,
    }
}

#[derive(Debug, PartialEq)]
enum DiscoveryStage {
    Config,
    Presence(RealmConfigDocument),
}

/// The sync nodes of the realm that announce presence, this node always included, sorted by id.
/// An unreadable config, a failed or late presence lookup, or invalid node ids leave this node
/// alone and mark the discovery as failed.
#[derive(Debug, PartialEq)]
pub struct DiscoveryOperation {
    realm_id: RealmId,
    local_node: NodeId,
    stage: DiscoveryStage,
}

impl DiscoveryOperation {
    pub fn new(realm_id: RealmId, local_node: NodeId) -> Self {
        Self {
            realm_id,
            local_node,
            stage: DiscoveryStage::Config,
        }
    }

    fn local_only(&self) -> RealmNodeDiscovery {
        RealmNodeDiscovery {
            nodes: vec![self.local_node],
            failed: true,
        }
    }

    fn discovered(
        &self,
        config: &RealmConfigDocument,
        presence: Option<RealmPresence>,
    ) -> RealmNodeDiscovery {
        let (nodes, failed) = match presence {
            // Bounded-stale candidates are allowed; an unreachable one is reported through the
            // partial-result fields.
            Some(presence) => match authorized_realm_nodes(config, presence.into_nodes()) {
                Ok(nodes) => (nodes, false),
                Err(error) => {
                    warn!(error = %error, "realm config contains invalid node ids; using local-only metadata results");
                    return self.local_only();
                }
            },
            None => (HashSet::new(), true),
        };
        let mut nodes = nodes.into_iter().collect::<Vec<_>>();
        if !nodes.contains(&self.local_node) {
            nodes.push(self.local_node);
        }
        nodes.sort_by_key(|node_id| node_id.to_string());
        RealmNodeDiscovery { nodes, failed }
    }
}

impl Operation for DiscoveryOperation {
    type Output = RealmNodeDiscovery;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<RealmNodeDiscovery>, MetadataApiError> {
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RealmNodeDiscovery>, MetadataApiError> {
        match std::mem::replace(&mut self.stage, DiscoveryStage::Config) {
            DiscoveryStage::Config => {
                let Ok(config) = result.child::<RealmConfigDocument, GetConfigError>()? else {
                    warn!("realm config unavailable; querying the local replica only");
                    return Ok(Next::Done(self.local_only()));
                };
                self.stage = DiscoveryStage::Presence(config);
                // Discovery is raced so a fanout degrades to the reachable nodes instead of
                // stalling behind offline peers.
                Ok(Next::Call(IoCall::Group(EffectGroup {
                    members: vec![child_member(GetNodesOperation::new(self.realm_id))],
                    limit: 1,
                    form: GroupForm::All {
                        deadline: Some(REALM_DISCOVERY_TIMEOUT),
                    },
                })))
            }
            DiscoveryStage::Presence(config) => {
                let IoResult::Group(GroupEvent::Finished { results }) = result else {
                    return Ok(Next::Done(self.discovered(&config, None)));
                };
                let presence = results
                    .into_iter()
                    .next()
                    .and_then(member_child::<_, GetNodesError>);
                let presence = match presence {
                    Some(Ok(presence)) => Some(presence),
                    Some(Err(error)) => {
                        warn!(error = %error, "realm node discovery failed, using best-effort local-only metadata results");
                        None
                    }
                    None => {
                        warn!(
                            "realm node discovery timed out, using best-effort local-only metadata results"
                        );
                        None
                    }
                };
                Ok(Next::Done(self.discovered(&config, presence)))
            }
        }
    }
}

/// The realm's discovery as one group member bounded by `wait`.
pub(super) fn discovery_call(realm_id: RealmId, local_node: NodeId, wait: Duration) -> IoCall {
    IoCall::Group(EffectGroup {
        members: vec![child_member(DiscoveryOperation::new(realm_id, local_node))],
        limit: 1,
        form: GroupForm::All {
            deadline: Some(wait),
        },
    })
}

/// The discovery a [`discovery_call`] answered; one that failed or ran late leaves this node
/// alone and marks the discovery as failed.
pub(super) fn discovery_answer(result: IoResult, local_node: NodeId) -> RealmNodeDiscovery {
    let found = match result {
        IoResult::Group(GroupEvent::Finished { results }) => (results.into_iter().next())
            .and_then(member_child::<RealmNodeDiscovery, MetadataApiError>),
        _ => None,
    };
    match found {
        Some(Ok(discovery)) => discovery,
        _ => RealmNodeDiscovery {
            nodes: vec![local_node],
            failed: true,
        },
    }
}

/// The request of `message` to `node` within `timeout`. A message that cannot be encoded reaches
/// the peer empty, which answers it as unavailable.
pub(super) fn peer_request(
    node: NodeId,
    message: &MetadataTransportMessage,
    timeout: Duration,
) -> Effect {
    let frame = encode_message(message).unwrap_or_default();
    let request = MetadataRequestEffect {
        peer: node,
        frame,
        timeout,
    };
    Effect::Net(NetEffect::MetadataRequest(Box::new(request)))
}

/// The decoded answer of `node` to a [`peer_request`] member.
pub(super) fn peer_answer(
    node: NodeId,
    event: Event,
) -> Result<MetadataTransportMessage, MetadataReadError> {
    match peer_reply(node, IoResult::Event(Box::new(event)))? {
        Ok(message) => Ok(message),
        Err(_) => Err(MetadataReadError::Unavailable),
    }
}

/// The output of a child member; any failure of the child reads as unavailable.
pub(super) fn child_part<T: 'static, E: 'static>(event: Event) -> Result<T, MetadataReadError> {
    let Event::Child(outcome) = event else {
        return Err(MetadataReadError::Unavailable);
    };
    let part = outcome.take::<T, E>().and_then(Result::ok);
    part.ok_or(MetadataReadError::Unavailable)
}

/// How one fanout asks a node and reads its answer.
pub(super) trait NodeCalls: Send + 'static {
    type Part: std::fmt::Debug + Send + PartialEq + 'static;

    /// The call this node answers itself.
    fn local(&self, node: NodeId) -> Effect;
    /// The request to a peer, bounded by `timeout`.
    fn remote(&self, node: NodeId, timeout: Duration) -> Effect;
    /// One node's answer to its call.
    fn read(
        &self,
        node: NodeId,
        local: bool,
        event: Event,
    ) -> Result<Self::Part, MetadataReadError>;
    /// Notes one node's result on its span.
    fn record(&self, _span: &Span, _result: &Result<Self::Part, MetadataReadError>) {}
}

/// Every node's answer in node order, with how complete the fanout was.
pub(super) type FanoutParts<T> = (Vec<(NodeId, T)>, MetadataFanoutStats);

#[derive(Debug, PartialEq)]
enum FanoutStage {
    Start,
    Discovery,
    Nodes(Vec<NodeId>),
}

/// Asks this node alone in local mode. Otherwise it asks the realm's nodes, at most
/// `QUERY_MAX_NODES` of them chosen by rank, `QUERY_FANOUT_LIMIT` at a time, all within the
/// scope's deadline. A forbidden answer fails the fanout; a missing document anywhere is
/// unavailable; any failed node fails a caller that asked for a complete answer.
pub(super) struct NodeFanoutOperation<C: NodeCalls> {
    realm_id: RealmId,
    local_node: NodeId,
    scope: MetadataFanoutScope,
    label: MetadataFanoutOperation,
    calls: C,
    map_local_error: fn(MetadataReadError) -> MetadataApiError,
    now_ms: u64,
    deadline_ms: Option<u64>,
    stage: FanoutStage,
    stats: MetadataFanoutStats,
}

impl<C: NodeCalls> NodeFanoutOperation<C> {
    pub(super) fn new(
        (realm_id, local_node): (RealmId, NodeId),
        scope: MetadataFanoutScope,
        label: MetadataFanoutOperation,
        calls: C,
        map_local_error: fn(MetadataReadError) -> MetadataApiError,
    ) -> Self {
        Self {
            realm_id,
            local_node,
            deadline_ms: None,
            scope,
            label,
            calls,
            map_local_error,
            now_ms: 0,
            stage: FanoutStage::Start,
            stats: MetadataFanoutStats::default(),
        }
    }

    /// Bounds the fanout by `deadline_ms` on the runner's clock; by default it is the
    /// distributed query deadline from its first step.
    pub(super) fn until(mut self, deadline_ms: u64) -> Self {
        self.deadline_ms = Some(deadline_ms);
        self
    }

    fn remaining(&self) -> Duration {
        let deadline = self.deadline_ms.unwrap_or(self.now_ms);
        Duration::from_millis(deadline.saturating_sub(self.now_ms))
    }

    fn member(&self, node: NodeId) -> Effect {
        if node == self.local_node {
            return self.calls.local(node);
        }
        self.calls.remote(node, self.remaining())
    }

    fn ask(&mut self, nodes: Vec<NodeId>) -> Result<Next<FanoutParts<C::Part>>, MetadataApiError> {
        let members = nodes.iter().map(|node| self.member(*node)).collect();
        let deadline = Some(self.remaining());
        self.stage = FanoutStage::Nodes(nodes);
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members,
            limit: QUERY_FANOUT_LIMIT,
            form: GroupForm::All { deadline },
        })))
    }

    fn discovered(
        &mut self,
        discovery: RealmNodeDiscovery,
    ) -> Result<Next<FanoutParts<C::Part>>, MetadataApiError> {
        let discovery_failed = self.scope.discovery_failed || discovery.failed;
        let mut nodes = discovery.nodes;
        nodes.retain(|node_id| !self.scope.skip.contains(node_id));
        if discovery_failed && !self.scope.allow_partial {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        let mut omitted = Vec::new();
        if nodes.len() > QUERY_MAX_NODES {
            let label = self.label.label();
            let mut subject = Vec::with_capacity(32 + label.len() + 32 + 1);
            subject.extend_from_slice(self.realm_id.as_bytes());
            subject.extend_from_slice(label.as_bytes());
            if let Some(request_subject) = self.scope.subject {
                subject.extend_from_slice(&request_subject);
            }
            subject.extend_from_slice(self.local_node.as_bytes());
            let selected = select_fanout_nodes(&nodes, self.local_node, &subject);
            omitted = (nodes.iter().copied())
                .filter(|node_id| !selected.contains(node_id))
                .collect();
            nodes = selected;
        }
        Span::current().record("node_count", nodes.len() as u64);
        self.stats = MetadataFanoutStats {
            nodes_queried: nodes.len(),
            nodes_failed: omitted.len() + usize::from(discovery_failed),
            failed_partitions: Vec::new(),
            discovery_failed,
            omitted,
        };
        self.ask(nodes)
    }

    /// One node's result; a member that did not finish in time is unavailable.
    fn node_result(
        &self,
        node: NodeId,
        member: Option<GroupResult>,
    ) -> Result<C::Part, MetadataReadError> {
        let local = node == self.local_node;
        let span = fanout_node_span(self.label, node, local);
        let result = match member {
            Some(GroupResult::Done(event)) => self.calls.read(node, local, *event),
            _ => Err(MetadataReadError::Unavailable),
        };
        self.calls.record(&span, &result);
        result
    }

    fn local_answer(
        &self,
        results: Vec<GroupResult>,
    ) -> Result<FanoutParts<C::Part>, MetadataApiError> {
        let node = self.local_node;
        let result = self.node_result(node, results.into_iter().next());
        let part = result.map_err(self.map_local_error)?;
        let stats = MetadataFanoutStats {
            nodes_queried: 1,
            ..MetadataFanoutStats::default()
        };
        Ok((vec![(node, part)], stats))
    }

    fn answers(
        &mut self,
        nodes: Vec<NodeId>,
        results: Vec<GroupResult>,
    ) -> Result<FanoutParts<C::Part>, MetadataApiError> {
        let mut stats = std::mem::take(&mut self.stats);
        let mut parts = Vec::with_capacity(nodes.len());
        let mut auth_error = None;
        let mut not_found = false;
        let mut results = results.into_iter();
        for node in nodes {
            match self.node_result(node, results.next()) {
                Ok(part) => parts.push((node, part)),
                // A rejected forwarded credential is a failed partition; local authorization
                // already vouched for the caller.
                Err(MetadataReadError::Unauthorized) => {
                    stats.nodes_failed += 1;
                    stats.failed_partitions.push(node);
                    warn!(node_id = ?node, operation = self.label.label(), "distributed metadata skipped unauthorized node result");
                }
                // An authenticated denial anywhere fails the whole query.
                Err(error @ MetadataReadError::Forbidden) => {
                    stats.nodes_failed += 1;
                    stats.failed_partitions.push(node);
                    auth_error.get_or_insert(map_read_error(error));
                }
                Err(MetadataReadError::NotFound) => {
                    stats.nodes_failed += 1;
                    stats.failed_partitions.push(node);
                    not_found = true;
                }
                Err(MetadataReadError::Unavailable | MetadataReadError::TooLarge) => {
                    stats.nodes_failed += 1;
                    stats.failed_partitions.push(node);
                    warn!(node_id = ?node, operation = self.label.label(), error = "unavailable", "distributed metadata skipped failed node result");
                }
            }
        }
        if let Some(error) = auth_error {
            return Err(error);
        }
        if not_found {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        // Nodes omitted by the fanout cap truncate the answer, so a caller that asked for a
        // complete result must not be told it is complete.
        if !self.scope.allow_partial && stats.nodes_failed > 0 {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        Ok((parts, stats))
    }
}

impl<C: NodeCalls> Operation for NodeFanoutOperation<C> {
    type Output = FanoutParts<C::Part>;
    type Error = MetadataApiError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
        let budget = DISTRIBUTED_QUERY_DEADLINE.as_millis() as u64;
        self.deadline_ms
            .get_or_insert(now_ms.saturating_add(budget));
    }

    fn first(&mut self) -> Result<Next<FanoutParts<C::Part>>, MetadataApiError> {
        ensure_query_mode(&self.scope.mode);
        if self.scope.mode == Some(ApiQueryMode::Local) {
            return self.ask(vec![self.local_node]);
        }
        match self.scope.target_nodes.take() {
            Some(nodes) => {
                let discovery = RealmNodeDiscovery {
                    nodes: deduplicate_fanout_nodes(nodes),
                    failed: false,
                };
                self.discovered(discovery)
            }
            None => {
                self.stage = FanoutStage::Discovery;
                let discovery = DiscoveryOperation::new(self.realm_id, self.local_node);
                Ok(Next::Call(IoCall::Group(EffectGroup {
                    members: vec![child_member(discovery)],
                    limit: 1,
                    form: GroupForm::All {
                        deadline: Some(self.remaining()),
                    },
                })))
            }
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<FanoutParts<C::Part>>, MetadataApiError> {
        let IoResult::Group(GroupEvent::Finished { results }) = result else {
            return Err(MetadataApiError::Internal(format!(
                "expected the fanout group, got {}",
                result.label()
            )));
        };
        match std::mem::replace(&mut self.stage, FanoutStage::Start) {
            FanoutStage::Start => Err(MetadataApiError::Internal("fanout not started".to_string())),
            FanoutStage::Discovery => {
                let discovery = results
                    .into_iter()
                    .next()
                    .and_then(member_child::<_, MetadataApiError>);
                let discovery = match discovery {
                    Some(Ok(discovery)) => discovery,
                    _ => RealmNodeDiscovery {
                        nodes: vec![self.local_node],
                        failed: true,
                    },
                };
                self.discovered(discovery)
            }
            FanoutStage::Nodes(_) if self.scope.mode == Some(ApiQueryMode::Local) => {
                Ok(Next::Done(self.local_answer(results)?))
            }
            FanoutStage::Nodes(nodes) => Ok(Next::Done(self.answers(nodes, results)?)),
        }
    }
}

#[cfg(test)]
#[path = "node_fanout_tests.rs"]
mod call_tests;
