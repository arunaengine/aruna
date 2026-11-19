//! Searches the objects of the realm's nodes for one signed page, then the distributed buckets
//! that page skipped through their range indexes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::cursor::OBJECT_CURSOR_CONTEXT;
use aruna_core::effects::{Effect, EffectGroup, GroupForm, NetEffect};
use aruna_core::events::{Event, GroupEvent, GroupResult, NetEvent};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::rows::event;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use tracing::Span;

use super::distributed_types::record_object_result;
use super::fanout_types::{
    MetadataFanoutOperation, MetadataFanoutScope, MetadataFanoutStats, RealmNodeDiscovery,
    fanout_bearer,
};
use super::node_fanout::{
    DiscoveryOperation, FanoutParts, NodeCalls, NodeFanoutOperation, child_part, member_child,
    peer_answer, peer_request,
};
use super::path_types::deduplicate_fanout_nodes;
use super::search::{
    CURSOR_MAX_BYTES, MAX_SKIPPED_SEARCHES, ObjectSearchPartitions, ObjectSearchPlan,
    assemble_object_execution, cursor_partitions, fresh_partitions, incomplete_reason,
    plan_object_search, take_skipped,
};
use super::search_types::{
    ObjectCursor, ObjectExecution, ObjectQueryMode, PendingBucket, SearchQueryRequest,
};
use super::{DISTRIBUTED_QUERY_DEADLINE, MetadataApiError, MetadataReadError, map_read_error};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::metadata::protocol::MetadataTransportMessage;
use crate::realm::load_config::config_unavailable;
use crate::s3::object::search::{SearchNodePage, SearchObjectsError, SearchObjectsInput};
use crate::s3::object::search_scan::SearchObjectsOperation;

/// How the object fanout asks each node, from where the cursor left that node.
struct ObjectCalls {
    auth_token: Option<AuthToken>,
    input: SearchObjectsInput,
    starts: HashMap<NodeId, Option<Vec<u8>>>,
    /// Whether this node keeps range indexes for distributed buckets.
    ranged: bool,
}

impl ObjectCalls {
    fn start(&self, node: NodeId) -> Option<Vec<u8>> {
        self.starts.get(&node).cloned().flatten()
    }
}

impl NodeCalls for ObjectCalls {
    type Part = SearchNodePage;

    fn local(&self, node: NodeId) -> Effect {
        let input = SearchObjectsInput {
            node_id: node,
            start_after: self.start(node),
            ..self.input.clone()
        };
        let search = SearchObjectsOperation::new(input, self.ranged);
        Effect::SubOperation(boxed_suboperation(search, |result| {
            Event::Child(ChildOutcome::new(result))
        }))
    }

    fn remote(&self, node: NodeId, timeout: Duration) -> Effect {
        let message = MetadataTransportMessage::SearchObjects {
            auth_token: self.auth_token.clone(),
            query: self.input.query.clone(),
            key_match: self.input.key_match,
            bucket: self.input.bucket.clone(),
            limit: self.input.limit,
            start_after: self.start(node),
            as_of: self.input.as_of,
        };
        peer_request(node, &message, timeout)
    }

    fn read(
        &self,
        node: NodeId,
        local: bool,
        event: Event,
    ) -> Result<SearchNodePage, MetadataReadError> {
        if local {
            return child_part::<SearchNodePage, SearchObjectsError>(event);
        }
        match peer_answer(node, event)? {
            MetadataTransportMessage::ObjectSearchResults { result } => result,
            _ => Err(MetadataReadError::Unavailable),
        }
    }

    fn record(&self, span: &Span, result: &Result<SearchNodePage, MetadataReadError>) {
        record_object_result(span, result);
    }
}

/// The skipped bucket searches of one page and what the realm-wide part answered.
struct Answered {
    partitions: ObjectSearchPartitions,
    parts: Vec<(NodeId, SearchNodePage)>,
    stats: MetadataFanoutStats,
    searched: Vec<PendingBucket>,
}

enum ObjectStage {
    Start,
    Signers(String),
    Discovery,
    Fanout(ObjectSearchPartitions),
    Skipped(Box<Answered>),
    Sign(Box<(ObjectExecution, ObjectCursor)>),
}

/// One page of an object search. A realm-wide page also searches up to
/// [`MAX_SKIPPED_SEARCHES`] distributed buckets it skipped, each through its range indexes from
/// where the cursor left it, all within the same deadline; the rest wait in the cursor.
pub struct ObjectSearchOperation {
    realm_id: RealmId,
    local_node: NodeId,
    request: Option<SearchQueryRequest>,
    plan: Option<ObjectSearchPlan>,
    ranged: bool,
    deadline_ms: Option<u64>,
    now_ms: u64,
    stage: ObjectStage,
}

/// The object search a caller asked for; `ranged` says whether this node keeps range indexes.
pub fn search_objects(
    realm_id: RealmId,
    local_node: NodeId,
    request: SearchQueryRequest,
    ranged: bool,
) -> ObjectSearchOperation {
    ObjectSearchOperation {
        realm_id,
        local_node,
        request: Some(request),
        plan: None,
        ranged,
        deadline_ms: None,
        now_ms: 0,
        stage: ObjectStage::Start,
    }
}

impl ObjectSearchOperation {
    fn plan(&self) -> Result<&ObjectSearchPlan, MetadataApiError> {
        (self.plan.as_ref())
            .ok_or_else(|| MetadataApiError::Internal("search not planned".to_string()))
    }

    fn deadline(&self) -> u64 {
        self.deadline_ms.unwrap_or(self.now_ms)
    }

    fn remaining(&self) -> Duration {
        Duration::from_millis(self.deadline().saturating_sub(self.now_ms))
    }

    fn fanout(
        &mut self,
        partitions: ObjectSearchPartitions,
    ) -> Result<Next<ObjectExecution>, MetadataApiError> {
        let plan = self.plan()?;
        let active: Vec<_> = (partitions.partitions.iter())
            .filter(|partition| !partition.exhausted)
            .map(|partition| partition.node_id)
            .collect();
        // A cursor with only skipped buckets left asks no node.
        if active.is_empty() {
            let stats = MetadataFanoutStats {
                discovery_failed: partitions.discovery_failed,
                ..MetadataFanoutStats::default()
            };
            return self.answered(partitions, (Vec::new(), stats));
        }
        let starts = (partitions.partitions.iter())
            .map(|partition| (partition.node_id, partition.start_after.clone()))
            .collect();
        let calls = ObjectCalls {
            auth_token: fanout_bearer(plan.bearer_token.as_deref()),
            input: SearchObjectsInput {
                auth: plan.auth.clone(),
                realm_id: self.realm_id,
                node_id: self.local_node,
                query: plan.query.clone(),
                key_match: plan.key_match,
                bucket: plan.bucket.clone(),
                limit: plan.limit,
                start_after: None,
                as_of: partitions.as_of,
            },
            starts,
            ranged: self.ranged,
        };
        let scope = MetadataFanoutScope::new(
            Some(plan.mode.fanout_mode()),
            Some(active),
            plan.mode.allow_partial(),
        )
        .with_subject(plan.fingerprint)
        .with_discovery_failed(partitions.discovery_failed);
        let fanout = NodeFanoutOperation::new(
            (self.realm_id, self.local_node),
            scope,
            MetadataFanoutOperation::ObjectSearch,
            calls,
            map_read_error,
        )
        .until(self.deadline());
        self.stage = ObjectStage::Fanout(partitions);
        Ok(Next::Call(IoCall::child(fanout)))
    }

    fn fresh(
        &mut self,
        nodes: (Vec<NodeId>, bool),
    ) -> Result<Next<ObjectExecution>, MetadataApiError> {
        let as_of = UNIX_EPOCH + Duration::from_millis(self.now_ms);
        let partitions = fresh_partitions(self.plan()?, self.local_node, nodes, as_of)?;
        self.fanout(partitions)
    }

    fn resumed(
        &mut self,
        raw: &str,
        signers: &[NodeId],
    ) -> Result<Next<ObjectExecution>, MetadataApiError> {
        let partitions = cursor_partitions(self.plan()?, self.local_node, raw, signers)?;
        self.fanout(partitions)
    }

    /// The search of one skipped bucket, from where the cursor left it.
    fn skipped(&self, plan: &ObjectSearchPlan, pending: &PendingBucket) -> Effect {
        let request = SearchQueryRequest {
            auth: plan.auth.clone(),
            bearer_token: plan.bearer_token.clone(),
            query: plan.query.clone(),
            key_match: plan.key_match,
            bucket: Some(pending.bucket.clone()),
            limit: plan.limit,
            cursor: pending.cursor.clone(),
            mode: plan.mode,
            target_nodes: plan.target_nodes.clone(),
        };
        let mut search = search_objects(self.realm_id, self.local_node, request, self.ranged);
        search.deadline_ms = Some(self.deadline());
        Effect::SubOperation(boxed_suboperation(search, |result| {
            Event::Child(ChildOutcome::new(result))
        }))
    }

    fn answered(
        &mut self,
        mut partitions: ObjectSearchPartitions,
        (mut parts, stats): FanoutParts<SearchNodePage>,
    ) -> Result<Next<ObjectExecution>, MetadataApiError> {
        let mut queue = std::mem::take(&mut partitions.pending);
        for bucket in take_skipped(&mut parts) {
            if !queue.iter().any(|pending| pending.bucket == bucket) {
                queue.push(PendingBucket {
                    bucket,
                    cursor: None,
                });
            }
        }
        let rest = queue.split_off(queue.len().min(MAX_SKIPPED_SEARCHES));
        partitions.pending.extend(rest);
        let answered = Answered {
            partitions,
            parts,
            stats,
            searched: queue,
        };
        if answered.searched.is_empty() {
            return self.assembled(answered, Vec::new());
        }
        let plan = self.plan()?;
        let members: Vec<_> = (answered.searched.iter())
            .map(|pending| self.skipped(plan, pending))
            .collect();
        let limit = members.len();
        self.stage = ObjectStage::Skipped(Box::new(answered));
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members,
            limit,
            form: GroupForm::All { deadline: None },
        })))
    }

    fn assembled(
        &mut self,
        answered: Answered,
        results: Vec<GroupResult>,
    ) -> Result<Next<ObjectExecution>, MetadataApiError> {
        let Answered {
            mut partitions,
            parts,
            stats,
            searched,
        } = answered;
        let strict = self.plan()?.mode == ObjectQueryMode::DistributedStrict;
        let (mut found, mut failed) = (Vec::new(), 0);
        let mut results = results.into_iter();
        for pending in searched {
            match results
                .next()
                .and_then(member_child::<ObjectExecution, MetadataApiError>)
            {
                Some(Ok(page)) => {
                    if let Some(cursor) = page.next_cursor.clone() {
                        partitions.pending.push(PendingBucket {
                            bucket: pending.bucket,
                            cursor: Some(cursor),
                        });
                    }
                    found.push(page);
                }
                Some(Err(error)) if strict => return Err(error),
                _ => failed += 1,
            }
        }
        // The cursor carries the rest; what would not fit in it stays unsearched.
        let size = |pending: &[PendingBucket]| {
            (pending.iter())
                .map(|entry| entry.bucket.len() + entry.cursor.as_ref().map_or(0, String::len))
                .sum::<usize>()
        };
        while size(&partitions.pending) > CURSOR_MAX_BYTES / 2 {
            partitions.pending.pop();
            failed += 1;
        }
        // A bucket search that ended after its ranges moved keeps the rest of this search
        // incomplete.
        partitions.layout.changed |= found.iter().any(|page| page.layout_changed);
        let found_changed = found.iter().map(|page| page.page_changed).sum::<usize>();
        let certain = found.iter().all(|page| page.changed_keys.is_some());
        partitions.changed_keys =
            (partitions.changed_keys.filter(|_| certain)).map(|keys| keys + found_changed);
        let signer = (self.plan()?, self.local_node);
        let (mut execution, cursor) = assemble_object_execution(signer, partitions, parts, stats)?;
        execution.page_changed += found_changed;
        for page in found {
            execution.hits.extend(page.hits);
            execution.incomplete_ranges += page.incomplete_ranges;
            execution.index_progress.extend(page.index_progress);
            execution.partial_ranges.extend(page.partial_ranges);
            execution.complete &= page.complete;
        }
        execution.incomplete_ranges += failed;
        execution.complete &= failed == 0;
        // Strict mode answers complete pages only; work left in the cursor is not missing.
        if strict && execution.incomplete_ranges > 0 {
            return Err(MetadataApiError::SearchIncomplete(incomplete_reason(
                execution.incomplete_ranges,
                execution.partial_ranges.len(),
                &execution.index_progress,
            )));
        }
        let Some(cursor) = cursor else {
            return Ok(Next::Done(execution));
        };
        let bytes = (cursor.signing_bytes(OBJECT_CURSOR_CONTEXT))
            .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
        self.stage = ObjectStage::Sign(Box::new((execution, cursor)));
        let sign = Effect::Net(NetEffect::CursorSign(bytes));
        Ok(Next::Call(IoCall::Effect(Box::new(sign))))
    }
}

impl Operation for ObjectSearchOperation {
    type Output = ObjectExecution;
    type Error = MetadataApiError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
        let budget = DISTRIBUTED_QUERY_DEADLINE.as_millis() as u64;
        self.deadline_ms
            .get_or_insert(now_ms.saturating_add(budget));
    }

    fn first(&mut self) -> Result<Next<ObjectExecution>, MetadataApiError> {
        let request = (self.request.take())
            .ok_or_else(|| MetadataApiError::Internal("search already started".to_string()))?;
        let plan = plan_object_search(self.realm_id, request)?;
        let (mode, cursor, targets) = (plan.mode, plan.cursor.clone(), plan.target_nodes.clone());
        self.plan = Some(plan);
        match (cursor, mode, targets) {
            (Some(raw), ObjectQueryMode::Local, _) => self.resumed(&raw, &[self.local_node]),
            (Some(raw), _, _) => {
                self.stage = ObjectStage::Signers(raw);
                Ok(Next::Call(IoCall::child(GetConfigOperation::new(
                    self.realm_id,
                ))))
            }
            (None, ObjectQueryMode::Local, _) => self.fresh((vec![self.local_node], false)),
            (None, _, Some(nodes)) => self.fresh((deduplicate_fanout_nodes(nodes), false)),
            (None, _, None) => {
                self.stage = ObjectStage::Discovery;
                let discovery = DiscoveryOperation::new(self.realm_id, self.local_node);
                let member = Effect::SubOperation(boxed_suboperation(discovery, |result| {
                    Event::Child(ChildOutcome::new(result))
                }));
                Ok(Next::Call(IoCall::Group(EffectGroup {
                    members: vec![member],
                    limit: 1,
                    form: GroupForm::All {
                        deadline: Some(self.remaining()),
                    },
                })))
            }
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ObjectExecution>, MetadataApiError> {
        match std::mem::replace(&mut self.stage, ObjectStage::Start) {
            ObjectStage::Start => Err(MetadataApiError::Internal("search not started".to_string())),
            ObjectStage::Signers(raw) => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config
                    .inspect_err(config_unavailable)
                    .map_err(|_| MetadataApiError::ServiceUnavailable)?;
                let signers = config
                    .node_ids()
                    .map_err(|_| MetadataApiError::ServiceUnavailable)?;
                self.resumed(&raw, &signers)
            }
            ObjectStage::Discovery => {
                let found = match result {
                    IoResult::Group(GroupEvent::Finished { results }) => results
                        .into_iter()
                        .next()
                        .and_then(member_child::<RealmNodeDiscovery, MetadataApiError>),
                    _ => None,
                };
                match found {
                    Some(Ok(discovery)) => self.fresh((discovery.nodes, discovery.failed)),
                    _ => self.fresh((vec![self.local_node], true)),
                }
            }
            ObjectStage::Fanout(partitions) => {
                let parts = result.child::<FanoutParts<SearchNodePage>, MetadataApiError>()??;
                self.answered(partitions, parts)
            }
            ObjectStage::Skipped(answered) => {
                let IoResult::Group(GroupEvent::Finished { results }) = result else {
                    return self.assembled(*answered, Vec::new());
                };
                self.assembled(*answered, results)
            }
            ObjectStage::Sign(signing) => {
                let Ok(Event::Net(NetEvent::CursorSigned(Some(signature)))) = event(result) else {
                    return Err(MetadataApiError::Internal(
                        "net handle unavailable for object search cursor signing".to_string(),
                    ));
                };
                let (mut execution, mut cursor) = *signing;
                cursor.signature = signature;
                let encoded = (cursor.encode())
                    .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
                execution.next_cursor = Some(encoded);
                Ok(Next::Done(execution))
            }
        }
    }
}

#[cfg(test)]
#[path = "object_search_tests.rs"]
mod call_tests;
