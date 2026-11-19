//! Runs one SPARQL query across the realm's nodes and merges the partitions as one store would.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::hidden_token;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::metadata::{
    AuthToken, MergeError, MergeJob, MergeOutput, MetadataEffect, MetadataError, MetadataEvent,
    MetadataQueryResults, QUERY_MAX_ROWS, SolutionRow,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::rows::event;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use tracing::Span;

use super::distributed::{ordered_query, record_query_result};
use super::distributed_types::{
    MetadataQueryForm, aggregate_query_results, query_form, query_select_limit,
};
use super::fanout::ensure_query_form;
use super::fanout_types::{
    MetadataFanoutOperation, MetadataFanoutScope, MetadataFanoutStats, fanout_bearer,
    query_union_safe,
};
use super::node_fanout::{FanoutParts, NodeCalls, NodeFanoutOperation};
use super::read_types::MetadataQueryExecution;
use super::{ApiQueryMode, DISTRIBUTED_QUERY_DEADLINE, MetadataApiError, MetadataReadError};
use crate::forward::fetch::{peer_call, peer_reply};
use crate::metadata::local_query::LocalQueryOperation;
use crate::metadata::protocol::{MetadataTransportMessage, encode_message, metadata_read_error};
use crate::metadata::query_cache::{RemoteKeyInput, credential_digest, remote_key};

/// Where one partition query runs.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Partition {
    /// This node's graphs of the realm, under the caller's own authority.
    Local(RealmId, Option<AuthContext>),
    /// A peer, under the forwarded credential.
    Remote {
        node: NodeId,
        auth_token: Option<AuthToken>,
    },
}

fn merge_call(job: MergeJob, budget: Duration) -> IoCall {
    let effect = MetadataEffect::MergeRows { job, budget };
    IoCall::Effect(Box::new(Effect::Metadata(effect)))
}

/// The merged rows or tie count of a [`merge_call`].
fn merged(result: IoResult) -> Result<MergeOutput, MergeError> {
    match event(result).map_err(|error| MergeError::Failed(error.to_string()))? {
        Event::Metadata(MetadataEvent::RowsMerged { result }) => result,
        Event::Metadata(MetadataEvent::Error { error, .. }) => {
            Err(MergeError::Failed(error.to_string()))
        }
        other => Err(MergeError::Failed(other.label().to_string())),
    }
}

fn merge_error(error: MergeError) -> MetadataApiError {
    match error {
        MergeError::TooLarge => MetadataApiError::BadRequest,
        MergeError::Refused(message) => MetadataApiError::InvalidQuery(message),
        MergeError::Cancelled => MetadataApiError::ServiceUnavailable,
        MergeError::Failed(message) => MetadataApiError::Internal(message),
    }
}

#[derive(Debug, PartialEq)]
enum TieStage {
    /// The query as asked, without a tie margin.
    Plain,
    /// The query with this margin past its `LIMIT`.
    Fetch { margin: usize, fetch: usize },
    /// The tie count of the rows that came back for this margin.
    Ties {
        margin: usize,
        fetch: usize,
        rows: Vec<SolutionRow>,
    },
}

/// Runs one partition's ordered query and keeps every row past its `LIMIT` that orders equal to
/// the last row inside it, refetching with a doubling margin, so no partition cuts a tie.
#[derive(Debug, PartialEq)]
pub(crate) struct TieFetchOperation {
    partition: Partition,
    graph_iris: Option<Vec<String>>,
    query: String,
    /// The `LIMIT` of an ordered query whose ties must stay whole.
    limit: Option<usize>,
    deadline_ms: u64,
    now_ms: u64,
    stage: TieStage,
}

impl TieFetchOperation {
    pub(super) fn new(
        partition: Partition,
        (graph_iris, query): (Option<Vec<String>>, String),
        limit: Option<usize>,
        deadline_ms: u64,
    ) -> Self {
        Self {
            partition,
            graph_iris,
            query,
            limit: limit.filter(|limit| *limit > 0),
            deadline_ms,
            now_ms: 0,
            stage: TieStage::Plain,
        }
    }

    fn remaining(&self) -> Duration {
        Duration::from_millis(self.deadline_ms.saturating_sub(self.now_ms))
    }

    fn run(&self, sparql: String) -> Result<IoCall, MetadataReadError> {
        Ok(match &self.partition {
            Partition::Local(realm_id, auth) => {
                let caller = (*realm_id, auth.clone());
                IoCall::child(LocalQueryOperation::new(
                    caller,
                    self.graph_iris.clone(),
                    sparql,
                ))
            }
            Partition::Remote { node, auth_token } => {
                let message = MetadataTransportMessage::QueryGraphs {
                    auth_token: auth_token.clone(),
                    graph_iris: self.graph_iris.clone(),
                    sparql,
                };
                let frame = encode_message(&message).map_err(|_| MetadataReadError::Unavailable)?;
                peer_call(*node, frame, self.remaining())
            }
        })
    }

    fn answer(&self, result: IoResult) -> Result<MetadataQueryResults, MetadataReadError> {
        match &self.partition {
            Partition::Local(..) => (result.child::<MetadataQueryResults, MetadataError>()?)
                .map_err(metadata_read_error),
            Partition::Remote { node, .. } => match peer_reply(*node, result)? {
                Ok(MetadataTransportMessage::QueryResults { result }) => result,
                _ => Err(MetadataReadError::Unavailable),
            },
        }
    }

    /// The fetch with `margin` rows past the limit, or the plain query once no margin is left.
    fn fetch(&mut self, margin: usize) -> Result<Next<MetadataQueryResults>, MetadataReadError> {
        let Some(limit) = self.limit else {
            return self.plain();
        };
        // Craqle refuses results over its row limit, so fetches stop there and keep what fits.
        let fetch = limit.saturating_add(margin).min(QUERY_MAX_ROWS).max(limit);
        if fetch == limit {
            return self.plain();
        }
        let Ok(mut parsed) = spargebra::SparqlParser::new().parse_query(&self.query) else {
            return self.plain();
        };
        let spargebra::Query::Select {
            pattern: spargebra::algebra::GraphPattern::Slice { length, .. },
            ..
        } = &mut parsed
        else {
            return self.plain();
        };
        *length = Some(fetch);
        self.stage = TieStage::Fetch { margin, fetch };
        Ok(Next::Call(self.run(parsed.to_string())?))
    }

    fn plain(&mut self) -> Result<Next<MetadataQueryResults>, MetadataReadError> {
        self.stage = TieStage::Plain;
        Ok(Next::Call(self.run(self.query.clone())?))
    }
}

impl Operation for TieFetchOperation {
    type Output = MetadataQueryResults;
    type Error = MetadataReadError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<MetadataQueryResults>, MetadataReadError> {
        self.fetch(1)
    }

    fn next(&mut self, result: IoResult) -> Result<Next<MetadataQueryResults>, MetadataReadError> {
        match std::mem::replace(&mut self.stage, TieStage::Plain) {
            TieStage::Plain => Ok(Next::Done(self.answer(result)?)),
            TieStage::Fetch { margin, fetch } => {
                let limit = self.limit.unwrap_or(0);
                let rows = match self.answer(result)? {
                    MetadataQueryResults::Solutions(rows) if rows.len() > limit => rows,
                    other => return Ok(Next::Done(other)),
                };
                let job = MergeJob::Ties {
                    partition: self.query.clone(),
                    rows: rows[limit - 1..].to_vec(),
                };
                self.stage = TieStage::Ties {
                    margin,
                    fetch,
                    rows,
                };
                Ok(Next::Call(merge_call(job, self.remaining())))
            }
            TieStage::Ties {
                margin,
                fetch,
                mut rows,
            } => {
                let Ok(MergeOutput::Tied(tied)) = merged(result) else {
                    return Err(MetadataReadError::Unavailable);
                };
                let limit = self.limit.unwrap_or(0);
                let kept = limit - 1 + tied;
                if kept < rows.len() || rows.len() < fetch || fetch == QUERY_MAX_ROWS {
                    rows.truncate(kept);
                    return Ok(Next::Done(MetadataQueryResults::Solutions(rows)));
                }
                self.fetch(margin.saturating_mul(2))
            }
        }
    }
}

/// Both sides' fanout as one: a node either side could not ask counts as failed.
fn merge_stats(left: MetadataFanoutStats, right: MetadataFanoutStats) -> MetadataFanoutStats {
    let mut failed_partitions = left.failed_partitions;
    for node_id in right.failed_partitions {
        if !failed_partitions.contains(&node_id) {
            failed_partitions.push(node_id);
        }
    }
    let mut omitted = left.omitted;
    for node_id in right.omitted {
        if !omitted.contains(&node_id) {
            omitted.push(node_id);
        }
    }
    MetadataFanoutStats {
        nodes_queried: left.nodes_queried.max(right.nodes_queried),
        nodes_failed: (left.nodes_failed.max(right.nodes_failed)).max(failed_partitions.len()),
        failed_partitions,
        discovery_failed: left.discovery_failed || right.discovery_failed,
        omitted,
    }
}

/// How the query fanout asks each node: a tie fetch here or at a peer.
#[derive(Debug, PartialEq)]
struct QueryCalls {
    realm_id: RealmId,
    auth: Option<AuthContext>,
    auth_token: Option<AuthToken>,
    graph_iris: Option<Vec<String>>,
    partition: String,
    ties: Option<usize>,
    deadline_ms: u64,
}

impl QueryCalls {
    fn fetch(&self, partition: Partition) -> Effect {
        let query = (self.graph_iris.clone(), self.partition.clone());
        let fetch = TieFetchOperation::new(partition, query, self.ties, self.deadline_ms);
        Effect::SubOperation(boxed_suboperation(fetch, |result| {
            Event::Child(ChildOutcome::new(result))
        }))
    }
}

impl NodeCalls for QueryCalls {
    type Part = MetadataQueryResults;

    fn local(&self, _node: NodeId) -> Effect {
        self.fetch(Partition::Local(self.realm_id, self.auth.clone()))
    }

    // Each fetch of a tie refetch asks within what is left of the query deadline.
    fn remote(&self, node: NodeId, _timeout: Duration) -> Effect {
        let auth_token = self.auth_token.clone();
        self.fetch(Partition::Remote { node, auth_token })
    }

    fn read(
        &self,
        _node: NodeId,
        _local: bool,
        event: Event,
    ) -> Result<MetadataQueryResults, MetadataReadError> {
        match event {
            Event::Child(outcome) => outcome
                .take::<MetadataQueryResults, MetadataReadError>()
                .ok_or(MetadataReadError::Unavailable)?,
            _ => Err(MetadataReadError::Unavailable),
        }
    }

    fn record(&self, span: &Span, result: &Result<MetadataQueryResults, MetadataReadError>) {
        record_query_result(span, result);
    }
}

type QueryAnswer = MetadataQueryExecution;

fn execution(results: MetadataQueryResults, fanout_stats: MetadataFanoutStats) -> QueryAnswer {
    MetadataQueryExecution {
        results,
        fanout_stats,
    }
}

#[derive(Debug, PartialEq)]
enum DistributedStage {
    Start,
    Join,
    Joined(MetadataFanoutStats),
    Cache,
    Fanout,
    Ordered(MetadataFanoutStats),
}

/// The caller of a distributed query: its own authority here and the bearer it forwards.
pub type QueryCaller = (Option<AuthContext>, Option<String>);

/// Runs one query across the realm. A bounded join no partition answers alone runs each side
/// across the realm and joins them; otherwise each node answers for its own graphs and the parts
/// merge, in order when ordered. Complete answers per credential scope use the query cache.
#[derive(PartialEq)]
pub struct DistributedQueryOperation {
    realm_id: RealmId,
    local_node: NodeId,
    caller: QueryCaller,
    graph_iris: Option<Vec<String>>,
    query: String,
    scope: MetadataFanoutScope,
    /// Whether the query text still needs the caller-facing form check.
    validate: bool,
    deadline_ms: Option<u64>,
    now_ms: u64,
    form: Option<MetadataQueryForm>,
    select_limit: Option<usize>,
    ordered: Option<String>,
    cache_key: Option<[u8; 32]>,
    cache_stamp: Option<[u64; 2]>,
    stage: DistributedStage,
}

/// Shows whether a bearer is forwarded, never its value.
impl std::fmt::Debug for DistributedQueryOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DistributedQueryOperation")
            .field("realm_id", &self.realm_id)
            .field("local_node", &self.local_node)
            .field("caller", &(&self.caller.0, hidden_token(&self.caller.1)))
            .field("graph_iris", &self.graph_iris)
            .field("query", &self.query)
            .field("scope", &self.scope)
            .field("validate", &self.validate)
            .field("deadline_ms", &self.deadline_ms)
            .field("now_ms", &self.now_ms)
            .field("form", &self.form)
            .field("select_limit", &self.select_limit)
            .field("ordered", &self.ordered)
            .field("cache_key", &self.cache_key)
            .field("cache_stamp", &self.cache_stamp)
            .field("stage", &self.stage)
            .finish()
    }
}

impl DistributedQueryOperation {
    pub(super) fn new(
        (realm_id, local_node): (RealmId, NodeId),
        caller: QueryCaller,
        (graph_iris, query): (Option<Vec<String>>, String),
        scope: MetadataFanoutScope,
    ) -> Self {
        Self {
            realm_id,
            local_node,
            caller,
            graph_iris,
            query,
            scope,
            validate: false,
            deadline_ms: None,
            now_ms: 0,
            form: None,
            select_limit: None,
            ordered: None,
            cache_key: None,
            cache_stamp: None,
            stage: DistributedStage::Start,
        }
    }

    /// Runs a caller's query, which first passes the form check every request does.
    pub(super) fn request(
        node: (RealmId, NodeId),
        caller: QueryCaller,
        query: (Option<Vec<String>>, String),
        scope: MetadataFanoutScope,
    ) -> Self {
        Self {
            validate: true,
            ..Self::new(node, caller, query, scope)
        }
    }

    /// Bounds the query by `deadline_ms` on the runner's clock, as a join's sides share it.
    fn until(mut self, deadline_ms: Option<u64>) -> Self {
        self.deadline_ms = deadline_ms;
        self
    }

    fn remaining(&self) -> Duration {
        let deadline = self.deadline_ms.unwrap_or(self.now_ms);
        Duration::from_millis(deadline.saturating_sub(self.now_ms))
    }

    fn mode(&self) -> ApiQueryMode {
        self.scope.mode.unwrap_or(ApiQueryMode::Distributed)
    }

    fn single_dataset(&self) -> bool {
        self.mode() == ApiQueryMode::Local || self.graph_iris.is_some()
    }

    fn join(&mut self) -> Result<Next<QueryAnswer>, MetadataApiError> {
        let plan =
            aruna_graph::merge::join_plan(&self.query).ok_or(MetadataApiError::BadRequest)?;
        let members = (plan.sides.iter())
            .map(|side| {
                let query = Self::new(
                    (self.realm_id, self.local_node),
                    self.caller.clone(),
                    (None, side.clone()),
                    self.scope.clone(),
                )
                .until(self.deadline_ms);
                Effect::SubOperation(boxed_suboperation(query, |result| {
                    Event::Child(ChildOutcome::new(result))
                }))
            })
            .collect();
        self.stage = DistributedStage::Join;
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members,
            limit: 2,
            form: GroupForm::All { deadline: None },
        })))
    }

    fn sides_answered(&mut self, result: IoResult) -> Result<Next<QueryAnswer>, MetadataApiError> {
        let IoResult::Group(GroupEvent::Finished { results }) = result else {
            return Err(MetadataApiError::ServiceUnavailable);
        };
        let mut sides = Vec::with_capacity(2);
        for member in results {
            let GroupResult::Done(event) = member else {
                return Err(MetadataApiError::ServiceUnavailable);
            };
            let Event::Child(outcome) = *event else {
                return Err(MetadataApiError::ServiceUnavailable);
            };
            let side = outcome.take::<QueryAnswer, MetadataApiError>();
            sides.push(side.ok_or(MetadataApiError::ServiceUnavailable)??);
        }
        let [left, right] = <[QueryAnswer; 2]>::try_from(sides)
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        let rows = |results: MetadataQueryResults| match results {
            MetadataQueryResults::Solutions(rows) => rows,
            MetadataQueryResults::Boolean(_) | MetadataQueryResults::Graph(_) => Vec::new(),
        };
        let stats = merge_stats(left.fanout_stats, right.fanout_stats);
        let job = MergeJob::Join {
            query: self.query.clone(),
            left: rows(left.results),
            right: rows(right.results),
        };
        self.stage = DistributedStage::Joined(stats);
        Ok(Next::Call(merge_call(job, self.remaining())))
    }

    fn fanout(&mut self) -> Result<Next<QueryAnswer>, MetadataApiError> {
        let ordered = match self.single_dataset() {
            true => None,
            false => ordered_query(&self.query)?,
        };
        let ties = ordered.as_ref().and(self.select_limit);
        let calls = QueryCalls {
            realm_id: self.realm_id,
            auth: self.caller.0.clone(),
            auth_token: fanout_bearer(self.caller.1.as_deref()),
            graph_iris: self.graph_iris.clone(),
            partition: ordered.clone().unwrap_or_else(|| self.query.clone()),
            ties,
            deadline_ms: self.deadline_ms.unwrap_or(self.now_ms),
        };
        self.ordered = ordered;
        let fanout = NodeFanoutOperation::new(
            (self.realm_id, self.local_node),
            self.scope.clone(),
            MetadataFanoutOperation::Query,
            calls,
        );
        let fanout = match self.deadline_ms {
            Some(deadline_ms) => fanout.until(deadline_ms),
            None => fanout,
        };
        self.stage = DistributedStage::Fanout;
        Ok(Next::Call(IoCall::child(fanout)))
    }

    fn parts(
        &mut self,
        (parts, stats): FanoutParts<MetadataQueryResults>,
    ) -> Result<Next<QueryAnswer>, MetadataApiError> {
        let form = self.form.ok_or(MetadataApiError::BadRequest)?;
        let parts: Vec<_> = parts.into_iter().map(|(_, result)| result).collect();
        if self.single_dataset() {
            let results = match parts.into_iter().next() {
                Some(result) => result,
                None => aggregate_query_results(Vec::new(), form, self.select_limit)?,
            };
            return Ok(self.answered(results, stats));
        }
        if let Some(partition) = self.ordered.clone() {
            let job = MergeJob::Order {
                partition,
                results: parts,
            };
            self.stage = DistributedStage::Ordered(stats);
            return Ok(Next::Call(merge_call(job, self.remaining())));
        }
        let results = aggregate_query_results(parts, form, self.select_limit)?;
        Ok(self.answered(results, stats))
    }

    /// The answer, kept in the query cache when it is complete.
    fn answered(
        &self,
        results: MetadataQueryResults,
        stats: MetadataFanoutStats,
    ) -> Next<QueryAnswer> {
        let complete = stats.nodes_failed == 0
            && !stats.discovery_failed
            && stats.failed_partitions.is_empty();
        let (Some(key), Some(stamp), true) = (self.cache_key, self.cache_stamp, complete) else {
            return Next::Done(execution(results, stats));
        };
        let store = MetadataEffect::QueryCachePut {
            key,
            stamp,
            results: results.clone(),
            nodes_queried: stats.nodes_queried,
        };
        Next::Then {
            output: execution(results, stats),
            calls: vec![IoCall::Effect(Box::new(Effect::Metadata(store)))],
        }
    }

    fn cache_read(&mut self, result: IoResult) -> Result<Next<QueryAnswer>, MetadataApiError> {
        if let Ok(Event::Metadata(MetadataEvent::QueryCacheEntry { stamp, entry })) = event(result)
        {
            if let Some((results, nodes_queried)) = entry {
                let stats = MetadataFanoutStats {
                    nodes_queried,
                    ..MetadataFanoutStats::default()
                };
                return Ok(Next::Done(execution(results, stats)));
            }
            self.cache_stamp = Some(stamp);
        }
        self.fanout()
    }
}

impl Operation for DistributedQueryOperation {
    type Output = QueryAnswer;
    type Error = MetadataApiError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
        let budget = DISTRIBUTED_QUERY_DEADLINE.as_millis() as u64;
        self.deadline_ms
            .get_or_insert(now_ms.saturating_add(budget));
    }

    fn first(&mut self) -> Result<Next<QueryAnswer>, MetadataApiError> {
        if self.validate {
            ensure_query_form(&self.query)?;
        }
        let distributed = self.mode() == ApiQueryMode::Distributed;
        if distributed && self.graph_iris.is_none() && !query_union_safe(&self.query) {
            return self.join();
        }
        let form = query_form(&self.query).ok_or(MetadataApiError::BadRequest)?;
        self.form = Some(form);
        self.select_limit = match form {
            MetadataQueryForm::Select => query_select_limit(&self.query),
            MetadataQueryForm::Ask => None,
        };
        // Remote partitions authorize on the forwarded credential, so entries are partitioned
        // by credential digest.
        let (auth, bearer) = (&self.caller.0, self.caller.1.as_deref());
        self.cache_key = credential_digest(auth.as_ref(), bearer)
            .filter(|_| !self.scope.discovery_failed)
            .map(|credential| {
                remote_key(&RemoteKeyInput {
                    distributed,
                    realm_id: self.realm_id,
                    credential: &credential,
                    graph_iris: self.graph_iris.as_deref(),
                    sparql: &self.query,
                    allow_partial: self.scope.allow_partial,
                    target_nodes: self.scope.target_nodes.as_deref(),
                })
                .bytes()
            });
        let Some(key) = self.cache_key else {
            return self.fanout();
        };
        self.stage = DistributedStage::Cache;
        let read = MetadataEffect::QueryCacheGet { key };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(read)))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<QueryAnswer>, MetadataApiError> {
        match std::mem::replace(&mut self.stage, DistributedStage::Start) {
            DistributedStage::Start => {
                Err(MetadataApiError::Internal("query not started".to_string()))
            }
            DistributedStage::Join => self.sides_answered(result),
            DistributedStage::Joined(stats) => match merged(result).map_err(merge_error)? {
                MergeOutput::Rows(rows) => Ok(Next::Done(execution(rows, stats))),
                MergeOutput::Tied(_) => Err(MetadataApiError::ServiceUnavailable),
            },
            DistributedStage::Cache => self.cache_read(result),
            DistributedStage::Fanout => {
                let parts =
                    result.child::<FanoutParts<MetadataQueryResults>, MetadataApiError>()??;
                self.parts(parts)
            }
            DistributedStage::Ordered(stats) => {
                let MergeOutput::Rows(rows) = merged(result).map_err(merge_error)? else {
                    return Err(MetadataApiError::ServiceUnavailable);
                };
                let form = self.form.ok_or(MetadataApiError::BadRequest)?;
                let results = aggregate_query_results(vec![rows], form, self.select_limit)?;
                Ok(self.answered(results, stats))
            }
        }
    }
}

#[cfg(test)]
#[path = "query_fanout_tests.rs"]
mod call_tests;
