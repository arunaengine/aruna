//! Finds the node a data tool call on a bucket runs on: this node, a named one, the member that
//! keeps the key, or the holder a realm-wide bucket search finds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::distribution::ReadRoute;
use thiserror::Error;

use super::tool::{Holder, RouteError, add_holder, pick_node, search_done};
use crate::distribution::guard::GuardError;
use crate::distribution::resolve::key_holders;
use crate::distribution::routing::route_read;
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::bucket_search::search_buckets_distributed;
use crate::metadata::api::search_types::{BucketSearchExecution, BucketSearchRequest};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

const SEARCH_PAGE: usize = 50;
/// How long finding the node that holds a bucket may take, every search page included.
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Where a tool call asks to run. Never shown in a `Debug` print, as it holds the caller's token.
#[derive(Clone, PartialEq)]
pub struct ToolRoute {
    pub realm_id: RealmId,
    pub here: NodeId,
    pub auth: AuthContext,
    pub bearer: Option<String>,
    pub bucket: String,
    pub named: Option<NodeId>,
    pub key: String,
}

impl std::fmt::Debug for ToolRoute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ToolRoute")
    }
}

/// What a holder search could not cover: the nodes that did not answer, and whether nodes
/// failed, node discovery failed or the search ran out of time.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Missed {
    pub nodes: Vec<NodeId>,
    pub incomplete: bool,
}

/// The holders of a bucket name, each distributed bucket once, and what the search missed.
#[derive(Debug, PartialEq)]
pub struct HolderFound {
    pub holders: Vec<Holder>,
    pub missed: Missed,
}

/// Why a tool call has no node to run on.
#[derive(Debug, Error, PartialEq)]
pub enum ToolRouteError {
    #[error(transparent)]
    Bucket(GetBucketError),
    #[error(transparent)]
    Guard(GuardError),
    #[error(transparent)]
    Search(MetadataApiError),
    /// Out of time, a bucket of the same name elsewhere is not ruled out.
    #[error("the holder search did not finish in time")]
    SearchLate,
    #[error("no node can be chosen for the call")]
    Route(RouteError, Missed),
    #[error(transparent)]
    Io(#[from] IoError),
}

/// Pages through the realm's bucket search for the nodes holding the bucket whose key the caller
/// may read, until it can tell or its time is up.
#[derive(Debug, PartialEq)]
pub struct HolderSearchOperation {
    route: ToolRoute,
    cursor: Option<String>,
    holders: Vec<Holder>,
    now_ms: u64,
    deadline_ms: u64,
}

impl HolderSearchOperation {
    pub fn new(route: ToolRoute) -> Self {
        Self {
            route,
            cursor: None,
            holders: Vec::new(),
            now_ms: 0,
            deadline_ms: 0,
        }
    }

    fn page(&mut self) -> Result<Next<HolderFound>, ToolRouteError> {
        let left = self.deadline_ms.saturating_sub(self.now_ms);
        if left == 0 {
            return Err(ToolRouteError::SearchLate);
        }
        let route = &self.route;
        let request = BucketSearchRequest {
            auth: route.auth.clone(),
            bearer_token: route.bearer.clone(),
            query: route.bucket.clone(),
            key: (!route.key.is_empty()).then(|| route.key.clone()),
            extras: None,
            limit: SEARCH_PAGE,
            group_id: None,
            cursor: self.cursor.take(),
            target_nodes: None,
            database: false,
        };
        let search = search_buckets_distributed(route.realm_id, route.here, request);
        let search = boxed_suboperation(search, |result| Event::Child(ChildOutcome::new(result)));
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members: vec![Effect::SubOperation(search)],
            limit: 1,
            form: GroupForm::All {
                deadline: Some(Duration::from_millis(left)),
            },
        })))
    }

    fn answered(
        &mut self,
        page: BucketSearchExecution,
    ) -> Result<Next<HolderFound>, ToolRouteError> {
        let stats = page.fanout_stats;
        let state = (!page.hits.is_empty(), stats.nodes_failed > 0);
        for hit in (page.hits.into_iter()).filter(|hit| hit.bucket == self.route.bucket) {
            let holder = Holder {
                node_id: hit.node_id,
                set_id: hit.set_id,
            };
            add_holder(&mut self.holders, holder);
        }
        self.cursor = page.next_cursor;
        if !search_done(!self.holders.is_empty(), state, self.cursor.is_none()) {
            return self.page();
        }
        Ok(Next::Done(HolderFound {
            holders: std::mem::take(&mut self.holders),
            missed: Missed {
                nodes: stats.failed_partitions,
                incomplete: stats.nodes_failed > 0,
            },
        }))
    }
}

impl Operation for HolderSearchOperation {
    type Output = HolderFound;
    type Error = ToolRouteError;

    fn first(&mut self) -> Result<Next<HolderFound>, ToolRouteError> {
        let limit = u64::try_from(DISCOVERY_TIMEOUT.as_millis()).unwrap_or(u64::MAX);
        self.deadline_ms = self.now_ms.saturating_add(limit);
        self.page()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<HolderFound>, ToolRouteError> {
        let unexpected = |got: &'static str| IoError::Unexpected {
            expected: "the search page",
            got,
        };
        let IoResult::Group(GroupEvent::Finished { results }) = result else {
            return Err(unexpected("another result").into());
        };
        let event = match results.into_iter().next() {
            Some(GroupResult::Done(event)) => *event,
            Some(GroupResult::Unfinished) => return Err(ToolRouteError::SearchLate),
            Some(GroupResult::Refused) | None => return Err(unexpected("no member").into()),
        };
        let outcome = match event {
            Event::Child(outcome) => outcome,
            other => return Err(unexpected(other.label()).into()),
        };
        let page = outcome.take::<BucketSearchExecution, MetadataApiError>();
        let page = page.ok_or_else(|| unexpected("another child"))?;
        self.answered(page.map_err(ToolRouteError::Search)?)
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

#[derive(Debug, PartialEq)]
enum NodeStep {
    Bucket,
    Route,
    Search,
    Members,
}

/// The node a data tool call runs on, `None` for this node.
#[derive(Debug, PartialEq)]
pub struct ToolNodeOperation {
    route: ToolRoute,
    step: NodeStep,
    local: bool,
    routed: Option<NodeId>,
    holders: Vec<Holder>,
    missed: Missed,
}

type NodeTurn = Result<Next<Option<NodeId>>, ToolRouteError>;

impl ToolNodeOperation {
    pub fn new(route: ToolRoute) -> Self {
        Self {
            route,
            step: NodeStep::Bucket,
            local: false,
            routed: None,
            holders: Vec::new(),
            missed: Missed::default(),
        }
    }

    /// A member that keeps no records of the key's range sends the call to one that does.
    fn bucket_read(&mut self, info: Option<BucketInfo>) -> NodeTurn {
        self.local = info.is_some();
        let binding = info.as_ref().and_then(|info| info.distribution.as_ref());
        let Some(binding) = binding else {
            return self.search();
        };
        self.step = NodeStep::Route;
        let route = route_read(binding, &self.route.key, false);
        Ok(Next::Call(IoCall::child(route)))
    }

    fn search(&mut self) -> NodeTurn {
        if self.route.named.is_some() || self.local {
            return self.pick(&[]);
        }
        self.step = NodeStep::Search;
        let search = HolderSearchOperation::new(self.route.clone());
        Ok(Next::Call(IoCall::child(search)))
    }

    fn found(&mut self, found: HolderFound) -> NodeTurn {
        self.holders = found.holders;
        self.missed = found.missed;
        let Some(set_id) = self.holders.first().and_then(|holder| holder.set_id) else {
            return self.pick(&[]);
        };
        self.step = NodeStep::Members;
        let members = key_holders(set_id, &self.route.key);
        Ok(Next::Call(IoCall::child(members)))
    }

    fn pick(&mut self, members: &[NodeId]) -> NodeTurn {
        let place = (self.route.named, self.local, self.routed);
        match pick_node(self.route.here, place, &self.holders, members) {
            Ok(node) => Ok(Next::Done(node)),
            Err(error) => Err(ToolRouteError::Route(error, self.missed.clone())),
        }
    }
}

impl Operation for ToolNodeOperation {
    type Output = Option<NodeId>;
    type Error = ToolRouteError;

    fn first(&mut self) -> NodeTurn {
        let route = &self.route;
        if route.named.is_some_and(|named| named != route.here) {
            return self.bucket_read(None);
        }
        let bucket = GetBucketOperation::new(route.bucket.clone());
        Ok(Next::Call(IoCall::child(bucket)))
    }

    fn next(&mut self, result: IoResult) -> NodeTurn {
        match self.step {
            NodeStep::Bucket => match result.child::<BucketInfo, GetBucketError>()? {
                Ok(info) => self.bucket_read(Some(info)),
                Err(GetBucketError::NotFound) => self.bucket_read(None),
                Err(error) => Err(ToolRouteError::Bucket(error)),
            },
            NodeStep::Route => {
                let route = result.child::<ReadRoute, GuardError>()?;
                let route = route.map_err(ToolRouteError::Guard)?;
                if let ReadRoute::Holder { node_id, .. } = route {
                    self.routed = Some(node_id);
                }
                self.search()
            }
            NodeStep::Search => self.found(result.child::<HolderFound, ToolRouteError>()??),
            NodeStep::Members => {
                let members = result.child::<Vec<NodeId>, IoError>()?;
                self.pick(&members.unwrap_or_default())
            }
        }
    }
}

#[cfg(test)]
#[path = "tool_route_tests.rs"]
mod tests;
