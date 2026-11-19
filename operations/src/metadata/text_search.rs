//! Searches the local graphs a caller may read, realm-wide or within a candidate filter.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::metadata::{
    MetadataEffect, MetadataError, MetadataEvent, MetadataSearchHit, REGISTRY_CANDIDATE_LIMIT,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::GroupId;
use tracing::{debug_span, field};
use ulid::Ulid;

use crate::metadata::iri_index::{MetadataIriError, ReferenceLookupOperation, ReferenceMatches};
use crate::metadata::read_timing::ReadTiming;
use crate::metadata::registry_cache::{Registry, RegistryReadOperation, cache_answer};
use crate::metadata::search_cursor::{compare_hits, tie_order};
use crate::metadata::visible_scope::ReadScopeOperation;

type Turn = Result<Next<Vec<MetadataSearchHit>>, MetadataError>;

type Matches = BTreeMap<Ulid, Vec<String>>;

fn graph_call(effect: MetadataEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Metadata(effect)))
}

fn unexpected(answer: &MetadataEvent) -> MetadataError {
    MetadataError::Backend(format!("unexpected metadata search answer: {answer:?}"))
}

#[derive(Debug, PartialEq)]
enum SearchStage {
    Registry,
    References,
    Scope(Option<Matches>),
    Search,
    Describe,
}

/// One text search over the local graphs a caller may read. Without a graph, group or reference
/// filter it spans the realm and orders hits by score; a filter narrows the graphs searched. A
/// reference filter with an empty query lists each matching subject instead, in tie order.
#[derive(Debug, PartialEq)]
pub struct LocalSearchOperation {
    realm_id: RealmId,
    auth: Option<AuthContext>,
    graph_iris: Option<Vec<String>>,
    group_id: Option<GroupId>,
    query: String,
    limit: usize,
    reference: Option<(String, String)>,
    stage: SearchStage,
    timing: ReadTiming,
}

impl LocalSearchOperation {
    pub fn new(
        (realm_id, auth): (RealmId, Option<AuthContext>),
        (graph_iris, group_id): (Option<Vec<String>>, Option<GroupId>),
        (query, limit): (String, usize),
        reference: Option<(String, String)>,
    ) -> Self {
        Self {
            realm_id,
            auth,
            graph_iris,
            group_id,
            query,
            limit,
            reference,
            stage: SearchStage::Registry,
            timing: ReadTiming::default(),
        }
    }

    // The read's span ends with its hit count or error, as the handle's search span did.
    fn settle(&self, turn: &Turn) {
        match turn {
            Ok(Next::Done(hits)) => {
                self.timing.note("result", "ok");
                self.timing.note("hit_count", hits.len() as u64);
                self.timing.finish(None);
            }
            Err(error) => self.timing.finish(Some(error)),
            Ok(_) => {}
        }
    }

    fn realm_wide(&self) -> bool {
        self.reference.is_none() && self.graph_iris.is_none() && self.group_id.is_none()
    }

    fn scope(&mut self, records: &[MetadataRegistryRecord], matches: Option<Matches>) -> Turn {
        let filter = (self.graph_iris.as_deref(), self.group_id);
        let caller = (self.realm_id, self.auth.clone());
        self.stage = SearchStage::Scope(matches);
        let scope = ReadScopeOperation::new(caller, records, filter);
        Ok(Next::Call(IoCall::child(scope)))
    }

    fn search(&mut self, visible: Vec<MetadataRegistryRecord>, matches: Option<Matches>) -> Turn {
        self.timing.lap("authorization_ms");
        if !self.realm_wide() {
            self.timing.note("authorized_graphs", visible.len() as u64);
        }
        if self.limit == 0 || visible.is_empty() {
            return Ok(Next::Done(Vec::new()));
        }
        if let Some(matches) = matches.filter(|_| self.query.trim().is_empty()) {
            self.stage = SearchStage::Describe;
            return Ok(Next::Call(self.describe(visible, &matches)));
        }
        self.stage = SearchStage::Search;
        Ok(Next::Call(graph_call(MetadataEffect::SearchScope {
            records: visible,
            exact: !self.realm_wide(),
            query: self.query.clone(),
            limit: self.limit,
        })))
    }

    // Every listed subject scores the same, so the page is cut in tie order before describing.
    fn describe(&self, visible: Vec<MetadataRegistryRecord>, matches: &Matches) -> IoCall {
        let mut targets = (visible.into_iter())
            .filter_map(|record| {
                let subject_iri = matches.get(&record.document_id)?.first()?.clone();
                Some((record, subject_iri))
            })
            .collect::<Vec<_>>();
        targets.sort_by(|(left, left_subject), (right, right_subject)| {
            tie_order(
                (&left.graph_iri, left_subject),
                (&right.graph_iri, right_subject),
            )
        });
        targets.truncate(self.limit);
        graph_call(MetadataEffect::DescribeHits { targets })
    }
}

impl Operation for LocalSearchOperation {
    type Output = Vec<MetadataSearchHit>;
    type Error = MetadataError;

    fn stamp(&mut self, now_ms: u64) {
        self.timing.stamp(now_ms);
    }

    // A reference filter names its candidates through the backlink index, never the registry.
    fn first(&mut self) -> Turn {
        self.timing.open(debug_span!(
            "metadata.search.local",
            query_len = self.query.len() as u64,
            limit = self.limit as u64,
            graph_filter_count = self.graph_iris.as_ref().map_or(0, Vec::len) as u64,
            registry_records = field::Empty,
            authorized_graphs = field::Empty,
            lazy = self.realm_wide(),
            registry_ms = field::Empty,
            authorization_ms = field::Empty,
            craqle_search_ms = field::Empty,
            elapsed_ms = field::Empty,
            result = field::Empty,
            hit_count = field::Empty,
        ));
        if (self.graph_iris.as_ref()).is_some_and(|graphs| graphs.len() > REGISTRY_CANDIDATE_LIMIT)
        {
            return Err(MetadataError::Backend(
                "metadata candidate limit exceeded".to_string(),
            ));
        }
        Ok(Next::Call(match &self.reference {
            Some((predicate_iri, object_iri)) => {
                self.stage = SearchStage::References;
                let iris = (predicate_iri.as_str(), object_iri.as_str());
                IoCall::child(ReferenceLookupOperation::new(
                    iris,
                    REGISTRY_CANDIDATE_LIMIT,
                ))
            }
            None => IoCall::child(RegistryReadOperation::new()),
        }))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        let turn = self.step(result);
        self.settle(&turn);
        turn
    }
}

impl LocalSearchOperation {
    fn step(&mut self, result: IoResult) -> Turn {
        match std::mem::replace(&mut self.stage, SearchStage::Search) {
            SearchStage::Registry => {
                let records = result.child::<Registry, MetadataError>()??;
                self.timing.lap("registry_ms");
                self.timing.note("registry_records", records.len() as u64);
                self.scope(&records, None)
            }
            SearchStage::References => {
                let found = result.child::<ReferenceMatches, MetadataIriError>()?;
                let (records, matches) = found?;
                self.timing.restart();
                self.scope(&records, Some(matches))
            }
            SearchStage::Scope(matches) => {
                let visible = result.child::<Vec<MetadataRegistryRecord>, MetadataError>()??;
                self.search(visible, matches)
            }
            SearchStage::Describe => match cache_answer(result)? {
                MetadataEvent::SearchResult { hits } => Ok(Next::Done(hits)),
                other => Err(unexpected(&other)),
            },
            SearchStage::Search => match self.searched(result)? {
                MetadataEvent::SearchResult { mut hits } => {
                    // The realm-wide search answers in index order; the search cursor's
                    // watermark needs the score-descending order instead.
                    if self.realm_wide() {
                        hits.sort_by(compare_hits);
                    }
                    Ok(Next::Done(hits))
                }
                other => Err(unexpected(&other)),
            },
        }
    }

    fn searched(&mut self, result: IoResult) -> Result<MetadataEvent, MetadataError> {
        self.timing.lap("craqle_search_ms");
        cache_answer(result)
    }
}

#[cfg(test)]
#[path = "text_search_tests.rs"]
mod call_tests;
