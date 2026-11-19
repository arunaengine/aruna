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
use ulid::Ulid;

use crate::metadata::iri_index::{MetadataIriError, ReferenceLookupOperation, ReferenceMatches};
use crate::metadata::registry_cache::{Registry, RegistryReadOperation, cache_answer};
use crate::metadata::search_cursor::{compare_hits, tie_order};
use crate::metadata::visible_scope::ReadScopeOperation;

type Turn = Result<Next<Vec<MetadataSearchHit>>, MetadataError>;

type Matches = BTreeMap<Ulid, Vec<String>>;

fn graph_call(effect: MetadataEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Metadata(effect)))
}

#[derive(Debug, PartialEq)]
enum SearchStage {
    Registry,
    References,
    Scope(Option<Matches>),
    Search,
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
        if self.limit == 0 || visible.is_empty() {
            return Ok(Next::Done(Vec::new()));
        }
        self.stage = SearchStage::Search;
        if let Some(matches) = matches.filter(|_| self.query.trim().is_empty()) {
            return Ok(Next::Call(self.describe(visible, &matches)));
        }
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

    // A reference filter names its candidates through the backlink index, never the registry.
    fn first(&mut self) -> Turn {
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
        match std::mem::replace(&mut self.stage, SearchStage::Search) {
            SearchStage::Registry => {
                let records = result.child::<Registry, MetadataError>()??;
                self.scope(&records, None)
            }
            SearchStage::References => {
                let found = result.child::<ReferenceMatches, MetadataIriError>()?;
                let (records, matches) = found?;
                self.scope(&records, Some(matches))
            }
            SearchStage::Scope(matches) => {
                let visible = result.child::<Vec<MetadataRegistryRecord>, MetadataError>()??;
                self.search(visible, matches)
            }
            SearchStage::Search => match cache_answer(result)? {
                MetadataEvent::SearchResult { mut hits } => {
                    // The realm-wide search answers in index order; the search cursor's
                    // watermark needs the score-descending order instead.
                    if self.realm_wide() {
                        hits.sort_by(compare_hits);
                    }
                    Ok(Next::Done(hits))
                }
                other => Err(MetadataError::Backend(format!(
                    "unexpected metadata search answer: {other:?}"
                ))),
            },
        }
    }
}

#[cfg(test)]
#[path = "text_search_tests.rs"]
mod call_tests;
