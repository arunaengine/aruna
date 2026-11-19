//! Runs one SPARQL query over the local graphs a caller may read, through the query cache.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::metadata::{
    MetadataEffect, MetadataError, MetadataEvent, MetadataQueryResults, QUERY_PREFIXES,
    REGISTRY_CANDIDATE_LIMIT,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use spargebra::{Query, SparqlParser};

use crate::metadata::api::fanout_types::pattern_contains_service;
use crate::metadata::query_cache::{LocalScopeKind, ScopeDigest, graphs_digest, local_key};
use crate::metadata::registry_cache::{Registry, RegistryReadOperation, cache_answer};
use crate::metadata::visible_scope::ReadScopeOperation;

/// The longest SPARQL text a metadata query may have.
pub(crate) const QUERY_MAX_BYTES: usize = 64 * 1024;

type Turn = Result<Next<MetadataQueryResults>, MetadataError>;

fn graph_call(effect: MetadataEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Metadata(effect)))
}

fn unexpected(answer: &MetadataEvent) -> MetadataError {
    MetadataError::Backend(format!("unexpected metadata query answer: {answer:?}"))
}

/// Parses a read-only metadata query: SELECT or ASK, within the size limit, without SERVICE.
pub(crate) fn parse_metadata_query(sparql: &str) -> Result<Query, MetadataError> {
    if sparql.len() > QUERY_MAX_BYTES {
        return Err(MetadataError::InvalidInput(format!(
            "SPARQL query exceeds the {QUERY_MAX_BYTES}-byte limit"
        )));
    }
    let query = SparqlParser::new()
        .parse_query(&format!("{QUERY_PREFIXES}{sparql}"))
        .map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
    let pattern = match &query {
        Query::Select { pattern, .. } | Query::Ask { pattern, .. } => pattern,
        Query::Construct { .. } | Query::Describe { .. } => {
            return Err(MetadataError::InvalidInput(
                "only SELECT and ASK metadata queries are supported".to_string(),
            ));
        }
    };
    if pattern_contains_service(pattern) {
        return Err(MetadataError::InvalidInput(
            "SERVICE is not supported in metadata queries".to_string(),
        ));
    }
    Ok(query)
}

#[derive(Debug, PartialEq)]
enum QueryStage {
    Stamp,
    Registry,
    Scope,
    Cached(Vec<String>),
    Query,
    Store(MetadataQueryResults),
}

/// One SPARQL query over the local graphs a caller may read. A graph filter must name only
/// readable graphs and is queried exactly; without one, every readable graph the store holds is
/// queried. Results are shared through the query cache between callers of the same scope.
#[derive(Debug, PartialEq)]
pub struct LocalQueryOperation {
    realm_id: RealmId,
    auth: Option<AuthContext>,
    graph_iris: Option<Vec<String>>,
    sparql: String,
    stamp: [u64; 2],
    key: [u8; 32],
    stage: QueryStage,
}

impl LocalQueryOperation {
    pub fn new(
        (realm_id, auth): (RealmId, Option<AuthContext>),
        graph_iris: Option<Vec<String>>,
        sparql: String,
    ) -> Self {
        Self {
            realm_id,
            auth,
            graph_iris,
            sparql,
            stamp: [0; 2],
            key: [0; 32],
            stage: QueryStage::Stamp,
        }
    }

    // Callers whose authorization resolved to the same graphs share one cache entry.
    fn lookup(&mut self, visible: Vec<MetadataRegistryRecord>) -> Turn {
        let graphs = (visible.into_iter())
            .map(|record| record.graph_iri)
            .collect::<Vec<_>>();
        let key = match &self.graph_iris {
            Some(requested) => {
                if graphs.len() != requested.iter().collect::<HashSet<_>>().len() {
                    return Err(MetadataError::GraphNotFound);
                }
                local_key(LocalScopeKind::Eager, &graphs_digest(&graphs), &self.sparql)
            }
            None => {
                let mut digest = ScopeDigest::default();
                graphs.iter().for_each(|graph_iri| digest.push(graph_iri));
                local_key(LocalScopeKind::Lazy, &digest.finish(), &self.sparql)
            }
        };
        self.key = key.bytes();
        self.stage = QueryStage::Cached(graphs);
        Ok(Next::Call(graph_call(MetadataEffect::QueryCacheGet {
            key: self.key,
        })))
    }

    fn query(&mut self, graph_iris: Vec<String>) -> Turn {
        self.stage = QueryStage::Query;
        Ok(Next::Call(graph_call(MetadataEffect::QueryScope {
            graph_iris,
            exact: self.graph_iris.is_some(),
            sparql: self.sparql.clone(),
        })))
    }
}

impl Operation for LocalQueryOperation {
    type Output = MetadataQueryResults;
    type Error = MetadataError;

    // The stamp is taken before any read, so a change racing this query keeps its result out
    // of the cache.
    fn first(&mut self) -> Turn {
        parse_metadata_query(&self.sparql)?;
        if (self.graph_iris.as_ref()).is_some_and(|graphs| graphs.len() > REGISTRY_CANDIDATE_LIMIT)
        {
            return Err(MetadataError::Backend(
                "metadata candidate limit exceeded".to_string(),
            ));
        }
        Ok(Next::Call(graph_call(MetadataEffect::QueryCacheStamp)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::replace(&mut self.stage, QueryStage::Query) {
            QueryStage::Stamp => match cache_answer(result)? {
                MetadataEvent::QueryCacheEntry { stamp, .. } => {
                    self.stamp = stamp;
                    self.stage = QueryStage::Registry;
                    Ok(Next::Call(IoCall::child(RegistryReadOperation::new())))
                }
                other => Err(unexpected(&other)),
            },
            QueryStage::Registry => {
                let records = result.child::<Registry, MetadataError>()??;
                let filter = (self.graph_iris.as_deref(), None);
                let caller = (self.realm_id, self.auth.clone());
                let scope = ReadScopeOperation::new(caller, &records, filter);
                self.stage = QueryStage::Scope;
                Ok(Next::Call(IoCall::child(scope)))
            }
            QueryStage::Scope => {
                let visible = result.child::<Vec<MetadataRegistryRecord>, MetadataError>()??;
                self.lookup(visible)
            }
            QueryStage::Cached(graph_iris) => match cache_answer(result)? {
                MetadataEvent::QueryCacheEntry {
                    entry: Some((results, _)),
                    ..
                } => Ok(Next::Done(results)),
                MetadataEvent::QueryCacheEntry { entry: None, .. } => self.query(graph_iris),
                other => Err(unexpected(&other)),
            },
            QueryStage::Query => match cache_answer(result)? {
                MetadataEvent::QueryResult { results } => {
                    let store = MetadataEffect::QueryCachePut {
                        key: self.key,
                        stamp: self.stamp,
                        results: results.clone(),
                        nodes_queried: 0,
                    };
                    self.stage = QueryStage::Store(results);
                    Ok(Next::Call(graph_call(store)))
                }
                other => Err(unexpected(&other)),
            },
            // A refused store only leaves the result out of the cache.
            QueryStage::Store(results) => Ok(Next::Done(results)),
        }
    }
}

#[cfg(test)]
#[path = "local_query_tests.rs"]
mod call_tests;
