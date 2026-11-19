//! Decides which registry records a caller may read locally: their graphs are not deleted, the
//! caller's read rules or public visibility admit them, and the request policies allow the read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::keyspaces::GRAPH_LIFECYCLE_KEYSPACE;
use aruna_core::metadata::{
    GraphLifecycleRecord, MetadataEffect, MetadataError, MetadataEvent, REGISTRY_CANDIDATE_LIMIT,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{GroupId, Key};
use tracing::{debug_span, field};

use crate::metadata::api::MetadataApiError;
use crate::metadata::api::access::{ReadAccess, ReadAccessOperation};
use crate::metadata::call_results::page_rows;
use crate::metadata::registry_cache::cache_answer;
use crate::metadata::span_timing::ReadTiming;

type Turn<T> = Result<Next<T>, MetadataError>;

/// The graphs marked deleted and the number of lifecycle pages read for them.
pub(crate) type DeletedGraphs = (HashSet<String>, u64);

/// The graphs whose lifecycle row marks them deleted, in one sweep of the lifecycle keyspace.
#[derive(Debug, PartialEq, Default)]
pub(crate) struct DeletedGraphsOperation {
    deleted: HashSet<String>,
    pages: u64,
}

impl DeletedGraphsOperation {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn page(after: Option<Key>) -> Turn<DeletedGraphs> {
        Ok(Next::Call(IoCall::Page {
            space: GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: REGISTRY_CANDIDATE_LIMIT.saturating_add(1),
        }))
    }
}

impl Operation for DeletedGraphsOperation {
    type Output = DeletedGraphs;
    type Error = MetadataError;

    fn first(&mut self) -> Turn<DeletedGraphs> {
        Self::page(None)
    }

    fn next(&mut self, result: IoResult) -> Turn<DeletedGraphs> {
        let (rows, next) = page_rows(result)?;
        self.pages += 1;
        if self.deleted.len().saturating_add(rows.len()) > REGISTRY_CANDIDATE_LIMIT {
            return Err(MetadataError::Backend(
                "metadata lifecycle candidate limit exceeded".to_string(),
            ));
        }
        for (_, value) in rows {
            let record: GraphLifecycleRecord = postcard::from_bytes(&value)
                .map_err(|error| MetadataError::Backend(error.to_string()))?;
            if record.is_deleted() {
                self.deleted.insert(record.graph_iri);
            }
        }
        match next {
            Some(after) => Self::page(Some(after)),
            None => Ok(Next::Done((std::mem::take(&mut self.deleted), self.pages))),
        }
    }
}

#[derive(Debug, PartialEq)]
enum ScopeStage {
    Generation,
    Deleted(u64),
    Refresh,
    Access,
}

/// The records of `records` a caller may read here, in their registry order: a graph filter or
/// group narrows the candidates first, the lifecycle sweep removes deleted graphs and refreshes
/// their cached deletion state, and the caller's read access decides the rest.
#[derive(Debug, PartialEq)]
pub(crate) struct ReadScopeOperation {
    realm_id: RealmId,
    auth: Option<AuthContext>,
    candidates: Vec<MetadataRegistryRecord>,
    deleted: HashSet<String>,
    stage: ScopeStage,
    /// The listed records and the graph filter's length, for the selection span.
    counts: (usize, usize),
    timing: ReadTiming,
}

impl ReadScopeOperation {
    pub(crate) fn new(
        (realm_id, auth): (RealmId, Option<AuthContext>),
        records: &[MetadataRegistryRecord],
        (graph_iris, group_id): (Option<&[String]>, Option<GroupId>),
    ) -> Self {
        let graphs = graph_iris.map(|graphs| graphs.iter().collect::<HashSet<_>>());
        let candidates = (records.iter())
            .filter(|record| {
                graphs
                    .as_ref()
                    .is_none_or(|graphs| graphs.contains(&record.graph_iri))
                    && group_id.is_none_or(|group_id| record.group_id == group_id)
            })
            .cloned()
            .collect();
        Self {
            realm_id,
            auth,
            candidates,
            deleted: HashSet::new(),
            stage: ScopeStage::Generation,
            counts: (records.len(), graph_iris.map_or(0, <[String]>::len)),
            timing: ReadTiming::default(),
        }
    }

    fn refresh(&mut self, generation: u64) -> Turn<Vec<MetadataRegistryRecord>> {
        let lifecycle = (self.candidates.iter())
            .map(|record| {
                let deleted = self.deleted.contains(&record.graph_iri);
                (record.graph_iri.clone(), deleted)
            })
            .collect();
        let refresh = MetadataEffect::RefreshLifecycle {
            lifecycle,
            generation,
        };
        self.stage = ScopeStage::Refresh;
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            refresh,
        )))))
    }

    // Records how the selection decided each candidate, as the handle's selection span did.
    fn visible(&mut self, access: &ReadAccess) -> Vec<MetadataRegistryRecord> {
        let candidates = std::mem::take(&mut self.candidates);
        let evaluated = candidates.len();
        let (live, deleted): (Vec<_>, Vec<_>) =
            (candidates.into_iter()).partition(|record| !self.deleted.contains(&record.graph_iri));
        let public = live.iter().filter(|record| record.public).count();
        let private = live.len() - public;
        let visible = (live.into_iter())
            .filter(|record| access.shows(record))
            .collect::<Vec<_>>();
        let timing = &self.timing;
        timing.note("filtered_count", (self.counts.0 - evaluated) as u64);
        timing.note("deleted_count", deleted.len() as u64);
        timing.note("public_count", public as u64);
        timing.note("private_checked_count", private as u64);
        timing.note("denied_count", (public + private - visible.len()) as u64);
        timing.note("visible_count", visible.len() as u64);
        timing.finish(None);
        visible
    }
}

impl Operation for ReadScopeOperation {
    type Output = Vec<MetadataRegistryRecord>;
    type Error = MetadataError;

    fn stamp(&mut self, now_ms: u64) {
        self.timing.stamp(now_ms);
    }

    fn first(&mut self) -> Turn<Vec<MetadataRegistryRecord>> {
        self.timing.open(debug_span!(
            "metadata.authorization.select_records",
            record_count = self.counts.0 as u64,
            graph_filter_count = self.counts.1 as u64,
            visible_count = field::Empty,
            deleted_count = field::Empty,
            filtered_count = field::Empty,
            lifecycle_cache_hits = field::Empty,
            lifecycle_cache_misses = field::Empty,
            lifecycle_reads = 1u64,
            public_count = field::Empty,
            private_checked_count = field::Empty,
            denied_count = field::Empty,
            elapsed_ms = field::Empty,
        ));
        let generation = Effect::Metadata(MetadataEffect::CacheGeneration);
        Ok(Next::Call(IoCall::Effect(Box::new(generation))))
    }

    fn next(&mut self, result: IoResult) -> Turn<Vec<MetadataRegistryRecord>> {
        match std::mem::replace(&mut self.stage, ScopeStage::Access) {
            ScopeStage::Generation => match cache_answer(result)? {
                MetadataEvent::CacheGeneration { generation } => {
                    self.stage = ScopeStage::Deleted(generation);
                    Ok(Next::Call(IoCall::child(DeletedGraphsOperation::new())))
                }
                other => Err(MetadataError::Backend(format!(
                    "unexpected metadata cache answer: {other:?}"
                ))),
            },
            ScopeStage::Deleted(generation) => {
                (self.deleted, _) = result.child::<DeletedGraphs, MetadataError>()??;
                self.refresh(generation)
            }
            // A refused refresh only leaves the cache to the next fill.
            ScopeStage::Refresh => {
                let accepted = match cache_answer(result)? {
                    MetadataEvent::RegistryStored { accepted } => accepted,
                    _ => false,
                };
                let evaluated = self.candidates.len() as u64;
                let (hits, misses) = if accepted {
                    (evaluated, 0)
                } else {
                    (0, evaluated)
                };
                self.timing.note("lifecycle_cache_hits", hits);
                self.timing.note("lifecycle_cache_misses", misses);
                let auth = self.auth.clone();
                let access = ReadAccessOperation::new(self.realm_id, auth, &self.candidates);
                Ok(Next::Call(IoCall::child(access)))
            }
            ScopeStage::Access => {
                let access = result.child::<ReadAccess, MetadataApiError>()?;
                let access = access.map_err(|error| MetadataError::Backend(error.to_string()))?;
                Ok(Next::Done(self.visible(&access)))
            }
        }
    }
}

#[cfg(test)]
#[path = "visible_scope_tests.rs"]
pub(crate) mod call_tests;
