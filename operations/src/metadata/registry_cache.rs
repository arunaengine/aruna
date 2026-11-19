//! Reads this node's live registry records through the graph store's registry cache, filling
//! the cache from the registry and lifecycle keyspaces when it holds none.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::keyspaces::METADATA_INDEX_KEYSPACE;
use aruna_core::metadata::{
    MetadataEffect, MetadataError, MetadataEvent, REGISTRY_CANDIDATE_LIMIT,
};
use aruna_core::operation::{HoldScope, Operation};
use aruna_core::rows::event;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{Key, Value};

use crate::metadata::call_results::page_rows;
use crate::metadata::visible_scope::DeletedGraphsOperation;

/// The live registry records of this node, ordered by document id.
pub type Registry = Arc<Vec<MetadataRegistryRecord>>;

type Turn<T> = Result<Next<T>, MetadataError>;

fn graph_call(effect: MetadataEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Metadata(effect)))
}

/// The graph store's answer to a cache effect; an error answer is the effect's failure.
pub(crate) fn cache_answer(result: IoResult) -> Result<MetadataEvent, MetadataError> {
    match event(result)? {
        Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error),
        Event::Metadata(answer) => Ok(answer),
        other => Err(MetadataError::Backend(format!(
            "unexpected metadata cache answer: {}",
            other.label()
        ))),
    }
}

fn unexpected(answer: &MetadataEvent) -> MetadataError {
    MetadataError::Backend(format!("unexpected metadata cache answer: {answer:?}"))
}

fn limit_exceeded() -> MetadataError {
    MetadataError::Backend("metadata candidate limit exceeded".to_string())
}

#[derive(Debug, PartialEq)]
enum FillStage {
    Snapshot,
    Registry,
    Lifecycle,
    Store(Registry),
}

/// Reads the registry and the graph deletion states in one sweep each and keeps them in the
/// registry cache, unless the cache changed meanwhile. Fills never overlap on this node, and a
/// fill that finds a fresh snapshot after waiting answers with it.
#[derive(Debug, PartialEq, Default)]
pub(crate) struct RegistryFillOperation {
    generation: u64,
    records: Vec<MetadataRegistryRecord>,
    stage: Option<FillStage>,
}

impl RegistryFillOperation {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn registry_page(&mut self, after: Option<Key>) -> Turn<Registry> {
        self.stage = Some(FillStage::Registry);
        let limit = (REGISTRY_CANDIDATE_LIMIT.saturating_sub(self.records.len())).saturating_add(1);
        Ok(Next::Call(IoCall::Page {
            space: METADATA_INDEX_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit,
        }))
    }

    fn registry_read(&mut self, rows: Vec<(Key, Value)>, next: Option<Key>) -> Turn<Registry> {
        if self.records.len().saturating_add(rows.len()) > REGISTRY_CANDIDATE_LIMIT {
            return Err(limit_exceeded());
        }
        for (_, value) in rows {
            let record =
                postcard::from_bytes::<MetadataRegistryRecord>(&value).map_err(|error| {
                    MetadataError::Backend(format!("metadata registry iteration failed: {error}"))
                })?;
            self.records.push(record);
        }
        match next {
            Some(after) => self.registry_page(Some(after)),
            None => {
                self.stage = Some(FillStage::Lifecycle);
                Ok(Next::Call(IoCall::child(DeletedGraphsOperation::new())))
            }
        }
    }

    // Lifecycle rows are deletion tombstones, so one sweep refreshes the deletion state of every
    // registry graph; snapshot readers binary-search the records by document id.
    fn store(&mut self, deleted: &HashSet<String>) -> Turn<Registry> {
        let mut records = std::mem::take(&mut self.records);
        records.sort_unstable_by_key(|record| record.document_id);
        let lifecycle = (records.iter())
            .map(|record| {
                (
                    record.graph_iri.clone(),
                    deleted.contains(&record.graph_iri),
                )
            })
            .collect();
        records.retain(|record| !deleted.contains(&record.graph_iri));
        let registry = Arc::new(records);
        let store = MetadataEffect::StoreRegistry {
            records: registry.as_ref().clone(),
            lifecycle,
            generation: self.generation,
        };
        self.stage = Some(FillStage::Store(registry));
        Ok(Next::Call(graph_call(store)))
    }
}

impl Operation for RegistryFillOperation {
    type Output = Registry;
    type Error = MetadataError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::RegistryFill)
    }

    fn first(&mut self) -> Turn<Registry> {
        self.stage = Some(FillStage::Snapshot);
        Ok(Next::Call(graph_call(MetadataEffect::RegistrySnapshot)))
    }

    fn next(&mut self, result: IoResult) -> Turn<Registry> {
        match self.stage.take() {
            Some(FillStage::Snapshot) => match cache_answer(result)? {
                MetadataEvent::RegistrySnapshot {
                    records: Some(records),
                    fresh: true,
                    ..
                } => Ok(Next::Done(records)),
                MetadataEvent::RegistrySnapshot { generation, .. } => {
                    self.generation = generation;
                    self.registry_page(None)
                }
                other => Err(unexpected(&other)),
            },
            Some(FillStage::Registry) => {
                let (rows, next) = page_rows(result)?;
                self.registry_read(rows, next)
            }
            Some(FillStage::Lifecycle) => {
                let deleted = result.child::<HashSet<String>, MetadataError>()??;
                self.store(&deleted)
            }
            // A fill the cache refused still answers with what it read.
            Some(FillStage::Store(registry)) => match cache_answer(result)? {
                MetadataEvent::RegistryStored { .. } => Ok(Next::Done(registry)),
                other => Err(unexpected(&other)),
            },
            None => Err(MetadataError::Backend("registry fill not started".into())),
        }
    }
}

#[derive(Debug, PartialEq)]
enum ReadStage {
    Snapshot,
    Fill,
}

/// This node's live registry records from the registry cache. A fresh snapshot answers at once;
/// an expired one answers too and is refilled after the answer; without one the cache is filled
/// first.
#[derive(Debug, PartialEq)]
pub struct RegistryReadOperation {
    stage: ReadStage,
}

impl RegistryReadOperation {
    pub fn new() -> Self {
        Self {
            stage: ReadStage::Snapshot,
        }
    }
}

impl Operation for RegistryReadOperation {
    type Output = Registry;
    type Error = MetadataError;

    fn first(&mut self) -> Turn<Registry> {
        Ok(Next::Call(graph_call(MetadataEffect::RegistrySnapshot)))
    }

    fn next(&mut self, result: IoResult) -> Turn<Registry> {
        if self.stage == ReadStage::Fill {
            return Ok(Next::Done(result.child::<Registry, MetadataError>()??));
        }
        match cache_answer(result)? {
            MetadataEvent::RegistrySnapshot {
                records: Some(records),
                fresh,
                ..
            } => match fresh {
                true => Ok(Next::Done(records)),
                false => Ok(Next::Then {
                    output: records,
                    calls: vec![IoCall::child(RegistryFillOperation::new())],
                }),
            },
            MetadataEvent::RegistrySnapshot { records: None, .. } => {
                self.stage = ReadStage::Fill;
                Ok(Next::Call(IoCall::child(RegistryFillOperation::new())))
            }
            other => Err(unexpected(&other)),
        }
    }
}

#[cfg(test)]
#[path = "registry_cache_tests.rs"]
mod call_tests;
