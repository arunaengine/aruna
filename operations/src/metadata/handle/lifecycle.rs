//! Guards effects against deleted graphs and keeps the visibility and registry caches fresh.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::sync::sync_graph_once;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{GRAPH_LIFECYCLE_KEYSPACE, METADATA_INDEX_KEYSPACE};
use aruna_core::metadata::{GraphLifecycleRecord, MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::telemetry::record_elapsed_ms;
use aruna_core::types::GroupId;
use aruna_graph::GraphHandle;
use tokio::time::sleep;
use tracing::{Span, field, warn};
use ulid::Ulid;

use super::{
    GRAPH_SYNC_AFTER, GRAPH_SYNC_ATTEMPTS, LifecycleDeletedEntry, MetadataHandle, MetadataInner,
    MetadataVisibilityCache, REGISTRY_CANDIDATE_LIMIT, RegistryCacheEntry, VISIBILITY_CACHE_TTL,
    VisibilityFillResult, handles, metadata_graph_fence,
};
use crate::driver::{DriverContext, drive};
use crate::metadata::graph_deleted::GraphDeletedOperation;
use crate::metadata::repository::parse_registry_iter;

impl MetadataVisibilityCache {
    pub(super) fn new() -> Self {
        Self {
            registry: Mutex::new(None),
            registry_fill: Arc::new(tokio::sync::Mutex::new(())),
            lifecycle_deleted: Mutex::new(HashMap::new()),
            generation: AtomicU64::new(0),
        }
    }

    pub(super) fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub(super) fn advance_generation(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    pub(super) fn registry_records(&self) -> Option<Arc<Vec<MetadataRegistryRecord>>> {
        match self.registry_records_any() {
            Some((records, true)) => Some(records),
            _ => None,
        }
    }

    // Expired entries are kept so readers can be served stale data while a
    // background refill replaces the entry; the bool flags freshness.
    pub(super) fn registry_records_any(&self) -> Option<(Arc<Vec<MetadataRegistryRecord>>, bool)> {
        let now = Instant::now();
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        registry.as_mut().and_then(|entry| {
            entry
                .snapshot()
                .map(|records| (records, entry.expires_at > now))
        })
    }

    pub(super) fn records_group_any(
        &self,
        group_id: GroupId,
    ) -> Option<(Arc<Vec<MetadataRegistryRecord>>, bool)> {
        let now = Instant::now();
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        registry.as_mut().and_then(|entry| {
            entry
                .group_snapshot(group_id)
                .map(|records| (records, entry.expires_at > now))
        })
    }

    #[cfg(test)]
    pub(super) fn store_registry_records(&self, records: Arc<Vec<MetadataRegistryRecord>>) {
        let map: BTreeMap<_, _> = records
            .iter()
            .map(|record| (record.document_id, record.clone()))
            .collect();
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        *registry = Some(RegistryCacheEntry {
            records: map,
            snapshot: Some(records),
            group_snapshots: HashMap::new(),
            expires_at: Instant::now() + VISIBILITY_CACHE_TTL,
        });
    }

    pub(super) fn store_visibility_fill(
        &self,
        records: Arc<Vec<MetadataRegistryRecord>>,
        lifecycle_entries: Vec<(String, bool)>,
        fill_generation: u64,
    ) -> bool {
        if records.len() > REGISTRY_CANDIDATE_LIMIT
            || lifecycle_entries.len() > REGISTRY_CANDIDATE_LIMIT
        {
            return false;
        }
        if self.current_generation() != fill_generation {
            return false;
        }
        let map: BTreeMap<_, _> = records
            .iter()
            .map(|record| (record.document_id, record.clone()))
            .collect();
        let now = Instant::now();
        let expires_at = now + VISIBILITY_CACHE_TTL;
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        let mut lifecycle = self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        if self.current_generation() != fill_generation {
            return false;
        }
        let protected = lifecycle_entries
            .iter()
            .map(|(graph_iri, _)| graph_iri.clone())
            .collect::<HashSet<_>>();
        Self::trim_lifecycle_deleted(&mut lifecycle, &protected, now);
        for (graph_iri, deleted) in lifecycle_entries {
            lifecycle.insert(
                graph_iri,
                LifecycleDeletedEntry {
                    deleted,
                    expires_at,
                },
            );
        }
        lifecycle.retain(|_, entry| entry.expires_at > now);
        *registry = Some(RegistryCacheEntry {
            records: map,
            snapshot: Some(records),
            group_snapshots: HashMap::new(),
            expires_at,
        });
        true
    }
    #[cfg(test)]
    pub(super) fn lifecycle_deleted(&self, graph_iri: &str) -> Option<bool> {
        match self.lifecycle_deleted_any(graph_iri) {
            Some((deleted, true)) => Some(deleted),
            _ => None,
        }
    }

    pub(super) fn lifecycle_deleted_any(&self, graph_iri: &str) -> Option<(bool, bool)> {
        let now = Instant::now();
        let lifecycle = self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        lifecycle
            .get(graph_iri)
            .map(|entry| (entry.deleted, entry.expires_at > now))
    }

    pub(super) fn store_lifecycle_deleted(&self, graph_iri: String, deleted: bool) {
        let mut lifecycle = self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        self.advance_generation();
        let now = Instant::now();
        let protected = HashSet::from([graph_iri.clone()]);
        Self::trim_lifecycle_deleted(&mut lifecycle, &protected, now);
        lifecycle.insert(
            graph_iri,
            LifecycleDeletedEntry {
                deleted,
                expires_at: now + VISIBILITY_CACHE_TTL,
            },
        );
    }

    // Bulk refresh after a registry fill: re-stamps every supplied graph and drops expired
    // leftovers (graphs no longer in the registry) so the map stays bounded.
    #[cfg(test)]
    pub(super) fn refresh_lifecycle_deleted(
        &self,
        entries: impl IntoIterator<Item = (String, bool)>,
    ) {
        let entries = entries.into_iter().collect::<Vec<_>>();
        if entries.len() > REGISTRY_CANDIDATE_LIMIT {
            return;
        }
        let now = Instant::now();
        let expires_at = now + VISIBILITY_CACHE_TTL;
        let mut lifecycle = self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        let protected = entries
            .iter()
            .map(|(graph_iri, _)| graph_iri.clone())
            .collect::<HashSet<_>>();
        Self::trim_lifecycle_deleted(&mut lifecycle, &protected, now);
        for (graph_iri, deleted) in entries {
            lifecycle.insert(
                graph_iri,
                LifecycleDeletedEntry {
                    deleted,
                    expires_at,
                },
            );
        }
        lifecycle.retain(|_, entry| entry.expires_at > now);
    }

    pub(super) fn trim_lifecycle_deleted(
        lifecycle: &mut HashMap<String, LifecycleDeletedEntry>,
        protected: &HashSet<String>,
        now: Instant,
    ) {
        lifecycle.retain(|_, entry| entry.expires_at > now);
        let protected_count = lifecycle
            .keys()
            .filter(|graph_iri| protected.contains(*graph_iri))
            .count();
        let available = REGISTRY_CANDIDATE_LIMIT.saturating_sub(protected.len());
        let remove_count = lifecycle
            .len()
            .saturating_sub(protected_count)
            .saturating_sub(available);
        let evicted = lifecycle
            .keys()
            .filter(|graph_iri| !protected.contains(*graph_iri))
            .take(remove_count)
            .cloned()
            .collect::<Vec<_>>();
        for graph_iri in evicted {
            lifecycle.remove(&graph_iri);
        }
    }

    // Incremental maintenance keeps the cached registry usable under writes;
    pub(super) fn upsert_registry_records(&self, updates: &[MetadataRegistryRecord]) {
        self.upsert_at(updates, None);
    }

    pub(super) fn upsert_at(
        &self,
        updates: &[MetadataRegistryRecord],
        expected_generation: Option<u64>,
    ) {
        if updates.is_empty() {
            return;
        }
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        if expected_generation.is_some_and(|generation| self.current_generation() != generation) {
            return;
        }
        self.advance_generation();
        let Some(entry) = registry.as_mut() else {
            return;
        };
        let new_records = updates
            .iter()
            .filter(|update| !entry.records.contains_key(&update.document_id))
            .count();
        if entry.records.len().saturating_add(new_records) > REGISTRY_CANDIDATE_LIMIT {
            *registry = None;
            return;
        }
        let mut touched_groups = HashSet::new();
        for update in updates {
            entry.records.insert(update.document_id, update.clone());
            touched_groups.insert(update.group_id);
        }
        for group_id in touched_groups {
            entry.group_snapshots.remove(&group_id);
        }
        entry.snapshot = None;
    }

    pub(super) fn remove_registry_record(&self, document_id: Ulid) {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        self.advance_generation();
        let Some(entry) = registry.as_mut() else {
            return;
        };
        if let Some(removed) = entry.records.remove(&document_id) {
            entry.group_snapshots.remove(&removed.group_id);
            entry.snapshot = None;
        }
    }

    pub(super) fn remove_graph_records(&self, graph_iri: &str) {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        self.advance_generation();
        let Some(entry) = registry.as_mut() else {
            return;
        };
        let removed_groups = entry
            .records
            .values()
            .filter(|record| record.graph_iri == graph_iri)
            .map(|record| record.group_id)
            .collect::<HashSet<_>>();
        if removed_groups.is_empty() {
            return;
        }
        entry
            .records
            .retain(|_, record| record.graph_iri != graph_iri);
        for group_id in removed_groups {
            entry.group_snapshots.remove(&group_id);
        }
        entry.snapshot = None;
    }

    pub(super) fn remove_lifecycle_entry(&self, graph_iri: &str) {
        let mut lifecycle = self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        self.advance_generation();
        lifecycle.remove(graph_iri);
    }

    pub(super) fn expire_now(&self) {
        let expired = Instant::now() - Duration::from_secs(1);
        if let Some(entry) = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .as_mut()
        {
            entry.expires_at = expired;
        }
        for entry in self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .values_mut()
        {
            entry.expires_at = expired;
        }
    }
}

/// Removes the local copy of `graph_iri` when its lifecycle record says it is deleted.
pub(crate) async fn prune_if_deleted(
    context: &DriverContext,
    graph_iri: String,
) -> Result<bool, MetadataError> {
    let (metadata, graph) = handles(context)?;
    let _graph_fence = metadata_graph_fence(&graph_iri)
        .acquire()
        .await
        .map_err(|error| {
            MetadataError::Backend(format!("metadata graph fence unavailable: {error}"))
        })?;
    if !drive(GraphDeletedOperation::new(&graph_iri), context).await? {
        return Ok(false);
    }
    metadata
        .inner
        .visibility_cache
        .remove_graph_records(&graph_iri);
    metadata
        .inner
        .visibility_cache
        .store_lifecycle_deleted(graph_iri.clone(), true);
    graph.store_lifecycle(graph_iri.clone(), true);
    if !graph.contains_graph(graph_iri.clone()).await? {
        return Ok(true);
    }
    graph.delete_graph(graph_iri).await?;
    Ok(true)
}

impl MetadataHandle {
    /// Applies document sync updates from peers; returns how many graphs changed.
    pub(crate) async fn reconcile_document_sync(
        &self,
        context: &DriverContext,
    ) -> Result<usize, MetadataError> {
        let graph = context
            .graph_handle
            .as_ref()
            .ok_or(MetadataError::HandleMissing)?;
        Ok(graph.reconcile().await?.len())
    }

    /// Removes the local copy of every deleted graph; returns how many were removed.
    pub(crate) async fn prune_deleted_graphs(
        &self,
        context: &DriverContext,
    ) -> Result<usize, MetadataError> {
        let graph = context
            .graph_handle
            .as_ref()
            .ok_or(MetadataError::HandleMissing)?;
        let mut pruned = 0usize;
        for graph_iri in graph.graphs().await? {
            if prune_if_deleted(context, graph_iri).await? {
                pruned += 1;
            }
        }
        Ok(pruned)
    }
}

pub(super) fn sync_best_effort(
    context: &DriverContext,
    graph_iri: String,
    mut peers: Vec<NodeId>,
) -> MetadataEvent {
    if let Some(net_handle) = context.net_handle.as_ref() {
        peers.retain(|peer| *peer != net_handle.node_id());
    }
    peers.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    peers.dedup();
    if peers.is_empty() {
        return MetadataEvent::GraphSyncScheduled { graph_iri, peers };
    }

    let context = context.clone();
    let task_graph_iri = graph_iri.clone();
    let task_peers = peers.clone();
    tokio::spawn(async move {
        for attempt in 1..=GRAPH_SYNC_ATTEMPTS {
            match sync_graph_once(&context, task_graph_iri.clone(), task_peers.clone()).await {
                Ok(()) => return,
                Err(error) => {
                    warn!(
                        graph_iri = %task_graph_iri,
                        attempt,
                        attempts = GRAPH_SYNC_ATTEMPTS,
                        error = ?error,
                        "Metadata graph sync attempt failed"
                    );
                    if attempt < GRAPH_SYNC_ATTEMPTS {
                        sleep(GRAPH_SYNC_AFTER).await;
                    }
                }
            }
        }

        warn!(
            graph_iri = %task_graph_iri,
            peer_count = task_peers.len(),
            "Metadata graph sync retries exhausted"
        );
    });

    MetadataEvent::GraphSyncScheduled { graph_iri, peers }
}

pub(super) async fn metadata_graph_deleted(
    context: &DriverContext,
    graph_iri: &str,
) -> Result<bool, MetadataError> {
    let (metadata, graph) = handles(context)?;
    let inner = &metadata.inner;
    if let Some((true, _)) = inner.visibility_cache.lifecycle_deleted_any(graph_iri) {
        return Ok(true);
    }

    let deleted = drive(GraphDeletedOperation::new(graph_iri), context).await?;
    inner
        .visibility_cache
        .store_lifecycle_deleted(graph_iri.to_string(), deleted);
    graph.store_lifecycle(graph_iri.to_string(), deleted);
    Ok(deleted)
}

pub(super) fn effect_rejects_deleted(effect: &MetadataEffect) -> bool {
    matches!(
        effect,
        MetadataEffect::ValidateCreateCrate { .. }
            | MetadataEffect::ValidateRoCrate { .. }
            | MetadataEffect::CreateCrate { .. }
            | MetadataEffect::ApplyRoCrate { .. }
            | MetadataEffect::UpsertDataEntity { .. }
            | MetadataEffect::UpsertContextualEntity { .. }
            | MetadataEffect::SetGraphPolicy { .. }
            | MetadataEffect::AddGraphPeer { .. }
            | MetadataEffect::GetGraphPolicy { .. }
            | MetadataEffect::ExportRoCrate { .. }
            | MetadataEffect::ExportRoCrateSummary { .. }
            | MetadataEffect::ExportSummaryAt { .. }
            | MetadataEffect::DescribeTitle { .. }
            | MetadataEffect::ExportRoCratePage { .. }
            | MetadataEffect::PlanBatch { .. }
            | MetadataEffect::MergeBatch { .. }
    )
}

#[tracing::instrument(
    name = "metadata.registry.list_local",
    level = "debug",
    skip(inner, graph),
    fields(
        cache_hit = field::Empty,
        stale = field::Empty,
        record_count = field::Empty,
        elapsed_ms = field::Empty,
    )
)]
pub(super) async fn list_local_records(
    inner: Arc<MetadataInner>,
    graph: &GraphHandle,
) -> Result<Arc<Vec<MetadataRegistryRecord>>, MetadataError> {
    let started = Instant::now();
    let span = Span::current();
    match inner.visibility_cache.registry_records_any() {
        Some((records, true)) => {
            span.record("cache_hit", true);
            span.record("stale", false);
            span.record("record_count", records.len() as u64);
            record_elapsed_ms(&span, "elapsed_ms", started);
            Ok(records)
        }
        Some((records, false)) => {
            // Serve the expired snapshot and refresh in the background so an
            // expiry never blocks reads on a refill.
            span.record("cache_hit", true);
            span.record("stale", true);
            span.record("record_count", records.len() as u64);
            record_elapsed_ms(&span, "elapsed_ms", started);
            spawn_visibility_refill(&inner, graph);
            Ok(records)
        }
        None => {
            let _fill = inner
                .visibility_cache
                .registry_fill
                .clone()
                .lock_owned()
                .await;
            if let Some((records, true)) = inner.visibility_cache.registry_records_any() {
                span.record("cache_hit", true);
                span.record("stale", false);
                span.record("record_count", records.len() as u64);
                record_elapsed_ms(&span, "elapsed_ms", started);
                return Ok(records);
            }
            span.record("cache_hit", false);
            let fill = fill_visibility_caches(&inner, graph).await?;
            let records = fill.records;
            span.record("record_count", records.len() as u64);
            record_elapsed_ms(&span, "elapsed_ms", started);
            Ok(records)
        }
    }
}

#[tracing::instrument(
    name = "metadata.registry.list_local_group",
    level = "debug",
    skip(inner, graph),
    fields(
        group_id = %group_id,
        cache_hit = field::Empty,
        stale = field::Empty,
        record_count = field::Empty,
        elapsed_ms = field::Empty,
    )
)]
pub(super) async fn list_local_group(
    inner: Arc<MetadataInner>,
    graph: &GraphHandle,
    group_id: GroupId,
) -> Result<Arc<Vec<MetadataRegistryRecord>>, MetadataError> {
    let started = Instant::now();
    let span = Span::current();
    match inner.visibility_cache.records_group_any(group_id) {
        Some((records, true)) => {
            span.record("cache_hit", true);
            span.record("stale", false);
            span.record("record_count", records.len() as u64);
            record_elapsed_ms(&span, "elapsed_ms", started);
            Ok(records)
        }
        Some((records, false)) => {
            // Serve the expired snapshot and refresh in the background so an
            // expiry never blocks reads on a refill.
            span.record("cache_hit", true);
            span.record("stale", true);
            span.record("record_count", records.len() as u64);
            record_elapsed_ms(&span, "elapsed_ms", started);
            spawn_visibility_refill(&inner, graph);
            Ok(records)
        }
        None => {
            let _fill = inner
                .visibility_cache
                .registry_fill
                .clone()
                .lock_owned()
                .await;
            if let Some((records, true)) = inner.visibility_cache.records_group_any(group_id) {
                span.record("cache_hit", true);
                span.record("stale", false);
                span.record("record_count", records.len() as u64);
                record_elapsed_ms(&span, "elapsed_ms", started);
                return Ok(records);
            }
            span.record("cache_hit", false);
            let fill = fill_visibility_caches(&inner, graph).await?;
            let records = if fill.store_accepted {
                inner
                    .visibility_cache
                    .records_group_any(group_id)
                    .map(|(records, _)| records)
                    .unwrap_or_else(|| records_for_group(&fill.records, group_id))
            } else {
                records_for_group(&fill.records, group_id)
            };
            span.record("record_count", records.len() as u64);
            record_elapsed_ms(&span, "elapsed_ms", started);
            Ok(records)
        }
    }
}

fn spawn_visibility_refill(inner: &Arc<MetadataInner>, graph: &GraphHandle) {
    let fill_lock = inner.visibility_cache.registry_fill.clone();
    let inner = inner.clone();
    let graph = graph.clone();
    tokio::spawn(async move {
        let Ok(_guard) = fill_lock.try_lock_owned() else {
            return;
        };
        if let Some((_, true)) = inner.visibility_cache.registry_records_any() {
            return;
        }
        if let Err(error) = fill_visibility_caches(&inner, &graph).await {
            warn!(error = %error, "Background visibility refill failed");
        }
    });
}

pub(super) fn records_for_group(
    records: &Arc<Vec<MetadataRegistryRecord>>,
    group_id: GroupId,
) -> Arc<Vec<MetadataRegistryRecord>> {
    Arc::new(
        records
            .iter()
            .filter(|record| record.group_id == group_id)
            .cloned()
            .collect(),
    )
}

#[tracing::instrument(
    name = "metadata.visibility.fill",
    level = "debug",
    skip(inner, graph),
    fields(
        registry_pages = field::Empty,
        lifecycle_pages = field::Empty,
        record_count = field::Empty,
        deleted_count = field::Empty,
        store_accepted = field::Empty,
        elapsed_ms = field::Empty,
    )
)]
pub(super) async fn fill_visibility_caches(
    inner: &Arc<MetadataInner>,
    graph: &GraphHandle,
) -> Result<VisibilityFillResult, MetadataError> {
    let started = Instant::now();
    let span = Span::current();
    let fill_generation = inner.visibility_cache.current_generation();

    let mut records = Vec::new();
    let mut start_after = None;
    let mut registry_pages = 0usize;
    loop {
        let event = inner
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Iter {
                key_space: METADATA_INDEX_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.map(IterStart::After),
                limit: REGISTRY_CANDIDATE_LIMIT
                    .saturating_sub(records.len())
                    .saturating_add(1),
                txn_id: None,
            }))
            .await;
        let (mut page, next_start_after) = parse_registry_iter(event).map_err(|error| {
            MetadataError::Backend(format!("metadata registry iteration failed: {error:?}"))
        })?;
        registry_pages += 1;
        if records.len().saturating_add(page.len()) > REGISTRY_CANDIDATE_LIMIT {
            return Err(MetadataError::Backend(
                "metadata candidate limit exceeded".to_string(),
            ));
        }
        records.append(&mut page);
        match next_start_after {
            Some(cursor) => start_after = Some(cursor),
            None => break,
        }
    }
    span.record("registry_pages", registry_pages as u64);
    span.record("record_count", records.len() as u64);
    // The registry keyspace iterates in (group, document) order; snapshot
    // consumers binary-search by document id (record_for_graph).
    records.sort_unstable_by_key(|record| record.document_id);

    // Lifecycle records are deletion tombstones, so one keyspace sweep refreshes the
    // deleted-state of every registry graph without per-graph point reads.
    let (deleted_graphs, lifecycle_pages) =
        list_deleted_iris(inner, REGISTRY_CANDIDATE_LIMIT).await?;
    span.record("lifecycle_pages", lifecycle_pages as u64);
    span.record("deleted_count", deleted_graphs.len() as u64);

    let lifecycle_entries = records
        .iter()
        .map(|record| {
            (
                record.graph_iri.clone(),
                deleted_graphs.contains(&record.graph_iri),
            )
        })
        .collect::<Vec<_>>();
    records.retain(|record| !deleted_graphs.contains(&record.graph_iri));
    let records = Arc::new(records);
    graph.warn_unprojected(records.as_ref()).await;
    let store_accepted = inner.visibility_cache.store_visibility_fill(
        records.clone(),
        lifecycle_entries,
        fill_generation,
    );
    span.record("store_accepted", store_accepted);
    record_elapsed_ms(&span, "elapsed_ms", started);
    Ok(VisibilityFillResult {
        records,
        store_accepted,
    })
}

pub(super) async fn list_deleted_iris(
    inner: &Arc<MetadataInner>,
    limit: usize,
) -> Result<(HashSet<String>, usize), MetadataError> {
    let mut deleted = HashSet::new();
    let mut start_after = None;
    let mut pages = 0usize;
    loop {
        let event = inner
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Iter {
                key_space: GRAPH_LIFECYCLE_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.map(IterStart::After),
                limit: limit.saturating_add(1),
                txn_id: None,
            }))
            .await;
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(MetadataError::Storage(error));
            }
            other => {
                return Err(MetadataError::Backend(format!(
                    "unexpected metadata graph lifecycle iteration result: {}",
                    other.label()
                )));
            }
        };
        pages += 1;
        if deleted.len().saturating_add(values.len()) > limit {
            return Err(MetadataError::Backend(
                "metadata lifecycle candidate limit exceeded".to_string(),
            ));
        }
        for (_, value) in values {
            let record: GraphLifecycleRecord = postcard::from_bytes(&value)
                .map_err(|error| MetadataError::Backend(error.to_string()))?;
            if record.is_deleted() {
                deleted.insert(record.graph_iri);
            }
        }
        match next_start_after {
            Some(cursor) => start_after = Some(cursor),
            None => break,
        }
    }
    Ok((deleted, pages))
}

/// Every live registry record of this node, from the visibility cache.
pub async fn list_cached_records(
    context: &DriverContext,
) -> Result<Arc<Vec<MetadataRegistryRecord>>, MetadataError> {
    let (metadata, graph) = handles(context)?;
    list_local_records(metadata.inner.clone(), graph).await
}

/// The live registry records of one group, from the visibility cache.
pub async fn list_cached_group(
    context: &DriverContext,
    group_id: GroupId,
) -> Result<Arc<Vec<MetadataRegistryRecord>>, MetadataError> {
    let (metadata, graph) = handles(context)?;
    list_local_group(metadata.inner.clone(), graph, group_id).await
}

/// Primes the visibility cache and craqle query indexes so the first
/// query after boot finds everything warm.
pub async fn warm_caches(context: &DriverContext) -> Result<(), MetadataError> {
    let (metadata, graph) = handles(context)?;
    graph.ensure_query_indexes().await?;
    let cache = &metadata.inner.visibility_cache;
    if cache.registry_records_any().is_none() {
        let _fill = cache.registry_fill.clone().lock_owned().await;
        if cache.registry_records_any().is_none() {
            fill_visibility_caches(&metadata.inner, graph).await?;
        }
    }
    Ok(())
}
