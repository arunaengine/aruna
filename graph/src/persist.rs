//! Flushes the document sync journal, either inside the request or on a deferred thread.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Instant;

use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataRequestDurability};
use aruna_core::telemetry::{record_elapsed_ms, record_error};
use tracing::{debug_span, field, warn};

use crate::GraphInner;

pub(crate) fn flush_sync_journal(
    inner: &GraphInner,
    effect_name: &'static str,
    graph_iri: Option<&str>,
) -> Result<(), MetadataError> {
    let Some(db) = &inner.document_sync_db else {
        return Ok(());
    };
    let span = debug_span!(
        "metadata.backend.document_sync.flush",
        effect = effect_name,
        graph_iri = graph_iri.unwrap_or("<none>"),
        mode = if inner.sync_all { "sync_all" } else { "buffer" },
        elapsed_ms = field::Empty,
        result = field::Empty,
    );
    let started = Instant::now();
    let result = span.in_scope(|| {
        db.persist(if inner.sync_all {
            fjall::PersistMode::SyncAll
        } else {
            fjall::PersistMode::Buffer
        })
    });
    record_elapsed_ms(&span, "elapsed_ms", started);
    match result {
        Ok(()) => {
            span.record("result", "ok");
            Ok(())
        }
        Err(error) => {
            record_error(&span, &error.to_string());
            Err(MetadataError::Persist(format!(
                "failed to flush document sync journal: {error}"
            )))
        }
    }
}

pub(crate) fn flush_metadata_persistence(
    inner: &GraphInner,
    effect_name: &'static str,
    graph_iri: Option<&str>,
) -> Result<(), MetadataError> {
    inner
        .node
        .persist_fjall()
        .map_err(|error| MetadataError::Persist(error.to_string()))?;
    flush_sync_journal(inner, effect_name, graph_iri)
}

pub(crate) fn effect_persists_sync(effect: &MetadataEffect) -> bool {
    match effect {
        MetadataEffect::ValidateCreateCrate { .. } | MetadataEffect::ValidateRoCrate { .. } => {
            false
        }
        MetadataEffect::CreateCrate { request } => {
            request_persists_sync(request.durability)
        }
        MetadataEffect::ApplyRoCrate { request } => {
            request_persists_sync(request.durability)
        }
        MetadataEffect::UpsertDataEntity { request }
        | MetadataEffect::UpsertContextualEntity { request } => {
            request_persists_sync(request.durability)
        }
        MetadataEffect::SetGraphPolicy { .. }
        | MetadataEffect::AddGraphPeer { .. }
        | MetadataEffect::DeleteGraph { .. } => true,
        MetadataEffect::SyncBestEffort { .. }
        | MetadataEffect::SearchGraphs { .. }
        | MetadataEffect::GetGraphPolicy { .. }
        | MetadataEffect::ExportRoCrate { .. }
        | MetadataEffect::ExportRoCrateSummary { .. }
        | MetadataEffect::ExportSummaryAt { .. }
        | MetadataEffect::DescribeTitle { .. }
        | MetadataEffect::IriReferences { .. }
        | MetadataEffect::ExportRoCratePage { .. }
        | MetadataEffect::ListGraphs
        | MetadataEffect::CachedRecords
        | MetadataEffect::ContainsGraph { .. }
        | MetadataEffect::ContainsDot { .. }
        | MetadataEffect::GraphSnapshot { .. }
        | MetadataEffect::ExportVersioned { .. }
        // A device runs no document sync, so an installed snapshot has no
        // journal entry to flush.
        | MetadataEffect::InstallSnapshot { .. }
        // A merged batch publishes nothing back to irokle.
        | MetadataEffect::PlanBatch { .. }
        | MetadataEffect::MergeBatch { .. }
        // Checks of unsaved candidates and this node's caches change no graph.
        | MetadataEffect::CheckStructure { .. }
        | MetadataEffect::CheckProfile { .. }
        | MetadataEffect::CachedShapes { .. }
        | MetadataEffect::CacheShapes { .. }
        | MetadataEffect::FlushGraph
        | MetadataEffect::MergeRows { .. }
        | MetadataEffect::CacheGeneration
        | MetadataEffect::CacheRecords { .. }
        | MetadataEffect::UncacheRecord { .. }
        | MetadataEffect::CachedGroup { .. }
        | MetadataEffect::QueryCacheGet { .. }
        | MetadataEffect::QueryCachePut { .. }
        | MetadataEffect::QueryCacheStamp
        | MetadataEffect::RegistrySnapshot
        | MetadataEffect::StoreRegistry { .. }
        | MetadataEffect::RefreshLifecycle { .. }
        | MetadataEffect::QueryScope { .. }
        | MetadataEffect::SearchScope { .. }
        | MetadataEffect::DescribeHits { .. } => false,
    }
}

pub(crate) fn effect_defers_persist(effect: &MetadataEffect) -> bool {
    match effect {
        MetadataEffect::CreateCrate { request } => {
            request.durability == MetadataRequestDurability::WalAlreadyDurable
        }
        MetadataEffect::ApplyRoCrate { request } => {
            request.durability == MetadataRequestDurability::WalAlreadyDurable
        }
        MetadataEffect::UpsertDataEntity { request }
        | MetadataEffect::UpsertContextualEntity { request } => {
            request.durability == MetadataRequestDurability::WalAlreadyDurable
        }
        _ => false,
    }
}

pub(crate) fn schedule_deferred_persist(
    inner: Arc<GraphInner>,
    effect_name: &'static str,
    graph_iri: Option<String>,
) {
    inner
        .deferred_persist_requested
        .store(true, Ordering::Release);
    if inner.deferred_persist_running.swap(true, Ordering::AcqRel) {
        return;
    }

    let worker_inner = inner.clone();
    let worker_graph_iri = graph_iri.clone();
    let spawn_result = thread::Builder::new()
        .name("metadata-deferred-persist".to_string())
        .spawn(move || {
            loop {
                while worker_inner
                    .deferred_persist_requested
                    .swap(false, Ordering::AcqRel)
                {
                    run_deferred_flush(&worker_inner, effect_name, worker_graph_iri.as_deref());
                }

                worker_inner
                    .deferred_persist_running
                    .store(false, Ordering::Release);
                if !worker_inner
                    .deferred_persist_requested
                    .load(Ordering::Acquire)
                {
                    break;
                }
                if worker_inner
                    .deferred_persist_running
                    .swap(true, Ordering::AcqRel)
                {
                    break;
                }
            }
        });

    if let Err(error) = spawn_result {
        inner
            .deferred_persist_running
            .store(false, Ordering::Release);
        warn!(
            event = "metadata.backend.deferred_persist.spawn_failed",
            effect = effect_name,
            error = %error,
            "Failed to spawn deferred metadata persist"
        );
    }
}

fn run_deferred_flush(inner: &GraphInner, effect_name: &'static str, graph_iri: Option<&str>) {
    let span = debug_span!(
        "metadata.backend.deferred_persist",
        effect = effect_name,
        graph_iri = graph_iri.unwrap_or("<none>"),
        elapsed_ms = field::Empty,
        result = field::Empty,
    );
    let started = Instant::now();
    let result = span.in_scope(|| flush_metadata_persistence(inner, effect_name, graph_iri));
    record_elapsed_ms(&span, "elapsed_ms", started);
    match result {
        Ok(()) => {
            span.record("result", "ok");
        }
        Err(error) => {
            record_error(&span, &error.to_string());
            warn!(
                event = "metadata.backend.deferred_persist.failed",
                effect = effect_name,
                graph_iri = graph_iri.unwrap_or("<none>"),
                error = %error,
                "Deferred metadata persist failed"
            );
        }
    }
}

fn request_persists_sync(durability: MetadataRequestDurability) -> bool {
    matches!(durability, MetadataRequestDurability::Durable)
}
