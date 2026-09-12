use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Instant;

use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataRequestDurability};
use aruna_core::telemetry::record_elapsed_ms;
use tracing::{debug_span, field, warn};

use super::MetadataInner;
use super::effects::record_error;

pub(super) fn flush_sync_journal(
    inner: &MetadataInner,
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
        mode = inner.document_sync_persist_policy.label(),
        elapsed_ms = field::Empty,
        result = field::Empty,
    );
    let started = Instant::now();
    let result = span.in_scope(|| db.persist(inner.document_sync_persist_policy.as_fjall()));
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

pub(super) fn flush_metadata_persistence(
    inner: &MetadataInner,
    effect_name: &'static str,
    graph_iri: Option<&str>,
) -> Result<(), MetadataError> {
    inner
        .node
        .persist_fjall()
        .map_err(|error| MetadataError::Persist(error.to_string()))?;
    flush_sync_journal(inner, effect_name, graph_iri)
}

pub(super) fn effect_persists_sync(effect: &MetadataEffect) -> bool {
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
        MetadataEffect::SyncGraphBestEffort { .. }
        | MetadataEffect::QueryGraphs { .. }
        | MetadataEffect::SearchGraphs { .. }
        | MetadataEffect::GetGraphPolicy { .. }
        | MetadataEffect::ExportRoCrate { .. }
        | MetadataEffect::ExportRoCrateSummary { .. }
        | MetadataEffect::ExportRoCratePage { .. }
        | MetadataEffect::ListGraphs
        | MetadataEffect::ContainsGraph { .. }
        | MetadataEffect::GraphSnapshot { .. }
        // A device runs no document sync, so an installed snapshot has no
        // journal entry to flush.
        | MetadataEffect::InstallSnapshot { .. }
        // A merged batch publishes nothing back to irokle.
        | MetadataEffect::PlanBatch { .. }
        | MetadataEffect::MergeBatch { .. } => false,
    }
}

pub(super) fn effect_defers_persist(effect: &MetadataEffect) -> bool {
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

pub(super) fn effect_skips_lifecycle(effect: &MetadataEffect) -> bool {
    matches!(
        effect,
        MetadataEffect::ValidateCreateCrate { .. } | MetadataEffect::ValidateRoCrate { .. }
    )
}

pub(super) fn schedule_deferred_persist(
    inner: Arc<MetadataInner>,
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

fn run_deferred_flush(inner: &MetadataInner, effect_name: &'static str, graph_iri: Option<&str>) {
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
