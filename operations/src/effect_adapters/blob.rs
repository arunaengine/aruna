//! Blob-handle adapters: blob, staging-source, and local-file effects. All three
//! need `DriverContext::blob_handle`; without it each keeps its explicit
//! missing-handle outcome instead of silently succeeding.

use aruna_core::effects::{BlobEffect, LocalFileEffect, StagingSourceEffect};
use aruna_core::errors::{BlobError, StagingSourceError};
use aruna_core::events::{BlobEvent, Event, LocalFileEvent, StagingSourceEvent};

use crate::driver::DriverContext;

pub(super) async fn dispatch_blob(effect: BlobEffect, context: &DriverContext) -> Event {
    if let Some(blob_handle) = &context.blob_handle {
        Box::pin(blob_handle.send_blob_effect(effect)).await
    } else {
        Event::Blob(BlobEvent::Error(BlobError::HandleMissing))
    }
}

pub(super) async fn dispatch_staging_source(
    effect: StagingSourceEffect,
    context: &DriverContext,
) -> Event {
    if crate::staging::native_source::is_native_effect(&effect) {
        Box::pin(crate::staging::native_source::send_native_effect(
            effect, context,
        ))
        .await
    } else if let Some(blob_handle) = &context.blob_handle {
        Box::pin(blob_handle.send_staging_effect(effect)).await
    } else {
        Event::StagingSource(StagingSourceEvent::Error {
            error: StagingSourceError::HandleMissing,
        })
    }
}

pub(super) async fn dispatch_local_file(effect: LocalFileEffect, context: &DriverContext) -> Event {
    if let Some(blob_handle) = &context.blob_handle {
        Box::pin(blob_handle.send_file_effect(effect)).await
    } else {
        Event::LocalFile(LocalFileEvent::Error {
            message: "this node has no local file adapter".to_string(),
        })
    }
}
