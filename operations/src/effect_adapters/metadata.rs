//! Hands metadata effects and metadata peer requests to the metadata handle, or answers that the
//! handle is missing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{MetadataRequestEffect, WritePeerRequest};
use aruna_core::events::{Event, MetadataAnswer, NetEvent, PeerAuthRefusal, PeerRequestError};
use aruna_core::metadata::{AuthToken, MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::repository::remote::{HarvestWrite, HarvestWriteFailure};
use aruna_core::structs::storage::dataset_description::DatasetSummaryRequest;
use std::future::{Future, ready};
use std::pin::Pin;
use tokio::time::{Instant, timeout};

use crate::driver::DriverContext;
use crate::forward::transport::MetadataWriteError;
use crate::jobs::metadata_class::{MetadataFailure, classify_metadata};
use crate::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentError, CreateDocumentOperation, CreateDocumentPayload,
};
use crate::metadata::forward::read::dataset_summary;
use crate::metadata::forward::write::{
    route_metadata_create, route_metadata_delete, route_metadata_update,
};
use crate::metadata::update_document::UpdateDocumentMutation;

// A boxed future: the metadata effect path reads authorization through the runner again,
// so its type must not contain the runner's own future.
pub(super) fn dispatch_metadata(
    effect: MetadataEffect,
    context: &DriverContext,
) -> Pin<Box<dyn Future<Output = Event> + Send + '_>> {
    match &context.metadata_handle {
        Some(metadata) => Box::pin(metadata.run_effect(context, effect)),
        None => Box::pin(ready(Event::Metadata(MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::HandleMissing,
        }))),
    }
}

// A boxed future for the same reason: the routed export drives operations again.
pub(super) fn dispatch_summary(
    request: DatasetSummaryRequest,
    context: &DriverContext,
) -> Pin<Box<dyn Future<Output = Event> + Send + '_>> {
    Box::pin(async move {
        let answer = dataset_summary(context, request).await;
        Event::Net(NetEvent::DatasetSummary(answer))
    })
}

// A boxed future for the same reason: the routed write drives operations again.
pub(super) fn dispatch_harvest_write(
    write: HarvestWrite,
    context: &DriverContext,
) -> Pin<Box<dyn Future<Output = Event> + Send + '_>> {
    Box::pin(async move {
        Event::Net(NetEvent::HarvestWrite(
            routed_write(write, context).await.map_err(harvest_failure),
        ))
    })
}

async fn routed_write(
    write: HarvestWrite,
    context: &DriverContext,
) -> Result<(), MetadataWriteError> {
    let context = std::sync::Arc::new(context.clone());
    match write {
        HarvestWrite::Create {
            actor,
            owner,
            group_id,
            document_id,
            document_path,
            jsonld,
        } => {
            let operation = CreateDocumentOperation::new_generated_id(CreateDocumentConfig {
                actor,
                group_id,
                document_id,
                document_path,
                public: false,
                payload: CreateDocumentPayload::RoCrate { jsonld },
            });
            let token = Some(AuthToken::internal(owner));
            Box::pin(route_metadata_create(operation, context, token)).await?;
        }
        HarvestWrite::Update {
            actor,
            owner,
            record,
            document_id,
            jsonld,
        } => {
            Box::pin(route_metadata_update(
                &context,
                actor,
                record.as_deref(),
                document_id,
                None,
                UpdateDocumentMutation::ReplaceRoCrate { jsonld },
                None,
                Some(AuthToken::internal(owner)),
                None,
            ))
            .await?;
        }
        HarvestWrite::Delete {
            actor,
            owner,
            record,
            document_id,
        } => {
            let token = Some(AuthToken::internal(owner));
            Box::pin(route_metadata_delete(
                &context,
                actor,
                record.as_deref(),
                document_id,
                token,
            ))
            .await?;
        }
    }
    Ok(())
}

fn harvest_failure(error: MetadataWriteError) -> HarvestWriteFailure {
    match error {
        MetadataWriteError::NotFound => HarvestWriteFailure::NotFound,
        MetadataWriteError::Create(CreateDocumentError::DocumentAlreadyExists) => {
            HarvestWriteFailure::AlreadyExists
        }
        error => match classify_metadata(error) {
            MetadataFailure::Retryable(message) => HarvestWriteFailure::Retryable(message),
            MetadataFailure::Permanent(message) => HarvestWriteFailure::Permanent(message),
            MetadataFailure::Validation(violations) => HarvestWriteFailure::Rejected(violations),
        },
    }
}

/// Checks a forwarded token with the validation state the metadata handle keeps.
pub(super) async fn dispatch_peer_auth(
    request: WritePeerRequest,
    context: &DriverContext,
) -> Event {
    let answer = match &context.metadata_handle {
        Some(handle) => handle.check_write_peer(request).await,
        None => Err(PeerAuthRefusal::Unavailable(
            "metadata auth handle unavailable".to_string(),
        )),
    };
    Event::Net(NetEvent::WritePeerAuth(answer))
}

/// Sends one encoded request and reports the answer with the time it took.
pub(super) async fn dispatch_request(
    effect: MetadataRequestEffect,
    context: &DriverContext,
) -> Event {
    let started = Instant::now();
    let result = match &context.metadata_handle {
        None => Err(PeerRequestError::Failed(
            "metadata transport unavailable".to_string(),
        )),
        Some(handle) => match timeout(
            effect.timeout,
            handle.request_frame(effect.peer, &effect.frame),
        )
        .await
        {
            Ok(Ok(answer)) => Ok(answer),
            Ok(Err(error)) => Err(PeerRequestError::Failed(error.to_string())),
            Err(_) => Err(PeerRequestError::TimedOut),
        },
    };
    Event::Net(NetEvent::MetadataAnswer(Box::new(MetadataAnswer {
        peer: effect.peer,
        elapsed: started.elapsed(),
        result,
    })))
}
