//! Hands metadata effects and metadata peer requests to the metadata handle, or answers that the
//! handle is missing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::MetadataRequestEffect;
use aruna_core::events::{Event, MetadataAnswer, NetEvent, PeerRequestError};
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::structs::storage::dataset_description::DatasetSummaryRequest;
use std::future::Future;
use std::pin::Pin;
use tokio::time::{Instant, timeout};

use crate::driver::DriverContext;
use crate::metadata::forward::read::{summary_answer, summary_export};
use crate::metadata::message_kind::MetadataRequestDelivery;

/// Runs one metadata effect on the graph store, or answers that the store is missing.
pub(super) async fn dispatch_metadata(effect: MetadataEffect, context: &DriverContext) -> Event {
    let Some(graph) = &context.graph_handle else {
        return Event::Metadata(MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::HandleMissing,
        });
    };
    let started = std::time::Instant::now();
    let event = graph.run(effect).await;
    aruna_core::telemetry::record_stage("craqle", started.elapsed());
    Event::Metadata(event)
}

// A boxed future for the same reason: the routed export drives operations again.
pub(super) fn dispatch_summary(
    request: DatasetSummaryRequest,
    context: &DriverContext,
) -> Pin<Box<dyn Future<Output = Event> + Send + '_>> {
    Box::pin(async move {
        let local_node = context.net_handle.as_ref().map(|net| net.node_id());
        let export = crate::driver::drive(summary_export(local_node, request), context).await;
        let answer = summary_answer(export);
        Event::Net(NetEvent::DatasetSummary(answer))
    })
}

/// Sends one encoded request and reports the answer with the time it took.
pub(super) async fn dispatch_request(
    effect: MetadataRequestEffect,
    context: &DriverContext,
) -> Event {
    let started = Instant::now();
    let result = match &context.metadata_handle {
        None => Err(PeerRequestError::Failed(MetadataError::HandleMissing)),
        Some(handle) => match timeout(
            effect.timeout,
            handle.request_frame(effect.peer, &effect.frame),
        )
        .await
        {
            Ok(Ok(answer)) => Ok(answer),
            Ok(Err(error)) => match error.delivery() {
                MetadataRequestDelivery::DefinitelyNotSent => {
                    Err(PeerRequestError::NotSent(error.into_metadata_error()))
                }
                MetadataRequestDelivery::PossiblySent => {
                    Err(PeerRequestError::Failed(error.into_metadata_error()))
                }
            },
            Err(_) => Err(PeerRequestError::TimedOut),
        },
    };
    Event::Net(NetEvent::MetadataAnswer(Box::new(MetadataAnswer {
        peer: effect.peer,
        elapsed: started.elapsed(),
        result,
    })))
}
