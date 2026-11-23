//! Runs metadata effects on the graph store and sends metadata peer requests over the net
//! handle, or answers that the handle is missing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::alpn::Alpn;
use aruna_core::effects::MetadataRequestEffect;
use aruna_core::events::{Event, MetadataAnswer, NetEvent, PeerRequestError};
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};
use aruna_p2p::frames::FrameError;
use tokio::time::{Instant, timeout};

use crate::driver::DriverContext;

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

/// Sends one encoded request over the net handle and reports the answer with the time it took.
pub(super) async fn dispatch_request(
    effect: MetadataRequestEffect,
    context: &DriverContext,
) -> Event {
    let started = Instant::now();
    let result = match (&context.net_handle, effect.shape) {
        (None, _) => Err(PeerRequestError::Failed(MetadataError::HandleMissing)),
        (Some(_), None) => Err(PeerRequestError::NotSent(MetadataError::InvalidInput(
            "the metadata request frame does not decode".to_string(),
        ))),
        (Some(net), Some(shape)) => {
            let request = net.frame_request(effect.peer, Alpn::Metadata, &effect.frame, &shape);
            match timeout(effect.timeout, request).await {
                Ok(Ok(answer)) => Ok(answer),
                Ok(Err(FrameError {
                    sent: false,
                    message,
                })) => Err(PeerRequestError::NotSent(MetadataError::Backend(message))),
                Ok(Err(FrameError { message, .. })) => {
                    Err(PeerRequestError::Failed(MetadataError::Backend(message)))
                }
                Err(_) => Err(PeerRequestError::TimedOut),
            }
        }
    };
    Event::Net(NetEvent::MetadataAnswer(Box::new(MetadataAnswer {
        peer: effect.peer,
        elapsed: started.elapsed(),
        result,
    })))
}
