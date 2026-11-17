//! Hands metadata effects and metadata peer requests to the metadata handle, or answers that the
//! handle is missing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, MetadataRequestEffect};
use aruna_core::events::{Event, MetadataAnswer, NetEvent, PeerRequestError};
use aruna_core::handle::Handle;
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};
use tokio::time::{Instant, timeout};

use crate::driver::DriverContext;

pub(super) async fn dispatch_metadata(effect: MetadataEffect, context: &DriverContext) -> Event {
    if let Some(metadata_handle) = &context.metadata_handle {
        Box::pin(metadata_handle.send_effect(Effect::Metadata(effect))).await
    } else {
        Event::Metadata(MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::HandleMissing,
        })
    }
}

/// Sends one encoded request and reports the answer with the time it took.
pub(super) async fn dispatch_request(effect: MetadataRequestEffect, context: &DriverContext) -> Event {
    let started = Instant::now();
    let result = match &context.metadata_handle {
        None => Err(PeerRequestError::Failed(
            "metadata transport unavailable".to_string(),
        )),
        Some(handle) => match timeout(effect.timeout, handle.request_frame(effect.peer, &effect.frame)).await {
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
