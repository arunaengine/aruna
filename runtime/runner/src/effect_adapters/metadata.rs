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

fn map_frame_error(error: FrameError) -> PeerRequestError {
    match error {
        FrameError {
            sent: false,
            peer_fault: false,
            message,
        } => PeerRequestError::NotSent(MetadataError::LocalUnavailable(message)),
        FrameError {
            sent: false,
            peer_fault: true,
            message,
        } => PeerRequestError::NotSent(MetadataError::Backend(message)),
        FrameError { message, .. } => PeerRequestError::Failed(MetadataError::Backend(message)),
    }
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
                Ok(Err(error)) => Err(map_frame_error(error)),
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

#[cfg(test)]
mod tests {
    use super::map_frame_error;
    use aruna_core::events::PeerRequestError;
    use aruna_core::metadata::MetadataError;
    use aruna_p2p::frames::FrameError;

    #[test]
    fn maps_frame_faults() {
        let local = FrameError {
            sent: false,
            peer_fault: false,
            message: "too many pooled connections".to_string(),
        };
        assert_eq!(
            map_frame_error(local),
            PeerRequestError::NotSent(MetadataError::LocalUnavailable(
                "too many pooled connections".to_string()
            ))
        );

        let dial = FrameError {
            sent: false,
            peer_fault: true,
            message: "connection failed".to_string(),
        };
        assert_eq!(
            map_frame_error(dial),
            PeerRequestError::NotSent(MetadataError::Backend("connection failed".to_string()))
        );

        let sent = FrameError {
            sent: true,
            peer_fault: true,
            message: "stream reset".to_string(),
        };
        assert_eq!(
            map_frame_error(sent),
            PeerRequestError::Failed(MetadataError::Backend("stream reset".to_string()))
        );
    }
}
