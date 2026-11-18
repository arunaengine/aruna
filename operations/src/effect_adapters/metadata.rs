//! Hands metadata effects and metadata peer requests to the metadata handle, or answers that the
//! handle is missing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::MetadataRequestEffect;
use aruna_core::events::{Event, MetadataAnswer, NetEvent, PeerRequestError, WritePeerRefusal};
use aruna_core::NodeId;
use aruna_core::metadata::{AuthToken, MetadataEffect, MetadataError, MetadataEvent};
use std::future::{Future, ready};
use std::pin::Pin;
use tokio::time::{Instant, timeout};

use crate::driver::DriverContext;
use crate::metadata::handle::{MetadataRequestDelivery, WritePeerError};

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

/// Sends one encoded request and reports the answer with the time it took.
pub(super) async fn dispatch_request(
    effect: MetadataRequestEffect,
    context: &DriverContext,
) -> Event {
    let started = Instant::now();
    let result = match &context.metadata_handle {
        None => Err(PeerRequestError::NotSent(
            "metadata transport unavailable".to_string(),
        )),
        Some(handle) => match timeout(
            effect.timeout,
            handle.request_frame(effect.peer, &effect.frame),
        )
        .await
        {
            Ok(Ok(answer)) => Ok(answer),
            Ok(Err(error)) => Err(match error.delivery() {
                MetadataRequestDelivery::DefinitelyNotSent => {
                    PeerRequestError::NotSent(error.to_string())
                }
                MetadataRequestDelivery::PossiblySent => {
                    PeerRequestError::Failed(error.to_string())
                }
            }),
            Err(_) => Err(PeerRequestError::TimedOut),
        },
    };
    Event::Net(NetEvent::MetadataAnswer(Box::new(MetadataAnswer {
        peer: effect.peer,
        elapsed: started.elapsed(),
        result,
    })))
}

/// Checks a forwarded caller with the metadata service's token state.
pub(super) async fn dispatch_peer_check(
    peer: NodeId,
    auth_token: Option<AuthToken>,
    context: &DriverContext,
) -> Event {
    let result = match &context.metadata_handle {
        None => Err(WritePeerRefusal::Unavailable(
            "forwarded writes need a metadata handle".to_string(),
        )),
        Some(handle) => (handle.authorize_write_peer(peer, auth_token).await)
            .map(Box::new)
            .map_err(|error| match error {
                WritePeerError::Unauthorized => WritePeerRefusal::Unauthorized,
                WritePeerError::Unavailable(error) => {
                    WritePeerRefusal::Unavailable(error.to_string())
                }
            }),
    };
    Event::Net(NetEvent::WritePeerChecked(result))
}

/// Verifies a token named for revocation; it may already be revoked.
pub(super) async fn dispatch_revocation_claims(
    token: AuthToken,
    context: &DriverContext,
) -> Event {
    let result = match (&context.metadata_handle, token) {
        (Some(handle), AuthToken::Bearer(token)) => (handle.claims_for_revocation(token.as_str()))
            .await
            .map(Box::new)
            .map_err(|error| error.to_string()),
        (None, _) => Err("token checks need a metadata handle".to_string()),
        _ => Err("only a bearer token can be revoked".to_string()),
    };
    Event::Net(NetEvent::RevocationClaims(result))
}
