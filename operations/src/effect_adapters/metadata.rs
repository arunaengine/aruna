//! Hands metadata effects and metadata peer requests to the metadata handle, or answers that the
//! handle is missing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::{MetadataRequestEffect, WritePeerRequest};
use aruna_core::events::{Event, MetadataAnswer, NetEvent, PeerAuthRefusal, PeerRequestError};
use aruna_core::metadata::{AuthToken, MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::structs::storage::dataset_description::DatasetSummaryRequest;
use std::future::{Future, ready};
use std::pin::Pin;
use tokio::time::{Instant, timeout};

use crate::driver::DriverContext;
use crate::metadata::forward::read::{summary_answer, summary_export};
use crate::metadata::message_kind::MetadataRequestDelivery;

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
        let local_node = context.net_handle.as_ref().map(|net| net.node_id());
        let export = crate::driver::drive(summary_export(local_node, request), context).await;
        let answer = summary_answer(export);
        Event::Net(NetEvent::DatasetSummary(answer))
    })
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

/// Refreshes a peer's last-contact time where the metadata handle keeps it.
pub(super) fn dispatch_contact(peer: NodeId, context: &DriverContext) -> Event {
    if let Some(handle) = &context.metadata_handle {
        handle.note_peer_contact(peer);
    }
    Event::Net(NetEvent::PeerContact)
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

/// Verifies a token named for revocation; it may already be revoked.
pub(super) async fn dispatch_revocation_claims(token: AuthToken, context: &DriverContext) -> Event {
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
