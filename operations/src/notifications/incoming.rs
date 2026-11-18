//! Serves inbound notification streams: reads one request frame, runs the operation the trigger
//! table names for it and writes its answer frame.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::structs::execution::notification::NotificationTransportMessage;
use aruna_core::time::unix_timestamp_millis;
use aruna_net::notification::{
    close_stream, drain_request, read_frame, write_frame, write_message,
};
use aruna_net::streams::BiStream;
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::driver::{DriverContext, drive_retry_until};
use crate::triggers::{PeerInput, peer_operation};

const NOTIFICATION_AUTH_TIMEOUT: Duration = Duration::from_secs(30);

fn deliver_deadline() -> Instant {
    Instant::now() + NOTIFICATION_AUTH_TIMEOUT
}

#[tracing::instrument(
    name = "notifications.incoming.stream",
    level = "debug",
    skip(context, stream),
    fields(peer = %peer)
)]
pub(crate) async fn handle_notification_stream(
    context: &DriverContext,
    mut stream: BiStream,
    peer: NodeId,
) {
    let Some(net_handle) = context.net_handle.as_ref() else {
        warn!(peer = %peer, "Dropping inbound notification stream without net handle");
        return;
    };
    let frame = match read_frame(&mut stream).await {
        Ok(frame) => frame,
        Err(error) => {
            warn!(peer = %peer, error = %error, "Failed to read notification message");
            return;
        }
    };
    if let Err(error) = drain_request(&mut stream).await {
        warn!(peer = %peer, error = %error, "Failed to drain notification request stream");
        close_stream(&mut stream).await;
        return;
    }
    let input = PeerInput {
        alpn: Alpn::Notification,
        peer,
        local_node: net_handle.node_id(),
        local_realm: *net_handle.realm_id(),
        now_ms: unix_timestamp_millis(),
        frame: &frame,
    };
    let written = match peer_operation(input) {
        // The same bound the config read had before, so a slow store cannot hold the stream.
        Some(operation) => {
            let build = || operation.clone();
            let deadline = deliver_deadline();
            match drive_retry_until(build, context, peer.as_bytes(), deadline).await {
                Ok(answer) => write_frame(&mut stream, &answer).await,
                Err(error) => Err(error.to_string()),
            }
        }
        // Every request has an operation; an answer frame sent as a request is refused.
        None => match postcard::from_bytes::<NotificationTransportMessage>(&frame) {
            Ok(message) => {
                debug!(peer = %peer, message = message.label(), "Received notification message");
                let reason = "unexpected notification control message".to_string();
                let response = NotificationTransportMessage::Reject(reason);
                write_message(&mut stream, &response).await
            }
            Err(error) => Err(error.to_string()),
        },
    };
    if let Err(error) = written {
        warn!(peer = %peer, error = %error, "Failed to write notification response");
    }
    close_stream(&mut stream).await;
}

#[cfg(test)]
#[path = "incoming_tests.rs"]
mod tests;
