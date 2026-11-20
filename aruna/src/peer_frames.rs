//! Reads the request of a peer protocol the trigger table serves and writes its answer back, in
//! that protocol's framing. The table decides what a request means; this file only moves bytes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::alpn::Alpn;
use aruna_core::heartbeat::{HEARTBEAT_IO_TIMEOUT, MAX_HEARTBEAT_BYTES};
use aruna_core::id::NodeId;
use aruna_core::structs::execution::notification::NotificationTransportMessage;
use aruna_net::NetHandle;
use aruna_net::heartbeat::HeartbeatSlot;
use aruna_net::notification::{
    close_stream, drain_request, read_frame, write_frame, write_message,
};
use aruna_net::shard_manifest::{
    close_shard_stream, read_manifest_request, write_manifest, write_rejection,
};
use aruna_net::streams::BiStream;
use aruna_operations::shard::incoming::ManifestAnswer;
use aruna_operations::triggers::{PeerAnswer, PeerFailure, PeerInput, PeerRequest};
use tracing::{debug, warn};

/// A peer request as its protocol frames it, with the admission it holds while it is served.
pub(crate) struct PeerRead {
    pub(crate) request: PeerRequest,
    pub(crate) slot: Option<HeartbeatSlot>,
}

/// Reads the request of one converted protocol in its framing; a refused or broken read is
/// only logged.
pub(crate) async fn read_peer(
    alpn: Alpn,
    stream: &mut BiStream,
    peer: NodeId,
    net_handle: &NetHandle,
) -> Option<PeerRead> {
    let (request, slot) = match alpn {
        Alpn::Heartbeat => {
            let Some(slot) = net_handle.heartbeat_slot(peer) else {
                debug!(%peer, "Dropping a heartbeat stream past the stream limits");
                return None;
            };
            let read = tokio::time::timeout(HEARTBEAT_IO_TIMEOUT, heartbeat_frame(stream)).await;
            let _ = stream.0.finish();
            match read {
                Ok(Ok(frame)) => (PeerRequest::Frame(frame), Some(slot)),
                Ok(Err(error)) => {
                    debug!(%peer, %error, "Dropping an invalid heartbeat");
                    return None;
                }
                Err(_) => {
                    debug!(%peer, "Heartbeat read timed out");
                    return None;
                }
            }
        }
        Alpn::Shard => match read_manifest_request(stream).await {
            Ok(message) => (PeerRequest::Manifest(message), None),
            Err(error) => {
                warn!(peer = %peer, error = %error, "Failed to read shard manifest request");
                return None;
            }
        },
        Alpn::Notification => {
            let frame = match read_frame(stream).await {
                Ok(frame) => frame,
                Err(error) => {
                    warn!(peer = %peer, error = %error, "Failed to read notification message");
                    return None;
                }
            };
            if let Err(error) = drain_request(stream).await {
                warn!(peer = %peer, error = %error, "Failed to drain notification request stream");
                close_stream(stream).await;
                return None;
            }
            (PeerRequest::Frame(frame), None)
        }
        other => {
            warn!(peer = %peer, alpn = ?other, "No peer table framing for this protocol");
            return None;
        }
    };
    Some(PeerRead { request, slot })
}

/// Writes the answer of one converted protocol in its framing and closes the stream.
pub(crate) async fn write_peer(
    input: &PeerInput,
    stream: &mut BiStream,
    answer: Result<PeerAnswer, PeerFailure>,
) {
    let peer = input.peer;
    let written = match (input.alpn, answer) {
        (Alpn::Shard, answer) => {
            let written = match answer {
                Ok(PeerAnswer::Manifest(ManifestAnswer::Manifest(manifest))) => {
                    write_manifest(stream, &manifest).await
                }
                Ok(PeerAnswer::Manifest(ManifestAnswer::Reject(reason))) => {
                    write_rejection(stream, reason).await
                }
                Ok(_) => write_rejection(stream, "unexpected shard answer".to_string()).await,
                Err(error) => write_rejection(stream, error.to_string()).await,
            };
            close_shard_stream(stream).await;
            written
        }
        (Alpn::Notification, answer) => {
            let written = match answer {
                Ok(PeerAnswer::Frame(frame)) => write_frame(stream, &frame).await,
                Ok(_) => refuse_notification(input, stream).await,
                Err(error) => Err(error.to_string()),
            };
            close_stream(stream).await;
            written
        }
        _ => Ok(()),
    };
    if let Err(error) = written {
        warn!(peer = %peer, alpn = ?input.alpn, error = %error, "Failed to write peer answer");
    }
}

/// Every notification request has an operation; an answer frame sent as a request is refused.
async fn refuse_notification(input: &PeerInput, stream: &mut BiStream) -> Result<(), String> {
    let PeerRequest::Frame(frame) = &input.request else {
        return Err("notification request without a frame".to_string());
    };
    let message = postcard::from_bytes::<NotificationTransportMessage>(frame)
        .map_err(|error| error.to_string())?;
    debug!(peer = %input.peer, message = message.label(), "Received notification message");
    let reason = "unexpected notification control message".to_string();
    write_message(stream, &NotificationTransportMessage::Reject(reason)).await
}

/// One length-prefixed heartbeat frame, at most [`MAX_HEARTBEAT_BYTES`] long.
async fn heartbeat_frame(stream: &mut BiStream) -> Result<Vec<u8>, String> {
    use tokio::io::AsyncReadExt;
    let mut length = [0u8; 4];
    stream
        .1
        .read_exact(&mut length)
        .await
        .map_err(|error| error.to_string())?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_HEARTBEAT_BYTES {
        return Err("heartbeat frame exceeds the size limit".to_string());
    }
    let mut bytes = vec![0u8; length];
    stream
        .1
        .read_exact(&mut bytes)
        .await
        .map_err(|error| error.to_string())?;
    Ok(bytes)
}
