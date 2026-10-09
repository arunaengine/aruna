//! Receives heartbeats with live telemetry from sync peers over the heartbeat protocol. One stream
//! carries one length-prefixed heartbeat and no response; nothing is written to disk.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::heartbeat::{MAX_HEARTBEAT_BYTES, NodeHeartbeat};
use aruna_net::streams::BiStream;
use tokio::io::AsyncReadExt;
use tokio::time::timeout;
use tracing::debug;

use crate::driver::DriverContext;

/// Deadline for reading or sending one heartbeat.
pub(crate) const HEARTBEAT_IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Reads one heartbeat of `peer` and stores it when the sender is a sync peer of this realm and
/// the heartbeat is newer than the held one.
pub async fn handle_heartbeat_stream(context: &DriverContext, mut stream: BiStream, peer: NodeId) {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return;
    };
    let Some(_slot) = net_handle.heartbeat_slot(peer) else {
        debug!(%peer, "Dropping a heartbeat stream past the stream limits");
        return;
    };
    let read = timeout(HEARTBEAT_IO_TIMEOUT, read_heartbeat(&mut stream)).await;
    let _ = stream.0.finish();
    let heartbeat = match read {
        Ok(Ok(heartbeat)) => heartbeat,
        Ok(Err(error)) => {
            debug!(%peer, %error, "Dropping an invalid heartbeat");
            return;
        }
        Err(_) => {
            debug!(%peer, "Heartbeat read timed out");
            return;
        }
    };
    if heartbeat.realm_id != *net_handle.realm_id() {
        debug!(%peer, "Dropping a heartbeat of another realm");
        return;
    }
    if !net_handle.record_heartbeat(peer, heartbeat) {
        debug!(%peer, "Heartbeat not stored: not newer, too frequent or not a sync peer");
    }
}

async fn read_heartbeat(stream: &mut BiStream) -> Result<NodeHeartbeat, String> {
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
    NodeHeartbeat::from_bytes(&bytes).map_err(|error| error.to_string())
}
