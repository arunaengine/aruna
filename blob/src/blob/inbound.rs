//! Accepts inbound bao streams: registers them, reads the first frame and closes refused ones.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::BlobEffect;
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_net::streams::BiStream;
use thiserror::Error;
use tokio::time::timeout;
use tracing::error;
use ulid::Ulid;

use super::BlobHandle;

/// Why an inbound bao stream was not accepted or its first frame not read.
#[derive(Debug, Error, PartialEq)]
pub enum InboundError {
    #[error("the inbound bao stream timed out")]
    TimedOut,
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("unexpected first event on the inbound bao stream: {0}")]
    Unexpected(String),
}

impl BlobHandle {
    /// Registers an inbound bao stream within `limit`, so a slow peer cannot hold a slot.
    pub async fn accept_stream(
        &self,
        peer: NodeId,
        stream: BiStream,
        limit: Duration,
    ) -> Result<Ulid, InboundError> {
        match timeout(limit, self.store_connection(peer, stream)).await {
            Ok(result) => Ok(result?),
            Err(_) => Err(InboundError::TimedOut),
        }
    }

    /// Reads the first frame of a registered stream within `limit`.
    pub async fn first_frame(
        &self,
        stream_id: Ulid,
        limit: Duration,
    ) -> Result<Vec<u8>, InboundError> {
        let read = self.send_blob_effect(BlobEffect::ReadMessage { stream_id });
        match timeout(limit, read).await {
            Err(_) => Err(InboundError::TimedOut),
            Ok(Event::Blob(BlobEvent::MessageReceived { payload, .. })) => Ok(payload),
            Ok(Event::Blob(BlobEvent::Error(error))) => Err(error.into()),
            Ok(other) => Err(InboundError::Unexpected(format!("{other:?}"))),
        }
    }

    /// Closes a registered stream that this node refuses or has finished answering.
    pub async fn close_stream(&self, stream_id: Ulid) {
        let event = self
            .send_blob_effect(BlobEffect::CloseConnection { stream_id })
            .await;
        if let Event::Blob(BlobEvent::Error(error)) = event {
            error!(error = ?error, "Failed to close rejected inbound bao stream");
        }
    }
}

/// Ends a stream that was refused before it was registered.
pub fn refuse_stream(mut stream: BiStream) {
    _ = stream.0.finish();
    _ = stream.1.stop(0u32.into());
}
