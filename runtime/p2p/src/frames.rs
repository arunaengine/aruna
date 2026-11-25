//! Sends one length-prefixed request frame to a peer and reads its answer frame and any body,
//! each read bounded in time and size, without knowing the protocol the frames carry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Instant;

use aruna_core::alpn::Alpn;
use aruna_core::effects::FrameShape;
use aruna_core::id::NodeId;
use aruna_core::telemetry::{record_elapsed_ms, record_error};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use tracing::{Span, field};

use crate::NetHandle;
use crate::streams::{BiStream, RecvStream};

/// Why a frame request failed, whether it may have reached the peer, and whether the failure
/// can be attributed to the remote peer rather than local capacity or frame limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameError {
    pub sent: bool,
    pub peer_fault: bool,
    pub message: String,
}

impl FrameError {
    fn local(message: impl Into<String>) -> Self {
        Self {
            sent: false,
            peer_fault: false,
            message: message.into(),
        }
    }

    fn unsent(message: impl Into<String>) -> Self {
        Self {
            sent: false,
            peer_fault: true,
            message: message.into(),
        }
    }

    fn from_open(error: crate::NetError) -> Self {
        Self {
            sent: false,
            peer_fault: error.is_peer_fault(),
            message: error.to_string(),
        }
    }

    // A lost stream carries the peer's close reason, which the stream error itself omits.
    fn sent(stream: &BiStream, message: impl Into<String>) -> Self {
        let message = message.into();
        let message = match stream.close_reason() {
            Some(reason) => format!("{message}: {reason}"),
            None => message,
        };
        Self {
            sent: true,
            peer_fault: true,
            message,
        }
    }
}

// The payload of one answer frame no larger than `cap`; an answer travels in its request's class.
async fn read_frame(recv: &mut RecvStream, class: u8, cap: usize) -> Result<Vec<u8>, String> {
    let mut header = [0u8; 5];
    recv.read_exact(&mut header)
        .await
        .map_err(|error| error.to_string())?;
    if header[0] != class {
        return Err("answer frame class differs from the request's".to_string());
    }
    let length = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    if length > cap {
        return Err("answer frame exceeds its cap".to_string());
    }
    let mut payload = vec![0u8; length];
    recv.read_exact(&mut payload)
        .await
        .map_err(|error| error.to_string())?;
    Ok(payload)
}

fn close(stream: &mut BiStream) {
    let _ = stream.0.finish();
    let _ = stream.1.stop(0u32.into());
}

impl NetHandle {
    /// Sends `frame` to `peer` in the class `shape` names and answers with the answer frame's
    /// payload, followed by the body when `shape` caps one.
    #[tracing::instrument(
        name = "net.frame.request",
        level = "debug",
        skip(self, frame, shape),
        fields(
            peer = ?peer,
            class = shape.class,
            open_stream_ms = field::Empty,
            write_ms = field::Empty,
            read_ms = field::Empty,
            elapsed_ms = field::Empty,
            error = field::Empty,
        )
    )]
    pub async fn frame_request(
        &self,
        peer: NodeId,
        alpn: Alpn,
        frame: &[u8],
        shape: &FrameShape,
    ) -> Result<Vec<u8>, FrameError> {
        let span = Span::current();
        let started = Instant::now();
        let result = self.exchange(peer, alpn, frame, shape, &span).await;
        record_elapsed_ms(&span, "elapsed_ms", started);
        if let Err(error) = &result {
            record_error(&span, &error.message);
        }
        result
    }

    async fn exchange(
        &self,
        peer: NodeId,
        alpn: Alpn,
        frame: &[u8],
        shape: &FrameShape,
        span: &Span,
    ) -> Result<Vec<u8>, FrameError> {
        let started = Instant::now();
        let length = u32::try_from(frame.len())
            .map_err(|_| FrameError::local("request frame exceeds the frame size"))?;
        let mut stream = timeout(shape.io_wait, self.open_stream(peer, alpn))
            .await
            .map_err(|_| FrameError::unsent("timed out opening the stream"))?
            .map_err(FrameError::from_open)?;
        record_elapsed_ms(span, "open_stream_ms", started);
        let write_started = Instant::now();
        let written = timeout(shape.io_wait, async {
            stream.0.write_all(&[shape.class]).await?;
            stream.0.write_all(&length.to_be_bytes()).await?;
            stream.0.write_all(frame).await?;
            stream.0.flush().await
        })
        .await;
        match written {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(FrameError::sent(&stream, error.to_string())),
            Err(_) => return Err(FrameError::sent(&stream, "timed out writing the request")),
        }
        if let Err(error) = stream.0.finish() {
            return Err(FrameError::sent(&stream, error.to_string()));
        }
        let read_started = Instant::now();
        record_elapsed_ms(span, "write_ms", write_started);
        let mut answer = match timeout(
            shape.answer_wait,
            read_frame(&mut stream.1, shape.class, shape.answer_cap),
        )
        .await
        {
            Ok(Ok(answer)) => answer,
            Ok(Err(error)) => return Err(FrameError::sent(&stream, error)),
            Err(_) => {
                return Err(FrameError::sent(
                    &stream,
                    "timed out waiting for the answer",
                ));
            }
        };
        // The body ends with the stream; a peer that keeps it open runs into the wait.
        if let Some(cap) = shape.body_cap {
            let cap = usize::try_from(cap).unwrap_or(usize::MAX);
            match timeout(shape.io_wait, stream.1.read_to_end(cap)).await {
                Ok(Ok(body)) => answer.extend(body),
                Ok(Err(error)) => return Err(FrameError::sent(&stream, error.to_string())),
                Err(_) => return Err(FrameError::sent(&stream, "timed out reading the body")),
            }
        }
        record_elapsed_ms(span, "read_ms", read_started);
        close(&mut stream);
        #[cfg(feature = "test-hooks")]
        if self.take_dropped(&answer) {
            return Err(FrameError::sent(&stream, "answer dropped by a test hook"));
        }
        Ok(answer)
    }
}

#[cfg(test)]
mod tests {
    use super::FrameError;
    use crate::NetError;

    #[test]
    fn local_skips_fault() {
        let local = FrameError::from_open(NetError::LocalUnavailable(
            "too many pooled connections".to_string(),
        ));
        assert!(!local.sent);
        assert!(!local.peer_fault);

        let peer = FrameError::from_open(NetError::Connection("refused".to_string()));
        assert!(!peer.sent);
        assert!(peer.peer_fault);

        let stream = FrameError::from_open(NetError::Stream("open_bi timed out".to_string()));
        assert!(!stream.sent);
        assert!(stream.peer_fault);
    }
}
