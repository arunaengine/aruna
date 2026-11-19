//! Sends one length-prefixed request frame to a peer and reads its answer frame and any body,
//! each read bounded in time and size, without knowing the protocol the frames carry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::effects::FrameShape;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

use crate::NetHandle;
use crate::streams::{BiStream, RecvStream};

/// Why a frame request failed, and whether the request may have reached the peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameError {
    pub sent: bool,
    pub message: String,
}

impl FrameError {
    fn unsent(message: impl Into<String>) -> Self {
        Self {
            sent: false,
            message: message.into(),
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
    pub async fn frame_request(
        &self,
        peer: NodeId,
        alpn: Alpn,
        frame: &[u8],
        shape: &FrameShape,
    ) -> Result<Vec<u8>, FrameError> {
        let length = u32::try_from(frame.len())
            .map_err(|_| FrameError::unsent("request frame exceeds the frame size"))?;
        let mut stream = timeout(shape.io_wait, self.open_stream(peer, alpn))
            .await
            .map_err(|_| FrameError::unsent("timed out opening the stream"))?
            .map_err(|error| FrameError::unsent(error.to_string()))?;
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
        close(&mut stream);
        #[cfg(feature = "test-hooks")]
        if self.take_dropped(&answer) {
            return Err(FrameError::sent(&stream, "answer dropped by a test hook"));
        }
        Ok(answer)
    }
}
