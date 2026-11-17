//! Carries notification request and answer frames between two nodes, one exchange per stream.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::structs::execution::notification_transport::{
    MAX_NOTIFICATION_SIZE, NotificationTransportMessage,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

use crate::NetHandle;
use crate::streams::BiStream;

pub const NOTIFICATION_IO_TIMEOUT: Duration = Duration::from_secs(10);

impl NetHandle {
    /// Sends one request frame to `node` and reads its answer frame.
    pub async fn notification_request(
        &self,
        node: NodeId,
        message: &NotificationTransportMessage,
    ) -> Result<NotificationTransportMessage, String> {
        let mut stream = (self.open_stream(node, Alpn::Notification).await)
            .map_err(|error| error.to_string())?;
        write_message(&mut stream, message).await?;
        stream.0.finish().map_err(|error| error.to_string())?;
        let response = read_message(&mut stream).await?;
        close_stream(&mut stream).await;
        Ok(response)
    }
}

/// Encodes and writes one frame.
pub async fn write_message(
    stream: &mut BiStream,
    message: &NotificationTransportMessage,
) -> Result<(), String> {
    let bytes = postcard::to_allocvec(message).map_err(|error| error.to_string())?;
    write_frame(stream, &bytes).await
}

/// Writes one length-prefixed frame body within the I/O timeout.
pub async fn write_frame(stream: &mut BiStream, bytes: &[u8]) -> Result<(), String> {
    if bytes.len() > MAX_NOTIFICATION_SIZE {
        return Err("notification message exceeds maximum size".to_string());
    }
    let write = async {
        let length = (bytes.len() as u32).to_be_bytes();
        stream
            .0
            .write_all(&length)
            .await
            .map_err(|error| error.to_string())?;
        stream
            .0
            .write_all(bytes)
            .await
            .map_err(|error| error.to_string())?;
        stream.0.flush().await.map_err(|error| error.to_string())
    };
    timeout(NOTIFICATION_IO_TIMEOUT, write)
        .await
        .map_err(|_| "timed out writing notification message".to_string())?
}

/// Reads one length-prefixed frame body within the I/O timeout, at most the frame bound.
pub async fn read_frame(stream: &mut BiStream) -> Result<Vec<u8>, String> {
    let read = async {
        let mut len_buf = [0u8; 4];
        stream
            .1
            .read_exact(&mut len_buf)
            .await
            .map_err(|error| error.to_string())?;
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > MAX_NOTIFICATION_SIZE {
            return Err("notification frame exceeds maximum size".to_string());
        }
        let mut bytes = vec![0u8; len];
        stream
            .1
            .read_exact(&mut bytes)
            .await
            .map_err(|error| error.to_string())?;
        Ok(bytes)
    };
    timeout(NOTIFICATION_IO_TIMEOUT, read)
        .await
        .map_err(|_| "timed out waiting for notification message".to_string())?
}

/// Reads and decodes one frame.
pub async fn read_message(stream: &mut BiStream) -> Result<NotificationTransportMessage, String> {
    let bytes = read_frame(stream).await?;
    postcard::from_bytes(&bytes).map_err(|error| error.to_string())
}

/// Checks that the request side sent nothing after its one frame.
pub async fn drain_request(stream: &mut BiStream) -> Result<(), String> {
    drain_reader(&mut stream.1).await
}

async fn drain_reader<R>(reader: &mut R) -> Result<(), String>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let mut trailing = [0u8; 1];
    match timeout(NOTIFICATION_IO_TIMEOUT, reader.read(&mut trailing)).await {
        Ok(Ok(0)) => Ok(()),
        Ok(Ok(_)) => Err("notification request stream has trailing bytes".to_string()),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err("timed out draining notification request stream".to_string()),
    }
}

pub async fn close_stream(stream: &mut BiStream) {
    let _ = stream.0.finish();
    let _ = stream.1.stop(0u32.into());
}

#[cfg(test)]
mod tests {
    use super::drain_reader;
    use tokio::io::{AsyncWriteExt, duplex};

    #[tokio::test]
    async fn drain_rejects_trailing() {
        let (mut writer, mut reader) = duplex(1);
        writer.write_all(&[1]).await.expect("write trailing byte");

        let error = drain_reader(&mut reader)
            .await
            .expect_err("trailing byte must be rejected");
        assert!(error.contains("trailing bytes"));
    }

    #[tokio::test]
    async fn drain_accepts_eof() {
        let (writer, mut reader) = duplex(1);
        drop(writer);

        drain_reader(&mut reader).await.expect("eof is clean");
    }

    #[tokio::test(start_paused = true)]
    async fn drain_times_out() {
        let (_writer, mut reader) = duplex(1);
        let task = tokio::spawn(async move { drain_reader(&mut reader).await });
        tokio::task::yield_now().await;
        tokio::time::advance(super::NOTIFICATION_IO_TIMEOUT).await;

        let error = task
            .await
            .expect("drain task completes")
            .expect_err("pending drain must time out");
        assert!(error.contains("timed out"));
    }
}
