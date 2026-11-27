//! Bodies of prepared SMB writes: the effective file image read in bounded chunks only once
//! the write consumes them, so a large file is never held in memory as a whole.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;
use std::task::Poll;

use aruna_core::stream::{BackendStream, ByteStream};
use aruna_ops_data::vfs::{CHUNK_BYTES, VfsError};
use bytes::Bytes;
use tokio::sync::mpsc;

/// Chunks a body producer reads ahead of the writer that consumes them.
const READ_AHEAD: usize = 2;

/// Reads a file image chunk by chunk through `read`, called with each offset and length.
struct ChunkReader<R> {
    read: R,
    size: u64,
}

impl<R, F> ChunkReader<R>
where
    R: FnMut(u64, u32) -> F,
    F: Future<Output = Result<Bytes, VfsError>>,
{
    async fn run(mut self, sender: mpsc::Sender<Result<Bytes, VfsError>>) {
        let mut offset = 0;
        while offset < self.size {
            let length = (self.size - offset).min(CHUNK_BYTES);
            let length = u32::try_from(length).unwrap_or(u32::MAX);
            // A short chunk means the image changed under the read; the write must fail.
            let item = match (self.read)(offset, length).await {
                Ok(chunk) if chunk.len() as u64 != u64::from(length) => Err(VfsError::Conflict),
                item => item,
            };
            let failed = item.is_err();
            if sender.send(item).await.is_err() || failed {
                return;
            }
            offset += u64::from(length);
        }
    }
}

/// The reader before the writer first polls, then the chunks it sends.
enum Feed<R> {
    Waiting(Option<Box<ChunkReader<R>>>),
    Reading(mpsc::Receiver<Result<Bytes, VfsError>>),
}

/// The `size` bytes `read` yields in chunks of at most [`CHUNK_BYTES`], read on first poll.
pub(super) fn chunked_body<R, F>(size: u64, read: R) -> ByteStream
where
    R: FnMut(u64, u32) -> F + Send + Sync + 'static,
    F: Future<Output = Result<Bytes, VfsError>> + Send + 'static,
{
    if size == 0 {
        return BackendStream::empty();
    }
    let mut feed = Feed::Waiting(Some(Box::new(ChunkReader { read, size })));
    let stream = futures_util::stream::poll_fn(move |task| {
        if let Feed::Waiting(reader) = &mut feed {
            let (sender, receiver) = mpsc::channel(READ_AHEAD);
            if let Some(reader) = reader.take() {
                tokio::spawn(reader.run(sender));
            }
            feed = Feed::Reading(receiver);
        }
        match &mut feed {
            Feed::Reading(receiver) => receiver.poll_recv(task),
            Feed::Waiting(_) => Poll::Ready(None),
        }
    });
    BackendStream::new(stream)
}
