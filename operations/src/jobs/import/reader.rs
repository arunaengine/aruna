use std::future::Future;
use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

use aruna_blob::blob::BlobHandle;
use aruna_core::effects::BlobEffect;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::structs::BackendLocation;
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf, SeekFrom};

const RANGE_CHUNK_BYTES: u64 = 1024 * 1024;

type RangeFuture = Pin<Box<dyn Future<Output = io::Result<Bytes>> + Send + 'static>>;

pub struct HiddenRangeReader {
    handle: BlobHandle,
    location: BackendLocation,
    length: u64,
    position: u64,
    buffer_start: u64,
    buffer: Bytes,
    pending: Option<RangeFuture>,
}

impl HiddenRangeReader {
    pub fn new(handle: BlobHandle, location: BackendLocation, length: u64) -> Self {
        Self {
            handle,
            location,
            length,
            position: 0,
            buffer_start: 0,
            buffer: Bytes::new(),
            pending: None,
        }
    }

    fn buffered(&self) -> Option<&[u8]> {
        let offset = self.position.checked_sub(self.buffer_start)?;
        let offset = usize::try_from(offset).ok()?;
        self.buffer.get(offset..)
    }

    fn begin_fetch(&mut self) {
        let start = self.position;
        let end = start.saturating_add(RANGE_CHUNK_BYTES).min(self.length);
        self.pending = Some(fetch_range(
            self.handle.clone(),
            self.location.clone(),
            start..end,
        ));
    }
}

impl Clone for HiddenRangeReader {
    fn clone(&self) -> Self {
        Self::new(self.handle.clone(), self.location.clone(), self.length)
    }
}

impl AsyncRead for HiddenRangeReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        destination: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if self.position >= self.length || destination.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }
            if let Some(buffered) = self.buffered().filter(|buffered| !buffered.is_empty()) {
                let copied = buffered.len().min(destination.remaining());
                destination.put_slice(&buffered[..copied]);
                self.position = self.position.saturating_add(copied as u64);
                return Poll::Ready(Ok(()));
            }
            if self.pending.is_none() {
                self.begin_fetch();
            }
            let pending = self.pending.as_mut().expect("range fetch initialized");
            match pending.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    self.pending = None;
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Ok(bytes)) => {
                    self.pending = None;
                    self.buffer_start = self.position;
                    self.buffer = bytes;
                    if self.buffer.is_empty() {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "hidden blob range returned no bytes",
                        )));
                    }
                }
            }
        }
    }
}

impl AsyncSeek for HiddenRangeReader {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        self.position = seek_offset(self.position, self.length, position)?;
        self.pending = None;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}

fn seek_offset(current: u64, length: u64, position: SeekFrom) -> io::Result<u64> {
    let target = match position {
        SeekFrom::Start(target) => i128::from(target),
        SeekFrom::End(delta) => i128::from(length) + i128::from(delta),
        SeekFrom::Current(delta) => i128::from(current) + i128::from(delta),
    };
    if !(0..=i128::from(length)).contains(&target) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "ZIP seek is outside the hidden blob",
        ));
    }
    u64::try_from(target).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "ZIP seek offset is not representable",
        )
    })
}

fn fetch_range(handle: BlobHandle, location: BackendLocation, range: Range<u64>) -> RangeFuture {
    Box::pin(async move {
        let expected = range.end.saturating_sub(range.start);
        let event = handle
            .send_blob_effect(BlobEffect::ReadHiddenRange { location, range })
            .await;
        let Event::Blob(BlobEvent::HiddenRead { mut blob, .. }) = event else {
            return match event {
                Event::Blob(BlobEvent::Error(error)) => Err(io::Error::other(error.to_string())),
                other => Err(io::Error::other(format!(
                    "unexpected hidden range event: {other:?}"
                ))),
            };
        };
        let capacity = usize::try_from(expected).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "hidden range is too large")
        })?;
        let mut bytes = BytesMut::with_capacity(capacity);
        while let Some(chunk) = blob.next().await {
            let chunk = chunk.map_err(|error| io::Error::other(error.to_string()))?;
            bytes.extend_from_slice(&chunk);
            if bytes.len() > capacity {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "hidden range exceeded requested length",
                ));
            }
        }
        if bytes.len() != capacity {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "hidden range was truncated",
            ));
        }
        Ok(bytes.freeze())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seek_checks_bounds() {
        assert_eq!(seek_offset(4, 10, SeekFrom::Current(3)).unwrap(), 7);
        assert_eq!(seek_offset(4, 10, SeekFrom::End(-2)).unwrap(), 8);
        assert!(seek_offset(0, 10, SeekFrom::Current(-1)).is_err());
        assert!(seek_offset(0, 10, SeekFrom::Start(11)).is_err());
    }
}
