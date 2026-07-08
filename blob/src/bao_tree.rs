use crate::error::BlobLibError;
use crate::hash::Hasher;
use aruna_net::streams::{RecvStream, SendStream};
use bytes::Bytes;
use futures::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use iroh_io::{AsyncSliceReader, AsyncSliceWriter, AsyncStreamReader, AsyncStreamWriter};
use opendal::{FuturesAsyncReader, FuturesAsyncWriter, Operator};
use std::{future::Future, io, time::Duration};
use tokio::io::AsyncWriteExt as TokioAsyncWriteExt;
use tokio::time::timeout;
use tracing::debug;

fn idle_timeout_error(action: &'static str, timeout: Duration) -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        format!("data-transfer idle timeout after {timeout:?} while {action}"),
    )
}

async fn with_transfer_idle_timeout<F, T>(
    future: F,
    timeout_duration: Duration,
    action: &'static str,
) -> io::Result<T>
where
    F: Future<Output = io::Result<T>>,
{
    timeout(timeout_duration, future)
        .await
        .map_err(|_| idle_timeout_error(action, timeout_duration))?
}

// ----- SendStream/RecvStream impls for bao_tree ----------

pub struct SendStreamWrapper<'a> {
    pub stream: &'a mut SendStream,
    pub idle_timeout: Duration,
}

impl<'a> SendStreamWrapper<'a> {
    pub fn new(stream: &'a mut SendStream, idle_timeout: Duration) -> Self {
        Self {
            stream,
            idle_timeout,
        }
    }
}

impl AsyncStreamWriter for SendStreamWrapper<'_> {
    async fn write(&mut self, data: &[u8]) -> std::io::Result<()> {
        debug!("Sending chunk with len: {}", data.len());
        with_transfer_idle_timeout(
            async {
                self.stream
                    .write_all(data)
                    .await
                    .map_err(|err| io::Error::other(err.to_string()))
            },
            self.idle_timeout,
            "writing bao chunk to network stream",
        )
        .await?;
        Ok(())
    }

    async fn write_bytes(&mut self, data: Bytes) -> std::io::Result<()> {
        with_transfer_idle_timeout(
            async {
                self.stream
                    .write_all(&data)
                    .await
                    .map_err(|err| io::Error::other(err.to_string()))
            },
            self.idle_timeout,
            "writing bao chunk to network stream",
        )
        .await?;
        Ok(())
    }

    async fn sync(&mut self) -> std::io::Result<()> {
        with_transfer_idle_timeout(
            async {
                self.stream
                    .flush()
                    .await
                    .map_err(|err| io::Error::other(err.to_string()))
            },
            self.idle_timeout,
            "flushing bao chunk stream",
        )
        .await?;
        Ok(())
    }
}

pub struct RecvStreamWrapper<'a> {
    pub stream: &'a mut RecvStream,
    pub idle_timeout: Duration,
}

impl<'a> RecvStreamWrapper<'a> {
    pub fn new(stream: &'a mut RecvStream, idle_timeout: Duration) -> Self {
        Self {
            stream,
            idle_timeout,
        }
    }
}

impl AsyncStreamReader for RecvStreamWrapper<'_> {
    async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
        debug!("Receiving chunk with len: {}", len);
        // Read bytes into buffer
        let mut bs = vec![0u8; len];
        with_transfer_idle_timeout(
            async {
                self.stream
                    .read_exact(&mut bs)
                    .await
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
            },
            self.idle_timeout,
            "reading bao chunk from network stream",
        )
        .await?;

        Ok(Bytes::from(bs))
    }

    async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
        // Read bytes into buffer
        let mut bs = [0u8; L];
        with_transfer_idle_timeout(
            async {
                self.stream
                    .read_exact(&mut bs)
                    .await
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
            },
            self.idle_timeout,
            "reading bao chunk from network stream",
        )
        .await?;

        Ok(bs)
    }
}

// ----- FuturesAsyncReader/FuturesAsyncWriter impls for bao_tree ----------
pub struct OpenDalWriter {
    pub writer: FuturesAsyncWriter,
    pub len: u64,
    pub hasher: Hasher,
    pub idle_timeout: Duration,
}

impl AsyncSliceWriter for OpenDalWriter {
    async fn write_at(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        debug!("[OpenDalWriter] Try to write data with offset {}", offset);
        with_transfer_idle_timeout(
            self.writer.write_all(data),
            self.idle_timeout,
            "writing replicated chunk to backend storage",
        )
        .await?;
        with_transfer_idle_timeout(
            self.writer.flush(),
            self.idle_timeout,
            "flushing replicated chunk to backend storage",
        )
        .await?;
        self.hasher.update(data);
        Ok(())
    }

    async fn write_bytes_at(&mut self, offset: u64, data: Bytes) -> std::io::Result<()> {
        debug!("[OpenDalWriter] Try to write bytes with offset {}", offset);
        with_transfer_idle_timeout(
            self.writer.write_all(&data),
            self.idle_timeout,
            "writing replicated chunk to backend storage",
        )
        .await?;
        with_transfer_idle_timeout(
            self.writer.flush(),
            self.idle_timeout,
            "flushing replicated chunk to backend storage",
        )
        .await?;
        self.hasher.update(&data);
        Ok(())
    }

    async fn set_len(&mut self, _len: u64) -> std::io::Result<()> {
        //Note: Nonsensical for most storage backends so just a no-op implementation
        Ok(())
    }

    async fn sync(&mut self) -> std::io::Result<()> {
        with_transfer_idle_timeout(
            self.writer.flush(),
            self.idle_timeout,
            "flushing replicated chunk to backend storage",
        )
        .await?;
        Ok(())
    }
}

impl OpenDalWriter {
    pub async fn new(
        operator: &Operator,
        storage_path: &str,
        blob_size: u64,
        idle_timeout: Duration,
    ) -> Result<Self, BlobLibError> {
        Ok(Self {
            writer: operator
                .writer(storage_path)
                .await?
                .into_futures_async_write(),
            len: blob_size,
            hasher: Hasher::new(),
            idle_timeout,
        })
    }
}

pub struct OpenDalReader {
    pub stream: FuturesAsyncReader,
    pub blob_size: u64,
    pub idle_timeout: Duration,
}

impl AsyncSliceReader for OpenDalReader {
    async fn read_at(&mut self, offset: u64, len: usize) -> std::io::Result<Bytes> {
        // Jump to offset
        with_transfer_idle_timeout(
            self.stream.seek(std::io::SeekFrom::Start(offset)),
            self.idle_timeout,
            "seeking source blob for bao transfer",
        )
        .await?;

        // Read bytes into buffer
        let mut bs = vec![0u8; len];
        with_transfer_idle_timeout(
            self.stream.read_exact(&mut bs),
            self.idle_timeout,
            "reading source blob chunk for bao transfer",
        )
        .await?;

        Ok(Bytes::from(bs))
    }

    async fn read_exact_at(&mut self, offset: u64, len: usize) -> std::io::Result<Bytes> {
        // Jump to offset
        with_transfer_idle_timeout(
            self.stream.seek(std::io::SeekFrom::Start(offset)),
            self.idle_timeout,
            "seeking source blob for bao transfer",
        )
        .await?;

        // Read bytes into buffer
        let mut bs = vec![0u8; len];
        with_transfer_idle_timeout(
            self.stream.read_exact(&mut bs),
            self.idle_timeout,
            "reading source blob chunk for bao transfer",
        )
        .await?;

        Ok(Bytes::from(bs))
    }

    async fn size(&mut self) -> std::io::Result<u64> {
        Ok(self.blob_size)
    }
}

impl OpenDalReader {
    pub async fn new(
        operator: &Operator,
        storage_path: &str,
        blob_size: u64,
        idle_timeout: Duration,
    ) -> Result<Self, BlobLibError> {
        Ok(OpenDalReader {
            stream: operator
                .reader(storage_path)
                .await?
                .into_futures_async_read(..)
                .await?,
            blob_size,
            idle_timeout,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{idle_timeout_error, with_transfer_idle_timeout};
    use std::io;
    use std::time::Duration;

    #[tokio::test]
    async fn transfer_idle_timeout_returns_timed_out_error() {
        let err = with_transfer_idle_timeout(
            std::future::pending::<io::Result<()>>(),
            Duration::from_millis(1),
            "reading bao chunk from network stream",
        )
        .await
        .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert_eq!(
            err.to_string(),
            "data-transfer idle timeout after 1ms while reading bao chunk from network stream"
        );
    }

    #[test]
    fn idle_timeout_error_uses_timed_out_kind() {
        let err = idle_timeout_error(
            "writing replicated chunk to backend storage",
            Duration::from_secs(1800),
        );

        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert_eq!(
            err.to_string(),
            "data-transfer idle timeout after 1800s while writing replicated chunk to backend storage"
        );
    }
}
