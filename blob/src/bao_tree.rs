use crate::error::BlobLibError;
use crate::hash::Hasher;
use bytes::Bytes;
use futures::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use iroh_io::{AsyncSliceReader, AsyncSliceWriter, AsyncStreamReader, AsyncStreamWriter};
use iroh_quinn::{RecvStream, SendStream};
use opendal::{FuturesAsyncReader, FuturesAsyncWriter, Operator};
use tokio::io::AsyncWriteExt as TokioAsyncWriteExt;
use tracing::debug;
// ----- SendStream/RecvStream impls for bao_tree ----------

pub struct SendStreamWrapper<'a>(pub &'a mut SendStream);
impl AsyncStreamWriter for SendStreamWrapper<'_> {
    async fn write(&mut self, data: &[u8]) -> std::io::Result<()> {
        debug!("Sending chunk with len: {}", data.len());
        self.0.write_all(data).await?;
        Ok(())
    }

    async fn write_bytes(&mut self, data: Bytes) -> std::io::Result<()> {
        self.0.write_all(data.iter().as_slice()).await?;
        Ok(())
    }

    async fn sync(&mut self) -> std::io::Result<()> {
        self.0.flush().await?;
        Ok(())
    }
}

pub struct RecvStreamWrapper<'a>(pub &'a mut RecvStream);
impl AsyncStreamReader for RecvStreamWrapper<'_> {
    async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
        debug!("Receiving chunk with len: {}", len);
        // Read bytes into buffer
        let mut bs = vec![0u8; len];
        self.0
            .read_exact(&mut bs)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        Ok(Bytes::from(bs))
    }

    async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
        // Read bytes into buffer
        let mut bs = [0u8; L];
        self.0
            .read_exact(&mut bs)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        Ok(bs)
    }
}

// ----- FuturesAsyncReader/FuturesAsyncWriter impls for bao_tree ----------
pub struct OpenDalWriter {
    pub writer: FuturesAsyncWriter,
    pub len: u64,
    pub hasher: Hasher,
}

impl AsyncSliceWriter for OpenDalWriter {
    async fn write_at(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        debug!("[OpenDalWriter] Try to write data with offset {}", offset);
        self.writer.write_all(data).await?;
        self.writer.flush().await?;
        self.hasher.update(data);
        Ok(())
    }

    async fn write_bytes_at(&mut self, offset: u64, data: Bytes) -> std::io::Result<()> {
        debug!("[OpenDalWriter] Try to write bytes with offset {}", offset);
        self.writer.write_all(&data).await?;
        self.writer.flush().await?;
        self.hasher.update(&data);
        Ok(())
    }

    async fn set_len(&mut self, _len: u64) -> std::io::Result<()> {
        //Note: Nonsensical for most storage backends so just a no-op implementation
        Ok(())
    }

    async fn sync(&mut self) -> std::io::Result<()> {
        self.writer.flush().await?;
        Ok(())
    }
}

impl OpenDalWriter {
    pub async fn new(
        operator: &Operator,
        storage_path: &str,
        blob_size: u64,
    ) -> Result<Self, BlobLibError> {
        Ok(Self {
            writer: operator
                .writer(storage_path)
                .await?
                .into_futures_async_write(),
            len: blob_size,
            hasher: Hasher::new(),
        })
    }
}

pub struct OpenDalReader {
    pub stream: FuturesAsyncReader,
    pub blob_size: u64,
}

impl AsyncSliceReader for OpenDalReader {
    async fn read_at(&mut self, offset: u64, len: usize) -> std::io::Result<Bytes> {
        // Jump to offset
        self.stream.seek(std::io::SeekFrom::Start(offset)).await?;

        // Read bytes into buffer
        let mut bs = vec![0u8; len];
        self.stream.read_exact(&mut bs).await?;

        Ok(Bytes::from(bs))
    }

    async fn read_exact_at(&mut self, offset: u64, len: usize) -> std::io::Result<Bytes> {
        // Jump to offset
        self.stream.seek(std::io::SeekFrom::Start(offset)).await?;

        // Read bytes into buffer
        let mut bs = vec![0u8; len];
        self.stream.read_exact(&mut bs).await?;

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
    ) -> Result<Self, BlobLibError> {
        Ok(OpenDalReader {
            stream: operator
                .reader(storage_path)
                .await?
                .into_futures_async_read(..)
                .await?,
            blob_size,
        })
    }
}
