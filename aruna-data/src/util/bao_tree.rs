use crate::io::io_handler::ObjectInfo;
use bytes::Bytes;
use futures::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SinkExt};
use iroh::endpoint::{RecvStream, SendStream};
use iroh_io::{AsyncSliceReader, AsyncSliceWriter, AsyncStreamReader, AsyncStreamWriter};
use opendal::{Buffer, BufferSink, FuturesAsyncReader, FuturesAsyncWriter};
use tokio::io::AsyncWriteExt as TokioAsyncWriteExt;
use tracing::debug;

pub struct FuturesAsyncReaderWrapper {
    pub stream: FuturesAsyncReader,
    pub info: ObjectInfo,
}

impl AsyncSliceReader for FuturesAsyncReaderWrapper {
    async fn read_at(&mut self, offset: u64, len: usize) -> std::io::Result<Bytes> {
        // Jump to offset
        self.stream.seek(std::io::SeekFrom::Start(offset)).await?;

        // Read bytes into buffer
        let mut bs = vec![0u8; len];
        self.stream.read(&mut bs).await?;

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
        Ok(self.info.file_size)
    }
}

pub struct RecvStreamWrapper(pub RecvStream);
impl AsyncStreamReader for RecvStreamWrapper {
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

pub struct SendStreamWrapper(pub SendStream);
impl AsyncStreamWriter for SendStreamWrapper {
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

pub struct OpenDalWriter {
    pub writer: FuturesAsyncWriter,
    pub len: u64,
}
impl AsyncSliceWriter for OpenDalWriter {
    async fn write_at(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        debug!("[OpenDalWriter] Try to write data with offset {}", offset);
        self.writer.write_all(data).await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn write_bytes_at(&mut self, offset: u64, data: Bytes) -> std::io::Result<()> {
        debug!("[OpenDalWriter] Try to write bytes with offset {}", offset);
        self.writer.write_all(data.as_ref()).await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn set_len(&mut self, _len: u64) -> std::io::Result<()> {
        Ok(())
    }

    async fn sync(&mut self) -> std::io::Result<()> {
        self.writer.flush().await?;
        Ok(())
    }
}

pub struct OpenDalSinkWriter {
    pub writer: BufferSink,
    pub len: u64,
}
impl AsyncSliceWriter for OpenDalSinkWriter {
    async fn write_at(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        debug!("Try to write data with offset {}", offset);
        self.writer.send(Buffer::from(data.to_vec())).await?;
        //self.writer.send(Bytes::from(data.to_vec())).await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn write_bytes_at(&mut self, offset: u64, data: Bytes) -> std::io::Result<()> {
        debug!("Try to write bytes with offset {}", offset);
        self.writer.send(Buffer::from(data.to_vec())).await?;
        //self.writer.send(Bytes::from(data.to_vec())).await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn set_len(&mut self, _len: u64) -> std::io::Result<()> {
        Ok(())
    }

    async fn sync(&mut self) -> std::io::Result<()> {
        self.writer.flush().await?;
        Ok(())
    }
}
