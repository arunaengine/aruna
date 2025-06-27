use anyhow::Result;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::{Builder, FuturesAsyncReader, FuturesBytesStream, Operator, Reader, services};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize)]
// Currently supported backends
pub enum Backend {
    S3,
    HTTP,
    Memory,
    Postgres,
    FileSystem,
}

impl FromStr for Backend {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "s3" => Ok(Backend::S3),
            "http" => Ok(Backend::HTTP),
            "memory" => Ok(Backend::Memory),
            "postgres" => Ok(Backend::Postgres),
            "filesystem" => Ok(Backend::FileSystem),
            _ => Err(anyhow::Error::msg("unknown backend")),
        }
    }
}

impl Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Backend::S3 => write!(f, "s3"),
            Backend::HTTP => write!(f, "http"),
            Backend::Memory => write!(f, "memory"),
            Backend::Postgres => write!(f, "postgres"),
            Backend::FileSystem => write!(f, "filesystem"),
        }
    }
}

pub async fn get_operator(backend: &Backend, config: HashMap<String, String>) -> Result<Operator> {
    let op = match backend {
        Backend::S3 => init_service::<services::S3>(config)?,
        Backend::HTTP => init_service::<services::Http>(config)?,
        Backend::Memory => init_service::<services::Memory>(config)?,
        Backend::Postgres => init_service::<services::Postgresql>(config)?,
        Backend::FileSystem => init_service::<services::Fs>(config)?,
    };

    Ok(op)
}

pub fn init_service<B: Builder>(cfg: HashMap<String, String>) -> Result<Operator> {
    let op = Operator::from_iter::<B>(cfg)?
        .layer(LoggingLayer::default())
        .layer(RetryLayer::new())
        .finish();

    Ok(op)
}

pub async fn get_reader(
    operator: &Operator,
    path: &str,
    concurrent: Option<usize>,
    chunk_size: Option<usize>,
) -> Result<Reader> {
    let mut builder = operator.reader_with(path);

    if let Some(concurrent) = concurrent {
        builder = builder.concurrent(concurrent);
    }
    if let Some(chunk_size) = chunk_size {
        builder = builder.chunk(chunk_size);
    }

    Ok(builder.await?)
}

pub async fn get_data_stream(
    operator: &Operator,
    path: &str,
    concurrent: Option<usize>,
    chunk_size: Option<usize>,
) -> Result<FuturesBytesStream> {
    let mut builder = operator.reader_with(path);

    if let Some(concurrent) = concurrent {
        builder = builder.concurrent(concurrent);
    }
    if let Some(chunk_size) = chunk_size {
        builder = builder.chunk(chunk_size);
    }

    Ok(builder.await?.into_bytes_stream(..).await?)
}

pub async fn get_data_async_reader(
    operator: &Operator,
    path: &str,
    concurrent: Option<usize>,
    chunk_size: Option<usize>,
) -> Result<FuturesAsyncReader> {
    let mut builder = operator.reader_with(path);

    if let Some(concurrent) = concurrent {
        builder = builder.concurrent(concurrent);
    }
    if let Some(chunk_size) = chunk_size {
        builder = builder.chunk(chunk_size);
    }

    Ok(builder.await?.into_futures_async_read(..).await?)
}
