use crate::util::s3::{validate_s3_bucket_name, validate_s3_object_key};
use anyhow::{Result, anyhow, bail};
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::{Builder, FuturesAsyncReader, FuturesBytesStream, Operator, Reader, services};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Display;
use std::path::Path;
use std::str::FromStr;
use ulid::Ulid;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

pub async fn get_backend_operator(
    backend_type: &Backend,
    mut config: HashMap<String, String>,
    root: &str,
) -> Result<Operator> {
    // Extend config with bucket/root
    if backend_type == &Backend::S3 && !config.contains_key("bucket") {
        config.insert("bucket".to_string(), root.to_string());
    } else {
        match config.entry("root".to_string()) {
            Entry::Occupied(mut entry) => {
                let base_path = Path::new(entry.get());
                let extended_path = base_path.join(root);
                let path_str = extended_path
                    .to_str()
                    .ok_or_else(|| anyhow!("Invalid backend root path provided"))?;
                entry.insert(path_str.to_string());
            }
            Entry::Vacant(entry) => {
                entry.insert(root.to_string());
            }
        }
    }

    // Create and check backend operator
    let operator = match backend_type {
        Backend::S3 => init_service::<services::S3>(config)?,
        Backend::HTTP => init_service::<services::Http>(config)?,
        Backend::Memory => init_service::<services::Memory>(config)?,
        Backend::Postgres => init_service::<services::Postgresql>(config)?,
        Backend::FileSystem => init_service::<services::Fs>(config)?,
    };

    // Check operator without crashing
    if let Err(e) = operator.check().await {
        tracing::error!("Operator check failed: {e}");
    }

    // Return operator
    Ok(operator)
}

pub fn init_service<B: Builder>(cfg: HashMap<String, String>) -> Result<Operator> {
    let op = Operator::from_iter::<B>(cfg)?
        .layer(LoggingLayer::default())
        .layer(RetryLayer::new())
        .finish();

    Ok(op)
}

/// Creates a valid (frontend path, backend path) tuple for internal use
pub fn create_paths(
    origin_path: &str,
    bucket: Option<String>,
    group_id: &Ulid,
    skip_frontend: bool,
) -> Result<(Option<String>, String)> {
    // Validate bucket name if provided
    if let Some(bucket_name) = &bucket {
        if !validate_s3_bucket_name(bucket_name)? {
            bail!("Invalid bucket name: {}", bucket_name);
        }
    }

    // Split path and check that
    let origin_path = origin_path.trim_start_matches("/");
    let parts = origin_path.split("/").collect::<Vec<&str>>();
    let backend_path = format!("{}/{}", group_id, Ulid::new().to_string());
    let frontend_path = if skip_frontend {
        None
    } else {
        Some(match parts.len() {
            0 => return Err(anyhow::anyhow!("Empty path is invalid")),
            1 => {
                // Root level object (ingestion only)
                if validate_s3_object_key(parts[0])?
                    && let Some(bucket) = bucket
                {
                    format!("{}/{}/{}", group_id, bucket, parts[0])
                } else {
                    return Err(anyhow::anyhow!(
                        "Bucket is mandatory for objects in root path"
                    ));
                }
            }
            _ => {
                // Object that can be mapped to bucket/key
                format!("{}/{}", group_id, origin_path)
            }
        })
    };

    Ok((frontend_path, backend_path))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_paths() {
        let group_id = Ulid::from_string("01JZN1B6AJZ7JE858HQ8QCADKX").unwrap();
        let key = "foo/bar/baz.txt";
        let bucket = "aruna-1";

        let (frontend, backend) = create_paths(key, None, &group_id, true).unwrap();
        let backend_parts = backend.split("/").collect::<Vec<&str>>();
        assert_eq!(frontend, None);
        assert_eq!(backend_parts.len(), 2);
        assert_eq!(backend_parts[0], group_id.to_string());
        Ulid::from_string(backend_parts[1]).unwrap();

        let (frontend, backend) = create_paths(key, None, &group_id, false).unwrap();
        let backend_parts = backend.split("/").collect::<Vec<&str>>();
        assert_eq!(frontend, Some(format!("{group_id}/{key}")));
        assert_eq!(backend_parts.len(), 2);
        assert_eq!(backend_parts[0], group_id.to_string());
        Ulid::from_string(backend_parts[1]).unwrap();

        let (frontend, backend) =
            create_paths(key, Some(bucket.to_string()), &group_id, false).unwrap();
        let backend_parts = backend.split("/").collect::<Vec<&str>>();
        assert_eq!(frontend, Some(format!("{group_id}/{key}")));
        assert_eq!(backend_parts.len(), 2);
        assert_eq!(backend_parts[0], group_id.to_string());
        Ulid::from_string(backend_parts[1]).unwrap();

        let root_key = "baz.txt";
        let (frontend, backend) =
            create_paths(root_key, Some(bucket.to_string()), &group_id, false).unwrap();
        let backend_parts = backend.split("/").collect::<Vec<&str>>();
        assert_eq!(frontend, Some(format!("{group_id}/{bucket}/{root_key}")));
        assert_eq!(backend_parts.len(), 2);
        assert_eq!(backend_parts[0], group_id.to_string());
        Ulid::from_string(backend_parts[1]).unwrap();

        let root_key = "/baz.txt";
        let (frontend, backend) =
            create_paths(root_key, Some(bucket.to_string()), &group_id, false).unwrap();
        let backend_parts = backend.split("/").collect::<Vec<&str>>();
        assert_eq!(frontend, Some(format!("{group_id}/{bucket}{root_key}")));
        assert_eq!(backend_parts.len(), 2);
        assert_eq!(backend_parts[0], group_id.to_string());
        Ulid::from_string(backend_parts[1]).unwrap();
    }
}
