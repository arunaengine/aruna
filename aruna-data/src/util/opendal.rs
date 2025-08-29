use crate::error::ArunaDataError;
use crate::util::s3::{validate_s3_bucket_name, validate_s3_object_key};
use anyhow::Result;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::{Builder, FuturesAsyncReader, FuturesBytesStream, Operator, Reader, services};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Display;
use std::path::Path;
use std::str::FromStr;
use ulid::Ulid;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
// Currently supported backends
pub enum Backend {
    #[default]
    S3,
    HTTP,
    Memory,
    Postgres,
    FileSystem,
}

impl Default for &Backend {
    fn default() -> Self {
        &Backend::S3
    }
}

impl FromStr for Backend {
    type Err = ArunaDataError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "s3" => Ok(Backend::S3),
            "http" => Ok(Backend::HTTP),
            "memory" => Ok(Backend::Memory),
            "postgres" => Ok(Backend::Postgres),
            "filesystem" => Ok(Backend::FileSystem),
            _ => Err(ArunaDataError::ServerError("unknown backend".to_string())),
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
) -> Result<Operator, ArunaDataError> {
    // Extend config with bucket/root
    if backend_type == &Backend::S3 && !config.contains_key("bucket") {
        config.insert("bucket".to_string(), root.to_string());
    } else {
        match config.entry("root".to_string()) {
            Entry::Occupied(mut entry) => {
                let base_path = Path::new(entry.get());
                let extended_path = base_path.join(root);
                let path_str = extended_path.to_str().ok_or_else(|| {
                    ArunaDataError::ServerError("Invalid backend root path provided".to_string())
                })?;
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

pub fn init_service<B: Builder>(cfg: HashMap<String, String>) -> Result<Operator, ArunaDataError> {
    let op = Operator::from_iter::<B>(cfg)?
        .layer(LoggingLayer::default())
        .layer(RetryLayer::new())
        .finish();

    Ok(op)
}

pub fn create_paths(
    key: Option<&str>,
    bucket: &str,
    group_id: &Ulid,
    skip_frontend: bool,
) -> Result<(Option<String>, String), ArunaDataError> {
    // Split path and check that
    let backend_path = format!("{}/{}", group_id, Ulid::new().to_string());
    let frontend_path = if skip_frontend {
        None
    } else {
        Some(match key {
            Some(key) => {
                let key = key.trim_start_matches("/");
                // This shouldnt be needed, because bucket should already exist,
                // but just for safety...
                if !validate_s3_bucket_name(bucket)? {
                    return Err(ArunaDataError::ServerError(format!(
                        "Invalid bucket name: {}",
                        bucket
                    )));
                }
                if !validate_s3_object_key(key)? {
                    return Err(ArunaDataError::ServerError(format!(
                        "Invalid key: {}",
                        bucket
                    )));
                }
                format!("{}/{}/{}", group_id, bucket, key)
            }

            None => {
                if !validate_s3_bucket_name(bucket)? {
                    return Err(ArunaDataError::ServerError(format!(
                        "Invalid bucket name: {}",
                        bucket
                    )));
                }
                format!("{}/{}", group_id, bucket)
            }
        })
    };
    Ok((frontend_path, backend_path))
}

// /// Creates a valid (frontend path, backend path) tuple for internal use
// pub fn create_paths(
//     origin_path: &str,
//     bucket: Option<String>,
//     group_id: &Ulid,
//     skip_frontend: bool,
// ) -> Result<(Option<String>, String), ArunaDataError> {
//     // Validate bucket name if provided
//     if let Some(bucket_name) = &bucket {
//         if !validate_s3_bucket_name(bucket_name)? {
//             ArunaDataError::ServerError(format!("Invalid bucket name: {}", bucket_name));
//         }
//     }
//
//     // Split path and check that
//     let origin_path = origin_path.trim_start_matches("/");
//     let parts = origin_path.split("/").collect::<Vec<&str>>();
//     let backend_path = format!("{}/{}", group_id, Ulid::new().to_string());
//     let frontend_path = if skip_frontend {
//         None
//     } else {
//         Some(match parts.len() {
//             0 => return Err(ArunaDataError::ServerError("Empty path is invalid".to_string())),
//             1 => {
//                 // Root level object (ingestion only)
//                 if validate_s3_object_key(parts[0])?
//                     && let Some(bucket) = bucket
//                 {
//                     format!("{}/{}/{}", group_id, bucket, parts[0])
//                 } else {
//                     return Err(ArunaDataError::ServerError(
//                         "Bucket is mandatory for objects in root path".to_string()
//                     ));
//                 }
//             }
//             _ => {
//                 // Object that can be mapped to bucket/key
//                 format!("{}/{}", group_id, origin_path)
//             }
//         })
//     };
//
//     Ok((frontend_path, backend_path))
// }

pub async fn get_reader(
    operator: &Operator,
    path: &str,
    concurrent: Option<usize>,
    chunk_size: Option<usize>,
) -> Result<Reader, ArunaDataError> {
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
) -> Result<FuturesBytesStream, ArunaDataError> {
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
) -> Result<FuturesAsyncReader, ArunaDataError> {
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
        let invalid_bucket = "アルナ#aruna";
        let invalid_key = "file<less>greater.txt";
        let valid_key = "foo/bar/baz.txt";
        let valid_bucket = "aruna-1";

        let (frontend, backend) =
            create_paths(Some(valid_key), valid_bucket, &group_id, true).unwrap();
        let backend_parts = backend.split("/").collect::<Vec<&str>>();
        assert_eq!(frontend, None);
        assert_eq!(backend_parts.len(), 2);
        assert_eq!(backend_parts[0], group_id.to_string());
        Ulid::from_string(backend_parts[1]).unwrap();

        let (frontend, backend) = create_paths(None, valid_bucket, &group_id, false).unwrap();
        let backend_parts = backend.split("/").collect::<Vec<&str>>();
        assert_eq!(frontend, Some(format!("{group_id}/{valid_bucket}")));
        assert_eq!(backend_parts.len(), 2);
        assert_eq!(backend_parts[0], group_id.to_string());
        Ulid::from_string(backend_parts[1]).unwrap();

        let (frontend, backend) =
            create_paths(Some(valid_key), valid_bucket, &group_id, false).unwrap();
        let backend_parts = backend.split("/").collect::<Vec<&str>>();
        assert_eq!(
            frontend,
            Some(format!("{group_id}/{valid_bucket}/{valid_key}"))
        );
        assert_eq!(backend_parts.len(), 2);
        assert_eq!(backend_parts[0], group_id.to_string());
        Ulid::from_string(backend_parts[1]).unwrap();

        assert!(create_paths(Some(invalid_key), valid_bucket, &group_id, false).is_err());
        assert!(create_paths(Some(valid_key), invalid_bucket, &group_id, false).is_err());
        assert!(create_paths(Some(invalid_key), invalid_bucket, &group_id, false).is_err());
    }
}
