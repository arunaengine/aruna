use crate::blob::Backend;
use crate::error::BlobLibError;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::structs::BackendConfig;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::{Builder, Operator, services};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::Path;
use std::str::FromStr;

pub enum OpenDalService {
    S3,
    FileSystem,
    Http,
    Postgres,
}

pub fn get_backend_operator(mut config: BackendConfig) -> Result<Operator, BlobLibError> {
    let backend_type = Backend::from_str(&config.backend_type)?;
    // Extend config with bucket/root
    if backend_type == Backend::S3 && !config.service_config.contains_key("bucket") {
        config
            .service_config
            .insert("bucket".to_string(), config.root.to_string());
    } else {
        match config.service_config.entry("root".to_string()) {
            Entry::Occupied(mut entry) => {
                let base_path = Path::new(entry.get());
                let extended_path = base_path.join(config.root);
                let path_str = extended_path.to_str().ok_or_else(|| {
                    BlobLibError::ConversionError("Invalid backend root path provided".to_string())
                })?;
                entry.insert(path_str.to_string());
            }
            Entry::Vacant(entry) => {
                entry.insert(config.root.to_string());
            }
        }
    }

    // Create and check backend
    let operator = match backend_type {
        Backend::S3 => init_service::<services::S3>(config.service_config)?,
        Backend::HTTP => init_service::<services::Http>(config.service_config)?,
        Backend::Postgres => init_service::<services::Postgresql>(config.service_config)?,
        Backend::FileSystem => init_service::<services::Fs>(config.service_config)?,
    };

    // Return operator
    Ok(operator)
}

pub fn emit_backend_operator(bucket: Option<String>, mut config: BackendConfig) -> BlobEvent {
    if let Some(bucket) = bucket {
        config.service_config.insert("bucket".to_string(), bucket);
    }

    match get_backend_operator(config) {
        Ok(operator) => BlobEvent::OperatorCreated { operator },
        Err(_) => BlobEvent::Error {
            error: BlobError::OperatorCreationFailed,
        },
    }
}
pub fn init_service<B: Builder>(cfg: HashMap<String, String>) -> Result<Operator, BlobLibError> {
    Ok(Operator::from_iter::<B>(cfg)?
        .layer(LoggingLayer::default())
        .layer(RetryLayer::new())
        .finish())
}

pub async fn check_operator(op: &Operator) -> Result<(), BlobLibError> {
    Ok(op.check().await?)
}

pub fn create_storage_path(prefix: String, bucket: String, hash: String) -> String {
    //TODO
    format!("{}_{}/{}", prefix, bucket, hash)
}
