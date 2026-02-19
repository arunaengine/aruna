use crate::blob::Backend;
use crate::error::BlobLibError;
use aruna_core::errors::BlobError;
use aruna_core::structs::BackendConfig;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::{Builder, Operator, services};
use std::collections::HashMap;
use std::str::FromStr;
use ulid::Ulid;

pub fn init_backend_operator(
    mut config: BackendConfig,
    bucket: String,
) -> Result<Operator, BlobError> {
    // Insert root and bucket (in case of S3 storage) into service config
    let backend_type = Backend::from_str(&config.backend_type)?;
    config
        .service_config
        .insert("root".to_string(), config.root);
    if Backend::S3 == backend_type {
        config.service_config.insert("bucket".to_string(), bucket);
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

pub fn init_service<B: Builder>(cfg: HashMap<String, String>) -> Result<Operator, BlobError> {
    Ok(Operator::from_iter::<B>(cfg)
        .map_err(|e| BlobError::OperatorCreationFailed(e.to_string()))?
        .layer(LoggingLayer::default())
        .layer(RetryLayer::new())
        .finish())
}

pub async fn _check_operator(op: &Operator) -> Result<(), BlobLibError> {
    Ok(op.check().await?)
}

pub fn create_storage_path(bucket: &str, path: &str) -> String {
    format!("{}/{}_{}", bucket, path, Ulid::new())
}
