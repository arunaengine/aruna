use crate::error::BlobLibError;
use aruna_core::errors::BlobError;
use aruna_core::structs::{Backend, BackendConfig};
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::{Builder, Operator, services};
use std::collections::HashMap;
use ulid::Ulid;

pub fn init_backend_operator(
    mut config: BackendConfig,
    bucket: String,
) -> Result<Operator, BlobError> {
    // Insert root and bucket (in case of S3 storage) into service config
    config
        .service_config
        .insert("root".to_string(), config.root);

    // Create and check backend
    let operator = match config.backend_type {
        Backend::S3 => {
            config.service_config.insert("bucket".to_string(), bucket);
            init_service::<services::S3>(config.service_config)?
        }
        Backend::HTTP => init_service::<services::Http>(config.service_config)?,
        Backend::Postgres => init_service::<services::Postgresql>(config.service_config)?,
        Backend::FileSystem => init_service::<services::Fs>(config.service_config)?,
    };
    //_check_operator(&operator).await?;

    // Return operator
    Ok(operator)
}

pub fn init_operator(
    backend_type: Backend,
    config: HashMap<String, String>,
) -> Result<Operator, BlobError> {
    Ok(match backend_type {
        Backend::S3 => init_service::<services::S3>(config)?,
        Backend::HTTP => init_service::<services::Http>(config)?,
        Backend::Postgres => init_service::<services::Postgresql>(config)?,
        Backend::FileSystem => init_service::<services::Fs>(config)?,
    })
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
