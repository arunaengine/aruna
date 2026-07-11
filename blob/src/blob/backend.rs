use super::BlobHandler;
use crate::error::BlobLibError;
use crate::opendal::init_operator;
use crate::s3::make_bucket;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::BUCKET_STATS_DB;
use aruna_core::structs::{Backend, BackendBucket, BackendLocation, ensure_confined_relative_path};
use opendal::Operator;
use std::path::PathBuf;
use ulid::Ulid;

impl BlobHandler {
    pub(super) async fn ensure_multipart_bucket(&self) -> Result<(), BlobLibError> {
        if self.backend_config.backend_type != Backend::S3 {
            return Ok(());
        }

        let Some(bucket) = self.backend_config.multipart_bucket.as_deref() else {
            return Ok(());
        };

        make_bucket(bucket, &self.backend_config.service_config)
            .await
            .map_err(|err| BlobLibError::IoError(std::io::Error::other(err.to_string())))
    }

    pub(super) fn multipart_bucket(&self) -> Result<&str, BlobError> {
        self.backend_config
            .multipart_bucket
            .as_deref()
            .ok_or_else(|| {
                BlobError::OperatorCreationFailed("multipart bucket not configured".to_string())
            })
    }

    pub(super) async fn eval_backend_bucket(&self) -> Result<String, BlobError> {
        if let Some(bucket) = self.backend_config.service_config.get("bucket") {
            return Ok(bucket.clone());
        }

        let buckets = self.fetch_bucket_stats().await?;
        if let Some(bucket_max_size) = self.backend_config.max_bucket_size {
            for bucket in buckets {
                if bucket.load < bucket_max_size {
                    return Ok(bucket.name);
                }
            }
        } else if let Some(bucket) = buckets.into_iter().next() {
            return Ok(bucket.name);
        }

        let bucket_name = generate_bucket_name(self.backend_config.bucket_prefix.as_deref());

        if Backend::S3 == self.backend_config.backend_type {
            make_bucket(&bucket_name, &self.backend_config.service_config).await?;
        }

        Ok(bucket_name)
    }

    pub(super) async fn fetch_bucket_stats(&self) -> Result<Vec<BackendBucket>, ConversionError> {
        let mut buckets = Vec::new();
        let mut start_after = None;

        loop {
            let event = self
                .storage
                .send_effect(Effect::Storage(StorageEffect::Iter {
                    key_space: BUCKET_STATS_DB.to_string(),
                    prefix: self
                        .backend_config
                        .bucket_prefix
                        .clone()
                        .map(|prefix| prefix.into()),
                    start: start_after.clone().map(IterStart::After),
                    limit: 1024,
                    txn_id: None,
                }))
                .await;

            let Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) = event
            else {
                return Ok(Vec::new());
            };

            buckets.extend(
                values
                    .into_iter()
                    .map(BackendBucket::try_from)
                    .collect::<Result<Vec<BackendBucket>, ConversionError>>()?,
            );

            if let Some(next_start_after) = next_start_after {
                start_after = Some(next_start_after);
            } else {
                break;
            }
        }

        Ok(buckets)
    }

    pub(super) async fn bucket_load(&self, bucket: &str) -> Result<u64, BlobError> {
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: BUCKET_STATS_DB.to_string(),
                key: bucket.as_bytes().to_vec().into(),
                txn_id: None,
            }))
            .await;

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return Err(BlobError::ReadError(
                "failed to read bucket stats".to_string(),
            ));
        };

        match value {
            Some(value) => Ok(u64::from_le_bytes(
                value.as_ref().try_into().map_err(ConversionError::from)?,
            )),
            None => Ok(0),
        }
    }

    pub(super) async fn write_bucket_load(&self, bucket: &str, load: u64) -> Result<(), BlobError> {
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: BUCKET_STATS_DB.to_string(),
                key: bucket.as_bytes().to_vec().into(),
                value: load.to_le_bytes().to_vec().into(),
                txn_id: None,
            }))
            .await;

        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(BlobError::ReadError(format!(
                "failed to write bucket stats: {error}"
            ))),
            _ => Err(BlobError::ReadError(
                "unexpected storage event while writing bucket stats".to_string(),
            )),
        }
    }

    pub(super) fn is_stats_managed_bucket(&self, bucket: &str) -> bool {
        self.backend_config.multipart_bucket.as_deref() != Some(bucket)
    }

    pub(super) async fn increment_bucket_load(&self, bucket: &str) -> Result<(), BlobError> {
        if !self.is_stats_managed_bucket(bucket) {
            return Ok(());
        }

        let load = self.bucket_load(bucket).await?;
        self.write_bucket_load(bucket, load.saturating_add(1)).await
    }

    pub(super) async fn decrement_bucket_load(&self, bucket: &str) -> Result<(), BlobError> {
        if !self.is_stats_managed_bucket(bucket) {
            return Ok(());
        }

        let load = self.bucket_load(bucket).await?;
        self.write_bucket_load(bucket, load.saturating_sub(1)).await
    }

    pub(super) fn operator_from_location(
        &self,
        location: &BackendLocation,
    ) -> Result<Operator, BlobError> {
        let mut config = self.backend_config.service_config.clone();
        config.insert("root".to_string(), location.root.clone());
        if Backend::S3 == self.backend_config.backend_type {
            config.insert("bucket".to_string(), location.storage_bucket.clone());
        }

        init_operator(self.backend_config.backend_type.clone(), config)
    }
}

pub(super) fn generate_bucket_name(prefix: Option<&str>) -> String {
    let prefix = prefix.unwrap_or("aruna-");
    format!("{}{}", prefix, Ulid::new().to_string().to_lowercase())
}

pub(super) fn build_backend_path(
    bucket: &str,
    key: &str,
    ulid: Ulid,
) -> Result<String, ConversionError> {
    let path = PathBuf::from(bucket).join(format!("{}_{}", key, ulid));
    ensure_confined_relative_path(&path)?;
    path.into_os_string()
        .into_string()
        .map_err(|_| ConversionError::OsStringError)
}

pub(super) fn build_multipart_part_path(upload_id: Ulid, part_number: u16, ulid: Ulid) -> String {
    PathBuf::from(upload_id.to_string())
        .join(format!("{:05}_{}", part_number, ulid))
        .into_os_string()
        .into_string()
        .expect("multipart part path must be valid utf-8")
}

pub(super) fn rebuild_backend_path(
    original_path: &str,
    ulid: Ulid,
) -> Result<String, ConversionError> {
    let original = PathBuf::from(original_path);
    let parent = original.parent().map(PathBuf::from).unwrap_or_default();
    let file_name = original
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(ConversionError::OsStringError)?;
    let base_name = file_name
        .rsplit_once('_')
        .map_or(file_name, |(base, _)| base);

    let path = parent.join(format!("{}_{}", base_name, ulid));
    ensure_confined_relative_path(&path)?;
    path.into_os_string()
        .into_string()
        .map_err(|_| ConversionError::OsStringError)
}
