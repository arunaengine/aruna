use anyhow::anyhow;
use anyhow::Result;
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use bytes::BytesMut;
use diesel_ulid::DieselUlid;
use digest::Digest;
use futures_util::StreamExt;
use md5::Md5;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::pin;

use crate::helpers::random_string;
use crate::structs::FileFormat;
use crate::{
    config::Backend,
    structs::{Object, ObjectLocation, PartETag},
    CONFIG,
};

use super::{location_handler::CompiledVariant, storage_backend::StorageBackend};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct FSBackend {
    _endpoint_id: String,
    pub base_path: String,
    temp: String,
    schema: CompiledVariant,
    use_pithos: bool,
    encryption: bool,
    compression: bool,
    dropbox: Option<String>,
}

impl FSBackend {
    #[tracing::instrument(level = "debug")]
    #[allow(dead_code)]
    pub async fn new(_endpoint_id: String) -> Result<Self> {
        let Backend::FileSystem {
            root_path,
            encryption,
            compression,
            dropbox_folder,
            backend_scheme,
            tmp,
        } = &CONFIG.backend
        else {
            return Err(anyhow!("Invalid backend"));
        };

        let temp = tmp
            .clone()
            .unwrap_or_else(|| "/tmp".to_string().to_ascii_lowercase());

        let compiled_schema = CompiledVariant::new(backend_scheme.as_str())?;

        let handler = FSBackend {
            _endpoint_id,
            temp,
            base_path: root_path.clone(),
            schema: compiled_schema,
            use_pithos: *encryption || *compression,
            encryption: *encryption,
            compression: *compression,
            dropbox: dropbox_folder.clone(),
        };
        Ok(handler)
    }

    #[tracing::instrument(level = "trace", skip(self, bucket))]
    pub async fn check_and_create_bucket(&self, bucket: String) -> Result<()> {
        let path = Path::new(&self.base_path).join(bucket);
        if !path.exists() {
            std::fs::create_dir_all(path).map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
        }
        Ok(())
    }
}

// Data backend for an FS based storage.
#[async_trait]
impl StorageBackend for FSBackend {
    // Uploads a single object in chunks
    // Objects are uploaded in chunks that come from a channel to allow modification in the data middleware
    #[tracing::instrument(level = "trace", skip(self, recv, location, _content_len))]
    async fn put_object(
        &self,
        recv: Receiver<Result<bytes::Bytes>>,
        location: ObjectLocation,
        _content_len: i64,
    ) -> Result<()> {
        self.check_and_create_bucket(location.bucket.to_string())
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;

        let mut file = tokio::fs::File::create(
            Path::new(&self.base_path)
                .join(&location.bucket)
                .join(&location.key),
        )
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;

        pin!(recv);
        while let Some(data) = recv.next().await {
            let data = data.map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
            file.write_all(&data).await.map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
        }
        Ok(())
    }

    // Downloads the given object from the s3 storage
    // The body is wrapped into an async reader and reads the data in chunks.
    // The chunks are then transferred into the sender.
    #[tracing::instrument(level = "trace", skip(self, location, _range, sender))]
    async fn get_object(
        &self,
        location: ObjectLocation,
        _range: Option<String>,
        sender: Sender<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>>,
    ) -> Result<()> {
        let file = tokio::fs::File::open(
            Path::new(&self.base_path)
                .join(&location.bucket)
                .join(&location.key),
        )
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;

        let mut reader = tokio::io::BufReader::new(file);
        let mut buf = BytesMut::with_capacity(1024 * 16);

        while reader.read_buf(&mut buf).await.is_ok() {
            sender.send(Ok(buf.split().freeze())).await.map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, location))]
    async fn head_object(&self, location: ObjectLocation) -> Result<i64> {
        let len = tokio::fs::File::open(
            Path::new(&self.base_path)
                .join(&location.bucket)
                .join(&location.key),
        )
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?
        .metadata()
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?
        .len() as i64;
        Ok(len)
    }

    // Initiates a multipart upload in s3 and returns the associated upload id.
    #[tracing::instrument(level = "trace", skip(self, location))]
    async fn init_multipart_upload(&self, location: ObjectLocation) -> Result<String> {
        self.check_and_create_bucket(location.bucket.clone())
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;

        let up_id: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(15)
            .map(char::from)
            .collect();

        let path = Path::new(&self.base_path).join(&up_id);
        std::fs::create_dir_all(path).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;

        return Ok(up_id);
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, recv, _location, upload_id, _content_len, part_number)
    )]
    async fn upload_multi_object(
        &self,
        recv: Receiver<Result<bytes::Bytes>>,
        _location: ObjectLocation,
        upload_id: String,
        _content_len: i64,
        part_number: i32,
    ) -> Result<PartETag> {
        let mut file = tokio::fs::File::create(
            Path::new(&self.base_path)
                .join(&upload_id)
                .join(format!(".{part_number}.part")),
        )
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;
        let mut md5 = Md5::new();

        pin!(recv);
        while let Some(data) = recv.next().await {
            let data = data.map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
            md5.update(&data);
            file.write_all(&data).await.map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
        }
        return Ok(PartETag {
            part_number,
            etag: format!("{:x}", md5.finalize()),
        });
    }

    #[tracing::instrument(level = "trace", skip(self, location, parts, upload_id))]
    async fn finish_multipart_upload(
        &self,
        location: ObjectLocation,
        parts: Vec<PartETag>,
        upload_id: String,
    ) -> Result<()> {
        self.check_and_create_bucket(location.bucket.to_string())
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;

        let mut final_file = tokio::fs::File::create(
            Path::new(&self.base_path)
                .join(&location.bucket)
                .join(&location.key),
        )
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;

        for part in parts {
            let mut file = tokio::fs::File::open(
                Path::new(&self.base_path)
                    .join(&upload_id)
                    .join(format!(".{}.part", part.part_number)),
            )
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
            tokio::io::copy(&mut file, &mut final_file)
                .await
                .map_err(|e| {
                    tracing::error!(error = ?e, msg = e.to_string());
                    e
                })?;
        }

        // Remove the temp dir
        tokio::fs::remove_dir_all(Path::new(&self.base_path).join(&upload_id))
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, _location))]
    async fn abort_multipart_upload(
        &self,
        _location: ObjectLocation,
        upload_id: String,
    ) -> Result<()> {
        let mut dir = tokio::fs::read_dir(Path::new(&self.base_path).join(&upload_id)).await?;
        while let Some(entry) = dir.next_entry().await? {
            tokio::fs::remove_file(entry.path()).await.map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, bucket))]
    async fn create_bucket(&self, bucket: String) -> Result<()> {
        self.check_and_create_bucket(bucket).await
    }

    #[tracing::instrument(level = "trace", skip(self, location))]
    /// Delete a object from the storage system
    /// # Arguments
    /// * `location` - The location of the object
    async fn delete_object(&self, location: ObjectLocation) -> Result<()> {
        tokio::fs::remove_file(
            Path::new(&self.base_path)
                .join(&location.bucket)
                .join(&location.key),
        )
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;
        Ok(())
    }

    async fn initialize_location(
        &self,
        obj: &Object,
        expected_size: Option<i64>,
        names: [Option<(DieselUlid, String)>; 4],
        temp: bool,
    ) -> Result<ObjectLocation> {
        if temp {
            // No pithos for temp
            let file_format = FileFormat::from_bools(false, self.encryption, false)?;
            return Ok(ObjectLocation {
                id: DieselUlid::generate(),
                bucket: self.temp.clone(),
                key: format!(
                    "{}{}",
                    obj.id.to_string().to_ascii_lowercase(),
                    random_string(3)
                ),
                file_format,
                raw_content_len: expected_size.unwrap_or_default(),
                is_temporary: true,
                ..Default::default()
            });
        }

        let (bucket, key) = self.schema.to_names(names);

        let file_format =
            FileFormat::from_bools(self.use_pithos, self.encryption, self.compression)?;

        Ok(ObjectLocation {
            id: DieselUlid::generate(),
            bucket,
            key,
            file_format,
            raw_content_len: expected_size.unwrap_or_default(),
            ..Default::default()
        })
    }
}
