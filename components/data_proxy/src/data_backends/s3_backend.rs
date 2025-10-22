use super::location_handler::CompiledVariant;
use super::storage_backend::StorageBackend;
use crate::config::Backend;
use crate::helpers::random_string;
use crate::structs::FileFormat;
use crate::structs::Object;
use crate::structs::ObjectLocation;
use crate::structs::PartETag;
use crate::CONFIG;
use anyhow::anyhow;
use anyhow::Result;
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use aws_sdk_s3::config::RequestChecksumCalculation;
use aws_sdk_s3::primitives::SdkBody;
use aws_sdk_s3::{
    config::Region,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};
use diesel_ulid::DieselUlid;
use futures::TryStreamExt;
use http_body_util::StreamBody;
use hyper::body::Frame;
use rand::random;
use tracing::error;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct S3Backend {
    pub s3_client: Client,
    endpoint_id: String,
    temp: String,
    schema: CompiledVariant,
    use_pithos: bool,
    encryption: bool,
    compression: bool,
    dropbox: Option<String>,
}

impl S3Backend {
    #[tracing::instrument]
    pub async fn new(endpoint_id: String) -> Result<Self> {
        let Backend::S3 {
            tmp,
            backend_scheme,
            host,
            encryption,
            compression,
            dropbox_bucket,
            force_path_style,
            ..
        } = &CONFIG.backend
        else {
            return Err(anyhow!("Invalid backend"));
        };

        let temp = tmp
            .clone()
            .unwrap_or_else(|| format!("temp-{endpoint_id}").to_ascii_lowercase());

        let compiled_schema = CompiledVariant::new(backend_scheme.as_str())?;

        let s3_endpoint = host.clone().ok_or_else(|| anyhow!("Missing s3 host"))?;
        tracing::debug!("S3 Endpoint: {}", s3_endpoint);

        #[allow(deprecated)]
        let config = aws_config::load_from_env().await;
        let s3_config = match force_path_style {
            Some(force_path_style) => aws_sdk_s3::config::Builder::from(&config)
                .region(Region::new("RegionOne"))
                .force_path_style(*force_path_style)
                .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
                .response_checksum_validation(
                    aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired,
                )
                .clone()
                .endpoint_url(&s3_endpoint)
                .build(),
            _ => aws_sdk_s3::config::Builder::from(&config)
                .region(Region::new("RegionOne"))
                .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
                .response_checksum_validation(
                    aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired,
                )
                .endpoint_url(&s3_endpoint)
                .build(),
        };

        let s3_client = Client::from_conf(s3_config);

        let handler = S3Backend {
            s3_client,
            endpoint_id,
            temp,
            schema: compiled_schema,
            use_pithos: *encryption || *compression,
            encryption: *encryption,
            compression: *compression,
            dropbox: dropbox_bucket.clone(),
        };
        Ok(handler)
    }
}

// Data backend for an S3 based storage.
#[async_trait]
impl StorageBackend for S3Backend {
    // Uploads a single object in chunks
    // Objects are uploaded in chunks that come from a channel to allow modification in the data middleware
    // The receiver can directly will be wrapped and will then be directly passed into the s3 client
    #[tracing::instrument(level = "trace", skip(self, recv, location, content_len))]
    async fn put_object(
        &self,
        recv: Receiver<Result<bytes::Bytes>>,
        location: ObjectLocation,
        content_len: i64,
    ) -> Result<()> {
        self.check_and_create_bucket(location.bucket.clone())
            .await?;

        let mapped = recv.map_ok(Frame::data);

        let hyper_body = StreamBody::new(mapped);
        let bytestream = ByteStream::from(SdkBody::from_body_1_x(hyper_body));

        match self
            .s3_client
            .put_object()
            .set_bucket(Some(location.bucket))
            .set_key(Some(location.key))
            .set_content_length(Some(content_len))
            .body(bytestream)
            .send()
            .await
        {
            Ok(_) => {}
            Err(err) => {
                error!(error = ?err, "Error putting object");
                return Err(err.into());
            }
        }

        Ok(())
    }

    // Downloads the given object from the s3 storage
    // The body is wrapped into an async reader and reads the data in chunks.
    // The chunks are then transferred into the sender.
    #[tracing::instrument(level = "trace", skip(self, location, range, sender))]
    async fn get_object(
        &self,
        location: ObjectLocation,
        range: Option<String>,
        sender: Sender<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>>,
    ) -> Result<()> {
        let object = self
            .s3_client
            .get_object()
            .set_bucket(Some(location.bucket))
            .set_key(Some(location.key))
            .set_range(range);

        let mut object_request = match object.send().await {
            Ok(value) => value,
            Err(err) => {
                error!(error = ?err, "Error getting object");
                return Err(err.into());
            }
        };

        //let id = location.id.to_string();
        while let Some(bytes) = object_request.body.next().await {
            //trace!(len = ?bytes.as_ref().map(|e| e.len()), ?id, "Sending bytes");
            sender
                .send(Ok(bytes.map_err(|e| {
                    error!(error = ?e, msg = e.to_string());
                    e
                })?))
                .await
                .map_err(|e| {
                    error!(error = ?e, msg = e.to_string());
                    e
                })?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, location))]
    async fn head_object(&self, location: ObjectLocation) -> Result<i64> {
        let object = self
            .s3_client
            .head_object()
            .set_bucket(Some(location.bucket))
            .set_key(Some(location.key))
            .send()
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;
        Ok(object.content_length().unwrap_or_default())
    }

    // Initiates a multipart upload in s3 and returns the associated upload id.
    #[tracing::instrument(level = "trace", skip(self, location))]
    async fn init_multipart_upload(&self, location: ObjectLocation) -> Result<String> {
        self.check_and_create_bucket(location.bucket.clone())
            .await?;

        let multipart = self
            .s3_client
            .create_multipart_upload()
            .set_bucket(Some(location.bucket))
            .set_key(Some(location.key))
            .send()
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;

        return Ok(multipart.upload_id().unwrap().to_string());
    }

    #[tracing::instrument(level = "trace", skip(self, recv, location, content_len))]
    async fn upload_multi_object(
        &self,
        recv: Receiver<Result<bytes::Bytes>>,
        location: ObjectLocation,
        upload_id: String,
        content_len: i64,
        part_number: i32,
    ) -> Result<PartETag> {
        let mapped = recv.map_ok(Frame::data);
        let hyper_body = StreamBody::new(mapped);
        let bytestream = ByteStream::from(SdkBody::from_body_1_x(hyper_body));

        let upload = self
            .s3_client
            .upload_part()
            .set_bucket(Some(location.bucket))
            .set_key(Some(location.key))
            .set_part_number(Some(part_number))
            .set_content_length(Some(content_len))
            .set_upload_id(Some(upload_id))
            .body(bytestream)
            .send()
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;

        return Ok(PartETag {
            part_number,
            etag: upload.e_tag.ok_or_else(|| {
                error!(error = "Missing etag");
                anyhow!("Missing etag")
            })?,
        });
    }

    #[tracing::instrument(level = "trace", skip(self, location, parts))]
    async fn finish_multipart_upload(
        &self,
        location: ObjectLocation,
        parts: Vec<PartETag>,
        upload_id: String,
    ) -> Result<()> {
        let mut completed_parts = Vec::new();
        for etag in parts {
            let part_number = etag.part_number;

            let completed_part = CompletedPart::builder()
                .e_tag(
                    etag.etag
                        .replace('-', "")
                        .replace("\"", "")
                        .replace("\\", ""),
                )
                .part_number(part_number)
                .build();

            completed_parts.push(completed_part);
        }

        match self
            .s3_client
            .complete_multipart_upload()
            .bucket(location.bucket)
            .key(location.key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            )
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                error!(error = ?e, "Error completing multipart upload");
                Err(e.into())
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self, location))]
    async fn abort_multipart_upload(
        &self,
        location: ObjectLocation,
        upload_id: String,
    ) -> Result<()> {
        let _ = self
            .s3_client
            .abort_multipart_upload()
            .bucket(location.bucket)
            .key(location.key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|e| {
                error!(error = ?e, "Error aborting multipart upload");
                e
            })?;
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
        self.s3_client
            .delete_object()
            .bucket(location.bucket)
            .key(location.key)
            .send()
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, obj, expected_size, names, temp))]
    /// Initialize a new location for a specific object
    /// This takes the object_info into account and creates a new location for the object
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

    #[tracing::instrument(level = "trace", skip(self, source, target))]
    /// Initialize a new location for a specific object
    /// This takes the object_info into account and creates a new location for the object
    async fn copy_data(&self, source: ObjectLocation, target: ObjectLocation) -> Result<()> {
        self.check_and_create_bucket(target.bucket.clone()).await?;
        self.s3_client
            .copy_object()
            .copy_source([source.bucket, source.key].join("/"))
            .bucket(target.bucket)
            .key(target.key)
            .send()
            .await?;
        Ok(())
    }
}

impl S3Backend {
    #[tracing::instrument(level = "trace", skip(self, bucket))]
    pub async fn check_and_create_bucket(&self, bucket: String) -> Result<()> {
        match self
            .s3_client
            .get_bucket_location()
            .bucket(bucket.clone())
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e1) => match self.s3_client.create_bucket().bucket(bucket).send().await {
                Ok(_) => Ok(()),
                Err(err) => {
                    error!(?e1, ?err, "Error creating bucket");
                    Err(err.into())
                }
            },
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_random_bucket(&self) -> String {
        format!("{}-{:x}", self.endpoint_id, random::<u8>()).to_ascii_lowercase()
    }
}
