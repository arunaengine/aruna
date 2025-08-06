use crate::structs::{Object, ObjectLocation, PartETag};
use anyhow::Result;
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use diesel_ulid::DieselUlid;
use std::fmt::Debug;

/// A generic backend API for storing and retrieving objects
/// Represents a very simple object storage API
/// Data is always read and written in chunks and send via channels following a CSP style pattern
#[async_trait]
pub trait StorageBackend: Debug + Send + Sync {
    /// Uploads the given object from the receiver and stores it in the provided location
    /// # Arguments
    ///
    /// * `recv` - The receiver from which to load the objects data chunks
    /// * `location` - The location of the object which to load
    /// * `content_len` - The size of the uploaded object
    async fn put_object(
        &self,
        recv: Receiver<Result<bytes::Bytes>>,
        location: ObjectLocation,
        content_len: i64,
    ) -> Result<()>;

    /// Downloads the given object from storage and put it into the sender
    /// # Arguments
    ///
    /// * `location` - The location of the object which to load
    /// * `ranges` - Optional: Set of ranges which to load from a larger file; work like HTTP range requests
    /// * `encryption_key` - The encryption key that is stored in backend
    /// * `chunk_size` - Size of the individual chunks which are send
    /// * `sender` - The target for the individual chunks of data
    async fn get_object(
        &self,
        location: ObjectLocation,
        range: Option<String>,
        sender: Sender<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>>,
    ) -> Result<()>;

    /// Gets meta information about a specific object
    async fn head_object(&self, location: ObjectLocation) -> Result<i64>;

    /// Initiates a multipart upload.
    /// Returns the UploadID of the multipart upload
    /// This is modelled after other multipart upload mechanisms like from S3
    /// This should be compatible with FS based multipart uploads as well
    /// # Arguments
    ///
    /// * `location` - The location of the object which to load
    async fn init_multipart_upload(&self, location: ObjectLocation) -> Result<String>;

    /// Uploads one part of an object in a multipart uploads
    /// Returns the ETag of the uploaded object
    /// # Arguments
    ///
    /// * `recv` - The receiver from which to load the objects data chunks; the chunks only represent a single part
    /// * `location` - The location of the object
    /// * `upload_id` - The upload id of the multipart uploads
    /// * `content_len` - The size of the uploaded object
    /// * `part_number` - The number of the uploaded part in the final sequence
    async fn upload_multi_object(
        &self,
        recv: Receiver<Result<bytes::Bytes>>,
        location: ObjectLocation,
        upload_id: String,
        content_len: i64,
        part_number: i32,
    ) -> Result<PartETag>;

    /// Finishes multipart uploads
    /// # Arguments
    ///
    /// * `location` - The location of the object
    /// * `parts` - The sequence of all uploaded parts that contain their part_number and their ETag
    /// * `upload_id` - The upload id of the multipart uploads
    async fn finish_multipart_upload(
        &self,
        location: ObjectLocation,
        parts: Vec<PartETag>,
        upload_id: String,
    ) -> Result<()>;

    /// Aborts multipart uploads
    /// # Arguments
    ///
    /// * `location` - The location of the object
    /// * `upload_id` - The upload id of the multipart uploads
    async fn abort_multipart_upload(
        &self,
        location: ObjectLocation,
        upload_id: String,
    ) -> Result<()>;

    /// Creates a bucket or the storage system equivalent
    /// # Arguments
    ///
    /// * `bucket` - Name of the bucket to create
    #[allow(dead_code)]
    async fn create_bucket(&self, bucket: String) -> Result<()>;

    /// Delete a object from the storage system
    /// # Arguments
    /// * `location` - The location of the object
    async fn delete_object(&self, location: ObjectLocation) -> Result<()>;

    /// Initialize a new location for a specific object
    /// This takes the object_info into account and creates a new location for the object
    async fn initialize_location(
        &self,
        obj: &Object,
        expected_size: Option<i64>,
        names: [Option<(DieselUlid, String)>; 4],
        temp: bool,
    ) -> Result<ObjectLocation>;


    /// Copies data from source to target
    /// # Arguments
    ///
    /// * `source` - The location of the source object
    /// * `target` - The location of the target object
    async fn copy_data(
        &self,
        source: ObjectLocation,
        target: ObjectLocation,
    ) -> Result<()>;
}
