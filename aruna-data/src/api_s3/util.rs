/*
use crate::io::io_handler::ObjectInfo2;
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use futures_util::TryStreamExt;
use http_body_util::StreamBody;
use hyper::body::Frame;
use s3s::dto::{CreateBucketInput, CreateMultipartUploadInput, UploadPartInput};
*/
use aruna_permission::manager::Action;
use aruna_storage::storage::lmdb::LmdbStore;
use opendal::{FuturesBytesStream, Operator};
use s3s::{S3Error, s3_error};

use crate::IOHandler;

pub fn get_s3_operation_permission(operation_name: &str) -> Option<Action> {
    match operation_name {
        // Write operations (operations that modify state/data)
        "AbortMultipartUpload" => Some(Action::Write),
        "CompleteMultipartUpload" => Some(Action::Write),
        "CopyObject" => Some(Action::Write),
        "CreateBucket" => Some(Action::Write),
        "CreateBucketMetadataTableConfiguration" => Some(Action::Write),
        "CreateMultipartUpload" => Some(Action::Write),
        "DeleteBucket" => Some(Action::Write),
        "DeleteBucketAnalyticsConfiguration" => Some(Action::Write),
        "DeleteBucketCors" => Some(Action::Write),
        "DeleteBucketEncryption" => Some(Action::Write),
        "DeleteBucketIntelligentTieringConfiguration" => Some(Action::Write),
        "DeleteBucketInventoryConfiguration" => Some(Action::Write),
        "DeleteBucketLifecycle" => Some(Action::Write),
        "DeleteBucketMetadataTableConfiguration" => Some(Action::Write),
        "DeleteBucketMetricsConfiguration" => Some(Action::Write),
        "DeleteBucketOwnershipControls" => Some(Action::Write),
        "DeleteBucketPolicy" => Some(Action::Write),
        "DeleteBucketReplication" => Some(Action::Write),
        "DeleteBucketTagging" => Some(Action::Write),
        "DeleteBucketWebsite" => Some(Action::Write),
        "DeleteObject" => Some(Action::Write),
        "DeleteObjectTagging" => Some(Action::Write),
        "DeleteObjects" => Some(Action::Write),
        "DeletePublicAccessBlock" => Some(Action::Write),
        "PutBucketAccelerateConfiguration" => Some(Action::Write),
        "PutBucketAcl" => Some(Action::Write),
        "PutBucketAnalyticsConfiguration" => Some(Action::Write),
        "PutBucketCors" => Some(Action::Write),
        "PutBucketEncryption" => Some(Action::Write),
        "PutBucketIntelligentTieringConfiguration" => Some(Action::Write),
        "PutBucketInventoryConfiguration" => Some(Action::Write),
        "PutBucketLifecycleConfiguration" => Some(Action::Write),
        "PutBucketLogging" => Some(Action::Write),
        "PutBucketMetricsConfiguration" => Some(Action::Write),
        "PutBucketNotificationConfiguration" => Some(Action::Write),
        "PutBucketOwnershipControls" => Some(Action::Write),
        "PutBucketPolicy" => Some(Action::Write),
        "PutBucketReplication" => Some(Action::Write),
        "PutBucketRequestPayment" => Some(Action::Write),
        "PutBucketTagging" => Some(Action::Write),
        "PutBucketVersioning" => Some(Action::Write),
        "PutBucketWebsite" => Some(Action::Write),
        "PutObject" => Some(Action::Write),
        "PutObjectAcl" => Some(Action::Write),
        "PutObjectLegalHold" => Some(Action::Write),
        "PutObjectLockConfiguration" => Some(Action::Write),
        "PutObjectRetention" => Some(Action::Write),
        "PutObjectTagging" => Some(Action::Write),
        "PutPublicAccessBlock" => Some(Action::Write),
        "RestoreObject" => Some(Action::Write),
        "UploadPart" => Some(Action::Write),
        "UploadPartCopy" => Some(Action::Write),
        "WriteGetObjectResponse" => Some(Action::Write),

        // Read operations (operations that only retrieve/read data)
        "GetBucketAccelerateConfiguration" => Some(Action::Read),
        "GetBucketAcl" => Some(Action::Read),
        "GetBucketAnalyticsConfiguration" => Some(Action::Read),
        "GetBucketCors" => Some(Action::Read),
        "GetBucketEncryption" => Some(Action::Read),
        "GetBucketIntelligentTieringConfiguration" => Some(Action::Read),
        "GetBucketInventoryConfiguration" => Some(Action::Read),
        "GetBucketLifecycleConfiguration" => Some(Action::Read),
        "GetBucketLocation" => Some(Action::Read),
        "GetBucketLogging" => Some(Action::Read),
        "GetBucketMetadataTableConfiguration" => Some(Action::Read),
        "GetBucketMetricsConfiguration" => Some(Action::Read),
        "GetBucketNotificationConfiguration" => Some(Action::Read),
        "GetBucketOwnershipControls" => Some(Action::Read),
        "GetBucketPolicy" => Some(Action::Read),
        "GetBucketPolicyStatus" => Some(Action::Read),
        "GetBucketReplication" => Some(Action::Read),
        "GetBucketRequestPayment" => Some(Action::Read),
        "GetBucketTagging" => Some(Action::Read),
        "GetBucketVersioning" => Some(Action::Read),
        "GetBucketWebsite" => Some(Action::Read),
        "GetObject" => Some(Action::Read),
        "GetObjectAcl" => Some(Action::Read),
        "GetObjectAttributes" => Some(Action::Read),
        "GetObjectLegalHold" => Some(Action::Read),
        "GetObjectLockConfiguration" => Some(Action::Read),
        "GetObjectRetention" => Some(Action::Read),
        "GetObjectTagging" => Some(Action::Read),
        "GetObjectTorrent" => Some(Action::Read),
        "GetPublicAccessBlock" => Some(Action::Read),
        "HeadBucket" => Some(Action::Read),
        "HeadObject" => Some(Action::Read),
        "ListBucketAnalyticsConfigurations" => Some(Action::Read),
        "ListBucketIntelligentTieringConfigurations" => Some(Action::Read),
        "ListBucketInventoryConfigurations" => Some(Action::Read),
        "ListBucketMetricsConfigurations" => Some(Action::Read),
        "ListBuckets" => Some(Action::Read),
        "ListMultipartUploads" => Some(Action::Read),
        "ListObjectVersions" => Some(Action::Read),
        "ListObjects" => Some(Action::Read),
        "ListObjectsV2" => Some(Action::Read),
        "ListParts" => Some(Action::Read),
        "SelectObjectContent" => Some(Action::Read),

        // Unknown operation
        _ => None,
    }
}

pub async fn evaluate_s3_range(
    input_range: Option<s3s::dto::Range>,
    file_size: u64,
    storage_path: &str,
    operator: &Operator,
) -> Result<(FuturesBytesStream, Option<String>, Option<String>, i64), S3Error> {
    Ok(if let Some(range) = input_range {
        let (range, bytes_range, content_range) = match range {
            s3s::dto::Range::Int { first, last } => match last {
                Some(end) => {
                    // File size is maximum
                    let end = if end >= file_size { file_size - 1 } else { end };

                    (
                        std::ops::Range {
                            start: first,
                            end: end + 1,
                        },
                        format!("bytes {}-{}/{}", first, end, file_size),
                        (end + 1 - first) as i64,
                    )
                }
                None => (
                    std::ops::Range {
                        start: first,
                        end: file_size,
                    },
                    format!("bytes {}-{}/{}", first, file_size - 1, file_size),
                    (file_size - first) as i64,
                ),
            },
            s3s::dto::Range::Suffix { length } => {
                // File size is maximum
                let length = if length > file_size {
                    file_size - 1
                } else {
                    length
                };

                (
                    std::ops::Range {
                        start: 0,
                        end: length + 1,
                    },
                    format!("bytes 0-{}/{}", length, file_size),
                    (length + 1) as i64,
                )
            }
        };

        let stream = IOHandler::<LmdbStore>::read_data_range(&operator, storage_path, range)
            .await
            .map_err(|e| s3_error!(InternalError, "{}", e))?;

        (
            stream,
            Some("bytes".to_string()),
            Some(bytes_range),
            content_range,
        )
    } else {
        let stream = IOHandler::<LmdbStore>::read_data(&operator, storage_path)
            .await
            .map_err(|e| s3_error!(InternalError, "{}", e))?;

        (stream, None, None, file_size as i64)
    })
}

/*
pub fn create_bucket(req: CreateBucketInput) -> Result<(), S3Error> {
    // Directly create bucket in backend storage

    Ok(())
}

pub fn create_multipart_upload(req: CreateMultipartUploadInput) -> Result<(), S3Error> {
    Ok(())
}

pub async fn upload_part(
    req: UploadPartInput,
    client: aws_sdk_s3::client::Client,
    info: ObjectInfo2,
) -> Result<UploadPartOutput, S3Error> {
    // Create stream from body?
    let hyper_body = StreamBody::new(req.body.unwrap().map_ok(Frame::data));
    let bytestream = ByteStream::from(SdkBody::from_body_1_x(hyper_body));

    client
        .upload_part()
        .set_bucket(None)
        .set_key(None)
        .set_content_length(None)
        .body(bytestream)
        .send()
        .await
        .map_err(|e| s3_error!(InternalError, "{}", e))
}

fn parse_s3_path(path: String) -> anyhow::Result<(String, String)> {
    //

    Ok(("".to_string(), "".to_string()))
}
*/
