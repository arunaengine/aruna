use crate::s3::auth::Action;
use aruna_core::stream::BackendStream;
use aruna_operations::s3::put_object::PutObjectInput as BlobPutObjectInput;
use base64::prelude::*;
use s3s::dto::PutObjectInput;
use s3s::{s3_error, S3Error, S3Result};

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

pub(crate) fn convert_input(mut input: PutObjectInput) -> S3Result<BlobPutObjectInput, S3Error> {
    match input.body.take() {
        None => Err(s3_error!(InvalidRequest, "Missing body")),
        Some(stream) => Ok(BlobPutObjectInput {
            bucket: input.bucket,
            key: input.key,
            content_length: input.content_length.map(|l| l as u64),
            body: Some(BackendStream::new(stream)),
        }),
    }
}

pub fn to_base64<T: AsRef<[u8]>>(input: T) -> String {
    BASE64_STANDARD.encode(input)
}

/*
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
*/
