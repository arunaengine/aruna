use crate::s3::auth::Action;
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum};
use aruna_core::structs::{
    MultipartChecksumType, MultipartUploadChecksumHint, ensure_confined_relative_path,
};
use aruna_operations::s3::complete_multipart_upload::CompleteMultipartPart;
use aruna_operations::s3::put_object::PutObjectInput as BlobPutObjectInput;
use base64::prelude::*;
use s3s::dto::ChecksumAlgorithm as S3ChecksumAlgorithm;
use s3s::dto::{
    ChecksumType, CompletedPart, CreateMultipartUploadInput, PartNumber, PutObjectInput,
};
use s3s::{S3Error, S3ErrorCode, S3Result, s3_error};
use std::path::Path;
use ulid::Ulid;

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

pub(crate) fn is_anonymous_object_read_operation(operation_name: &str) -> bool {
    matches!(operation_name, "GetObject")
}

pub(crate) fn validate_object_key(key: &str) -> S3Result<()> {
    if key.is_empty() {
        return Err(s3_error!(InvalidArgument, "Object key must not be empty"));
    }

    ensure_confined_relative_path(Path::new(key))
        .map_err(|err| s3_error!(InvalidArgument, "{}", err.to_string()))
}

pub(crate) fn convert_input(mut input: PutObjectInput) -> S3Result<BlobPutObjectInput, S3Error> {
    match input.body.take() {
        None => Err(s3_error!(InvalidRequest, "Missing body")),
        Some(stream) => Ok(BlobPutObjectInput {
            bucket: input.bucket,
            key: input.key,
            content_length: input.content_length.map(|l| l as u64),
            body: Some(BackendStream::new_from_boxed(stream)),
        }),
    }
}

pub(crate) fn parse_multipart_checksum_hint(
    input: &CreateMultipartUploadInput,
) -> S3Result<Option<MultipartUploadChecksumHint>> {
    let algorithm = input
        .checksum_algorithm
        .as_ref()
        .map(checksum_algorithm_from_s3)
        .transpose()?;
    let checksum_type = input
        .checksum_type
        .as_ref()
        .map(multipart_checksum_type_from_s3)
        .unwrap_or(MultipartChecksumType::FullObject);

    Ok(
        (algorithm.is_some() || input.checksum_type.is_some()).then_some(
            MultipartUploadChecksumHint {
                algorithm,
                checksum_type,
            },
        ),
    )
}

pub(crate) fn parse_completed_part(part: &CompletedPart) -> S3Result<CompleteMultipartPart> {
    let Some(part_number) = part.part_number else {
        return Err(s3_error!(InvalidPart, "Missing part number"));
    };
    let part_number = parse_multipart_part_number(part_number, S3ErrorCode::InvalidPart)?;

    let mut expected_checksums = Vec::new();
    for (value, algorithm) in [
        (part.checksum_crc32.as_deref(), ChecksumAlgorithm::Crc32),
        (part.checksum_crc32c.as_deref(), ChecksumAlgorithm::Crc32c),
        (
            part.checksum_crc64nvme.as_deref(),
            ChecksumAlgorithm::Crc64Nvme,
        ),
        (part.checksum_sha1.as_deref(), ChecksumAlgorithm::Sha1),
        (part.checksum_sha256.as_deref(), ChecksumAlgorithm::Sha256),
    ] {
        if let Some(value) = value {
            expected_checksums.push(ExpectedChecksum {
                algorithm,
                digest: decode_checksum_header(algorithm, value)?,
            });
        }
    }

    Ok(CompleteMultipartPart {
        part_number,
        etag: part.e_tag.as_ref().map(|etag| etag.value().to_string()),
        expected_checksums,
    })
}

pub(crate) fn parse_multipart_part_number(
    part_number: PartNumber,
    error_code: S3ErrorCode,
) -> S3Result<u16> {
    if !(1..=10_000).contains(&part_number) {
        return Err(S3Error::with_message(error_code, "Invalid part number"));
    }

    u16::try_from(part_number).map_err(|_| S3Error::with_message(error_code, "Invalid part number"))
}

pub(crate) fn decode_checksum_header(
    algorithm: ChecksumAlgorithm,
    value: &str,
) -> S3Result<Vec<u8>> {
    let digest = base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|_| s3_error!(InvalidDigest, "Invalid checksum encoding"))?;
    if digest.len() != algorithm.digest_len() {
        return Err(s3_error!(InvalidDigest, "Invalid checksum length"));
    }
    Ok(digest)
}

pub(crate) fn parse_upload_id(upload_id: &str) -> S3Result<Ulid> {
    Ulid::from_string(upload_id)
        .map_err(|_| s3_error!(NoSuchUpload, "The specified upload does not exist."))
}

pub(crate) fn parse_version_id(version_id: Option<String>) -> S3Result<Option<Ulid>> {
    version_id
        .map(|version_id| {
            Ulid::from_string(&version_id)
                .map_err(|_| s3_error!(InvalidArgument, "Invalid version id"))
        })
        .transpose()
}

pub(crate) fn multipart_checksum_type_from_s3(
    checksum_type: &ChecksumType,
) -> MultipartChecksumType {
    match checksum_type.as_str() {
        ChecksumType::COMPOSITE => MultipartChecksumType::Composite,
        _ => MultipartChecksumType::FullObject,
    }
}

pub(crate) fn s3_checksum_type_from_multipart(
    checksum_type: MultipartChecksumType,
) -> ChecksumType {
    match checksum_type {
        MultipartChecksumType::FullObject => ChecksumType::from_static(ChecksumType::FULL_OBJECT),
        MultipartChecksumType::Composite => ChecksumType::from_static(ChecksumType::COMPOSITE),
    }
}

pub(crate) fn checksum_algorithm_from_s3(
    algorithm: &S3ChecksumAlgorithm,
) -> S3Result<ChecksumAlgorithm> {
    match algorithm.as_str() {
        S3ChecksumAlgorithm::CRC32 => Ok(ChecksumAlgorithm::Crc32),
        S3ChecksumAlgorithm::CRC32C => Ok(ChecksumAlgorithm::Crc32c),
        S3ChecksumAlgorithm::CRC64NVME => Ok(ChecksumAlgorithm::Crc64Nvme),
        S3ChecksumAlgorithm::SHA1 => Ok(ChecksumAlgorithm::Sha1),
        S3ChecksumAlgorithm::SHA256 => Ok(ChecksumAlgorithm::Sha256),
        _ => Err(s3_error!(InvalidRequest, "Unsupported checksum algorithm")),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        get_s3_operation_permission, is_anonymous_object_read_operation,
        parse_multipart_part_number, validate_object_key,
    };
    use crate::s3::auth::Action;
    use s3s::S3ErrorCode;

    #[test]
    fn validate_object_key_accepts_ordinary_keys() {
        for key in ["object.bin", "nested/path/object.bin", "a.b..c/keep..dots"] {
            assert!(
                validate_object_key(key).is_ok(),
                "key {key:?} must be valid"
            );
        }
    }

    #[test]
    fn validate_object_key_rejects_traversal_and_control_keys() {
        let cases = [
            "",
            "/absolute/key",
            "../escape",
            "../../etc/passwd",
            "nested/../../escape",
            "trailing/..",
            "with\u{0000}null",
            "with\u{007f}delete",
            "with\nnewline",
        ];
        for key in cases {
            assert_eq!(
                *validate_object_key(key).unwrap_err().code(),
                S3ErrorCode::InvalidArgument,
                "key {key:?} must be rejected"
            );
        }
    }

    #[test]
    fn anonymous_public_access_only_allows_get_object() {
        assert!(is_anonymous_object_read_operation("GetObject"));

        for operation in [
            "HeadObject",
            "GetObjectAttributes",
            "GetObjectTagging",
            "GetBucketCors",
            "GetBucketVersioning",
            "ListBuckets",
            "ListObjectsV2",
            "ListObjectVersions",
            "ListMultipartUploads",
            "ListParts",
        ] {
            assert!(
                !is_anonymous_object_read_operation(operation),
                "{operation} must not be allowed anonymously"
            );
        }
    }

    #[test]
    fn authenticated_read_classification_still_includes_metadata_operations() {
        assert_eq!(get_s3_operation_permission("GetObject"), Some(Action::Read));
        assert_eq!(
            get_s3_operation_permission("HeadObject"),
            Some(Action::Read)
        );
        assert_eq!(
            get_s3_operation_permission("ListObjectsV2"),
            Some(Action::Read)
        );
    }

    #[test]
    fn rejects_upload_part_number_zero() {
        assert_eq!(
            *parse_multipart_part_number(0, S3ErrorCode::InvalidArgument)
                .unwrap_err()
                .code(),
            S3ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn rejects_upload_part_number_above_limit() {
        assert_eq!(
            *parse_multipart_part_number(10_001, S3ErrorCode::InvalidArgument)
                .unwrap_err()
                .code(),
            S3ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn rejects_negative_completed_part_number() {
        assert_eq!(
            *parse_multipart_part_number(-1, S3ErrorCode::InvalidPart)
                .unwrap_err()
                .code(),
            S3ErrorCode::InvalidPart
        );
    }

    #[test]
    fn accepts_multipart_part_number_bounds() {
        assert_eq!(
            parse_multipart_part_number(1, S3ErrorCode::InvalidArgument).unwrap(),
            1
        );
        assert_eq!(
            parse_multipart_part_number(10_000, S3ErrorCode::InvalidPart).unwrap(),
            10_000
        );
    }
}
