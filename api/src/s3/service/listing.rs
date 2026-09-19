//! Builds the object, version, upload and part listing pages for the S3 adapter.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{ArunaS3Service, URL_ENCODE_SET};
use crate::s3::checksum::{ChecksumSelection, encode_checksums};
use crate::s3::error::IntoS3Error;
use crate::s3::scope::SubpathScope;
use crate::s3::util::{map_checksum_algorithm, map_checksum_type};
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::storage::blob::BlobHeadKey;
use aruna_operations::driver::drive;
use aruna_operations::s3::listing::common_prefix_of;
use aruna_operations::s3::multipart::parts::ListPartsResult;
use aruna_operations::s3::multipart::uploads::ListUploadsResult;
use aruna_operations::s3::object::list::{
    ListBucketInput as LOV2I, ListBucketOperation, ListContinuationToken,
};
use aruna_operations::s3::object::versions::{ListVersionsItem, ListVersionsResult};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use percent_encoding::utf8_percent_encode;
use s3s::dto::{
    ChecksumType, CommonPrefix, DeleteMarkerEntry, ETag, EncodingType, Initiator,
    ListMultipartUploadsInput, ListMultipartUploadsOutput, ListObjectVersionsInput,
    ListObjectVersionsOutput, ListPartsInput, ListPartsOutput,
    MultipartUpload as S3MultipartUpload, Object, ObjectVersion, ObjectVersionStorageClass, Owner,
    Part, StorageClass,
};
use s3s::{S3Response, S3Result, s3_error};
use ulid::Ulid;

/// One page of a shared object listing before protocol-specific mapping.
pub(super) struct ObjectListingPage {
    pub(super) contents: Vec<Object>,
    pub(super) common_prefixes: Vec<CommonPrefix>,
    pub(super) continuation_token: Option<ListContinuationToken>,
}

/// Resumes a delimited ListObjects page past the group the marker collapses
/// into, because such a marker names an already returned common prefix.
pub(super) fn marker_continuation_token(
    bucket: &str,
    marker: Option<&str>,
    prefix: Option<&str>,
    delimiter: Option<&str>,
) -> S3Result<Option<ListContinuationToken>> {
    let Some(marker) = marker.filter(|marker| !marker.is_empty()) else {
        return Ok(None);
    };
    let Some(group) = common_prefix_of(marker, prefix, delimiter) else {
        return Ok(None);
    };
    let last_key = BlobHeadKey::object_prefix(bucket, marker)
        .map_err(|_| s3_error!(InvalidArgument, "Invalid marker"))?;
    Ok(Some(ListContinuationToken {
        last_key,
        last_common_prefix: Some(group),
    }))
}

/// V1 `NextMarker`. A delimited page may end on a common prefix, and a page whose keys
/// were all filtered out has no trailing `<Key>` to resume from, so both dead-end without
/// one. An undelimited page with contents needs none: the client resumes from its last key.
pub(super) fn next_marker_for(
    delimiter: Option<&str>,
    token: Option<&ListContinuationToken>,
    contents_empty: bool,
) -> Option<String> {
    let token = token?;
    if delimiter.is_some() || contents_empty {
        next_marker_of(token)
    } else {
        None
    }
}

/// Names the last entry of a truncated page, preferring the common prefix the
/// page stopped inside.
pub(super) fn next_marker_of(token: &ListContinuationToken) -> Option<String> {
    if let Some(group) = token.last_common_prefix.clone() {
        return Some(group);
    }
    BlobHeadKey::from_bytes(&token.last_key)
        .ok()
        .map(|head| head.key)
}

impl ArunaS3Service {
    pub(super) fn decode_list_token(
        token: Option<&str>,
    ) -> S3Result<Option<ListContinuationToken>> {
        token
            .map(|token| {
                let decoded = STANDARD
                    .decode(token)
                    .map_err(|_| s3_error!(InvalidArgument, "Invalid continuation token"))?;
                ListContinuationToken::from_bytes(&decoded)
                    .map_err(|_| s3_error!(InvalidArgument, "Invalid continuation token"))
            })
            .transpose()
    }

    pub(super) fn encode_list_token(
        token: Option<&ListContinuationToken>,
    ) -> S3Result<Option<String>> {
        token
            .map(|token| {
                token
                    .to_bytes()
                    .map(|bytes| STANDARD.encode(bytes))
                    .map_err(|err| s3_error!(InternalError, "{}", err.to_string()))
            })
            .transpose()
    }

    async fn readable_prefix(
        &self,
        input: &LOV2I,
        prefix: String,
        scope: &SubpathScope,
        remaining_pages: &mut usize,
    ) -> S3Result<bool> {
        let mut input = LOV2I {
            prefix: Some(prefix),
            delimiter: None,
            continuation_token: None,
            start_after: None,
            max_keys: None,
            ..input.clone()
        };
        loop {
            consume_scope_page(remaining_pages)?;
            let result = drive(ListBucketOperation::new(input.clone()), &self.state)
                .await
                .map_err(IntoS3Error::into_s3_error)?;
            if result
                .objects
                .iter()
                .any(|object| scope.allows_key(&object.head.key))
            {
                return Ok(true);
            }
            let Some(token) = result.continuation_token else {
                return Ok(false);
            };
            input.continuation_token = Some(token);
        }
    }

    /// Runs one listing page shared by ListObjects and ListObjectsV2.
    pub(super) async fn run_object_listing(
        &self,
        mut input: LOV2I,
        owner: Option<Owner>,
        url_encoded: bool,
        scope: Option<&SubpathScope>,
    ) -> S3Result<ObjectListingPage> {
        let mut remaining_pages = 100;
        if scope.is_some() {
            input.max_keys = Some(
                input
                    .max_keys
                    .unwrap_or(ListBucketOperation::DEFAULT_MAX_KEYS)
                    .min(remaining_pages - 1),
            );
        }
        let result = loop {
            consume_scope_page(&mut remaining_pages)?;
            let mut result = drive(ListBucketOperation::new(input.clone()), &self.state)
                .await
                .map_err(IntoS3Error::into_s3_error)?;

            if let Some(scope) = scope {
                result
                    .objects
                    .retain(|object| scope.allows_key(&object.head.key));
                let mut prefixes = Vec::new();
                for prefix in result.common_prefixes {
                    if scope.allows_prefix(&prefix)
                        && self
                            .readable_prefix(&input, prefix.clone(), scope, &mut remaining_pages)
                            .await?
                    {
                        prefixes.push(prefix);
                    }
                }
                result.common_prefixes = prefixes;
            }

            if !result.objects.is_empty()
                || !result.common_prefixes.is_empty()
                || result.continuation_token.is_none()
            {
                break result;
            }
            input.continuation_token = result.continuation_token;
            input.start_after = None;
        };

        let encode_field = |value: String| -> String {
            if url_encoded {
                utf8_percent_encode(&value, URL_ENCODE_SET).to_string()
            } else {
                value
            }
        };

        let scoped_token = if scope.is_some() && result.continuation_token.is_some() {
            scoped_marker(
                &input.bucket,
                result.objects.last().map(|object| object.head.key.as_str()),
                result.common_prefixes.last().map(String::as_str),
            )?
        } else {
            None
        };
        let contents: Vec<Object> = result
            .objects
            .into_iter()
            .map(|object| {
                let response_fields = self.build_response_fields(
                    object.location.as_ref(),
                    None,
                    None,
                    object.source_metadata.as_ref(),
                    object.last_refresh,
                    object.version_created_at,
                );
                Object {
                    e_tag: response_fields.e_tag,
                    key: Some(encode_field(object.head.key)),
                    last_modified: response_fields.last_modified,
                    owner: owner.clone(),
                    size: response_fields.content_length,
                    ..Default::default()
                }
            })
            .collect();
        let common_prefixes: Vec<CommonPrefix> = result
            .common_prefixes
            .into_iter()
            .map(|prefix| CommonPrefix {
                prefix: Some(encode_field(prefix)),
            })
            .collect();

        Ok(ObjectListingPage {
            contents,
            common_prefixes,
            continuation_token: scope
                .map(|_| scoped_token)
                .unwrap_or(result.continuation_token),
        })
    }
}

fn consume_scope_page(remaining_pages: &mut usize) -> S3Result<()> {
    *remaining_pages = remaining_pages.checked_sub(1).ok_or_else(|| {
        s3_error!(
            SlowDown,
            "Listing scan limit reached; request a narrower prefix"
        )
    })?;
    Ok(())
}

pub(super) fn scoped_marker(
    bucket: &str,
    last_key: Option<&str>,
    last_prefix: Option<&str>,
) -> S3Result<Option<ListContinuationToken>> {
    let group = last_prefix.filter(|prefix| last_key.is_none_or(|key| *prefix > key));
    let Some(entry) = group.or(last_key) else {
        return Ok(None);
    };
    let last_key = BlobHeadKey::object_prefix(bucket, entry)
        .map_err(|_| s3_error!(InternalError, "Invalid listing marker"))?;
    Ok(Some(ListContinuationToken {
        last_key,
        last_common_prefix: group.map(str::to_string),
    }))
}

pub(super) fn list_parts_output(
    input: ListPartsInput,
    max_parts: usize,
    result: ListPartsResult,
) -> S3Response<ListPartsOutput> {
    let checksum_algorithm = result
        .upload
        .checksum_hint
        .as_ref()
        .and_then(|hint| hint.algorithm)
        .and_then(map_checksum_algorithm);
    let checksum_type = result
        .upload
        .checksum_hint
        .as_ref()
        .map(|hint| map_checksum_type(hint.checksum_type));
    let initiator = Some(Initiator {
        display_name: None,
        id: Some(result.upload.created_by.to_string()),
    });
    let owner = Some(Owner {
        display_name: None,
        id: Some(result.upload.group_id.to_string()),
    });

    let parts = result
        .parts
        .into_iter()
        .map(|part| {
            let checksums = encode_checksums(
                &part.location.hashes,
                ChecksumSelection::AllStored,
                ChecksumType::from_static(ChecksumType::FULL_OBJECT),
                None,
            );
            Part {
                part_number: Some(i32::from(part.part_number)),
                size: Some(part.location.blob_size as i64),
                last_modified: Some(part.created_at.into()),
                e_tag: part
                    .location
                    .hashes
                    .get(HASH_MD5)
                    .map(|value| ETag::Strong(hex::encode(value))),
                checksum_crc32: checksums.checksum_crc32,
                checksum_crc32c: checksums.checksum_crc32c,
                checksum_crc64nvme: checksums.checksum_crc64nvme,
                checksum_md5: None,
                checksum_sha1: checksums.checksum_sha1,
                checksum_sha256: checksums.checksum_sha256,
                checksum_sha512: None,
                checksum_xxhash128: None,
                checksum_xxhash3: None,
                checksum_xxhash64: None,
            }
        })
        .collect();

    S3Response::new(ListPartsOutput {
        bucket: Some(input.bucket),
        key: Some(input.key),
        upload_id: Some(input.upload_id),
        part_number_marker: input.part_number_marker,
        max_parts: Some(i32::try_from(max_parts).unwrap_or(i32::MAX)),
        is_truncated: Some(result.is_truncated),
        next_part_number_marker: result.next_part_marker.map(i32::from),
        parts: Some(parts),
        initiator,
        owner,
        storage_class: Some(StorageClass::from_static(StorageClass::STANDARD)),
        checksum_algorithm,
        checksum_type,
        ..Default::default()
    })
}

pub(super) fn list_uploads_output(
    input: ListMultipartUploadsInput,
    max_uploads: usize,
    result: ListUploadsResult,
) -> S3Response<ListMultipartUploadsOutput> {
    let url_encoded = input
        .encoding_type
        .as_ref()
        .is_some_and(|encoding_type| encoding_type.as_str() == EncodingType::URL);
    let encode_field = |value: String| -> String {
        if url_encoded {
            utf8_percent_encode(&value, URL_ENCODE_SET).to_string()
        } else {
            value
        }
    };

    let uploads: Vec<S3MultipartUpload> = result
        .uploads
        .into_iter()
        .map(|record| S3MultipartUpload {
            key: Some(encode_field(record.key)),
            upload_id: Some(record.upload_id.to_string()),
            initiated: Some(record.created_at.into()),
            initiator: Some(Initiator {
                display_name: None,
                id: Some(record.created_by.to_string()),
            }),
            owner: Some(Owner {
                display_name: None,
                id: Some(record.group_id.to_string()),
            }),
            storage_class: Some(StorageClass::from_static(StorageClass::STANDARD)),
            checksum_algorithm: record
                .checksum_hint
                .as_ref()
                .and_then(|hint| hint.algorithm)
                .and_then(map_checksum_algorithm),
            checksum_type: record
                .checksum_hint
                .as_ref()
                .map(|hint| map_checksum_type(hint.checksum_type)),
        })
        .collect();
    let common_prefixes: Vec<CommonPrefix> = result
        .common_prefixes
        .into_iter()
        .map(|prefix| CommonPrefix {
            prefix: Some(encode_field(prefix)),
        })
        .collect();

    S3Response::new(ListMultipartUploadsOutput {
        bucket: Some(input.bucket),
        prefix: input.prefix.map(&encode_field),
        delimiter: input.delimiter.map(&encode_field),
        key_marker: input.key_marker.map(&encode_field),
        upload_id_marker: input.upload_id_marker,
        max_uploads: Some(i32::try_from(max_uploads).unwrap_or(i32::MAX)),
        is_truncated: Some(result.is_truncated),
        next_key_marker: result.next_key_marker.map(&encode_field),
        next_upload_id_marker: result
            .next_upload_marker
            .map(|upload_id| upload_id.to_string()),
        uploads: Some(uploads),
        common_prefixes: Some(common_prefixes),
        encoding_type: input.encoding_type,
        ..Default::default()
    })
}

impl ArunaS3Service {
    pub(super) fn list_versions_output(
        &self,
        input: ListObjectVersionsInput,
        group_id: Ulid,
        max_keys: usize,
        result: ListVersionsResult,
    ) -> S3Response<ListObjectVersionsOutput> {
        let owner = Some(Owner {
            display_name: None,
            id: Some(group_id.to_string()),
        });
        let url_encoded = input
            .encoding_type
            .as_ref()
            .is_some_and(|encoding_type| encoding_type.as_str() == EncodingType::URL);
        let encode_field = |value: String| -> String {
            if url_encoded {
                utf8_percent_encode(&value, URL_ENCODE_SET).to_string()
            } else {
                value
            }
        };

        let mut versions = Vec::new();
        let mut delete_markers = Vec::new();
        for item in result.items {
            match item {
                ListVersionsItem::Version {
                    key,
                    version_id,
                    is_latest,
                    location,
                    source_metadata,
                    created_at,
                } => {
                    let response_fields = self.build_response_fields(
                        location.as_ref(),
                        None,
                        None,
                        source_metadata.as_ref(),
                        None,
                        Some(created_at),
                    );
                    versions.push(ObjectVersion {
                        key: Some(encode_field(key)),
                        version_id: Some(version_id.to_string()),
                        is_latest: Some(is_latest),
                        last_modified: Some(created_at.into()),
                        e_tag: response_fields.e_tag,
                        size: response_fields.content_length,
                        owner: owner.clone(),
                        storage_class: Some(ObjectVersionStorageClass::from_static(
                            ObjectVersionStorageClass::STANDARD,
                        )),
                        ..Default::default()
                    });
                }
                ListVersionsItem::DeleteMarker {
                    key,
                    version_id,
                    is_latest,
                    created_at,
                } => {
                    delete_markers.push(DeleteMarkerEntry {
                        key: Some(encode_field(key)),
                        version_id: Some(version_id.to_string()),
                        is_latest: Some(is_latest),
                        last_modified: Some(created_at.into()),
                        owner: owner.clone(),
                    });
                }
            }
        }
        let common_prefixes: Vec<CommonPrefix> = result
            .common_prefixes
            .into_iter()
            .map(|prefix| CommonPrefix {
                prefix: Some(encode_field(prefix)),
            })
            .collect();

        S3Response::new(ListObjectVersionsOutput {
            name: Some(input.bucket),
            prefix: input.prefix.map(&encode_field),
            delimiter: input.delimiter.map(&encode_field),
            key_marker: input.key_marker.map(&encode_field),
            version_id_marker: input.version_id_marker,
            max_keys: Some(i32::try_from(max_keys).unwrap_or(i32::MAX)),
            is_truncated: Some(result.is_truncated),
            next_key_marker: result.next_key_marker.map(&encode_field),
            next_version_id_marker: result
                .next_version_marker
                .map(|version_id| version_id.to_string()),
            versions: Some(versions),
            delete_markers: Some(delete_markers),
            common_prefixes: Some(common_prefixes),
            encoding_type: input.encoding_type,
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{consume_scope_page, scoped_marker};
    use aruna_core::structs::storage::blob::BlobHeadKey;

    #[test]
    fn scoped_scan_bounded() {
        let mut remaining = 100;
        for _ in 0..100 {
            consume_scope_page(&mut remaining).unwrap();
        }
        let error = consume_scope_page(&mut remaining).unwrap_err();
        assert_eq!(error.code(), &s3s::S3ErrorCode::SlowDown);
    }

    #[test]
    fn prefix_pages_progress() {
        let mut remaining = 100;
        for expected in (97..100).rev() {
            consume_scope_page(&mut remaining).unwrap();
            assert_eq!(remaining, expected);
        }
    }

    #[test]
    fn prefix_token_precedence() {
        let token = scoped_marker("study", Some("imaging/a"), Some("imaging/b/"))
            .unwrap()
            .unwrap();
        assert_eq!(token.last_common_prefix.as_deref(), Some("imaging/b/"));
        assert_eq!(
            BlobHeadKey::from_bytes(&token.last_key).unwrap().key,
            "imaging/b/"
        );

        let token = scoped_marker("study", Some("imaging/z"), Some("imaging/b/"))
            .unwrap()
            .unwrap();
        assert_eq!(token.last_common_prefix, None);
        assert_eq!(
            BlobHeadKey::from_bytes(&token.last_key).unwrap().key,
            "imaging/z"
        );
    }
}
