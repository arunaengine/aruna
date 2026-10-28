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
use aruna_core::structs::storage::blob::UserAccess;
use aruna_operations::s3::listing::common_prefix_of;
use aruna_operations::s3::multipart::parts::ListPartsResult;
use aruna_operations::s3::multipart::uploads::ListUploadsResult;
use aruna_operations::s3::object::list::ListedObject;
use aruna_operations::s3::object::list::{ListBucketInput as LOV2I, ListContinuationToken};
use aruna_operations::s3::object::versions::{ListVersionsItem, ListVersionsResult};
use aruna_operations::s3::object::visible::{HiddenCursor, NextPage, VisibleError, visible_page};
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
    /// A sealed cursor in place of the token, when the page stopped among hidden entries.
    pub(super) sealed: Option<String>,
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

    /// Runs one listing page shared by ListObjects and ListObjectsV2. Only entries the reader
    /// may see show, a common prefix once one below it does; a page that stopped among hidden
    /// entries continues from a sealed cursor.
    pub(super) async fn run_object_listing(
        &self,
        input: LOV2I,
        (owner, reader): (Option<Owner>, Option<&UserAccess>),
        url_encoded: bool,
        scope: Option<&SubpathScope>,
        (bounds, hidden): (Option<(String, Option<String>)>, Option<HiddenCursor>),
    ) -> S3Result<ObjectListingPage> {
        let bucket = input.bucket.clone();
        let groups = |prefix: &str| scope.is_none_or(|scope| scope.allows_prefix(prefix));
        let listed = visible_page(
            self.state.as_ref(),
            input,
            (hidden, bounds),
            |object: &ListedObject| {
                let object = object.clone();
                let bucket = &bucket;
                Box::pin(async move {
                    if !scope.is_none_or(|scope| scope.allows_key(&object.head.key)) {
                        return Ok(false);
                    }
                    self.link_listable(reader, bucket, &object).await
                })
            },
            groups,
        );
        let page = listed.await.map_err(|error| match error {
            VisibleError::List(error) => error.into_s3_error(),
            VisibleError::Shows(error) => error,
            VisibleError::Cursor => s3_error!(InternalError, "The listing cursor was not sealed"),
        })?;
        let (continuation_token, sealed) = match page.next {
            Some(NextPage::After(token)) => (Some(token), None),
            Some(NextPage::Sealed(sealed)) => (None, Some(sealed)),
            None => (None, None),
        };
        let result = (page.objects, page.prefixes);

        let encode_field = |value: String| -> String {
            if url_encoded {
                utf8_percent_encode(&value, URL_ENCODE_SET).to_string()
            } else {
                value
            }
        };

        let contents: Vec<Object> = (result.0.into_iter())
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
        let common_prefixes: Vec<CommonPrefix> = (result.1.into_iter())
            .map(|prefix| CommonPrefix {
                prefix: Some(encode_field(prefix)),
            })
            .collect();

        Ok(ObjectListingPage {
            contents,
            common_prefixes,
            continuation_token,
            sealed,
        })
    }
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
                    ..
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
    use super::scoped_marker;
    use aruna_core::structs::storage::blob::BlobHeadKey;

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
