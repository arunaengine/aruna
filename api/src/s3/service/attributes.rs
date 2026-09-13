//! Object-attribute decoding and response shaping for the S3 adapter.
//!
//! `GetObjectAttributes` asks for an explicit subset of attributes and, for
//! multipart objects, a bounded page of parts. Both the request mask and the
//! response shape are assembled here so the trait implementation stays a thin
//! mapping and the missing-version fallback keeps using the read path.

use super::response::ObjectResponseFields;
use crate::s3::checksum::{ChecksumSelection, EncodedChecksums, encode_checksums};
use crate::s3::util::{checksum_response_hashes, map_checksum_type};
use aruna_core::structs::MultipartObjectPart;
use aruna_operations::s3::get_attributes::GetObjectAttributesResult;
use aruna_operations::s3::get_object::ObjectInfo;
use aruna_operations::s3::list_parts::ListPartsOperation;
use s3s::dto::{
    Checksum, ChecksumType, GetObjectAttributesOutput, GetObjectAttributesParts, ObjectAttributes,
    ObjectPart, StorageClass,
};
use s3s::{S3Result, s3_error};

/// The attribute subset one request selected. At least one entry is required.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct RequestedAttributes {
    pub(super) e_tag: bool,
    pub(super) checksum: bool,
    pub(super) object_parts: bool,
    pub(super) object_size: bool,
    pub(super) storage_class: bool,
}

impl RequestedAttributes {
    pub(super) fn from_request(attributes: &[ObjectAttributes]) -> S3Result<Self> {
        let mut requested = Self::default();
        for attribute in attributes {
            match attribute.as_str() {
                ObjectAttributes::ETAG => requested.e_tag = true,
                ObjectAttributes::CHECKSUM => requested.checksum = true,
                ObjectAttributes::OBJECT_PARTS => requested.object_parts = true,
                ObjectAttributes::OBJECT_SIZE => requested.object_size = true,
                ObjectAttributes::STORAGE_CLASS => requested.storage_class = true,
                _ => {}
            }
        }
        if !requested.any() {
            return Err(s3_error!(
                InvalidArgument,
                "At least one object attribute must be specified"
            ));
        }
        Ok(requested)
    }

    fn any(&self) -> bool {
        self.e_tag || self.checksum || self.object_parts || self.object_size || self.storage_class
    }
}

/// The requested part-number marker. A negative marker is rejected and a value
/// beyond the part-number domain saturates, matching the listing convention.
pub(super) fn parse_part_number_marker(marker: Option<i32>) -> S3Result<Option<u16>> {
    match marker {
        None => Ok(None),
        Some(marker) if marker < 0 => Err(s3_error!(InvalidArgument, "Invalid part-number-marker")),
        Some(marker) => Ok(Some(u16::try_from(marker).unwrap_or(u16::MAX))),
    }
}

/// The requested page size, rejected when negative and capped at the multipart
/// listing maximum.
pub(super) fn parse_max_parts(max_parts: Option<i32>) -> S3Result<usize> {
    match max_parts {
        None => Ok(ListPartsOperation::DEFAULT_MAX_PARTS),
        Some(max_parts) => usize::try_from(max_parts)
            .map_err(|_| s3_error!(InvalidArgument, "max-parts must be non-negative"))
            .map(|max_parts| max_parts.min(ListPartsOperation::DEFAULT_MAX_PARTS)),
    }
}

/// The checksum block of the response. The resolved read's own hashes win over
/// the stored location, and only checksum attributes are exposed: the MD5 and
/// extended hashes stay absent, as AWS reports them.
pub(super) fn attributes_checksum(
    remote: Option<&ObjectInfo>,
    result: &GetObjectAttributesResult,
) -> Option<Checksum> {
    let composite_hashes = result
        .summary
        .as_ref()
        .map(|summary| summary.composite_hashes.clone())
        .unwrap_or_default();
    if let Some(info) = remote {
        return Some(storage_checksum(encode_checksums(
            checksum_response_hashes(info.checksum_type, &info.hashes, &info.composite_hashes),
            ChecksumSelection::AllStored,
            map_checksum_type(info.checksum_type),
            info.part_count,
        )));
    }
    result.location.as_ref().map(|location| {
        storage_checksum(encode_checksums(
            checksum_response_hashes(result.checksum_type, &location.hashes, &composite_hashes),
            ChecksumSelection::AllStored,
            map_checksum_type(result.checksum_type),
            result.summary.as_ref().map(|summary| summary.part_count),
        ))
    })
}

fn storage_checksum(encoded: EncodedChecksums) -> Checksum {
    Checksum {
        checksum_crc32: encoded.checksum_crc32,
        checksum_crc32c: encoded.checksum_crc32c,
        checksum_crc64nvme: encoded.checksum_crc64nvme,
        checksum_md5: None,
        checksum_sha1: encoded.checksum_sha1,
        checksum_sha256: encoded.checksum_sha256,
        checksum_sha512: None,
        checksum_type: encoded.checksum_type,
        checksum_xxhash128: None,
        checksum_xxhash3: None,
        checksum_xxhash64: None,
    }
}

/// One page of the multipart part list, marker-filtered and truncated with the
/// same fallback the listing uses when `max_parts` empties the page.
pub(super) fn attributes_parts(
    result: &GetObjectAttributesResult,
    requested_marker: Option<i32>,
    marker: Option<u16>,
    max_parts: usize,
) -> Option<GetObjectAttributesParts> {
    result.summary.as_ref().map(|summary| {
        let mut parts: Vec<&MultipartObjectPart> = result.parts.iter().collect();
        if let Some(marker) = marker {
            parts.retain(|part| part.part_number > marker);
        }
        let is_truncated = parts.len() > max_parts;
        parts.truncate(max_parts);
        // With max_parts=0 the truncation empties `parts`, so fall back to
        // the marker preceding the first unreturned part (request marker/0).
        let next_part_number_marker = is_truncated.then(|| {
            parts
                .last()
                .map(|part| part.part_number)
                .unwrap_or(marker.unwrap_or(0))
        });
        let object_part_list: Vec<ObjectPart> = parts
            .into_iter()
            .map(|part| {
                let checksums = encode_checksums(
                    &part.hashes,
                    ChecksumSelection::AllStored,
                    ChecksumType::from_static(ChecksumType::FULL_OBJECT),
                    None,
                );
                ObjectPart {
                    part_number: Some(i32::from(part.part_number)),
                    size: Some(part.size as i64),
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
        GetObjectAttributesParts {
            total_parts_count: Some(i32::try_from(summary.part_count).unwrap_or(i32::MAX)),
            is_truncated: Some(is_truncated),
            max_parts: Some(i32::try_from(max_parts).unwrap_or(i32::MAX)),
            part_number_marker: requested_marker,
            next_part_number_marker: next_part_number_marker.map(i32::from),
            parts: Some(object_part_list),
        }
    })
}

/// Assembles the response from the selected attributes only. `last_modified`
/// is always reported because it has no attribute flag.
pub(super) fn attributes_output(
    requested: RequestedAttributes,
    response_fields: ObjectResponseFields,
    result: &GetObjectAttributesResult,
    checksum: Option<Checksum>,
    object_parts: Option<GetObjectAttributesParts>,
) -> GetObjectAttributesOutput {
    GetObjectAttributesOutput {
        e_tag: requested
            .e_tag
            .then(|| response_fields.e_tag.clone())
            .flatten(),
        last_modified: response_fields.last_modified,
        object_size: requested
            .object_size
            .then_some(response_fields.content_length)
            .flatten(),
        storage_class: requested
            .storage_class
            .then(|| StorageClass::from_static(StorageClass::STANDARD)),
        version_id: result.version_id.map(|version_id| version_id.to_string()),
        checksum,
        object_parts,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{MultipartChecksumType, MultipartObjectSummary};
    use std::collections::HashMap;

    fn result(part_count: usize, parts: Vec<MultipartObjectPart>) -> GetObjectAttributesResult {
        GetObjectAttributesResult {
            location: None,
            source_metadata: None,
            version_created_at: None,
            version_id: None,
            resolved_version_id: None,
            checksum_type: MultipartChecksumType::FullObject,
            summary: Some(MultipartObjectSummary {
                checksum_type: MultipartChecksumType::FullObject,
                part_count,
                composite_hashes: HashMap::new(),
            }),
            parts,
        }
    }

    fn part(number: u16, size: u64) -> MultipartObjectPart {
        MultipartObjectPart {
            part_number: number,
            size,
            hashes: HashMap::new(),
        }
    }

    #[test]
    fn selects_requested_attributes() {
        let requested = RequestedAttributes::from_request(&[
            ObjectAttributes::from_static(ObjectAttributes::ETAG),
            ObjectAttributes::from_static(ObjectAttributes::OBJECT_SIZE),
        ])
        .unwrap();
        assert!(requested.e_tag);
        assert!(requested.object_size);
        assert!(!requested.checksum);
        assert!(!requested.object_parts);
        assert!(!requested.storage_class);
    }

    #[test]
    fn requires_one_attribute() {
        assert_eq!(
            *RequestedAttributes::from_request(&[]).unwrap_err().code(),
            s3s::S3ErrorCode::InvalidArgument
        );
        assert_eq!(
            *RequestedAttributes::from_request(&[ObjectAttributes::from_static("unknown")])
                .unwrap_err()
                .code(),
            s3s::S3ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn parses_part_marker() {
        assert_eq!(parse_part_number_marker(None).unwrap(), None);
        assert_eq!(parse_part_number_marker(Some(3)).unwrap(), Some(3));
        assert_eq!(
            parse_part_number_marker(Some(i32::MAX)).unwrap(),
            Some(u16::MAX)
        );
        assert_eq!(
            *parse_part_number_marker(Some(-1)).unwrap_err().code(),
            s3s::S3ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn parses_max_parts() {
        assert_eq!(
            parse_max_parts(None).unwrap(),
            ListPartsOperation::DEFAULT_MAX_PARTS
        );
        assert_eq!(parse_max_parts(Some(2)).unwrap(), 2);
        assert_eq!(
            parse_max_parts(Some(i32::MAX)).unwrap(),
            ListPartsOperation::DEFAULT_MAX_PARTS
        );
        assert_eq!(
            *parse_max_parts(Some(-1)).unwrap_err().code(),
            s3s::S3ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn paginates_parts_in_order() {
        let result = result(3, vec![part(1, 10), part(2, 20), part(3, 30)]);
        let page = attributes_parts(&result, None, None, 2).unwrap();
        assert_eq!(page.total_parts_count, Some(3));
        assert_eq!(page.is_truncated, Some(true));
        assert_eq!(page.max_parts, Some(2));
        assert_eq!(page.next_part_number_marker, Some(2));
        let numbers: Vec<i32> = page
            .parts
            .unwrap()
            .iter()
            .map(|part| part.part_number.unwrap())
            .collect();
        assert_eq!(numbers, vec![1, 2]);
    }

    #[test]
    fn marker_filters_and_limits() {
        let result = result(3, vec![part(1, 10), part(2, 20), part(3, 30)]);
        let page = attributes_parts(&result, Some(1), Some(1), 10).unwrap();
        assert_eq!(page.part_number_marker, Some(1));
        assert_eq!(page.is_truncated, Some(false));
        assert_eq!(page.next_part_number_marker, None);
        let numbers: Vec<i32> = page
            .parts
            .unwrap()
            .iter()
            .map(|part| part.part_number.unwrap())
            .collect();
        assert_eq!(numbers, vec![2, 3]);
    }

    #[test]
    fn zero_page_advances_by_marker() {
        let result = result(2, vec![part(1, 10), part(2, 20)]);
        let page = attributes_parts(&result, Some(1), Some(1), 0).unwrap();
        assert_eq!(page.is_truncated, Some(true));
        assert!(page.parts.unwrap().is_empty());
        assert_eq!(page.next_part_number_marker, Some(1));
    }
}
