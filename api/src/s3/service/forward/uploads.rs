//! Lists the open multipart uploads of a distributed bucket across its key ranges: uploads live
//! only on the coordinator of their key's range, so each range is listed from this member's
//! upload inventory or by its coordinator, and the pages merge in key order with S3 markers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionSnapshot, KeyRange,
};
use aruna_operations::distribution::guard::upload_tags;
use aruna_operations::distribution::uploads::{InventoryUploadsInput, InventoryUploadsOperation};
use aruna_operations::driver::drive;
use aruna_operations::s3::multipart::uploads::ListUploadsResult;
use http::Method;
use s3s::dto::{
    CommonPrefix, Initiator, ListMultipartUploadsInput, ListMultipartUploadsOutput,
    MultipartUpload as S3MultipartUpload, Owner, StorageClass,
};
use s3s::{S3Response, S3Result, s3_error};
use ulid::Ulid;

use super::list::{
    MAX_PAGE_BYTES, MAX_RANGES_PER_PAGE, elements, incomplete, range_query, timestamp, unreachable,
};
use super::{Forward, ForwardBody, ForwardRequest, answer_text, xml_field};
use crate::s3::error::guard_error;
use crate::s3::service::ArunaS3Service;
use crate::s3::service::listing::list_uploads_output;

/// The entries one range answered for an upload listing, raw keys included.
#[derive(Default)]
struct UploadsPart {
    uploads: Vec<(String, S3MultipartUpload)>,
    prefixes: Vec<String>,
    /// The raw next markers when the range has more entries.
    next: Option<(String, Option<String>)>,
}

fn url_of(input: &ListMultipartUploadsInput) -> bool {
    (input.encoding_type.as_ref())
        .is_some_and(|encoding| encoding.as_str() == s3s::dto::EncodingType::URL)
}

impl ArunaS3Service {
    /// One range's open uploads from this member's upload inventory, ids as clients see them.
    pub(in crate::s3::service) async fn range_uploads(
        &self,
        binding: &DistributionBinding,
        input: ListMultipartUploadsInput,
        (lower, upper): (String, Option<String>),
        (upload_id_marker, max_uploads): (Option<Ulid>, usize),
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let operation = InventoryUploadsOperation::new(InventoryUploadsInput {
            set_id: binding.set_id,
            bucket: input.bucket.clone(),
            prefix: input.prefix.clone(),
            delimiter: input.delimiter.clone(),
            key_marker: input.key_marker.clone(),
            upload_id_marker,
            max_uploads,
            lower,
            upper,
        });
        let result = drive(operation, &self.state)
            .await
            .map_err(|error| s3_error!(InternalError, "The upload listing failed: {error}"))?;
        let listed: Vec<(String, Ulid)> = (result.uploads.iter())
            .map(|upload| (upload.key.clone(), upload.upload_id))
            .collect();
        let mut response = list_uploads_output(input, max_uploads, result);
        let tags = upload_tags(self.state.as_ref(), binding, &listed)
            .await
            .map_err(|error| guard_error(&error))?;
        let output = &mut response.output;
        for (upload, tag) in output.uploads.iter_mut().flatten().zip(tags) {
            upload.upload_id = Some(tag.encode());
        }
        Ok(response)
    }

    /// One range's uploads from this member's inventory, for a merged page.
    async fn local_uploads(
        &self,
        binding: &DistributionBinding,
        input: &ListMultipartUploadsInput,
        range: &KeyRange,
        (markers, remaining): ((Option<String>, Option<Ulid>), usize),
    ) -> S3Result<UploadsPart> {
        let operation = InventoryUploadsOperation::new(InventoryUploadsInput {
            set_id: binding.set_id,
            bucket: input.bucket.clone(),
            prefix: input.prefix.clone(),
            delimiter: input.delimiter.clone(),
            key_marker: markers.0,
            upload_id_marker: markers.1,
            max_uploads: remaining,
            lower: range.lower.clone(),
            upper: range.upper.clone(),
        });
        let result: ListUploadsResult = drive(operation, &self.state)
            .await
            .map_err(|error| s3_error!(InternalError, "The upload listing failed: {error}"))?;
        let next = result.is_truncated.then(|| {
            (
                result.next_key_marker.clone().unwrap_or_default(),
                result.next_upload_marker.map(|id| id.to_string()),
            )
        });
        let listed: Vec<(String, Ulid)> = (result.uploads.iter())
            .map(|upload| (upload.key.clone(), upload.upload_id))
            .collect();
        let tags = upload_tags(self.state.as_ref(), binding, &listed)
            .await
            .map_err(|error| guard_error(&error))?;
        let prefixes = result.common_prefixes.clone();
        let output = list_uploads_output(input.clone(), remaining, result).output;
        let uploads = (output.uploads.unwrap_or_default().into_iter())
            .zip(listed.into_iter().zip(tags))
            .map(|(mut upload, ((key, _), tag))| {
                upload.upload_id = Some(tag.encode());
                (key, upload)
            })
            .collect();
        Ok(UploadsPart {
            uploads,
            prefixes,
            next,
        })
    }

    /// One range's uploads from its coordinator, the only member that keeps them.
    async fn remote_uploads(
        &self,
        user: &UserAccess,
        (binding, snapshot): (&DistributionBinding, &DistributionSnapshot),
        range: &KeyRange,
        input: &ListMultipartUploadsInput,
        (markers, remaining, owner): ((Option<String>, Option<String>), usize, &Owner),
    ) -> S3Result<UploadsPart> {
        let coordinator = snapshot
            .member(range.coordinator)
            .ok_or_else(|| incomplete(binding.set_id, range))?;
        let mut base = vec![
            ("uploads", String::new()),
            ("max-uploads", remaining.to_string()),
        ];
        base.extend(input.prefix.clone().map(|prefix| ("prefix", prefix)));
        base.extend(
            input
                .delimiter
                .clone()
                .map(|delimiter| ("delimiter", delimiter)),
        );
        base.extend(markers.0.map(|key| ("key-marker", key)));
        base.extend(markers.1.map(|id| ("upload-id-marker", id)));
        let forward = Forward {
            set_id: binding.set_id,
            range_id: range.range_id,
            node_id: coordinator.node_id,
        };
        let request = ForwardRequest {
            method: Method::GET,
            operation: "ListMultipartUploads",
            bucket: input.bucket.clone(),
            key: String::new(),
            query: Some(range_query(&base, range)),
            headers: Vec::new(),
            body: ForwardBody::Empty,
        };
        let body = match self.forward_write(user, &forward, request).await {
            Ok(response) => answer_text(response).await?,
            Err(error) if unreachable(&error) => return Err(incomplete(binding.set_id, range)),
            Err(error) => return Err(error),
        };
        let url_encoded = url_of(input);
        let mut part = UploadsPart::default();
        for entry in elements(&body, "Upload") {
            let Some(key) = xml_field(entry, "Key") else {
                continue;
            };
            part.uploads.push((
                key.clone(),
                S3MultipartUpload {
                    key: Some(self.encoded(key, url_encoded)),
                    upload_id: xml_field(entry, "UploadId"),
                    initiated: timestamp(xml_field(entry, "Initiated")),
                    initiator: Some(Initiator {
                        display_name: None,
                        id: xml_field(entry, "ID"),
                    }),
                    owner: Some(owner.clone()),
                    storage_class: Some(StorageClass::from_static(StorageClass::STANDARD)),
                    ..Default::default()
                },
            ));
        }
        part.prefixes = (elements(&body, "CommonPrefixes").into_iter())
            .filter_map(|entry| xml_field(entry, "Prefix"))
            .collect();
        if xml_field(&body, "IsTruncated").as_deref() == Some("true") {
            let key = xml_field(&body, "NextKeyMarker").unwrap_or_default();
            part.next = Some((key, xml_field(&body, "NextUploadIdMarker")));
        }
        Ok(part)
    }

    /// One merged upload page over every range under the prefix.
    pub(in crate::s3::service) async fn merged_uploads(
        &self,
        user: &UserAccess,
        binding: &DistributionBinding,
        input: ListMultipartUploadsInput,
        (group_id, upload_id_marker, max_uploads): (Ulid, Option<Ulid>, usize),
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let snapshot = self.snapshot(binding).await?;
        let url_encoded = url_of(&input);
        let owner = Owner {
            display_name: None,
            id: Some(group_id.to_string()),
        };
        let prefix = input.prefix.clone().unwrap_or_default();
        let marker = (input.key_marker.clone()).filter(|marker| !marker.is_empty());
        let mut uploads = Vec::new();
        let mut prefixes: Vec<String> = Vec::new();
        let mut next: Option<(String, Option<String>)> = None;
        let mut last: Option<String> = None;
        let ranges = snapshot.listed_ranges(&prefix);
        let ranges = ranges.into_iter().filter(|range| {
            marker.as_ref().is_none_or(|key| {
                range
                    .upper
                    .as_deref()
                    .is_none_or(|upper| key.as_str() < upper)
            })
        });
        let local = |range: &KeyRange| range.coordinator == binding.member_id;
        let mut first = true;
        let mut bytes = 0usize;
        for (visited, range) in ranges.enumerate() {
            let remaining = max_uploads.saturating_sub(uploads.len() + prefixes.len());
            if remaining == 0 || visited == MAX_RANGES_PER_PAGE || bytes >= MAX_PAGE_BYTES {
                // Nothing listed yet: the nil upload id starts the next page at this range.
                let start = || (range.lower.clone(), Some(Ulid::nil().to_string()));
                next = Some(last.clone().map_or_else(start, |key| (key, None)));
                break;
            }
            // The key marker skips a common prefix in every range; the id only in the first.
            let start = (marker.clone(), upload_id_marker.filter(|_| first));
            first = false;
            let part = match local(range) {
                true => {
                    self.local_uploads(binding, &input, range, (start, remaining))
                        .await?
                }
                false => {
                    let start = (start.0, start.1.map(|id| id.to_string()));
                    let context = (binding, &snapshot);
                    let query = (start, remaining, &owner);
                    self.remote_uploads(user, context, range, &input, query)
                        .await?
                }
            };
            let keys = (part.uploads.iter().map(|(key, _)| key)).chain(part.prefixes.iter());
            bytes += keys.map(String::len).sum::<usize>();
            for raw in part.prefixes {
                if prefixes.last() == Some(&raw) {
                    continue;
                }
                prefixes.push(raw);
            }
            let greatest = (part.uploads.iter().map(|(key, _)| key))
                .chain(prefixes.last())
                .max()
                .cloned();
            last = greatest.or(last);
            uploads.extend(part.uploads.into_iter().map(|(_, upload)| upload));
            if part.next.is_some() {
                next = part.next;
                break;
            }
        }
        let encode = |value: String| self.encoded(value, url_encoded);
        Ok(S3Response::new(ListMultipartUploadsOutput {
            bucket: Some(input.bucket.clone()),
            prefix: input.prefix.clone().map(&encode),
            delimiter: input.delimiter.clone().map(&encode),
            key_marker: input.key_marker.clone().map(&encode),
            upload_id_marker: input.upload_id_marker.clone(),
            max_uploads: Some(i32::try_from(max_uploads).unwrap_or(i32::MAX)),
            is_truncated: Some(next.is_some()),
            next_key_marker: next.as_ref().map(|(key, _)| encode(key.clone())),
            next_upload_id_marker: next.and_then(|(_, id)| id),
            uploads: Some(uploads),
            common_prefixes: Some(
                (prefixes.into_iter())
                    .map(|prefix| CommonPrefix {
                        prefix: Some(encode(prefix)),
                    })
                    .collect(),
            ),
            encoding_type: input.encoding_type.clone(),
            ..Default::default()
        }))
    }
}
