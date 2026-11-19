//! Lists a distributed bucket's open multipart uploads across its key ranges, merged in key
//! order with S3 markers: each range from local upload inventory or by its coordinator.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Uploads live only on the coordinator of their key's range.

use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::structs::storage::distribution::DistributionBinding;
use aruna_operations::distribution::routing::upload_tags;
use aruna_operations::distribution::uploads::{InventoryUploadsInput, InventoryUploadsOperation};
use aruna_operations::driver::drive;
use aruna_operations::s3::object::member_merge::{
    MarkerQuery, MergedMarkersOperation, UploadSource, UploadsPart,
};
use s3s::dto::{
    CommonPrefix, Initiator, ListMultipartUploadsInput, ListMultipartUploadsOutput,
    MultipartUpload as S3MultipartUpload, Owner, StorageClass,
};
use s3s::{S3Response, S3Result, s3_error};
use ulid::Ulid;

use super::list::timestamp;
use crate::s3::error::guard_error;
use crate::s3::service::ArunaS3Service;
use crate::s3::service::listing::list_uploads_output;

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
        let tags = drive(upload_tags(binding, &listed), self.state.as_ref())
            .await
            .map_err(|error| guard_error(&error))?;
        let output = &mut response.output;
        for (upload, tag) in output.uploads.iter_mut().flatten().zip(tags) {
            upload.upload_id = Some(tag.encode());
        }
        Ok(response)
    }

    /// One merged upload page over every range under the prefix.
    pub(in crate::s3::service) async fn merged_uploads(
        &self,
        user: &UserAccess,
        binding: &DistributionBinding,
        input: ListMultipartUploadsInput,
        (group_id, upload_id_marker, max_uploads): (Ulid, Option<Ulid>, usize),
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let url_encoded = url_of(&input);
        let owner = Owner {
            display_name: None,
            id: Some(group_id.to_string()),
        };
        let source = UploadSource {
            from: self.merge_caller(user, binding),
            query: MarkerQuery {
                bucket: input.bucket.clone(),
                prefix: input.prefix.clone(),
                delimiter: input.delimiter.clone(),
                key_marker: input.key_marker.clone(),
                id_marker: upload_id_marker.map(|id| id.to_string()),
            },
        };
        let merge = MergedMarkersOperation::new(source, max_uploads);
        let merged =
            (drive(merge, self.state.as_ref()).await).map_err(|error| self.merge_error(error))?;
        let mut uploads: Vec<S3MultipartUpload> = Vec::new();
        for part in merged.parts {
            match part {
                UploadsPart::Local(result, tags) => {
                    let output = list_uploads_output(input.clone(), max_uploads, *result).output;
                    let listed = output.uploads.unwrap_or_default().into_iter().zip(tags);
                    uploads.extend(listed.map(|(mut upload, tag)| {
                        upload.upload_id = Some(tag);
                        upload
                    }));
                }
                UploadsPart::Remote(listed) => {
                    uploads.extend(listed.into_iter().map(|entry| S3MultipartUpload {
                        key: Some(self.encoded(entry.key, url_encoded)),
                        upload_id: entry.upload_id,
                        initiated: timestamp(entry.initiated),
                        initiator: Some(Initiator {
                            display_name: None,
                            id: entry.initiator,
                        }),
                        owner: Some(owner.clone()),
                        storage_class: Some(StorageClass::from_static(StorageClass::STANDARD)),
                        ..Default::default()
                    }));
                }
            }
        }
        let next = merged.next;
        let prefixes = merged.prefixes;
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
