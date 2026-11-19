//! Lists the versions of a distributed bucket across its key ranges: each range seeks in this
//! member's ordered inventory or asks a holder, and the pages merge in key order with S3 markers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::structs::storage::distribution::DistributionBinding;
use aruna_operations::distribution::versions::{
    InventoryVersionsInput, InventoryVersionsOperation,
};
use aruna_operations::driver::drive;
use aruna_operations::s3::object::member_merge::{
    MarkerQuery, MergedMarkersOperation, RemoteVersion, VersionSource, VersionsPart,
};
use s3s::dto::{
    CommonPrefix, DeleteMarkerEntry, ListObjectVersionsInput, ListObjectVersionsOutput,
    ObjectVersion, ObjectVersionStorageClass, Owner,
};
use s3s::{S3Result, s3_error};
use ulid::Ulid;

use super::list::{e_tag, timestamp};
use crate::s3::service::ArunaS3Service;

impl ArunaS3Service {
    /// The versions of the key range a forwarded listing names, from this member's inventory.
    pub(in crate::s3::service) async fn range_versions(
        &self,
        binding: &DistributionBinding,
        input: ListObjectVersionsInput,
        (lower, upper): (String, Option<String>),
        (group_id, max_keys, version_id_marker): (Ulid, usize, Option<Ulid>),
    ) -> S3Result<ListObjectVersionsOutput> {
        let operation = InventoryVersionsOperation::new(InventoryVersionsInput {
            set_id: binding.set_id,
            bucket: input.bucket.clone(),
            prefix: input.prefix.clone(),
            delimiter: input.delimiter.clone(),
            key_marker: input.key_marker.clone(),
            version_id_marker,
            max_keys,
            lower,
            upper,
        });
        let result = drive(operation, &self.state)
            .await
            .map_err(|error| s3_error!(InternalError, "The version listing failed: {error}"))?;
        Ok(self
            .list_versions_output(input, group_id, max_keys, result)
            .output)
    }

    fn remote_version(
        &self,
        entry: RemoteVersion,
        owner: &Owner,
        url_encoded: bool,
    ) -> ObjectVersion {
        ObjectVersion {
            key: Some(self.encoded(entry.key, url_encoded)),
            version_id: entry.version_id,
            is_latest: entry.is_latest,
            last_modified: timestamp(entry.last_modified),
            e_tag: e_tag(entry.etag),
            size: entry.size,
            owner: Some(owner.clone()),
            storage_class: Some(ObjectVersionStorageClass::from_static(
                ObjectVersionStorageClass::STANDARD,
            )),
            ..Default::default()
        }
    }

    fn remote_marker(
        &self,
        entry: RemoteVersion,
        owner: &Owner,
        url_encoded: bool,
    ) -> DeleteMarkerEntry {
        DeleteMarkerEntry {
            key: Some(self.encoded(entry.key, url_encoded)),
            version_id: entry.version_id,
            is_latest: entry.is_latest,
            last_modified: timestamp(entry.last_modified),
            owner: Some(owner.clone()),
        }
    }

    /// One merged version page over every range under the prefix.
    pub(in crate::s3::service) async fn merged_versions(
        &self,
        user: &UserAccess,
        binding: &DistributionBinding,
        input: ListObjectVersionsInput,
        (group_id, max_keys): (Ulid, usize),
    ) -> S3Result<ListObjectVersionsOutput> {
        let url_encoded = url_of(&input);
        let owner = Owner {
            display_name: None,
            id: Some(group_id.to_string()),
        };
        let source = VersionSource {
            from: self.merge_caller(user, binding),
            query: MarkerQuery {
                bucket: input.bucket.clone(),
                prefix: input.prefix.clone(),
                delimiter: input.delimiter.clone(),
                key_marker: input.key_marker.clone(),
                id_marker: input.version_id_marker.clone(),
            },
        };
        let merge = MergedMarkersOperation::new(source, max_keys);
        let merged =
            (drive(merge, self.state.as_ref()).await).map_err(|error| self.merge_error(error))?;
        let (mut versions, mut markers) = (Vec::new(), Vec::new());
        for part in merged.parts {
            match part {
                VersionsPart::Local(result) => {
                    let output = self
                        .list_versions_output(input.clone(), group_id, max_keys, *result)
                        .output;
                    versions.extend(output.versions.unwrap_or_default());
                    markers.extend(output.delete_markers.unwrap_or_default());
                }
                VersionsPart::Remote {
                    versions: listed,
                    markers: deleted,
                } => {
                    versions.extend(
                        (listed.into_iter())
                            .map(|entry| self.remote_version(entry, &owner, url_encoded)),
                    );
                    markers.extend(
                        (deleted.into_iter())
                            .map(|entry| self.remote_marker(entry, &owner, url_encoded)),
                    );
                }
            }
        }
        let next = merged.next;
        let prefixes = merged.prefixes;
        let encode = |value: String| self.encoded(value, url_encoded);
        Ok(ListObjectVersionsOutput {
            name: Some(input.bucket.clone()),
            prefix: input.prefix.clone().map(&encode),
            delimiter: input.delimiter.clone().map(&encode),
            key_marker: input.key_marker.clone().map(&encode),
            version_id_marker: input.version_id_marker.clone(),
            max_keys: Some(i32::try_from(max_keys).unwrap_or(i32::MAX)),
            is_truncated: Some(next.is_some()),
            next_key_marker: next.as_ref().map(|(key, _)| encode(key.clone())),
            next_version_id_marker: next.and_then(|(_, id)| id),
            versions: Some(versions),
            delete_markers: Some(markers),
            common_prefixes: Some(
                (prefixes.into_iter())
                    .map(|prefix| CommonPrefix {
                        prefix: Some(encode(prefix)),
                    })
                    .collect(),
            ),
            encoding_type: input.encoding_type.clone(),
            ..Default::default()
        })
    }
}

fn url_of(input: &ListObjectVersionsInput) -> bool {
    (input.encoding_type.as_ref())
        .is_some_and(|encoding| encoding.as_str() == s3s::dto::EncodingType::URL)
}
