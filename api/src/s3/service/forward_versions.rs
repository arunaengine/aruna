//! Lists the versions of a distributed bucket across its key ranges: each range seeks in this
//! member's ordered inventory or asks a holder, and the pages merge in key order with S3 markers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionSnapshot, KeyRange,
};
use aruna_operations::distribution::versions::{
    InventoryVersionsInput, InventoryVersionsOperation,
};
use aruna_operations::driver::drive;
use s3s::dto::{
    CommonPrefix, DeleteMarkerEntry, ListObjectVersionsInput, ListObjectVersionsOutput,
    ObjectVersion, ObjectVersionStorageClass, Owner,
};
use s3s::{S3Result, s3_error};
use ulid::Ulid;

use super::ArunaS3Service;
use super::forward::xml_field;
use super::forward_list::{
    MAX_PAGE_BYTES, MAX_RANGES_PER_PAGE, e_tag, elements, range_query, timestamp,
};

/// The entries one range answered for a version listing, raw positions included.
#[derive(Default)]
struct VersionsPart {
    versions: Vec<(String, ObjectVersion)>,
    markers: Vec<(String, DeleteMarkerEntry)>,
    prefixes: Vec<String>,
    /// The raw next markers when the range has more entries.
    next: Option<(String, Option<String>)>,
}

impl ArunaS3Service {
    /// One range's versions from this member's ordered inventory.
    async fn local_versions(
        &self,
        input: &ListObjectVersionsInput,
        (set_id, range): (Ulid, &KeyRange),
        (markers, remaining, group_id): ((Option<String>, Option<Ulid>), usize, Ulid),
    ) -> S3Result<VersionsPart> {
        let operation = InventoryVersionsOperation::new(InventoryVersionsInput {
            set_id,
            bucket: input.bucket.clone(),
            prefix: input.prefix.clone(),
            delimiter: input.delimiter.clone(),
            key_marker: markers.0,
            version_id_marker: markers.1,
            max_keys: remaining,
            lower: range.lower.clone(),
            upper: range.upper.clone(),
        });
        let result = drive(operation, &self.state)
            .await
            .map_err(|error| s3_error!(InternalError, "The version listing failed: {error}"))?;
        let next = result.is_truncated.then(|| {
            (
                result.next_key_marker.clone().unwrap_or_default(),
                result.next_version_marker.map(|id| id.to_string()),
            )
        });
        let prefixes = result.common_prefixes.clone();
        let output = self
            .list_versions_output(input.clone(), group_id, remaining, result)
            .output;
        let url_encoded = url_of(input);
        let raw = |key: Option<&str>| Self::raw(key.unwrap_or_default(), url_encoded);
        Ok(VersionsPart {
            versions: (output.versions.unwrap_or_default().into_iter())
                .map(|version| (raw(version.key.as_deref()), version))
                .collect(),
            markers: (output.delete_markers.unwrap_or_default().into_iter())
                .map(|marker| (raw(marker.key.as_deref()), marker))
                .collect(),
            prefixes,
            next,
        })
    }

    /// The versions of the key range a forwarded listing names, from this member's inventory.
    pub(super) async fn range_versions(
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

    /// One range's versions from a holder.
    async fn remote_versions(
        &self,
        user: &UserAccess,
        context: (&DistributionBinding, &DistributionSnapshot),
        range: &KeyRange,
        input: &ListObjectVersionsInput,
        (markers, remaining, owner): ((Option<String>, Option<String>), usize, Option<&Owner>),
    ) -> S3Result<VersionsPart> {
        let mut base = vec![
            ("versions", String::new()),
            ("max-keys", remaining.to_string()),
        ];
        base.extend(input.prefix.clone().map(|prefix| ("prefix", prefix)));
        base.extend(
            input
                .delimiter
                .clone()
                .map(|delimiter| ("delimiter", delimiter)),
        );
        base.extend(markers.0.map(|key| ("key-marker", key)));
        base.extend(markers.1.map(|id| ("version-id-marker", id)));
        let query = range_query(&base, range);
        let request = ("ListObjectVersions", input.bucket.as_str(), query);
        let body = self.ask_holders(user, context, range, request).await?;
        let url_encoded = url_of(input);
        let mut part = VersionsPart::default();
        for entry in elements(&body, "Version") {
            let Some(key) = xml_field(entry, "Key") else {
                continue;
            };
            part.versions.push((
                key.clone(),
                ObjectVersion {
                    key: Some(self.encoded(key, url_encoded)),
                    version_id: xml_field(entry, "VersionId"),
                    is_latest: xml_field(entry, "IsLatest").map(|value| value == "true"),
                    last_modified: timestamp(xml_field(entry, "LastModified")),
                    e_tag: e_tag(xml_field(entry, "ETag")),
                    size: xml_field(entry, "Size").and_then(|size| size.parse().ok()),
                    owner: owner.cloned(),
                    storage_class: Some(ObjectVersionStorageClass::from_static(
                        ObjectVersionStorageClass::STANDARD,
                    )),
                    ..Default::default()
                },
            ));
        }
        for entry in elements(&body, "DeleteMarker") {
            let Some(key) = xml_field(entry, "Key") else {
                continue;
            };
            part.markers.push((
                key.clone(),
                DeleteMarkerEntry {
                    key: Some(self.encoded(key, url_encoded)),
                    version_id: xml_field(entry, "VersionId"),
                    is_latest: xml_field(entry, "IsLatest").map(|value| value == "true"),
                    last_modified: timestamp(xml_field(entry, "LastModified")),
                    owner: owner.cloned(),
                },
            ));
        }
        part.prefixes = (elements(&body, "CommonPrefixes").into_iter())
            .filter_map(|entry| xml_field(entry, "Prefix"))
            .collect();
        if xml_field(&body, "IsTruncated").as_deref() == Some("true") {
            let key = xml_field(&body, "NextKeyMarker").unwrap_or_default();
            part.next = Some((key, xml_field(&body, "NextVersionIdMarker")));
        }
        Ok(part)
    }

    /// One merged version page over every range under the prefix.
    pub(super) async fn merged_versions(
        &self,
        user: &UserAccess,
        binding: &DistributionBinding,
        input: ListObjectVersionsInput,
        (group_id, max_keys): (Ulid, usize),
    ) -> S3Result<ListObjectVersionsOutput> {
        let snapshot = self.snapshot(binding).await?;
        let url_encoded = url_of(&input);
        let owner = Owner {
            display_name: None,
            id: Some(group_id.to_string()),
        };
        let prefix = input.prefix.clone().unwrap_or_default();
        let marker = (input.key_marker.clone()).filter(|marker| !marker.is_empty());
        let version_marker = input.version_id_marker.clone();
        let mut versions = Vec::new();
        let mut markers = Vec::new();
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
        let mut first = true;
        let mut bytes = 0usize;
        for (visited, range) in ranges.enumerate() {
            let remaining =
                max_keys.saturating_sub(versions.len() + markers.len() + prefixes.len());
            if remaining == 0 || visited == MAX_RANGES_PER_PAGE || bytes >= MAX_PAGE_BYTES {
                // Nothing listed yet: the nil version id starts the next page at this range.
                let start = || (range.lower.clone(), Some(Ulid::nil().to_string()));
                next = Some(last.clone().map_or_else(start, |key| (key, None)));
                break;
            }
            let start = match first {
                true => (marker.clone(), version_marker.clone()),
                false => (None, None),
            };
            first = false;
            let part = match snapshot.lists_locally(binding.member_id, range) {
                true => {
                    let id = (start.1.as_deref())
                        .map(|id| Ulid::from_string(id))
                        .transpose()
                        .map_err(|_| s3_error!(InvalidArgument, "Invalid version-id-marker"))?;
                    let ranged = (binding.set_id, range);
                    self.local_versions(&input, ranged, ((start.0, id), remaining, group_id))
                        .await?
                }
                false => {
                    let context = (binding, &snapshot);
                    let query = (start, remaining, Some(&owner));
                    self.remote_versions(user, context, range, &input, query)
                        .await?
                }
            };
            let keys = (part.versions.iter().map(|(key, _)| key))
                .chain(part.markers.iter().map(|(key, _)| key))
                .chain(part.prefixes.iter());
            bytes += keys.map(String::len).sum::<usize>();
            for raw in part.prefixes {
                if prefixes.last() == Some(&raw) {
                    continue;
                }
                prefixes.push(raw);
            }
            let greatest = (part.versions.iter().map(|(key, _)| key))
                .chain(part.markers.iter().map(|(key, _)| key))
                .chain(prefixes.last())
                .max()
                .cloned();
            last = greatest.or(last);
            versions.extend(part.versions.into_iter().map(|(_, version)| version));
            markers.extend(part.markers.into_iter().map(|(_, marker)| marker));
            if part.next.is_some() {
                next = part.next;
                break;
            }
        }
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
