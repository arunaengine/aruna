//! Database releases: tagged release crate versions, the rows that find them, and the check
//! which exact versions a release pins, made with the caller's own access.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::NodeId;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::BUCKET_RELEASE_KEYSPACE;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::identifier::{Identifier, IdentifierTarget};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::bucket_metadata::{
    RELEASE_TAG, ReleasePointer, ReleaseRef, release_prefix,
};
use aruna_core::structs::storage::data::identity::content_id;
use aruna_core::structs::storage::distribution::KeyRange;
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::origin::{OriginVersion, SourcePin};
use aruna_core::structs::storage::replication::VersionedObjectArn;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use ulid::Ulid;

use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::distribution::status::read_view;
use crate::driver::{DriverContext, drive};
use crate::metadata::protocol::AuthToken;
use crate::s3::bucket::get::GetBucketOperation;
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectOperation};
use crate::s3::object::list::{ListBucketInput, ListBucketOperation};
use crate::s3::object::origin::read_origin;

/// Rows read per storage page while looking for a bucket's releases.
const ROW_PAGE: usize = 256;

/// One exact version whose releases are asked for.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MembershipQuery {
    pub key: String,
    pub version_id: Ulid,
}

/// The releases that pin one queried version; `complete` is false when some release could not
/// be checked, so one may be missing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReleaseMembers {
    pub releases: Vec<ReleaseRef>,
    pub complete: bool,
}

impl ReleaseMembers {
    fn complete() -> Self {
        Self {
            releases: Vec::new(),
            complete: true,
        }
    }

    fn merge(&mut self, other: ReleaseMembers) {
        for release in other.releases {
            if !self.releases.contains(&release) {
                self.releases.push(release);
            }
        }
        self.complete &= other.complete;
    }
}

#[derive(Debug, Error)]
pub enum ReleaseError {
    #[error("release state unavailable: {0}")]
    Unavailable(String),
}

/// Where the bucket lives, for the exact ids its versions are named by.
#[derive(Clone, Copy, Debug)]
pub struct BucketScope<'a> {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub bucket: &'a str,
    pub info: &'a BucketInfo,
}

impl BucketScope<'_> {
    /// The exact id of a version of this bucket: its global id in a distributed bucket, else
    /// the versioned node ARN.
    pub fn exact(&self, key: &str, version_id: Ulid) -> Option<OriginVersion> {
        match self.info.distribution {
            Some(_) => Some(OriginVersion::Global(GlobalId::Version {
                realm_id: self.realm_id,
                version_id,
            })),
            None => {
                VersionedObjectArn::new(self.realm_id, self.node_id, self.bucket, key, version_id)
                    .ok()
                    .map(OriginVersion::Node)
            }
        }
    }
}

/// Checks the releases held on this node against `queries`. With `ranges`, only release
/// crates whose key lies in one of them count, as for a holder answering for those ranges.
pub async fn local_members(
    context: &DriverContext,
    scope: BucketScope<'_>,
    auth: &AuthContext,
    queries: &[MembershipQuery],
    ranges: Option<&[KeyRange]>,
) -> Result<Vec<ReleaseMembers>, ReleaseError> {
    let mut answers = vec![ReleaseMembers::complete(); queries.len()];
    for (version_id, pointer) in release_rows(context, scope.bucket).await? {
        if ranges.is_some_and(|ranges| !ranges.iter().any(|range| range.contains(&pointer.key))) {
            continue;
        }
        let asked: Vec<usize> = (queries.iter().enumerate())
            .filter(|(_, query)| query.key.starts_with(&pointer.prefix) && query.key != pointer.key)
            .map(|(index, _)| index)
            .collect();
        if asked.is_empty() {
            continue;
        }
        let checked = check_release(context, scope, auth, version_id, &pointer).await;
        match checked {
            Ok(Some((release, pinned))) => {
                for index in asked {
                    let query = &queries[index];
                    let id = scope
                        .exact(&query.key, query.version_id)
                        .map(|id| id.to_id());
                    if id.is_some_and(|id| pinned.contains(&id)) {
                        answers[index].releases.push(release.clone());
                    }
                }
            }
            Ok(None) => {}
            Err(_) => asked
                .into_iter()
                .for_each(|index| answers[index].complete = false),
        }
    }
    Ok(answers)
}

/// Every release row of `bucket` this node holds, in key order.
pub async fn release_rows(
    context: &DriverContext,
    bucket: &str,
) -> Result<Vec<(Ulid, ReleasePointer)>, ReleaseError> {
    let prefix = release_prefix(bucket);
    let mut rows = Vec::new();
    let mut start = None;
    loop {
        let iter = StorageEffect::Iter {
            key_space: BUCKET_RELEASE_KEYSPACE.to_string(),
            prefix: Some(prefix.clone().into()),
            start: start.take().map(IterStart::After),
            limit: ROW_PAGE,
            txn_id: None,
        };
        let (values, next) = match context.storage_handle.send_storage_effect(iter).await {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            other => return Err(ReleaseError::Unavailable(format!("{other:?}"))),
        };
        for (key, value) in values {
            let Some(bytes) = key.get(prefix.len()..) else {
                continue;
            };
            let Ok(id) = <[u8; 16]>::try_from(bytes) else {
                continue;
            };
            let pointer = ReleasePointer::from_bytes(value.as_ref())
                .map_err(|error| ReleaseError::Unavailable(error.to_string()))?;
            rows.push((Ulid::from_bytes(id), pointer));
        }
        match next {
            Some(next) => start = Some(next),
            None => return Ok(rows),
        }
    }
}

/// A release the caller may read, with the exact ids it pins. `None` when the caller may not
/// read it, or its version is gone or carries no release tag.
async fn check_release(
    context: &DriverContext,
    scope: BucketScope<'_>,
    auth: &AuthContext,
    version_id: Ulid,
    pointer: &ReleasePointer,
) -> Result<Option<(ReleaseRef, BTreeSet<String>)>, ReleaseError> {
    let path = object_permission_path(
        scope.realm_id,
        scope.info.group_id,
        scope.node_id,
        scope.bucket,
        &pointer.key,
    );
    let extras = PolicyRequestExtras::operation("s3.GetObject");
    match authorize(
        context,
        scope.realm_id,
        auth,
        &path,
        &Permission::READ,
        extras,
    )
    .await
    {
        Ok(()) => {}
        Err(AuthorizeError::Storage(error)) => {
            return Err(ReleaseError::Unavailable(error.to_string()));
        }
        Err(_) => return Ok(None),
    }
    let Some(crate_version) = scope.exact(&pointer.key, version_id) else {
        return Ok(None);
    };
    let read = GetObjectOperation::new(GetObjectInput {
        bucket: scope.bucket.to_string(),
        key: pointer.key.clone(),
        version_id: Some(version_id),
        range: None,
        group_id: scope.info.group_id,
        user_identity: auth.user_id,
        realm_id: scope.realm_id,
        node_id: scope.node_id,
    })
    .with_restrictions(auth.path_restrictions.clone());
    let result = match drive(read, context).await {
        Ok(result) => result,
        Err(
            GetObjectError::NoSuchKey
            | GetObjectError::NoSuchVersion
            | GetObjectError::DeleteMarker,
        ) => return Ok(None),
        Err(error) => return Err(ReleaseError::Unavailable(error.to_string())),
    };
    if result.metadata.get(RELEASE_TAG) != Some(&pointer.label) {
        return Ok(None);
    }
    let mut bytes = Vec::new();
    let mut blob = result.blob;
    while let Some(chunk) = blob.next().await {
        let chunk = chunk.map_err(|error| ReleaseError::Unavailable(error.to_string()))?;
        bytes.extend_from_slice(&chunk);
    }
    let crate_json: Value = serde_json::from_slice(&bytes)
        .map_err(|error| ReleaseError::Unavailable(error.to_string()))?;
    let release = ReleaseRef {
        crate_version,
        label: pointer.label.clone(),
        upstream: upstream_of(&crate_json),
    };
    Ok(Some((release, pinned_ids(&crate_json))))
}

fn graph(crate_json: &Value) -> impl Iterator<Item = &Value> {
    crate_json["@graph"].as_array().into_iter().flatten()
}

/// The exact version ids the release crate names as its files.
fn pinned_ids(crate_json: &Value) -> BTreeSet<String> {
    graph(crate_json)
        .filter(|entity| entity["@type"] == "File")
        .filter_map(|entity| entity["@id"].as_str().map(str::to_string))
        .collect()
}

/// The upstream identifiers the release crate's root names.
fn upstream_of(crate_json: &Value) -> Vec<Identifier> {
    let entities: BTreeMap<&str, &Value> = graph(crate_json)
        .filter_map(|entity| Some((entity["@id"].as_str()?, entity)))
        .collect();
    let Some(root) = entities.get("./") else {
        return Vec::new();
    };
    let ids = root["identifier"].as_array().into_iter().flatten();
    ids.filter_map(|reference| entities.get(reference["@id"].as_str()?))
        .filter_map(|entity| {
            Some(Identifier {
                name: entity["name"].as_str()?.to_string(),
                value: entity["value"].as_str()?.to_string(),
                url: entity["url"].as_str().unwrap_or_default().to_string(),
                target: IdentifierTarget::Key,
            })
        })
        .collect()
}

/// Release membership a record holder answers for some ranges of a distributed bucket; the
/// ranges it no longer holds come back refused.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RangeMembers {
    pub answers: Vec<ReleaseMembers>,
    pub refused: Vec<Ulid>,
}

/// Who asks: the context this node validated, and the bearer it was validated from. Without
/// a bearer no other node is asked, since an identity is never asserted to another node.
#[derive(Clone, Copy, Debug)]
pub struct MembershipCaller<'a> {
    pub auth: &'a AuthContext,
    pub bearer: Option<&'a str>,
}

/// The releases that pin each queried exact version of `scope`'s bucket, checked with the
/// caller's own access. An ordinary bucket answers from this node. A distributed bucket asks
/// one current record holder per range; a range no holder answers leaves every answer
/// incomplete.
pub async fn release_members(
    context: &DriverContext,
    scope: BucketScope<'_>,
    caller: MembershipCaller<'_>,
    queries: &[MembershipQuery],
) -> Result<Vec<ReleaseMembers>, ReleaseError> {
    let Some(binding) = &scope.info.distribution else {
        return local_members(context, scope, caller.auth, queries, None).await;
    };
    let view = read_view(context, binding.set_id)
        .await
        .map_err(|error| ReleaseError::Unavailable(error.to_string()))?
        .ok_or_else(|| ReleaseError::Unavailable("this node holds no record of the set".into()))?;
    let snapshot = view.snapshot();
    let mut answers = vec![ReleaseMembers::complete(); queries.len()];
    let mut local = Vec::new();
    // Per range, the holders still to try, this member first, else the coordinator first.
    let mut pending: Vec<(Ulid, Vec<NodeId>)> = Vec::new();
    for range in &snapshot.config.ranges {
        let mut holders = snapshot.record_holders(range);
        if holders
            .iter()
            .any(|member| member.member_id == binding.member_id)
        {
            local.push(range.clone());
            continue;
        }
        holders.sort_by_key(|member| member.member_id != range.coordinator);
        let nodes = holders.into_iter().map(|member| member.node_id).collect();
        pending.push((range.range_id, nodes));
    }
    let own = local_members(context, scope, caller.auth, queries, Some(&local)).await?;
    merge(&mut answers, own);
    let token = caller
        .bearer
        .and_then(|bearer| AuthToken::bearer(bearer).ok());
    let handle = context.metadata_handle.as_ref();
    while !pending.is_empty() {
        let (Some(token), Some(handle)) = (token.clone(), handle) else {
            break;
        };
        let mut asked: BTreeMap<NodeId, Vec<Ulid>> = BTreeMap::new();
        let mut unanswered = Vec::new();
        for (range_id, mut nodes) in std::mem::take(&mut pending) {
            match nodes.is_empty() {
                true => unanswered.push(range_id),
                false => {
                    let node = nodes.remove(0);
                    asked.entry(node).or_default().push(range_id);
                    pending.push((range_id, nodes));
                }
            }
        }
        if !unanswered.is_empty() {
            answers
                .iter_mut()
                .for_each(|answer| answer.complete = false);
        }
        for (node, range_ids) in asked {
            let request = (
                scope.bucket.to_string(),
                range_ids.clone(),
                queries.to_vec(),
            );
            let refused = match handle
                .request_release_members(node, Some(token.clone()), request)
                .await
            {
                Ok(answer) if answer.answers.len() == queries.len() => {
                    merge(&mut answers, answer.answers);
                    answer.refused
                }
                Ok(_) | Err(_) => range_ids.clone(),
            };
            // Answered ranges are done; refused ones move to their next holder.
            pending.retain(|(range_id, _)| {
                !range_ids.contains(range_id) || refused.contains(range_id)
            });
        }
    }
    if !pending.is_empty() {
        answers
            .iter_mut()
            .for_each(|answer| answer.complete = false);
    }
    Ok(answers)
}

fn merge(answers: &mut [ReleaseMembers], more: Vec<ReleaseMembers>) {
    for (answer, more) in answers.iter_mut().zip(more) {
        answer.merge(more);
    }
}

/// A holder's answer for the ranges of `range_ids` it holds now, with the caller context it
/// validated from the request's bearer itself.
pub async fn holder_members(
    context: &DriverContext,
    (realm_id, node_id): (RealmId, NodeId),
    auth: &AuthContext,
    (bucket, range_ids, queries): (&str, &[Ulid], &[MembershipQuery]),
) -> Result<RangeMembers, ReleaseError> {
    let info = drive(GetBucketOperation::new(bucket.to_string()), context)
        .await
        .map_err(|error| ReleaseError::Unavailable(error.to_string()))?;
    let Some(binding) = &info.distribution else {
        return Ok(RangeMembers {
            answers: Vec::new(),
            refused: range_ids.to_vec(),
        });
    };
    let view = read_view(context, binding.set_id)
        .await
        .map_err(|error| ReleaseError::Unavailable(error.to_string()))?
        .ok_or_else(|| ReleaseError::Unavailable("this node holds no record of the set".into()))?;
    let held: Vec<KeyRange> = (view.snapshot().held_ranges(binding.member_id).into_iter())
        .filter(|range| range_ids.contains(&range.range_id))
        .cloned()
        .collect();
    let refused = (range_ids.iter())
        .filter(|range_id| !held.iter().any(|range| range.range_id == **range_id))
        .copied()
        .collect();
    let scope = BucketScope {
        realm_id,
        node_id,
        bucket,
        info: &info,
    };
    let answers = local_members(context, scope, auth, queries, Some(&held)).await?;
    Ok(RangeMembers { answers, refused })
}

/// One current file a release pins.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReleaseFile {
    pub key: String,
    pub version: OriginVersion,
    pub size: u64,
    /// The content id, when the bytes have a known BLAKE3.
    pub content: Option<String>,
    /// What pins a reference version: its source and the source's ETag.
    pub pin: Option<(SourcePin, Option<String>)>,
}

/// What a publisher declares about a release besides its files.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReleaseSpec {
    pub label: String,
    pub prefix: String,
    pub upstream: Vec<Identifier>,
    /// Runs that prepared the files, by run id.
    pub preparation: Vec<Ulid>,
}

/// The release crate: deterministic for the same spec and files, so a repeated publish of an
/// unchanged release yields the same bytes.
pub fn release_crate(spec: &ReleaseSpec, files: &[ReleaseFile]) -> Vec<u8> {
    let mut graph = vec![json!({
        "@id": "ro-crate-metadata.json",
        "@type": "CreativeWork",
        "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
        "about": {"@id": "./"}
    })];
    let identifiers: Vec<Value> = (0..spec.upstream.len())
        .map(|index| json!({"@id": format!("#upstream-{index}")}))
        .collect();
    let based_on: Vec<Value> = (spec.upstream.iter())
        .map(|identifier| json!({"@id": identifier.url}))
        .collect();
    let runs: Vec<Value> = (spec.preparation.iter())
        .map(|run_id| json!({"@id": format!("#run-{run_id}")}))
        .collect();
    let parts: Vec<Value> = (files.iter())
        .map(|file| json!({"@id": file.version.to_id()}))
        .collect();
    graph.push(json!({
        "@id": "./",
        "@type": "Dataset",
        "name": format!("Release {}", spec.label),
        "version": spec.label,
        "description": format!("Files under `{}` pinned at publication", spec.prefix),
        "identifier": identifiers,
        "isBasedOn": based_on,
        "mentions": runs,
        "hasPart": parts
    }));
    for (index, identifier) in spec.upstream.iter().enumerate() {
        graph.push(json!({
            "@id": format!("#upstream-{index}"),
            "@type": "PropertyValue",
            "name": identifier.name,
            "value": identifier.value,
            "url": identifier.url
        }));
    }
    for run_id in &spec.preparation {
        graph.push(json!({
            "@id": format!("#run-{run_id}"),
            "@type": "CreateAction",
            "name": format!("Preparation run {run_id}"),
            "identifier": run_id.to_string()
        }));
    }
    let mut pinned = Vec::new();
    for file in files {
        let mut entity = json!({
            "@id": file.version.to_id(),
            "@type": "File",
            "name": file.key,
            "contentSize": file.size.to_string(),
            "contentUrl": file.version.to_id()
        });
        if let Some(content) = &file.content {
            entity["identifier"] = json!(content);
        }
        if let Some((pin, etag)) = &file.pin {
            if let Some(url) = &pin.url {
                entity["isBasedOn"] = json!({"@id": url});
            }
            let id = file.version.to_id();
            let pins = [
                ("source-path", Some(&pin.path)),
                ("source-etag", etag.as_ref()),
                ("source-version", pin.source_version.as_ref()),
            ];
            let mut properties = Vec::new();
            for (term, value) in pins {
                let Some(value) = value else { continue };
                let property = format!("{id}#{term}");
                properties.push(json!({"@id": property}));
                pinned.push(json!({
                    "@id": property,
                    "@type": "PropertyValue",
                    "propertyID": format!("https://w3id.org/aruna/terms/{term}"),
                    "value": value
                }));
            }
            if !properties.is_empty() {
                entity["additionalProperty"] = json!(properties);
            }
        }
        graph.push(entity);
    }
    graph.extend(pinned);
    let document = json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": graph
    });
    serde_json::to_vec_pretty(&document).unwrap_or_default()
}

/// The current versions under `prefix` this node holds, `skip` left out, with what pins them.
pub async fn release_files(
    context: &DriverContext,
    scope: BucketScope<'_>,
    prefix: &str,
    skip: &str,
) -> Result<Vec<ReleaseFile>, ReleaseError> {
    let unavailable = |error: &dyn std::fmt::Display| ReleaseError::Unavailable(error.to_string());
    let mut files = Vec::new();
    let mut continuation = None;
    loop {
        let listed = drive(
            ListBucketOperation::new(ListBucketInput {
                bucket: scope.bucket.to_string(),
                group_id: scope.info.group_id,
                continuation_token: continuation.take(),
                max_keys: Some(1_000),
                prefix: Some(prefix.to_string()),
                delimiter: None,
                start_after: None,
            }),
            context,
        )
        .await
        .map_err(|error| unavailable(&error))?;
        for object in listed.objects {
            let key = object.head.key;
            if key == skip {
                continue;
            }
            let origin = read_origin(
                context,
                (scope.realm_id, scope.node_id),
                scope.bucket,
                &key,
                None,
            )
            .await
            .map_err(|error| unavailable(&error))?;
            let Some(version) = scope.exact(&key, origin.version_id) else {
                continue;
            };
            // Every field comes from the one version the origin read pinned.
            let size = origin.size.unwrap_or_default();
            let pin = origin.source.map(|source| {
                let pin = SourcePin {
                    url: source.url,
                    path: source.path,
                    source_version: source.version,
                };
                (pin, source.etag)
            });
            files.push(ReleaseFile {
                key,
                version,
                size,
                content: origin.content_hash.map(content_id),
                pin,
            });
        }
        match listed.continuation_token {
            Some(token) => continuation = Some(token),
            None => return Ok(files),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crate_pins_files() {
        // The same release gives the same bytes, and names every file by its exact version.
        let realm_id = RealmId::from_bytes([1; 32]);
        let version = |seed: u8| {
            OriginVersion::Global(GlobalId::Version {
                realm_id,
                version_id: Ulid::from_bytes([seed; 16]),
            })
        };
        let spec = ReleaseSpec {
            label: "6.0".to_string(),
            prefix: "db/".to_string(),
            upstream: vec![Identifier {
                name: "DOI".to_string(),
                value: "10.5281/zenodo.1".to_string(),
                url: "https://doi.org/10.5281/zenodo.1".to_string(),
                target: IdentifierTarget::Key,
            }],
            preparation: vec![Ulid::from_bytes([9; 16])],
        };
        let files = vec![
            ReleaseFile {
                key: "db/a.fasta".to_string(),
                version: version(2),
                size: 4,
                content: Some(content_id([7; 32])),
                pin: None,
            },
            ReleaseFile {
                key: "db/b.tsv".to_string(),
                version: version(3),
                size: 9,
                content: None,
                pin: Some((
                    SourcePin {
                        url: Some("https://zenodo.org/records/1/files/b.tsv".to_string()),
                        path: "b.tsv".to_string(),
                        source_version: Some("3".to_string()),
                    },
                    Some("\"abc\"".to_string()),
                )),
            },
        ];
        let bytes = release_crate(&spec, &files);
        assert_eq!(bytes, release_crate(&spec, &files));
        let crate_json: Value = serde_json::from_slice(&bytes).unwrap();
        let pinned = pinned_ids(&crate_json);
        assert!(pinned.contains(&version(2).to_id()) && pinned.contains(&version(3).to_id()));
        assert_eq!(upstream_of(&crate_json), spec.upstream);
        let reference = graph(&crate_json)
            .find(|entity| entity["@id"] == version(3).to_id().as_str())
            .unwrap();
        assert_eq!(
            reference["isBasedOn"]["@id"],
            "https://zenodo.org/records/1/files/b.tsv"
        );
        let etag = format!("{}#source-etag", version(3).to_id());
        let property = graph(&crate_json)
            .find(|entity| entity["@id"] == etag.as_str())
            .unwrap();
        assert_eq!(property["value"], "\"abc\"");
        // The stored pin keeps its path and source version.
        for (term, value) in [("source-path", "b.tsv"), ("source-version", "3")] {
            let id = format!("{}#{term}", version(3).to_id());
            let entity = graph(&crate_json).find(|entity| entity["@id"] == id.as_str());
            assert_eq!(entity.unwrap()["value"], value);
        }
    }
}
