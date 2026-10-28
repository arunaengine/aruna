//! Publishes database releases: lists the current files under a prefix at each range's
//! coordinator, authorizes every file for the caller, and writes the tagged release crate as
//! one guarded version. Also answers other members and runs the release effects.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use aruna_core::NodeId;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::BUCKET_RELEASE_KEYSPACE;
use aruna_core::stream::BackendStream;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::identifier::Identifier;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::document::group_admin_path;
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::structs::storage::bucket_metadata::{
    RELEASE_PREFIX_TAG, RELEASE_TAG, ReleasePointer, curator_path, release_prefix,
};
use aruna_core::structs::storage::data::identity::content_id;
use aruna_core::structs::storage::distribution::KeyRange;
use aruna_core::structs::storage::origin::SourcePin;
use aruna_core::structs::storage::release::{
    CrateBytes, CrateRead, CrateReadError, HolderAnswer, HolderAsk, HolderRequest, RangeFiles,
    RangeMembers, ReleaseEffect, ReleaseEvent, ReleaseFile,
};
use bytes::Bytes;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use ulid::Ulid;

use super::release::{
    Access, BucketScope, MembersConfig, ReleaseError, ReleaseMembersOperation, load_access,
};
use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::distribution::guard::local_fence;
use crate::distribution::status::read_view;
use crate::driver::{DriverContext, bucket_snapshot, drive, gate_context, now_ms};
use crate::metadata::protocol::{AuthToken, MetadataReadError};
use crate::realm::get_config::GetConfigOperation;
use crate::replication::queue::complete_put;
use crate::s3::bucket::get::GetBucketOperation;
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectOperation};
use crate::s3::object::list::{ListBucketInput, ListBucketOperation};
use crate::s3::object::origin::{OriginError, read_origin};
use crate::s3::object::put::{PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation};

#[derive(Debug, Error, PartialEq)]
pub enum PublishError {
    #[error("the caller may not publish this release: {0}")]
    Forbidden(&'static str),
    #[error("{0}")]
    Unavailable(String),
    #[error("{0}")]
    Conflict(String),
}

impl From<ReleaseError> for PublishError {
    fn from(error: ReleaseError) -> Self {
        Self::Unavailable(error.to_string())
    }
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

const UNREADABLE: &str = "a file under the prefix is not readable";

/// Keys listed for one page of release files, so a holder's reply stays far below the transport
/// ceiling.
const FILE_PAGE: usize = 256;

/// Every current file under `prefix` in `range`, page by page.
async fn all_files(
    context: &DriverContext,
    scope: BucketScope<'_>,
    access: &Access,
    asked: (&str, &str),
    range: Option<&KeyRange>,
) -> Result<Vec<ReleaseFile>, PublishError> {
    let (mut files, mut after) = (Vec::new(), None);
    loop {
        let (page, next) = release_files(context, scope, access, asked, (range, after)).await?;
        files.extend(page);
        match next {
            Some(next) => after = Some(next),
            None => return Ok(files),
        }
    }
}

/// One page of the current versions under `prefix` in `range` (the whole bucket without one)
/// after the key `after`, `skip` left out, and the key the next page starts after. Every field
/// of a file comes from one exact version, and the caller must be able to read every file, or
/// nothing is published.
pub async fn release_files(
    context: &DriverContext,
    scope: BucketScope<'_>,
    access: &Access,
    (prefix, skip): (&str, &str),
    (range, after): (Option<&KeyRange>, Option<String>),
) -> Result<(Vec<ReleaseFile>, Option<String>), PublishError> {
    let unavailable = |error: &dyn std::fmt::Display| PublishError::Unavailable(error.to_string());
    let mut files = Vec::new();
    let (mut continuation, mut after) = (None, after);
    loop {
        let listed = drive(
            ListBucketOperation::new(ListBucketInput {
                bucket: scope.bucket.to_string(),
                group_id: scope.info.group_id,
                continuation_token: continuation.take(),
                max_keys: Some(FILE_PAGE),
                prefix: Some(prefix.to_string()),
                delimiter: None,
                start_after: after.take(),
            }),
            context,
        )
        .await
        .map_err(|error| unavailable(&error))?;
        let last = (listed.objects.last()).map(|object| object.head.key.clone());
        for object in listed.objects {
            let key = object.head.key;
            if key == skip || range.is_some_and(|range| !range.contains(&key)) {
                continue;
            }
            if !access.0.allows(Some(&key), &Permission::READ) {
                return Err(PublishError::Forbidden(UNREADABLE));
            }
            let place = (scope.realm_id, scope.node_id);
            let origin = read_origin(context, place, scope.bucket, &key, None)
                .await
                .map_err(|error| unavailable(&error))?;
            let Some(version) = scope.exact(&key, origin.version_id) else {
                continue;
            };
            // Every field comes from the one version the origin read pinned.
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
                size: origin.size.unwrap_or_default(),
                content: origin.content_hash.map(content_id),
                pin,
            });
        }
        // A page with no key goes on to the next; otherwise its last key is the position.
        match (listed.continuation_token, last) {
            (Some(token), None) => continuation = Some(token),
            (Some(_), last) => return Ok((files, last)),
            (None, _) => return Ok((files, None)),
        }
    }
}

/// One release to publish.
#[derive(Clone, Debug)]
pub struct PublishInput {
    pub auth: AuthContext,
    /// Asks the coordinators of other ranges; required when the prefix spans them.
    pub bearer: Option<String>,
    pub key: String,
    pub spec: ReleaseSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Published {
    pub version_id: Ulid,
    pub files: usize,
    pub replayed: bool,
}

/// Publishes a release on the node that writes `key`: the caller needs bucket WRITE and admin
/// or curator rights, may read every pinned file, and gets the existing version back when the
/// identical release is already the key's head.
pub async fn publish_release(
    context: &DriverContext,
    scope: BucketScope<'_>,
    input: PublishInput,
) -> Result<Published, PublishError> {
    let realm_id = scope.realm_id;
    let bucket_path =
        bucket_permission_path(realm_id, scope.info.group_id, scope.node_id, scope.bucket);
    allowed(context, realm_id, &input.auth, &bucket_path)
        .await?
        .then_some(())
        .ok_or(PublishError::Forbidden("no WRITE on the bucket"))?;
    let admin = group_admin_path(realm_id, scope.info.group_id);
    let curator = curator_path(realm_id);
    if !allowed(context, realm_id, &input.auth, &admin).await?
        && !allowed(context, realm_id, &input.auth, &curator).await?
    {
        return Err(PublishError::Forbidden("neither group admin nor curator"));
    }
    let access = load_access(context, scope, &input.auth).await?;
    let files = coordinated_files(context, scope, &access, &input).await?;
    let bytes = release_crate(&input.spec, &files);
    let hash = *blake3::hash(&bytes).as_bytes();
    // A concurrent publish moves the head; the next pass then finds its release or conflicts.
    for _ in 0..2 {
        let place = (realm_id, scope.node_id);
        let head = match read_origin(context, place, scope.bucket, &input.key, None).await {
            Ok(current) if current.content_hash == Some(hash) => {
                let rows = release_rows(context, scope.bucket).await?;
                let same = rows.iter().any(|(version_id, pointer)| {
                    *version_id == current.version_id
                        && pointer.label == input.spec.label
                        && pointer.prefix == input.spec.prefix
                });
                if same {
                    return Ok(Published {
                        version_id: current.version_id,
                        files: files.len(),
                        replayed: true,
                    });
                }
                Some(current.version_id)
            }
            Ok(current) => Some(current.version_id),
            Err(OriginError::VersionMissing) => None,
            Err(error) => return Err(PublishError::Unavailable(error.to_string())),
        };
        if let Some(version_id) = put_crate(context, scope, &input, (bytes.clone(), head)).await? {
            return Ok(Published {
                version_id,
                files: files.len(),
                replayed: false,
            });
        }
    }
    Err(PublishError::Conflict(
        "the release key changed while the release was written; publish again".to_string(),
    ))
}

/// Rows read per storage page while looking for a bucket's releases.
const ROW_PAGE: usize = 256;

/// Every release row of `bucket` this node holds, in key order.
pub(crate) async fn release_rows(
    context: &DriverContext,
    bucket: &str,
) -> Result<Vec<(Ulid, ReleasePointer)>, PublishError> {
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
            other => return Err(PublishError::Unavailable(format!("{other:?}"))),
        };
        for (key, value) in values {
            let Some(bytes) = key.get(prefix.len()..) else {
                continue;
            };
            let Ok(id) = <[u8; 16]>::try_from(bytes) else {
                continue;
            };
            let pointer = ReleasePointer::from_bytes(value.as_ref())
                .map_err(|error| PublishError::Unavailable(error.to_string()))?;
            rows.push((Ulid::from_bytes(id), pointer));
        }
        match next {
            Some(next) => start = Some(next),
            None => return Ok(rows),
        }
    }
}

async fn allowed(
    context: &DriverContext,
    realm_id: RealmId,
    auth: &AuthContext,
    path: &str,
) -> Result<bool, PublishError> {
    let extras = PolicyRequestExtras::rest();
    match authorize(context, realm_id, auth, path, &Permission::WRITE, extras).await {
        Ok(()) => Ok(true),
        Err(AuthorizeError::Storage(error)) => Err(PublishError::Unavailable(error.to_string())),
        Err(_) => Ok(false),
    }
}

/// The current files under the prefix, each range listed by its own coordinator; a range no
/// coordinator lists now refuses the publish, since a copy may be behind.
async fn coordinated_files(
    context: &DriverContext,
    scope: BucketScope<'_>,
    access: &Access,
    input: &PublishInput,
) -> Result<Vec<ReleaseFile>, PublishError> {
    let asked = (input.spec.prefix.as_str(), input.key.as_str());
    let Some(binding) = &scope.info.distribution else {
        return all_files(context, scope, access, asked, None).await;
    };
    let unavailable = |error: &dyn std::fmt::Display| PublishError::Unavailable(error.to_string());
    let view = read_view(context, binding.set_id)
        .await
        .map_err(|error| unavailable(&error))?
        .ok_or_else(|| PublishError::Unavailable("this node holds no record of the set".into()))?;
    let fence = local_fence(context, binding.set_id)
        .await
        .map_err(|error| unavailable(&error))?;
    let snapshot = view.snapshot();
    let mut files = Vec::new();
    for range in snapshot.listed_ranges(&input.spec.prefix) {
        if snapshot.lists_locally(binding.member_id, range, &fence) {
            files.extend(all_files(context, scope, access, asked, Some(range)).await?);
            continue;
        }
        let refused = || {
            PublishError::Unavailable(format!(
                "range {} could not be listed by its coordinator",
                range.range_id
            ))
        };
        let (Some(coordinator), Some(bearer), Some(handle)) = (
            snapshot.list_holder(range),
            input.bearer.as_ref(),
            context.metadata_handle.as_ref(),
        ) else {
            return Err(refused());
        };
        let token = AuthToken::bearer(bearer.clone()).map_err(|error| unavailable(&error))?;
        let mut after = None;
        loop {
            let ask = HolderAsk::Files {
                range_id: range.range_id,
                prefix: input.spec.prefix.clone(),
                skip: input.key.clone(),
                after: after.take(),
            };
            let bucket = scope.bucket.to_string();
            match handle
                .request_release_holder(coordinator.node_id, Some(token.clone()), bucket, ask)
                .await
            {
                Ok(HolderAnswer::Files(listed)) if !listed.refused => {
                    files.extend(listed.files);
                    match listed.next {
                        Some(next) => after = Some(next),
                        None => break,
                    }
                }
                // The coordinator refused a file; the generic refusal names none.
                Err(MetadataReadError::Forbidden) => {
                    return Err(PublishError::Forbidden(UNREADABLE));
                }
                _ => return Err(refused()),
            }
        }
    }
    files.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(files)
}

/// Writes the release crate as one version tagged with its label and prefix; `None` when the
/// key's head is no longer `head`, so nothing was written.
async fn put_crate(
    context: &DriverContext,
    scope: BucketScope<'_>,
    input: &PublishInput,
    (bytes, head): (Vec<u8>, Option<Ulid>),
) -> Result<Option<Ulid>, PublishError> {
    let failed = |error: &dyn std::fmt::Display| PublishError::Unavailable(error.to_string());
    let realm = drive(GetConfigOperation::new(scope.realm_id), context)
        .await
        .map_err(|error| failed(&error))?;
    let routing = bucket_snapshot(context, scope.info)
        .await
        .map_err(|error| failed(&error))?;
    let gate = gate_context(context, scope.realm_id, now_ms())
        .await
        .map_err(|error| failed(&error))?;
    let size = bytes.len() as u64;
    let body = futures_util::stream::iter([Ok::<Bytes, std::io::Error>(Bytes::from(bytes))]);
    let tags = HashMap::from([
        (RELEASE_TAG.to_string(), input.spec.label.clone()),
        (RELEASE_PREFIX_TAG.to_string(), input.spec.prefix.clone()),
        (
            aruna_core::structs::storage::blob::CONTENT_TYPE_KEY.to_string(),
            "application/ld+json".to_string(),
        ),
    ]);
    let mut operation = PutObjectOperation::new(PutObjectConfig {
        user_id: input.auth.user_id,
        group_id: scope.info.group_id,
        realm_id: scope.realm_id,
        node_id: scope.node_id,
        request: PutObjectInput {
            bucket: scope.bucket.to_string(),
            key: input.key.clone(),
            content_length: Some(size),
            body: Some(BackendStream::new(body)),
        },
        expected_checksums: Vec::new(),
        checksum_type: None,
        exists: false,
        version_source: None,
        preassigned_version_id: None,
        quota_ceiling: realm.quota.effective_group_ceiling(&scope.info.group_id),
        routing,
    })
    .with_metadata(tags)
    .with_expected_head(head)
    .with_restrictions(input.auth.path_restrictions.clone());
    if let Some(gate) = gate {
        operation = operation.with_gate(gate);
    }
    let result = match drive(operation, context).await {
        Ok(result) => result,
        Err(PutObjectError::HeadChanged) => return Ok(None),
        Err(error) => return Err(failed(&error)),
    };
    complete_put(
        context,
        scope.realm_id,
        scope.node_id,
        input.auth.clone(),
        scope.info.group_id,
        scope.bucket.to_string(),
        input.key.clone(),
        result.version_id,
        result.location.blob_size,
    )
    .await;
    Ok(Some(result.version_id))
}

/// A holder's answer for its ranges, with the caller context it validated from the request's
/// bearer itself. It answers only for ranges it coordinates now.
pub async fn holder_answer(
    context: &DriverContext,
    (realm_id, node_id): (RealmId, NodeId),
    auth: &AuthContext,
    (bucket, ask): (&str, HolderAsk),
) -> Result<HolderAnswer, PublishError> {
    let unavailable = |error: &dyn std::fmt::Display| PublishError::Unavailable(error.to_string());
    let info = drive(GetBucketOperation::new(bucket.to_string()), context)
        .await
        .map_err(|error| unavailable(&error))?;
    let scope = BucketScope {
        realm_id,
        node_id,
        bucket,
        info: &info,
    };
    let access = load_access(context, scope, auth).await?;
    match ask {
        HolderAsk::Members { range_ids, queries } => {
            let config = MembersConfig {
                realm_id,
                node_id,
                bucket: bucket.to_string(),
                info: info.clone(),
                auth: auth.clone(),
                access,
                bearer: None,
                queries,
                ranges: Some(range_ids),
            };
            let output = drive(ReleaseMembersOperation::new(config), context).await?;
            Ok(HolderAnswer::Members(RangeMembers {
                answers: output.answers,
                releases: output.releases,
                complete: output.complete,
                refused: output.refused,
            }))
        }
        HolderAsk::Files {
            range_id,
            prefix,
            skip,
            after,
        } => {
            let refused = RangeFiles {
                files: Vec::new(),
                refused: true,
                next: None,
            };
            let Some(binding) = &info.distribution else {
                return Ok(HolderAnswer::Files(refused));
            };
            let view = read_view(context, binding.set_id)
                .await
                .map_err(|error| unavailable(&error))?
                .ok_or_else(|| PublishError::Unavailable("no record of the set".into()))?;
            let fence = local_fence(context, binding.set_id)
                .await
                .map_err(|error| unavailable(&error))?;
            let snapshot = view.snapshot();
            let range = (snapshot.config.ranges.iter()).find(|range| range.range_id == range_id);
            let Some(range) =
                range.filter(|range| snapshot.lists_locally(binding.member_id, range, &fence))
            else {
                return Ok(HolderAnswer::Files(refused));
            };
            let asked = (prefix.as_str(), skip.as_str());
            let page = (Some(range), after);
            let (files, next) = release_files(context, scope, &access, asked, page).await?;
            Ok(HolderAnswer::Files(RangeFiles {
                files,
                refused: false,
                next,
            }))
        }
        HolderAsk::Crate { key, version_id } => {
            if !access.0.allows(Some(&key), &Permission::READ) {
                return Err(PublishError::Forbidden("the release crate is not readable"));
            }
            let read = CrateRead {
                bucket: bucket.to_string(),
                key,
                version_id,
                group_id: info.group_id,
                auth: auth.clone(),
                realm_id,
                node_id,
            };
            match read_crate(context, read).await {
                Ok(found) if found.metadata.contains_key(RELEASE_TAG) => {
                    Ok(HolderAnswer::Crate(Some(found.bytes)))
                }
                Ok(_) | Err(CrateReadError::Missing) => Ok(HolderAnswer::Crate(None)),
                Err(CrateReadError::Failed(reason)) => Err(PublishError::Unavailable(reason)),
            }
        }
    }
}

/// Runs one release effect: a crate read with the caller's identity, or a holder request.
pub async fn dispatch_release(context: &DriverContext, effect: ReleaseEffect) -> ReleaseEvent {
    match effect {
        ReleaseEffect::ReadCrate(read) => ReleaseEvent::CrateRead(read_crate(context, *read).await),
        ReleaseEffect::Holder(request) => ReleaseEvent::Holder(ask_holder(context, request).await),
    }
}

/// Reads one release crate version on this node with the caller's identity.
pub async fn read_crate(
    context: &DriverContext,
    read: CrateRead,
) -> Result<CrateBytes, CrateReadError> {
    let operation = GetObjectOperation::new(GetObjectInput {
        bucket: read.bucket,
        key: read.key,
        version_id: Some(read.version_id),
        range: None,
        group_id: read.group_id,
        user_identity: read.auth.user_id,
        realm_id: read.realm_id,
        node_id: read.node_id,
    })
    .with_restrictions(read.auth.path_restrictions.clone());
    let result = match drive(operation, context).await {
        Ok(result) => result,
        Err(
            GetObjectError::NoSuchKey
            | GetObjectError::NoSuchVersion
            | GetObjectError::DeleteMarker,
        ) => return Err(CrateReadError::Missing),
        Err(error) => return Err(CrateReadError::Failed(error.to_string())),
    };
    let mut bytes = Vec::new();
    let mut blob = result.blob;
    while let Some(chunk) = blob.next().await {
        let chunk = chunk.map_err(|error| CrateReadError::Failed(error.to_string()))?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(CrateBytes {
        metadata: result.metadata,
        bytes,
    })
}

async fn ask_holder(
    context: &DriverContext,
    request: HolderRequest,
) -> Result<HolderAnswer, String> {
    let handle = context
        .metadata_handle
        .as_ref()
        .ok_or("no metadata handle")?;
    let token = AuthToken::bearer(request.bearer).map_err(|error| format!("{error:?}"))?;
    handle
        .request_release_holder(request.node_id, Some(token), request.bucket, request.ask)
        .await
        .map_err(|error| format!("{error:?}"))
}

#[cfg(test)]
mod tests {
    use super::super::release::{pinned_ids, upstream_of};
    use super::*;
    use aruna_core::structs::identity::identifier::IdentifierTarget;
    use aruna_core::structs::storage::global_id::GlobalId;
    use aruna_core::structs::storage::origin::OriginVersion;

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
        let graph = |crate_json: &Value| -> Vec<Value> {
            crate_json["@graph"].as_array().cloned().unwrap_or_default()
        };
        let pinned = pinned_ids(&crate_json);
        assert!(pinned.contains(&version(2).to_id()) && pinned.contains(&version(3).to_id()));
        assert_eq!(upstream_of(&crate_json), spec.upstream);
        let reference = graph(&crate_json)
            .into_iter()
            .find(|entity| entity["@id"] == version(3).to_id().as_str())
            .unwrap();
        assert_eq!(
            reference["isBasedOn"]["@id"],
            "https://zenodo.org/records/1/files/b.tsv"
        );
        let etag = format!("{}#source-etag", version(3).to_id());
        let property = graph(&crate_json)
            .into_iter()
            .find(|entity| entity["@id"] == etag.as_str())
            .unwrap();
        assert_eq!(property["value"], "\"abc\"");
        // The stored pin keeps its path and source version.
        for (term, value) in [("source-path", "b.tsv"), ("source-version", "3")] {
            let id = format!("{}#{term}", version(3).to_id());
            let entity = graph(&crate_json)
                .into_iter()
                .find(|entity| entity["@id"] == id.as_str());
            assert_eq!(entity.unwrap()["value"], value);
        }
    }
}
