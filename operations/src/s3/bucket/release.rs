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
use aruna_core::structs::storage::distribution::KeyRange;
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::origin::OriginVersion;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use ulid::Ulid;

use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::distribution::status::read_view;
use crate::driver::{DriverContext, drive};
use crate::metadata::protocol::AuthToken;
use crate::s3::bucket::get::GetBucketOperation;
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectOperation};

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
