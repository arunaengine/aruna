//! Resolves what a reference preflight asks about: content identities as given, or the
//! versions a bucket operation would touch, each key checked for the caller's write permission.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::group_backend::MAX_KEY_LEN;

use super::super::head_versions::{HeadVersions, HeadVersionsError, HeadVersionsOperation};
use super::super::prefix_scan::{PrefixScanError, PrefixScanOperation, ScannedRows};
use super::super::preflight_types::{
    MetadataResolvedTarget, MetadataStorageOperation, ReferenceTarget,
};
use super::super::read_access::{child_results, children};
use super::super::readable::PathPermissionOperation;
use super::super::{
    ARUNA_DATA_PREFIX, BLOB_VERSIONS_KEYSPACE, BlobHeadKey, BlobVersion, Key, MetadataApiError,
    VersionKey, W3idIdentifier, bucket_permission_path, object_permission_path,
};
use super::{
    LONG_KEY_ROWS, MAX_TARGET_VERSIONS, ResolvedPreflightTargets, decode_version, version_targets,
};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

type Versions = Vec<(VersionKey, BlobVersion)>;

/// The content identities a caller named, each once.
fn content_targets(
    content_w3ids: Vec<String>,
    remove_resolvable_locations: bool,
) -> Result<ResolvedPreflightTargets, MetadataApiError> {
    if content_w3ids.is_empty() || content_w3ids.len() > MAX_TARGET_VERSIONS {
        return Err(MetadataApiError::BadRequest);
    }
    let mut targets = BTreeMap::new();
    for content_w3id in content_w3ids {
        let W3idIdentifier::ContentHash(content_hash) =
            W3idIdentifier::parse(&content_w3id).map_err(|_| MetadataApiError::BadRequest)?
        else {
            return Err(MetadataApiError::BadRequest);
        };
        targets
            .entry(content_hash)
            .or_insert_with(|| MetadataResolvedTarget {
                content_w3id: format!("{ARUNA_DATA_PREFIX}{}", hex::encode(content_hash)),
                content_hash,
                queried_iris: vec![content_w3id],
                targeted_versions: Vec::new(),
                removed_locations: Vec::new(),
                remove_resolvable_locations,
            });
    }
    Ok(ResolvedPreflightTargets {
        targets: targets.into_values().collect(),
        complete: true,
    })
}

/// One version row scan of a purge: a key length below `MAX_KEY_LEN`, or one lead byte of the
/// length varint of longer keys, whose rows must also match the prefix.
#[derive(Debug, PartialEq)]
enum Scan {
    Short(Vec<u8>),
    Long(Vec<u8>),
}

/// The scans of every version under `key_prefix` of `bucket`, also those without a head.
/// Version rows sort by key length first, so each key length below `MAX_KEY_LEN` is one range.
fn purge_scans(bucket: &str, key_prefix: Option<&str>) -> Result<VecDeque<Scan>, MetadataApiError> {
    let bucket_rows =
        VersionKey::bucket_prefix(bucket).map_err(|_| MetadataApiError::BadRequest)?;
    let Some(prefix) = key_prefix else {
        return Ok(VecDeque::from([Scan::Short(bucket_rows)]));
    };
    let mut scans = (prefix.len()..MAX_KEY_LEN)
        .map(|length| {
            let mut range = bucket_rows.clone();
            range.extend(postcard::to_allocvec(&length)?);
            range.extend(prefix.as_bytes());
            Ok(Scan::Short(range))
        })
        .collect::<Result<VecDeque<_>, postcard::Error>>()
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
    // Keys of `MAX_KEY_LEN` bytes and more have a length varint whose second byte is 8 or more.
    for lead in 0x80u8..=0xFF {
        let mut range = bucket_rows.clone();
        range.push(lead);
        scans.push_back(Scan::Long(range));
    }
    Ok(scans)
}

/// The versions one bucket operation would touch under a prefix: the current versions of at
/// most `MAX_TARGET_VERSIONS` keys, or for a purge at most that many versions, also those
/// without a head; `false` marks a cut scope.
#[derive(Debug, PartialEq)]
pub(in crate::metadata::api) struct ResolveVersionsOperation {
    bucket: String,
    prefix: Option<String>,
    operation: MetadataStorageOperation,
    scans: VecDeque<Scan>,
    long_budget: usize,
    versions: Versions,
}

type VersionTurn = Result<Next<(Versions, bool)>, MetadataApiError>;

impl ResolveVersionsOperation {
    pub(in crate::metadata::api) fn new(
        bucket: &str,
        prefix: Option<&str>,
        operation: MetadataStorageOperation,
    ) -> Self {
        Self {
            bucket: bucket.to_string(),
            prefix: prefix.map(str::to_string),
            operation,
            scans: VecDeque::new(),
            long_budget: LONG_KEY_ROWS,
            versions: Vec::new(),
        }
    }

    fn scan(&mut self) -> VersionTurn {
        let Some(scan) = self.scans.front() else {
            return Ok(Next::Done((std::mem::take(&mut self.versions), true)));
        };
        let (range, limit) = match scan {
            Scan::Short(range) => {
                let limit = MAX_TARGET_VERSIONS - self.versions.len();
                ((range.clone().into(), None), limit)
            }
            Scan::Long(range) => {
                let from: Key = [range.as_slice(), &[8]].concat().into();
                ((range.clone().into(), Some(from)), self.long_budget)
            }
        };
        let scan = PrefixScanOperation::new(BLOB_VERSIONS_KEYSPACE, range, limit);
        Ok(Next::Call(IoCall::child(scan)))
    }

    fn scanned(&mut self, (rows, more): ScannedRows) -> VersionTurn {
        let Some(scan) = self.scans.pop_front() else {
            return Err(MetadataApiError::Internal("purge scan lost".to_string()));
        };
        let long = matches!(scan, Scan::Long(_));
        if long {
            self.long_budget = self.long_budget.saturating_sub(rows.len());
        }
        let prefix = self.prefix.clone().unwrap_or_default();
        for (key, value) in rows {
            let found = decode_version(&key, &value)?;
            if !long {
                self.versions.push(found);
                continue;
            }
            if !found.0.key.starts_with(&prefix) {
                continue;
            }
            if self.versions.len() == MAX_TARGET_VERSIONS {
                return Ok(Next::Done((std::mem::take(&mut self.versions), false)));
            }
            self.versions.push(found);
        }
        if more {
            return Ok(Next::Done((std::mem::take(&mut self.versions), false)));
        }
        self.scan()
    }
}

impl Operation for ResolveVersionsOperation {
    type Output = (Versions, bool);
    type Error = MetadataApiError;

    fn first(&mut self) -> VersionTurn {
        match self.operation {
            MetadataStorageOperation::LatestVersionTombstone => {
                let prefix_key = match self.prefix.as_deref() {
                    Some(prefix) => BlobHeadKey::object_prefix(&self.bucket, prefix),
                    None => BlobHeadKey::bucket_prefix(&self.bucket),
                }
                .map_err(|_| MetadataApiError::BadRequest)?;
                let heads = HeadVersionsOperation::new(prefix_key.into(), MAX_TARGET_VERSIONS);
                Ok(Next::Call(IoCall::child(heads)))
            }
            MetadataStorageOperation::AllVersionsPurge => {
                self.scans = purge_scans(&self.bucket, self.prefix.as_deref())?;
                self.scan()
            }
        }
    }

    fn next(&mut self, result: IoResult) -> VersionTurn {
        match self.operation {
            MetadataStorageOperation::LatestVersionTombstone => {
                let heads = result.child::<HeadVersions, HeadVersionsError>()??;
                Ok(Next::Done(heads))
            }
            MetadataStorageOperation::AllVersionsPurge => {
                let scanned = result.child::<ScannedRows, PrefixScanError>()??;
                self.scanned(scanned)
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum TargetStage {
    Start,
    Bucket,
    Permitted,
    Versions,
    Keys(Versions, bool),
}

/// The targets of one preflight. A bucket target needs the caller's write permission on its
/// scope and on every key it resolves to; a cut scope is incomplete.
#[derive(Debug, PartialEq)]
pub(in crate::metadata::api) struct ResolveTargetsOperation {
    realm_id: RealmId,
    local_node: NodeId,
    auth: AuthContext,
    target: Option<ReferenceTarget>,
    s3_endpoint: Option<String>,
    bucket: String,
    prefix: Option<String>,
    operation: MetadataStorageOperation,
    group_id: Option<ulid::Ulid>,
    stage: TargetStage,
}

type TargetTurn = Result<Next<ResolvedPreflightTargets>, MetadataApiError>;

impl ResolveTargetsOperation {
    pub(in crate::metadata::api) fn new(
        (realm_id, local_node): (RealmId, NodeId),
        auth: AuthContext,
        target: ReferenceTarget,
        s3_endpoint: Option<String>,
    ) -> Self {
        Self {
            realm_id,
            local_node,
            auth,
            target: Some(target),
            s3_endpoint,
            bucket: String::new(),
            prefix: None,
            operation: MetadataStorageOperation::default(),
            group_id: None,
            stage: TargetStage::Start,
        }
    }

    fn permission(&self, group_id: ulid::Ulid, path: String) -> PathPermissionOperation {
        let scope = (self.realm_id, group_id);
        PathPermissionOperation::new(scope, self.auth.clone(), path, Permission::WRITE)
    }

    /// Checks the caller's write permission on every key the versions name, once each.
    fn keys(&mut self, (versions, complete): (Versions, bool)) -> TargetTurn {
        let group_id = (self.group_id)
            .ok_or_else(|| MetadataApiError::Internal("bucket unread".to_string()))?;
        let keys: BTreeSet<_> = (versions.iter())
            .map(|(version_key, _)| version_key.key.clone())
            .collect();
        let (realm_id, node) = (self.realm_id, self.local_node);
        let checks: Vec<_> = (keys.iter())
            .map(|key| {
                let path = object_permission_path(realm_id, group_id, node, &self.bucket, key);
                self.permission(group_id, path)
            })
            .collect();
        if checks.is_empty() {
            return Ok(Next::Done(self.resolved(versions, complete)));
        }
        self.stage = TargetStage::Keys(versions, complete);
        Ok(Next::Call(children(checks)))
    }

    fn resolved(&self, versions: Versions, complete: bool) -> ResolvedPreflightTargets {
        let endpoint = self.s3_endpoint.as_deref();
        let targets = version_targets(versions, self.operation, self.local_node, endpoint);
        ResolvedPreflightTargets { targets, complete }
    }
}

impl Operation for ResolveTargetsOperation {
    type Output = ResolvedPreflightTargets;
    type Error = MetadataApiError;

    fn first(&mut self) -> TargetTurn {
        let target = (self.target.take())
            .ok_or_else(|| MetadataApiError::Internal("targets already resolved".to_string()))?;
        match target {
            ReferenceTarget::ContentW3ids {
                content_w3ids,
                remove_resolvable_locations,
            } => {
                let targets = content_targets(content_w3ids, remove_resolvable_locations)?;
                Ok(Next::Done(targets))
            }
            ReferenceTarget::BucketPrefix {
                bucket,
                prefix,
                operation,
            } => {
                if bucket.trim().is_empty() {
                    return Err(MetadataApiError::BadRequest);
                }
                self.prefix = prefix.filter(|prefix| !prefix.is_empty());
                self.operation = operation;
                self.bucket = bucket;
                self.stage = TargetStage::Bucket;
                let read = GetBucketOperation::new(self.bucket.clone());
                Ok(Next::Call(IoCall::child(read)))
            }
        }
    }

    fn next(&mut self, result: IoResult) -> TargetTurn {
        match std::mem::replace(&mut self.stage, TargetStage::Start) {
            TargetStage::Start => Err(MetadataApiError::Internal(
                "targets not started".to_string(),
            )),
            TargetStage::Bucket => {
                let info =
                    (result.child::<BucketInfo, GetBucketError>()?).map_err(
                        |error| match error {
                            GetBucketError::NotFound => MetadataApiError::NotFound,
                            _ => MetadataApiError::ServiceUnavailable,
                        },
                    )?;
                let (realm_id, group_id, node) = (self.realm_id, info.group_id, self.local_node);
                let bucket = &self.bucket;
                let path = match self.prefix.as_deref() {
                    Some(prefix) => {
                        object_permission_path(realm_id, group_id, node, bucket, prefix)
                    }
                    None => bucket_permission_path(realm_id, group_id, node, bucket),
                };
                let check = self.permission(group_id, path);
                self.group_id = Some(group_id);
                self.stage = TargetStage::Permitted;
                Ok(Next::Call(IoCall::child(check)))
            }
            TargetStage::Permitted => {
                result.child::<(), MetadataApiError>()??;
                let prefix = self.prefix.as_deref();
                let versions = ResolveVersionsOperation::new(&self.bucket, prefix, self.operation);
                self.stage = TargetStage::Versions;
                Ok(Next::Call(IoCall::child(versions)))
            }
            TargetStage::Versions => {
                let versions = result.child::<(Versions, bool), MetadataApiError>()??;
                self.keys(versions)
            }
            TargetStage::Keys(versions, complete) => {
                let checked = child_results::<(), MetadataApiError>(result)?;
                for check in checked {
                    check.ok_or(MetadataApiError::ServiceUnavailable)??;
                }
                Ok(Next::Done(self.resolved(versions, complete)))
            }
        }
    }
}

#[cfg(test)]
#[path = "targets_tests.rs"]
mod call_tests;
