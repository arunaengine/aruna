//! Finds the exact stored objects the file entities of a metadata graph name.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{LinkedObject, StoredObject};
use aruna_core::operation::Operation;
use aruna_core::repo_layout::{data_path, entity_path, is_file, path_id};
use aruna_core::structs::checksum::{HASH_BLAKE3, HASH_SHA256};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::{NodeRealm, RealmId};
use aruna_core::structs::storage::blob::{BucketInfo, HashIndex, object_permission_path};
use aruna_core::structs::storage::data::identity::{DataIdentity, text_values};
use aruna_core::structs::storage::dataset_location::DatasetLocation;
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::replication::VersionedObjectArn;

use super::calls::{child_member, group_call, group_results, member_outcome};
use super::{GitError, download, location, objects};
use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::blob::permission_paths::{ResolvePathsError, ResolvePathsOperation};
use crate::metadata::global_entries::{LocalVersionError, LocalVersionOperation};
use crate::replication::bao_read::{BaoReadError, BaoReadOutput};
use crate::replication::managed_read::ManagedReadOperation;
use crate::replication::protocol::{BaoReadRequest, BaoReadTarget};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::get::{GetObjectError, GetObjectInput, ObjectInfo};
use crate::s3::object::read::ObjectInfoOperation;

type Document = MetadataRegistryRecord;

/// Lookups that run at the same time for one graph.
const LOOKUPS: usize = 8;
/// More file entities than this are left out of a repository.
const MAX_FILES: usize = 10_000;
/// Candidates tried per source; a few are enough to find a readable one.
const MAX_CANDIDATES: usize = 4;

/// What the pending call of resolving an exact version answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExactStep {
    Bucket,
    Authorize,
    Info,
    Remote,
}

/// Size and digests of an exact object version, read here or from its node.
fn resolve_exact(
    node: Option<NodeId>,
    exact: &VersionedObjectArn,
    auth: &AuthContext,
) -> ResolveExactOperation {
    ResolveExactOperation {
        node,
        exact: exact.clone(),
        auth: auth.clone(),
        step: ExactStep::Bucket,
        bucket: None,
    }
}

#[derive(Debug, PartialEq)]
struct ResolveExactOperation {
    node: Option<NodeId>,
    exact: VersionedObjectArn,
    auth: AuthContext,
    step: ExactStep,
    bucket: Option<BucketInfo>,
}

type ExactTurn = Result<Next<StoredObject>, GitError>;

impl ResolveExactOperation {
    fn bucket(&self) -> Result<&BucketInfo, IoError> {
        self.bucket.as_ref().ok_or(IoError::Unfinished)
    }

    fn finish(&self, size: u64, hashes: std::collections::HashMap<String, Vec<u8>>) -> ExactTurn {
        let exact = &self.exact;
        let bucket = self.bucket.as_ref();
        let global = bucket
            .and_then(|bucket| bucket.distribution.as_ref())
            .map(|_| GlobalId::Version {
                realm_id: exact.realm_id,
                version_id: exact.version,
            });
        let blake3 = (hashes.get(HASH_BLAKE3))
            .and_then(|hash| <[u8; 32]>::try_from(hash.as_slice()).ok())
            .ok_or(GitError::Unavailable)?;
        Ok(Next::Done(StoredObject {
            node_id: exact.node_id,
            group_id: bucket.map(|bucket| bucket.group_id),
            bucket: exact.bucket.clone(),
            key: exact.key.clone(),
            version_id: exact.version,
            size,
            sha256: (hashes.get(HASH_SHA256))
                .map(hex::encode)
                .ok_or(GitError::Unavailable)?,
            blake3,
            global,
        }))
    }
}

impl Operation for ResolveExactOperation {
    type Output = StoredObject;
    type Error = GitError;

    fn first(&mut self) -> ExactTurn {
        let node = self.node.ok_or(GitError::Unavailable)?;
        if self.exact.node_id == node {
            let bucket = GetBucketOperation::new(self.exact.bucket.clone());
            return Ok(Next::Call(IoCall::child(bucket)));
        }
        self.step = ExactStep::Remote;
        let request = BaoReadRequest {
            auth_context: self.auth.clone(),
            realm_id: self.exact.realm_id,
            target: BaoReadTarget::ExactVersion(self.exact.clone()),
            expected_blake3: None,
            metadata_only: true,
            destination: None,
            known_refs: Vec::new(),
            range: None,
        };
        let read = ManagedReadOperation::new(self.exact.node_id, request);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> ExactTurn {
        let node = self.node.ok_or(GitError::Unavailable)?;
        let exact = &self.exact;
        match self.step {
            ExactStep::Bucket => {
                let bucket = result.child::<BucketInfo, GetBucketError>()?;
                let bucket = bucket.map_err(|_| GitError::NotFound)?;
                // Size and digests of a local object are shown only to users who may read it.
                let path = object_permission_path(
                    exact.realm_id,
                    bucket.group_id,
                    node,
                    &exact.bucket,
                    &exact.key,
                );
                let check = AuthorizeOperation::new(
                    exact.realm_id,
                    &self.auth,
                    &path,
                    &Permission::READ,
                    PolicyRequestExtras::rest(),
                );
                self.bucket = Some(bucket);
                self.step = ExactStep::Authorize;
                Ok(Next::Call(IoCall::child(check)))
            }
            ExactStep::Authorize => {
                let allowed = result.child::<(), AuthorizeError>()?;
                allowed.map_err(|_| GitError::NotFound)?;
                let input = GetObjectInput {
                    bucket: exact.bucket.clone(),
                    key: exact.key.clone(),
                    version_id: Some(exact.version),
                    range: None,
                    group_id: self.bucket()?.group_id,
                    user_identity: self.auth.user_id,
                    realm_id: self.auth.realm_id,
                    node_id: node,
                };
                self.step = ExactStep::Info;
                let info = ObjectInfoOperation::new(input, None);
                Ok(Next::Call(IoCall::child(info)))
            }
            ExactStep::Info => {
                let info = result.child::<ObjectInfo, GetObjectError>()?;
                let info = info.map_err(|_| GitError::NotFound)?;
                self.finish(info.size, info.hashes)
            }
            ExactStep::Remote => match result.child::<BaoReadOutput, BaoReadError>()? {
                Ok(BaoReadOutput::Metadata { size, hashes, .. }) => self.finish(size, hashes),
                _ => Err(GitError::NotFound),
            },
        }
    }
}

/// File entities of `entities` that name an exact object version of this realm.
fn exact_files<'a>(
    entities: impl Iterator<Item = &'a serde_json::Value>,
    realm_id: RealmId,
) -> Vec<(String, VersionedObjectArn)> {
    let mut files = Vec::new();
    for entity in entities {
        let listed = |value: &serde_json::Value| -> Vec<String> {
            let values = value
                .as_array()
                .cloned()
                .unwrap_or_else(|| vec![value.clone()]);
            values
                .iter()
                .filter_map(|item| item.as_str().or_else(|| item["@id"].as_str()))
                .map(str::to_owned)
                .collect()
        };
        let Some(id) = entity["@id"].as_str() else {
            continue;
        };
        // RO-Crate `File` is an alias of schema.org MediaObject, which exports may spell out.
        let file = |kind: &String| {
            let local = kind
                .trim_start_matches("http://schema.org/")
                .trim_start_matches("https://schema.org/");
            matches!(local, "File" | "MediaObject")
        };
        if !listed(&entity["@type"]).iter().any(file) {
            continue;
        }
        let urls = listed(&entity["contentUrl"]);
        let Some(exact) = DataIdentity::read(id, &urls).exact else {
            continue;
        };
        if exact.realm_id == realm_id {
            files.push((id.to_string(), exact));
        }
        if files.len() >= 10_000 {
            break;
        }
    }
    files
}

/// What the pending call of locating a data entity answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocateStep {
    Version,
    Resolve,
    Bucket,
    Object,
    Paths,
}

/// The exact version a data entity names: its versioned ARN, the current version at its
/// `s3://` location on this node when that holds its content, or a copy of its content.
fn located(ids: NodeRealm, document: &Document, identity: &DataIdentity) -> LocatedOperation {
    LocatedOperation {
        ids,
        realm_id: document.realm_id,
        identity: identity.clone(),
        step: LocateStep::Version,
        found: Vec::new(),
    }
}

#[derive(Debug, PartialEq)]
struct LocatedOperation {
    ids: NodeRealm,
    realm_id: RealmId,
    identity: DataIdentity,
    step: LocateStep,
    found: Vec<VersionedObjectArn>,
}

type LocateTurn = Result<Next<Vec<VersionedObjectArn>>, GitError>;

impl LocatedOperation {
    fn node(&self) -> Option<NodeId> {
        self.ids.map(|(node_id, _)| node_id)
    }

    /// The version of this realm a global id names, if this node can look it up.
    fn global_version(&self) -> Option<ulid::Ulid> {
        self.node()?;
        let global = self.identity.global.as_ref();
        match global.filter(|global| global.realm_id() == self.realm_id)? {
            GlobalId::Version { version_id, .. } => Some(*version_id),
            _ => None,
        }
    }

    fn check_location(&mut self) -> LocateTurn {
        let Some(location) = self
            .identity
            .location
            .as_ref()
            .filter(|_| self.node().is_some())
        else {
            return self.check_hash();
        };
        let bucket = GetBucketOperation::new(location.bucket.clone());
        self.step = LocateStep::Bucket;
        Ok(Next::Call(IoCall::child(bucket)))
    }

    fn check_hash(&mut self) -> LocateTurn {
        let realm_id = self.realm_id;
        let identity = &self.identity;
        let same_realm = identity.hash_realm.is_none_or(|realm| realm == realm_id);
        match identity.hash.filter(|_| same_realm) {
            Some(hash) => {
                self.step = LocateStep::Paths;
                Ok(Next::Call(IoCall::child(ResolvePathsOperation::new(hash))))
            }
            None => Ok(Next::Done(std::mem::take(&mut self.found))),
        }
    }
}

impl Operation for LocatedOperation {
    type Output = Vec<VersionedObjectArn>;
    type Error = GitError;

    fn first(&mut self) -> LocateTurn {
        let exact = self.identity.exact.as_ref();
        if let Some(exact) = exact.filter(|exact| exact.realm_id == self.realm_id) {
            return Ok(Next::Done(vec![exact.clone()]));
        }
        // A global id resolves on a member of its set, elsewhere through the resolver's holders.
        match self.global_version() {
            Some(version_id) => {
                let local = LocalVersionOperation::new(version_id);
                Ok(Next::Call(IoCall::child(local)))
            }
            None => self.check_location(),
        }
    }

    fn next(&mut self, result: IoResult) -> LocateTurn {
        let realm_id = self.realm_id;
        match self.step {
            LocateStep::Version => {
                let version_id = self.global_version().ok_or(IoError::Unfinished)?;
                let node = self.node().ok_or(IoError::Unfinished)?;
                match result.child::<Option<(String, String)>, LocalVersionError>()? {
                    Ok(Some((bucket, key))) => {
                        let exact =
                            VersionedObjectArn::new(realm_id, node, bucket, key, version_id);
                        self.found.extend(exact.ok());
                        self.check_location()
                    }
                    _ => {
                        self.step = LocateStep::Resolve;
                        Ok(Next::Call(download::version_call(self.ids, version_id)))
                    }
                }
            }
            LocateStep::Resolve => {
                let version_id = self.global_version().ok_or(IoError::Unfinished)?;
                if let Some(resolved) = download::version_answer(result)? {
                    let holders = resolved.holders.iter().take(MAX_CANDIDATES);
                    self.found.extend(holders.filter_map(|holder| {
                        let (bucket, key) = (&resolved.bucket, &resolved.key);
                        VersionedObjectArn::new(realm_id, *holder, bucket, key, version_id).ok()
                    }));
                }
                self.check_location()
            }
            LocateStep::Bucket => match result.child::<BucketInfo, GetBucketError>()? {
                Ok(bucket) => {
                    let location = self.identity.location.as_ref();
                    let location = location.ok_or(IoError::Unfinished)?;
                    let scope = (location.bucket.as_str(), bucket.group_id);
                    let read = objects::described(self.node(), scope, &location.key, None);
                    self.step = LocateStep::Object;
                    Ok(Next::Call(IoCall::child(read)))
                }
                Err(_) => self.check_hash(),
            },
            LocateStep::Object => {
                let wanted = self.identity.hash;
                let object = result.child::<Option<StoredObject>, GitError>()?;
                let object = object.ok().flatten();
                if let (Some(object), Some(node)) = (object, self.node())
                    && wanted.is_none_or(|hash| hash == object.blake3)
                {
                    let (bucket, key) = (&object.bucket, &object.key);
                    let exact =
                        VersionedObjectArn::new(realm_id, node, bucket, key, object.version_id);
                    self.found.extend(exact.ok());
                }
                self.check_hash()
            }
            LocateStep::Paths => {
                if let Ok(aliases) = result.child::<Vec<HashIndex>, ResolvePathsError>()? {
                    // Copies on this node come first.
                    let node = self.node();
                    let mut aliases: Vec<_> = (aliases.into_iter())
                        .filter(|alias| alias.realm_id == realm_id)
                        .collect();
                    aliases.sort_by_key(|alias| Some(alias.node_id) != node);
                    let copies = aliases.into_iter().take(MAX_CANDIDATES);
                    self.found.extend(copies.filter_map(|alias| {
                        VersionedObjectArn::new(
                            realm_id,
                            alias.node_id,
                            alias.bucket,
                            alias.key,
                            alias.version_id,
                        )
                        .ok()
                    }));
                }
                Ok(Next::Done(std::mem::take(&mut self.found)))
            }
        }
    }
}

/// The readable exact object a file entity names, tried in the order it was located.
fn lookup(
    ids: NodeRealm,
    document: &Document,
    auth: &AuthContext,
    identity: &DataIdentity,
) -> LookupOperation {
    LookupOperation {
        ids,
        locate: Some(located(ids, document, identity)),
        auth: auth.clone(),
        located: false,
        candidates: VecDeque::new(),
        failure: GitError::NotFound,
    }
}

#[derive(Debug, PartialEq)]
struct LookupOperation {
    ids: NodeRealm,
    locate: Option<LocatedOperation>,
    auth: AuthContext,
    located: bool,
    candidates: VecDeque<VersionedObjectArn>,
    failure: GitError,
}

impl LookupOperation {
    fn next_candidate(&mut self) -> ExactTurn {
        let Some(exact) = self.candidates.pop_front() else {
            return Err(std::mem::replace(&mut self.failure, GitError::NotFound));
        };
        let node = self.ids.map(|(node_id, _)| node_id);
        let read = resolve_exact(node, &exact, &self.auth);
        Ok(Next::Call(IoCall::child(read)))
    }
}

impl Operation for LookupOperation {
    type Output = StoredObject;
    type Error = GitError;

    fn first(&mut self) -> ExactTurn {
        let locate = self.locate.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(locate)))
    }

    fn next(&mut self, result: IoResult) -> ExactTurn {
        if !self.located {
            self.located = true;
            self.candidates = result.child::<Vec<VersionedObjectArn>, GitError>()??.into();
            return self.next_candidate();
        }
        match result.child::<StoredObject, GitError>()? {
            Ok(object) => Ok(Next::Done(object)),
            Err(error) => {
                self.failure = error;
                self.next_candidate()
            }
        }
    }
}

/// One file entity of a graph and what its identifiers name.
#[derive(Debug, PartialEq)]
struct LinkedFile {
    id: String,
    path: Option<String>,
    identity: DataIdentity,
}

/// File entities that name an Aruna object in any supported form, with the repository path of
/// a plain copy: `localPath`, the key inside the dataset location, or a layout of the keys.
/// An object the caller cannot read stays out of the repository; its entity still describes it.
pub(super) fn linked(
    ids: NodeRealm,
    document: &Document,
    jsonld: &str,
    auth: &AuthContext,
) -> LinkedOperation {
    let value = serde_json::from_str::<serde_json::Value>(jsonld).ok();
    let entities = value
        .iter()
        .flat_map(|value| value["@graph"].as_array().into_iter().flatten());
    let files = entities
        .filter(|entity| is_file(entity))
        .take(MAX_FILES)
        .filter_map(|entity| {
            let id = entity["@id"].as_str()?;
            Some(LinkedFile {
                id: id.to_string(),
                path: entity_path(entity),
                identity: DataIdentity::read(id, &text_values(entity.get("contentUrl"))),
            })
        })
        .collect();
    LinkedOperation {
        ids,
        document: document.clone(),
        auth: auth.clone(),
        parsed: value.is_some(),
        files,
        location: None,
        looking: false,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct LinkedOperation {
    ids: NodeRealm,
    document: Document,
    auth: AuthContext,
    parsed: bool,
    files: Vec<LinkedFile>,
    location: Option<DatasetLocation>,
    looking: bool,
}

type LinkedTurn = Result<Next<Vec<LinkedObject>>, GitError>;

impl LinkedOperation {
    /// Names the objects found. Independent lookups keep the entity order.
    fn finish(&mut self, results: Vec<Result<StoredObject, GitError>>) -> LinkedTurn {
        let mut linked = Vec::new();
        for (file, resolved) in std::mem::take(&mut self.files).into_iter().zip(results) {
            let object = match resolved {
                Ok(object) => object,
                Err(error) if file.identity.is_aruna() => {
                    tracing::warn!(entity = %file.id, %error, "Leaving unreadable object out of Git");
                    continue;
                }
                Err(_) => continue,
            };
            let stored = file.identity.location.as_ref().zip(self.location.as_ref());
            let path = file.path.or_else(|| {
                let (source, location) = stored?;
                location
                    .path(&source.bucket, &source.key)
                    .and_then(|path| data_path(&path_id(path)))
            });
            linked.push(LinkedObject {
                entity: file.id,
                path,
                object,
            });
        }
        let storage_key = |linked: &LinkedObject| aruna_core::archive_path::StorageKey {
            bucket: linked.object.bucket.clone(),
            key: linked.object.key.clone(),
        };
        let sources: Vec<_> = (linked.iter())
            .filter(|linked| linked.path.is_none())
            .map(|linked| Some(storage_key(linked)))
            .collect();
        let layout = aruna_core::archive_path::KeyLayout::new(&sources);
        for linked in linked.iter_mut().filter(|linked| linked.path.is_none()) {
            let source = storage_key(linked);
            linked.path = Some(layout.path(&source).unwrap_or_else(|| {
                aruna_core::archive_path::synthesized_path(linked.object.blake3, &linked.entity)
            }));
        }
        Ok(Next::Done(linked))
    }
}

impl Operation for LinkedOperation {
    type Output = Vec<LinkedObject>;
    type Error = GitError;

    fn first(&mut self) -> LinkedTurn {
        if !self.parsed {
            return Ok(Next::Done(Vec::new()));
        }
        let effective = location::effective(&self.document);
        Ok(Next::Call(IoCall::child(effective)))
    }

    fn next(&mut self, result: IoResult) -> LinkedTurn {
        if self.looking {
            let results = group_results(result)?.into_iter().map(|member| {
                member_outcome::<StoredObject, GitError>(member).unwrap_or(Err(GitError::NotFound))
            });
            return self.finish(results.collect());
        }
        let chosen = result.child::<(DatasetLocation, bool), GitError>()?;
        self.location = chosen.ok().map(|(location, _)| location);
        if self.files.is_empty() {
            return self.finish(Vec::new());
        }
        let members = (self.files.iter())
            .map(|file| {
                let find = lookup(self.ids, &self.document, &self.auth, &file.identity);
                child_member(find)
            })
            .collect();
        self.looking = true;
        Ok(Next::Call(group_call(members, LOOKUPS, None)))
    }
}

/// The first File entity naming an exact object `auth` cannot read. The ARC later links objects
/// as the metadata event's author, so a restricted writer must only name objects it may read.
pub fn unreadable_file(
    node: Option<NodeId>,
    auth: &AuthContext,
    entities: &[serde_json::Value],
) -> UnreadableFileOperation {
    UnreadableFileOperation {
        node,
        auth: auth.clone(),
        files: exact_files(entities.iter(), auth.realm_id).into(),
        asked: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct UnreadableFileOperation {
    node: Option<NodeId>,
    auth: AuthContext,
    files: VecDeque<(String, VersionedObjectArn)>,
    asked: Option<String>,
}

impl UnreadableFileOperation {
    fn next_file(&mut self) -> Result<Next<Option<String>>, GitError> {
        let Some((entity, exact)) = self.files.pop_front() else {
            return Ok(Next::Done(None));
        };
        self.asked = Some(entity);
        let read = resolve_exact(self.node, &exact, &self.auth);
        Ok(Next::Call(IoCall::child(read)))
    }
}

impl Operation for UnreadableFileOperation {
    type Output = Option<String>;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Option<String>>, GitError> {
        self.next_file()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<String>>, GitError> {
        match result.child::<StoredObject, GitError>()? {
            Ok(_) => self.next_file(),
            Err(_) => Ok(Next::Done(self.asked.take())),
        }
    }
}

#[cfg(test)]
#[path = "linked_tests.rs"]
mod tests;
