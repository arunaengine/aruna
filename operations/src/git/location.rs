//! Reads and chooses a dataset's storage location, which replicates as a Git record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{GitChange, GitRecord};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::reducer::decode_reducer_state;
use aruna_core::rows::{read_row, value};
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{
    BucketInfo, bucket_permission_path, object_permission_path,
};
use aruna_core::structs::storage::dataset_location::{DatasetLocation, default_bucket};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::GroupId;
use tracing::warn;
use ulid::Ulid;

use super::access::{Repository, RepositoryOperation};
use super::objects::EnsureBucketOperation;
use super::publish::PublishRecordOperation;
use super::state::reduce;
use super::{GitError, records};
use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_core::git_state::Ancestry;

type Document = MetadataRegistryRecord;
type Chosen = (DatasetLocation, bool);

/// The location a reduced state chose, or the default one; `true` marks the default.
pub fn chosen(document: &Document, location: Option<(Ulid, DatasetLocation)>) -> Chosen {
    match location {
        Some((_, location)) => (location, false),
        None => (
            DatasetLocation::default_for(document.group_id, document.document_id),
            true,
        ),
    }
}

/// The chosen location, or the default one; `true` marks the default.
pub fn effective(document: &Document) -> EffectiveOperation {
    EffectiveOperation {
        document: document.clone(),
    }
}

#[derive(Debug, PartialEq)]
pub struct EffectiveOperation {
    document: Document,
}

impl Operation for EffectiveOperation {
    type Output = Chosen;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Chosen>, GitError> {
        let scan = records::scan(self.document.document_id);
        Ok(Next::Call(IoCall::child(scan)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Chosen>, GitError> {
        let records = result.child::<Vec<GitRecord>, GitError>()??;
        let location = reduce(&records, &Ancestry::new()).0.location;
        Ok(Next::Done(chosen(&self.document, location)))
    }
}

/// The location of a dataset the caller may read.
pub fn get(node: Option<NodeId>, auth: &AuthContext, id: Ulid) -> GetLocationOperation {
    GetLocationOperation {
        open: Some(RepositoryOperation::new(node, auth, id, Permission::READ)),
        reading: false,
    }
}

#[derive(Debug, PartialEq)]
pub struct GetLocationOperation {
    open: Option<RepositoryOperation>,
    reading: bool,
}

impl Operation for GetLocationOperation {
    type Output = Chosen;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Chosen>, GitError> {
        let open = self.open.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(open)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Chosen>, GitError> {
        if self.reading {
            return Ok(Next::Done(result.child::<Chosen, GitError>()??));
        }
        let (document, _) = result.child::<Repository, GitError>()??;
        self.reading = true;
        Ok(Next::Call(IoCall::child(effective(&document))))
    }
}

/// Records `location` as the dataset's choice after WRITE on the dataset and on the location.
pub fn set(
    node: Option<NodeId>,
    auth: &AuthContext,
    id: Ulid,
    location: DatasetLocation,
) -> SetLocationOperation {
    SetLocationOperation {
        node,
        auth: auth.clone(),
        open: Some(RepositoryOperation::new(node, auth, id, Permission::WRITE)),
        location: Some(location),
        recording: false,
    }
}

#[derive(Debug, PartialEq)]
pub struct SetLocationOperation {
    node: Option<NodeId>,
    auth: AuthContext,
    open: Option<RepositoryOperation>,
    location: Option<DatasetLocation>,
    recording: bool,
}

impl Operation for SetLocationOperation {
    type Output = DatasetLocation;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<DatasetLocation>, GitError> {
        let open = self.open.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(open)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<DatasetLocation>, GitError> {
        if self.recording {
            return Ok(Next::Done(result.child::<DatasetLocation, GitError>()??));
        }
        let (document, _) = result.child::<Repository, GitError>()??;
        let location = self.location.take().ok_or(IoError::Unfinished)?;
        self.recording = true;
        let record = RecordLocationOperation::new(self.node, &self.auth, &document, location);
        Ok(Next::Call(IoCall::child(record)))
    }
}

/// Records `location` for a document whose WRITE the caller already proved, as the create
/// that returned the document did. A location moves no ref, so it needs no repository hold.
pub fn record(
    node: Option<NodeId>,
    auth: &AuthContext,
    document: &Document,
    location: DatasetLocation,
) -> RecordLocationOperation {
    RecordLocationOperation::new(node, auth, document, location)
}

#[derive(Debug, PartialEq)]
pub struct RecordLocationOperation {
    node: Option<NodeId>,
    auth: AuthContext,
    document: Document,
    location: DatasetLocation,
    publishing: bool,
}

impl RecordLocationOperation {
    fn new(
        node: Option<NodeId>,
        auth: &AuthContext,
        document: &Document,
        location: DatasetLocation,
    ) -> Self {
        Self {
            node,
            auth: auth.clone(),
            document: document.clone(),
            location,
            publishing: false,
        }
    }
}

impl Operation for RecordLocationOperation {
    type Output = DatasetLocation;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<DatasetLocation>, GitError> {
        let check = WritableOperation::new(self.node, &self.auth, &self.document, &self.location);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<DatasetLocation>, GitError> {
        if self.publishing {
            result.child::<GitRecord, GitError>()??;
            return Ok(Next::Done(self.location.clone()));
        }
        result.child::<BucketInfo, GitError>()??;
        self.publishing = true;
        let change = GitChange::Location(self.location.clone());
        let publish = PublishRecordOperation::new(
            self.node,
            &self.document,
            self.auth.user_id,
            change,
            (None, Vec::new()),
        );
        Ok(Next::Call(IoCall::child(publish)))
    }
}

fn existing_call(name: &str) -> IoCall {
    IoCall::child(GetBucketOperation::new(name.to_string()))
}

fn existing_answer(result: IoResult) -> Result<BucketInfo, GitError> {
    match result.child::<BucketInfo, GetBucketError>()? {
        Ok(bucket) => Ok(bucket),
        Err(GetBucketError::NotFound) => Err(GitError::NotFound),
        Err(_) => Err(GitError::Unavailable),
    }
}

/// The call that checks WRITE of `auth` under the location's prefix in `bucket`.
fn permit_call(
    node: Option<NodeId>,
    auth: &AuthContext,
    bucket: &BucketInfo,
    location: &DatasetLocation,
) -> Result<IoCall, GitError> {
    let node = node.ok_or(GitError::Unavailable)?;
    let prefix = location.prefix.trim_end_matches('/');
    let name = &location.bucket;
    let path = match prefix.is_empty() {
        true => bucket_permission_path(auth.realm_id, bucket.group_id, node, name),
        false => object_permission_path(auth.realm_id, bucket.group_id, node, name, prefix),
    };
    let authorize = AuthorizeOperation::new(
        auth.realm_id,
        auth,
        &path,
        &Permission::WRITE,
        PolicyRequestExtras::rest(),
    );
    Ok(IoCall::child(authorize))
}

fn permit_answer(result: IoResult) -> Result<(), GitError> {
    Ok(result.child::<(), AuthorizeError>()??)
}

/// This node's bucket of a location when `auth` may write under its prefix. The default bucket
/// is created on first use; any other bucket must already exist here.
pub fn writable(
    node: Option<NodeId>,
    auth: &AuthContext,
    document: &Document,
    location: &DatasetLocation,
) -> WritableOperation {
    WritableOperation::new(node, auth, document, location)
}

#[derive(Debug, PartialEq)]
pub struct WritableOperation {
    node: Option<NodeId>,
    auth: AuthContext,
    document: Document,
    location: DatasetLocation,
    owned: bool,
    bucket: Option<BucketInfo>,
}

impl WritableOperation {
    fn new(
        node: Option<NodeId>,
        auth: &AuthContext,
        document: &Document,
        location: &DatasetLocation,
    ) -> Self {
        Self {
            node,
            auth: auth.clone(),
            document: document.clone(),
            location: location.clone(),
            owned: location.bucket == default_bucket(document.group_id),
            bucket: None,
        }
    }
}

impl Operation for WritableOperation {
    type Output = BucketInfo;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<BucketInfo>, GitError> {
        if !self.location.valid() {
            return Err(GitError::Invalid);
        }
        Ok(Next::Call(match self.owned {
            true => IoCall::child(EnsureBucketOperation::new(&self.document, &self.auth)),
            false => existing_call(&self.location.bucket),
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<BucketInfo>, GitError> {
        if let Some(bucket) = self.bucket.take() {
            permit_answer(result)?;
            return Ok(Next::Done(bucket));
        }
        let bucket = match self.owned {
            true => result.child::<BucketInfo, GitError>()??,
            false => existing_answer(result)?,
        };
        let permit = permit_call(self.node, &self.auth, &bucket, &self.location)?;
        self.bucket = Some(bucket);
        Ok(Next::Call(permit))
    }
}

/// How a location is checked before a dataset or group default may use it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CheckKind {
    /// A dataset's own choice: the bucket must exist and allow the write.
    Chosen,
    /// A group default: the bucket must also belong to the group, and a missing one is named.
    Default,
}

/// Checks a location without creating anything; the group's generated bucket always passes,
/// since it is created on first use.
#[derive(Debug, PartialEq)]
pub struct CheckLocationOperation {
    node: Option<NodeId>,
    auth: AuthContext,
    group_id: GroupId,
    location: DatasetLocation,
    kind: CheckKind,
    permitting: bool,
}

impl CheckLocationOperation {
    fn new(
        node: Option<NodeId>,
        auth: &AuthContext,
        group_id: GroupId,
        location: &DatasetLocation,
        kind: CheckKind,
    ) -> Self {
        Self {
            node,
            auth: auth.clone(),
            group_id,
            location: location.clone(),
            kind,
            permitting: false,
        }
    }

    fn found(&self, found: Result<BucketInfo, GitError>) -> Result<BucketInfo, GitError> {
        let name = &self.location.bucket;
        if self.kind == CheckKind::Chosen {
            return found;
        }
        let bucket = match found {
            Err(GitError::NotFound) => {
                return Err(GitError::Refused(format!(
                    "bucket {name} does not exist on this node; choose another storage location"
                )));
            }
            found => found?,
        };
        if bucket.group_id != self.group_id {
            return Err(GitError::Refused(format!(
                "bucket {name} belongs to another group; choose a bucket of this group"
            )));
        }
        Ok(bucket)
    }
}

impl Operation for CheckLocationOperation {
    type Output = ();
    type Error = GitError;

    fn first(&mut self) -> Result<Next<()>, GitError> {
        if self.location.bucket == default_bucket(self.group_id) {
            return Ok(Next::Done(()));
        }
        Ok(Next::Call(existing_call(&self.location.bucket)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GitError> {
        if self.permitting {
            permit_answer(result)?;
            return Ok(Next::Done(()));
        }
        let bucket = self.found(existing_answer(result))?;
        self.permitting = true;
        let permit = permit_call(self.node, &self.auth, &bucket, &self.location)?;
        Ok(Next::Call(permit))
    }
}

/// Checks a location chosen for a dataset that does not exist yet: a bucket other than the
/// group's default one must exist here and allow `auth` to write under the prefix.
pub fn precheck(
    node: Option<NodeId>,
    auth: &AuthContext,
    group_id: GroupId,
    location: &DatasetLocation,
) -> CheckLocationOperation {
    CheckLocationOperation::new(node, auth, group_id, location, CheckKind::Chosen)
}

/// Checks a group default here: its bucket exists on this node, belongs to the group and
/// allows `auth` to write under the prefix.
pub fn check_default(
    node: Option<NodeId>,
    auth: &AuthContext,
    group_id: GroupId,
    location: &DatasetLocation,
) -> CheckLocationOperation {
    CheckLocationOperation::new(node, auth, group_id, location, CheckKind::Default)
}

/// Where the group's new datasets store their files, as stored on this node. Groups record
/// their generated bucket at creation; a group without a record uses the same.
pub fn group_default(group_id: GroupId) -> GroupDefaultOperation {
    GroupDefaultOperation { group_id }
}

#[derive(Debug, PartialEq)]
pub struct GroupDefaultOperation {
    group_id: GroupId,
}

impl Operation for GroupDefaultOperation {
    type Output = DatasetLocation;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<DatasetLocation>, GitError> {
        let target = AdminDocumentTarget::Group {
            group_id: self.group_id,
        };
        let key = reducer_state_key(&target);
        Ok(Next::Call(read_row(DOCUMENT_STATE_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<DatasetLocation>, GitError> {
        let state = value(result)?
            .map(|bytes| decode_reducer_state(&bytes))
            .transpose()
            .map_err(|_| GitError::Unavailable)?;
        let location = state.and_then(|state| state.group_location());
        let group_id = self.group_id;
        Ok(Next::Done(location.unwrap_or_else(|| {
            DatasetLocation::group_default(group_id)
        })))
    }
}

/// What the pending call of resolving a location answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResolveStep {
    Chosen,
    Default,
    DefaultChecked,
}

/// Where a new dataset stores its files: the explicit choice, else the group default, which
/// gets `<document id>/` appended to its prefix at create (`true`).
pub fn resolve(
    node: Option<NodeId>,
    auth: &AuthContext,
    group_id: GroupId,
    chosen: Option<DatasetLocation>,
) -> ResolveLocationOperation {
    ResolveLocationOperation {
        node,
        auth: auth.clone(),
        group_id,
        location: chosen,
        step: ResolveStep::Default,
    }
}

#[derive(Debug, PartialEq)]
pub struct ResolveLocationOperation {
    node: Option<NodeId>,
    auth: AuthContext,
    group_id: GroupId,
    location: Option<DatasetLocation>,
    step: ResolveStep,
}

impl ResolveLocationOperation {
    fn check(&self, kind: CheckKind) -> Result<IoCall, GitError> {
        let location = self.location.as_ref().ok_or(IoError::Unfinished)?;
        let check =
            CheckLocationOperation::new(self.node, &self.auth, self.group_id, location, kind);
        Ok(IoCall::child(check))
    }

    fn finish(&mut self, shared: bool) -> Result<Next<Chosen>, GitError> {
        let location = self.location.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Done((location, shared)))
    }
}

impl Operation for ResolveLocationOperation {
    type Output = Chosen;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Chosen>, GitError> {
        if self.location.is_some() {
            self.step = ResolveStep::Chosen;
            return Ok(Next::Call(self.check(CheckKind::Chosen)?));
        }
        Ok(Next::Call(IoCall::child(group_default(self.group_id))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Chosen>, GitError> {
        match self.step {
            ResolveStep::Chosen => {
                result.child::<(), GitError>()??;
                self.finish(false)
            }
            ResolveStep::Default => {
                self.location = Some(result.child::<DatasetLocation, GitError>()??);
                self.step = ResolveStep::DefaultChecked;
                Ok(Next::Call(self.check(CheckKind::Default)?))
            }
            ResolveStep::DefaultChecked => {
                result.child::<(), GitError>()??;
                self.finish(true)
            }
        }
    }
}

/// Records the resolved location of a just-created dataset, so later group default changes
/// never move it. A location not recorded here can be set with PUT later.
pub fn establish(
    node: Option<NodeId>,
    auth: &AuthContext,
    document: &Document,
    (location, shared): Chosen,
) -> EstablishOperation {
    let location = match shared {
        true => location.for_dataset(document.document_id),
        false => location,
    };
    EstablishOperation {
        document_id: document.document_id,
        // A dataset without a record already uses exactly this location, so none is written.
        record: (location != DatasetLocation::default_for(document.group_id, document.document_id))
            .then(|| RecordLocationOperation::new(node, auth, document, location)),
    }
}

#[derive(Debug, PartialEq)]
pub struct EstablishOperation {
    document_id: Ulid,
    record: Option<RecordLocationOperation>,
}

impl Operation for EstablishOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(match self.record.take() {
            Some(record) => Next::Call(IoCall::child(record)),
            None => Next::Done(()),
        })
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if let Err(error) = result.child::<DatasetLocation, GitError>()? {
            let document_id = self.document_id;
            warn!(%document_id, %error, "Chosen storage location not recorded at create");
        }
        Ok(Next::Done(()))
    }
}

#[cfg(test)]
#[path = "location_tests.rs"]
mod tests;
