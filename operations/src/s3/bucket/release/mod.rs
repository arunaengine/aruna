//! Database release membership: tagged release crate versions, the rows that find them, and
//! the check which exact versions a release pins, decided with the caller's own access.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, NetEffect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::keyspaces::{
    BUCKET_RELEASE_KEYSPACE, DISTRIBUTION_FENCE_KEYSPACE, DISTRIBUTION_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::identifier::{Identifier, IdentifierTarget};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::bucket_metadata::{
    RELEASE_TAG, ReleasePointer, ReleaseRef, release_prefix,
};
use aruna_core::structs::storage::distribution::fence::LocalFence;
use aruna_core::structs::storage::distribution::{DistributionView, KeyRange};
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::origin::OriginVersion;
use aruna_core::structs::storage::release::{
    CrateBytes, CrateRead, CrateReadError, HolderAnswer, HolderAsk, HolderRequest, ReleaseEffect,
    ReleaseEvent, ValidRelease,
};
pub use aruna_core::structs::storage::release::{
    MembershipQuery, RangeFiles, RangeMembers, ReleaseFile, ReleaseMembers,
};
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::types::{Effects, Key};
use serde_json::Value;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::auth::request_authorization::{AuthorizeError, BucketAccess};
use crate::driver::{DriverContext, drive};

/// Rows read per storage page while looking for a bucket's releases.
const ROW_PAGE: usize = 256;

#[derive(Debug, Error, PartialEq)]
pub enum ReleaseError {
    #[error("release state unavailable: {0}")]
    Unavailable(String),
    #[error("unexpected event in state {state}: {got}")]
    UnexpectedEvent { state: &'static str, got: String },
}

impl From<ConversionError> for ReleaseError {
    fn from(error: ConversionError) -> Self {
        Self::Unavailable(error.to_string())
    }
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
        exact_id(
            self.realm_id,
            self.node_id,
            (self.bucket, self.info),
            key,
            version_id,
        )
    }
}

fn exact_id(
    realm_id: RealmId,
    node_id: NodeId,
    (bucket, info): (&str, &BucketInfo),
    key: &str,
    version_id: Ulid,
) -> Option<OriginVersion> {
    match info.distribution {
        Some(_) => Some(OriginVersion::Global(GlobalId::Version {
            realm_id,
            version_id,
        })),
        None => VersionedObjectArn::new(realm_id, node_id, bucket, key, version_id)
            .ok()
            .map(OriginVersion::Node),
    }
}

/// The caller's loaded rules and policies; compared by identity, since they are read once.
#[derive(Clone)]
pub struct Access(pub Arc<BucketAccess>);

impl std::fmt::Debug for Access {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Access")
    }
}

impl PartialEq for Access {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// What one membership check covers.
#[derive(Debug, PartialEq)]
pub struct MembersConfig {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub bucket: String,
    pub info: BucketInfo,
    pub auth: AuthContext,
    pub access: Access,
    /// Asks other members of a distributed bucket; `None` answers only what is local.
    pub bearer: Option<String>,
    /// Empty lists every valid release.
    pub queries: Vec<MembershipQuery>,
    /// A holder answers for exactly these ranges and asks nobody else.
    pub ranges: Option<Vec<Ulid>>,
}

#[derive(Debug, PartialEq)]
pub struct MembersOutput {
    pub answers: Vec<ReleaseMembers>,
    pub releases: Vec<ValidRelease>,
    /// `false` when some release could not be checked, so one may be missing.
    pub complete: bool,
    /// Ranges a holder was asked for but does not coordinate.
    pub refused: Vec<Ulid>,
}

#[derive(Debug, PartialEq)]
enum MembersState {
    Init,
    ReadSet,
    Rows,
    Crate,
    Holder,
    Finish,
    Error,
}

/// Decides which releases pin the queried versions. Only a range's coordinator answers for it,
/// since a copy may not have received a release yet: this node for the ranges it coordinates,
/// the coordinator itself for the others, asked with the caller's bearer. A release crate
/// counts only when the caller may read it and its exact version carries the release tag.
#[derive(Debug, PartialEq)]
pub struct ReleaseMembersOperation {
    config: MembersConfig,
    state: MembersState,
    /// The ranges this node answers for; `None` answers for the whole bucket.
    local: Option<Vec<KeyRange>>,
    /// Ranges asked of their coordinator, with the node to ask.
    remote: Vec<(Ulid, NodeId)>,
    refused: Vec<Ulid>,
    candidates: Vec<(Ulid, ReleasePointer)>,
    asked: Vec<(NodeId, Vec<Ulid>)>,
    output: MembersOutput,
    error: Option<ReleaseError>,
}

impl ReleaseMembersOperation {
    pub fn new(config: MembersConfig) -> Self {
        let answers = vec![ReleaseMembers::complete(); config.queries.len()];
        Self {
            config,
            state: MembersState::Init,
            local: None,
            remote: Vec::new(),
            refused: Vec::new(),
            candidates: Vec::new(),
            asked: Vec::new(),
            output: MembersOutput {
                answers,
                releases: Vec::new(),
                complete: true,
                refused: Vec::new(),
            },
            error: None,
        }
    }

    fn fail(&mut self, error: ReleaseError) -> Effects {
        self.state = MembersState::Error;
        self.error = Some(error);
        smallvec![]
    }

    fn unexpected(&mut self, got: Event) -> Effects {
        let state = match self.state {
            MembersState::ReadSet => "ReadSet",
            MembersState::Rows => "Rows",
            MembersState::Crate => "Crate",
            MembersState::Holder => "Holder",
            _ => "Other",
        };
        self.fail(ReleaseError::UnexpectedEvent {
            state,
            got: format!("{got:?}"),
        })
    }

    fn incomplete(&mut self) {
        self.output.complete = false;
        self.output
            .answers
            .iter_mut()
            .for_each(|answer| answer.complete = false);
    }

    fn read_rows(&mut self, start: Option<Key>) -> Effects {
        self.state = MembersState::Rows;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BUCKET_RELEASE_KEYSPACE.to_string(),
            prefix: Some(release_prefix(&self.config.bucket).into()),
            start: start.map(IterStart::After),
            limit: ROW_PAGE,
            txn_id: None,
        })]
    }

    /// Splits the set's ranges into the ones this node coordinates and the ones asked of their
    /// coordinator; a holder refuses what it does not coordinate.
    fn plan(&mut self, view: &DistributionView, fence: &LocalFence) -> Effects {
        let Some(member_id) =
            (self.config.info.distribution.as_ref()).map(|binding| binding.member_id)
        else {
            return self.read_rows(None);
        };
        let snapshot = view.snapshot();
        let mut local = Vec::new();
        for range in &snapshot.config.ranges {
            let asked =
                (self.config.ranges.as_ref()).is_none_or(|ids| ids.contains(&range.range_id));
            if !asked {
                continue;
            }
            if snapshot.lists_locally(member_id, range, fence) {
                local.push(range.clone());
                continue;
            }
            let coordinator = snapshot.list_holder(range).map(|member| member.node_id);
            match (&self.config.ranges, coordinator) {
                (Some(_), _) => self.refused.push(range.range_id),
                (None, Some(node)) if node != self.config.node_id => {
                    self.remote.push((range.range_id, node))
                }
                (None, _) => self.incomplete(),
            }
        }
        self.local = Some(local);
        self.read_rows(None)
    }

    fn on_set(&mut self, values: Vec<(Key, Option<aruna_core::types::Value>)>) -> Effects {
        let mut values = values.into_iter().map(|(_, value)| value);
        let (Some(Some(set)), fence) = (values.next(), values.next().flatten()) else {
            self.incomplete();
            return self.finish();
        };
        let view = match DistributionView::from_bytes(set.as_ref()) {
            Ok(view) => view,
            Err(error) => return self.fail(error.into()),
        };
        let fence = match LocalFence::decode(fence.as_deref()) {
            Ok(fence) => fence,
            Err(error) => return self.fail(error.into()),
        };
        self.plan(&view, &fence)
    }

    fn on_rows(
        &mut self,
        values: Vec<(Key, aruna_core::types::Value)>,
        next: Option<Key>,
    ) -> Effects {
        let prefix_len = release_prefix(&self.config.bucket).len();
        for (key, value) in values {
            let Some(id) = key
                .get(prefix_len..)
                .and_then(|id| <[u8; 16]>::try_from(id).ok())
            else {
                continue;
            };
            let pointer = match ReleasePointer::from_bytes(value.as_ref()) {
                Ok(pointer) => pointer,
                Err(error) => return self.fail(error.into()),
            };
            let held = (self.local.as_ref())
                .is_none_or(|ranges| ranges.iter().any(|range| range.contains(&pointer.key)));
            let wanted = self.config.queries.is_empty()
                || (self.config.queries.iter()).any(|query| {
                    query.key.starts_with(&pointer.prefix) && query.key != pointer.key
                });
            // A crate the caller may not read is never opened, so nothing of it shows.
            let readable = (self.config.access.0).allows(Some(&pointer.key), &Permission::READ);
            if held && wanted && readable {
                self.candidates.push((Ulid::from_bytes(id), pointer));
            }
        }
        match next {
            Some(next) => self.read_rows(Some(next)),
            None => self.next_crate(),
        }
    }

    fn next_crate(&mut self) -> Effects {
        let Some((version_id, pointer)) = self.candidates.last() else {
            return self.next_holder();
        };
        self.state = MembersState::Crate;
        let read = CrateRead {
            bucket: self.config.bucket.clone(),
            key: pointer.key.clone(),
            version_id: *version_id,
            group_id: self.config.info.group_id,
            auth: self.config.auth.clone(),
            realm_id: self.config.realm_id,
            node_id: self.config.node_id,
        };
        let effect = ReleaseEffect::ReadCrate(Box::new(read));
        smallvec![Effect::Net(NetEffect::Release(Box::new(effect)))]
    }

    fn on_crate(&mut self, read: Result<CrateBytes, CrateReadError>) -> Effects {
        let Some((version_id, pointer)) = self.candidates.pop() else {
            return self.fail(ReleaseError::Unavailable("no crate was asked for".into()));
        };
        let crate_json = match read {
            // A purged version, or one of an earlier bucket of the name, is no release.
            Err(CrateReadError::Missing) => return self.next_crate(),
            Err(CrateReadError::Failed(_)) => {
                self.mark_asked(&pointer);
                return self.next_crate();
            }
            Ok(read) if read.metadata.get(RELEASE_TAG) != Some(&pointer.label) => {
                return self.next_crate();
            }
            Ok(read) => match serde_json::from_slice::<Value>(&read.bytes) {
                Ok(crate_json) => crate_json,
                Err(_) => {
                    self.mark_asked(&pointer);
                    return self.next_crate();
                }
            },
        };
        let config = &self.config;
        let scope = (config.bucket.as_str(), &config.info);
        let Some(crate_version) = exact_id(
            config.realm_id,
            config.node_id,
            scope,
            &pointer.key,
            version_id,
        ) else {
            return self.next_crate();
        };
        let release = ReleaseRef {
            crate_version,
            label: pointer.label.clone(),
            upstream: upstream_of(&crate_json),
        };
        let pinned = pinned_ids(&crate_json);
        for (index, query) in config.queries.iter().enumerate() {
            let id = exact_id(
                config.realm_id,
                config.node_id,
                scope,
                &query.key,
                query.version_id,
            );
            let member = query.key.starts_with(&pointer.prefix)
                && id.is_some_and(|id| pinned.contains(&id.to_id()));
            if member {
                self.output.answers[index].releases.push(release.clone());
            }
        }
        self.output.releases.push(ValidRelease {
            release,
            key: pointer.key,
            version_id,
            prefix: pointer.prefix,
        });
        self.next_crate()
    }

    /// A crate that could not be read leaves the answers it may hold incomplete.
    fn mark_asked(&mut self, pointer: &ReleasePointer) {
        self.output.complete = false;
        for (index, query) in self.config.queries.iter().enumerate() {
            if query.key.starts_with(&pointer.prefix) {
                self.output.answers[index].complete = false;
            }
        }
    }

    fn next_holder(&mut self) -> Effects {
        if self.remote.is_empty() {
            return self.finish();
        }
        let Some(bearer) = self.config.bearer.clone() else {
            // Without a bearer no other node is asked, so their ranges stay unchecked.
            self.remote.clear();
            self.incomplete();
            return self.finish();
        };
        let mut groups: BTreeMap<NodeId, Vec<Ulid>> = BTreeMap::new();
        for (range_id, node) in std::mem::take(&mut self.remote) {
            groups.entry(node).or_default().push(range_id);
        }
        self.asked = groups.into_iter().collect();
        self.ask_holder(bearer)
    }

    fn ask_holder(&mut self, bearer: String) -> Effects {
        let Some((node_id, range_ids)) = self.asked.last().cloned() else {
            return self.finish();
        };
        self.state = MembersState::Holder;
        let request = HolderRequest {
            node_id,
            bearer,
            bucket: self.config.bucket.clone(),
            ask: HolderAsk::Members {
                range_ids,
                queries: self.config.queries.clone(),
            },
        };
        let effect = ReleaseEffect::Holder(request);
        smallvec![Effect::Net(NetEffect::Release(Box::new(effect)))]
    }

    fn on_holder(&mut self, answer: Result<HolderAnswer, String>) -> Effects {
        self.asked.pop();
        match answer {
            Ok(HolderAnswer::Members(members))
                if members.answers.len() == self.output.answers.len()
                    && members.refused.is_empty() =>
            {
                for (answer, more) in self.output.answers.iter_mut().zip(members.answers) {
                    answer.merge(more);
                }
                for release in members.releases {
                    if !self.output.releases.contains(&release) {
                        self.output.releases.push(release);
                    }
                }
                self.output.complete &= members.complete;
            }
            // The coordinator did not answer for all its ranges, so they stay unchecked.
            _ => self.incomplete(),
        }
        match self.config.bearer.clone() {
            Some(bearer) => self.ask_holder(bearer),
            None => self.finish(),
        }
    }

    fn finish(&mut self) -> Effects {
        self.output.refused = std::mem::take(&mut self.refused);
        self.state = MembersState::Finish;
        smallvec![]
    }
}

impl Operation for ReleaseMembersOperation {
    type Output = MembersOutput;
    type Error = ReleaseError;

    fn start(&mut self) -> Effects {
        let Some(binding) = &self.config.info.distribution else {
            // An ordinary bucket has no ranges a holder could answer for.
            if let Some(ids) = self.config.ranges.clone() {
                self.refused = ids;
                return self.finish();
            }
            return self.read_rows(None);
        };
        self.state = MembersState::ReadSet;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    DISTRIBUTION_KEYSPACE.to_string(),
                    binding.set_id.to_bytes().to_vec().into(),
                ),
                (
                    DISTRIBUTION_FENCE_KEYSPACE.to_string(),
                    LocalFence::key(binding.set_id).into(),
                ),
            ],
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (_, Event::Storage(StorageEvent::Error { error })) => {
                self.fail(ReleaseError::Unavailable(error.to_string()))
            }
            (MembersState::ReadSet, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.on_set(values)
            }
            (
                MembersState::Rows,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.on_rows(values, next_start_after),
            (MembersState::Crate, Event::Net(NetEvent::Release(ReleaseEvent::CrateRead(read)))) => {
                self.on_crate(read)
            }
            (MembersState::Holder, Event::Net(NetEvent::Release(ReleaseEvent::Holder(answer)))) => {
                self.on_holder(answer)
            }
            (_, event) => self.unexpected(event),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, MembersState::Finish | MembersState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match (self.state, self.error) {
            (MembersState::Finish, _) => Ok(self.output),
            (_, Some(error)) => Err(error),
            _ => Err(ReleaseError::Unavailable(
                "release check did not finish".into(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Who asks: the context this node validated, and the bearer it was validated from.
#[derive(Clone, Copy, Debug)]
pub struct MembershipCaller<'a> {
    pub auth: &'a AuthContext,
    pub bearer: Option<&'a str>,
}

/// Loads the caller's access once, then runs the membership check.
pub async fn release_members(
    context: &DriverContext,
    scope: BucketScope<'_>,
    caller: MembershipCaller<'_>,
    queries: &[MembershipQuery],
) -> Result<MembersOutput, ReleaseError> {
    let access = load_access(context, scope, caller.auth).await?;
    let config = MembersConfig {
        realm_id: scope.realm_id,
        node_id: scope.node_id,
        bucket: scope.bucket.to_string(),
        info: scope.info.clone(),
        auth: caller.auth.clone(),
        access,
        bearer: caller.bearer.map(str::to_string),
        queries: queries.to_vec(),
        ranges: None,
    };
    drive(ReleaseMembersOperation::new(config), context).await
}

/// The caller's rules and policies for the bucket, read once for every key they decide.
pub async fn load_access(
    context: &DriverContext,
    scope: BucketScope<'_>,
    auth: &AuthContext,
) -> Result<Access, ReleaseError> {
    let place = (scope.realm_id, scope.info.group_id, scope.node_id);
    match BucketAccess::load(context, auth, place, scope.bucket).await {
        Ok(access) => Ok(Access(Arc::new(access))),
        Err(AuthorizeError::Storage(error)) => Err(ReleaseError::Unavailable(error.to_string())),
        Err(error) => Err(ReleaseError::Unavailable(error.to_string())),
    }
}

fn graph(crate_json: &Value) -> impl Iterator<Item = &Value> {
    crate_json["@graph"].as_array().into_iter().flatten()
}

/// The exact version ids the release crate names as its files.
pub(crate) fn pinned_ids(crate_json: &Value) -> BTreeSet<String> {
    graph(crate_json)
        .filter(|entity| entity["@type"] == "File")
        .filter_map(|entity| entity["@id"].as_str().map(str::to_string))
        .collect()
}

/// The upstream identifiers the release crate's root names.
pub(crate) fn upstream_of(crate_json: &Value) -> Vec<Identifier> {
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

#[cfg(test)]
mod tests;
