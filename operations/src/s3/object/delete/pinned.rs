//! Refuses the permanent deletion of object versions a release of the bucket pins; a delete
//! marker stays allowed, since the pinned version remains readable by its id.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Every release counts, also one the caller cannot read, so a refusal names none.

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::BUCKET_RELEASE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::bucket_metadata::release_marker;
use aruna_core::structs::storage::release::{MembershipQuery, ValidRelease};
use aruna_core::structs::storage::storage_purge::StoragePurgeScope;
use aruna_core::types::{Key, Value};
use std::collections::VecDeque;
use thiserror::Error;
use ulid::Ulid;

use crate::s3::bucket::release::access::LoadAccessOperation;
use crate::s3::bucket::release::view::{
    FilePage, ReleaseView, ReleaseViewOperation, ViewConfig, ViewError,
};
use crate::s3::bucket::release::{
    Access, BucketScope, MembersConfig, MembersOutput, ReleaseError, ReleaseMembersOperation,
};

#[derive(Clone, Debug, Error, PartialEq)]
pub enum PinnedError {
    #[error(
        "version {version_id} of `{key}` is pinned by a release; delete the key for a delete \
         marker instead, or delete the release crate version first"
    )]
    Version { key: String, version_id: Ulid },
    #[error(
        "versions in this scope are pinned by a release; delete the release crate versions \
         first, or delete the keys for delete markers instead"
    )]
    Scope,
    #[error("the releases of the bucket could not all be checked, so nothing was deleted: {0}")]
    Unchecked(String),
}

impl From<IoError> for PinnedError {
    fn from(error: IoError) -> Self {
        unchecked(error)
    }
}

fn unchecked(error: impl std::fmt::Display) -> PinnedError {
    PinnedError::Unchecked(error.to_string())
}

/// Where the bucket lives, kept by the checks that run after their caller's borrow.
#[derive(Clone, Debug, PartialEq)]
struct PinPlace {
    realm_id: RealmId,
    node_id: NodeId,
    bucket: String,
    info: BucketInfo,
}

impl PinPlace {
    fn of(scope: BucketScope<'_>) -> Self {
        Self {
            realm_id: scope.realm_id,
            node_id: scope.node_id,
            bucket: scope.bucket.to_string(),
            info: scope.info.clone(),
        }
    }

    fn scope(&self) -> BucketScope<'_> {
        BucketScope {
            realm_id: self.realm_id,
            node_id: self.node_id,
            bucket: &self.bucket,
            info: &self.info,
        }
    }
}

/// Whether a release pins each queried version, in query order. An answer that may miss a
/// release refuses, since the deletion could break it.
pub fn pinned(
    queries: &[MembershipQuery],
    output: &MembersOutput,
) -> Result<Vec<bool>, PinnedError> {
    if !output.complete || output.answers.len() != queries.len() {
        return Err(PinnedError::Unchecked(
            "a release crate could not be read".to_string(),
        ));
    }
    Ok((output.answers.iter())
        .map(|answer| !answer.releases.is_empty())
        .collect())
}

/// Every release of the bucket that pins `queries`, whatever the caller may read. A
/// distributed bucket deletes no single versions, so only this node's releases count.
#[derive(Debug, PartialEq)]
struct RetainedReleases {
    place: PinPlace,
    auth: AuthContext,
    queries: Vec<MembershipQuery>,
    loaded: bool,
}

impl Operation for RetainedReleases {
    type Output = MembersOutput;
    type Error = PinnedError;

    fn first(&mut self) -> Result<Next<MembersOutput>, PinnedError> {
        let load = LoadAccessOperation::new(self.place.scope(), &self.auth);
        Ok(Next::Call(IoCall::child(load)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<MembersOutput>, PinnedError> {
        if self.loaded {
            let output = result.child::<MembersOutput, ReleaseError>()?;
            return output.map(Next::Done).map_err(unchecked);
        }
        let access = result.child::<Access, ReleaseError>()?.map_err(unchecked)?;
        self.loaded = true;
        let place = &self.place;
        let members = ReleaseMembersOperation::new(MembersConfig {
            realm_id: place.realm_id,
            node_id: place.node_id,
            bucket: place.bucket.clone(),
            info: place.info.clone(),
            auth: self.auth.clone(),
            access,
            bearer: None,
            queries: std::mem::take(&mut self.queries),
            ranges: None,
            retention: true,
        });
        Ok(Next::Call(IoCall::child(members)))
    }
}

/// Rows a permanent delete's transaction must still read as the pin check did: the bucket's
/// release marker, so a release added since fails the delete instead of losing its version.
pub type ReleaseGuards = Vec<(String, Key, Option<Value>)>;

type PinsOutput = (Vec<bool>, ReleaseGuards);

/// Whether a release pins each of `queries`, and the guards their deletes must keep. The
/// bucket's release marker is read before its releases.
#[derive(Debug, PartialEq)]
pub struct VersionPinsOperation {
    place: PinPlace,
    auth: AuthContext,
    queries: Vec<MembershipQuery>,
    guards: Option<ReleaseGuards>,
}

impl VersionPinsOperation {
    pub fn new(scope: BucketScope<'_>, auth: &AuthContext, queries: Vec<MembershipQuery>) -> Self {
        Self {
            place: PinPlace::of(scope),
            auth: auth.clone(),
            queries,
            guards: None,
        }
    }

    fn marker(&self) -> Key {
        release_marker(&self.place.bucket).into()
    }
}

impl Operation for VersionPinsOperation {
    type Output = PinsOutput;
    type Error = PinnedError;

    fn first(&mut self) -> Result<Next<PinsOutput>, PinnedError> {
        if self.queries.is_empty() || self.place.info.distribution.is_some() {
            return Ok(Next::Done((vec![false; self.queries.len()], Vec::new())));
        }
        Ok(Next::Call(read_row(BUCKET_RELEASE_KEYSPACE, self.marker())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PinsOutput>, PinnedError> {
        let Some(guards) = self.guards.take() else {
            let row = (
                BUCKET_RELEASE_KEYSPACE.to_string(),
                self.marker(),
                result.value()?,
            );
            self.guards = Some(vec![row]);
            let retained = RetainedReleases {
                place: self.place.clone(),
                auth: self.auth.clone(),
                queries: self.queries.clone(),
                loaded: false,
            };
            return Ok(Next::Call(IoCall::child(retained)));
        };
        let output = result.child::<MembersOutput, PinnedError>()??;
        Ok(Next::Done((pinned(&self.queries, &output)?, guards)))
    }
}

/// Refuses the permanent deletion of one version a release pins; else outputs the guards its
/// delete must keep.
#[derive(Debug, PartialEq)]
pub struct RefusePinnedOperation {
    pins: Option<VersionPinsOperation>,
    key: String,
    version_id: Ulid,
}

impl RefusePinnedOperation {
    pub fn new(
        scope: BucketScope<'_>,
        auth: &AuthContext,
        (key, version_id): (&str, Ulid),
    ) -> Self {
        let query = MembershipQuery {
            key: key.to_string(),
            version_id,
        };
        Self {
            pins: Some(VersionPinsOperation::new(scope, auth, vec![query])),
            key: key.to_string(),
            version_id,
        }
    }
}

impl Operation for RefusePinnedOperation {
    type Output = ReleaseGuards;
    type Error = PinnedError;

    fn first(&mut self) -> Result<Next<ReleaseGuards>, PinnedError> {
        let pins = self.pins.take();
        let pins = pins.ok_or_else(|| unchecked("the pin check already ran"))?;
        Ok(Next::Call(IoCall::child(pins)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ReleaseGuards>, PinnedError> {
        let (pins, guards) = result.child::<PinsOutput, PinnedError>()??;
        match pins.first() {
            Some(true) => Err(PinnedError::Version {
                key: self.key.clone(),
                version_id: self.version_id,
            }),
            _ => Ok(Next::Done(guards)),
        }
    }
}

/// Whether a release of `prefix` may pin a key the purge `scope` deletes, and the key prefix
/// to look for in it.
fn reach<'a>(scope: &'a StoragePurgeScope, prefix: &str) -> Option<&'a str> {
    let wanted = scope.list_prefix().unwrap_or_default();
    (wanted.starts_with(prefix) || prefix.starts_with(wanted)).then_some(wanted)
}

type ScopeTurn = Result<Next<()>, PinnedError>;

/// Refuses a file, prefix or bucket purge while a release pins a version in it.
#[derive(Debug, PartialEq)]
pub struct RefuseScopeOperation {
    place: PinPlace,
    auth: AuthContext,
    purge: StoragePurgeScope,
    /// The releases still to look through, once they were listed.
    releases: Option<VecDeque<ValidRelease>>,
}

impl RefuseScopeOperation {
    pub fn new(scope: BucketScope<'_>, auth: &AuthContext, purge: &StoragePurgeScope) -> Self {
        Self {
            place: PinPlace::of(scope),
            auth: auth.clone(),
            purge: purge.clone(),
            releases: None,
        }
    }

    fn listed(&mut self, output: MembersOutput) -> ScopeTurn {
        if !output.complete {
            return Err(PinnedError::Unchecked(
                "a release crate could not be read".to_string(),
            ));
        }
        self.releases = Some(output.releases.into());
        self.next_release()
    }

    /// Looks for a file of the scope in the next release that can reach it.
    fn next_release(&mut self) -> ScopeTurn {
        let place = self.place.clone();
        let auth = AuthContext {
            path_restrictions: None,
            ..self.auth.clone()
        };
        let releases = self.releases.as_mut();
        let releases = releases.ok_or_else(|| unchecked("the releases were not listed"))?;
        while let Some(release) = releases.pop_front() {
            let Some(wanted) = reach(&self.purge, &release.prefix) else {
                continue;
            };
            let view = ReleaseViewOperation::new(ViewConfig {
                realm_id: place.realm_id,
                node_id: place.node_id,
                bucket: place.bucket,
                info: place.info,
                auth,
                key: release.key,
                version_id: release.version_id,
                page: FilePage {
                    prefix: wanted.to_string(),
                    limit: 1,
                    ..FilePage::default()
                },
            });
            return Ok(Next::Call(IoCall::child(view)));
        }
        Ok(Next::Done(()))
    }

    fn viewed(&mut self, view: Result<ReleaseView, ViewError>) -> ScopeTurn {
        let bucket = &self.place.bucket;
        let pins = match view {
            Ok(view) => {
                (view.files.first()).is_some_and(|file| self.purge.matches_key(bucket, &file.key))
            }
            Err(ViewError::Missing | ViewError::NoRelease) => false,
            Err(error) => return Err(unchecked(error)),
        };
        if pins {
            return Err(PinnedError::Scope);
        }
        self.next_release()
    }
}

impl Operation for RefuseScopeOperation {
    type Output = ();
    type Error = PinnedError;

    fn first(&mut self) -> ScopeTurn {
        if self.place.info.distribution.is_some() {
            return Ok(Next::Done(()));
        }
        let retained = RetainedReleases {
            place: self.place.clone(),
            auth: self.auth.clone(),
            queries: Vec::new(),
            loaded: false,
        };
        Ok(Next::Call(IoCall::child(retained)))
    }

    fn next(&mut self, result: IoResult) -> ScopeTurn {
        match self.releases {
            None => self.listed(result.child::<MembersOutput, PinnedError>()??),
            Some(_) => self.viewed(result.child::<ReleaseView, ViewError>()?),
        }
    }
}

#[cfg(test)]
#[path = "pinned_tests.rs"]
mod tests;
