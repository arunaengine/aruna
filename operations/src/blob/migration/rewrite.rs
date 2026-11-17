//! Moves one local version to the stored format of its bucket's encryption transition. The
//! adapter rewrites the copy; publication needs the same version, source copy and target plan.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::cleanup_rows::schedule_cleanup_effect;
use crate::distribution::inventory::{StagedCall, outside_answer};
use crate::s3::object::put::abe::keyed;
use crate::s3::usage::{QuotaGate, StoredDelta, UsageCounterUpdate, UsageUpdateError};
use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    BUCKET_ENCRYPTION_KEYSPACE, COPY_OWNER_KEYSPACE, MANAGED_COPY_KEYSPACE,
    TRANSITION_CLEANUP_KEYSPACE, TRANSITION_KEYSPACE,
};
use aruna_core::managed_copy::{ManagedCopyError, check_serveable};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::identity::realm::{QuotaConfig, RealmId};
use aruna_core::structs::storage::abe::{ObjectEnvelope, PendingCopy};
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobVersion, BlobVersionState, CopyOwner, ManagedCopyKey,
    ManagedCopyRecord, ResolvedBackend, VersionKey,
};
use aruna_core::structs::storage::cleanup::{ReclaimCandidate, ReclaimCandidateKey};
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyError, BucketKeyRef, ReadLease,
};
use aruna_core::structs::storage::transition::{EncryptionTransition, TransitionKind, cleanup_key};
use aruna_core::types::{GroupId, Key, Value};
use std::time::SystemTime;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RewriteState {
    ReadVersion,
    ReadLocation,
    ReadEnvelope,
    KeepEnvelope,
    CreateEnvelope,
    Admit,
    Rewrite,
    ReadSettings,
    CheckVersion,
    CheckCopy,
    ReadTarget,
    WriteRows,
    DropOwner,
    DropEnvelope,
    Quota,
    UpdateUsage,
}

/// What happened to one version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RewriteOutcome {
    /// The version now names a copy in the target format.
    Moved,
    /// Nothing to do: already moved, not materialized, or changed meanwhile.
    Skipped,
    /// The source copy needs a key generation that is locked on this node.
    AwaitingKey,
    /// A re-key must come back: the version is still pending or changed before publication.
    Unfinished,
}

#[derive(Debug, Error, PartialEq)]
pub enum RewriteError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error(transparent)]
    Usage(#[from] UsageUpdateError),
    #[error(transparent)]
    ManagedCopy(#[from] ManagedCopyError),
    #[error("the rewritten copy does not match the original bytes")]
    ContentMismatch,
    #[error("version rewrite did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: {received}")]
    InvalidStateEvent {
        state: RewriteState,
        received: &'static str,
    },
}

impl From<IoError> for RewriteError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

type Step = Result<Next<RewriteOutcome>, RewriteError>;

/// A read outside the publishing transaction, so the rewrite never holds it open.
fn outside(call: IoCall) -> IoCall {
    StagedCall::Outside(call).into_call()
}

fn parsed<T>(
    value: Option<Value>,
    parse: impl FnOnce(&[u8]) -> Result<T, ConversionError>,
) -> Result<Option<T>, RewriteError> {
    Ok(value.map(|value| parse(value.as_ref())).transpose()?)
}

/// Moves one version of a bucket in a transition to a copy in the target format.
#[derive(Debug, PartialEq)]
pub struct RewriteVersionOperation {
    version_key: VersionKey,
    transition: EncryptionTransition,
    now: SystemTime,
    state: RewriteState,
    version: Option<BlobVersion>,
    old: Option<BackendLocation>,
    new: Option<BackendLocation>,
    lease: Option<ReadLease>,
    copy: Option<ManagedCopyRecord>,
    owns_row: bool,
    usage: Option<UsageCounterUpdate>,
    /// The version's envelope rows read before the rewrite, when the transition moves them.
    envelope_rows: Option<abe::EnvelopeRows>,
    /// The envelope to publish: a new one for a new generation, else the version's own.
    envelope: Option<ObjectEnvelope>,
    /// The pending copy row that a re-encoding in the same generation points at the new copy.
    pending: Option<PendingCopy>,
    /// The bucket's group and the usage charge of the replaced envelope rows.
    charge: Option<(GroupId, u64)>,
    /// Realm quota and origin; none leaves the group unlimited.
    quota: Option<(QuotaConfig, RealmId, NodeId)>,
    gate: Option<QuotaGate>,
    deletes: Vec<(String, Key)>,
    /// A scoped re-key of the current generation, outside any transition.
    rekey: bool,
}

impl RewriteVersionOperation {
    pub fn new(version_key: VersionKey, transition: EncryptionTransition, now: SystemTime) -> Self {
        Self {
            version_key,
            transition,
            now,
            state: RewriteState::ReadVersion,
            version: None,
            old: None,
            new: None,
            lease: None,
            copy: None,
            owns_row: false,
            usage: None,
            envelope_rows: None,
            envelope: None,
            pending: None,
            charge: None,
            quota: None,
            gate: None,
            deletes: Vec::new(),
            rekey: false,
        }
    }

    /// Gives a version of the target generation a new object key and envelope; `transition`
    /// names that generation and is not stored.
    pub fn rekey(mut self) -> Self {
        self.rekey = true;
        self
    }

    /// A re-key moves only archives already in the target format; a transition the others.
    fn moves(&self, old: &BackendLocation) -> bool {
        let target = self.transition.target.plan.map(|plan| plan.key);
        match self.rekey {
            true => {
                old.format
                    .bucket_key()
                    .is_some_and(|key| Some(key) == target)
                    && !self.transition.needs(old)
            }
            false => self.transition.needs(old),
        }
    }

    pub fn with_quota(mut self, quota: QuotaConfig, realm: RealmId, node: NodeId) -> Self {
        self.quota = Some((quota, realm, node));
        self
    }

    /// A re-key waits for a pending version instead of skipping it.
    fn waits(&self, pending: bool) -> RewriteOutcome {
        match self.rekey && pending {
            true => RewriteOutcome::Unfinished,
            false => RewriteOutcome::Skipped,
        }
    }

    fn unexpected(&self, received: Event) -> RewriteError {
        RewriteError::InvalidStateEvent {
            state: self.state,
            received: received.label(),
        }
    }

    /// Ends with `outcome` once the transaction commits.
    fn end(&mut self, outcome: RewriteOutcome) -> Step {
        self.lease = None;
        let calls = self.leave();
        Ok(Next::Then {
            output: outcome,
            calls,
        })
    }

    /// Releases a written copy; one that no commit owns goes to the cleanup queue, whose
    /// reconcile deletes it.
    fn leave(&self) -> Vec<IoCall> {
        let Some(new) = self.new.as_ref() else {
            return Vec::new();
        };
        let release = Effect::Blob(BlobEffect::ReleaseReservation { id: new.ulid });
        let mut calls = vec![IoCall::from(release)];
        if !self.owns_row {
            calls.push(IoCall::from(schedule_cleanup_effect()));
        }
        calls
    }

    fn handle_version(&mut self, value: Option<Value>) -> Step {
        let Some(version) = parsed(value, BlobVersion::from_bytes)? else {
            return self.end(RewriteOutcome::Skipped);
        };
        let key = match &version.state {
            BlobVersionState::Materialized { .. } => version.location_key(),
            _ => None,
        };
        let Some(key) = key else {
            // Pending content keeps its old envelope until promotion.
            let pending = matches!(version.state, BlobVersionState::PendingContent { .. });
            return self.end(self.waits(pending));
        };
        self.version = Some(version);
        self.state = RewriteState::ReadLocation;
        let read = read_row(BLOB_LOCATIONS_KEYSPACE, key.to_bytes());
        Ok(Next::Call(outside(read)))
    }

    /// A sealed source is read only under a lease admitted for its archive.
    fn handle_location(&mut self, value: Option<Value>) -> Step {
        let Some(old) = parsed(value, BackendLocation::from_bytes)? else {
            return self.end(RewriteOutcome::Skipped);
        };
        if old.staging || old.partial || !self.moves(&old) {
            // A sealed archive in another format or generation waits for its transition.
            let sealed = old.staging || old.partial || old.format.bucket_key().is_some();
            return self.end(self.waits(sealed));
        }
        self.old = Some(old);
        self.read_envelope()
    }

    fn admit(&mut self, key: BucketKeyRef, archive: ArchiveKey) -> Step {
        self.state = RewriteState::Admit;
        let admit = Effect::Blob(BlobEffect::AdmitRead { key, archive });
        Ok(Next::Call(admit.into()))
    }

    fn handle_admit(&mut self, event: Event) -> Step {
        match event {
            Event::Blob(BlobEvent::ReadAdmitted { lease }) => {
                self.lease = Some(lease);
                self.rewrite()
            }
            Event::Blob(BlobEvent::Error(BlobError::BucketKey(BucketKeyError::Locked(_)))) => {
                self.end(RewriteOutcome::AwaitingKey)
            }
            Event::Blob(BlobEvent::Error(error)) => Err(error.into()),
            other => Err(self.unexpected(other)),
        }
    }

    /// A rotation keeps the sealed blocks of an archive; every other change encodes again.
    fn rewrite(&mut self) -> Step {
        let old = self.old.clone().ok_or(RewriteError::NotFinished)?;
        let target = self.transition.target;
        let sealed = old.format.bucket_key().is_some() && target.plan.is_some();
        let resolved = ResolvedBackend::new(old.backend.clone(), old.storage_class.clone())
            .with_compression(target.compression)
            .with_encryption(target.plan);
        self.state = RewriteState::Rewrite;
        let rewrite = Effect::Blob(BlobEffect::RewriteCopy {
            bucket: self.version_key.bucket.clone(),
            key: self.version_key.key.clone(),
            source: old,
            lease: self.lease.take().map(Box::new),
            target: Box::new(resolved),
            grants_only: sealed && self.transition.kind == TransitionKind::Rotate,
            object: (self.envelope.as_ref())
                .or(self.pending.as_ref().map(|pending| &pending.source))
                .map(|envelope| Box::new(envelope.context.public_key)),
        });
        Ok(Next::Call(rewrite.into()))
    }

    /// The first publication read opens the transaction.
    fn handle_rewritten(&mut self, event: Event) -> Step {
        let new = match event {
            Event::Blob(BlobEvent::CopyRewritten { location }) => location,
            Event::Blob(BlobEvent::Error(BlobError::BucketKey(BucketKeyError::Locked(_)))) => {
                return self.end(RewriteOutcome::AwaitingKey);
            }
            Event::Blob(BlobEvent::Error(error)) => return Err(error.into()),
            other => return Err(self.unexpected(other)),
        };
        let same = self.old.as_ref().is_some_and(|old| {
            old.get_blake3() == new.get_blake3() && old.blob_size == new.blob_size
        });
        self.new = Some(new);
        if !same {
            return Err(RewriteError::ContentMismatch);
        }
        self.state = RewriteState::ReadSettings;
        let bucket: Key = self.version_key.bucket.as_bytes().to_vec().into();
        let mut reads = vec![
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket.clone()),
            (TRANSITION_KEYSPACE.to_string(), bucket),
        ];
        reads.extend(self.envelope_reads()?);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// Publishes only while the bucket still writes this transition's target and the
    /// transition was not replaced.
    fn handle_settings(&mut self, values: Vec<(Key, Option<Value>)>) -> Step {
        let Some(([(_, settings), (_, transition)], envelope)) = values.split_first_chunk() else {
            return Err(RewriteError::NotFinished);
        };
        let settings = BucketEncryption::from_row(settings.as_ref().map(|row| row.as_ref()));
        let stored = transition
            .as_ref()
            .map(|row| EncryptionTransition::from_bytes(row));
        let current = match (settings, stored.transpose()) {
            (Ok(settings), _) if self.rekey => self.transition.still_current(&settings),
            (Ok(settings), Ok(Some(stored))) => {
                stored.started_at_ms == self.transition.started_at_ms
                    && stored.kind == self.transition.kind
                    && self.transition.still_current(&settings)
            }
            (Ok(_), Ok(None)) => false,
            (Err(error), _) | (_, Err(error)) => return Err(error.into()),
        };
        // A re-key comes back for a version that changed before publication.
        if !current || !self.check_envelope(envelope)? {
            return self.end(self.waits(true));
        }
        self.state = RewriteState::CheckVersion;
        let read = read_row(BLOB_VERSIONS_KEYSPACE, self.version_key.to_bytes()?);
        Ok(Next::Call(read))
    }

    fn handle_check(&mut self, value: Option<Value>) -> Step {
        let current = parsed(value, BlobVersion::from_bytes)?;
        if current != self.version {
            return self.end(self.waits(true));
        }
        let governed = (self.version.as_ref()).is_some_and(|v| !v.placement_policies.is_empty());
        let Some(old) = self.old.as_ref().filter(|_| governed) else {
            return self.read_target();
        };
        let key = ManagedCopyKey::new(self.version_key.clone(), old.backend.clone());
        self.state = RewriteState::CheckCopy;
        Ok(Next::Call(read_row(MANAGED_COPY_KEYSPACE, key.to_bytes()?)))
    }

    /// A governed version moves only while its registration names the old copy and may serve.
    fn handle_copy(&mut self, value: Option<Value>) -> Step {
        let copy = check_serveable(value.as_ref().map(|value| value.as_ref()))?;
        if !(self.old.as_ref()).is_some_and(|old| copy.location.same_object(old)) {
            return Err(ManagedCopyError::Mismatched.into());
        }
        self.copy = Some(copy);
        self.read_target()
    }

    fn read_target(&mut self) -> Step {
        let new = self.new.as_ref().ok_or(RewriteError::NotFinished)?;
        let key = new.location_key()?.to_bytes();
        self.state = RewriteState::ReadTarget;
        Ok(Next::Call(read_row(BLOB_LOCATIONS_KEYSPACE, key)))
    }
}

impl Operation for RewriteVersionOperation {
    type Output = RewriteOutcome;
    type Error = RewriteError;

    fn first(&mut self) -> Step {
        let read = read_row(BLOB_VERSIONS_KEYSPACE, self.version_key.to_bytes()?);
        self.state = RewriteState::ReadVersion;
        Ok(Next::Call(outside(read)))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.state {
            RewriteState::ReadVersion => self.handle_version(outside_answer(result)?.value()?),
            RewriteState::ReadLocation => self.handle_location(outside_answer(result)?.value()?),
            RewriteState::ReadEnvelope => {
                let values = outside_answer(result)?.values()?;
                self.handle_envelope(keyed(&values))
            }
            RewriteState::KeepEnvelope => self.handle_kept(outside_answer(result)?.value()?),
            RewriteState::CreateEnvelope => self.handle_created(result.event()?),
            RewriteState::Admit => self.handle_admit(result.event()?),
            RewriteState::Rewrite => self.handle_rewritten(result.event()?),
            RewriteState::ReadSettings => self.handle_settings(keyed(&result.values()?)),
            RewriteState::CheckVersion => self.handle_check(result.value()?),
            RewriteState::CheckCopy => self.handle_copy(result.value()?),
            RewriteState::ReadTarget => self.handle_target(result.value()?),
            RewriteState::WriteRows => {
                result.done()?;
                self.handle_rows()
            }
            RewriteState::DropOwner => {
                result.done()?;
                self.drop_envelope()
            }
            RewriteState::DropEnvelope => {
                result.done()?;
                self.update_usage()
            }
            RewriteState::Quota => self.handle_quota(result),
            RewriteState::UpdateUsage => self.handle_usage(result),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    /// Nothing committed, so no row owns a written copy.
    fn abort(&mut self) -> Vec<IoCall> {
        self.lease = None;
        self.owns_row = false;
        self.leave()
    }
}
#[path = "../rewrite/rows.rs"]
mod rows;

#[path = "../rewrite/abe.rs"]
mod abe;

#[cfg(test)]
#[path = "../rewrite/tests.rs"]
mod tests;
