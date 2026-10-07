//! Moves one local version to the stored format of its bucket's encryption transition. The
//! adapter rewrites the copy; publication needs the same version, source copy and target plan.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::cleanup::schedule_cleanup_effect;
use crate::blob::managed_copy::{ManagedCopyError, check_serveable, read_effect};
use crate::blob::records::{blob_location_read, owner_delete_effect, read_version_effect};
use crate::node::usage_stats::{QuotaGate, StoredDelta, UsageCounterUpdate, UsageUpdateError};
use aruna_core::NodeId;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    BUCKET_ENCRYPTION_KEYSPACE, COPY_OWNER_KEYSPACE, MANAGED_COPY_KEYSPACE,
    TRANSITION_CLEANUP_KEYSPACE, TRANSITION_KEYSPACE,
};
use aruna_core::operation::Operation;
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
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use smallvec::smallvec;
use std::time::SystemTime;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RewriteState {
    Init,
    ReadVersion,
    ReadLocation,
    ReadEnvelope,
    KeepEnvelope,
    CreateEnvelope,
    Admit,
    Rewrite,
    StartTransaction,
    ReadSettings,
    CheckVersion,
    CheckCopy,
    ReadTarget,
    WriteRows,
    DropOwner,
    DropEnvelope,
    Quota,
    UpdateUsage,
    Commit,
    Abort,
    Release,
    Finish,
    Error,
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
    #[error("unexpected event in state {state:?}: {received:?}")]
    InvalidStateEvent {
        state: RewriteState,
        received: Box<Event>,
    },
}

/// Moves one version of a bucket in a transition to a copy in the target format.
#[derive(Debug, PartialEq)]
pub struct RewriteVersionOperation {
    version_key: VersionKey,
    transition: EncryptionTransition,
    now: SystemTime,
    state: RewriteState,
    txn_id: Option<TxnId>,
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
    output: Option<Result<RewriteOutcome, RewriteError>>,
}

impl RewriteVersionOperation {
    pub fn new(version_key: VersionKey, transition: EncryptionTransition, now: SystemTime) -> Self {
        Self {
            version_key,
            transition,
            now,
            state: RewriteState::Init,
            txn_id: None,
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
            output: None,
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

    fn unexpected(&mut self, received: Event) -> Effects {
        let state = self.state;
        self.fail(RewriteError::InvalidStateEvent {
            state,
            received: Box::new(received),
        })
    }

    fn fail(&mut self, error: RewriteError) -> Effects {
        self.output = Some(Err(error));
        self.leave()
    }

    fn end(&mut self, outcome: RewriteOutcome) -> Effects {
        self.output = Some(Ok(outcome));
        self.leave()
    }

    fn leave(&mut self) -> Effects {
        self.lease = None;
        if let Some(txn_id) = self.txn_id.take() {
            self.state = RewriteState::Abort;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        self.release()
    }

    fn release(&mut self) -> Effects {
        match self.new.as_ref() {
            Some(new) => {
                self.state = RewriteState::Release;
                smallvec![Effect::Blob(BlobEffect::ReleaseReservation {
                    id: new.ulid
                })]
            }
            None => self.finish(),
        }
    }

    /// A written copy that no commit owns goes to the cleanup queue, whose reconcile deletes it.
    fn finish(&mut self) -> Effects {
        self.state = match self.output {
            Some(Ok(_)) => RewriteState::Finish,
            _ => RewriteState::Error,
        };
        match (self.new.is_some(), self.owns_row) {
            (true, false) => smallvec![schedule_cleanup_effect()],
            _ => smallvec![],
        }
    }

    fn read_value<T>(
        &mut self,
        event: Event,
        parse: impl FnOnce(&[u8]) -> Result<T, ConversionError>,
    ) -> Result<Option<T>, Effects> {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return Err(self.unexpected(event));
        };
        match value.map(|value| parse(value.as_ref())).transpose() {
            Ok(parsed) => Ok(parsed),
            Err(error) => Err(self.fail(error.into())),
        }
    }

    fn handle_version(&mut self, event: Event) -> Effects {
        let version = match self.read_value(event, BlobVersion::from_bytes) {
            Ok(Some(version)) => version,
            Ok(None) => return self.end(RewriteOutcome::Skipped),
            Err(effects) => return effects,
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
        smallvec![blob_location_read(&key, None)]
    }

    /// A sealed source is read only under a lease admitted for its archive.
    fn handle_location(&mut self, event: Event) -> Effects {
        let old = match self.read_value(event, BackendLocation::from_bytes) {
            Ok(Some(old)) => old,
            Ok(None) => return self.end(RewriteOutcome::Skipped),
            Err(effects) => return effects,
        };
        if old.staging || old.partial || !self.moves(&old) {
            // A sealed archive in another format or generation waits for its transition.
            let sealed = old.staging || old.partial || old.format.bucket_key().is_some();
            return self.end(self.waits(sealed));
        }
        self.old = Some(old);
        self.read_envelope()
    }

    fn admit(&mut self, key: BucketKeyRef, archive: ArchiveKey) -> Effects {
        self.state = RewriteState::Admit;
        smallvec![Effect::Blob(BlobEffect::AdmitRead { key, archive })]
    }

    fn handle_admit(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::ReadAdmitted { lease }) => {
                self.lease = Some(lease);
                self.rewrite()
            }
            Event::Blob(BlobEvent::Error(BlobError::BucketKey(BucketKeyError::Locked(_)))) => {
                self.end(RewriteOutcome::AwaitingKey)
            }
            Event::Blob(BlobEvent::Error(error)) => self.fail(error.into()),
            other => self.unexpected(other),
        }
    }

    /// A rotation keeps the sealed blocks of an archive; every other change encodes again.
    fn rewrite(&mut self) -> Effects {
        let Some(old) = self.old.clone() else {
            return self.fail(RewriteError::NotFinished);
        };
        let target = self.transition.target;
        let sealed = old.format.bucket_key().is_some() && target.plan.is_some();
        let resolved = ResolvedBackend::new(old.backend.clone(), old.storage_class.clone())
            .with_compression(target.compression)
            .with_encryption(target.plan);
        self.state = RewriteState::Rewrite;
        smallvec![Effect::Blob(BlobEffect::RewriteCopy {
            bucket: self.version_key.bucket.clone(),
            key: self.version_key.key.clone(),
            source: old,
            lease: self.lease.take().map(Box::new),
            target: Box::new(resolved),
            grants_only: sealed && self.transition.kind == TransitionKind::Rotate,
            object: (self.envelope.as_ref())
                .or(self.pending.as_ref().map(|pending| &pending.source))
                .map(|envelope| Box::new(envelope.context.public_key)),
        })]
    }

    fn handle_rewritten(&mut self, event: Event) -> Effects {
        let new = match event {
            Event::Blob(BlobEvent::CopyRewritten { location }) => location,
            Event::Blob(BlobEvent::Error(BlobError::BucketKey(BucketKeyError::Locked(_)))) => {
                return self.end(RewriteOutcome::AwaitingKey);
            }
            Event::Blob(BlobEvent::Error(error)) => return self.fail(error.into()),
            other => return self.unexpected(other),
        };
        let same = self.old.as_ref().is_some_and(|old| {
            old.get_blake3() == new.get_blake3() && old.blob_size == new.blob_size
        });
        self.new = Some(new);
        if !same {
            return self.fail(RewriteError::ContentMismatch);
        }
        self.state = RewriteState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected(event);
        };
        self.txn_id = Some(txn_id);
        self.state = RewriteState::ReadSettings;
        let bucket: Key = self.version_key.bucket.as_bytes().to_vec().into();
        let mut reads = vec![
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket.clone()),
            (TRANSITION_KEYSPACE.to_string(), bucket),
        ];
        match self.envelope_reads() {
            Ok(envelope) => reads.extend(envelope),
            Err(error) => return self.fail(error),
        }
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })]
    }

    /// Publishes only while the bucket still writes this transition's target and the
    /// transition was not replaced.
    fn handle_settings(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected(event);
        };
        let Some(([(_, settings), (_, transition)], envelope)) = values.split_first_chunk() else {
            return self.fail(RewriteError::NotFinished);
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
            (Err(error), _) | (_, Err(error)) => return self.fail(error.into()),
        };
        // A re-key comes back for a version that changed before publication.
        if !current {
            return self.end(self.waits(true));
        }
        match self.check_envelope(envelope) {
            Ok(true) => {}
            Ok(false) => return self.end(self.waits(true)),
            Err(error) => return self.fail(error),
        }
        match read_version_effect(&self.version_key, self.txn_id) {
            Ok(effect) => {
                self.state = RewriteState::CheckVersion;
                smallvec![effect]
            }
            Err(error) => self.fail(error.into()),
        }
    }

    fn handle_check(&mut self, event: Event) -> Effects {
        let current = match self.read_value(event, BlobVersion::from_bytes) {
            Ok(current) => current,
            Err(effects) => return effects,
        };
        if current != self.version {
            return self.end(self.waits(true));
        }
        let governed = (self.version.as_ref()).is_some_and(|v| !v.placement_policies.is_empty());
        let Some(old) = self.old.as_ref().filter(|_| governed) else {
            return self.read_target();
        };
        let key = ManagedCopyKey::new(self.version_key.clone(), old.backend.clone());
        match read_effect(&key, self.txn_id) {
            Ok(effect) => {
                self.state = RewriteState::CheckCopy;
                smallvec![effect]
            }
            Err(error) => self.fail(error.into()),
        }
    }

    /// A governed version moves only while its registration names the old copy and may serve.
    fn handle_copy(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let copy = match check_serveable(value.as_ref().map(|value| value.as_ref())) {
            Ok(copy) => copy,
            Err(error) => return self.fail(error.into()),
        };
        if !(self.old.as_ref()).is_some_and(|old| copy.location.same_object(old)) {
            return self.fail(ManagedCopyError::Mismatched.into());
        }
        self.copy = Some(copy);
        self.read_target()
    }

    fn read_target(&mut self) -> Effects {
        match self.new.as_ref().map(BackendLocation::location_key) {
            Some(Ok(key)) => {
                self.state = RewriteState::ReadTarget;
                smallvec![blob_location_read(&key, self.txn_id)]
            }
            Some(Err(error)) => self.fail(error.into()),
            None => self.fail(RewriteError::NotFinished),
        }
    }
}

impl Operation for RewriteVersionOperation {
    type Output = RewriteOutcome;
    type Error = RewriteError;

    fn start(&mut self) -> Effects {
        match read_version_effect(&self.version_key, None) {
            Ok(effect) => {
                self.state = RewriteState::ReadVersion;
                smallvec![effect]
            }
            Err(error) => self.fail(error.into()),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event
            && !matches!(self.state, RewriteState::Commit | RewriteState::Abort)
        {
            let error = error.clone();
            return self.fail(error.into());
        }
        match self.state {
            RewriteState::Init => self.start(),
            RewriteState::ReadVersion => self.handle_version(event),
            RewriteState::ReadLocation => self.handle_location(event),
            RewriteState::ReadEnvelope => self.handle_envelope(event),
            RewriteState::KeepEnvelope => self.handle_kept(event),
            RewriteState::CreateEnvelope => self.handle_created(event),
            RewriteState::Admit => self.handle_admit(event),
            RewriteState::Rewrite => self.handle_rewritten(event),
            RewriteState::StartTransaction => self.handle_started(event),
            RewriteState::ReadSettings => self.handle_settings(event),
            RewriteState::CheckVersion => self.handle_check(event),
            RewriteState::CheckCopy => self.handle_copy(event),
            RewriteState::ReadTarget => self.handle_target(event),
            RewriteState::WriteRows => self.handle_rows(event),
            RewriteState::DropOwner => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => self.drop_envelope(),
                other => self.unexpected(other),
            },
            RewriteState::DropEnvelope => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => self.update_usage(),
                other => self.unexpected(other),
            },
            RewriteState::Quota => self.handle_quota(event),
            RewriteState::UpdateUsage => self.handle_usage(event),
            RewriteState::Commit => self.handle_commit(event),
            RewriteState::Abort => match event {
                Event::Storage(StorageEvent::TransactionAborted { .. })
                | Event::Storage(StorageEvent::Error { .. }) => self.release(),
                other => self.unexpected(other),
            },
            RewriteState::Release => match event {
                Event::Blob(BlobEvent::ReservationReleased { .. })
                | Event::Blob(BlobEvent::Error(_)) => self.finish(),
                other => self.unexpected(other),
            },
            RewriteState::Finish | RewriteState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RewriteState::Finish | RewriteState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        let complete = self.is_complete();
        let output = self.output.filter(|_| complete);
        output.unwrap_or(Err(RewriteError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        self.lease = None;
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}
#[path = "../rewrite_rows.rs"]
mod rows;

#[path = "../rewrite_abe.rs"]
mod abe;

#[cfg(test)]
#[path = "../rewrite_tests.rs"]
mod tests;
