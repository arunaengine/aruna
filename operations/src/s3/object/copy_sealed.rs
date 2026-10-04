//! Copies an encrypted object within its bucket by publishing a new version of the same
//! archive. No content is read, so the copy also works while the bucket is locked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::records::{
    HeadAliasContext, add_index_effect, owner_write_effect, write_head_effect, write_version_effect,
};
use crate::node::usage_stats::{QuotaGate, QuotaGateError, UsageCounterUpdate, UsageUpdateError};
use crate::s3::purge_fence::{PurgeFenceError, check_write_fence, write_fence_read};
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    ArchiveKey, BlobHeadKey, BlobVersion, BlobVersionState, CopyOwner, CurrentVersionPointer,
    VersionKey,
};
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{Effects, GroupId, TxnId};
use smallvec::smallvec;
use std::collections::HashMap;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error, PartialEq)]
pub enum SealedCopyError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    PurgeFence(#[from] PurgeFenceError),
    #[error(transparent)]
    Usage(#[from] UsageUpdateError),
    #[error(transparent)]
    Quota(#[from] QuotaGateError),
    #[error("group storage quota exceeded: {usage} bytes would exceed limit of {limit} bytes")]
    QuotaExceeded { limit: u64, usage: u64 },
    #[error("The specified version does not exist.")]
    NoSuchVersion,
    #[error("the source version no longer uses the archive the copy was asked for")]
    SourceChanged,
    #[error("a copy of a governed encrypted object needs its plaintext")]
    Governed,
    #[error("Invalid operation state")]
    InvalidState,
    #[error("operation did not finish")]
    NotFinished,
}

/// One same-bucket copy of a sealed version, described by what HEAD returned for it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedCopyInput {
    pub bucket: String,
    pub source_key: String,
    pub source_version_id: Ulid,
    /// The archive HEAD described; the source version must still use exactly it.
    pub archive: ArchiveKey,
    /// Original size of the object, for logical usage and quota.
    pub size: u64,
    pub dest_key: String,
    /// Replacement metadata; `None` keeps the source's.
    pub metadata: Option<HashMap<String, String>>,
    pub user_id: UserId,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub quota_ceiling: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedCopyResult {
    pub version_id: Ulid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Step {
    Init,
    Start,
    Fence,
    ReadSource,
    ReadLiveness,
    WriteHead,
    WriteIndex,
    WriteVersion,
    WriteOwner,
    Quota,
    Usage,
    Commit,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct SealedCopyOperation {
    input: SealedCopyInput,
    step: Step,
    txn_id: Option<TxnId>,
    version_id: Ulid,
    version: Option<BlobVersion>,
    existing: Option<CurrentVersionPointer>,
    was_live: bool,
    quota: Option<QuotaGate>,
    usage: Option<UsageCounterUpdate>,
    output: Option<Result<SealedCopyResult, SealedCopyError>>,
}

impl SealedCopyOperation {
    pub fn new(input: SealedCopyInput) -> Self {
        Self {
            input,
            step: Step::Init,
            txn_id: None,
            version_id: Ulid::generate(),
            version: None,
            existing: None,
            was_live: false,
            quota: None,
            usage: None,
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<SealedCopyError>) -> Effects {
        self.step = Step::Error;
        self.output = Some(Err(error.into()));
        self.abort()
    }

    fn txn(&self) -> Result<TxnId, SealedCopyError> {
        self.txn_id.ok_or(SealedCopyError::InvalidState)
    }

    fn dest_version(&self) -> VersionKey {
        VersionKey::new(&self.input.bucket, &self.input.dest_key, self.version_id)
    }

    fn alias_context(&self) -> HeadAliasContext {
        HeadAliasContext::new(
            self.input.realm_id,
            self.input.group_id,
            self.input.node_id,
            self.input.bucket.clone(),
            self.input.dest_key.clone(),
        )
    }

    /// Reads the exact source version and the destination head in the transaction.
    fn read_source(&mut self) -> Result<Effects, SealedCopyError> {
        let source = VersionKey::new(
            &self.input.bucket,
            &self.input.source_key,
            self.input.source_version_id,
        );
        let head = BlobHeadKey::new(&self.input.bucket, &self.input.dest_key);
        self.step = Step::ReadSource;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    BLOB_VERSIONS_KEYSPACE.to_string(),
                    source.to_bytes()?.into()
                ),
                (BLOB_HEAD_KEYSPACE.to_string(), head.to_bytes()?.into()),
            ],
            txn_id: self.txn_id,
        })])
    }

    /// The new version keeps the source's content state, known or pending, on the same archive.
    fn source_read(&mut self, event: Event) -> Result<Effects, SealedCopyError> {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return Err(SealedCopyError::InvalidState);
        };
        let [(_, source), (_, head)] =
            <[_; 2]>::try_from(values).map_err(|_| SealedCopyError::InvalidState)?;
        let source = source.ok_or(SealedCopyError::NoSuchVersion)?;
        let source = BlobVersion::from_bytes(source.as_ref())?;
        if !source.placement_policies.is_empty() {
            return Err(SealedCopyError::Governed);
        }
        let state = match source.state {
            BlobVersionState::Materialized {
                blob_hash,
                backend,
                encoding: encoding @ EncodingClass::Pithos { .. },
                ..
            } if backend == self.input.archive.backend => BlobVersionState::Materialized {
                blob_hash,
                backend,
                encoding,
                source: None,
            },
            BlobVersionState::PendingContent { archive, .. } if archive == self.input.archive => {
                BlobVersionState::PendingContent {
                    archive,
                    source: None,
                }
            }
            _ => return Err(SealedCopyError::SourceChanged),
        };
        let metadata = self.input.metadata.clone().unwrap_or(source.metadata);
        let mut version = BlobVersion::deleted(SystemTime::now(), self.input.user_id);
        version.state = state;
        self.version = Some(version.with_metadata(metadata));
        self.existing = head
            .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
            .transpose()?;
        let Some(pointer) = self.existing.as_ref() else {
            return self.write_head();
        };
        let key = VersionKey::new(&self.input.bucket, &self.input.dest_key, pointer.version_id);
        self.step = Step::ReadLiveness;
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.to_bytes()?.into(),
            txn_id: self.txn_id,
        })])
    }

    fn liveness_read(&mut self, event: Event) -> Result<Effects, SealedCopyError> {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return Err(SealedCopyError::InvalidState);
        };
        self.was_live = value
            .map(|value| BlobVersion::from_bytes(value.as_ref()))
            .transpose()?
            .is_some_and(|version| !version.is_deleted());
        self.write_head()
    }

    fn write_head(&mut self) -> Result<Effects, SealedCopyError> {
        let pointer = CurrentVersionPointer::next_for(self.existing.as_ref(), self.version_id)?;
        self.step = Step::WriteHead;
        Ok(smallvec![write_head_effect(
            &self.alias_context(),
            pointer,
            self.txn_id
        )?])
    }

    /// A known content hash gets its path index; a pending archive has none yet.
    fn write_index(&mut self) -> Result<Effects, SealedCopyError> {
        let version = self.version.as_ref().ok_or(SealedCopyError::InvalidState)?;
        let Some(hash) = version.state.blob_hash().copied() else {
            return self.write_version();
        };
        self.step = Step::WriteIndex;
        let context = self.alias_context();
        Ok(smallvec![add_index_effect(
            &context,
            hash,
            self.version_id,
            self.txn_id
        )?])
    }

    fn write_version(&mut self) -> Result<Effects, SealedCopyError> {
        let version = self.version.as_ref().ok_or(SealedCopyError::InvalidState)?;
        let effect = write_version_effect(&self.dest_version(), version, self.txn_id)?;
        self.step = Step::WriteVersion;
        Ok(smallvec![effect])
    }

    /// The new version owns the archive too, so neither alias frees it while the other lives.
    fn write_owner(&mut self) -> Result<Effects, SealedCopyError> {
        let owner = CopyOwner::new(self.input.archive.clone(), self.dest_version());
        self.step = Step::WriteOwner;
        Ok(smallvec![owner_write_effect(&owner, self.txn_id)?])
    }

    /// The copy adds a logical object but no physical bytes: the archive is already credited.
    fn start_quota(&mut self) -> Result<Effects, SealedCopyError> {
        let txn_id = self.txn()?;
        let delta = UsageDelta {
            objects: i128::from(!self.was_live),
            logical_bytes: i128::from(self.input.size),
            ..Default::default()
        };
        self.usage = Some(UsageCounterUpdate::for_group(self.input.group_id, delta));
        match self.input.quota_ceiling.filter(|_| self.input.size > 0) {
            Some(ceiling) => {
                let mut gate = QuotaGate::new_for_realm(
                    ceiling,
                    self.input.size,
                    self.input.group_id,
                    self.input.node_id,
                    self.input.realm_id,
                );
                self.step = Step::Quota;
                let effects = gate.start(txn_id);
                self.quota = Some(gate);
                Ok(effects)
            }
            None => self.start_usage(),
        }
    }

    fn quota_step(&mut self, event: Event) -> Result<Effects, SealedCopyError> {
        let txn_id = self.txn()?;
        let gate = self.quota.as_mut().ok_or(SealedCopyError::InvalidState)?;
        if let Some(effects) = gate.step(event, txn_id)? {
            return Ok(effects);
        }
        if gate.is_exceeded() {
            let (limit, usage) = (gate.ceiling(), gate.projected_usage());
            return Err(SealedCopyError::QuotaExceeded { limit, usage });
        }
        self.start_usage()
    }

    fn start_usage(&mut self) -> Result<Effects, SealedCopyError> {
        let txn_id = self.txn()?;
        let update = self.usage.as_mut().ok_or(SealedCopyError::InvalidState)?;
        if update.is_noop() {
            return self.commit();
        }
        self.step = Step::Usage;
        Ok(update.start(txn_id))
    }

    fn usage_step(&mut self, event: Event) -> Result<Effects, SealedCopyError> {
        let txn_id = self.txn()?;
        let update = self.usage.as_mut().ok_or(SealedCopyError::InvalidState)?;
        match update.step(event, txn_id)? {
            Some(effects) => Ok(effects),
            None => self.commit(),
        }
    }

    fn commit(&mut self) -> Result<Effects, SealedCopyError> {
        let txn_id = self.txn()?;
        self.step = Step::Commit;
        Ok(smallvec![Effect::Storage(
            StorageEffect::CommitTransaction { txn_id }
        )])
    }

    fn advance(&mut self, event: Event) -> Result<Effects, SealedCopyError> {
        let written = matches!(event, Event::Storage(StorageEvent::WriteResult { .. }));
        match (self.step, event) {
            (Step::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.step = Step::Fence;
                Ok(smallvec![write_fence_read(&self.input.bucket, self.txn_id)])
            }
            (Step::Fence, event) => {
                check_write_fence(event, &self.input.bucket, &self.input.dest_key)?;
                self.read_source()
            }
            (Step::ReadSource, event) => self.source_read(event),
            (Step::ReadLiveness, event) => self.liveness_read(event),
            (Step::WriteHead, _) if written => self.write_index(),
            (Step::WriteIndex, _) if written => self.write_version(),
            (Step::WriteVersion, _) if written => self.write_owner(),
            (Step::WriteOwner, _) if written => self.start_quota(),
            (Step::Quota, event) => self.quota_step(event),
            (Step::Usage, event) => self.usage_step(event),
            (Step::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.step = Step::Finish;
                self.output = Some(Ok(SealedCopyResult {
                    version_id: self.version_id,
                }));
                Ok(smallvec![])
            }
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            _ => Err(SealedCopyError::InvalidState),
        }
    }
}

impl Operation for SealedCopyOperation {
    type Output = SealedCopyResult;
    type Error = SealedCopyError;

    fn start(&mut self) -> Effects {
        self.step = Step::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.advance(event) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.step, Step::Finish | Step::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(SealedCopyError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
#[path = "copy_sealed_tests.rs"]
mod tests;
