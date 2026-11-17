//! Writes the envelopes of same-bucket copies published while their bucket was locked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::document_repository::GetConfigOperation;
use crate::driver::{DriverContext, drive};
use crate::s3::object::put::abe::{abe_reads, envelope_rows, parse_abe};
use crate::s3::usage::QuotaGate;
use crate::s3::usage::RowChange;
use crate::s3::usage::UsageCounterUpdate;
use crate::storage_read::iter_prefix_page;
use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, ConversionError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{
    ABE_COPY_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE,
    S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::realm::{QuotaConfig, RealmId};
use aruna_core::structs::storage::abe::{
    AbeEffect, AbeError, AbeEvent, EnvelopeArchive, ObjectEnvelope, PendingCopy,
};
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobVersion, BlobVersionState, BucketInfo, VersionKey,
};
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyRef};
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{GroupId, Key, KeySpace, Value};
use std::collections::HashMap;
use tracing::warn;
use ulid::Ulid;

/// Pending copies read per page while an unlock completes them.
const COPY_PAGE: usize = 64;
/// Length of the bucket id that prefixes each pending copy row.
const ROW_PREFIX: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyOutcome {
    Completed,
    /// The key locked again; the copy stays pending.
    Locked,
    /// The copy or its pending row is gone.
    Gone,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum State {
    Read,
    Create,
    Check,
    Locate,
    Quota,
    Write,
    Delete,
    Usage,
}

type Step = Result<Next<CopyOutcome>, AbeError>;

/// The values of a read as the keyed rows `parse_abe` takes.
fn keyed(values: &[Option<Value>]) -> Vec<(Key, Option<Value>)> {
    (values.iter())
        .map(|value| (Key::from(Vec::new()), value.clone()))
        .collect()
}

/// Envelopes one pending copy with the current epoch under the publication fence, in one
/// transaction.
#[derive(Debug, PartialEq)]
pub struct CopyEnvelopeOperation {
    version: VersionKey,
    row: Vec<u8>,
    pending: PendingCopy,
    state: State,
    envelope: Option<ObjectEnvelope>,
    /// Metadata, group and location key of the checked version, kept until it is published.
    stored: Option<(HashMap<String, String>, GroupId, Vec<u8>)>,
    usage: Option<UsageCounterUpdate>,
    /// Realm quota and origin; none leaves the group unlimited.
    quota: Option<(QuotaConfig, RealmId, NodeId)>,
    gate: Option<QuotaGate>,
    /// The envelope rows, held while the quota gate runs.
    writes: Vec<(KeySpace, Key, Value)>,
}

impl CopyEnvelopeOperation {
    pub fn new(version: VersionKey, row: Vec<u8>, pending: PendingCopy) -> Self {
        Self {
            version,
            row,
            pending,
            state: State::Read,
            envelope: None,
            stored: None,
            usage: None,
            quota: None,
            gate: None,
            writes: Vec::new(),
        }
    }

    pub fn with_quota(mut self, quota: QuotaConfig, realm: RealmId, node: NodeId) -> Self {
        self.quota = Some((quota, realm, node));
        self
    }

    fn key(&self) -> BucketKeyRef {
        self.pending.source.context.parameters.key
    }

    fn admitted(&mut self, values: Vec<Option<Value>>) -> Step {
        let (parameters, epoch) = parse_abe(&keyed(&values), self.key())?;
        if self.pending.source.context.parameters != parameters {
            return Err(AbeError::Parameters);
        }
        self.state = State::Create;
        let copy = Effect::Blob(BlobEffect::Abe(Box::new(AbeEffect::Copy {
            source: self.pending.source.clone(),
            epoch,
            write_id: Ulid::generate(),
            object_key: self.version.key.clone(),
        })));
        Ok(Next::Call(IoCall::Effect(Box::new(copy))))
    }

    fn created(&mut self, event: Event) -> Step {
        let Event::Blob(event) = event else {
            return Err(AbeError::Crypto);
        };
        match event {
            BlobEvent::Abe(event) => match *event {
                AbeEvent::Envelope(envelope) => self.envelope = Some(envelope),
                _ => return Err(AbeError::Context),
            },
            BlobEvent::Error(BlobError::Abe(AbeError::Required)) => {
                return Ok(Next::Done(CopyOutcome::Locked));
            }
            BlobEvent::Error(BlobError::Abe(error)) => return Err(error),
            _ => return Err(AbeError::Crypto),
        }
        let row = copy_row(self.key().bucket_id, &self.version).map_err(|_| AbeError::Context)?;
        let version = self.version.to_bytes().map_err(|_| AbeError::Context)?;
        let bucket = self.version.bucket.as_bytes().to_vec();
        let mut reads = vec![
            (ABE_COPY_KEYSPACE.to_string(), row.into()),
            (BLOB_VERSIONS_KEYSPACE.to_string(), version.into()),
            (S3_BUCKET_KEYSPACE.to_string(), bucket.clone().into()),
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket.into()),
        ];
        reads.extend(abe_reads(self.key()));
        self.state = State::Check;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn checked(&mut self, values: Vec<Option<Value>>) -> Step {
        let values = keyed(&values);
        let [
            (_, row),
            (_, version),
            (_, bucket),
            (_, settings),
            anchors @ ..,
        ] = values.as_slice()
        else {
            return Err(AbeError::Context);
        };
        let version = version.as_deref().map(BlobVersion::from_bytes).transpose();
        let (Some(row), Ok(Some(version))) = (row, version) else {
            return Ok(Next::Done(CopyOutcome::Gone));
        };
        if row.as_ref() != self.row.as_slice() || version.is_deleted() {
            return Ok(Next::Done(CopyOutcome::Gone));
        }
        let bucket = bucket.as_deref().ok_or(AbeError::Missing)?;
        let bucket = BucketInfo::from_bytes(bucket).map_err(|_| AbeError::Context)?;
        still_active(settings.as_deref(), self.key())?;
        let (parameters, epoch) = parse_abe(anchors, self.key())?;
        let envelope = self.envelope.as_ref().ok_or(AbeError::Context)?;
        envelope.anchored(&parameters, epoch)?;
        let group = bucket.group_id;
        let location = version.location_key().map(|key| key.to_bytes());
        let pending = matches!(
            &version.state,
            BlobVersionState::PendingContent { archive, .. } if *archive == self.pending.archive
        );
        self.stored = Some((
            version.metadata,
            group,
            location.clone().unwrap_or_default(),
        ));
        match (pending, location) {
            (true, _) => self.publish(),
            (false, Some(key)) => {
                self.state = State::Locate;
                Ok(Next::Call(read_row(BLOB_LOCATIONS_KEYSPACE, key)))
            }
            (false, None) => Err(AbeError::Context),
        }
    }

    /// The materialized location must still be the pending source's archive and bucket key.
    fn located(&mut self, value: Option<Value>) -> Step {
        let location = value.as_deref().map(BackendLocation::from_bytes);
        let Some(Ok(location)) = location else {
            return Err(AbeError::Context);
        };
        if ArchiveKey::of(&location) != self.pending.archive
            || location.format.bucket_key() != Some(self.key())
        {
            return Err(AbeError::Context);
        }
        self.publish()
    }

    fn publish(&mut self) -> Step {
        let (Some((metadata, group, location_key)), Some(envelope)) =
            (self.stored.take(), self.envelope.as_ref())
        else {
            return Err(AbeError::Context);
        };
        let archive = EnvelopeArchive {
            archive: self.pending.archive.clone(),
            location_key,
        };
        let limit = RoCrateLimits::default().metadata_bytes;
        let (writes, charge) =
            envelope_rows(envelope, &self.version, &archive, (&metadata, limit))?;
        // The envelope replaces the pending row and its charge.
        let added = i128::from(charge) - self.row.len() as i128;
        let delta = UsageDelta {
            logical_bytes: added,
            ..Default::default()
        };
        let usage = UsageCounterUpdate::for_group(group, delta);
        self.usage = Some(usage.in_bucket(&self.version.bucket, RowChange::Kept));
        let ceiling = self.quota.as_ref().and_then(|(quota, realm, node)| {
            let ceiling = quota.effective_group_ceiling(&group)?;
            Some((ceiling, *realm, *node))
        });
        let (Some((ceiling, realm, node)), true) = (ceiling, added > 0) else {
            self.state = State::Write;
            return Ok(Next::Call(IoCall::Write(writes)));
        };
        let mut gate = QuotaGate::new_for_realm(ceiling, added as u64, group, node, realm);
        let call = gate.start_calls();
        self.gate = Some(gate);
        self.writes = writes;
        self.state = State::Quota;
        Ok(Next::Call(call))
    }

    /// A positive replacement charge must fit the group quota.
    fn quota_step(&mut self, result: IoResult) -> Step {
        let gate = self.gate.as_mut().ok_or(AbeError::Context)?;
        match gate.step_calls(result) {
            Ok(Some(call)) => Ok(Next::Call(call)),
            Ok(None) if gate.is_exceeded() => Err(AbeError::Limit),
            Ok(None) => {
                self.state = State::Write;
                Ok(Next::Call(IoCall::Write(std::mem::take(&mut self.writes))))
            }
            Err(_) => Err(AbeError::Unavailable),
        }
    }

    fn usage_step(&mut self, result: IoResult) -> Step {
        let usage = self.usage.as_mut().ok_or(AbeError::Context)?;
        match usage.step_calls(result) {
            Ok(Some(call)) => Ok(Next::Call(call.into_call())),
            Ok(None) => Ok(Next::Done(CopyOutcome::Completed)),
            Err(_) => Err(AbeError::Unavailable),
        }
    }
}

impl Operation for CopyEnvelopeOperation {
    type Output = CopyOutcome;
    type Error = AbeError;

    fn first(&mut self) -> Step {
        Ok(Next::Call(IoCall::Read(abe_reads(self.key()))))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.state {
            State::Read => self.admitted(result.values()?),
            State::Create => self.created(result.event()?),
            State::Check => self.checked(result.values()?),
            State::Locate => self.located(result.value()?),
            State::Quota => self.quota_step(result),
            State::Write => {
                result.done()?;
                let row =
                    copy_row(self.key().bucket_id, &self.version).map_err(|_| AbeError::Context)?;
                self.state = State::Delete;
                let delete = (ABE_COPY_KEYSPACE.to_string(), row.into());
                Ok(Next::Call(IoCall::Delete(vec![delete])))
            }
            State::Delete => {
                result.done()?;
                let usage = self.usage.as_mut().ok_or(AbeError::Context)?;
                self.state = State::Usage;
                Ok(Next::Call(usage.start_calls().into_call()))
            }
            State::Usage => self.usage_step(result),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// Key of the pending copy row of `version`, prefixed by its stable bucket id.
pub(crate) fn copy_row(bucket_id: Ulid, version: &VersionKey) -> Result<Vec<u8>, ConversionError> {
    Ok([&bucket_id.to_bytes()[..], &version.to_bytes()?].concat())
}

/// The version of a pending copy row, after its bucket id prefix.
pub(crate) fn copy_version(row: &[u8]) -> Result<VersionKey, ConversionError> {
    let (_, version) = row
        .split_at_checked(ROW_PREFIX)
        .ok_or_else(|| ConversionError::InvalidLength("pending copy row".to_string()))?;
    VersionKey::from_bytes(version)
}

/// Fails unless the bucket settings `row` still seal new writes to `key`.
pub(crate) fn still_active(row: Option<&[u8]>, key: BucketKeyRef) -> Result<(), AbeError> {
    let settings = BucketEncryption::from_row(row).map_err(|_| AbeError::Context)?;
    match settings.active_key() == Some(key) {
        true => Ok(()),
        false => Err(AbeError::Parameters),
    }
}

/// Pages through pending copies and envelopes those of `key`; a lock in between stops the walk.
pub async fn complete_copies(
    context: &DriverContext,
    key: BucketKeyRef,
    (realm, node): (RealmId, NodeId),
) -> Result<usize, String> {
    let mut quota = None;
    let mut start_after = None;
    let mut completed = 0;
    loop {
        let (rows, next) = iter_prefix_page(
            &context.storage_handle,
            ABE_COPY_KEYSPACE,
            Some(key.bucket_id.to_bytes().to_vec().into()),
            start_after,
            COPY_PAGE,
            None,
        )
        .await?;
        if quota.is_none() && !rows.is_empty() {
            let config = drive(GetConfigOperation::new(realm), context).await;
            quota = Some(config.map_err(|error| error.to_string())?.quota);
        }
        for (row, value) in &rows {
            let pending = PendingCopy::from_bytes(value).map_err(|error| error.to_string())?;
            // Rows of another generation wait for that key.
            if pending.source.context.parameters.key != key {
                continue;
            }
            let version = copy_version(row).map_err(|error| error.to_string())?;
            let mut operation = CopyEnvelopeOperation::new(version, value.to_vec(), pending);
            if let Some(quota) = &quota {
                operation = operation.with_quota(quota.clone(), realm, node);
            }
            match drive(operation, context).await {
                Ok(CopyOutcome::Completed) => completed += 1,
                Ok(CopyOutcome::Gone) => {}
                Ok(CopyOutcome::Locked) => return Ok(completed),
                // A raise in between leaves this copy for the next unlock.
                Err(error) => warn!(%error, "Failed to envelope a pending copy"),
            }
        }
        match next {
            Some(next) if !rows.is_empty() => start_after = Some(next),
            _ => return Ok(completed),
        }
    }
}
