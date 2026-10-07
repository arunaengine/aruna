//! Writes the envelopes of same-bucket copies published while their bucket was locked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::{DriverContext, drive};
use crate::jobs::store::iter_prefix_page;
use crate::node::usage_stats::UsageCounterUpdate;
use crate::s3::object::put::abe::{abe_reads, envelope_rows, parse_abe};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    ABE_COPY_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE,
    S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::storage::abe::{
    AbeEffect, AbeError, AbeEvent, EnvelopeArchive, ObjectEnvelope, PendingCopy,
};
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobVersion, BlobVersionState, BucketInfo, VersionKey,
};
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyRef};
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use smallvec::smallvec;
use std::collections::HashMap;
use tracing::warn;
use ulid::Ulid;

/// Pending copies read per page while an unlock completes them.
const COPY_PAGE: usize = 64;

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
    Init,
    Read,
    Create,
    Start,
    Check,
    Locate,
    Write,
    Delete,
    Usage,
    Commit,
    Done,
}

/// Envelopes one pending copy with the current epoch under the publication fence.
#[derive(Debug, PartialEq)]
pub struct CopyEnvelopeOperation {
    version: VersionKey,
    row: Vec<u8>,
    pending: PendingCopy,
    state: State,
    txn: Option<TxnId>,
    envelope: Option<ObjectEnvelope>,
    /// Metadata, group and location key of the checked version, kept until it is published.
    stored: Option<(HashMap<String, String>, GroupId, Vec<u8>)>,
    usage: Option<UsageCounterUpdate>,
    output: Option<Result<CopyOutcome, AbeError>>,
}

impl CopyEnvelopeOperation {
    pub fn new(version: VersionKey, row: Vec<u8>, pending: PendingCopy) -> Self {
        Self {
            version,
            row,
            pending,
            state: State::Init,
            txn: None,
            envelope: None,
            stored: None,
            usage: None,
            output: None,
        }
    }

    fn key(&self) -> BucketKeyRef {
        self.pending.source.context.parameters.key
    }

    fn finish(&mut self, output: Result<CopyOutcome, AbeError>) -> Effects {
        self.state = State::Done;
        self.output = Some(output);
        self.abort()
    }

    fn created(&mut self, event: BlobEvent) -> Effects {
        match event {
            BlobEvent::Abe(event) => match *event {
                AbeEvent::Envelope(envelope) => {
                    self.envelope = Some(envelope);
                    self.state = State::Start;
                    smallvec![Effect::Storage(StorageEffect::StartTransaction {
                        read: false
                    })]
                }
                _ => self.finish(Err(AbeError::Context)),
            },
            BlobEvent::Error(BlobError::Abe(AbeError::Required)) => {
                self.finish(Ok(CopyOutcome::Locked))
            }
            BlobEvent::Error(BlobError::Abe(error)) => self.finish(Err(error)),
            _ => self.finish(Err(AbeError::Crypto)),
        }
    }

    fn checked(&mut self, values: &[(Key, Option<Value>)]) -> Effects {
        let [
            (_, row),
            (_, version),
            (_, bucket),
            (_, settings),
            anchors @ ..,
        ] = values
        else {
            return self.finish(Err(AbeError::Context));
        };
        let version = version.as_deref().map(BlobVersion::from_bytes).transpose();
        let (Some(row), Ok(Some(version))) = (row, version) else {
            return self.finish(Ok(CopyOutcome::Gone));
        };
        if row.as_ref() != self.row.as_slice() || version.is_deleted() {
            return self.finish(Ok(CopyOutcome::Gone));
        }
        let result = (|| {
            let bucket = bucket.as_deref().ok_or(AbeError::Missing)?;
            let bucket = BucketInfo::from_bytes(bucket).map_err(|_| AbeError::Context)?;
            still_active(settings.as_deref(), self.key())?;
            let (parameters, epoch) = parse_abe(anchors, self.key())?;
            let envelope = self.envelope.as_ref().ok_or(AbeError::Context)?;
            envelope.anchored(&parameters, epoch)?;
            Ok(bucket.group_id)
        })();
        let group = match result {
            Ok(group) => group,
            Err(error) => return self.finish(Err(error)),
        };
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
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                    key: key.into(),
                    txn_id: self.txn,
                })]
            }
            (false, None) => self.finish(Err(AbeError::Context)),
        }
    }

    /// The materialized location must still be the pending source's archive and bucket key.
    fn located(&mut self, value: Option<Value>) -> Effects {
        let location = value.as_deref().map(BackendLocation::from_bytes);
        let Some(Ok(location)) = location else {
            return self.finish(Err(AbeError::Context));
        };
        if ArchiveKey::of(&location) != self.pending.archive
            || location.format.bucket_key() != Some(self.key())
        {
            return self.finish(Err(AbeError::Context));
        }
        self.publish()
    }

    fn publish(&mut self) -> Effects {
        let (Some((metadata, group, location_key)), Some(envelope)) =
            (self.stored.take(), self.envelope.as_ref())
        else {
            return self.finish(Err(AbeError::Context));
        };
        let archive = EnvelopeArchive {
            archive: self.pending.archive.clone(),
            location_key,
        };
        let limit = RoCrateLimits::default().metadata_bytes;
        let rows = (&metadata, limit, self.txn);
        match envelope_rows(envelope, &self.version, &archive, rows) {
            Ok((effect, charge)) => {
                let delta = UsageDelta {
                    logical_bytes: i128::from(charge),
                    ..Default::default()
                };
                self.usage = Some(UsageCounterUpdate::for_group(group, delta));
                self.state = State::Write;
                smallvec![effect]
            }
            Err(error) => self.finish(Err(error)),
        }
    }
}

impl Operation for CopyEnvelopeOperation {
    type Output = CopyOutcome;
    type Error = AbeError;

    fn start(&mut self) -> Effects {
        self.state = State::Read;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: abe_reads(self.key()),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (State::Read, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let (parameters, epoch) = match parse_abe(&values, self.key()) {
                    Ok(value) => value,
                    Err(error) => return self.finish(Err(error)),
                };
                if self.pending.source.context.parameters != parameters {
                    return self.finish(Err(AbeError::Parameters));
                }
                self.state = State::Create;
                smallvec![Effect::Blob(BlobEffect::Abe(Box::new(AbeEffect::Copy {
                    source: self.pending.source.clone(),
                    epoch,
                    write_id: Ulid::generate(),
                    object_key: self.version.key.clone(),
                })))]
            }
            (State::Create, Event::Blob(event)) => self.created(event),
            (State::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn = Some(txn_id);
                let Ok(version) = self.version.to_bytes() else {
                    return self.finish(Err(AbeError::Context));
                };
                let bucket = self.version.bucket.as_bytes().to_vec();
                let mut reads = vec![
                    (ABE_COPY_KEYSPACE.to_string(), version.clone().into()),
                    (BLOB_VERSIONS_KEYSPACE.to_string(), version.into()),
                    (S3_BUCKET_KEYSPACE.to_string(), bucket.clone().into()),
                    (BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket.into()),
                ];
                reads.extend(abe_reads(self.key()));
                self.state = State::Check;
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads,
                    txn_id: self.txn,
                })]
            }
            (State::Check, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.checked(&values)
            }
            (State::Locate, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.located(value)
            }
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                let Ok(version) = self.version.to_bytes() else {
                    return self.finish(Err(AbeError::Context));
                };
                self.state = State::Delete;
                smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes: vec![(ABE_COPY_KEYSPACE.to_string(), version.into())],
                    txn_id: self.txn,
                })]
            }
            (State::Delete, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                let (Some(txn_id), Some(usage)) = (self.txn, self.usage.as_mut()) else {
                    return self.finish(Err(AbeError::Context));
                };
                self.state = State::Usage;
                usage.start(txn_id)
            }
            (State::Usage, event @ Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                let (Some(txn_id), Some(usage)) = (self.txn, self.usage.as_mut()) else {
                    return self.finish(Err(AbeError::Context));
                };
                match usage.step(event, txn_id) {
                    Ok(Some(effects)) => effects,
                    Ok(None) => {
                        self.state = State::Commit;
                        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                    }
                    Err(_) => self.finish(Err(AbeError::Unavailable)),
                }
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { txn_id }))
                if self.txn == Some(txn_id) =>
            {
                self.txn = None;
                self.finish(Ok(CopyOutcome::Completed))
            }
            (_, Event::Storage(StorageEvent::Error { .. })) => {
                self.finish(Err(AbeError::Unavailable))
            }
            _ => self.finish(Err(AbeError::Context)),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == State::Done
    }

    fn finalize(self) -> Result<CopyOutcome, AbeError> {
        self.output.unwrap_or(Err(AbeError::Context))
    }

    fn abort(&mut self) -> Effects {
        self.txn.take().map_or_else(Effects::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
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
pub async fn complete_copies(context: &DriverContext, key: BucketKeyRef) -> Result<usize, String> {
    let mut start_after = None;
    let mut completed = 0;
    loop {
        let (rows, next) = iter_prefix_page(
            &context.storage_handle,
            ABE_COPY_KEYSPACE,
            None,
            start_after,
            COPY_PAGE,
            None,
        )
        .await?;
        for (row, value) in &rows {
            let pending = PendingCopy::from_bytes(value).map_err(|error| error.to_string())?;
            if pending.source.context.parameters.key != key {
                continue;
            }
            let version = VersionKey::from_bytes(row).map_err(|error| error.to_string())?;
            let operation = CopyEnvelopeOperation::new(version, value.to_vec(), pending);
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
