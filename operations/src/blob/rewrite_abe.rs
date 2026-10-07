//! Moves a version's envelope with its copy: a new generation gets a new object key and envelope,
//! the same generation keeps them, decryption drops them. All commit in the version's transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::abe::copies::copy_row;
use crate::node::usage_stats::StoredDelta;
use crate::s3::bucket::key::rows::Row;
use crate::s3::object::put::abe::{abe_reads, envelope_write, parse_abe};
use aruna_core::keyspaces::{
    ABE_ARCHIVE_KEYSPACE, ABE_COPY_KEYSPACE, ABE_ENVELOPE_KEYSPACE, ABE_VERSION_KEYSPACE,
    S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::storage::abe::{
    AbeEffect, AbeError, AbeEvent, EnvelopePlan, envelope_charge,
};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::usage::UsageDelta;
use ulid::Ulid;

/// The envelope id row and pending copy row of a version, as read before the rewrite.
#[derive(Debug, PartialEq)]
pub(super) struct EnvelopeRows {
    id: Option<Value>,
    copy: Option<Value>,
}

fn abe_error(error: AbeError) -> RewriteError {
    RewriteError::Blob(error.into())
}

impl RewriteVersionOperation {
    fn copy_key(&self) -> Result<Key, RewriteError> {
        let source = self.old.as_ref().and_then(|old| old.format.bucket_key());
        let target = self.transition.target.plan.map(|plan| plan.key);
        let source = source.or(target).ok_or(RewriteError::NotFinished)?;
        Ok(copy_row(source.bucket_id, &self.version_key)?.into())
    }

    pub(super) fn read_envelope(&mut self) -> Effects {
        let (Ok(version), Ok(row)) = (self.version_key.to_bytes(), self.copy_key()) else {
            return self.fail(RewriteError::NotFinished);
        };
        let mut reads = vec![
            (ABE_VERSION_KEYSPACE.to_string(), version.into()),
            (ABE_COPY_KEYSPACE.to_string(), row),
        ];
        if let Some(plan) = self.transition.target.plan {
            reads.extend(abe_reads(plan.key));
        }
        self.state = RewriteState::ReadEnvelope;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn same_generation(&self) -> bool {
        let source = self.old.as_ref().and_then(|old| old.format.bucket_key());
        source.is_some() && self.transition.target.plan.map(|plan| plan.key) == source
    }

    /// Admits a sealed source for reading; a plain source is rewritten directly.
    fn proceed(&mut self) -> Effects {
        let Some(old) = self.old.as_ref() else {
            return self.fail(RewriteError::NotFinished);
        };
        match old.format.bucket_key() {
            Some(key) => {
                let archive = ArchiveKey::of(old);
                self.admit(key, archive)
            }
            None => self.rewrite(),
        }
    }

    /// A version with envelope rows, or a plain version, gets its new envelope before the
    /// rewrite grants to it.
    pub(super) fn handle_envelope(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected(event);
        };
        let Some(([(_, id), (_, copy)], anchors)) = values.split_first_chunk() else {
            return self.fail(RewriteError::NotFinished);
        };
        let plain = (self.old.as_ref()).is_some_and(|old| old.format.bucket_key().is_none());
        if id.is_none() && copy.is_none() && !plain {
            return self.proceed();
        }
        self.envelope_rows = Some(EnvelopeRows {
            id: id.clone(),
            copy: copy.clone(),
        });
        let Some(target) = self.transition.target.plan else {
            return self.proceed();
        };
        if self.same_generation() {
            return self.keep_envelope();
        }
        let (parameters, epoch) = match parse_abe(anchors, target.key) {
            Ok(value) => value,
            Err(error) => return self.fail(abe_error(error)),
        };
        // A complete envelope keeps its write id; a pending copy gets its first one.
        let id = id.as_deref().and_then(|id| <[u8; 16]>::try_from(id).ok());
        let plan = EnvelopePlan {
            parameters,
            epoch,
            write_id: id.map_or_else(Ulid::generate, Ulid::from_bytes),
            object_key: self.version_key.key.clone(),
            bucket_public: target.public_key,
        };
        self.state = RewriteState::CreateEnvelope;
        smallvec![Effect::Blob(BlobEffect::Abe(Box::new(
            AbeEffect::Envelope(plan)
        )))]
    }

    pub(super) fn handle_created(&mut self, event: Event) -> Effects {
        let envelope = match event {
            Event::Blob(BlobEvent::Abe(event)) => match *event {
                AbeEvent::Envelope(envelope) => envelope,
                other => return self.unexpected(Event::Blob(BlobEvent::Abe(Box::new(other)))),
            },
            Event::Blob(BlobEvent::Error(error)) => return self.fail(error.into()),
            other => return self.unexpected(other),
        };
        self.envelope = Some(envelope);
        self.proceed()
    }

    /// A re-encoding in the same generation keeps the object key, write id and envelope.
    fn keep_envelope(&mut self) -> Effects {
        let Some(rows) = self.envelope_rows.as_ref() else {
            return self.fail(RewriteError::NotFinished);
        };
        if let Some(id) = rows.id.clone() {
            self.state = RewriteState::KeepEnvelope;
            return smallvec![Effect::Storage(StorageEffect::Read {
                key_space: ABE_ENVELOPE_KEYSPACE.to_string(),
                key: id,
                txn_id: None,
            })];
        }
        match rows
            .copy
            .as_deref()
            .map(PendingCopy::from_bytes)
            .transpose()
        {
            Ok(pending) => {
                self.pending = pending;
                self.proceed()
            }
            Err(error) => self.fail(abe_error(error)),
        }
    }

    pub(super) fn handle_kept(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        match value.as_deref().map(ObjectEnvelope::from_bytes) {
            Some(Ok(envelope)) => {
                self.envelope = Some(envelope);
                self.proceed()
            }
            Some(Err(error)) => self.fail(abe_error(error)),
            None => self.fail(abe_error(AbeError::Pending)),
        }
    }

    /// The envelope made for this rewrite, which the epoch fence checks; a kept one is older.
    fn fresh_envelope(&self) -> Option<&ObjectEnvelope> {
        self.envelope.as_ref().filter(|_| !self.same_generation())
    }

    /// Transaction reads that recheck the envelope rows and anchor the new envelope.
    pub(super) fn envelope_reads(&self) -> Result<Vec<(String, Key)>, RewriteError> {
        let Some(rows) = self.envelope_rows.as_ref() else {
            return Ok(Vec::new());
        };
        let bucket = self.version_key.bucket.as_bytes().to_vec().into();
        let mut reads = vec![
            (S3_BUCKET_KEYSPACE.to_string(), bucket),
            (
                ABE_VERSION_KEYSPACE.to_string(),
                self.version_key.to_bytes()?.into(),
            ),
            (ABE_COPY_KEYSPACE.to_string(), self.copy_key()?),
        ];
        if let Some(id) = rows.id.clone() {
            reads.push((ABE_ENVELOPE_KEYSPACE.to_string(), id.clone()));
            reads.push((ABE_ARCHIVE_KEYSPACE.to_string(), id));
        }
        if let Some(envelope) = self.fresh_envelope() {
            reads.extend(abe_reads(envelope.context.parameters.key));
        }
        Ok(reads)
    }

    /// False when the envelope rows changed since the rewrite; then the next pass moves them.
    pub(super) fn check_envelope(
        &mut self,
        values: &[(Key, Option<Value>)],
    ) -> Result<bool, RewriteError> {
        let Some(rows) = self.envelope_rows.as_ref() else {
            return Ok(true);
        };
        let Some(([(_, bucket), (_, id), (_, copy)], rest)) = values.split_first_chunk() else {
            return Err(RewriteError::NotFinished);
        };
        if *id != rows.id || *copy != rows.copy {
            return Ok(false);
        }
        let (old, anchors) = rest.split_at(rest.len().min(2 * usize::from(id.is_some())));
        let mut charge = copy.as_ref().map_or(0, |row| row.len() as u64);
        if let [(_, envelope), (_, archive)] = old {
            charge += envelope_charge(
                envelope.as_deref().unwrap_or_default(),
                archive.as_deref().unwrap_or_default(),
            );
        }
        if let Some(envelope) = self.fresh_envelope() {
            let key = envelope.context.parameters.key;
            let (parameters, epoch) = parse_abe(anchors, key).map_err(abe_error)?;
            envelope.anchored(&parameters, epoch).map_err(abe_error)?;
        }
        let bucket = bucket.as_deref().ok_or(RewriteError::NotFinished)?;
        self.charge = Some((BucketInfo::from_bytes(bucket)?.group_id, charge));
        Ok(true)
    }

    /// The new envelope rows for `published` and the deletes of the replaced ones, with the
    /// change of the group's charge.
    pub(super) fn envelope_writes(
        &mut self,
        published: &BackendLocation,
        metadata: &std::collections::HashMap<String, String>,
    ) -> Result<(Vec<Row>, i128), RewriteError> {
        let (Some(rows), Some((_, old_charge))) = (self.envelope_rows.as_ref(), self.charge) else {
            return Ok((Vec::new(), 0));
        };
        let (id, copy) = (rows.id.clone(), rows.copy.is_some());
        let version: Key = self.version_key.to_bytes()?.into();
        if let Some(mut pending) = self.pending.clone() {
            // The pending row names the new copy, whose grants include its source object key.
            if !self.owns_row {
                return Err(RewriteError::ContentMismatch);
            }
            pending.archive = ArchiveKey::of(published);
            let row = pending.to_bytes().map_err(abe_error)?;
            let added = row.len() as i128 - i128::from(old_charge);
            return Ok((
                vec![(ABE_COPY_KEYSPACE.to_string(), self.copy_key()?, row.into())],
                added,
            ));
        }
        if copy {
            self.deletes
                .push((ABE_COPY_KEYSPACE.to_string(), self.copy_key()?));
        }
        let Some(envelope) = self.envelope.as_ref() else {
            if let Some(id) = id {
                self.deletes.extend([
                    (ABE_VERSION_KEYSPACE.to_string(), version),
                    (ABE_ENVELOPE_KEYSPACE.to_string(), id.clone()),
                    (ABE_ARCHIVE_KEYSPACE.to_string(), id),
                ]);
            }
            return Ok((Vec::new(), -i128::from(old_charge)));
        };
        // The envelope names the new copy's object key, so only that copy may be published.
        if !self.owns_row {
            return Err(RewriteError::ContentMismatch);
        }
        let limit = RoCrateLimits::default().metadata_bytes;
        let (effect, charge) = envelope_write(
            envelope,
            &self.version_key,
            published,
            (metadata, limit, None),
        )
        .map_err(abe_error)?;
        let Effect::Storage(StorageEffect::BatchWrite { writes, .. }) = effect else {
            return Err(RewriteError::NotFinished);
        };
        Ok((writes, i128::from(charge) - i128::from(old_charge)))
    }

    /// The stored change of a new copy and the group's envelope charge change, as one update.
    pub(super) fn usage_with(
        &self,
        stored: Option<StoredDelta>,
        added: i128,
    ) -> Option<UsageCounterUpdate> {
        let delta = UsageDelta {
            logical_bytes: added,
            ..Default::default()
        };
        match (self.charge.map(|(group, _)| group), stored) {
            (Some(group), Some(stored)) => {
                Some(UsageCounterUpdate::with_stored(group, delta, stored))
            }
            (Some(group), None) => Some(UsageCounterUpdate::for_group(group, delta)),
            (None, stored) => stored.map(UsageCounterUpdate::for_stored),
        }
    }

    /// Deletes the replaced envelope rows in the version's transaction.
    pub(super) fn drop_envelope(&mut self) -> Effects {
        if self.deletes.is_empty() {
            return self.update_usage();
        }
        self.state = RewriteState::DropEnvelope;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: std::mem::take(&mut self.deletes),
            txn_id: self.txn_id,
        })]
    }
}
