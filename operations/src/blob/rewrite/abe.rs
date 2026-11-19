//! Moves a version's envelope with its copy: a new generation gets a new object key and envelope,
//! the same generation keeps them, decryption drops them. All commit in the version's transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::abe_copies::copy_row;
use crate::s3::bucket::key::rows::Row;
use crate::s3::object::put::abe::{abe_reads, located_rows, parse_abe};
use crate::s3::usage::RowChange;
use crate::s3::usage::StoredDelta;
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

    pub(super) fn read_envelope(&mut self) -> Step {
        // A plain-to-plain rewrite has no envelope rows.
        if self.transition.target.plan.is_none() && self.plain() {
            return self.proceed();
        }
        let (Ok(version), Ok(row)) = (self.version_key.to_bytes(), self.copy_key()) else {
            return Err(RewriteError::NotFinished);
        };
        let mut reads = vec![
            (ABE_VERSION_KEYSPACE.to_string(), version.into()),
            (ABE_COPY_KEYSPACE.to_string(), row),
        ];
        if let Some(plan) = self.transition.target.plan {
            reads.extend(abe_reads(plan.key));
        }
        self.state = RewriteState::ReadEnvelope;
        Ok(Next::Call(outside(IoCall::Read(reads))))
    }

    fn plain(&self) -> bool {
        (self.old.as_ref()).is_some_and(|old| old.format.bucket_key().is_none())
    }

    fn same_generation(&self) -> bool {
        let source = self.old.as_ref().and_then(|old| old.format.bucket_key());
        source.is_some()
            && !self.rekey
            && self.transition.target.plan.map(|plan| plan.key) == source
    }

    /// Admits a sealed source for reading; a plain source is rewritten directly.
    fn proceed(&mut self) -> Step {
        let old = self.old.as_ref().ok_or(RewriteError::NotFinished)?;
        match old.format.bucket_key() {
            Some(key) => {
                let archive = ArchiveKey::of(old);
                self.admit(key, archive)
            }
            None => self.rewrite(),
        }
    }

    /// A version with envelope rows, or a plain version moving into a bucket with ABE
    /// parameters, gets its new envelope before the rewrite grants to it.
    pub(super) fn handle_envelope(&mut self, values: Vec<(Key, Option<Value>)>) -> Step {
        let Some(([(_, id), (_, copy)], anchors)) = values.split_first_chunk() else {
            return Err(RewriteError::NotFinished);
        };
        // Without target parameters a plain version gets a bucket-only copy.
        let admitted = anchors.first().is_some_and(|(_, row)| row.is_some());
        if id.is_none() && copy.is_none() && !(self.plain() && admitted) {
            // Without an envelope there is no object key to replace.
            if self.rekey {
                return self.end(RewriteOutcome::Skipped);
            }
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
        let (parameters, epoch) = parse_abe(anchors, target.key).map_err(abe_error)?;
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
        let create = Effect::Blob(BlobEffect::Abe(Box::new(AbeEffect::Envelope(plan))));
        Ok(Next::Call(create.into()))
    }

    pub(super) fn handle_created(&mut self, event: Event) -> Step {
        let envelope = match event {
            Event::Blob(BlobEvent::Abe(event)) => match *event {
                AbeEvent::Envelope(envelope) => envelope,
                other => {
                    let other = Event::Blob(BlobEvent::Abe(Box::new(other)));
                    return Err(self.unexpected(other));
                }
            },
            Event::Blob(BlobEvent::Error(error)) => return Err(error.into()),
            other => return Err(self.unexpected(other)),
        };
        self.envelope = Some(envelope);
        self.proceed()
    }

    /// A re-encoding in the same generation keeps the object key, write id and envelope.
    fn keep_envelope(&mut self) -> Step {
        let rows = self
            .envelope_rows
            .as_ref()
            .ok_or(RewriteError::NotFinished)?;
        if let Some(id) = rows.id.clone() {
            self.state = RewriteState::KeepEnvelope;
            return Ok(Next::Call(outside(read_row(ABE_ENVELOPE_KEYSPACE, id))));
        }
        let pending = rows.copy.as_deref().map(PendingCopy::from_bytes);
        self.pending = pending.transpose().map_err(abe_error)?;
        self.proceed()
    }

    pub(super) fn handle_kept(&mut self, value: Option<Value>) -> Step {
        match value.as_deref().map(ObjectEnvelope::from_bytes) {
            Some(Ok(envelope)) => {
                self.envelope = Some(envelope);
                self.proceed()
            }
            Some(Err(error)) => Err(abe_error(error)),
            None => Err(abe_error(AbeError::Pending)),
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
        let (writes, charge) =
            located_rows(envelope, &self.version_key, published, (metadata, limit))
                .map_err(abe_error)?;
        Ok((writes, i128::from(charge) - i128::from(old_charge)))
    }

    /// A gate for a positive envelope charge change, when the group has a quota.
    pub(super) fn quota_gate(&self, added: i128) -> Option<QuotaGate> {
        let ((quota, realm, node), (group, _)) = (self.quota.as_ref()?, self.charge?);
        let ceiling = quota.effective_group_ceiling(&group)?;
        let added = u64::try_from(added).ok().filter(|added| *added > 0)?;
        Some(QuotaGate::new_for_realm(
            ceiling, added, group, *node, *realm,
        ))
    }

    /// The new envelope charge must fit the group quota before the usage update.
    pub(super) fn handle_quota(&mut self, result: IoResult) -> Step {
        let gate = self.gate.as_mut().ok_or(RewriteError::NotFinished)?;
        match gate.step_calls(result) {
            Ok(Some(call)) => Ok(Next::Call(call)),
            Ok(None) if gate.is_exceeded() => Err(abe_error(AbeError::Limit)),
            Ok(None) => {
                self.gate = None;
                self.update_usage()
            }
            Err(_) => Err(abe_error(AbeError::Unavailable)),
        }
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
        let bucket = &self.version_key.bucket;
        match (self.charge.map(|(group, _)| group), stored) {
            (Some(group), Some(stored)) => Some(
                UsageCounterUpdate::with_stored(group, delta, stored)
                    .in_bucket(bucket, RowChange::Kept),
            ),
            (Some(group), None) => {
                Some(UsageCounterUpdate::for_group(group, delta).in_bucket(bucket, RowChange::Kept))
            }
            (None, stored) => stored.map(UsageCounterUpdate::for_stored),
        }
    }

    /// Deletes the replaced envelope rows in the version's transaction.
    pub(super) fn drop_envelope(&mut self) -> Step {
        if self.deletes.is_empty() {
            return self.update_usage();
        }
        self.state = RewriteState::DropEnvelope;
        Ok(Next::Call(IoCall::Delete(std::mem::take(
            &mut self.deletes,
        ))))
    }
}
