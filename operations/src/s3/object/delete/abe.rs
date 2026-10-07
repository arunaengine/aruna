//! Removes a deleted version's envelope rows in the delete transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::abe::copies::copy_row;
use aruna_core::keyspaces::{
    ABE_ARCHIVE_KEYSPACE, ABE_COPY_KEYSPACE, ABE_ENVELOPE_KEYSPACE, ABE_VERSION_KEYSPACE,
};
use aruna_core::structs::storage::abe::envelope_charge;

impl DeleteObjectOperation {
    fn target_key(&self) -> Result<VersionKey, DeleteObjectError> {
        let version_id = self
            .input
            .version_id
            .ok_or(DeleteObjectError::InvalidOperationState)?;
        Ok(VersionKey::new(
            &self.input.bucket,
            &self.input.key,
            version_id,
        ))
    }
    fn envelope_key(&self) -> Result<Vec<u8>, DeleteObjectError> {
        Ok(self.target_key()?.to_bytes()?)
    }
    pub(super) fn read_envelope_version(&mut self) -> Effects {
        let key = match self.envelope_key() {
            Ok(key) => key,
            Err(err) => return self.emit_error(err),
        };
        self.state = DeleteObjectState::ReadEnvelopeVersion;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: ABE_VERSION_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: self.txn_id,
        })]
    }
    pub(super) fn envelope_version_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        let Some(id) = value else {
            // A pending copy has no envelope yet, only its pending row under its bucket key.
            let location = self.target_archive.as_ref().and_then(|(_, l)| l.as_ref());
            let Some(bucket_key) = location.and_then(|l| l.format.bucket_key()) else {
                return self.remove_managed_copies();
            };
            let key = match self
                .target_key()
                .and_then(|version| Ok(copy_row(bucket_key, &version)?))
            {
                Ok(key) => key,
                Err(err) => return self.emit_error(err),
            };
            self.state = DeleteObjectState::ReadEnvelopeRows;
            return smallvec![Effect::Storage(StorageEffect::BatchRead {
                reads: vec![(ABE_COPY_KEYSPACE.to_string(), key.into())],
                txn_id: self.txn_id,
            })];
        };
        self.state = DeleteObjectState::ReadEnvelopeRows;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (ABE_ENVELOPE_KEYSPACE.to_string(), id.clone()),
                (ABE_ARCHIVE_KEYSPACE.to_string(), id),
            ],
            txn_id: self.txn_id,
        })]
    }
    pub(super) fn envelope_rows_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        let key = match self.envelope_key() {
            Ok(key) => key,
            Err(err) => return self.emit_error(err),
        };
        if let [(row, pending)] = values.as_slice() {
            self.envelope_bytes = pending.as_ref().map_or(0, |value| value.len() as u64);
            self.state = DeleteObjectState::DeleteEnvelopeRows;
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes: vec![(ABE_COPY_KEYSPACE.to_string(), row.clone())],
                txn_id: self.txn_id,
            })];
        }
        let [(id, envelope), (_, archive)] = values.as_slice() else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        self.envelope_bytes = envelope_charge(
            envelope.as_deref().unwrap_or_default(),
            archive.as_deref().unwrap_or_default(),
        );
        self.state = DeleteObjectState::DeleteEnvelopeRows;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: vec![
                (ABE_VERSION_KEYSPACE.to_string(), key.into()),
                (ABE_ENVELOPE_KEYSPACE.to_string(), id.clone()),
                (ABE_ARCHIVE_KEYSPACE.to_string(), id.clone()),
            ],
            txn_id: self.txn_id,
        })]
    }
    pub(super) fn envelope_rows_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        self.remove_managed_copies()
    }
}
