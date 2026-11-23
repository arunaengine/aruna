//! Removes a deleted version's envelope rows in the delete transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::abe_copies::copy_row;
use aruna_core::keyspaces::{
    ABE_ARCHIVE_KEYSPACE, ABE_COPY_KEYSPACE, ABE_ENVELOPE_KEYSPACE, ABE_VERSION_KEYSPACE,
    BUCKET_ENCRYPTION_KEYSPACE,
};
use aruna_core::structs::storage::abe::envelope_charge;
use aruna_core::structs::storage::encryption::BucketEncryption;

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
    pub(super) fn read_envelope_version(&mut self) -> Turn {
        let key = self.envelope_key()?;
        let bucket = self.input.bucket.as_bytes().to_vec();
        let reads = vec![
            (ABE_VERSION_KEYSPACE.to_string(), key.into()),
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket.into()),
        ];
        self.call(DeleteObjectState::ReadEnvelopeVersion, IoCall::Read(reads))
    }

    pub(super) fn envelope_version_read(&mut self, values: Vec<Option<Value>>) -> Turn {
        let [value, settings] =
            <[_; 2]>::try_from(values).map_err(|_| DeleteObjectError::InvalidOperationState)?;
        let reads = match value {
            Some(id) => vec![
                (ABE_ENVELOPE_KEYSPACE.to_string(), id.clone()),
                (ABE_ARCHIVE_KEYSPACE.to_string(), id),
            ],
            // A pending copy has no envelope yet, only its pending row under the stable bucket id.
            None => {
                let settings = BucketEncryption::from_row(settings.as_deref())?;
                let Some(bucket_id) = settings.bucket_id else {
                    return self.remove_managed_copies();
                };
                let key = copy_row(bucket_id, &self.target_key()?)?;
                vec![(ABE_COPY_KEYSPACE.to_string(), key.into())]
            }
        };
        self.envelope_keys = reads.iter().map(|(_, key)| key.clone()).collect();
        self.call(DeleteObjectState::ReadEnvelopeRows, IoCall::Read(reads))
    }

    pub(super) fn envelope_rows_read(&mut self, values: Vec<Option<Value>>) -> Turn {
        let key = self.envelope_key()?;
        let keys = std::mem::take(&mut self.envelope_keys);
        let deletes = match (keys.as_slice(), values.as_slice()) {
            ([row], [pending]) => {
                self.envelope_bytes = pending.as_ref().map_or(0, |value| value.len() as u64);
                vec![(ABE_COPY_KEYSPACE.to_string(), row.clone())]
            }
            ([id, _], [envelope, archive]) => {
                self.envelope_bytes = envelope_charge(
                    envelope.as_deref().unwrap_or_default(),
                    archive.as_deref().unwrap_or_default(),
                );
                vec![
                    (ABE_VERSION_KEYSPACE.to_string(), key.into()),
                    (ABE_ENVELOPE_KEYSPACE.to_string(), id.clone()),
                    (ABE_ARCHIVE_KEYSPACE.to_string(), id.clone()),
                ]
            }
            _ => return Err(DeleteObjectError::InvalidOperationState),
        };
        self.call(
            DeleteObjectState::DeleteEnvelopeRows,
            IoCall::Delete(deletes),
        )
    }
}
