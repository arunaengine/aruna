//! Captures and publishes single-upload envelopes in the version transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::{
    ABE_ARCHIVE_KEYSPACE, ABE_ENVELOPE_KEYSPACE, ABE_EPOCH_KEYSPACE, ABE_PARAMETERS_KEYSPACE,
    ABE_VERSION_KEYSPACE,
};
use aruna_core::structs::storage::abe::{
    AbeError, AbeParameters, EnvelopeArchive, EnvelopePlan, envelope_charge,
};

impl PutObjectOperation {
    pub(super) fn read_abe(&mut self, fence: bool) -> Effects {
        let Some(plan) = self.seal_plan else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        self.state = if fence {
            PutObjectState::FenceAbe
        } else {
            PutObjectState::ReadAbe
        };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (ABE_PARAMETERS_KEYSPACE.to_string(), plan.key.key().into()),
                (
                    ABE_EPOCH_KEYSPACE.to_string(),
                    plan.key.bucket_id.to_bytes().to_vec().into()
                )
            ],
            txn_id: if fence { self.txn_id } else { None }
        })]
    }
    pub(super) fn abe_read(&mut self, event: Event, fence: bool) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        let parsed = (|| {
            if values.len() != 2 {
                return Err(AbeError::Context);
            }
            let parameters =
                AbeParameters::from_bytes(values[0].1.as_deref().ok_or(AbeError::Parameters)?)?;
            let epoch = u64::from_be_bytes(
                values[1]
                    .1
                    .as_deref()
                    .ok_or(AbeError::Epoch)?
                    .try_into()
                    .map_err(|_| AbeError::Epoch)?,
            );
            let plan = self.seal_plan.ok_or(AbeError::Context)?;
            if epoch == 0
                || parameters.key != plan.key
                || parameters.realm_id != self.config.realm_id
                || parameters.node_id != self.config.node_id
            {
                return Err(AbeError::Context);
            }
            Ok((parameters, epoch))
        })();
        let (parameters, epoch) = match parsed {
            Ok(value) => value,
            Err(error) => {
                return self.emit_error(PutObjectError::BlobWriteFailed(BlobError::from(error)));
            }
        };
        if fence {
            let Some(envelope) = &self.envelope else {
                return self.emit_error(PutObjectError::BlobWriteFailed(BlobError::from(
                    AbeError::Context,
                )));
            };
            if let Err(error) = envelope.anchored(&parameters, epoch) {
                return self.emit_error(PutObjectError::BlobWriteFailed(BlobError::from(error)));
            }
            return self.start_fence();
        }
        self.envelope_plan = Some(EnvelopePlan {
            parameters,
            epoch,
            write_id: Ulid::generate(),
            object_key: self.config.request.key.clone(),
            bucket_public: self.seal_plan.map(|p| p.public_key).unwrap_or_default(),
        });
        self.write_blob()
    }
    pub(super) fn store_envelope(&mut self) -> Effects {
        let Some(envelope) = &self.envelope else {
            return self.write_copy_owner();
        };
        let result = (|| {
            let version_id = self.version_id.ok_or(AbeError::Context)?;
            let location = self.get_output().ok_or(AbeError::Context)?;
            let version = self
                .version_key(version_id)
                .to_bytes()
                .map_err(|_| AbeError::Context)?;
            let id = envelope.context.write_id.to_bytes().to_vec();
            let archive = EnvelopeArchive {
                archive: ArchiveKey::of(location),
                location_key: location
                    .location_key()
                    .map_err(|_| AbeError::Context)?
                    .to_bytes(),
            };
            let bytes = envelope.to_bytes()?;
            let archive_bytes = postcard::to_allocvec(&archive).map_err(|_| AbeError::Context)?;
            let metadata = postcard::to_allocvec(&self.metadata).map_err(|_| AbeError::Context)?;
            let total = bytes.len() + archive_bytes.len() + id.len() + metadata.len();
            if total as u64 > self.rocrate_limits.metadata_bytes {
                return Err(AbeError::Limit);
            }
            let charge = envelope_charge(&bytes, &archive_bytes);
            let writes = vec![
                (
                    ABE_ENVELOPE_KEYSPACE.to_string(),
                    id.clone().into(),
                    bytes.into(),
                ),
                (
                    ABE_VERSION_KEYSPACE.to_string(),
                    version.into(),
                    id.clone().into(),
                ),
                (
                    ABE_ARCHIVE_KEYSPACE.to_string(),
                    id.into(),
                    archive_bytes.into(),
                ),
            ];
            Ok((writes, charge))
        })();
        match result {
            Ok((writes, charge)) => {
                self.envelope_bytes = charge;
                self.state = PutObjectState::WriteEnvelope;
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes,
                    txn_id: self.txn_id
                })]
            }
            Err(error) => self.emit_error(PutObjectError::BlobWriteFailed(BlobError::from(error))),
        }
    }
}
