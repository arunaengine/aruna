//! Captures and publishes single-upload envelopes in the version transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::{
    ABE_ARCHIVE_KEYSPACE, ABE_ENVELOPE_KEYSPACE, ABE_EPOCH_KEYSPACE, ABE_PARAMETERS_KEYSPACE,
    ABE_VERSION_KEYSPACE,
};
use aruna_core::structs::storage::abe::{
    AbeError, AbeParameters, EnvelopeArchive, EnvelopePlan, ObjectEnvelope, envelope_charge,
};
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::types::{Key, TxnId, Value};

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
        let txn_id = if fence { self.txn_id } else { None };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: abe_reads(plan.key),
            txn_id
        })]
    }
    pub(super) fn abe_read(&mut self, event: Event, fence: bool) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        let parsed = (|| {
            let plan = self.seal_plan.ok_or(AbeError::Context)?;
            let (parameters, epoch) = parse_abe(&values, plan.key)?;
            if parameters.realm_id != self.config.realm_id
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
            let version = self.version_key(version_id);
            let limit = self.rocrate_limits.metadata_bytes;
            let rows = (&self.metadata, limit, self.txn_id);
            envelope_write(envelope, &version, location, rows)
        })();
        match result {
            Ok((effect, charge)) => {
                self.envelope_bytes = charge;
                self.state = PutObjectState::WriteEnvelope;
                smallvec![effect]
            }
            Err(error) => self.emit_error(PutObjectError::BlobWriteFailed(BlobError::from(error))),
        }
    }
}

/// Reads the parameters of `key` and its bucket's epoch.
pub(crate) fn abe_reads(key: BucketKeyRef) -> Vec<(String, Key)> {
    vec![
        (ABE_PARAMETERS_KEYSPACE.to_string(), key.key().into()),
        (
            ABE_EPOCH_KEYSPACE.to_string(),
            key.bucket_id.to_bytes().to_vec().into(),
        ),
    ]
}

/// Parses the answer to `abe_reads(key)`; the parameters must belong to `key`.
pub(crate) fn parse_abe(
    values: &[(Key, Option<Value>)],
    key: BucketKeyRef,
) -> Result<(AbeParameters, u64), AbeError> {
    let [(_, parameters), (_, epoch)] = values else {
        return Err(AbeError::Context);
    };
    let parameters = AbeParameters::from_bytes(parameters.as_deref().ok_or(AbeError::Parameters)?)?;
    let epoch = epoch.as_deref().ok_or(AbeError::Epoch)?;
    let epoch = u64::from_be_bytes(epoch.try_into().map_err(|_| AbeError::Epoch)?);
    if epoch == 0 || parameters.key != key {
        return Err(AbeError::Context);
    }
    Ok((parameters, epoch))
}

/// The write publishing `envelope` for `version` stored at `location`, with its usage charge.
pub(crate) fn envelope_write(
    envelope: &ObjectEnvelope,
    version: &VersionKey,
    location: &BackendLocation,
    rows: (&HashMap<String, String>, u64, Option<TxnId>),
) -> Result<(Effect, u64), AbeError> {
    // A pending multipart archive has no location key until it is hashed.
    let location_key = location.location_key().map(|key| key.to_bytes());
    let archive = EnvelopeArchive {
        archive: ArchiveKey::of(location),
        location_key: location_key.unwrap_or_default(),
    };
    envelope_rows(envelope, version, &archive, rows)
}

/// The write publishing `envelope` for `version` with its archive mapping, and its usage charge.
pub(crate) fn envelope_rows(
    envelope: &ObjectEnvelope,
    version: &VersionKey,
    archive: &EnvelopeArchive,
    (metadata, limit, txn_id): (&HashMap<String, String>, u64, Option<TxnId>),
) -> Result<(Effect, u64), AbeError> {
    let version = version.to_bytes().map_err(|_| AbeError::Context)?;
    let id = envelope.context.write_id.to_bytes().to_vec();
    let bytes = envelope.to_bytes()?;
    let archive_bytes = postcard::to_allocvec(archive).map_err(|_| AbeError::Context)?;
    let metadata = postcard::to_allocvec(metadata).map_err(|_| AbeError::Context)?;
    let total = bytes.len() + archive_bytes.len() + id.len() + metadata.len();
    if total as u64 > limit {
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
    Ok((
        Effect::Storage(StorageEffect::BatchWrite { writes, txn_id }),
        charge,
    ))
}
