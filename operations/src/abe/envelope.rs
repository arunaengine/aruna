//! Resolves envelopes against exact versions and admitted archive mappings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::*;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::storage::abe::{AbeError, AbeParameters, EnvelopeArchive, ObjectEnvelope};
use aruna_core::structs::storage::blob::{ArchiveKey, BlobVersion, VersionKey};
use aruna_core::types::Value;
use ulid::Ulid;

type Envelope = (ObjectEnvelope, EnvelopeArchive);

/// Reads the version, its envelope and archive, and the parameter anchor in one transaction.
#[derive(Debug, PartialEq)]
pub struct EnvelopeOperation {
    version: VersionKey,
    id: Option<Ulid>,
    location: Option<Vec<u8>>,
    /// The archive a version without a content hash waits on.
    pending: Option<ArchiveKey>,
    result: Option<Envelope>,
}
impl EnvelopeOperation {
    pub fn new(bucket: String, key: String, version: Ulid) -> Self {
        Self {
            version: VersionKey::new(bucket, key, version),
            id: None,
            location: None,
            pending: None,
            result: None,
        }
    }
    fn read_envelope(
        &mut self,
        [version, id]: [Option<Value>; 2],
    ) -> Result<Next<Envelope>, AbeError> {
        let version = version.and_then(|v| BlobVersion::from_bytes(&v).ok());
        let version = version.ok_or(AbeError::Missing)?;
        self.location = version.location_key().map(|k| k.to_bytes());
        self.pending = version.state.pending_archive().cloned();
        let id = id.and_then(|v| <[u8; 16]>::try_from(v.as_ref()).ok());
        let id = Ulid::from_bytes(id.ok_or(AbeError::Pending)?);
        self.id = Some(id);
        let key = id.to_bytes().to_vec();
        Ok(Next::Call(IoCall::Read(vec![
            (ABE_ENVELOPE_KEYSPACE.to_string(), key.clone().into()),
            (ABE_ARCHIVE_KEYSPACE.to_string(), key.into()),
        ])))
    }
    fn read_anchor(
        &mut self,
        [envelope, archive]: [Option<Value>; 2],
    ) -> Result<Next<Envelope>, AbeError> {
        let envelope = ObjectEnvelope::from_bytes(envelope.as_deref().ok_or(AbeError::Pending)?)?;
        let archive: EnvelopeArchive =
            postcard::from_bytes(archive.as_deref().ok_or(AbeError::Pending)?)
                .map_err(|_| AbeError::Context)?;
        if self.id != Some(envelope.context.write_id)
            || self.location.as_deref().unwrap_or_default() != archive.location_key
            || (self.location.is_none() && self.pending.as_ref() != Some(&archive.archive))
            || envelope.context.object_key != self.version.key
        {
            return Err(AbeError::Context);
        }
        let key = envelope.context.parameters.key.key();
        self.result = Some((envelope, archive));
        let read = (ABE_PARAMETERS_KEYSPACE.to_string(), key.into());
        Ok(Next::Call(IoCall::Read(vec![read])))
    }
}
impl Operation for EnvelopeOperation {
    type Output = Envelope;
    type Error = AbeError;
    /// The reads see one snapshot, so the version and its envelope agree.
    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }
    fn first(&mut self) -> Result<Next<Envelope>, AbeError> {
        let key = self.version.to_bytes().map_err(|_| AbeError::Context)?;
        Ok(Next::Call(IoCall::Read(vec![
            (BLOB_VERSIONS_KEYSPACE.to_string(), key.clone().into()),
            (ABE_VERSION_KEYSPACE.to_string(), key.into()),
        ])))
    }
    fn next(&mut self, result: IoResult) -> Result<Next<Envelope>, AbeError> {
        let IoResult::Values(values) = result else {
            return Err(AbeError::Context);
        };
        if self.id.is_none() {
            let values = values.try_into().map_err(|_| AbeError::Context)?;
            return self.read_envelope(values);
        }
        if self.result.is_none() {
            let values = values.try_into().map_err(|_| AbeError::Context)?;
            return self.read_anchor(values);
        }
        let [parameters] = <[_; 1]>::try_from(values).map_err(|_| AbeError::Context)?;
        let parameters = parameters.map(|v| AbeParameters::from_bytes(&v));
        let result = self.result.take().ok_or(AbeError::Context)?;
        match parameters {
            Some(Ok(parameters)) if result.0.context.parameters == parameters => {
                Ok(Next::Done(result))
            }
            _ => Err(AbeError::Parameters),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_version_fails() {
        // A version row that is absent ends the read before any envelope row.
        let mut operation =
            EnvelopeOperation::new("b".into(), "k".into(), Ulid::from_bytes([1; 16]));
        assert_eq!(operation.transaction(), TransactionMode::ReadSnapshot);
        assert!(
            matches!(operation.first(), Ok(Next::Call(IoCall::Read(reads))) if reads.len() == 2)
        );
        let empty = IoResult::Values(vec![None, None]);
        assert_eq!(operation.next(empty), Err(AbeError::Missing));
        assert_eq!(operation.next(IoResult::Done), Err(AbeError::Context));
    }
}
