//! Resolves envelopes against exact versions and admitted archive mappings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::*;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::abe::{AbeError, AbeParameters, EnvelopeArchive, ObjectEnvelope};
use aruna_core::structs::storage::blob::{BlobVersion, VersionKey};
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, PartialEq)]
enum State {
    Init,
    Start,
    Version,
    Envelope,
    Anchor,
    Commit,
    Done,
}
#[derive(Debug, PartialEq)]
pub struct EnvelopeOperation {
    version: VersionKey,
    txn: Option<TxnId>,
    state: State,
    id: Option<Ulid>,
    location: Option<Vec<u8>>,
    result: Option<(ObjectEnvelope, EnvelopeArchive)>,
    output: Option<Result<(ObjectEnvelope, EnvelopeArchive), AbeError>>,
}
impl EnvelopeOperation {
    pub fn new(bucket: String, key: String, version: Ulid) -> Self {
        Self {
            version: VersionKey::new(bucket, key, version),
            txn: None,
            state: State::Init,
            id: None,
            location: None,
            result: None,
            output: None,
        }
    }
    fn fail(&mut self, error: AbeError) -> Effects {
        self.state = State::Done;
        self.output = Some(Err(error));
        self.abort()
    }
}
impl Operation for EnvelopeOperation {
    type Output = (ObjectEnvelope, EnvelopeArchive);
    type Error = AbeError;
    fn start(&mut self) -> Effects {
        self.state = State::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }
    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (State::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn = Some(txn_id);
                self.state = State::Version;
                let key = match self.version.to_bytes() {
                    Ok(k) => k,
                    Err(_) => return self.fail(AbeError::Context),
                };
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: vec![
                        (BLOB_VERSIONS_KEYSPACE.to_string(), key.clone().into()),
                        (ABE_VERSION_KEYSPACE.to_string(), key.into())
                    ],
                    txn_id: self.txn
                })]
            }
            (State::Version, Event::Storage(StorageEvent::BatchReadResult { values }))
                if values.len() == 2 =>
            {
                let Some(version) = values[0]
                    .1
                    .as_ref()
                    .and_then(|v| BlobVersion::from_bytes(v).ok())
                else {
                    return self.fail(AbeError::Missing);
                };
                self.location = version.location_key().map(|k| k.to_bytes());
                let Some(id) = values[1]
                    .1
                    .as_ref()
                    .and_then(|v| <[u8; 16]>::try_from(v.as_ref()).ok())
                    .map(Ulid::from_bytes)
                else {
                    return self.fail(AbeError::Pending);
                };
                self.id = Some(id);
                self.state = State::Envelope;
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: vec![
                        (
                            ABE_ENVELOPE_KEYSPACE.to_string(),
                            id.to_bytes().to_vec().into()
                        ),
                        (
                            ABE_ARCHIVE_KEYSPACE.to_string(),
                            id.to_bytes().to_vec().into()
                        )
                    ],
                    txn_id: self.txn
                })]
            }
            (State::Envelope, Event::Storage(StorageEvent::BatchReadResult { values }))
                if values.len() == 2 =>
            {
                let parsed = (|| {
                    let envelope = ObjectEnvelope::from_bytes(
                        values[0].1.as_deref().ok_or(AbeError::Pending)?,
                    )?;
                    let archive: EnvelopeArchive =
                        postcard::from_bytes(values[1].1.as_deref().ok_or(AbeError::Pending)?)
                            .map_err(|_| AbeError::Context)?;
                    if self.id != Some(envelope.context.write_id)
                        || self.location.as_deref().unwrap_or_default() != archive.location_key
                        || envelope.context.object_key != self.version.key
                    {
                        return Err(AbeError::Context);
                    }
                    Ok((envelope, archive))
                })();
                let result = match parsed {
                    Ok(r) => r,
                    Err(e) => return self.fail(e),
                };
                let key = result.0.context.parameters.key.key();
                self.result = Some(result);
                self.state = State::Anchor;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: ABE_PARAMETERS_KEYSPACE.to_string(),
                    key: key.into(),
                    txn_id: self.txn
                })]
            }
            (State::Anchor, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let parameters = match value.as_ref().map(|v| AbeParameters::from_bytes(v)) {
                    Some(Ok(p)) => p,
                    _ => return self.fail(AbeError::Parameters),
                };
                if self
                    .result
                    .as_ref()
                    .is_none_or(|(e, _)| e.context.parameters != parameters)
                {
                    return self.fail(AbeError::Parameters);
                }
                let Some(txn_id) = self.txn else {
                    return self.fail(AbeError::Context);
                };
                self.state = State::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { txn_id }))
                if self.txn == Some(txn_id) =>
            {
                self.txn = None;
                self.state = State::Done;
                self.output = self.result.take().map(Ok);
                smallvec![]
            }
            (_, Event::Storage(StorageEvent::Error { .. })) => self.fail(AbeError::Unavailable),
            _ => self.fail(AbeError::Context),
        }
    }
    fn is_complete(&self) -> bool {
        self.state == State::Done
    }
    fn finalize(self) -> Result<Self::Output, AbeError> {
        self.output.unwrap_or(Err(AbeError::Context))
    }
    fn abort(&mut self) -> Effects {
        self.txn.take().map_or_else(Effects::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}
