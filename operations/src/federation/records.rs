//! Node-local transfer records changed inside one storage transaction, so racing requests keep
//! the first winner and a revocation is never lost.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::FEDERATION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use super::export::GrantRecord;
use super::import::ImportRecord;

const CONFLICT_RETRIES: u8 = 5;

type Writes = Vec<(String, Key, Value)>;

/// One conditional change of a transfer record.
#[derive(Clone, Debug, PartialEq)]
pub enum RecordChange {
    /// Stores a grant record unless one exists; the stored one wins.
    StoreGrant { key: Vec<u8>, record: GrantRecord },
    /// Sets the revoked flag of a grant record; nothing clears it.
    RevokeGrant { key: Vec<u8> },
    /// Binds an import key to an upload with its record unless another upload is bound; binding
    /// the same upload again replaces its record.
    BindImport {
        key: Vec<u8>,
        record_key: Vec<u8>,
        upload_id: Ulid,
        record: ImportRecord,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum RecordOutcome {
    /// The stored grant record: the new one or an earlier winner.
    Grant(GrantRecord),
    /// Whether a grant record existed to revoke.
    Revoked(bool),
    /// The upload bound to the import key: the new one or an earlier winner.
    Bound(Ulid),
}

#[derive(Debug, Error, PartialEq)]
pub enum RecordError {
    #[error("transfer record storage failed: {0}")]
    Storage(String),
    #[error("transfer record is invalid: {0}")]
    Invalid(String),
    #[error("unexpected transfer record event")]
    Unexpected,
}

#[derive(Debug, PartialEq)]
enum State {
    Start,
    Read,
    Write,
    Commit,
    Finish,
}

#[derive(Debug, PartialEq)]
pub struct RecordOperation {
    change: RecordChange,
    state: State,
    txn_id: Option<TxnId>,
    conflicts: u8,
    pending: Option<RecordOutcome>,
    output: Option<Result<RecordOutcome, RecordError>>,
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Value, RecordError> {
    postcard::to_allocvec(value)
        .map(Value::from)
        .map_err(|error| RecordError::Invalid(error.to_string()))
}

fn entry(key: &[u8], value: Value) -> (String, Key, Value) {
    (FEDERATION_KEYSPACE.to_string(), key.to_vec().into(), value)
}

impl RecordOperation {
    pub fn new(change: RecordChange) -> Self {
        Self {
            change,
            state: State::Start,
            txn_id: None,
            conflicts: 0,
            pending: None,
            output: None,
        }
    }

    fn fail(&mut self, error: RecordError) -> Effects {
        self.output = Some(Err(error));
        self.state = State::Finish;
        self.abort()
    }

    fn key(&self) -> &[u8] {
        match &self.change {
            RecordChange::StoreGrant { key, .. }
            | RecordChange::RevokeGrant { key }
            | RecordChange::BindImport { key, .. } => key,
        }
    }

    /// The writes and outcome for the stored value of the key.
    fn decide(&self, stored: Option<&[u8]>) -> Result<(Writes, RecordOutcome), RecordError> {
        let decode = |bytes: &[u8]| {
            postcard::from_bytes::<GrantRecord>(bytes)
                .map_err(|error| RecordError::Invalid(error.to_string()))
        };
        Ok(match (&self.change, stored) {
            (RecordChange::StoreGrant { .. }, Some(bytes)) => {
                (Vec::new(), RecordOutcome::Grant(decode(bytes)?))
            }
            (RecordChange::StoreGrant { key, record }, None) => (
                vec![entry(key, encode(record)?)],
                RecordOutcome::Grant(record.clone()),
            ),
            (RecordChange::RevokeGrant { key }, Some(bytes)) => {
                let mut record = decode(bytes)?;
                record.revoked = true;
                (
                    vec![entry(key, encode(&record)?)],
                    RecordOutcome::Revoked(true),
                )
            }
            (RecordChange::RevokeGrant { .. }, None) => (Vec::new(), RecordOutcome::Revoked(false)),
            (
                RecordChange::BindImport {
                    key,
                    record_key,
                    upload_id,
                    record,
                },
                stored,
            ) => {
                let bound = stored
                    .map(|bytes| <[u8; 16]>::try_from(bytes).map(Ulid::from_bytes))
                    .transpose()
                    .map_err(|_| RecordError::Invalid("invalid upload binding".to_string()))?;
                match bound {
                    Some(bound) if bound != *upload_id => (Vec::new(), RecordOutcome::Bound(bound)),
                    _ => (
                        vec![
                            entry(record_key, encode(record)?),
                            entry(key, upload_id.to_bytes().to_vec().into()),
                        ],
                        RecordOutcome::Bound(*upload_id),
                    ),
                }
            }
        })
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(RecordError::Unexpected);
        };
        self.state = State::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }
}

impl Operation for RecordOperation {
    type Output = RecordOutcome;
    type Error = RecordError;

    fn start(&mut self) -> Effects {
        self.state = State::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (State::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = State::Read;
                let key = self.key().to_vec().into();
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: FEDERATION_KEYSPACE.to_string(),
                    key,
                    txn_id: Some(txn_id),
                })]
            }
            (State::Read, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let (writes, outcome) = match self.decide(value.as_deref()) {
                    Ok(decision) => decision,
                    Err(error) => return self.fail(error),
                };
                self.pending = Some(outcome);
                if writes.is_empty() {
                    return self.commit();
                }
                self.state = State::Write;
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes,
                    txn_id: self.txn_id,
                })]
            }
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => self.commit(),
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = State::Finish;
                self.output = Some(self.pending.take().ok_or(RecordError::Unexpected));
                smallvec![]
            }
            // A racing writer committed first: reread and decide again.
            (
                State::Commit,
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                }),
            ) if self.conflicts < CONFLICT_RETRIES => {
                self.txn_id = None;
                self.conflicts += 1;
                self.start()
            }
            (_, Event::Storage(StorageEvent::Error { error })) => {
                if self.state == State::Commit {
                    self.txn_id = None;
                }
                self.fail(RecordError::Storage(error.to_string()))
            }
            _ => self.fail(RecordError::Unexpected),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == State::Finish
    }

    fn finalize(self) -> Result<RecordOutcome, RecordError> {
        self.output.unwrap_or(Err(RecordError::Unexpected))
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map(|txn_id| smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })])
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::federation::Signed;
    use aruna_core::structs::identity::auth::NodeCapabilities;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::transfer::ExportGrant;
    use ed25519_dalek::SigningKey;
    use url::Url;

    fn grant_record() -> GrantRecord {
        let key = SigningKey::from_bytes(&[4; 32]);
        let source = RealmId::from_bytes(key.verifying_key().to_bytes());
        let grant = ExportGrant {
            source,
            audience: RealmId::from_bytes([9; 32]),
            intent_digest: String::new(),
            export_job_id: Ulid::from_bytes([5; 16]),
            document_id: Ulid::from_bytes([6; 16]),
            source_revision: Ulid::from_bytes([7; 16]),
            dataset_digest: String::new(),
            selection_digest: String::new(),
            artifact_url: Url::parse("https://a.example.org/artifact").unwrap(),
            artifact_blake3: String::new(),
            artifact_size: 1,
            issued_at: 1,
            expires_at: 2,
        };
        let capabilities = NodeCapabilities::management_node(key).unwrap();
        GrantRecord {
            grant: Signed::sign(grant, &capabilities).unwrap(),
            principal: UserId::new(Ulid::from_bytes([1; 16]), source),
            document_path: "/doc".to_string(),
            with_files: false,
            sources: Vec::new(),
            revoked: false,
        }
    }

    fn started(txn: u8) -> Event {
        Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([txn; 16]),
        })
    }

    fn read(value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: value.map(Value::from),
        })
    }

    fn committed(txn: u8) -> Event {
        Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: Ulid::from_bytes([txn; 16]),
        })
    }

    #[test]
    fn racing_revocation_wins() {
        // A grant stored after a conflicting revocation rereads and keeps the revoked record.
        let record = grant_record();
        let change = RecordChange::StoreGrant {
            key: b"grant".to_vec(),
            record: record.clone(),
        };
        let mut operation = RecordOperation::new(change);
        operation.start();
        operation.step(started(1));
        let effects = operation.step(read(None));
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::BatchWrite { .. })
        ));
        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        let conflict = StorageError::TransactionConflict;
        let effects = operation.step(Event::Storage(StorageEvent::Error { error: conflict }));
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::StartTransaction { .. })
        ));
        operation.step(started(2));
        let revoked = GrantRecord {
            revoked: true,
            ..record
        };
        let stored = postcard::to_allocvec(&revoked).unwrap();
        let effects = operation.step(read(Some(stored)));
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::CommitTransaction { .. })
        ));
        operation.step(committed(2));
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Ok(RecordOutcome::Grant(revoked)));
    }

    #[test]
    fn first_binding_wins() {
        // Another upload already bound to the key is kept and nothing is written.
        let winner = Ulid::from_bytes([1; 16]);
        let record = grant_record();
        let import = ImportRecord {
            intent: Signed {
                payload: aruna_core::transfer::ImportIntent {
                    realm_id: record.grant.payload.audience,
                    descriptor_digest: String::new(),
                    principal: record.principal,
                    destination: aruna_core::transfer::ImportDestination {
                        group_id: Ulid::nil(),
                        bucket: "lab".to_string(),
                        prefix: String::new(),
                        metadata_path: "run".to_string(),
                    },
                    max_bytes: 1,
                    nonce: String::new(),
                    issued_at: 1,
                    expires_at: 2,
                    intent_id: Ulid::nil(),
                },
                signer: record.grant.signer.clone(),
                signature: record.grant.signature.clone(),
            },
            grant: record.grant,
        };
        let change = RecordChange::BindImport {
            key: b"upload".to_vec(),
            record_key: b"import".to_vec(),
            upload_id: Ulid::from_bytes([2; 16]),
            record: import,
        };
        let mut operation = RecordOperation::new(change);
        operation.start();
        operation.step(started(1));
        let effects = operation.step(read(Some(winner.to_bytes().to_vec())));
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::CommitTransaction { .. })
        ));
        operation.step(committed(1));
        assert_eq!(operation.finalize(), Ok(RecordOutcome::Bound(winner)));
    }
}
