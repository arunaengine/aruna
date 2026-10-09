//! Node-local transfer records changed inside one storage transaction, so racing requests keep
//! the first winner and a revocation is never lost.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    DEDUP_INDEX_KEYSPACE, FEDERATION_KEYSPACE, JOB_KEYSPACE, ROCRATE_UPLOAD_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{RoCrateUploadRecord, job_record_key, parse_dedup_value};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use super::export::GrantRecord;
use super::import::ImportRecord;
use crate::jobs::import::{upload_key, upload_stale};
use crate::jobs::store::decode_job_record;

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
    /// the same upload again replaces its record. A bound `stale` upload is replaced when this
    /// transaction still finds it stale.
    BindImport {
        key: Vec<u8>,
        record_key: Vec<u8>,
        upload_id: Ulid,
        record: ImportRecord,
        stale: Option<StaleUpload>,
    },
    /// Clears the source confirmation of an import record and reports whether it was set.
    ConsumeConfirmation { record_key: Vec<u8> },
}

/// A bound upload the caller found stale, checked again inside the binding transaction.
#[derive(Clone, Debug, PartialEq)]
pub struct StaleUpload {
    pub upload_id: Ulid,
    /// The job dedup index key of the import key; its job keeps a gone upload bound.
    pub dedup_key: Vec<u8>,
    pub now_ms: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RecordOutcome {
    /// The stored grant record: the new one or an earlier winner.
    Grant(GrantRecord),
    /// Whether a grant record existed to revoke.
    Revoked(bool),
    /// The upload bound to the import key: the new one or an earlier winner.
    Bound(Ulid),
    /// Whether the import record held a source confirmation.
    Confirmed(bool),
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
    Job,
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
    /// The bound upload this attempt confirmed stale, with the writes that retire it.
    confirmed: Option<(Ulid, Writes)>,
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
            confirmed: None,
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
            | RecordChange::BindImport { key, .. }
            | RecordChange::ConsumeConfirmation { record_key: key } => key,
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
                    ..
                },
                stored,
            ) => {
                let bound = stored
                    .map(|bytes| <[u8; 16]>::try_from(bytes).map(Ulid::from_bytes))
                    .transpose()
                    .map_err(|_| RecordError::Invalid("invalid upload binding".to_string()))?;
                let confirmed = self.confirmed.as_ref();
                match bound {
                    Some(bound)
                        if bound != *upload_id
                            && confirmed.is_none_or(|(stale, _)| *stale != bound) =>
                    {
                        (Vec::new(), RecordOutcome::Bound(bound))
                    }
                    _ => {
                        let mut writes = vec![
                            entry(record_key, encode(record)?),
                            entry(key, upload_id.to_bytes().to_vec().into()),
                        ];
                        writes.extend(confirmed.into_iter().flat_map(|(_, retire)| retire.clone()));
                        (writes, RecordOutcome::Bound(*upload_id))
                    }
                }
            }
            (RecordChange::ConsumeConfirmation { record_key }, Some(bytes)) => {
                let mut record = postcard::from_bytes::<ImportRecord>(bytes)
                    .map_err(|error| RecordError::Invalid(error.to_string()))?;
                if !record.confirmed {
                    return Ok((Vec::new(), RecordOutcome::Confirmed(false)));
                }
                record.confirmed = false;
                let writes = vec![entry(record_key, encode(&record)?)];
                (writes, RecordOutcome::Confirmed(true))
            }
            (RecordChange::ConsumeConfirmation { .. }, None) => {
                (Vec::new(), RecordOutcome::Confirmed(false))
            }
        })
    }

    fn stale(&self) -> Option<StaleUpload> {
        match &self.change {
            RecordChange::BindImport { stale, .. } => stale.clone(),
            _ => None,
        }
    }

    /// Decides the change for the stored value of the key and writes it.
    fn apply(&mut self, stored: Option<&[u8]>) -> Effects {
        let (writes, outcome) = match self.decide(stored) {
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

    /// Checks the bound stale upload again from the reads of this transaction.
    fn check_stale(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let (Some(stale), Ok([(_, binding), (_, upload), (_, dedup)])) =
            (self.stale(), <[(Key, Option<Value>); 3]>::try_from(values))
        else {
            return self.fail(RecordError::Unexpected);
        };
        if binding.as_deref() != Some(stale.upload_id.to_bytes().as_slice()) {
            return self.apply(binding.as_deref());
        }
        if let Some(upload) = upload {
            return match postcard::from_bytes(upload.as_ref()) {
                Ok(upload) => self.confirm(&stale, Some(upload), false),
                Err(error) => self.fail(RecordError::Invalid(error.to_string())),
            };
        }
        match dedup.and_then(|value| parse_dedup_value(value.as_ref()).ok()) {
            Some((job_id, _)) => {
                self.state = State::Job;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: JOB_KEYSPACE.to_string(),
                    key: job_record_key(job_id),
                    txn_id: self.txn_id,
                })]
            }
            None => self.confirm(&stale, None, false),
        }
    }

    /// Confirms the still bound `stale` upload when [`upload_stale`] holds, then decides.
    fn confirm(
        &mut self,
        stale: &StaleUpload,
        upload: Option<RoCrateUploadRecord>,
        job_held: bool,
    ) -> Effects {
        if upload_stale(upload.as_ref(), job_held, stale.now_ms) {
            let mut retire = Vec::new();
            // Pinning the expiry makes a racing claim conflict on commit, then refuse the upload.
            if let Some(mut upload) = upload {
                upload.expires_at_ms = 0;
                match encode(&upload) {
                    Ok(value) => retire.push((
                        ROCRATE_UPLOAD_KEYSPACE.to_string(),
                        upload_key(stale.upload_id),
                        value,
                    )),
                    Err(error) => return self.fail(error),
                }
            }
            self.confirmed = Some((stale.upload_id, retire));
        }
        self.apply(Some(stale.upload_id.to_bytes().as_slice()))
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
        self.confirmed = None;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (State::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = State::Read;
                let key: Key = self.key().to_vec().into();
                // A stale upload is read in this transaction, so a racing claim conflicts.
                if let Some(stale) = self.stale() {
                    let reads = vec![
                        (FEDERATION_KEYSPACE.to_string(), key),
                        (
                            ROCRATE_UPLOAD_KEYSPACE.to_string(),
                            upload_key(stale.upload_id),
                        ),
                        (DEDUP_INDEX_KEYSPACE.to_string(), stale.dedup_key.into()),
                    ];
                    return smallvec![Effect::Storage(StorageEffect::BatchRead {
                        reads,
                        txn_id: Some(txn_id),
                    })];
                }
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: FEDERATION_KEYSPACE.to_string(),
                    key,
                    txn_id: Some(txn_id),
                })]
            }
            (State::Read, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.apply(value.as_deref())
            }
            (State::Read, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.check_stale(values)
            }
            // The stale upload is gone; it stays bound while an import job holds the key.
            (State::Job, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let held = value.is_some_and(|value| decode_job_record(value.as_ref()).is_ok());
                match self.stale() {
                    Some(stale) => self.confirm(&stale, None, held),
                    None => self.fail(RecordError::Unexpected),
                }
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

    /// Binds upload `[2; 16]` to the import key, replacing a still stale `stale` upload.
    fn binding(stale: Option<StaleUpload>) -> RecordChange {
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
            confirmed: false,
        };
        RecordChange::BindImport {
            key: b"upload".to_vec(),
            record_key: b"import".to_vec(),
            upload_id: Ulid::from_bytes([2; 16]),
            record: import,
            stale,
        }
    }

    /// Upload `upload_id`, found stale at time 10.
    fn stale(upload_id: Ulid) -> Option<StaleUpload> {
        Some(StaleUpload {
            upload_id,
            dedup_key: b"dedup".to_vec(),
            now_ms: 10,
        })
    }

    /// The rows a stale binding check reads; with `job`, the dedup row names job `[7; 16]`.
    fn checked(binding: Ulid, upload: Option<&RoCrateUploadRecord>, job: bool) -> Event {
        let upload = upload.map(|upload| postcard::to_allocvec(upload).unwrap());
        let job_id = aruna_core::structs::execution::job::JobId::from_bytes([7; 16]);
        let dedup =
            job.then(|| aruna_core::structs::execution::job::encode_dedup_value(job_id, [0; 32]));
        let values = [Some(binding.to_bytes().to_vec()), upload, dedup];
        Event::Storage(StorageEvent::BatchReadResult {
            values: values
                .into_iter()
                .map(|value| (Vec::new().into(), value.map(Value::from)))
                .collect(),
        })
    }

    fn wrote(effects: &Effects) -> Option<Writes> {
        match effects.first() {
            Some(Effect::Storage(StorageEffect::BatchWrite { writes, .. })) => Some(writes.clone()),
            _ => None,
        }
    }

    #[test]
    fn first_binding_wins() {
        // Another upload already bound to the key is kept and nothing is written.
        let winner = Ulid::from_bytes([1; 16]);
        let mut operation = RecordOperation::new(binding(None));
        operation.start();
        operation.step(started(1));
        let effects = operation.step(read(Some(winner.to_bytes().to_vec())));
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::CommitTransaction { .. })
        ));
        operation.step(committed(1));
        assert_eq!(operation.finalize(), Ok(RecordOutcome::Bound(winner)));
        // A stale upload gives way in the same transaction; a newer binding is kept.
        let replace = |stored: Ulid| {
            let mut operation = RecordOperation::new(binding(stale(winner)));
            operation.start();
            operation.step(started(2));
            let effects = operation.step(checked(stored, None, false));
            let wrote = wrote(&effects).is_some();
            if wrote {
                operation.step(Event::Storage(StorageEvent::BatchWriteResult {
                    entries: Vec::new(),
                }));
            }
            operation.step(committed(2));
            (wrote, operation.finalize())
        };
        let replaced = Ulid::from_bytes([2; 16]);
        assert_eq!(replace(winner), (true, Ok(RecordOutcome::Bound(replaced))));
        let newer = Ulid::from_bytes([3; 16]);
        assert_eq!(replace(newer), (false, Ok(RecordOutcome::Bound(newer))));
    }

    #[test]
    fn racing_claim_rechecked() {
        // A claim that commits after the caller found the upload stale conflicts with the
        // binding transaction or with its retired upload; the retry keeps the claimed upload.
        let old = Ulid::from_bytes([1; 16]);
        let owner = grant_record().principal;
        let mut upload = crate::jobs::import::upload_record(owner, old, 10);
        let mut operation = RecordOperation::new(binding(stale(old)));
        operation.start();
        operation.step(started(1));
        let writes = wrote(&operation.step(checked(old, Some(&upload), false))).unwrap();
        let retired: RoCrateUploadRecord = postcard::from_bytes(&writes[2].2).unwrap();
        assert_eq!((writes.len(), retired.expires_at_ms), (3, 0));
        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        let conflict = StorageError::TransactionConflict;
        operation.step(Event::Storage(StorageEvent::Error { error: conflict }));
        operation.step(started(2));
        upload.claimed_by = Some(aruna_core::structs::execution::job::JobId::from_bytes(
            [7; 16],
        ));
        let effects = operation.step(checked(old, Some(&upload), false));
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::CommitTransaction { .. })
        ));
        operation.step(committed(2));
        assert_eq!(operation.finalize(), Ok(RecordOutcome::Bound(old)));
    }

    #[test]
    fn held_upload_kept() {
        // A gone upload stays bound while the import job of its key exists in this transaction.
        use aruna_core::structs::execution::job::{JobId, JobPayload, JobRecord};
        let old = Ulid::from_bytes([1; 16]);
        let node = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let payload = JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        };
        let principal = grant_record().principal;
        let job = JobRecord::new(
            JobId::from_bytes([7; 16]),
            payload,
            principal,
            node,
            1,
            1,
            None,
        );
        let job = job.to_bytes().unwrap();
        for (stored, kept) in [(Some(job), true), (None, false)] {
            let mut operation = RecordOperation::new(binding(stale(old)));
            operation.start();
            operation.step(started(1));
            let effects = operation.step(checked(old, None, true));
            assert!(matches!(
                &effects[0],
                Effect::Storage(StorageEffect::Read { key_space, .. }) if key_space == JOB_KEYSPACE
            ));
            let effects = operation.step(read(stored));
            assert_eq!(wrote(&effects).is_none(), kept);
        }
    }
}
