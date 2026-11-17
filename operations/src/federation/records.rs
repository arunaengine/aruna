//! Node-local transfer records changed inside one storage transaction, so racing requests keep
//! the first winner and a revocation is never lost.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::federation::Signed;
use aruna_core::keyspaces::{
    DEDUP_INDEX_KEYSPACE, FEDERATION_KEYSPACE, JOB_KEYSPACE, ROCRATE_UPLOAD_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{decode, done, encode, encoded_row, read_row, values};
use aruna_core::structs::execution::job::{
    JobRecord, RoCrateUploadRecord, job_record_key, parse_dedup_value,
};
use aruna_core::transfer::{ExportGrant, ImportIntent};
use aruna_core::types::{Key, Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use aruna_core::structs::execution::job::{upload_key, upload_stale};
use aruna_core::transfer::GrantRecord;

type Writes = Vec<(String, Key, Value)>;

/// One conditional change of a transfer record.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RecordChange {
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

impl RecordChange {
    /// The record key the change reads and writes; it also seeds the runner's retry backoff.
    pub(crate) fn key(&self) -> &[u8] {
        match self {
            RecordChange::StoreGrant { key, .. }
            | RecordChange::RevokeGrant { key }
            | RecordChange::BindImport { key, .. }
            | RecordChange::ConsumeConfirmation { record_key: key } => key,
        }
    }
}

/// A bound upload the caller found stale, checked again inside the binding transaction.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StaleUpload {
    pub upload_id: Ulid,
    /// The job dedup index key of the import key; its job keeps a gone upload bound.
    pub dedup_key: Vec<u8>,
    pub now_ms: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RecordOutcome {
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
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("transfer record is invalid: {0}")]
    Invalid(String),
}

#[derive(Debug, PartialEq)]
enum Phase {
    Read,
    Job,
    Write,
}

type Turn = Result<Next<RecordOutcome>, RecordError>;

/// Changes one transfer record in the runner's write transaction; a lost conflict reruns it.
#[derive(Debug, PartialEq)]
pub(crate) struct RecordOperation {
    change: RecordChange,
    phase: Phase,
    /// The bound upload this attempt confirmed stale, with the writes that retire it.
    confirmed: Option<(Ulid, Writes)>,
    pending: Option<RecordOutcome>,
}

fn entry(key: &[u8], value: Value) -> (String, Key, Value) {
    (FEDERATION_KEYSPACE.to_string(), key.to_vec().into(), value)
}

impl RecordOperation {
    pub fn new(change: RecordChange) -> Self {
        Self {
            change,
            phase: Phase::Read,
            confirmed: None,
            pending: None,
        }
    }

    /// The writes and outcome for the stored value of the key.
    fn decide(&self, stored: Option<&[u8]>) -> Result<(Writes, RecordOutcome), RecordError> {
        Ok(match (&self.change, stored) {
            (RecordChange::StoreGrant { .. }, Some(bytes)) => {
                (Vec::new(), RecordOutcome::Grant(decode(bytes)?))
            }
            (RecordChange::StoreGrant { key, record }, None) => (
                vec![entry(key, encode(record)?)],
                RecordOutcome::Grant(record.clone()),
            ),
            (RecordChange::RevokeGrant { key }, Some(bytes)) => {
                let mut record: GrantRecord = decode(bytes)?;
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
                let mut record: ImportRecord = decode(bytes)?;
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
    fn apply(&mut self, stored: Option<&[u8]>) -> Turn {
        let (writes, outcome) = self.decide(stored)?;
        if writes.is_empty() {
            return Ok(Next::Done(outcome));
        }
        self.pending = Some(outcome);
        self.phase = Phase::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// Checks the bound stale upload again from the reads of this transaction.
    fn check_stale(&mut self, stale: &StaleUpload, result: IoResult) -> Turn {
        let [binding, upload, dedup] = values::<3>(result)?;
        if binding.as_deref() != Some(stale.upload_id.to_bytes().as_slice()) {
            return self.apply(binding.as_deref());
        }
        if let Some(upload) = upload {
            return self.confirm(stale, Some(decode(&upload)?), false);
        }
        match dedup.and_then(|value| parse_dedup_value(value.as_ref()).ok()) {
            Some((job_id, _)) => {
                self.phase = Phase::Job;
                Ok(Next::Call(read_row(JOB_KEYSPACE, job_record_key(job_id))))
            }
            None => self.confirm(stale, None, false),
        }
    }

    /// Confirms the still bound `stale` upload when [`upload_stale`] holds, then decides.
    fn confirm(
        &mut self,
        stale: &StaleUpload,
        upload: Option<RoCrateUploadRecord>,
        job_held: bool,
    ) -> Turn {
        if upload_stale(upload.as_ref(), job_held, stale.now_ms) {
            let mut retire = Vec::new();
            // Pinning the expiry makes a racing claim conflict on commit, then refuse the upload.
            if let Some(mut upload) = upload {
                upload.expires_at_ms = 0;
                let key = upload_key(stale.upload_id);
                retire.push(encoded_row(ROCRATE_UPLOAD_KEYSPACE, key, &upload)?);
            }
            self.confirmed = Some((stale.upload_id, retire));
        }
        self.apply(Some(stale.upload_id.to_bytes().as_slice()))
    }
}

impl Operation for RecordOperation {
    type Output = RecordOutcome;
    type Error = RecordError;

    fn first(&mut self) -> Turn {
        let key = (
            FEDERATION_KEYSPACE.to_string(),
            self.change.key().to_vec().into(),
        );
        // A stale upload is read in this transaction, so a racing claim conflicts.
        let reads = match self.stale() {
            Some(stale) => vec![
                key,
                (
                    ROCRATE_UPLOAD_KEYSPACE.to_string(),
                    upload_key(stale.upload_id),
                ),
                (DEDUP_INDEX_KEYSPACE.to_string(), stale.dedup_key.into()),
            ],
            None => vec![key],
        };
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match (&self.phase, self.stale()) {
            (Phase::Read, Some(stale)) => self.check_stale(&stale, result),
            (Phase::Read, None) => {
                let [stored] = values::<1>(result)?;
                self.apply(stored.as_deref())
            }
            // The stale upload is gone; it stays bound while an import job holds the key.
            (Phase::Job, Some(stale)) => {
                let [job] = values::<1>(result)?;
                let held = job.is_some_and(|value| JobRecord::from_bytes(value.as_ref()).is_ok());
                self.confirm(&stale, None, held)
            }
            (Phase::Write, _) => {
                done(result)?;
                let unfinished = RecordError::Io(IoError::Unfinished);
                self.pending.take().map(Next::Done).ok_or(unfinished)
            }
            (Phase::Job, None) => Err(IoError::Unfinished.into()),
        }
    }

    fn transaction(&self) -> bool {
        true
    }

    /// Six attempts in all, as the change's own retry had.
    fn conflict_retries() -> u32 {
        5
    }
}

/// Binds an upload of another realm's artifact to the intent and grant it arrived with.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportRecord {
    pub intent: Signed<ImportIntent>,
    pub grant: Signed<ExportGrant>,
    /// The source confirmed the grant since the import job last started; a start consumes it.
    pub confirmed: bool,
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

    fn stored(rows: Vec<Option<Vec<u8>>>) -> IoResult {
        IoResult::Values(rows.into_iter().map(|row| row.map(Value::from)).collect())
    }

    #[test]
    fn racing_revocation_wins() {
        // A grant stored after a conflicting revocation rereads and keeps the revoked record.
        let record = grant_record();
        let change = RecordChange::StoreGrant {
            key: b"grant".to_vec(),
            record: record.clone(),
        };
        let mut operation = RecordOperation::new(change.clone());
        let read = vec![(FEDERATION_KEYSPACE.to_string(), b"grant".to_vec().into())];
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(read))));
        assert!(wrote(&operation.next(stored(vec![None]))).is_some());
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Done(RecordOutcome::Grant(record.clone())))
        );
        // The runner reruns a lost commit as a fresh change, which rereads the record.
        assert!(RecordOperation::is_conflict(&IoError::Conflict.into()));
        assert_eq!(RecordOperation::conflict_retries(), 5);
        let mut operation = RecordOperation::new(change);
        operation.first().unwrap();
        let revoked = GrantRecord {
            revoked: true,
            ..record
        };
        let row = postcard::to_allocvec(&revoked).unwrap();
        assert_eq!(
            operation.next(stored(vec![Some(row)])),
            Ok(Next::Done(RecordOutcome::Grant(revoked)))
        );
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
    fn checked(binding: Ulid, upload: Option<&RoCrateUploadRecord>, job: bool) -> IoResult {
        let upload = upload.map(|upload| postcard::to_allocvec(upload).unwrap());
        let job_id = aruna_core::structs::execution::job::JobId::from_bytes([7; 16]);
        let dedup =
            job.then(|| aruna_core::structs::execution::job::encode_dedup_value(job_id, [0; 32]));
        stored(vec![Some(binding.to_bytes().to_vec()), upload, dedup])
    }

    fn wrote(turn: &Turn) -> Option<Writes> {
        match turn {
            Ok(Next::Call(IoCall::Write(writes))) => Some(writes.clone()),
            _ => None,
        }
    }

    #[test]
    fn first_binding_wins() {
        // Another upload already bound to the key is kept and nothing is written.
        let winner = Ulid::from_bytes([1; 16]);
        let mut operation = RecordOperation::new(binding(None));
        operation.first().unwrap();
        assert_eq!(
            operation.next(stored(vec![Some(winner.to_bytes().to_vec())])),
            Ok(Next::Done(RecordOutcome::Bound(winner)))
        );
        // A stale upload gives way in the same transaction; a newer binding is kept.
        let replace = |bound: Ulid| {
            let mut operation = RecordOperation::new(binding(stale(winner)));
            operation.first().unwrap();
            let turn = operation.next(checked(bound, None, false));
            match wrote(&turn) {
                Some(_) => (true, operation.next(IoResult::Done)),
                None => (false, turn),
            }
        };
        let replaced = Ulid::from_bytes([2; 16]);
        let bound = |upload| -> Turn { Ok(Next::Done(RecordOutcome::Bound(upload))) };
        assert_eq!(replace(winner), (true, bound(replaced)));
        let newer = Ulid::from_bytes([3; 16]);
        assert_eq!(replace(newer), (false, bound(newer)));
    }

    #[test]
    fn racing_claim_rechecked() {
        // A claim that commits after the caller found the upload stale conflicts with the
        // binding transaction or with its retired upload; the retry keeps the claimed upload.
        let old = Ulid::from_bytes([1; 16]);
        let owner = grant_record().principal;
        let mut upload = crate::jobs::import::upload::tests::upload_record(owner, old, 10);
        let mut operation = RecordOperation::new(binding(stale(old)));
        operation.first().unwrap();
        let writes = wrote(&operation.next(checked(old, Some(&upload), false))).unwrap();
        let retired: RoCrateUploadRecord = postcard::from_bytes(&writes[2].2).unwrap();
        assert_eq!((writes.len(), retired.expires_at_ms), (3, 0));
        let mut operation = RecordOperation::new(binding(stale(old)));
        operation.first().unwrap();
        upload.claimed_by = Some(aruna_core::structs::execution::job::JobId::from_bytes(
            [7; 16],
        ));
        assert_eq!(
            operation.next(checked(old, Some(&upload), false)),
            Ok(Next::Done(RecordOutcome::Bound(old)))
        );
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
        for (row, kept) in [(Some(job), true), (None, false)] {
            let mut operation = RecordOperation::new(binding(stale(old)));
            operation.first().unwrap();
            let read = read_row(JOB_KEYSPACE, job_record_key(JobId::from_bytes([7; 16])));
            assert_eq!(
                operation.next(checked(old, None, true)),
                Ok(Next::Call(read))
            );
            let turn = operation.next(stored(vec![row]));
            assert_eq!(wrote(&turn).is_none(), kept);
        }
    }
}
