use super::{
    PURGE_BATCH, S3SessionError, decode_index, encode_index, expiry_key, expiry_parts, expiry_secs,
    owner_key,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    S3_SESSION_EXPIRY_KEYSPACE, S3_SESSION_KEYSPACE, S3_SESSION_OWNER_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::S3Session;
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExpiryCandidate {
    index_key: Key,
    owner_key: Option<Vec<u8>>,
    access_key: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PurgeS3SessionsResult {
    pub scanned: usize,
    pub purged: usize,
    pub removed: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PurgeSessionState {
    Init,
    StartTransaction,
    ScanExpiry,
    ReadSessions,
    ReadOwners,
    WriteOwners,
    DeleteRows,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct PurgeS3SessionsOperation {
    now: SystemTime,
    candidates: Vec<ExpiryCandidate>,
    removals: BTreeMap<Vec<u8>, BTreeSet<String>>,
    writes: Vec<(String, Key, aruna_core::types::Value)>,
    deletes: Vec<(String, Key)>,
    result: PurgeS3SessionsResult,
    txn_id: Option<Ulid>,
    state: PurgeSessionState,
    output: Result<PurgeS3SessionsResult, S3SessionError>,
}

impl PurgeS3SessionsOperation {
    pub fn new(now: SystemTime) -> Self {
        Self {
            now,
            candidates: Vec::new(),
            removals: BTreeMap::new(),
            writes: Vec::new(),
            deletes: Vec::new(),
            result: PurgeS3SessionsResult::default(),
            txn_id: None,
            state: PurgeSessionState::Init,
            output: Err(S3SessionError::NotFinished),
        }
    }

    fn fail(&mut self, error: S3SessionError) -> Effects {
        self.state = PurgeSessionState::Error;
        self.output = Err(error);
        self.abort()
    }

    fn start_purge(&mut self) -> Effects {
        if !matches!(self.state, PurgeSessionState::Init) {
            return self.fail(S3SessionError::Failed);
        }
        if expiry_secs(self.now).is_err() {
            return self.fail(S3SessionError::InvalidExpiry);
        }
        self.state = PurgeSessionState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected(event, "StorageEvent::TransactionStarted");
        };
        self.txn_id = Some(txn_id);
        self.state = PurgeSessionState::ScanExpiry;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_SESSION_EXPIRY_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: PURGE_BATCH,
            txn_id: Some(txn_id),
        })]
    }

    fn expiry_scanned(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.unexpected(event, "StorageEvent::IterResult");
        };
        let now_secs = match expiry_secs(self.now) {
            Ok(seconds) => seconds,
            Err(error) => return self.fail(error),
        };
        for (index_key, owner_value) in values {
            let Some(seconds) = index_key
                .as_ref()
                .get(..8)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u64::from_be_bytes)
            else {
                self.result.scanned += 1;
                self.delete_index(index_key);
                continue;
            };
            if seconds > now_secs {
                break;
            }
            self.result.scanned += 1;
            let Some((_, access_key)) = expiry_parts(index_key.as_ref()) else {
                self.delete_index(index_key);
                continue;
            };
            self.candidates.push(ExpiryCandidate {
                index_key,
                owner_key: (owner_value.len() == 64).then(|| owner_value.as_ref().to_vec()),
                access_key,
            });
        }
        if self.candidates.is_empty() {
            return self.write_owners();
        }
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        self.state = PurgeSessionState::ReadSessions;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: self
                .candidates
                .iter()
                .map(|candidate| {
                    (
                        S3_SESSION_KEYSPACE.to_string(),
                        candidate.access_key.as_bytes().into(),
                    )
                })
                .collect(),
            txn_id: Some(txn_id),
        })]
    }

    fn sessions_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected(event, "StorageEvent::BatchReadResult");
        };
        if values.len() != self.candidates.len() {
            return self.fail(S3SessionError::IndexInconsistent);
        }
        for (candidate, (_, value)) in self.candidates.clone().into_iter().zip(values) {
            let Some(value) = value else {
                self.delete_candidate(&candidate, false);
                continue;
            };
            let session = match S3Session::from_bytes(value.as_ref()) {
                Ok(session) if session.access_key == candidate.access_key => session,
                Ok(_) | Err(_) => {
                    self.delete_candidate(&candidate, true);
                    continue;
                }
            };
            let stored_key = match expiry_key(session.expiry, &session.access_key) {
                Ok(key) => key,
                Err(_) => {
                    self.delete_candidate(&candidate, true);
                    continue;
                }
            };
            if stored_key != candidate.index_key {
                self.delete_index(candidate.index_key);
                continue;
            }
            if !session.is_expired(self.now) {
                continue;
            }
            let actual_owner = owner_key(session.user_identity, session.group_id)
                .as_ref()
                .to_vec();
            self.removals
                .entry(actual_owner)
                .or_default()
                .insert(candidate.access_key.clone());
            self.delete_candidate(&candidate, true);
            self.result.purged += 1;
        }
        self.read_owners()
    }

    fn read_owners(&mut self) -> Effects {
        if self.removals.is_empty() {
            return self.write_owners();
        }
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        self.state = PurgeSessionState::ReadOwners;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: self
                .removals
                .keys()
                .map(|key| (S3_SESSION_OWNER_KEYSPACE.to_string(), key.clone().into()))
                .collect(),
            txn_id: Some(txn_id),
        })]
    }

    fn owners_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected(event, "StorageEvent::BatchReadResult");
        };
        if values.len() != self.removals.len() {
            return self.fail(S3SessionError::IndexInconsistent);
        }
        for (key, value) in values {
            let removals = self.removals.get(key.as_ref()).cloned().unwrap_or_default();
            let mut index = match decode_index(value.as_ref()) {
                Ok(index) => index,
                Err(error) => return self.fail(error),
            };
            for access_key in removals {
                index.remove(&access_key);
            }
            if index.is_empty() {
                self.deletes
                    .push((S3_SESSION_OWNER_KEYSPACE.to_string(), key));
            } else {
                let value = match encode_index(&index) {
                    Ok(value) => value,
                    Err(error) => return self.fail(error),
                };
                self.writes
                    .push((S3_SESSION_OWNER_KEYSPACE.to_string(), key, value));
            }
        }
        self.write_owners()
    }

    fn write_owners(&mut self) -> Effects {
        if self.writes.is_empty() {
            return self.delete_rows();
        }
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        self.state = PurgeSessionState::WriteOwners;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: std::mem::take(&mut self.writes),
            txn_id: Some(txn_id),
        })]
    }

    fn owners_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.unexpected(event, "StorageEvent::BatchWriteResult");
        };
        self.delete_rows()
    }

    fn delete_rows(&mut self) -> Effects {
        if self.deletes.is_empty() {
            return self.commit_transaction();
        }
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        self.state = PurgeSessionState::DeleteRows;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: std::mem::take(&mut self.deletes),
            txn_id: Some(txn_id),
        })]
    }

    fn rows_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.unexpected(event, "StorageEvent::BatchDeleteResult");
        };
        self.commit_transaction()
    }

    fn commit_transaction(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        self.state = PurgeSessionState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected(event, "StorageEvent::TransactionCommitted");
        };
        self.txn_id = None;
        self.output = Ok(self.result.clone());
        self.state = PurgeSessionState::Finish;
        smallvec![]
    }

    fn delete_index(&mut self, index_key: Key) {
        self.deletes
            .push((S3_SESSION_EXPIRY_KEYSPACE.to_string(), index_key));
        self.result.removed += 1;
    }

    fn delete_candidate(&mut self, candidate: &ExpiryCandidate, primary: bool) {
        if let Some(owner_key) = candidate.owner_key.as_ref() {
            self.removals
                .entry(owner_key.clone())
                .or_default()
                .insert(candidate.access_key.clone());
        }
        if primary {
            self.deletes.push((
                S3_SESSION_KEYSPACE.to_string(),
                candidate.access_key.as_bytes().into(),
            ));
        }
        self.delete_index(candidate.index_key.clone());
    }

    fn unexpected(&mut self, received: Event, expected: &'static str) -> Effects {
        self.fail(S3SessionError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            received,
        })
    }
}

impl Operation for PurgeS3SessionsOperation {
    type Output = PurgeS3SessionsResult;
    type Error = S3SessionError;

    fn start(&mut self) -> Effects {
        self.start_purge()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.fail(error.clone().into());
        }
        match self.state {
            PurgeSessionState::StartTransaction => self.transaction_started(event),
            PurgeSessionState::ScanExpiry => self.expiry_scanned(event),
            PurgeSessionState::ReadSessions => self.sessions_read(event),
            PurgeSessionState::ReadOwners => self.owners_read(event),
            PurgeSessionState::WriteOwners => self.owners_written(event),
            PurgeSessionState::DeleteRows => self.rows_deleted(event),
            PurgeSessionState::CommitTransaction => self.transaction_committed(event),
            PurgeSessionState::Init | PurgeSessionState::Finish | PurgeSessionState::Error => {
                self.unexpected(event, "valid session operation event")
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            PurgeSessionState::Finish | PurgeSessionState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}
