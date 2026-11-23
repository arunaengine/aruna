//! Purges expired S3 sessions in batches and clears their expiry and owner index rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    PURGE_BATCH, S3SessionError, decode_index, encode_index, expiry_key, expiry_parts, expiry_secs,
    owner_key,
};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{S3_SESSION_KEYSPACE, SESSION_EXPIRY_KEYSPACE, SESSION_OWNER_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::s3_session::S3Session;
use aruna_core::types::{Key, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExpiryCandidate {
    index_key: Key,
    owner_key: Option<Vec<u8>>,
    access_key: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PurgeSessionsResult {
    pub scanned: usize,
    pub purged: usize,
    pub removed: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PurgePhase {
    Scan,
    Sessions,
    Owners,
    Write,
    Delete,
}

#[derive(Debug, PartialEq)]
pub struct PurgeSessionsOperation {
    now: SystemTime,
    candidates: Vec<ExpiryCandidate>,
    removals: BTreeMap<Vec<u8>, BTreeSet<String>>,
    writes: Vec<(String, Key, Value)>,
    deletes: Vec<(String, Key)>,
    result: PurgeSessionsResult,
    phase: PurgePhase,
}

impl PurgeSessionsOperation {
    pub fn new(now: SystemTime) -> Self {
        Self {
            now,
            candidates: Vec::new(),
            removals: BTreeMap::new(),
            writes: Vec::new(),
            deletes: Vec::new(),
            result: PurgeSessionsResult::default(),
            phase: PurgePhase::Scan,
        }
    }

    /// Takes the expired index rows of the first page; malformed rows only go.
    fn expiry_scanned(
        &mut self,
        rows: Vec<(Key, Value)>,
    ) -> Result<Next<PurgeSessionsResult>, S3SessionError> {
        let now_secs = expiry_secs(self.now)?;
        for (index_key, owner_value) in rows {
            let seconds = (index_key.as_ref().get(..8))
                .and_then(|bytes| bytes.try_into().ok())
                .map(u64::from_be_bytes);
            let Some(seconds) = seconds else {
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
        self.phase = PurgePhase::Sessions;
        let reads = (self.candidates.iter())
            .map(|candidate| {
                (
                    S3_SESSION_KEYSPACE.to_string(),
                    candidate.access_key.as_bytes().into(),
                )
            })
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// Decides each candidate: an expired session goes with its rows, a stale row alone.
    fn sessions_read(
        &mut self,
        values: Vec<Option<Value>>,
    ) -> Result<Next<PurgeSessionsResult>, S3SessionError> {
        if values.len() != self.candidates.len() {
            return Err(S3SessionError::IndexInconsistent);
        }
        for (candidate, value) in std::mem::take(&mut self.candidates).into_iter().zip(values) {
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
            let Ok(stored_key) = expiry_key(session.expiry, &session.access_key) else {
                self.delete_candidate(&candidate, true);
                continue;
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
            (self.removals.entry(actual_owner).or_default()).insert(candidate.access_key.clone());
            self.delete_candidate(&candidate, true);
            self.result.purged += 1;
        }
        self.phase = PurgePhase::Owners;
        let reads = (self.removals.keys())
            .map(|key| (SESSION_OWNER_KEYSPACE.to_string(), key.clone().into()))
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// Drops the purged sessions from their owner indexes; an emptied index goes.
    fn owners_read(
        &mut self,
        values: Vec<Option<Value>>,
    ) -> Result<Next<PurgeSessionsResult>, S3SessionError> {
        if values.len() != self.removals.len() {
            return Err(S3SessionError::IndexInconsistent);
        }
        for ((key, removals), value) in std::mem::take(&mut self.removals).into_iter().zip(values) {
            let mut index = decode_index(value.as_ref())?;
            for access_key in removals {
                index.remove(&access_key);
            }
            let key: Key = key.into();
            match index.is_empty() {
                true => self.deletes.push((SESSION_OWNER_KEYSPACE.to_string(), key)),
                false => {
                    let value = encode_index(&index)?;
                    self.writes
                        .push((SESSION_OWNER_KEYSPACE.to_string(), key, value));
                }
            }
        }
        self.phase = PurgePhase::Write;
        Ok(Next::Call(IoCall::Write(std::mem::take(&mut self.writes))))
    }

    fn delete_index(&mut self, index_key: Key) {
        self.deletes
            .push((SESSION_EXPIRY_KEYSPACE.to_string(), index_key));
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
}

impl Operation for PurgeSessionsOperation {
    type Output = PurgeSessionsResult;
    type Error = S3SessionError;

    fn first(&mut self) -> Result<Next<PurgeSessionsResult>, S3SessionError> {
        expiry_secs(self.now).map_err(|_| S3SessionError::InvalidExpiry)?;
        Ok(Next::Call(IoCall::Page {
            space: SESSION_EXPIRY_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: PURGE_BATCH,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PurgeSessionsResult>, S3SessionError> {
        match self.phase {
            PurgePhase::Scan => self.expiry_scanned(result.page()?.0),
            PurgePhase::Sessions => self.sessions_read(result.values()?),
            PurgePhase::Owners => self.owners_read(result.values()?),
            PurgePhase::Write => {
                result.done()?;
                self.phase = PurgePhase::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            PurgePhase::Delete => {
                result.done()?;
                Ok(Next::Done(self.result.clone()))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn page(rows: Vec<(Key, Value)>) -> IoResult {
        IoResult::Page { rows, next: None }
    }

    #[test]
    fn scans_expiry_rows() {
        let mut purge =
            PurgeSessionsOperation::new(SystemTime::UNIX_EPOCH + Duration::from_secs(10));
        let scan = IoCall::Page {
            space: SESSION_EXPIRY_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: PURGE_BATCH,
        };
        assert_eq!(purge.first(), Ok(Next::Call(scan)));
        assert_eq!(
            purge.transaction(),
            aruna_core::operation::TransactionMode::Write
        );
        // A malformed index row only goes; nothing else is read.
        let malformed = vec![(Key::from(vec![1u8]), Value::from(Vec::new()))];
        assert_eq!(
            purge.next(page(malformed)),
            Ok(Next::Call(IoCall::Read(Vec::new())))
        );
        assert_eq!(
            purge.next(IoResult::Values(Vec::new())),
            Ok(Next::Call(IoCall::Read(Vec::new())))
        );
        assert_eq!(
            purge.next(IoResult::Values(Vec::new())),
            Ok(Next::Call(IoCall::Write(Vec::new())))
        );
        let deletes = vec![(SESSION_EXPIRY_KEYSPACE.to_string(), Key::from(vec![1u8]))];
        assert_eq!(
            purge.next(IoResult::Done),
            Ok(Next::Call(IoCall::Delete(deletes)))
        );
        let done = PurgeSessionsResult {
            scanned: 1,
            purged: 0,
            removed: 1,
        };
        assert_eq!(purge.next(IoResult::Done), Ok(Next::Done(done)));
    }

    #[test]
    fn refuses_short_answers() {
        let mut purge = PurgeSessionsOperation::new(SystemTime::UNIX_EPOCH);
        purge.phase = PurgePhase::Sessions;
        purge.candidates = vec![ExpiryCandidate {
            index_key: Key::from(vec![0u8; 8]),
            owner_key: None,
            access_key: "a".to_string(),
        }];
        let short = purge.next(IoResult::Values(Vec::new()));
        assert_eq!(short, Err(S3SessionError::IndexInconsistent));
    }
}
