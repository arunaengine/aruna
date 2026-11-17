//! Private state of an install on its receiver: values by install and key, changed and removed by
//! compare and swap, with the install's byte count and the call's settled reply in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::PLUGIN_STATE_KEYSPACE;
use aruna_plugins::broker::{Entry, ErrorKind, HostError, StatePut, StateRemove};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::intents::settled_row;
use super::rows::{Answer, Ask, Plan, Row, RowsOperation, Turn, row, unexpected, values, StoreError, decode, encode};

/// Bytes of one key, of one value, and of all keys and values of one install.
pub const MAX_STATE_KEY: usize = 256;
pub const MAX_STATE_VALUE: usize = 256 * 1024;
pub const MAX_STATE_BYTES: u64 = 16 << 20;

const VALUE_ROW: u8 = b'k';
const BYTES_ROW: u8 = b'b';
/// The last version the install's state handed out, kept across removals.
const VERSION_ROW: u8 = b'v';

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StateRow {
    version: u64,
    value: Vec<u8>,
}

/// Why a put changed nothing, or failed in storage.
#[derive(Debug, thiserror::Error)]
pub enum PutError {
    #[error("{0}")]
    Refused(String, ErrorKind),
    #[error(transparent)]
    Store(#[from] StoreError),
}

fn value_key(install_id: Ulid, key: &str) -> Vec<u8> {
    let mut row = vec![VALUE_ROW];
    row.extend_from_slice(&install_id.to_bytes());
    row.extend_from_slice(key.as_bytes());
    row
}

fn bytes_key(install_id: Ulid) -> Vec<u8> {
    let mut row = vec![BYTES_ROW];
    row.extend_from_slice(&install_id.to_bytes());
    row
}

fn version_key(install_id: Ulid) -> Vec<u8> {
    let mut row = vec![VERSION_ROW];
    row.extend_from_slice(&install_id.to_bytes());
    row
}

/// A key the guest may use: not empty, within its bound.
pub fn check_key(key: &str) -> Result<(), HostError> {
    match key.is_empty() || key.len() > MAX_STATE_KEY {
        true => Err(HostError::new(
            ErrorKind::Invalid,
            "a state key has 1 to 256 bytes",
        )),
        false => Ok(()),
    }
}

/// Reads the install's value at `key`.
pub fn read_state(install_id: Ulid, key: &str) -> RowsOperation<ReadState> {
    RowsOperation::direct(ReadState {
        key: value_key(install_id, key),
    })
}

#[derive(Debug, PartialEq)]
pub struct ReadState {
    key: Vec<u8>,
}

impl Plan for ReadState {
    type Output = Option<Entry>;
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<Self::Output>, StoreError> {
        Ok(Turn::Ask(Ask::Read(vec![(
            PLUGIN_STATE_KEYSPACE,
            self.key.clone(),
        )])))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<Self::Output>, StoreError> {
        let [row] = values::<1>(answer)?;
        let row = row.map(|bytes| decode::<StateRow>(&bytes)).transpose()?;
        Ok(Turn::Finish(row.map(|row| Entry {
            version: row.version,
            value: row.value,
        })))
    }
}

/// Writes the value if the stored version is still `expected` (`None`: no value yet) and the
/// install stays within its bytes, and settles the call's intent with the new version. Versions
/// never repeat within an install, so one read before a removal never matches a later value.
pub fn put_state(run_id: Ulid, install_id: Ulid, put: StatePut) -> RowsOperation<PutState> {
    RowsOperation::transaction(PutState {
        run_id,
        install_id,
        put,
        written: None,
    })
}

#[derive(Debug, PartialEq)]
pub struct PutState {
    run_id: Ulid,
    install_id: Ulid,
    put: StatePut,
    /// The new version once its rows are sent.
    written: Option<u64>,
}

impl Plan for PutState {
    type Output = u64;
    type Error = PutError;

    fn first(&mut self) -> Result<Turn<u64>, PutError> {
        if self.put.value.len() > MAX_STATE_VALUE {
            let message = "a state value has at most 256 KiB".to_string();
            return Err(PutError::Refused(message, ErrorKind::Limit));
        }
        Ok(Turn::Ask(Ask::Read(vec![
            (PLUGIN_STATE_KEYSPACE, value_key(self.install_id, &self.put.key)),
            (PLUGIN_STATE_KEYSPACE, bytes_key(self.install_id)),
            (PLUGIN_STATE_KEYSPACE, version_key(self.install_id)),
        ])))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<u64>, PutError> {
        if let Some(next) = self.written {
            return match answer {
                Answer::Done => Ok(Turn::Finish(next)),
                other => Err(unexpected(other).into()),
            };
        }
        let [current, total, last] = values::<3>(answer)?;
        let current = current.map(|bytes| decode::<StateRow>(&bytes)).transpose()?;
        let version = current.as_ref().map(|row| row.version);
        if version != self.put.expected {
            let message = format!("the value is at version {version:?}");
            return Err(PutError::Refused(message, ErrorKind::Conflict));
        }
        let total = total.map_or(Ok(0), |bytes| decode::<u64>(&bytes))?;
        let size = |value: &[u8]| (self.put.key.len() + value.len()) as u64;
        let old = current.as_ref().map_or(0, |row| size(&row.value));
        let total = (total.saturating_sub(old)).saturating_add(size(&self.put.value));
        if total > MAX_STATE_BYTES {
            let message = "the install's state would exceed 16 MiB".to_string();
            return Err(PutError::Refused(message, ErrorKind::Limit));
        }
        let last = last.map_or(Ok(0), |bytes| decode::<u64>(&bytes))?;
        let next = last.max(version.unwrap_or_default()) + 1;
        let state = StateRow {
            version: next,
            value: std::mem::take(&mut self.put.value),
        };
        let install_id = self.install_id;
        let rows = vec![
            row(PLUGIN_STATE_KEYSPACE, value_key(install_id, &self.put.key), encode(&state)?),
            row(PLUGIN_STATE_KEYSPACE, bytes_key(install_id), encode(&total)?),
            row(PLUGIN_STATE_KEYSPACE, version_key(install_id), encode(&next)?),
            settled_row(self.run_id, &self.put.intent, next.to_string())?,
        ];
        self.written = Some(next);
        Ok(Turn::Ask(Ask::Write(rows)))
    }
}

/// Removes the value if the stored version is still `expected`, frees its bytes, and settles the
/// call's intent; an absent value is a conflict, as it was not at that version.
pub fn remove_state(
    run_id: Ulid,
    install_id: Ulid,
    remove: StateRemove,
) -> RowsOperation<RemoveState> {
    RowsOperation::transaction(RemoveState {
        run_id,
        install_id,
        remove,
        phase: RemovePhase::Reading,
    })
}

#[derive(Debug, PartialEq)]
pub struct RemoveState {
    run_id: Ulid,
    install_id: Ulid,
    remove: StateRemove,
    phase: RemovePhase,
}

#[derive(Debug, PartialEq)]
enum RemovePhase {
    Reading,
    /// The value row is being deleted; these rows follow.
    Deleting(Vec<Row>),
    Writing,
}

impl Plan for RemoveState {
    type Output = ();
    type Error = PutError;

    fn first(&mut self) -> Result<Turn<()>, PutError> {
        Ok(Turn::Ask(Ask::Read(vec![
            (PLUGIN_STATE_KEYSPACE, value_key(self.install_id, &self.remove.key)),
            (PLUGIN_STATE_KEYSPACE, bytes_key(self.install_id)),
        ])))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<()>, PutError> {
        match (std::mem::replace(&mut self.phase, RemovePhase::Writing), answer) {
            (RemovePhase::Reading, answer) => {
                let [current, total] = values::<2>(answer)?;
                let current = current.map(|bytes| decode::<StateRow>(&bytes)).transpose()?;
                let Some(row) = current.filter(|row| row.version == self.remove.expected) else {
                    let message = format!("the value is not at version {}", self.remove.expected);
                    return Err(PutError::Refused(message, ErrorKind::Conflict));
                };
                let total = total.map_or(Ok(0), |bytes| decode::<u64>(&bytes))?;
                let freed = (self.remove.key.len() + row.value.len()) as u64;
                let total = encode(&total.saturating_sub(freed))?;
                let rows = vec![
                    row(PLUGIN_STATE_KEYSPACE, bytes_key(self.install_id), total),
                    settled_row(self.run_id, &self.remove.intent, String::new())?,
                ];
                self.phase = RemovePhase::Deleting(rows);
                let key = value_key(self.install_id, &self.remove.key);
                Ok(Turn::Ask(Ask::Delete(vec![(PLUGIN_STATE_KEYSPACE, key)])))
            }
            (RemovePhase::Deleting(rows), Answer::Done) => Ok(Turn::Ask(Ask::Write(rows))),
            (RemovePhase::Writing, Answer::Done) => Ok(Turn::Finish(())),
            (_, other) => Err(unexpected(other).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use crate::plugins::intents::{Begun, begin};
    use crate::plugins::runs::tests::opened;
    use aruna_plugins::broker::Intent;

    fn put(key: &str, value: &[u8], expected: Option<u64>, ordinal: u32) -> StatePut {
        StatePut {
            key: key.into(),
            value: value.to_vec(),
            expected,
            intent: Intent::new(ordinal, &[key.as_bytes(), value]),
        }
    }

    #[tokio::test]
    async fn compare_and_swap() {
        let (_dir, ctx) = opened();
        let (run, install, other) = (
            Ulid::from_parts(9, 1),
            Ulid::from_parts(1, 1),
            Ulid::from_parts(1, 2),
        );
        let first = put("cursor", b"a", None, 0);
        assert_eq!(drive(put_state(run, install, first.clone()), &ctx).await.unwrap(), 1);
        assert_eq!(
            drive(begin(run, first.intent.clone()), &ctx).await.unwrap(),
            Begun::Settled("1".into())
        );
        let stale = drive(put_state(run, install, put("cursor", b"b", None, 1)), &ctx).await;
        assert!(matches!(
            stale,
            Err(PutError::Refused(_, ErrorKind::Conflict))
        ));
        assert_eq!(
            drive(put_state(run, install, put("cursor", b"b", Some(1), 2)), &ctx).await
                .unwrap(),
            2
        );
        let entry = drive(read_state(install, "cursor"), &ctx).await.unwrap().unwrap();
        assert_eq!((entry.version, entry.value), (2, b"b".to_vec()));
        assert_eq!(drive(read_state(other, "cursor"), &ctx).await.unwrap(), None);

        // The bytes of all keys of one install are bounded.
        let big = vec![0u8; MAX_STATE_VALUE];
        let count = MAX_STATE_BYTES as usize / MAX_STATE_VALUE;
        for n in 1..count {
            let key = format!("k{n}");
            drive(put_state(run, install, put(&key, &big, None, 10 + n as u32)), &ctx).await
                .unwrap();
        }
        let full = drive(put_state(run, install, put("last", &big, None, 900)), &ctx).await;
        assert!(matches!(full, Err(PutError::Refused(_, ErrorKind::Limit))));
        assert!(
            drive(put_state(run, other, put("last", &big, None, 901)), &ctx).await
                .is_ok()
        );
        assert!(check_key("").is_err() && check_key(&"k".repeat(257)).is_err());
    }

    #[tokio::test]
    async fn versions_never_repeat() {
        // A key removed and written again gets a new version, so the old one cannot change it.
        let (_dir, ctx) = opened();
        let (run, install) = (Ulid::from_parts(9, 1), Ulid::from_parts(1, 1));
        let first = drive(put_state(run, install, put("k", b"a", None, 1)), &ctx).await
            .unwrap();
        let remove = StateRemove {
            key: "k".into(),
            expected: first,
            intent: Intent::new(2, &[b"k"]),
        };
        drive(remove_state(run, install, remove.clone()), &ctx).await.unwrap();
        assert_eq!(drive(read_state(install, "k"), &ctx).await.unwrap(), None);
        let again = drive(put_state(run, install, put("k", b"b", None, 3)), &ctx).await
            .unwrap();
        assert!(again > first, "{again} after {first}");
        let stale = drive(put_state(run, install, put("k", b"c", Some(first), 4)), &ctx).await;
        assert!(matches!(
            stale,
            Err(PutError::Refused(_, ErrorKind::Conflict))
        ));
        let old = StateRemove {
            intent: Intent::new(5, &[b"k"]),
            ..remove
        };
        let refused = drive(remove_state(run, install, old.clone()), &ctx).await;
        assert!(matches!(
            refused,
            Err(PutError::Refused(_, ErrorKind::Conflict))
        ));
    }

    #[tokio::test]
    async fn remove_frees_bytes() {
        // A removal at the stored version frees the value's bytes; another version conflicts.
        let (_dir, ctx) = opened();
        let (run, install) = (Ulid::from_parts(9, 1), Ulid::from_parts(1, 1));
        let big = vec![0u8; MAX_STATE_VALUE];
        let count = MAX_STATE_BYTES as usize / MAX_STATE_VALUE;
        for n in 1..count {
            let key = format!("k{n}");
            drive(put_state(run, install, put(&key, &big, None, n as u32)), &ctx).await
                .unwrap();
        }
        let full = drive(put_state(run, install, put("last", &big, None, 900)), &ctx).await;
        assert!(matches!(full, Err(PutError::Refused(_, ErrorKind::Limit))));
        let remove = |expected, ordinal| StateRemove {
            key: "k1".into(),
            expected,
            intent: Intent::new(ordinal, &[b"k1"]),
        };
        let stale = drive(remove_state(run, install, remove(2, 901)), &ctx).await;
        assert!(matches!(
            stale,
            Err(PutError::Refused(_, ErrorKind::Conflict))
        ));
        drive(remove_state(run, install, remove(1, 902)), &ctx).await
            .unwrap();
        assert_eq!(drive(read_state(install, "k1"), &ctx).await.unwrap(), None);
        assert_eq!(
            drive(begin(run, remove(1, 902).intent), &ctx).await.unwrap(),
            Begun::Settled(String::new())
        );
        let again = drive(remove_state(run, install, remove(1, 903)), &ctx).await;
        assert!(matches!(
            again,
            Err(PutError::Refused(_, ErrorKind::Conflict))
        ));
        assert!(
            drive(put_state(run, install, put("last", &big, None, 904)), &ctx).await
                .is_ok()
        );
    }

    #[test]
    fn put_decisions() {
        let (run, install) = (Ulid::from_parts(9, 1), Ulid::from_parts(1, 1));
        let mut plan = PutState {
            run_id: run,
            install_id: install,
            put: put("k", b"v", Some(3), 0),
            written: None,
        };
        let Turn::Ask(Ask::Read(reads)) = plan.first().unwrap() else {
            panic!("a put reads first");
        };
        assert_eq!(reads.len(), 3);
        let stored = |version| Some(encode(&StateRow { version, value: b"old".to_vec() }).unwrap());
        let answer = |version, total: u64, last: u64| {
            Answer::Values(vec![stored(version), Some(encode(&total).unwrap()), Some(encode(&last).unwrap())])
        };
        // Another version is a conflict and writes nothing.
        let conflict = plan.next(answer(2, 4, 2));
        assert!(matches!(conflict, Err(PutError::Refused(_, ErrorKind::Conflict))));
        // The stored version writes the value, the bytes, the version and the settled intent.
        let Turn::Ask(Ask::Write(rows)) = plan.next(answer(3, 4, 7)).unwrap() else {
            panic!("a matching put writes");
        };
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[2].2.to_vec(), encode(&8u64).unwrap());
        assert_eq!(plan.next(Answer::Done).unwrap(), Turn::Finish(8));
        // A value over its bound is refused before any read.
        let mut big = PutState {
            run_id: run,
            install_id: install,
            put: put("k", &vec![0; MAX_STATE_VALUE + 1], None, 1),
            written: None,
        };
        assert!(matches!(big.first(), Err(PutError::Refused(_, ErrorKind::Limit))));
        assert!(matches!(plan.next(Answer::Page(Vec::new(), None)), Err(PutError::Store(_))));
    }

    #[test]
    fn remove_decisions() {
        let (run, install) = (Ulid::from_parts(9, 1), Ulid::from_parts(1, 1));
        let remove = StateRemove {
            key: "k".into(),
            expected: 3,
            intent: Intent::new(1, &[b"k"]),
        };
        let mut plan = RemoveState {
            run_id: run,
            install_id: install,
            remove,
            phase: RemovePhase::Reading,
        };
        assert!(matches!(plan.first().unwrap(), Turn::Ask(Ask::Read(_))));
        let row = encode(&StateRow { version: 3, value: b"abc".to_vec() }).unwrap();
        let read = Answer::Values(vec![Some(row), Some(encode(&10u64).unwrap())]);
        let Turn::Ask(Ask::Delete(keys)) = plan.next(read).unwrap() else {
            panic!("a matching removal deletes the value");
        };
        assert_eq!(keys, [(PLUGIN_STATE_KEYSPACE, value_key(install, "k"))]);
        let Turn::Ask(Ask::Write(rows)) = plan.next(Answer::Done).unwrap() else {
            panic!("the bytes and intent follow");
        };
        assert_eq!(rows[0].2.to_vec(), encode(&6u64).unwrap());
        assert_eq!(plan.next(Answer::Done).unwrap(), Turn::Finish(()));
        let mut absent = RemoveState {
            phase: RemovePhase::Reading,
            ..plan
        };
        let missing = absent.next(Answer::Values(vec![None, None]));
        assert!(matches!(missing, Err(PutError::Refused(_, ErrorKind::Conflict))));
    }
}
