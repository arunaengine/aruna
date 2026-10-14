//! Moves the bytes a same-name write replaces aside, so their versions stay readable.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::records::{blob_location_read, write_location_effect};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::PLAIN_NAME_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::storage::backend_import::{aside_path, plain_name_key};
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef, BlobLocationKey};
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

#[derive(Debug, Error, PartialEq)]
pub enum AsideError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("moving the replaced bytes aside failed: {0}")]
    Blob(BlobError),
    #[error("unexpected event in state {0:?}")]
    Unexpected(AsideState),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AsideState {
    Init,
    ReadHint,
    ReadRow,
    Copy,
    Start,
    Recheck,
    Write,
    Commit,
    Discard,
    Finish,
}

/// Runs while the caller holds the path's claim. The hint names the location row of the
/// bytes at the path; only a row that still names the path, with bytes that still match
/// it, is repointed to the copy. Anything else leaves nothing a version still reads.
#[derive(Debug, PartialEq)]
pub struct PlainAsideOperation {
    backend: BackendRef,
    path: String,
    state: AsideState,
    key: Option<BlobLocationKey>,
    current: Option<BackendLocation>,
    moved: Option<BackendLocation>,
    txn_id: Option<TxnId>,
    output: Option<Result<(), AsideError>>,
}

impl PlainAsideOperation {
    pub fn new(backend: BackendRef, path: String) -> Self {
        Self {
            backend,
            path,
            state: AsideState::Init,
            key: None,
            current: None,
            moved: None,
            txn_id: None,
            output: None,
        }
    }

    fn finish(&mut self, result: Result<(), AsideError>) -> Effects {
        self.state = AsideState::Finish;
        self.output = Some(result);
        self.abort()
    }

    fn names_path(&self, location: &BackendLocation) -> bool {
        location.backend == self.backend
            && location.storage_bucket.is_empty()
            && location.backend_path == self.path
    }

    fn row_read(&mut self, value: Option<&[u8]>) -> Effects {
        let row = match value.map(BackendLocation::from_bytes).transpose() {
            Ok(row) => row,
            Err(error) => return self.finish(Err(error.into())),
        };
        let Some(row) = row.filter(|row| self.names_path(row)) else {
            return self.finish(Ok(()));
        };
        let path = aside_path(&self.path, Ulid::generate());
        self.current = Some(row.clone());
        self.state = AsideState::Copy;
        smallvec![Effect::Blob(BlobEffect::CopyAside { source: row, path })]
    }

    fn copied(&mut self, copy: BackendLocation) -> Effects {
        let Some(current) = self.current.as_ref() else {
            return self.finish(Err(AsideError::Unexpected(self.state)));
        };
        if copy.get_blake3() != current.get_blake3() || copy.blob_size != current.blob_size {
            warn!(path = %self.path, "The replaced file changed outside Aruna; no copy is kept");
            return self.discard(copy);
        }
        self.moved = Some(BackendLocation {
            backend_path: copy.backend_path,
            ulid: copy.ulid,
            ..current.clone()
        });
        self.state = AsideState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    /// The copy is Aruna's own and no row names it, so it goes again.
    fn discard(&mut self, copy: BackendLocation) -> Effects {
        let mut effects = self.abort();
        self.state = AsideState::Discard;
        effects.push(Effect::Blob(BlobEffect::Delete { location: copy }));
        effects
    }

    fn rechecked(&mut self, value: Option<&[u8]>) -> Effects {
        let (Some(current), Some(moved), Some(key)) = (&self.current, &self.moved, &self.key)
        else {
            return self.finish(Err(AsideError::Unexpected(self.state)));
        };
        let row = match value.map(BackendLocation::from_bytes).transpose() {
            Ok(row) => row,
            Err(error) => return self.finish(Err(error.into())),
        };
        if row.as_ref() != Some(current) {
            let copy = moved.clone();
            return self.discard(copy);
        }
        let hash = key.blake3_hash;
        match write_location_effect(hash, moved.clone(), self.txn_id) {
            Ok(effect) => {
                self.state = AsideState::Write;
                smallvec![effect]
            }
            Err(error) => self.finish(Err(error.into())),
        }
    }
}

impl Operation for PlainAsideOperation {
    type Output = ();
    type Error = AsideError;

    fn start(&mut self) -> Effects {
        self.state = AsideState::ReadHint;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: PLAIN_NAME_KEYSPACE.to_string(),
            key: plain_name_key(&self.backend, &self.path),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            if self.state == AsideState::Discard {
                return self.finish(Ok(()));
            }
            return self.finish(Err(error.into()));
        }
        match (self.state, event) {
            (AsideState::ReadHint, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let Some(value) = value else {
                    return self.finish(Ok(()));
                };
                match BlobLocationKey::from_bytes(value.as_ref()) {
                    Ok(key) => {
                        self.state = AsideState::ReadRow;
                        let effect = blob_location_read(&key, None);
                        self.key = Some(key);
                        smallvec![effect]
                    }
                    Err(error) => self.finish(Err(error.into())),
                }
            }
            (AsideState::ReadRow, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.row_read(value.as_deref())
            }
            (AsideState::Copy, Event::Blob(BlobEvent::WriteFinished { location })) => {
                self.copied(location)
            }
            (AsideState::Copy, Event::Blob(BlobEvent::Error(error))) => {
                self.finish(Err(AsideError::Blob(error)))
            }
            (AsideState::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                let Some(key) = self.key.as_ref() else {
                    return self.finish(Err(AsideError::Unexpected(self.state)));
                };
                self.state = AsideState::Recheck;
                smallvec![blob_location_read(key, Some(txn_id))]
            }
            (AsideState::Recheck, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.rechecked(value.as_deref())
            }
            (AsideState::Write, Event::Storage(StorageEvent::WriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.finish(Err(AsideError::Unexpected(self.state)));
                };
                self.state = AsideState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (AsideState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.finish(Ok(()))
            }
            (AsideState::Discard, Event::Blob(_)) => self.finish(Ok(())),
            (AsideState::Discard, Event::Storage(StorageEvent::TransactionAborted { .. })) => {
                smallvec![]
            }
            (state, _) => self.finish(Err(AsideError::Unexpected(state))),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == AsideState::Finish
    }

    fn finalize(self) -> Result<(), AsideError> {
        self.output
            .unwrap_or(Err(AsideError::Unexpected(AsideState::Finish)))
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map(|txn_id| smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })])
            .unwrap_or_default()
    }
}

/// Emits the sub-operation for the claimed `path` on `backend`.
pub fn set_aside(backend: BackendRef, path: String) -> Effect {
    Effect::SubOperation(boxed_suboperation(
        PlainAsideOperation::new(backend, path),
        |result| {
            Event::SubOperation(SubOperationEvent::PlainSetAside {
                result: result.map_err(|error| error.to_string()),
            })
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::storage::format::{EncodingClass, StoredFormat};
    use std::collections::HashMap;

    fn backend() -> BackendRef {
        BackendRef::Group(Ulid::from_bytes([5u8; 16]))
    }

    fn location(path: &str, hash: u8) -> BackendLocation {
        BackendLocation {
            backend: backend(),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: path.to_string(),
            ulid: Ulid::from_parts(1, u128::from(hash)),
            format: StoredFormat::default(),
            created_by: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 3,
            hashes: HashMap::from([(HASH_BLAKE3.to_string(), vec![hash; 32])]),
        }
    }

    fn read(value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: value.map(Into::into),
        })
    }

    /// Steps through the hint and the row and answers the copy with `copy`.
    fn copied(row: &BackendLocation, copy: BackendLocation) -> (PlainAsideOperation, Effects) {
        let mut operation = PlainAsideOperation::new(backend(), "raw/a.csv".to_string());
        operation.start();
        let key = BlobLocationKey::new([1u8; 32], EncodingClass::Raw, backend());
        operation.step(read(Some(key.to_bytes())));
        let effects = operation.step(read(Some(row.to_bytes().unwrap())));
        let [Effect::Blob(BlobEffect::CopyAside { path, .. })] = effects.as_slice() else {
            panic!("expected an aside copy, got {effects:?}")
        };
        assert!(path.starts_with(".aruna-versions/raw/a.csv/"));
        let effects = operation.step(Event::Blob(BlobEvent::WriteFinished { location: copy }));
        (operation, effects)
    }

    #[test]
    fn nothing_to_move() {
        let mut operation = PlainAsideOperation::new(backend(), "raw/a.csv".to_string());
        operation.start();
        assert!(operation.step(read(None)).is_empty());
        assert_eq!(operation.finalize(), Ok(()));

        // A row that names another path keeps its bytes where they are.
        let mut operation = PlainAsideOperation::new(backend(), "raw/a.csv".to_string());
        operation.start();
        let key = BlobLocationKey::new([1u8; 32], EncodingClass::Raw, backend());
        operation.step(read(Some(key.to_bytes())));
        let other = location("raw/b.csv", 1);
        assert!(
            operation
                .step(read(Some(other.to_bytes().unwrap())))
                .is_empty()
        );
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn repoints_moved_row() {
        let row = location("raw/a.csv", 1);
        let (mut operation, effects) = copied(&row, location(".aruna-versions/raw/a.csv/01", 1));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::from(3),
        }));
        let effects = operation.step(read(Some(row.to_bytes().unwrap())));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected the row write, got {effects:?}")
        };
        let moved = BackendLocation::from_bytes(value).unwrap();
        assert_eq!(moved.backend_path, ".aruna-versions/raw/a.csv/01");
        assert_eq!(moved.hashes, row.hashes);
        assert!(!moved.is_external(), "the aside copy is Aruna's own");

        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: TxnId::from(3),
        }));
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn drops_stale_copy() {
        // The file changed outside Aruna, so its old row is no longer what the path holds.
        let row = location("raw/a.csv", 1);
        let (mut operation, effects) = copied(&row, location(".aruna-versions/raw/a.csv/01", 2));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::Delete { .. })]
        ));
        operation.step(Event::Blob(BlobEvent::DeleteFinished));
        assert_eq!(operation.finalize(), Ok(()));

        // A row repointed while the copy ran keeps its new path; the copy goes.
        let (mut operation, _) = copied(&row, location(".aruna-versions/raw/a.csv/01", 1));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::from(3),
        }));
        let changed = location("raw/elsewhere.csv", 1);
        let effects = operation.step(read(Some(changed.to_bytes().unwrap())));
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::Storage(StorageEffect::AbortTransaction { .. }),
                Effect::Blob(BlobEffect::Delete { .. })
            ]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: TxnId::from(3),
        }));
        operation.step(Event::Blob(BlobEvent::DeleteFinished));
        assert_eq!(operation.finalize(), Ok(()));
    }
}
