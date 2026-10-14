//! Removes a backend import rule and its object rows; versions and bytes stay.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::ImportRuleError;
use crate::groups::fence::{parse_group_record, read_group_record};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BACKEND_IMPORT_KEYSPACE, IMPORTED_OBJECT_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::backend_import::{import_key, object_prefix};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use smallvec::smallvec;
use ulid::Ulid;

const DELETE_PAGE: usize = 512;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RemoveState {
    Init,
    Start,
    Read,
    Delete,
    Commit,
    Scan,
    Purge,
    Finish,
}

/// The rule goes in one transaction; its object rows follow in bounded pages. Imported
/// versions remain readable, and the routing rule for the folder stays in place.
#[derive(Debug, PartialEq)]
pub struct RemoveImportOperation {
    group_id: GroupId,
    import_id: Ulid,
    state: RemoveState,
    txn_id: Option<TxnId>,
    output: Option<Result<(), ImportRuleError>>,
}

impl RemoveImportOperation {
    pub fn new(group_id: GroupId, import_id: Ulid) -> Self {
        Self {
            group_id,
            import_id,
            state: RemoveState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, error: ImportRuleError) -> Effects {
        self.state = RemoveState::Finish;
        self.output = Some(Err(error));
        self.abort()
    }

    fn scan(&mut self) -> Effects {
        self.state = RemoveState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: IMPORTED_OBJECT_KEYSPACE.to_string(),
            prefix: Some(object_prefix(self.import_id)),
            start: None,
            limit: DELETE_PAGE,
            txn_id: None,
        })]
    }

    fn purge(&mut self, values: Vec<(Key, Value)>) -> Effects {
        if values.is_empty() {
            self.state = RemoveState::Finish;
            self.output = Some(Ok(()));
            return smallvec![];
        }
        self.state = RemoveState::Purge;
        let deletes = values
            .into_iter()
            .map(|(key, _)| (IMPORTED_OBJECT_KEYSPACE.to_string(), key))
            .collect();
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })]
    }
}

impl Operation for RemoveImportOperation {
    type Output = ();
    type Error = ImportRuleError;

    fn start(&mut self) -> Effects {
        self.state = RemoveState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error.into()),
            (RemoveState::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = RemoveState::Read;
                let key = import_key(self.group_id, self.import_id);
                smallvec![read_group_record(
                    self.group_id,
                    BACKEND_IMPORT_KEYSPACE,
                    key,
                    txn_id
                )]
            }
            (RemoveState::Read, event) => match parse_group_record(event) {
                Ok(Some(_)) => {
                    self.state = RemoveState::Delete;
                    smallvec![Effect::Storage(StorageEffect::Delete {
                        key_space: BACKEND_IMPORT_KEYSPACE.to_string(),
                        key: import_key(self.group_id, self.import_id),
                        txn_id: self.txn_id,
                    })]
                }
                Ok(None) => self.fail(ImportRuleError::NotFound),
                Err(error) => self.fail(error.into()),
            },
            (RemoveState::Delete, Event::Storage(StorageEvent::DeleteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(ImportRuleError::Unexpected("Delete"));
                };
                self.state = RemoveState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (RemoveState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.scan()
            }
            (RemoveState::Scan, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                self.purge(values)
            }
            (RemoveState::Purge, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                self.scan()
            }
            _ => self.fail(ImportRuleError::Unexpected("RemoveImport")),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == RemoveState::Finish
    }

    fn finalize(self) -> Result<(), ImportRuleError> {
        self.output
            .unwrap_or(Err(ImportRuleError::Unexpected("Finish")))
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map(|txn_id| smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })])
            .unwrap_or_default()
    }
}
