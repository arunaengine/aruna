//! Removes a backend import rule and its object rows; versions and bytes stay.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::ImportRuleError;
use crate::groups::fence::{parse_group_record, read_group_record};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, IMPORT_REMOVAL_KEYSPACE, IMPORTED_OBJECT_KEYSPACE,
};
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
    Mark,
    Commit,
    Scan,
    Purge,
    Unmark,
    Finish,
}

/// The rule goes in one transaction with a removal mark; its object rows follow in pages and
/// the mark goes last, so an interrupted removal resumes from the mark. Imported versions
/// remain readable, and the routing rule for the folder stays in place.
#[derive(Debug, PartialEq)]
pub struct RemoveImportOperation {
    /// `None` when resuming the rows of a rule that is already gone.
    group_id: Option<GroupId>,
    import_id: Ulid,
    state: RemoveState,
    txn_id: Option<TxnId>,
    output: Option<Result<(), ImportRuleError>>,
}

impl RemoveImportOperation {
    pub fn new(group_id: GroupId, import_id: Ulid) -> Self {
        Self {
            group_id: Some(group_id),
            import_id,
            state: RemoveState::Init,
            txn_id: None,
            output: None,
        }
    }

    /// Deletes the remaining object rows of a removal that was interrupted.
    pub fn resume(import_id: Ulid) -> Self {
        Self {
            group_id: None,
            import_id,
            state: RemoveState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn mark_key(&self) -> Key {
        self.import_id.to_bytes().to_vec().into()
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
            self.state = RemoveState::Unmark;
            return smallvec![Effect::Storage(StorageEffect::Delete {
                key_space: IMPORT_REMOVAL_KEYSPACE.to_string(),
                key: self.mark_key(),
                txn_id: None,
            })];
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
        if self.group_id.is_none() {
            return self.scan();
        }
        self.state = RemoveState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, self.group_id, event) {
            (_, _, Event::Storage(StorageEvent::Error { error })) => self.fail(error.into()),
            (
                RemoveState::Start,
                Some(group_id),
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = RemoveState::Read;
                let key = import_key(group_id, self.import_id);
                smallvec![read_group_record(
                    group_id,
                    BACKEND_IMPORT_KEYSPACE,
                    key,
                    txn_id
                )]
            }
            (RemoveState::Read, Some(group_id), event) => match parse_group_record(event) {
                Ok(Some(_)) => {
                    self.state = RemoveState::Delete;
                    smallvec![Effect::Storage(StorageEffect::Delete {
                        key_space: BACKEND_IMPORT_KEYSPACE.to_string(),
                        key: import_key(group_id, self.import_id),
                        txn_id: self.txn_id,
                    })]
                }
                Ok(None) => self.fail(ImportRuleError::NotFound),
                Err(error) => self.fail(error.into()),
            },
            (RemoveState::Delete, _, Event::Storage(StorageEvent::DeleteResult { .. })) => {
                self.state = RemoveState::Mark;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: IMPORT_REMOVAL_KEYSPACE.to_string(),
                    key: self.mark_key(),
                    value: Vec::new().into(),
                    txn_id: self.txn_id,
                })]
            }
            (RemoveState::Mark, _, Event::Storage(StorageEvent::WriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(ImportRuleError::Unexpected("Mark"));
                };
                self.state = RemoveState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (RemoveState::Commit, _, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.scan()
            }
            (RemoveState::Scan, _, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                self.purge(values)
            }
            (RemoveState::Purge, _, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                self.scan()
            }
            (RemoveState::Unmark, _, Event::Storage(StorageEvent::DeleteResult { .. })) => {
                self.state = RemoveState::Finish;
                self.output = Some(Ok(()));
                smallvec![]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn storage(event: StorageEvent) -> Event {
        Event::Storage(event)
    }

    /// Answers a page scan with `rows` object rows and checks that their delete follows.
    fn drain(operation: &mut RemoveImportOperation, rows: usize) -> Effects {
        let values = (0..rows)
            .map(|row| (Key::from(vec![row as u8]), Value::from(Vec::new())))
            .collect();
        let effects = operation.step(storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        }));
        if rows > 0 {
            assert!(matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::BatchDelete { .. })]
            ));
        }
        effects
    }

    fn unmarked(operation: &mut RemoveImportOperation, effects: Effects) {
        let [
            Effect::Storage(StorageEffect::Delete {
                key_space, txn_id, ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected the mark delete, got {effects:?}")
        };
        assert_eq!(
            (key_space.as_str(), *txn_id),
            (IMPORT_REMOVAL_KEYSPACE, None)
        );
        operation.step(storage(StorageEvent::DeleteResult {
            key: Vec::new().into(),
        }));
        assert!(operation.is_complete());
    }

    #[test]
    fn marks_removed_rule() {
        let (group_id, import_id) = (Ulid::from_parts(1, 1), Ulid::from_parts(2, 2));
        let mut operation = RemoveImportOperation::new(group_id, import_id);
        operation.start();
        let txn_id = TxnId::from(7);
        operation.step(storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = operation.step(storage(StorageEvent::BatchReadResult {
            values: vec![
                (Key::from(Vec::new()), Some(vec![1u8].into())),
                (Key::from(Vec::new()), None),
            ],
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Delete {
                txn_id: Some(_),
                ..
            })]
        ));

        // The mark commits with the rule deletion, before any row goes.
        let effects = operation.step(storage(StorageEvent::DeleteResult {
            key: Vec::new().into(),
        }));
        let [
            Effect::Storage(StorageEffect::Write {
                key_space,
                key,
                txn_id: mark_txn,
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected the removal mark, got {effects:?}")
        };
        assert_eq!(key_space, IMPORT_REMOVAL_KEYSPACE);
        assert_eq!(key.as_ref(), import_id.to_bytes().as_slice());
        assert_eq!(*mark_txn, Some(txn_id));
        let effects = operation.step(storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { .. })]
        ));
        operation.step(storage(StorageEvent::TransactionCommitted { txn_id }));

        drain(&mut operation, 2);
        operation.step(storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        let effects = drain(&mut operation, 0);
        unmarked(&mut operation, effects);
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn resumes_row_removal() {
        let mut operation = RemoveImportOperation::resume(Ulid::from_parts(2, 2));
        let effects = operation.start();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { .. })]
        ));
        let effects = drain(&mut operation, 0);
        unmarked(&mut operation, effects);
        assert_eq!(operation.finalize(), Ok(()));

        // A resumed removal never touches a rule.
        let mut operation = RemoveImportOperation::resume(Ulid::from_parts(2, 2));
        operation.start();
        operation.step(storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::from(7),
        }));
        assert_eq!(
            operation.finalize(),
            Err(ImportRuleError::Unexpected("RemoveImport"))
        );
    }
}
