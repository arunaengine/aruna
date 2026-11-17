//! Reports what blocks a group deletion on each realm node, without changing any state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::EffectOperation;
use aruna_core::structs::identity::group_delete::GroupDeletionError;
use aruna_core::types::{Effects, GroupId, TxnId};
use smallvec::smallvec;

use super::resources::EmptyCheck;

#[derive(Debug, PartialEq)]
enum State {
    Init,
    Start,
    Scan,
    Close,
    Finish,
}

/// Runs the deletion emptiness scan in a read transaction.
#[derive(Debug, PartialEq)]
pub(super) struct CheckOperation {
    scan: EmptyCheck,
    txn_id: Option<TxnId>,
    state: State,
    result: Option<Result<(), GroupDeletionError>>,
}

impl CheckOperation {
    pub fn new(group_id: GroupId, now: SystemTime) -> Self {
        Self {
            scan: EmptyCheck::new(group_id, now),
            txn_id: None,
            state: State::Init,
            result: None,
        }
    }

    fn fail(&mut self, error: GroupDeletionError) -> Effects {
        self.result = Some(Err(error));
        self.state = State::Finish;
        self.abort()
    }

    fn close(&mut self, txn_id: TxnId, result: Result<(), GroupDeletionError>) -> Effects {
        self.result = Some(result);
        self.state = State::Close;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }
}

impl EffectOperation for CheckOperation {
    type Output = ();
    type Error = GroupDeletionError;

    fn start(&mut self) -> Effects {
        self.state = State::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, self.txn_id, event) {
            (State::Start, None, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = State::Scan;
                self.scan.start(txn_id)
            }
            (State::Scan, Some(txn_id), event) => match self.scan.step(event, txn_id) {
                Ok(Some(effects)) => effects,
                Ok(None) => self.close(txn_id, Ok(())),
                Err(error @ GroupDeletionError::NotEmpty { .. }) => self.close(txn_id, Err(error)),
                Err(error) => self.fail(error),
            },
            (
                State::Close,
                Some(txn_id),
                Event::Storage(StorageEvent::TransactionCommitted { txn_id: done }),
            ) if done == txn_id => {
                self.txn_id = None;
                self.state = State::Finish;
                smallvec![]
            }
            (_, _, Event::Storage(StorageEvent::Error { error })) => self.fail(error.into()),
            _ => self.fail(GroupDeletionError::Unavailable(
                "unexpected deletion check event".into(),
            )),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == State::Finish
    }

    fn finalize(self) -> Result<(), GroupDeletionError> {
        match (self.state, self.result) {
            (State::Finish, Some(result)) => result,
            _ => Err(GroupDeletionError::Unavailable(
                "deletion check did not finish".into(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map(|txn_id| smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })])
            .unwrap_or_default()
    }
}
