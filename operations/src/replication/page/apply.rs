//! Applies one reference page on a member in a single transaction: admission, version rows,
//! heads, usage and inventory of every record commit together or not at all.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BlobVersion;
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;

pub use super::plan::PageError;
use super::plan::{
    PageGuards, PageItem, Plan, check_guards, guard_reads, page_items, page_rows, plan_items,
    previous_reads,
};
use super::{PageAnswer, ReferencePage};
use crate::node::usage_stats::{RowChange, UsageCounterUpdate, schedule_snapshot_publish};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ApplyState {
    Init,
    StartTransaction,
    ReadGuards,
    /// Reads the versions the replaced heads name.
    ReadPrevious,
    WriteRows,
    UpdateUsage,
    Commit,
    Schedule,
    /// Ends the transaction without a write; the output says whether the page was refused.
    Abort,
    Finish,
    Error,
}

/// A coordinator's page to this member, sent by the authenticated `publisher`.
#[derive(Clone, Debug, PartialEq)]
pub struct ApplyInput {
    pub page: ReferencePage,
    pub publisher: NodeId,
    pub realm_id: RealmId,
    /// This node, which holds the target bucket.
    pub local: NodeId,
}

#[derive(Debug, PartialEq)]
pub struct PageApplyOperation {
    input: ApplyInput,
    state: ApplyState,
    txn_id: Option<TxnId>,
    items: Vec<PageItem>,
    guards: Option<PageGuards>,
    plans: Vec<Plan>,
    usage: Option<UsageCounterUpdate>,
    answer: Option<PageAnswer>,
    output: Option<Result<PageAnswer, PageError>>,
}

impl PageApplyOperation {
    pub fn new(input: ApplyInput) -> Self {
        Self {
            input,
            state: ApplyState::Init,
            txn_id: None,
            items: Vec::new(),
            guards: None,
            plans: Vec::new(),
            usage: None,
            answer: None,
            output: None,
        }
    }

    fn txn(&self) -> Result<TxnId, PageError> {
        self.txn_id.ok_or(StorageError::TransactionNotFound.into())
    }

    /// Ends the transaction, if one is open, with `output`.
    fn end(&mut self, output: Result<PageAnswer, PageError>) -> Effects {
        self.output = Some(output);
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = ApplyState::Abort;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => self.settle(),
        }
    }

    fn settle(&mut self) -> Effects {
        self.state = match self.output {
            Some(Ok(_)) => ApplyState::Finish,
            _ => ApplyState::Error,
        };
        smallvec![]
    }

    fn read_previous(&mut self, txn_id: TxnId) -> Result<Effects, PageError> {
        let reads = previous_reads(&self.input.page, &self.plans)?;
        if reads.is_empty() {
            return self.write_rows(txn_id, Vec::new());
        }
        self.state = ApplyState::ReadPrevious;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })])
    }

    fn write_rows(
        &mut self,
        txn_id: TxnId,
        previous: Vec<Option<BlobVersion>>,
    ) -> Result<Effects, PageError> {
        let guards = self.guards.as_ref().ok_or(PageError::NotFinished)?;
        let page = &self.input.page;
        let here = (self.input.realm_id, self.input.local);
        let rows = page_rows(page, &self.items, &self.plans, (guards, previous), here)?;
        if rows.written == 0 {
            return Ok(self.end(Ok(rows.answer)));
        }
        let usage = UsageCounterUpdate::for_group(guards.group_id, rows.delta)
            .in_bucket(&page.bucket, RowChange::Kept)
            .with_versions(rows.written)
            .with_inventories(rows.inventory);
        self.usage = Some(usage);
        self.answer = Some(rows.answer);
        self.state = ApplyState::WriteRows;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: rows.writes,
            txn_id: Some(txn_id),
        })])
    }

    fn commit(&mut self, txn_id: TxnId) -> Effects {
        self.state = ApplyState::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn step_inner(&mut self, event: Event) -> Result<Effects, PageError> {
        match (self.state, event) {
            (
                ApplyState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = ApplyState::ReadGuards;
                Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: guard_reads(&self.input.page)?,
                    txn_id: Some(txn_id),
                })])
            }
            (ApplyState::ReadGuards, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let page = &self.input.page;
                let guards = check_guards(page, self.input.publisher, values)?;
                self.plans = plan_items(page, &self.items, &guards);
                self.guards = Some(guards);
                self.read_previous(self.txn()?)
            }
            (
                ApplyState::ReadPrevious,
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => {
                let previous = (values.into_iter())
                    .map(|(_, value)| value.map(|v| BlobVersion::from_bytes(&v)).transpose())
                    .collect::<Result<Vec<_>, _>>()?;
                self.write_rows(self.txn()?, previous)
            }
            (ApplyState::WriteRows, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                let txn_id = self.txn()?;
                self.state = ApplyState::UpdateUsage;
                let usage = self.usage.as_mut().ok_or(PageError::NotFinished)?;
                Ok(usage.start(txn_id))
            }
            (ApplyState::UpdateUsage, event @ Event::Storage(_)) => {
                let txn_id = self.txn()?;
                let usage = self.usage.as_mut().ok_or(PageError::NotFinished)?;
                match usage.step(event, txn_id)? {
                    Some(effects) => Ok(effects),
                    None => Ok(self.commit(txn_id)),
                }
            }
            (ApplyState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = ApplyState::Schedule;
                Ok(smallvec![schedule_snapshot_publish()])
            }
            (ApplyState::Commit, Event::Storage(StorageEvent::Error { error })) => {
                // A failed commit leaves no transaction to abort; its outcome is retried.
                self.txn_id = None;
                Err(error.into())
            }
            (ApplyState::Schedule, Event::Task(_)) => {
                let answer = self.answer.take().ok_or(PageError::NotFinished)?;
                self.output = Some(Ok(answer));
                Ok(self.settle())
            }
            (ApplyState::Abort, Event::Storage(_)) => Ok(self.settle()),
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (state, event) => Err(PageError::UnexpectedEvent {
                state: state_name(state),
                received: format!("{event:?}"),
            }),
        }
    }
}

fn state_name(state: ApplyState) -> &'static str {
    match state {
        ApplyState::Init => "Init",
        ApplyState::StartTransaction => "StartTransaction",
        ApplyState::ReadGuards => "ReadGuards",
        ApplyState::ReadPrevious => "ReadPrevious",
        ApplyState::WriteRows => "WriteRows",
        ApplyState::UpdateUsage => "UpdateUsage",
        ApplyState::Commit => "Commit",
        ApplyState::Schedule => "Schedule",
        ApplyState::Abort => "Abort",
        ApplyState::Finish => "Finish",
        ApplyState::Error => "Error",
    }
}

impl Operation for PageApplyOperation {
    type Output = PageAnswer;
    type Error = PageError;

    fn start(&mut self) -> Effects {
        let input = &self.input;
        match page_items(&input.page, input.publisher, input.realm_id) {
            Ok(items) => self.items = items,
            Err(error) => return self.end(Err(error)),
        }
        self.state = ApplyState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if matches!(self.state, ApplyState::Finish | ApplyState::Error) {
            return smallvec![];
        }
        match self.step_inner(event) {
            Ok(effects) => effects,
            Err(error) => self.end(Err(error)),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ApplyState::Finish | ApplyState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(PageError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = ApplyState::Abort;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => smallvec![],
        }
    }
}

/// The answer a member sends for the outcome of applying a page.
pub fn page_answer(result: Result<PageAnswer, PageError>) -> PageAnswer {
    result.unwrap_or_else(|error| PageAnswer::Refused {
        retry: error.retryable(),
        reason: error.to_string(),
    })
}

#[cfg(test)]
#[path = "apply_tests.rs"]
pub(super) mod tests;
