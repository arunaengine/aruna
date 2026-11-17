//! Applies one reference page on a member in a single transaction: admission, version rows,
//! heads, usage and inventory of every record commit together or not at all.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BlobVersion;
use aruna_core::types::Key;

use super::plan::PageError;
use super::plan::{
    PageGuards, PageItem, Plan, check_guards, guard_reads, page_items, page_rows, plan_items,
    previous_reads,
};
use super::{PageAnswer, ReferencePage};
use crate::s3::usage::{RowChange, UsageCounterUpdate, schedule_snapshot_publish};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ApplyState {
    ReadGuards,
    /// Reads the versions the replaced heads name.
    ReadPrevious,
    WriteRows,
    UpdateUsage,
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

type Step = Result<Next<PageAnswer>, PageError>;

/// Applies one reference page on a member in one transaction; the usage snapshot publish is
/// scheduled after the commit.
#[derive(Debug, PartialEq)]
pub struct PageApplyOperation {
    input: ApplyInput,
    state: ApplyState,
    items: Vec<PageItem>,
    guards: Option<PageGuards>,
    plans: Vec<Plan>,
    usage: Option<UsageCounterUpdate>,
    answer: Option<PageAnswer>,
}

impl PageApplyOperation {
    pub fn new(input: ApplyInput) -> Self {
        Self {
            input,
            state: ApplyState::ReadGuards,
            items: Vec::new(),
            guards: None,
            plans: Vec::new(),
            usage: None,
            answer: None,
        }
    }

    fn read_previous(&mut self) -> Step {
        let reads = previous_reads(&self.input.page, &self.plans)?;
        if reads.is_empty() {
            return self.write_rows(Vec::new());
        }
        self.state = ApplyState::ReadPrevious;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn write_rows(&mut self, previous: Vec<Option<BlobVersion>>) -> Step {
        let guards = self.guards.as_ref().ok_or(PageError::NotFinished)?;
        let page = &self.input.page;
        let here = (self.input.realm_id, self.input.local);
        let rows = page_rows(page, &self.items, &self.plans, (guards, previous), here)?;
        // Nothing to store: the page ends with its answer and the transaction writes nothing.
        if rows.written == 0 {
            return Ok(Next::Done(rows.answer));
        }
        let usage = UsageCounterUpdate::for_group(guards.group_id, rows.delta)
            .in_bucket(&page.bucket, RowChange::Kept)
            .with_versions(rows.written)
            .with_inventories(rows.inventory);
        self.usage = Some(usage);
        self.answer = Some(rows.answer);
        self.state = ApplyState::WriteRows;
        Ok(Next::Call(IoCall::Write(rows.writes)))
    }

    fn usage_step(&mut self, result: IoResult) -> Step {
        let usage = self.usage.as_mut().ok_or(PageError::NotFinished)?;
        if let Some(call) = usage.step_calls(result)? {
            return Ok(Next::Call(call.into_call()));
        }
        let answer = self.answer.take().ok_or(PageError::NotFinished)?;
        let publish = IoCall::Effect(Box::new(schedule_snapshot_publish()));
        Ok(Next::Then {
            output: answer,
            calls: vec![publish],
        })
    }
}

impl Operation for PageApplyOperation {
    type Output = PageAnswer;
    type Error = PageError;

    fn first(&mut self) -> Step {
        let input = &self.input;
        self.items = page_items(&input.page, input.publisher, input.realm_id)?;
        Ok(Next::Call(IoCall::Read(guard_reads(&self.input.page)?)))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.state {
            ApplyState::ReadGuards => {
                let values = (result.values()?.into_iter())
                    .map(|value| (Key::from(Vec::new()), value))
                    .collect();
                let page = &self.input.page;
                let guards = check_guards(page, self.input.publisher, values)?;
                self.plans = plan_items(page, &self.items, &guards);
                self.guards = Some(guards);
                self.read_previous()
            }
            ApplyState::ReadPrevious => {
                let previous = (result.values()?.into_iter())
                    .map(|value| value.map(|v| BlobVersion::from_bytes(&v)).transpose())
                    .collect::<Result<Vec<_>, _>>()?;
                self.write_rows(previous)
            }
            ApplyState::WriteRows => {
                result.done()?;
                self.state = ApplyState::UpdateUsage;
                let usage = self.usage.as_mut().ok_or(PageError::NotFinished)?;
                Ok(Next::Call(usage.start_calls().into_call()))
            }
            ApplyState::UpdateUsage => self.usage_step(result),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
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
