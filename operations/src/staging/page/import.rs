//! Registers one listing page of source files as reference versions in a single transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::connectors::repository::{parse_secret_read, read_secret_effect};
use crate::connectors::resolver::secret_fingerprint;
use crate::distribution::guard::{authority_reads, write_epochs};
use crate::metadata::call_results::page_rows as range_rows;
use crate::replication::queue::schedule_blob_drain;
use crate::s3::usage::{RowChange, UsageCounterUpdate, schedule_snapshot_publish};
use crate::staging::page::rows::{
    MAX_PAGE_ENTRIES, PageGuards, PreparedRows, check_guards, decode_heads, guard_reads,
    head_range, head_reads, interval_heads, page_rows, version_reads,
};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::keyspaces::RELATIONSHIP_OUT_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};
use aruna_core::structs::storage::blob::BlobVersion;
use aruna_core::structs::storage::blob::CurrentVersionPointer;
use aruna_core::structs::sync_relationship_prefix;
use std::collections::BTreeSet;
use ulid::Ulid;

pub use crate::staging::page::rows::{
    PageEntry, PageImportError, PageImportInput, PageImportResult,
};

/// What the pending call of a page answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PageStep {
    Guards,
    /// The heads in the key interval of the page, which proves absent keys absent.
    Heads,
    /// The heads key by key, when the interval holds too many other keys.
    HeadKeys,
    /// A distributed bucket's record: every entry's key must be coordinated here.
    Record,
    /// Whether an ordinary bucket has a sync relationship that replicates its versions.
    Relationships,
    Secret,
    Versions,
    WriteRows,
    DeleteRows,
    Usage,
}

type PageNext = Result<Next<PageImportResult>, PageImportError>;

#[derive(Debug, PartialEq)]
pub struct PageImportOperation {
    input: PageImportInput,
    step: PageStep,
    guards: Option<PageGuards>,
    usage: Option<UsageCounterUpdate>,
    result: Option<PageImportResult>,
    /// Only builds the rows, in a read snapshot, and keeps them in `prepared`.
    prepare: bool,
    prepared: Option<PreparedRows>,
}

impl PageImportOperation {
    pub fn new(input: PageImportInput) -> Self {
        Self {
            input,
            step: PageStep::Guards,
            guards: None,
            usage: None,
            result: None,
            prepare: false,
            prepared: None,
        }
    }

    /// Rows a [`PreparePageOperation`] built from the same input.
    pub fn with_prepared(mut self, prepared: Option<PreparedRows>) -> Self {
        self.prepared = prepared;
        self
    }

    fn call(&mut self, step: PageStep, call: IoCall) -> PageNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn guards_mut(&mut self) -> Result<&mut PageGuards, PageImportError> {
        self.guards.as_mut().ok_or(PageImportError::NotFinished)
    }

    fn read_secret(&mut self) -> PageNext {
        let connector_id = self.input.connector.connector_id;
        let read = IoCall::from(read_secret_effect(connector_id, None));
        self.call(PageStep::Secret, read)
    }

    /// One relationship row is enough; read in the transaction, it conflicts with a
    /// relationship created before the page commits.
    fn read_relationships(&mut self) -> PageNext {
        let read = IoCall::Page {
            space: RELATIONSHIP_OUT_KEYSPACE.to_string(),
            prefix: sync_relationship_prefix(&self.input.bucket).into(),
            after: None,
            limit: 1,
        };
        self.call(PageStep::Relationships, read)
    }

    /// Takes the page's heads and reads what replicates the bucket's versions.
    fn take_heads(&mut self, heads: Vec<Option<CurrentVersionPointer>>) -> PageNext {
        let guards = self.guards_mut()?;
        guards.heads = heads;
        // Read in the page transaction, so a fence or record set meanwhile conflicts.
        let record = (guards.binding.as_ref()).map(|binding| authority_reads(binding.set_id));
        match record {
            Some(reads) => self.call(PageStep::Record, IoCall::Read(reads)),
            None => self.read_relationships(),
        }
    }

    /// Every key of the page must lie in a range this member coordinates.
    fn check_record(&mut self, result: IoResult) -> PageNext {
        let [record, fence] = values::<2>(result)?;
        let guards = self.guards.as_mut().ok_or(PageImportError::NotFinished)?;
        let binding = (guards.binding.as_ref()).ok_or(PageImportError::NotFinished)?;
        let keys = (self.input.entries.iter()).map(|entry| entry.target_key.as_str());
        let (routes, search) = write_epochs(binding, keys, record.as_deref(), fence.as_deref())?;
        let resolved: BTreeSet<Ulid> = routes.iter().map(|(range, _)| *range).collect();
        if (self.input.ranges.as_ref()).is_some_and(|held| *held != resolved) {
            return Err(PageImportError::RangesMoved(resolved));
        }
        guards.epochs = routes.into_iter().map(|(_, epoch)| epoch).collect();
        guards.search = search;
        self.read_secret()
    }

    fn read_versions(&mut self) -> PageNext {
        let heads = self.guards.as_ref().map(|guards| guards.heads.as_slice());
        let reads = version_reads(&self.input, heads.unwrap_or_default())?;
        if reads.is_empty() {
            return self.write_rows(Vec::new());
        }
        self.call(PageStep::Versions, IoCall::Read(reads))
    }

    fn write_rows(&mut self, versions: Vec<Option<BlobVersion>>) -> PageNext {
        let guards = self.guards.as_ref().ok_or(PageImportError::NotFinished)?;
        let rows = match self.prepared.take() {
            Some(prepared) if prepared.guards == *guards && prepared.versions == versions => {
                prepared.rows
            }
            _ if self.prepare => {
                let rows = page_rows(&self.input, guards, versions.clone())?;
                let result = rows.result.clone();
                let guards = guards.clone();
                self.prepared = Some(PreparedRows {
                    guards,
                    versions,
                    rows,
                });
                // The read snapshot ends without writes.
                return Ok(Next::Done(result));
            }
            _ => page_rows(&self.input, guards, versions)?,
        };
        if rows.result.registered > 0 {
            let usage = UsageCounterUpdate::for_group(self.input.group_id, rows.delta)
                .in_bucket(&self.input.bucket, RowChange::Kept)
                .with_versions(rows.result.registered);
            self.usage = Some(usage.with_inventories(rows.inventory));
        }
        self.result = Some(rows.result);
        self.call(PageStep::WriteRows, IoCall::Write(rows.writes))
    }

    fn update_usage(&mut self) -> PageNext {
        match self.usage.as_mut().filter(|usage| !usage.is_noop()) {
            Some(usage) => {
                let call = usage.start_calls().into_call();
                self.call(PageStep::Usage, call)
            }
            None => self.finish(),
        }
    }

    fn step_usage(&mut self, result: IoResult) -> PageNext {
        let usage = self.usage.as_mut().ok_or(PageImportError::NotFinished)?;
        match usage.step_calls(result)? {
            Some(call) => Ok(Next::Call(call.into_call())),
            None => self.finish(),
        }
    }

    /// Ends the page; the dirty markers and obligations it commits are the durable work, so
    /// the timers that pick them up are only woken after the commit.
    fn finish(&mut self) -> PageNext {
        let output = self.result.take().ok_or(PageImportError::NotFinished)?;
        if self.usage.is_none() {
            return Ok(Next::Done(output));
        }
        let wake = |effect: Effect| IoCall::Effect(Box::new(effect));
        Ok(Next::Then {
            output,
            calls: vec![
                wake(schedule_snapshot_publish()),
                wake(schedule_blob_drain()),
            ],
        })
    }
}

impl Operation for PageImportOperation {
    type Output = PageImportResult;
    type Error = PageImportError;

    fn first(&mut self) -> PageNext {
        let entries = self.input.entries.len();
        if entries > MAX_PAGE_ENTRIES {
            return Err(PageImportError::TooManyEntries(entries));
        }
        Ok(Next::Call(IoCall::Read(guard_reads(&self.input)?)))
    }

    fn next(&mut self, result: IoResult) -> PageNext {
        match self.step {
            PageStep::Guards => {
                self.guards = Some(check_guards(&self.input, result.values()?)?);
                match head_range(&self.input)? {
                    Some(read) => {
                        let read = IoCall::Effect(Box::new(Effect::Storage(read)));
                        self.call(PageStep::Heads, read)
                    }
                    None => self.take_heads(Vec::new()),
                }
            }
            PageStep::Heads => match range_rows(result)? {
                (_, Some(_)) => {
                    let reads = head_reads(&self.input)?;
                    self.call(PageStep::HeadKeys, IoCall::Read(reads))
                }
                (rows, None) => self.take_heads(interval_heads(&self.input, rows)?),
            },
            PageStep::HeadKeys => self.take_heads(decode_heads(result.values()?)?),
            PageStep::Record => self.check_record(result),
            PageStep::Relationships => {
                let (rows, _) = result.page()?;
                self.guards_mut()?.downstream = !rows.is_empty();
                self.read_secret()
            }
            PageStep::Secret => {
                let connector_id = self.input.connector.connector_id;
                let secret = parse_secret_read(result.event()?, connector_id)?;
                if secret.as_ref().map(secret_fingerprint) != self.input.secret_fingerprint {
                    return Err(PageImportError::ConnectorChanged);
                }
                self.read_versions()
            }
            PageStep::Versions => {
                let versions = (result.values()?.into_iter())
                    .map(|value| {
                        value
                            .map(|value| BlobVersion::from_bytes(value.as_ref()))
                            .transpose()
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                self.write_rows(versions)
            }
            PageStep::WriteRows => {
                done(result)?;
                let deletes = self.input.deletes.clone();
                self.call(PageStep::DeleteRows, IoCall::Delete(deletes))
            }
            PageStep::DeleteRows => {
                done(result)?;
                self.update_usage()
            }
            PageStep::Usage => self.step_usage(result),
        }
    }

    fn transaction(&self) -> TransactionMode {
        match self.prepare {
            true => TransactionMode::ReadSnapshot,
            false => TransactionMode::Write,
        }
    }
}

/// Builds a page's rows in a read snapshot ahead of the page's turn, so the turn only reads,
/// compares and writes them.
#[derive(Debug, PartialEq)]
pub struct PreparePageOperation(PageImportOperation);

impl PreparePageOperation {
    pub fn new(input: PageImportInput) -> Self {
        let mut page = PageImportOperation::new(input);
        page.prepare = true;
        Self(page)
    }

    fn prepared(
        &mut self,
        next: Next<PageImportResult>,
    ) -> Result<Next<PreparedRows>, PageImportError> {
        match next {
            Next::Call(call) => Ok(Next::Call(call)),
            Next::Wait => Ok(Next::Wait),
            Next::Done(_) | Next::Then { .. } => {
                let prepared = self.0.prepared.take();
                prepared.map(Next::Done).ok_or(PageImportError::NotFinished)
            }
        }
    }
}

impl Operation for PreparePageOperation {
    type Output = PreparedRows;
    type Error = PageImportError;

    fn first(&mut self) -> Result<Next<PreparedRows>, PageImportError> {
        let next = self.0.first()?;
        self.prepared(next)
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PreparedRows>, PageImportError> {
        let next = self.0.next(result)?;
        self.prepared(next)
    }

    fn transaction(&self) -> TransactionMode {
        self.0.transaction()
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
