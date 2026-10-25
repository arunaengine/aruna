//! Registers one listing page of source files as reference versions in a single transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::connectors::repository::{parse_secret_read, read_secret_effect};
use crate::connectors::resolver::secret_fingerprint;
use crate::distribution::guard::{authority_read, authority_rows, write_epoch};
use crate::node::usage_stats::{UsageCounterUpdate, schedule_snapshot_publish};
use crate::replication::queue::schedule_blob_drain;
use crate::staging::page_rows::{
    MAX_PAGE_ENTRIES, PageGuards, check_guards, guard_reads, page_rows, version_reads,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::BlobVersion;
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;

pub use crate::staging::page_rows::{
    PageEntry, PageImportError, PageImportInput, PageImportResult, PageImportState,
};

#[derive(Debug, PartialEq)]
pub struct PageImportOperation {
    input: PageImportInput,
    state: PageImportState,
    txn_id: Option<TxnId>,
    guards: Option<PageGuards>,
    usage: Option<UsageCounterUpdate>,
    result: Option<PageImportResult>,
    output: Option<Result<PageImportResult, PageImportError>>,
}

impl PageImportOperation {
    pub fn new(input: PageImportInput) -> Self {
        Self {
            input,
            state: PageImportState::Init,
            txn_id: None,
            guards: None,
            usage: None,
            result: None,
            output: None,
        }
    }

    fn fail(&mut self, error: PageImportError) -> Effects {
        self.output = Some(Err(error));
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = PageImportState::Abort;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => {
                self.state = PageImportState::Error;
                smallvec![]
            }
        }
    }

    fn read_secret(&mut self) -> Result<Effects, PageImportError> {
        self.state = PageImportState::ReadSecret;
        let connector_id = self.input.connector.connector_id;
        Ok(smallvec![read_secret_effect(
            connector_id,
            Some(self.txn()?)
        )])
    }

    fn txn(&self) -> Result<TxnId, PageImportError> {
        self.txn_id.ok_or(StorageError::TransactionNotFound.into())
    }

    fn read_versions(&mut self, txn_id: TxnId) -> Result<Effects, PageImportError> {
        let heads = self.guards.as_ref().map(|guards| guards.heads.as_slice());
        let reads = version_reads(&self.input, heads.unwrap_or_default())?;
        if reads.is_empty() {
            return self.write_rows(txn_id, Vec::new());
        }
        self.state = PageImportState::ReadVersions;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })])
    }

    fn write_rows(
        &mut self,
        txn_id: TxnId,
        versions: Vec<Option<BlobVersion>>,
    ) -> Result<Effects, PageImportError> {
        let guards = self.guards.as_ref().ok_or(PageImportError::NotFinished)?;
        let rows = page_rows(&self.input, guards, versions)?;
        if rows.result.registered > 0 {
            let usage = UsageCounterUpdate::for_group(self.input.group_id, rows.delta);
            self.usage = Some(usage.with_inventories(rows.inventory));
        }
        self.result = Some(rows.result);
        self.state = PageImportState::WriteRows;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: rows.writes,
            txn_id: Some(txn_id),
        })])
    }

    fn delete_rows(&mut self, txn_id: TxnId) -> Effects {
        if self.input.deletes.is_empty() {
            return self.update_usage(txn_id);
        }
        self.state = PageImportState::DeleteRows;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: self.input.deletes.clone(),
            txn_id: Some(txn_id),
        })]
    }

    fn update_usage(&mut self, txn_id: TxnId) -> Effects {
        match self.usage.as_mut().filter(|usage| !usage.is_noop()) {
            Some(usage) => {
                self.state = PageImportState::UpdateUsage;
                usage.start(txn_id)
            }
            None => self.commit(txn_id),
        }
    }

    fn commit(&mut self, txn_id: TxnId) -> Effects {
        self.state = PageImportState::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn finish(&mut self) -> Effects {
        self.state = PageImportState::Finish;
        self.output = Some(self.result.take().ok_or(PageImportError::NotFinished));
        smallvec![]
    }

    fn step_inner(&mut self, event: Event) -> Result<Effects, PageImportError> {
        match (self.state, event) {
            (
                PageImportState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = PageImportState::ReadGuards;
                Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: guard_reads(&self.input)?,
                    txn_id: Some(txn_id),
                })])
            }
            (
                PageImportState::ReadGuards,
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => {
                let guards = check_guards(&self.input, values)?;
                // Read in the page transaction, so a fence or record set meanwhile conflicts.
                let txn_id = self.txn_id;
                let record =
                    (guards.binding.as_ref()).map(|binding| authority_read(binding.set_id, txn_id));
                self.guards = Some(guards);
                match record {
                    Some(read) => {
                        self.state = PageImportState::ReadRecord;
                        Ok(smallvec![read])
                    }
                    None => self.read_secret(),
                }
            }
            (
                PageImportState::ReadRecord,
                event @ Event::Storage(StorageEvent::BatchReadResult { .. }),
            ) => {
                let (record, fence) = authority_rows(event)?;
                let guards = self.guards.as_mut().ok_or(PageImportError::NotFinished)?;
                let binding = guards
                    .binding
                    .as_ref()
                    .ok_or(PageImportError::NotFinished)?;
                // Every key of the page must lie in a range this member coordinates.
                guards.epochs = self
                    .input
                    .entries
                    .iter()
                    .map(|entry| {
                        let key = &entry.target_key;
                        write_epoch(binding, key, record.as_deref(), fence.as_deref())
                    })
                    .collect::<Result<_, _>>()?;
                self.read_secret()
            }
            (
                PageImportState::ReadSecret,
                event @ Event::Storage(StorageEvent::VaultResult { .. }),
            ) => {
                let secret = parse_secret_read(event, self.input.connector.connector_id)?;
                if secret.as_ref().map(secret_fingerprint) != self.input.secret_fingerprint {
                    return Err(PageImportError::ConnectorChanged);
                }
                self.read_versions(self.txn()?)
            }
            (
                PageImportState::ReadVersions,
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => {
                let versions = values
                    .into_iter()
                    .map(|(_, value)| {
                        value
                            .map(|value| BlobVersion::from_bytes(value.as_ref()))
                            .transpose()
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                self.write_rows(self.txn()?, versions)
            }
            (PageImportState::WriteRows, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                Ok(self.delete_rows(self.txn()?))
            }
            (
                PageImportState::DeleteRows,
                Event::Storage(StorageEvent::BatchDeleteResult { .. }),
            ) => Ok(self.update_usage(self.txn()?)),
            (PageImportState::UpdateUsage, event @ Event::Storage(_)) => {
                let txn_id = self.txn()?;
                let usage = self.usage.as_mut().ok_or(PageImportError::NotFinished)?;
                match usage.step(event, txn_id)? {
                    Some(effects) => Ok(effects),
                    None => Ok(self.commit(txn_id)),
                }
            }
            (
                PageImportState::Commit,
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
                self.txn_id = None;
                if self.usage.is_none() {
                    return Ok(self.finish());
                }
                // The dirty markers and obligations committed above are the durable work.
                self.state = PageImportState::Schedule;
                Ok(smallvec![schedule_snapshot_publish()])
            }
            (PageImportState::Schedule, Event::Task(_)) => {
                self.state = PageImportState::ScheduleDrain;
                Ok(smallvec![schedule_blob_drain()])
            }
            (PageImportState::Commit, Event::Storage(StorageEvent::Error { error })) => {
                // A failed commit leaves no transaction to abort.
                self.txn_id = None;
                Err(error.into())
            }
            (PageImportState::ScheduleDrain, Event::Task(_)) => Ok(self.finish()),
            (PageImportState::Abort, Event::Storage(_)) => {
                self.state = PageImportState::Error;
                Ok(smallvec![])
            }
            (state, Event::Storage(StorageEvent::Error { error }))
                if state != PageImportState::Abort =>
            {
                Err(error.into())
            }
            (state, event) => Err(PageImportError::UnexpectedEvent {
                state,
                expected: expected_event(state),
                received: format!("{event:?}"),
            }),
        }
    }
}

fn expected_event(state: PageImportState) -> &'static str {
    match state {
        PageImportState::StartTransaction => "TransactionStarted",
        PageImportState::ReadGuards | PageImportState::ReadVersions => "BatchReadResult",
        PageImportState::ReadSecret => "VaultResult",
        PageImportState::ReadRecord => "ReadResult",
        PageImportState::WriteRows | PageImportState::UpdateUsage => "BatchWriteResult",
        PageImportState::DeleteRows => "BatchDeleteResult",
        PageImportState::Commit => "TransactionCommitted",
        PageImportState::Schedule | PageImportState::ScheduleDrain => "Task",
        PageImportState::Abort => "TransactionAborted",
        PageImportState::Init | PageImportState::Finish | PageImportState::Error => "no event",
    }
}

impl Operation for PageImportOperation {
    type Output = PageImportResult;
    type Error = PageImportError;

    fn start(&mut self) -> Effects {
        let entries = self.input.entries.len();
        if entries > MAX_PAGE_ENTRIES {
            return self.fail(PageImportError::TooManyEntries(entries));
        }
        self.state = PageImportState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if matches!(self.state, PageImportState::Finish | PageImportState::Error) {
            return smallvec![];
        }
        match self.step_inner(event) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PageImportState::Finish | PageImportState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(PageImportError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = PageImportState::Abort;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => smallvec![],
        }
    }
}

#[cfg(test)]
#[path = "page_tests.rs"]
mod tests;
