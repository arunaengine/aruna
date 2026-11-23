//! One pass over this node's plugin task rows: each is settled by [`SettleTask`], and the resends
//! and cancels it hands back run here.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::InstallState;
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use tracing::warn;
use ulid::Ulid;

use super::super::check::CallError;
use super::super::install::{config_answer, config_call};
use super::super::rows::{Rows, StoreError, decode, scan_rows};
use super::super::work::{WorkChange, install_at, update_work, work_scan};
use super::cancel::CancelTask;
use super::send::SendTask;
use super::settle::{Followup, SettleTask, Settled};
use crate::jobs::lifecycle::submit_external::IngressNode;
use aruna_core::install_work::{WorkKind, WorkRow};

#[derive(Debug, PartialEq)]
enum PassPhase {
    Config,
    Scan,
    Settle(Ulid, Box<WorkRow>),
    Resend,
    Cancel(Ulid, Ulid, JobId),
    Mark,
}

/// Follows this node's task families: settles, adopts, resends, cancels and proves absence.
/// Answers how many rows a change settled.
#[derive(Debug, PartialEq)]
pub struct SettleTasks {
    node: IngressNode,
    config: Option<Arc<RealmConfigDocument>>,
    rows: VecDeque<(Vec<u8>, Vec<u8>)>,
    settled: usize,
    phase: PassPhase,
}

type PassNext = Result<Next<usize>, StoreError>;

impl SettleTasks {
    pub fn new(node: IngressNode) -> Self {
        Self {
            node,
            config: None,
            rows: VecDeque::new(),
            settled: 0,
            phase: PassPhase::Config,
        }
    }

    fn config(&self) -> Result<Arc<RealmConfigDocument>, StoreError> {
        self.config
            .clone()
            .ok_or(StoreError::from(IoError::Unfinished))
    }

    fn advance(&mut self) -> PassNext {
        while let Some((key, value)) = self.rows.pop_front() {
            let row: WorkRow = decode(&value)?;
            let Some(install_id) = install_at(&key).filter(|_| row.kind == WorkKind::Recipe) else {
                continue;
            };
            let settle =
                SettleTask::new(self.config()?, self.node.local, (install_id, row.clone()));
            self.phase = PassPhase::Settle(install_id, Box::new(row));
            return Ok(Next::Call(IoCall::child(settle)));
        }
        Ok(Next::Done(self.settled))
    }

    fn follow(&mut self, (install_id, row): (Ulid, WorkRow), settled: Settled) -> PassNext {
        self.settled += usize::from(settled.changed);
        let config = self.config()?;
        match settled.followup {
            Followup::Nothing => self.advance(),
            Followup::Resend => {
                let install = (config.installs.iter()).find(|install| {
                    install.install_id == install_id && install.state == InstallState::Active
                });
                let Some(install) = install else {
                    return self.advance();
                };
                let send = SendTask::new(install, row.work_id, None, self.node.clone());
                self.phase = PassPhase::Resend;
                Ok(Next::Call(IoCall::child(send)))
            }
            Followup::Cancel { alias, family } => {
                let task = (row.account, alias, family);
                let cancel = CancelTask::new(config, self.node.local, task);
                self.phase = PassPhase::Cancel(install_id, row.work_id, alias);
                Ok(Next::Call(IoCall::child(cancel)))
            }
        }
    }
}

impl Operation for SettleTasks {
    type Output = usize;
    type Error = StoreError;

    fn first(&mut self) -> PassNext {
        Ok(Next::Call(config_call(self.node.realm_id)))
    }

    fn next(&mut self, result: IoResult) -> PassNext {
        match std::mem::replace(&mut self.phase, PassPhase::Mark) {
            PassPhase::Config => {
                let config = config_answer(result)
                    .map_err(|error| StoreError::Message(error.to_string()))?;
                self.config = Some(Arc::new(config));
                self.phase = PassPhase::Scan;
                Ok(Next::Call(IoCall::child(scan_rows(work_scan()))))
            }
            PassPhase::Scan => {
                self.rows = result.child::<Rows, StoreError>()??.into();
                self.advance()
            }
            PassPhase::Settle(install_id, row) => {
                let settled = result.child::<Settled, StoreError>()??;
                self.follow((install_id, *row), settled)
            }
            PassPhase::Resend => {
                if let Err(CallError(error)) = result.child::<(), CallError>()? {
                    warn!(error = %error.message, "A plugin task's resend did not go out");
                }
                self.advance()
            }
            PassPhase::Cancel(install_id, work_id, alias) => {
                if !result.child::<bool, IoError>()?.unwrap_or(false) {
                    return self.advance();
                }
                let update = update_work((install_id, work_id), WorkChange::Cancelling(alias));
                self.phase = PassPhase::Mark;
                Ok(Next::Call(IoCall::child(update)))
            }
            PassPhase::Mark => {
                result.child::<Option<WorkRow>, StoreError>()??;
                self.advance()
            }
        }
    }
}

#[cfg(test)]
#[path = "pass_tests.rs"]
mod tests;
