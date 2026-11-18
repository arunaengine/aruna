//! Repairs settled installs on a management node: each account and role is made to match its
//! record again, as the admin who granted it, then unnamed plugin accounts are swept.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use tracing::{info, warn};
use ulid::Ulid;

use super::account::{AccountChange, AccountError, RepairAccount, StepError};
use super::install::{InstallError, config_answer, config_call};
use super::sweep::SweepOrphans;
use crate::realm::mutate_placement::is_management;

/// The node a repair runs on.
pub type RepairNode = (RealmId, NodeId);

/// The repair of one install's account and role, as the admin who granted it.
pub(crate) fn repair_account(
    (realm_id, node_id): RepairNode,
    install: &PluginInstall,
    now_ms: u64,
) -> RepairAccount {
    let user_id = install.granted_by;
    let actor = Actor {
        node_id,
        user_id,
        realm_id,
    };
    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let change = AccountChange::of((&actor, &auth), install, now_ms / 1000);
    RepairAccount::new(change, install)
}

/// Repairs each settled install and answers how many changed; one that fails is logged and the
/// others are still repaired.
#[derive(Debug, PartialEq)]
pub(crate) struct RepairSettled {
    node: RepairNode,
    now_ms: u64,
    installs: VecDeque<PluginInstall>,
    current: Option<Ulid>,
    repaired: usize,
}

impl RepairSettled {
    pub(crate) fn new(node: RepairNode, installs: &[PluginInstall], now_ms: u64) -> Self {
        let unsettled = [InstallState::Preparing, InstallState::Removing];
        let installs = (installs.iter())
            .filter(|install| !unsettled.contains(&install.state))
            .cloned()
            .collect();
        Self {
            node,
            now_ms,
            installs,
            current: None,
            repaired: 0,
        }
    }

    fn advance(&mut self) -> Next<usize> {
        let Some(install) = self.installs.pop_front() else {
            return Next::Done(self.repaired);
        };
        self.current = Some(install.install_id);
        let repair = repair_account(self.node, &install, self.now_ms);
        Next::Call(IoCall::child(repair))
    }
}

impl Operation for RepairSettled {
    type Output = usize;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<usize>, IoError> {
        Ok(self.advance())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, IoError> {
        let install = self.current.take().ok_or(IoError::Unfinished)?;
        match result.child::<bool, StepError>()? {
            Ok(true) => {
                info!(%install, "Repaired a plugin install's account and role");
                self.repaired += 1;
            }
            Ok(false) => {}
            Err(error) => warn!(%install, %error, "Could not repair a plugin install"),
        }
        Ok(self.advance())
    }
}

#[derive(Debug, PartialEq)]
enum ReconcilePhase {
    Config,
    Repair(Arc<RealmConfigDocument>),
    Sweep(usize),
}

/// Repairs every settled install from its current record on a management node, then sweeps
/// accounts no record names any more. Another node has nothing to do.
#[derive(Debug, PartialEq)]
pub struct ReconcileInstalls {
    node: RepairNode,
    now_ms: u64,
    phase: ReconcilePhase,
}

impl ReconcileInstalls {
    pub fn new(node: RepairNode, now_ms: u64) -> Self {
        Self {
            node,
            now_ms,
            phase: ReconcilePhase::Config,
        }
    }
}

impl Operation for ReconcileInstalls {
    type Output = usize;
    type Error = InstallError;

    fn first(&mut self) -> Result<Next<usize>, InstallError> {
        Ok(Next::Call(config_call(self.node.0)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, InstallError> {
        match std::mem::replace(&mut self.phase, ReconcilePhase::Sweep(0)) {
            ReconcilePhase::Config => {
                let config = config_answer(result)?;
                if !is_management(&config, self.node.1) {
                    return Ok(Next::Done(0));
                }
                let repair = RepairSettled::new(self.node, &config.installs, self.now_ms);
                self.phase = ReconcilePhase::Repair(Arc::new(config));
                Ok(Next::Call(IoCall::child(repair)))
            }
            ReconcilePhase::Repair(config) => {
                let repaired = result.child::<usize, IoError>()??;
                self.phase = ReconcilePhase::Sweep(repaired);
                let sweep = SweepOrphans::new(config, self.node.1, self.now_ms);
                Ok(Next::Call(IoCall::child(sweep)))
            }
            ReconcilePhase::Sweep(repaired) => {
                let swept = result.child::<usize, AccountError>()?;
                let swept = swept.unwrap_or_else(|error| {
                    warn!(%error, "Could not sweep unnamed plugin accounts");
                    0
                });
                Ok(Next::Done(repaired + swept))
            }
        }
    }
}

#[cfg(test)]
#[path = "reconcile_tests.rs"]
mod tests;
