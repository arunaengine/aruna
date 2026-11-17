//! Admits webhook requests on an install's receiver: checks the install and its rate, verifies
//! the request with its bound secret, removes credential headers and queues one run per delivery.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::keyspaces::PLUGIN_HOOKS_KEYSPACE;
use aruna_core::plugin_runs::{PluginRun, hook_key, hook_time_key};
use ulid::Ulid;

use super::rows::{Answer, Ask, Plan, Row, RowsOperation, Turn, done, encode, row, values};
use super::runs::{Admission, RunError};

/// Headers removed before a request is stored: hop-by-hop ones and the relay mark. The check
/// itself removes the credential headers.
pub(crate) const DROPPED: &[&str] = &[
    "connection",
    "host",
    "keep-alive",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "x-aruna-relay-hop",
];

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum HookError {
    #[error("no install with a webhook trigger has this id")]
    NotFound,
    #[error("this node does not run the install; its receiver is {0}")]
    Elsewhere(NodeId),
    #[error("the install takes no webhooks now: {0}")]
    Paused(&'static str),
    #[error("the install is over its webhook rate")]
    Rate,
    #[error("the request did not pass its check")]
    Unverified,
    #[error("the request cannot be checked now: {0}")]
    Unavailable(String),
    #[error(transparent)]
    Run(#[from] RunError),
}

/// Queues a webhook's run once per delivery id in one transaction: `false` for a delivery id
/// the receiver already admitted, which starts nothing. The caller holds `ADMISSIONS`.
#[derive(Debug, PartialEq)]
pub struct AdmitHook {
    /// The delivery's rows, written with the run; `None` without a delivery id.
    delivery: Option<(Vec<u8>, Vec<Row>)>,
    admission: Admission,
    phase: HookPhase,
}

#[derive(Debug, PartialEq)]
enum HookPhase {
    Seen,
    Admitting,
    Writing,
}

impl AdmitHook {
    pub fn new(
        run: PluginRun,
        (install_id, delivery_id): (Ulid, Option<String>),
        now_ms: u64,
    ) -> Result<Self, RunError> {
        let delivery = match delivery_id {
            Some(id) => {
                let key = hook_key(install_id, &id);
                let time = hook_time_key(now_ms, install_id, &id);
                let rows = vec![
                    row(PLUGIN_HOOKS_KEYSPACE, key.clone(), encode(&now_ms)?),
                    row(PLUGIN_HOOKS_KEYSPACE, time, Vec::new()),
                ];
                Some((key, rows))
            }
            None => None,
        };
        Ok(Self {
            delivery,
            admission: Admission::new(run),
            phase: HookPhase::Seen,
        })
    }
}

impl Plan for AdmitHook {
    type Output = bool;
    type Error = RunError;

    fn first(&mut self) -> Result<Turn<bool>, RunError> {
        match &self.delivery {
            Some((key, _)) => Ok(Turn::Ask(Ask::Read(vec![(
                PLUGIN_HOOKS_KEYSPACE,
                key.clone(),
            )]))),
            None => {
                self.phase = HookPhase::Admitting;
                Ok(Turn::Ask(self.admission.ask()))
            }
        }
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<bool>, RunError> {
        match self.phase {
            HookPhase::Seen => {
                let [seen] = values::<1>(answer)?;
                if seen.is_some() {
                    return Ok(Turn::Finish(false));
                }
                self.phase = HookPhase::Admitting;
                Ok(Turn::Ask(self.admission.ask()))
            }
            HookPhase::Admitting => {
                let Some(mut rows) = self.admission.take(answer)? else {
                    return Ok(Turn::Ask(self.admission.ask()));
                };
                if let Some((_, delivery)) = self.delivery.take() {
                    rows.splice(0..0, delivery);
                }
                self.phase = HookPhase::Writing;
                Ok(Turn::Ask(Ask::Write(rows)))
            }
            HookPhase::Writing => {
                done(answer)?;
                Ok(Turn::Finish(true))
            }
        }
    }
}

#[cfg(test)]
#[path = "hooks_tests.rs"]
mod tests;
