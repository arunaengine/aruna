//! Runs row plans against the node's storage handle for the document sync service.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::document_apply::ApplyError;
use aruna_core::effects::Effect;
use aruna_core::errors::StorageError;
use aruna_core::operation::{ByCalls, EffectOperation, Runnable};
use aruna_core::row_plan::{PlanOperation, RowPlan};
use aruna_core::time::unix_timestamp_millis;
use aruna_storage::StorageHandle;

use crate::error::{NetError, Result};

impl From<ApplyError> for NetError {
    fn from(error: ApplyError) -> Self {
        match error {
            ApplyError::Invalid(reason) => Self::Bootstrap(reason),
            ApplyError::Deferred(reason) => Self::Deferred(reason),
            ApplyError::Storage(error) => Self::Storage(error),
            ApplyError::Failed(reason) => Self::Dht(reason),
        }
    }
}

/// Runs `plan` once in its transaction.
pub(super) async fn run_plan<P>(storage: &StorageHandle, plan: P) -> Result<P::Output>
where
    P: RowPlan<Error = ApplyError>,
    P::Output: PartialEq,
{
    let operation = PlanOperation::new(plan);
    let mut machine = Runnable::<ByCalls>::into_machine(operation);
    machine.stamp(unix_timestamp_millis());
    let mut effects: VecDeque<Effect> = machine.start().into_iter().collect();
    while let Some(effect) = effects.pop_front() {
        let Effect::Storage(effect) = effect else {
            return Err(NetError::Dht("a row plan asked for other I/O".to_string()));
        };
        let event = storage.send_storage_effect(effect).await;
        if !machine.is_complete() {
            effects.extend(machine.step(event));
        }
    }
    machine.finalize().map_err(NetError::from)
}

/// Runs the plan `build` makes, again after each lost transaction conflict it allows.
pub(super) async fn retry_plan<P>(
    storage: &StorageHandle,
    build: impl Fn() -> P,
    conflicted: &str,
) -> Result<P::Output>
where
    P: RowPlan<Error = ApplyError>,
    P::Output: PartialEq,
{
    for _ in 0..=P::conflict_retries() {
        tokio::task::yield_now().await;
        match run_plan(storage, build()).await {
            Err(NetError::Storage(StorageError::TransactionConflict)) => {}
            result => return result,
        }
    }
    Err(NetError::Dht(conflicted.to_string()))
}
