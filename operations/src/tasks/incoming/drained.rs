//! Removes disabled tenant backends that hold nothing any more, once the reclaim sweep emptied them.
//! The blob handle's claim must outlive the removal, so this stays with the task input.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::group_backend::GroupStorage;
use tracing::{info, warn};

use crate::groups::backends::remove::{
    DisabledScanOperation, HoldingScanOperation, RemoveBackendOperation,
};
use crate::driver::{DriverContext, drive};

/// Deletes the record and credentials of every disabled tenant backend that no
/// longer holds anything. Runs after the reclaim sweep, which is what empties
/// them; a backend on `retain` keeps its rows and so is never removed.
pub(crate) async fn remove_drained_backends(context: &DriverContext) -> Result<usize, String> {
    let disabled = drive(DisabledScanOperation::default(), context)
        .await
        .map_err(|error| error.to_string())?;
    if disabled.is_empty() {
        return Ok(0);
    }
    // Read before the scan: a backend held at no point since here can have
    // gained neither a copy nor a cleanup row that the scan would then miss.
    let idle = idle_backends(context, disabled);
    if idle.is_empty() {
        return Ok(0);
    }
    let holding = drive(HoldingScanOperation::default(), context)
        .await
        .map_err(|error| error.to_string())?;

    let mut removed = 0usize;
    for (record, generation) in idle {
        let backend = BackendRef::Group(record.backend_id);
        if holding.contains(&backend) {
            continue;
        }
        let _claim = match context.blob_handle.as_ref() {
            Some(blob_handle) => match blob_handle.claim_backend(record.backend_id, generation) {
                Some(claim) => Some(claim),
                None => continue,
            },
            None => None,
        };
        match drive(
            RemoveBackendOperation::new(record.group_id, record.backend_id),
            context,
        )
        .await
        {
            Ok(()) => {
                info!(backend = %backend, "Removed drained storage backend");
                removed = removed.saturating_add(1);
            }
            Err(error) => warn!(backend = %backend, error = %error, "Backend removal failed"),
        }
    }
    Ok(removed)
}

/// Every disabled backend nothing is currently holding, with the hold
/// generation the later claim has to still match.
fn idle_backends(context: &DriverContext, disabled: Vec<GroupStorage>) -> Vec<(GroupStorage, u64)> {
    let Some(blob_handle) = context.blob_handle.as_ref() else {
        // Nothing in this process can run a blob effect, so nothing to exclude.
        return disabled.into_iter().map(|record| (record, 0)).collect();
    };
    disabled
        .into_iter()
        .filter_map(|record| {
            blob_handle
                .idle_generation(record.backend_id)
                .map(|generation| (record, generation))
        })
        .collect()
}
