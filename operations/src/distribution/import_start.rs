//! Runs the imports a former coordinator handed over as jobs of this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::NODE_STATE_KEYSPACE;
use aruna_core::structs::execution::job::StagingJobSpec;
use aruna_core::types::Key;

use super::imports::KeptStart;
use crate::driver::DriverContext;
use crate::jobs::JOB_RETENTION_MS;
use crate::jobs::service::submit_handed_job;

/// Runs each fetched job here as its own job from its handed position; a started job leaves
/// the row at once.
pub(crate) async fn start_kept(context: &DriverContext, start: KeptStart) -> Result<(), String> {
    let Some(local) = context.net_handle.as_ref().map(|net| net.node_id()) else {
        return Ok(());
    };
    let KeptStart {
        key,
        mut kept,
        bucket,
    } = start;
    let key = Key::from(key);
    while let Some((spec, resume)) = kept.ready.first().cloned() {
        let spec = StagingJobSpec {
            bucket_identity: bucket.identity(),
            ..spec
        };
        submit_handed_job(context, spec, resume.as_ref(), local, JOB_RETENTION_MS).await?;
        kept.ready.remove(0);
        let value = postcard::to_allocvec(&kept).map_err(|error| error.to_string())?;
        let write = StorageEffect::Write {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: key.clone(),
            value: value.into(),
            txn_id: None,
        };
        match context.storage_handle.send_storage_effect(write).await {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => return Err(format!("handed over imports not kept: {}", other.label())),
        }
    }
    let delete = StorageEffect::Delete {
        key_space: NODE_STATE_KEYSPACE.to_string(),
        key,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(delete).await {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        other => Err(format!(
            "handed over imports not dropped: {}",
            other.label()
        )),
    }
}
