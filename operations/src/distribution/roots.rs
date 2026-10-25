//! Checks the source roots a member holds references of before it completes a transfer: each
//! root is resolved through the member's own connector of it, in the set's group, and probed
//! once; a checked root stays checked. A root that fails is tried again on the next pass.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::SOURCE_ROOT_KEYSPACE;
use aruna_core::structs::storage::distribution::source_root::{RootEntry, checked_key, decode_key};
use aruna_core::types::{GroupId, Key, Value};
use tracing::warn;
use ulid::Ulid;

use crate::connectors::{ResolveBindingInput, ResolveBindingOperation};
use crate::driver::{DriverContext, drive};
use crate::staging::check_source::CheckSourceOperation;

/// Root rows one read returns; a set has few roots, so one page is typical.
const ROOT_PAGE: usize = 256;
/// Unchecked roots one pass probes at most.
const MAX_PROBES: usize = 8;

/// What a walk over a set's root rows found.
#[derive(Default)]
struct Walk {
    /// The last root row seen whose checked marker has not followed yet.
    unchecked: Option<([u8; 32], RootEntry)>,
    probes: usize,
    all_checked: bool,
}

/// Whether every source root this node recorded for the set is checked: readable through
/// this node's connector of it and allowed for the set's group. Probes a few roots per call.
pub async fn roots_checked(
    context: &DriverContext,
    set_id: Ulid,
    group_id: GroupId,
) -> Result<bool, String> {
    let prefix = set_id.to_bytes().to_vec();
    let mut walk = Walk {
        all_checked: true,
        ..Walk::default()
    };
    let mut after = None;
    loop {
        let rows = page(context, &prefix, after.take()).await?;
        let Some((last, _)) = rows.last() else {
            break;
        };
        after = Some(last.to_vec());
        for (key, value) in rows {
            let Some((digest, checked)) = decode_key(&key) else {
                continue;
            };
            match (checked, walk.unchecked.take()) {
                (true, Some((open, _))) if open == digest => {}
                (true, open) => walk.unchecked = open,
                (false, open) => {
                    if let Some((open, entry)) = open {
                        walk.check(context, set_id, group_id, (open, entry)).await?;
                    }
                    let entry = RootEntry::from_bytes(&value).map_err(|e| e.to_string())?;
                    walk.unchecked = Some((digest, entry));
                }
            }
        }
    }
    if let Some(open) = walk.unchecked.take() {
        walk.check(context, set_id, group_id, open).await?;
    }
    Ok(walk.all_checked)
}

impl Walk {
    /// Probes one unchecked root while the pass has probes left, and marks it checked.
    async fn check(
        &mut self,
        context: &DriverContext,
        set_id: Ulid,
        group_id: GroupId,
        (digest, entry): ([u8; 32], RootEntry),
    ) -> Result<(), String> {
        if self.probes >= MAX_PROBES {
            self.all_checked = false;
            return Ok(());
        }
        self.probes += 1;
        if let Err(reason) = probe(context, group_id, &entry).await {
            warn!(
                %set_id,
                root = %hex::encode(digest),
                kind = ?entry.root.kind,
                reason,
                "A source root of the set is not readable here; the transfer waits"
            );
            self.all_checked = false;
            return Ok(());
        }
        let mark = StorageEffect::Write {
            key_space: SOURCE_ROOT_KEYSPACE.to_string(),
            key: checked_key(set_id, &digest).into(),
            value: Vec::new().into(),
            txn_id: None,
        };
        match context.storage_handle.send_storage_effect(mark).await {
            Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
            other => Err(format!("unexpected write answer {other:?}")),
        }
    }
}

/// Resolves the root through this node's connector of it in the group, then checks it.
async fn probe(
    context: &DriverContext,
    group_id: GroupId,
    entry: &RootEntry,
) -> Result<(), String> {
    let input = ResolveBindingInput {
        source: entry.probe(),
        group_id: Some(group_id),
    };
    let access = drive(ResolveBindingOperation::new(input), context)
        .await
        .map_err(|error| error.to_string())?;
    drive(CheckSourceOperation::new(access), context)
        .await
        .map_err(|error| error.to_string())
}

async fn page(
    context: &DriverContext,
    prefix: &[u8],
    after: Option<Vec<u8>>,
) -> Result<Vec<(Key, Value)>, String> {
    let iter = StorageEffect::Iter {
        key_space: SOURCE_ROOT_KEYSPACE.to_string(),
        prefix: Some(prefix.to_vec().into()),
        start: after.map(|key| IterStart::After(key.into())),
        limit: ROOT_PAGE,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(iter).await {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected scan answer {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::test_store::{context, get, open};
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::storage::distribution::source_root::{SourceRoot, root_key};
    use std::collections::BTreeMap;

    fn entry(bucket: &str) -> RootEntry {
        RootEntry {
            root: SourceRoot {
                kind: SourceConnectorKind::S3,
                public_config: BTreeMap::from([("bucket".to_string(), bucket.to_string())]),
                namespace: None,
            },
            probe_path: "a.fa".to_string(),
        }
    }

    async fn put(handle: &aruna_storage::storage::StorageHandle, key: Vec<u8>, value: Vec<u8>) {
        let write = StorageEffect::Write {
            key_space: SOURCE_ROOT_KEYSPACE.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        assert!(matches!(
            handle.send_storage_effect(write).await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    #[tokio::test]
    async fn unreadable_root_holds_completion() {
        // Checked roots pass; a root no connector of the group reads stays unchecked.
        let (_dir, handle) = open();
        let context = context(&handle);
        let (set_id, group_id) = (Ulid::from_bytes([1; 16]), Ulid::from_bytes([2; 16]));
        assert!(roots_checked(&context, set_id, group_id).await.unwrap());
        let known = entry("known");
        let digest = known.root.digest().unwrap();
        put(
            &handle,
            root_key(set_id, &digest),
            known.to_bytes().unwrap(),
        )
        .await;
        put(&handle, checked_key(set_id, &digest), Vec::new()).await;
        assert!(roots_checked(&context, set_id, group_id).await.unwrap());
        let unknown = entry("unknown");
        let digest = unknown.root.digest().unwrap();
        put(
            &handle,
            root_key(set_id, &digest),
            unknown.to_bytes().unwrap(),
        )
        .await;
        assert!(!roots_checked(&context, set_id, group_id).await.unwrap());
        let marker = get(&handle, SOURCE_ROOT_KEYSPACE, checked_key(set_id, &digest)).await;
        assert_eq!(marker, None, "a failed probe marks nothing");
    }
}
