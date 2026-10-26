//! Lists how files get into a bucket, from configuration only: syncs that copy into it and
//! storage folders registered into it. Outgoing syncs and member nodes are not sources.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Nothing here reads the bucket's objects, so the cost never grows with the bucket.

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BACKEND_IMPORT_KEYSPACE, RELATIONSHIP_IN_KEYSPACE};
use aruna_core::structs::storage::backend_import::BackendImport;
use aruna_core::structs::{SyncMode, SyncRelationship, sync_relationship_prefix};
use aruna_core::types::GroupId;
use ulid::Ulid;

use crate::driver::DriverContext;

/// At most this many rows of each kind are read.
const MAX_ROWS: usize = 256;

/// One incoming path of a bucket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FileSource {
    /// Another bucket's files are copied in by a sync.
    Sync {
        relationship_id: Ulid,
        source: String,
        mode: SyncMode,
    },
    /// A storage backend folder of the group is registered in place.
    Storage {
        import_id: Ulid,
        backend_id: Ulid,
        backend_prefix: String,
        key_prefix: String,
    },
}

/// The incoming paths of `bucket`, owned by `group_id`, and whether every row was read.
pub async fn bucket_sources(
    context: &DriverContext,
    group_id: GroupId,
    bucket: &str,
) -> Result<(Vec<FileSource>, bool), String> {
    let mut sources = Vec::new();
    let (syncs, all_syncs) = rows(
        context,
        RELATIONSHIP_IN_KEYSPACE,
        sync_relationship_prefix(bucket),
    )
    .await?;
    for value in syncs {
        let relationship =
            SyncRelationship::from_bytes(&value).map_err(|error| error.to_string())?;
        sources.push(FileSource::Sync {
            relationship_id: relationship.id,
            source: relationship.source.to_string(),
            mode: relationship.mode,
        });
    }
    let (imports, all_imports) = rows(
        context,
        BACKEND_IMPORT_KEYSPACE,
        group_id.to_bytes().to_vec(),
    )
    .await?;
    for value in imports {
        let import = BackendImport::from_bytes(&value).map_err(|error| error.to_string())?;
        if import.bucket == bucket {
            sources.push(FileSource::Storage {
                import_id: import.import_id,
                backend_id: import.backend_id,
                backend_prefix: import.backend_prefix,
                key_prefix: import.key_prefix,
            });
        }
    }
    Ok((sources, all_syncs && all_imports))
}

async fn rows(
    context: &DriverContext,
    key_space: &str,
    prefix: Vec<u8>,
) -> Result<(Vec<Vec<u8>>, bool), String> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: Some(prefix.into()),
            start: None::<IterStart>,
            limit: MAX_ROWS,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((
            values
                .into_iter()
                .map(|(_, value)| value.to_vec())
                .collect(),
            next_start_after.is_none(),
        )),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage answer {other:?}")),
    }
}
