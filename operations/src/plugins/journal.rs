//! Reads and prunes this node's plugin event journal: kept rows for replay, ordered by journal
//! time (rows of one millisecond in no set order), and pending rows the matcher has not read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::EVENT_JOURNAL_KEYSPACE;
use aruna_core::plugin_runs::{
    JOURNAL_RETENTION_MS, JOURNAL_ROW, SOURCE_ROW, SourceEvent, journal_entries, journal_key,
};
use ulid::Ulid;

use super::store::{StoreError, Txn};
use crate::driver::DriverContext;

/// Rows a journal page holds at most.
pub const JOURNAL_PAGE: usize = 500;

/// One journal row: its key and its event.
pub type JournalRow = (Vec<u8>, SourceEvent);

/// At most `limit` rows of `kind` after the key `after`, oldest first, and the key to continue
/// after when more may follow. A row that does not decode is skipped.
pub async fn journal_page(
    txn: &Txn<'_>,
    kind: u8,
    after: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Vec<JournalRow>, Option<Vec<u8>>), StoreError> {
    let (rows, next) = txn
        .page(EVENT_JOURNAL_KEYSPACE, &[kind], after, limit)
        .await?;
    let rows = (rows.into_iter())
        .filter_map(|(key, value)| Some((key, SourceEvent::from_bytes(&value).ok()?)))
        .collect();
    Ok((rows, next))
}

/// The version write `effect` with the event's journal rows in the same batch. A single write
/// becomes a batch, so its caller must accept `BatchWriteResult`.
pub fn journaled(effect: Effect, event: &SourceEvent) -> Result<Effect, ConversionError> {
    let rows = journal_entries(event)?;
    match effect {
        Effect::Storage(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id,
        }) => Ok(Effect::Storage(StorageEffect::BatchWrite {
            writes: [vec![(key_space, key, value)], rows].concat(),
            txn_id,
        })),
        Effect::Storage(StorageEffect::BatchWrite { writes, txn_id }) => {
            Ok(Effect::Storage(StorageEffect::BatchWrite {
                writes: [writes, rows].concat(),
                txn_id,
            }))
        }
        other => Ok(other),
    }
}

/// Deletes kept rows journaled longer than the retention ago, and source-id rows whose anchor
/// is that old, after which a source is not journaled again; returns how many rows.
pub async fn prune_journal(ctx: &DriverContext, now_ms: u64) -> Result<usize, StoreError> {
    let txn = Txn::direct(ctx);
    let kept_from = now_ms.saturating_sub(JOURNAL_RETENTION_MS);
    let cutoff = journal_key(JOURNAL_ROW, kept_from, Ulid::nil());
    let mut pruned = 0;
    loop {
        let (rows, _) = txn
            .page(EVENT_JOURNAL_KEYSPACE, &[JOURNAL_ROW], None, JOURNAL_PAGE)
            .await?;
        let old: Vec<Vec<u8>> = (rows.into_iter())
            .map(|(key, _)| key)
            .take_while(|key| *key < cutoff)
            .collect();
        if old.is_empty() {
            break;
        }
        pruned += old.len();
        txn.delete(EVENT_JOURNAL_KEYSPACE, old).await?;
    }
    let sources = txn.scan(EVENT_JOURNAL_KEYSPACE, &[SOURCE_ROW]).await?;
    let old: Vec<Vec<u8>> = (sources.into_iter())
        .filter(|(_, value)| {
            let anchor = value.as_slice().try_into().map(u64::from_be_bytes);
            anchor.is_ok_and(|anchor| anchor < kept_from)
        })
        .map(|(key, _)| key)
        .collect();
    pruned += old.len();
    txn.delete(EVENT_JOURNAL_KEYSPACE, old).await?;
    Ok(pruned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::runs::tests::opened;
    use aruna_core::plugin_runs::{SourceKind, journal_entries};

    fn event(journaled_ms: u64) -> SourceEvent {
        SourceEvent {
            kind: SourceKind::DatasetCreated,
            group_id: Ulid::from_parts(1, 1),
            path: "/r/g/x/meta/a@d".into(),
            resource: "d".into(),
            revision: "e".into(),
            source: format!("dataset_created/{journaled_ms}"),
            anchor_ms: journaled_ms,
            journaled_ms,
        }
    }

    #[tokio::test]
    async fn retention_pruning() {
        let (_dir, ctx) = opened();
        let txn = Txn::direct(&ctx);
        for at in [10, 20, 10 + JOURNAL_RETENTION_MS] {
            let rows = (journal_entries(&event(at)).unwrap().into_iter())
                .map(|(_, key, value)| (key.to_vec(), value.to_vec()))
                .collect();
            txn.write(EVENT_JOURNAL_KEYSPACE, rows).await.unwrap();
        }
        let now = 15 + JOURNAL_RETENTION_MS;
        // One kept row and the source-id row of the same old event.
        assert_eq!(prune_journal(&ctx, now).await.unwrap(), 2);
        let sources = txn
            .scan(EVENT_JOURNAL_KEYSPACE, &[SOURCE_ROW])
            .await
            .unwrap();
        assert_eq!(sources.len(), 2);
        let (kept, _) = journal_page(&txn, JOURNAL_ROW, None, 10).await.unwrap();
        let times: Vec<u64> = kept.iter().map(|(_, row)| row.journaled_ms).collect();
        assert_eq!(times, [20, 10 + JOURNAL_RETENTION_MS]);
        // Pending rows are the matcher's; retention pruning leaves them.
        let (pending, _) = journal_page(&txn, b'u', None, 10).await.unwrap();
        assert_eq!(pending.len(), 3);
    }

    #[tokio::test]
    async fn local_create_once() {
        // The accepting node journals a creation in its batch; its projection adds nothing.
        use crate::metadata::create_document::{
            CreateDocumentConfig, CreateDocumentOperation, CreateDocumentPayload,
            create_metadata_document,
        };
        let fixture = crate::plugins::install::tests::Fixture::new().await;
        let mut ctx = fixture.ctx.clone();
        let dir = tempfile::tempdir().unwrap();
        let storage = fixture.ctx.storage_handle.clone();
        let metadata = crate::metadata::MetadataHandle::new(
            dir.path().join("meta"),
            fixture.actor.node_id,
            storage,
            None,
            None,
            None,
        );
        ctx.metadata_handle = Some(metadata.unwrap());
        let ctx = std::sync::Arc::new(ctx);
        let create = CreateDocumentOperation::new(CreateDocumentConfig {
            actor: fixture.actor.clone(),
            group_id: fixture.group_id,
            document_id: Ulid::nil(),
            document_path: "/docs/one/".to_string(),
            public: false,
            payload: CreateDocumentPayload::Scaffold {
                name: "one".to_string(),
                description: String::new(),
                date_published: "2026-01-01".to_string(),
                license: None,
            },
        });
        let created = create_metadata_document(create, ctx.clone()).await.unwrap();
        for _ in 0..3 {
            crate::metadata::projector::drain_projection_queue(&ctx)
                .await
                .unwrap();
        }
        let txn = Txn::direct(&ctx);
        let (kept, _) = journal_page(&txn, JOURNAL_ROW, None, 10).await.unwrap();
        let resources: Vec<String> = kept.into_iter().map(|(_, row)| row.resource).collect();
        assert_eq!(resources, [created.record.document_id.to_string()]);
    }
}
