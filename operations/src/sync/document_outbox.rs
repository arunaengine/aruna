//! Reads, deletes and retimes the document sync outbox records waiting for publication.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentOutboxRecord;
use aruna_core::document_apply::{ApplyError, Step};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{OUTBOX_INDEX_KEYSPACE, SYNC_OUTBOX_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::row_plan::{NeedsRows, PlanRows, PlanStop, Planned, RowPlan};
use aruna_core::rows::{event, page};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Key, KeySpace};
use byteview::ByteView;
use tracing::warn;

use crate::sync::outbox_record::{
    ADMIN_OUTBOX_PREFIX, outbox_stream_prefixes, revocation_index_key,
};

/// One stored outbox record; a row that does not decode is an error.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadOutboxPlan {
    pub key: Vec<u8>,
}

impl RowPlan for ReadOutboxPlan {
    type Output = Option<DocumentOutboxRecord>;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<Self::Output>> {
        let record = rows
            .read(SYNC_OUTBOX_KEYSPACE, self.key.clone())?
            .map(|bytes| postcard::from_bytes::<DocumentOutboxRecord>(&bytes))
            .transpose()
            .map_err(|error| ApplyError::Failed(error.to_string()))?;
        Ok(Planned::output(record))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::None
    }
}

#[derive(Debug, PartialEq)]
pub struct OutboxReadBatch {
    pub records: Vec<(Vec<u8>, DocumentOutboxRecord)>,
    pub has_more: bool,
    pub next_start_after: Option<Vec<u8>>,
}

/// One page of outbox records; rows that do not decode are deleted on the way.
#[derive(Clone, Debug, PartialEq)]
pub struct OutboxPagePlan {
    pub prefix: Vec<u8>,
    pub start_after: Option<Vec<u8>>,
    pub limit: usize,
}

impl RowPlan for OutboxPagePlan {
    type Output = OutboxReadBatch;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<OutboxReadBatch>> {
        let values = rows.scan_after(
            SYNC_OUTBOX_KEYSPACE,
            Some(ByteView::from(self.prefix.clone())),
            self.start_after.clone().map(ByteView::from),
            self.limit.saturating_add(1),
        )?;
        let has_more = values.len() > self.limit;
        let page: Vec<_> = values.into_iter().take(self.limit).collect();
        let next_start_after = page.last().map(|(key, _)| key.to_vec());
        let mut records = Vec::with_capacity(page.len());
        let mut malformed = Vec::new();
        for (key, value) in page {
            match postcard::from_bytes::<DocumentOutboxRecord>(&value) {
                Ok(record) => records.push((key.to_vec(), record)),
                Err(_) => malformed.push(key.to_vec()),
            }
        }
        let deletes = outbox_deletes(rows, &malformed)?;
        for key in &malformed {
            warn!(key = ?key, "Deleting malformed document sync outbox record");
        }
        let batch = OutboxReadBatch {
            records,
            has_more,
            next_start_after,
        };
        Ok(Planned::changes(batch, deletes, Vec::new()))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::None
    }
}

/// Deletes outbox records with the revocation index row each admin record owns.
#[derive(Clone, Debug, PartialEq)]
pub struct DeleteOutboxPlan {
    pub keys: Vec<Vec<u8>>,
}

impl RowPlan for DeleteOutboxPlan {
    type Output = ();
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<()>> {
        let deletes = outbox_deletes(rows, &self.keys)?;
        Ok(Planned::changes((), deletes, Vec::new()))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::None
    }
}

fn outbox_deletes(rows: &mut PlanRows, keys: &[Vec<u8>]) -> Step<Vec<(KeySpace, Key)>> {
    let mut deletes = Vec::with_capacity(keys.len());
    let mut waiting = false;
    for key in keys {
        match index_row(rows, key) {
            Ok(index_key) => {
                deletes.push((
                    SYNC_OUTBOX_KEYSPACE.to_string(),
                    ByteView::from(key.clone()),
                ));
                deletes.extend(
                    index_key.map(|index_key| (OUTBOX_INDEX_KEYSPACE.to_string(), index_key)),
                );
            }
            Err(PlanStop::NeedsRows) => waiting = true,
            Err(stop) => return Err(stop),
        }
    }
    if waiting {
        return Err(NeedsRows.into());
    }
    Ok(deletes)
}

/// The existing index row of an admin record; a malformed value can clear only its own row.
fn index_row(rows: &mut PlanRows, key: &[u8]) -> Step<Option<Key>> {
    if !key.starts_with(ADMIN_OUTBOX_PREFIX) {
        return Ok(None);
    }
    let Some(value) = rows.read(SYNC_OUTBOX_KEYSPACE, key.to_vec())? else {
        return Ok(None);
    };
    let index_key = match postcard::from_bytes::<DocumentOutboxRecord>(&value) {
        Ok(record) => revocation_index_key(&record),
        Err(_) => Some(ByteView::from(key.to_vec())),
    };
    let Some(index_key) = index_key else {
        return Ok(None);
    };
    Ok(rows
        .read(OUTBOX_INDEX_KEYSPACE, index_key.clone())?
        .map(|_| index_key))
}

/// The last key of every outbox stream that holds a record, with the stream's prefix.
#[derive(Debug, Default, PartialEq)]
pub struct OutboxTailsOperation {
    position: usize,
    tails: Vec<(Vec<u8>, Vec<u8>)>,
}

impl OutboxTailsOperation {
    pub fn new() -> Self {
        Self::default()
    }

    fn advance(&mut self) -> Next<Vec<(Vec<u8>, Vec<u8>)>> {
        let Some(prefix) = outbox_stream_prefixes().get(self.position).copied() else {
            return Next::Done(std::mem::take(&mut self.tails));
        };
        let last = StorageEffect::Last {
            key_space: SYNC_OUTBOX_KEYSPACE.to_string(),
            prefix: Some(prefix.to_vec().into()),
            txn_id: None,
        };
        Next::Call(IoCall::Effect(Box::new(Effect::Storage(last))))
    }
}

impl Operation for OutboxTailsOperation {
    type Output = Vec<(Vec<u8>, Vec<u8>)>;
    type Error = ApplyError;

    fn first(&mut self) -> Result<Next<Self::Output>, ApplyError> {
        Ok(self.advance())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ApplyError> {
        let values = match event(result)? {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values,
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(ApplyError::Storage(error));
            }
            other => {
                return Err(IoError::Unexpected {
                    expected: "StorageEvent::IterResult",
                    got: other.label(),
                }
                .into());
            }
        };
        if let (Some(prefix), Some((key, _))) = (
            outbox_stream_prefixes().get(self.position),
            values.into_iter().next(),
        ) {
            self.tails.push((prefix.to_vec(), key.to_vec()));
        }
        self.position += 1;
        Ok(self.advance())
    }
}

/// Arms the outbox drain when any record waits, unless it is armed or running already.
#[derive(Debug, Default, PartialEq)]
pub struct OutboxTimerOperation {
    arming: bool,
}

impl OutboxTimerOperation {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Operation for OutboxTimerOperation {
    type Output = ();
    type Error = ApplyError;

    fn first(&mut self) -> Result<Next<()>, ApplyError> {
        Ok(Next::Call(IoCall::Page {
            space: SYNC_OUTBOX_KEYSPACE.to_string(),
            prefix: ByteView::from(Vec::new()),
            after: None,
            limit: 1,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ApplyError> {
        if self.arming {
            return match event(result)? {
                Event::Task(TaskEvent::Error { message, .. }) => Err(ApplyError::Failed(message)),
                _ => Ok(Next::Done(())),
            };
        }
        let (rows, _) = page(result)?;
        if rows.is_empty() {
            return Ok(Next::Done(()));
        }
        self.arming = true;
        let arm = Effect::Task(TaskEffect::ScheduleIdleTimer {
            key: TaskKey::DrainSyncOutbox,
            after: Duration::ZERO,
        });
        Ok(Next::Call(IoCall::Effect(Box::new(arm))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use crate::sync::outbox_record::{
        OUTBOX_DRAIN_SIZE, admin_outbox_prefix, outbox_key, outbox_prefix, revocation_index_entry,
    };
    use crate::sync::outbox_record::{
        new_identified_record, new_outbox_record, write_outbox_effect,
    };
    use crate::testing::{read_outbox_record, read_outbox_records, test_context};
    use aruna_core::NodeId;
    use aruna_core::UserId;
    use aruna_core::admin_documents::{
        AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
    };
    use aruna_core::document::{DocumentChange, DocumentChangeKind, DocumentSyncRevision};
    use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
    use aruna_core::handle::Handle;
    use aruna_core::row_plan::PlanOperation;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::types::Value;
    use aruna_storage::{FjallStorage, StorageHandle};
    use tempfile::tempdir;
    use ulid::Ulid;

    async fn delete_outbox_records(
        storage: &StorageHandle,
        keys: Vec<Vec<u8>>,
    ) -> Result<(), ApplyError> {
        let plan = DeleteOutboxPlan { keys };
        drive(PlanOperation::new(plan), &test_context(storage.clone())).await
    }

    fn node(seed: u8) -> NodeId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        iroh::SecretKey::from_bytes(&bytes).public()
    }

    fn target() -> DocumentTarget {
        DocumentTarget::RealmConfig {
            realm_id: RealmId::from_bytes([7u8; 32]),
        }
    }

    fn user_admin_event(user_id: UserId, origin_seq: u64) -> DocumentOutboxEvent {
        DocumentOutboxEvent::admin(AdminDocumentEvent {
            event_id: Ulid::from_parts(1, u128::from(origin_seq)),
            target: AdminDocumentTarget::User { user_id },
            origin_node_id: node(1),
            origin_seq,
            observed: AdminDocumentClock::default(),
            actor: Actor {
                node_id: node(1),
                user_id,
                realm_id: user_id.realm_id,
            },
            op: AdminDocumentOperation::UserNameSet {
                name: format!("user-{origin_seq}"),
            },
        })
    }

    fn revocation_event(origin_seq: u64) -> DocumentOutboxEvent {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let user_id = UserId::local(Ulid::from_parts(7, 1), realm_id);
        DocumentOutboxEvent::admin(AdminDocumentEvent {
            event_id: Ulid::from_parts(2, u128::from(origin_seq)),
            target: AdminDocumentTarget::RealmConfig { realm_id },
            origin_node_id: node(1),
            origin_seq,
            observed: AdminDocumentClock::default(),
            actor: Actor {
                node_id: node(1),
                user_id,
                realm_id,
            },
            op: AdminDocumentOperation::ConfigTokenRevoked {
                token_hash: aruna_core::auth::bearer_token_hash(&format!("token-{origin_seq}")),
                expires_at: 100,
                token_owner: user_id,
            },
        })
    }

    fn placement(shard: u32) -> aruna_core::structs::placement::record::PlacementRef {
        aruna_core::structs::placement::record::PlacementRef {
            strategy_id: Ulid::from_parts(42, 1),
            shard,
        }
    }

    fn change() -> DocumentChange {
        DocumentChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::from_parts(8, 1),
                actor: node(1),
                updated_at_ms: 9,
            },
            kind: DocumentChangeKind::Upsert,
            placement: placement(5),
        }
    }

    fn delete_change() -> DocumentChange {
        DocumentChange {
            kind: DocumentChangeKind::Delete,
            ..change()
        }
    }

    async fn write_raw_record(storage: &StorageHandle, key: Vec<u8>, value: Vec<u8>) {
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: SYNC_OUTBOX_KEYSPACE.to_string(),
                key: ByteView::from(key),
                value: ByteView::from(value),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected outbox write event: {other:?}"),
        }
    }

    async fn write_index(storage: &StorageHandle, key: Vec<u8>) {
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: OUTBOX_INDEX_KEYSPACE.to_string(),
                key: ByteView::from(key),
                value: Value::from(vec![1u8]),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected index write event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn malformed_record_deleted() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let corrupt_key = b"000-corrupt-outbox".to_vec();
        write_raw_record(&storage, corrupt_key.clone(), vec![1, 2, 3]).await;

        let batch = read_outbox_records(&storage, &[], None, OUTBOX_DRAIN_SIZE)
            .await
            .expect("outbox read succeeds");

        assert!(batch.records.is_empty());
        assert!(!batch.has_more);
        assert_eq!(batch.next_start_after, Some(corrupt_key.clone()));
        assert_eq!(
            read_outbox_record(&storage, &corrupt_key)
                .await
                .expect("corrupt record was deleted"),
            None
        );
    }

    #[tokio::test]
    async fn malformed_before_valid() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let corrupt_key = b"000-corrupt-outbox".to_vec();
        let valid = new_outbox_record(
            node(1),
            target(),
            vec![node(2)],
            DocumentOutboxEvent::Upsert {
                bytes: vec![4, 5],
                change: change(),
            },
            aruna_core::structs::placement::record::PlacementRef::NIL,
            false,
        );
        write_raw_record(&storage, corrupt_key.clone(), vec![1, 2, 3]).await;
        match storage
            .send_effect(write_outbox_effect(&valid).expect("valid outbox effect"))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected valid outbox write event: {other:?}"),
        }

        let batch = read_outbox_records(&storage, &[], None, 1)
            .await
            .expect("outbox read succeeds");

        assert!(batch.records.is_empty());
        assert!(batch.has_more);
        assert_eq!(batch.next_start_after, Some(corrupt_key.clone()));
        let next = read_outbox_records(&storage, &[], batch.next_start_after, 1)
            .await
            .expect("outbox read after malformed page succeeds");
        assert_eq!(next.records, vec![(outbox_key(&valid).to_vec(), valid)]);
        assert!(!next.has_more);
        assert_eq!(
            read_outbox_record(&storage, &corrupt_key)
                .await
                .expect("corrupt record was deleted"),
            None
        );
    }

    #[test]
    fn prefix_kind_scoped() {
        let upsert = DocumentOutboxEvent::Upsert {
            bytes: vec![1, 2],
            change: change(),
        };
        let delete = DocumentOutboxEvent::Delete {
            change: delete_change(),
        };

        assert_eq!(outbox_prefix(&upsert), outbox_prefix(&upsert));
        assert_ne!(outbox_prefix(&upsert), outbox_prefix(&delete));
    }

    #[test]
    fn record_roundtrip_deduplicates() {
        let peer = node(3);
        let record = new_outbox_record(
            node(1),
            target(),
            vec![peer, peer],
            DocumentOutboxEvent::Upsert {
                bytes: vec![4, 5],
                change: change(),
            },
            aruna_core::structs::placement::record::PlacementRef::NIL,
            false,
        );
        let bytes = postcard::to_allocvec(&record).expect("record serializes");
        let decoded: DocumentOutboxRecord = postcard::from_bytes(&bytes).expect("record decodes");

        assert_eq!(decoded, record);
        assert_eq!(decoded.peers, vec![peer]);
    }

    #[test]
    fn upsert_roundtrip_revision() {
        let record = new_outbox_record(
            node(1),
            target(),
            vec![node(3)],
            DocumentOutboxEvent::Upsert {
                bytes: vec![4, 5],
                change: change(),
            },
            aruna_core::structs::placement::record::PlacementRef::NIL,
            true,
        );
        let bytes = postcard::to_allocvec(&record).expect("record serializes");
        let decoded: DocumentOutboxRecord = postcard::from_bytes(&bytes).expect("record decodes");

        assert_eq!(decoded, record);
        assert!(decoded.allow_genesis);
        // Upsert mirrors the envelope change's ref, never the admin fallback.
        assert_eq!(decoded.placement, change().placement);
        assert_ne!(
            decoded.placement,
            aruna_core::structs::placement::record::PlacementRef::NIL
        );
    }

    #[test]
    fn delete_roundtrip_revision() {
        let record = new_outbox_record(
            node(1),
            target(),
            vec![node(3)],
            DocumentOutboxEvent::Delete {
                change: delete_change(),
            },
            aruna_core::structs::placement::record::PlacementRef::NIL,
            false,
        );
        let bytes = postcard::to_allocvec(&record).expect("record serializes");
        let decoded: DocumentOutboxRecord = postcard::from_bytes(&bytes).expect("record decodes");

        assert_eq!(decoded, record);
        assert_eq!(decoded.placement, delete_change().placement);
    }

    #[test]
    fn kind_keys_unique() {
        let event = DocumentOutboxEvent::Upsert {
            bytes: vec![1],
            change: change(),
        };
        let left = new_outbox_record(
            node(1),
            target(),
            vec![node(2)],
            event.clone(),
            aruna_core::structs::placement::record::PlacementRef::NIL,
            false,
        );
        let right = new_outbox_record(
            node(1),
            target(),
            vec![node(2)],
            event,
            aruna_core::structs::placement::record::PlacementRef::NIL,
            false,
        );
        let prefix = outbox_prefix(&left.event);

        assert_ne!(outbox_key(&left), outbox_key(&right));
        assert!(outbox_key(&left).starts_with(prefix.as_ref()));
        assert!(outbox_key(&right).starts_with(prefix.as_ref()));
    }

    #[test]
    fn keys_follow_id() {
        let event = DocumentOutboxEvent::Upsert {
            bytes: vec![1],
            change: change(),
        };
        let mut older = new_outbox_record(
            node(1),
            target(),
            vec![node(2)],
            event.clone(),
            aruna_core::structs::placement::record::PlacementRef::NIL,
            false,
        );
        older.outbox_id = Ulid::from_parts(1, 0);
        let mut newer = new_outbox_record(
            node(1),
            DocumentTarget::RealmConfig {
                realm_id: RealmId::from_bytes([9u8; 32]),
            },
            vec![node(2)],
            event,
            aruna_core::structs::placement::record::PlacementRef::NIL,
            false,
        );
        newer.outbox_id = Ulid::from_parts(2, 0);

        assert!(outbox_key(&older) < outbox_key(&newer));
    }

    #[test]
    fn admin_keys_ordered() {
        let user_id = UserId::local(Ulid::from_parts(7, 1), RealmId::from_bytes([3; 32]));
        let target = DocumentTarget::User { user_id };
        let mut earlier = new_outbox_record(
            node(1),
            target.clone(),
            Vec::new(),
            user_admin_event(user_id, 1),
            placement(9),
            false,
        );
        earlier.outbox_id = Ulid::from_parts(2, 2);
        let mut later = new_outbox_record(
            node(1),
            target,
            Vec::new(),
            user_admin_event(user_id, 2),
            placement(9),
            false,
        );
        later.outbox_id = Ulid::from_parts(1, 1);

        assert!(outbox_key(&earlier) < outbox_key(&later));
        // AdminOperation records take the supplied ref (no envelope change).
        assert_eq!(earlier.placement, placement(9));
    }

    #[test]
    fn admin_prefix_scopes() {
        let user_id = UserId::local(Ulid::from_parts(7, 1), RealmId::from_bytes([3; 32]));
        let record = new_outbox_record(
            node(1),
            DocumentTarget::User { user_id },
            Vec::new(),
            user_admin_event(user_id, 1),
            placement(9),
            false,
        );
        let key = outbox_key(&record);

        assert!(key.starts_with(admin_outbox_prefix(node(1)).as_ref()));
        assert!(!key.starts_with(admin_outbox_prefix(node(2)).as_ref()));
    }

    #[test]
    fn index_uses_key() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let record = new_outbox_record(
            node(1),
            DocumentTarget::RealmConfig { realm_id },
            Vec::new(),
            revocation_event(1),
            placement(9),
            false,
        );
        let (keyspace, key, value) = revocation_index_entry(&record);

        assert_eq!(keyspace, OUTBOX_INDEX_KEYSPACE);
        assert_eq!(key, outbox_key(&record));
        assert_eq!(value, Value::from(vec![1u8]));
    }

    #[tokio::test]
    async fn delete_clears_index() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let record = new_outbox_record(
            node(1),
            DocumentTarget::RealmConfig { realm_id },
            Vec::new(),
            revocation_event(1),
            placement(9),
            false,
        );
        let record_key = outbox_key(&record).to_vec();
        let (_, index_key, _) = revocation_index_entry(&record);
        write_raw_record(
            &storage,
            record_key.clone(),
            postcard::to_allocvec(&record).expect("record serializes"),
        )
        .await;
        write_index(&storage, index_key.to_vec()).await;

        delete_outbox_records(&storage, vec![record_key.clone()])
            .await
            .expect("outbox delete succeeds");
        assert!(
            read_outbox_record(&storage, &record_key)
                .await
                .expect("outbox read succeeds")
                .is_none()
        );
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: OUTBOX_INDEX_KEYSPACE.to_string(),
                key: index_key,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {}
            other => panic!("unexpected index read event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn keeps_generic_index() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let user_id = UserId::local(Ulid::from_parts(7, 1), RealmId::from_bytes([3; 32]));
        let metadata = new_outbox_record(
            node(1),
            target(),
            vec![node(2)],
            DocumentOutboxEvent::Upsert {
                bytes: vec![4, 5],
                change: change(),
            },
            aruna_core::structs::placement::record::PlacementRef::NIL,
            false,
        );
        let admin = new_outbox_record(
            node(1),
            DocumentTarget::User { user_id },
            Vec::new(),
            user_admin_event(user_id, 1),
            placement(9),
            false,
        );
        let metadata_key = outbox_key(&metadata).to_vec();
        let admin_key = outbox_key(&admin).to_vec();
        write_raw_record(
            &storage,
            metadata_key.clone(),
            postcard::to_allocvec(&metadata).expect("metadata serializes"),
        )
        .await;
        write_raw_record(
            &storage,
            admin_key.clone(),
            postcard::to_allocvec(&admin).expect("admin serializes"),
        )
        .await;
        write_index(&storage, metadata_key.clone()).await;
        write_index(&storage, admin_key.clone()).await;

        delete_outbox_records(&storage, vec![metadata_key.clone(), admin_key.clone()])
            .await
            .expect("outbox delete succeeds");

        for key in [metadata_key, admin_key] {
            match storage
                .send_storage_effect(StorageEffect::Read {
                    key_space: OUTBOX_INDEX_KEYSPACE.to_string(),
                    key: ByteView::from(key),
                    txn_id: None,
                })
                .await
            {
                Event::Storage(StorageEvent::ReadResult { value: Some(_), .. }) => {}
                other => panic!("unexpected index read event: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn clears_malformed_index() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let record = new_outbox_record(
            node(1),
            DocumentTarget::RealmConfig { realm_id },
            Vec::new(),
            revocation_event(1),
            placement(9),
            false,
        );
        let record_key = outbox_key(&record).to_vec();
        write_raw_record(&storage, record_key.clone(), vec![1, 2, 3]).await;
        write_index(&storage, record_key.clone()).await;

        delete_outbox_records(&storage, vec![record_key.clone()])
            .await
            .expect("outbox delete succeeds");
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: OUTBOX_INDEX_KEYSPACE.to_string(),
                key: ByteView::from(record_key),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {}
            other => panic!("unexpected index read event: {other:?}"),
        }
    }

    #[test]
    fn key_ignores_placement() {
        let user_id = UserId::local(Ulid::from_parts(7, 1), RealmId::from_bytes([3; 32]));
        let event = user_admin_event(user_id, 1);
        let outbox_id = Ulid::from_parts(5, 5);
        let target = DocumentTarget::User { user_id };
        let nil = new_identified_record(
            outbox_id,
            node(1),
            target.clone(),
            Vec::new(),
            event.clone(),
            aruna_core::structs::placement::record::PlacementRef::NIL,
            false,
        );
        let sharded = new_identified_record(
            outbox_id,
            node(1),
            target,
            Vec::new(),
            event,
            placement(17),
            false,
        );
        // The FIFO key layout must not depend on the placement field.
        assert_ne!(nil.placement, sharded.placement);
        assert_eq!(outbox_key(&nil), outbox_key(&sharded));
    }

    fn stored_rows() -> Vec<(Key, aruna_core::types::Value)> {
        vec![(ByteView::from(vec![1]), ByteView::from(vec![2]))]
    }

    #[test]
    fn tails_cover_streams() {
        let mut operation = OutboxTailsOperation::new();
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Effect(_)))
        ));
        let empty = || {
            IoResult::Event(Box::new(Event::Storage(StorageEvent::IterResult {
                values: Vec::new(),
                next_start_after: None,
            })))
        };
        let found = IoResult::Event(Box::new(Event::Storage(StorageEvent::IterResult {
            values: stored_rows(),
            next_start_after: None,
        })));
        assert!(matches!(
            operation.next(found),
            Ok(Next::Call(IoCall::Effect(_)))
        ));
        assert!(matches!(
            operation.next(empty()),
            Ok(Next::Call(IoCall::Effect(_)))
        ));
        let prefix = outbox_stream_prefixes()[0].to_vec();
        assert_eq!(
            operation.next(empty()),
            Ok(Next::Done(vec![(prefix, vec![1])]))
        );
    }

    #[test]
    fn restore_arms_timer() {
        let mut operation = OutboxTimerOperation::new();
        operation.first().unwrap();
        let page = IoResult::Page {
            rows: stored_rows(),
            next: None,
        };
        let arm = Effect::Task(TaskEffect::ScheduleIdleTimer {
            key: TaskKey::DrainSyncOutbox,
            after: Duration::ZERO,
        });
        assert_eq!(
            operation.next(page),
            Ok(Next::Call(IoCall::Effect(Box::new(arm))))
        );
        let armed = Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainSyncOutbox,
            after: Duration::ZERO,
        });
        assert_eq!(
            operation.next(IoResult::Event(Box::new(armed))),
            Ok(Next::Done(()))
        );
    }

    #[test]
    fn restore_skips_empty() {
        let mut operation = OutboxTimerOperation::new();
        operation.first().unwrap();
        let page = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(operation.next(page), Ok(Next::Done(())));
    }
}
