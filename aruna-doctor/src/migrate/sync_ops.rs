//! Refuses the migration while the document sync store holds ops this node has not applied,
//! admitted or still pending, that this release cannot read, as old node removals or Git records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::info_topics::cursor_key;
use super::{git_record, session_refs};
use crate::explorer::ExplorerError;
use aruna_core::document::{DocumentEvent, DocumentTarget};
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use irokle::{ActorClock, ActorId, Event, FjallStorage, Op, OpId, Storage, TopicId, TopicPayload};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// The applied cursor the node keeps per document topic.
#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
struct AppliedCursor {
    lineage: OpId,
    clock: ActorClock,
    marks: BTreeMap<ActorId, OpId>,
}

/// Ops of the pending pool one page reads.
const PENDING_PAGE: usize = 256;

/// The document sync ops found that this release cannot read.
#[derive(Default)]
struct Unread {
    count: usize,
    first: Option<TopicId>,
}

impl Unread {
    fn check(&mut self, op: &Op) {
        if let TopicPayload::Event(envelope) = &op.signed.body.payload
            && envelope.type_id == DocumentEvent::TYPE_ID
            && !readable(&envelope.payload)
        {
            self.count += 1;
            self.first.get_or_insert(op.signed.body.topic_id);
        }
    }
}

/// Fails with recovery steps when an op this node has not applied cannot be read by this
/// release: one past the applied cursor of a document topic, or one still waiting in the
/// pending pool. The node would drop it, so its change would never apply.
pub(super) fn unread_ops(
    db: &OptimisticTxDatabase,
    sync_path: &Path,
    applied: &OptimisticTxKeyspace,
) -> Result<(), ExplorerError> {
    if !sync_path.exists() {
        return Ok(());
    }
    let storage = FjallStorage::open(sync_path)?;
    let read = db.read_tx();
    let mut unread = Unread::default();
    let topics = storage.list_topics()?;
    for topic in &topics {
        if topic.event_type_id != DocumentEvent::TYPE_ID {
            continue;
        }
        let stored = read.get(applied, cursor_key(&topic.topic_id))?;
        let cursor = applied_clock(&storage, &topic.topic_id, topic.genesis, stored.as_deref())?;
        for (actor_id, tip) in storage.actor_clock(&topic.topic_id)?.iter() {
            for actor_seq in cursor.get(actor_id) + 1..=*tip {
                let Some(op_id) = storage.actor_index(&topic.topic_id, actor_id, actor_seq)? else {
                    continue;
                };
                if let Some(op) = storage.get_op(&op_id)? {
                    unread.check(&op);
                }
            }
        }
    }
    let provisional = storage.provisional_topics()?.into_iter();
    let topic_ids =
        (topics.iter().map(|topic| topic.topic_id)).chain(provisional.map(|topic| topic.topic_id));
    pending_ops(&storage, topic_ids.collect(), &mut unread)?;
    let Some(topic_id) = unread.first else {
        return Ok(());
    };
    Err(ExplorerError::Refused(format!(
        "{} document sync ops this node has not applied yet came from an earlier release \
         and this release cannot read them (first in topic {topic_id}). Start the node with the \
         earlier release until its document sync has applied them, stop it, then run the \
         migration again",
        unread.count
    )))
}

/// Checks every op of the pending pool: those waiting for a dependency, found as the waiters
/// of each topic's missing dependencies, and those ready for admission, read page by page.
fn pending_ops(
    storage: &FjallStorage,
    topic_ids: BTreeSet<TopicId>,
    unread: &mut Unread,
) -> Result<(), ExplorerError> {
    let mut seen = BTreeSet::new();
    for topic_id in &topic_ids {
        // A waiting op lists every dependency it misses, a waiting op among them too.
        for dependency in storage.pending_missing_deps(topic_id)? {
            for (_, op) in storage.pending_waiters(&dependency)? {
                if seen.insert(op.id) {
                    unread.check(&op);
                }
            }
        }
    }
    let mut after = None;
    loop {
        let page = storage.ready_pending_after(after.as_ref(), PENDING_PAGE)?;
        for (_, op) in &page {
            if seen.insert(op.id) {
                unread.check(op);
            }
        }
        match page.last() {
            Some((_, op)) if page.len() == PENDING_PAGE => after = Some(op.id),
            _ => return Ok(()),
        }
    }
}

/// The clock the node replays a topic from. A cursor of another genesis, or whose positions
/// hold other ops now, counts as none: the node then replays the topic from its start.
fn applied_clock(
    storage: &FjallStorage,
    topic_id: &TopicId,
    genesis: OpId,
    stored: Option<&[u8]>,
) -> Result<ActorClock, ExplorerError> {
    let Some(cursor) = stored
        .and_then(|value| postcard::from_bytes::<AppliedCursor>(value).ok())
        .filter(|cursor| cursor.lineage == genesis)
    else {
        return Ok(ActorClock::default());
    };
    for (actor_id, actor_seq) in cursor.clock.iter().filter(|(_, seq)| **seq > 0) {
        let current = storage.actor_index(topic_id, actor_id, *actor_seq)?;
        if current.is_none() || current.as_ref() != cursor.marks.get(actor_id) {
            return Ok(ActorClock::default());
        }
    }
    Ok(cursor.clock)
}

/// Whether `payload` reads as a current event, and a Git record in it as a current record.
fn readable(payload: &[u8]) -> bool {
    match session_refs::decode::<DocumentEvent>(payload, &[]) {
        Ok(DocumentEvent::Upsert {
            target: DocumentTarget::GitRecord { .. },
            bytes,
            ..
        }) => matches!(git_record(&bytes), Ok(None)),
        Ok(_) => true,
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::super::migrate_output;
    use super::super::tests::{strip, write};
    use super::*;
    use aruna_core::UserId;
    use aruna_core::admin_documents::{
        AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
    };
    use aruna_core::document::{DocumentChange, DocumentChangeKind, DocumentSyncRevision};
    use aruna_core::keyspaces::APPLIED_OPS_KEYSPACE;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use irokle::oplog::Oplog;
    use irokle::{Ed25519Signer, EventEnvelope, ReplicationPolicy, Signer, TopicGenesis};
    use ulid::Ulid;

    /// A current delete of the realm config document, as the sync payload carries it.
    fn current_event() -> EventEnvelope {
        let event = DocumentEvent::Delete {
            event_id: Ulid::from(1),
            target: DocumentTarget::RealmConfig {
                realm_id: RealmId([3; 32]),
            },
            change: DocumentChange {
                base: None,
                current: DocumentSyncRevision {
                    generation: 1,
                    event_id: Ulid::from(1),
                    actor: iroh::SecretKey::from_bytes(&[1; 32]).public(),
                    updated_at_ms: 1,
                },
                kind: DocumentChangeKind::Delete,
                placement: PlacementRef::NIL,
            },
        };
        EventEnvelope::encode_event(&event).expect("event encodes")
    }

    /// A realm config node removal as an earlier release published it, without cutoffs.
    fn old_removal() -> Vec<u8> {
        let realm_id = RealmId([3; 32]);
        let origin = iroh::SecretKey::from_bytes(&[1; 32]);
        let removed = iroh::SecretKey::from_bytes(&[9; 32]).public();
        let event = AdminDocumentEvent {
            event_id: Ulid::from(2),
            target: AdminDocumentTarget::RealmConfig { realm_id },
            origin_node_id: origin.public(),
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor: Actor {
                node_id: origin.public(),
                user_id: UserId::nil(realm_id),
                realm_id,
            },
            op: AdminDocumentOperation::ConfigNodeRemoved {
                node_id: removed,
                cutoffs: Vec::new(),
            },
        };
        let event = DocumentEvent::AdminOperation {
            target: DocumentTarget::RealmConfig { realm_id },
            event: Box::new(event),
            placement: PlacementRef::NIL,
            origin_signature: origin.sign(b"removal"),
        };
        let current = postcard::to_allocvec(&event).expect("event encodes");
        let removed = [
            postcard::to_allocvec(&removed).expect("id encodes"),
            vec![0],
        ]
        .concat();
        strip(&current, &removed, 1)
    }

    /// Unapplied ops stop the migration only when this release cannot read one; once the
    /// applied cursor covers it, the migration runs.
    #[test]
    fn refuses_unread_ops() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().join("db");
        let database = path.to_str().expect("path");
        let sync = path.join("document-sync");
        let topic_id = TopicId::hash(b"documents");
        let signer = Ed25519Signer::from_bytes(&[5; 32]);
        let actor_id = irokle::actor_id_for(topic_id, signer.peer_id());
        let oplog = Oplog::with_storage(FjallStorage::open(&sync).expect("sync store"));
        let genesis = TopicGenesis {
            event_type_id: DocumentEvent::TYPE_ID.to_string(),
            initial_peers: Default::default(),
            replication_policy: ReplicationPolicy::default(),
        };
        let genesis = oplog
            .create_topic_genesis(topic_id, actor_id, genesis, &signer)
            .expect("genesis");
        oplog
            .create_event_op(topic_id, actor_id, current_event(), &signer)
            .expect("current op");
        drop(oplog);
        migrate_output(database).expect("a current op does not stop the migration");

        let oplog = Oplog::with_storage(FjallStorage::open(&sync).expect("sync store"));
        let envelope = EventEnvelope::new::<DocumentEvent>(old_removal().into());
        let old = oplog
            .create_event_op(topic_id, actor_id, envelope, &signer)
            .expect("old op");
        drop(oplog);
        let error = migrate_output(database).expect_err("an unread op stops the migration");
        assert!(matches!(error, ExplorerError::Refused(_)), "{error}");

        let mut clock = ActorClock::default();
        clock.set(actor_id, old.signed.body.actor_seq);
        let cursor = AppliedCursor {
            lineage: genesis.id,
            clock,
            marks: BTreeMap::from([(actor_id, old.id)]),
        };
        let cursor = postcard::to_allocvec(&cursor).expect("cursor encodes");
        write(
            &path,
            APPLIED_OPS_KEYSPACE,
            vec![(cursor_key(&topic_id).as_slice(), cursor)],
        );
        migrate_output(database).expect("applied ops do not stop the migration");
    }

    /// Buffers an op of `topic_id` as a peer sent it: waiting for a dependency that never
    /// arrived, or with `ready` set, depending only on `ready` and waiting for admission.
    fn buffer(
        storage: &FjallStorage,
        topic_id: TopicId,
        envelope: EventEnvelope,
        (seq, ready): (u64, Option<OpId>),
    ) {
        let signer = Ed25519Signer::from_bytes(&[72; 32]);
        let author = signer.peer_id();
        let actor_id = irokle::actor_id_for(topic_id, author);
        let dependency = ready.unwrap_or_else(|| OpId::hash(seq.to_be_bytes()));
        let missing: std::collections::BTreeSet<OpId> = match ready {
            Some(_) => Default::default(),
            None => [dependency].into(),
        };
        let body = irokle::OpBody {
            topic_id,
            author,
            actor_id,
            actor_seq: seq,
            actor_prev: None,
            deps: [dependency].into(),
            generation: 9,
            payload: TopicPayload::Event(envelope),
        };
        let op = Op::sign(body, &signer).unwrap();
        let meta = irokle::storage::OpMeta {
            id: op.id,
            topic_id,
            author,
            actor_id,
            actor_seq: seq,
            actor_prev: None,
            deps: [dependency].into(),
            generation: 9,
            observed_clock: ActorClock::default(),
            ready: ready.is_some(),
            missing_deps: missing,
        };
        storage.put_pending_op(author, op, meta).unwrap();
    }

    /// An op still waiting for a dependency is checked too: once it arrives the node would
    /// apply the op, so an old one stops the migration as an admitted one does.
    #[test]
    fn refuses_unread_pending() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().join("db");
        let database = path.to_str().expect("path");
        let sync = path.join("document-sync");
        let topic_id = TopicId::hash(b"documents");
        let signer = Ed25519Signer::from_bytes(&[5; 32]);
        let actor_id = irokle::actor_id_for(topic_id, signer.peer_id());
        let storage = FjallStorage::open(&sync).expect("sync store");
        let genesis = TopicGenesis {
            event_type_id: DocumentEvent::TYPE_ID.to_string(),
            initial_peers: Default::default(),
            replication_policy: ReplicationPolicy::default(),
        };
        let genesis = Oplog::with_storage(storage.clone())
            .create_topic_genesis(topic_id, actor_id, genesis, &signer)
            .expect("genesis");
        buffer(&storage, topic_id, current_event(), (2, None));
        drop(storage);
        migrate_output(database).expect("a current waiting op does not stop the migration");

        let storage = FjallStorage::open(&sync).expect("sync store");
        let old = EventEnvelope::new::<DocumentEvent>(old_removal().into());
        buffer(&storage, topic_id, old.clone(), (3, None));
        // One more, ready for admission, is found by the paged listing of ready ops.
        buffer(&storage, topic_id, old, (4, Some(genesis.id)));
        assert_eq!(storage.ready_pending_after(None, 8).unwrap().len(), 1);
        drop(storage);
        let error = migrate_output(database).expect_err("unread pending ops stop it");
        let ExplorerError::Refused(message) = error else {
            panic!("the migration refuses: {error}");
        };
        assert!(message.starts_with("2 document sync ops"), "{message}");
    }
}
