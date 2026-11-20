//! Binds a drained record's publication to what the drain saw: its topic's lineage and the row
//! it read, so a later genesis or a replacement row is never taken for that publication.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::calls::{IoCall, IoError, IoResult};
use aruna_core::document::DocumentOutboxRecord;
use aruna_core::document_apply::{ApplyError, Step};
use aruna_core::effects::{Effect, EffectGroup, GroupForm, NetEffect, ShardNetEffect};
use aruna_core::events::{Event, GroupEvent, GroupResult, NetEvent, ShardNetEvent};
use aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE;
use aruna_core::row_plan::{PlanRows, Planned, RowPlan};
use byteview::ByteView;
use irokle::{ActorClock, TopicId};

use super::defer::DrainRecord;
use crate::sync::document_outbox::DeleteOutboxPlan;

/// Lineage queries one call runs at once.
const LINEAGE_QUERIES: usize = 16;

/// Each topic's lineage, `None` where it could not be learned.
pub type Lineages = BTreeMap<TopicId, Option<[u8; 32]>>;

/// The records a kept publication still stands for: those this process published on the lineage
/// their topic still has. Any other publishes again, as after a genesis tie-break.
pub fn standing(
    published: &BTreeMap<Vec<u8>, [u8; 32]>,
    records: &[DrainRecord],
    lineages: &Lineages,
) -> BTreeSet<Vec<u8>> {
    let holds = |(key, _, topic): &&DrainRecord| {
        let current = lineages.get(topic).copied().flatten();
        published.get(key).is_some_and(|lineage| current == Some(*lineage))
    };
    records.iter().filter(holds).map(|(key, ..)| key.clone()).collect()
}

/// The digest of the stored row of `record`, which the outbox writes as its postcard encoding.
pub fn row_digest(record: &DocumentOutboxRecord) -> Option<[u8; 32]> {
    let bytes = postcard::to_allocvec(record).ok()?;
    Some(*blake3::hash(&bytes).as_bytes())
}

/// The call that asks for the lineage root of each topic: the hash of its genesis alone, which
/// a genesis tie-break changes and later writes do not. `None` for no topics.
pub fn lineage_call(topics: &[TopicId]) -> Option<IoCall> {
    if topics.is_empty() {
        return None;
    }
    let root = |topic| {
        let frontier = ActorClock::default();
        let root = ShardNetEffect::FrontierRoot { topic, frontier };
        Effect::Net(NetEffect::Shard(Box::new(root)))
    };
    Some(IoCall::Group(EffectGroup {
        members: topics.iter().copied().map(root).collect(),
        limit: LINEAGE_QUERIES,
        form: GroupForm::All { deadline: None },
    }))
}

/// The lineages the answer to [`lineage_call`] for `topics` holds.
pub fn lineages(topics: &[TopicId], result: IoResult) -> Result<Lineages, IoError> {
    let IoResult::Group(GroupEvent::Finished { results }) = result else {
        let (expected, got) = ("the lineage roots", "another answer");
        return Err(IoError::Unexpected { expected, got });
    };
    let root = |result: GroupResult| match result {
        GroupResult::Done(event) => match *event {
            Event::Net(NetEvent::Shard(answer)) => match *answer {
                ShardNetEvent::Root(Ok(root)) => Some(root),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    };
    Ok(topics.iter().copied().zip(results.into_iter().map(root)).collect())
}

/// Deletes outbox rows only while each still holds the row the drain read, by its digest: a row
/// a replacement rewrote since stays for the next drain. Answers the keys it deleted.
#[derive(Debug, PartialEq)]
pub struct DeleteUnchanged {
    pub rows: Vec<(Vec<u8>, Option<[u8; 32]>)>,
}

impl RowPlan for DeleteUnchanged {
    type Output = Vec<Vec<u8>>;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<Vec<Vec<u8>>>> {
        for (key, digest) in &self.rows {
            if digest.is_some() {
                rows.want(SYNC_OUTBOX_KEYSPACE, ByteView::from(key.clone()));
            }
        }
        let mut keys = Vec::with_capacity(self.rows.len());
        for (key, digest) in &self.rows {
            let Some(digest) = digest else {
                keys.push(key.clone());
                continue;
            };
            let row = rows.read(SYNC_OUTBOX_KEYSPACE, ByteView::from(key.clone()))?;
            if row.is_some_and(|value| blake3::hash(&value).as_bytes() == digest) {
                keys.push(key.clone());
            }
        }
        let planned = DeleteOutboxPlan { keys: keys.clone() }.plan(rows)?;
        Ok(Planned::changes(keys, planned.deletes, planned.writes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::document::{
        DocumentChange, DocumentChangeKind, DocumentOutboxEvent, DocumentSyncRevision,
        DocumentTarget,
    };
    use aruna_core::structs::placement::record::PlacementRef;
    use ulid::Ulid;

    use crate::sync::outbox::record::new_identified_record;

    fn record(key: &[u8], topic: &[u8]) -> DrainRecord {
        let actor = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let event_id = Ulid::from_parts(1, 1);
        let current = DocumentSyncRevision {
            generation: 1,
            event_id,
            actor,
            updated_at_ms: 1,
        };
        let change = DocumentChange {
            base: None,
            current,
            kind: DocumentChangeKind::Delete,
            placement: PlacementRef::NIL,
        };
        let target = DocumentTarget::Group {
            group_id: Ulid::from_parts(2, 2),
        };
        let event = DocumentOutboxEvent::Delete { change };
        let (peers, placement) = (Vec::new(), PlacementRef::NIL);
        let record =
            new_identified_record(event_id, actor, target, peers, event, placement, false);
        (key.to_vec(), record, TopicId::hash(topic))
    }

    // A kept publication stands only while its topic keeps the lineage it was published on; on
    // another genesis, or without a known lineage, the record publishes again.
    #[test]
    fn moved_lineage_republishes() {
        let records = [
            record(b"kept", b"one"),
            record(b"moved", b"two"),
            record(b"unknown", b"three"),
        ];
        let published = BTreeMap::from([
            (b"kept".to_vec(), [1; 32]),
            (b"moved".to_vec(), [1; 32]),
            (b"unknown".to_vec(), [1; 32]),
        ]);
        let lineages = Lineages::from([
            (TopicId::hash(b"one"), Some([1; 32])),
            (TopicId::hash(b"two"), Some([2; 32])),
            (TopicId::hash(b"three"), None),
        ]);
        let standing = standing(&published, &records, &lineages);
        assert_eq!(standing, BTreeSet::from([b"kept".to_vec()]));
    }

    // A row is deleted only while it holds what the drain read; a replaced row stays.
    #[test]
    fn replaced_row_kept() {
        let (read, replaced) = (b"read".to_vec(), b"replaced".to_vec());
        let digest = Some(*blake3::hash(b"read").as_bytes());
        let row = |key: &[u8], value: &[u8]| {
            let at = (SYNC_OUTBOX_KEYSPACE.to_string(), ByteView::from(key.to_vec()));
            (at, Some(ByteView::from(value.to_vec())))
        };
        let known = [row(b"upsert/one", &read), row(b"upsert/two", &replaced)];
        let mut rows = PlanRows::with_rows(known, 0);
        let plan = DeleteUnchanged {
            rows: vec![
                (b"upsert/one".to_vec(), digest),
                (b"upsert/two".to_vec(), digest),
            ],
        };
        let Ok(planned) = plan.plan(&mut rows) else {
            panic!("the known rows decide the plan");
        };
        assert_eq!(planned.output, vec![b"upsert/one".to_vec()]);
        let deleted: Vec<_> = planned.deletes.iter().map(|(_, key)| key.to_vec()).collect();
        assert_eq!(deleted, vec![b"upsert/one".to_vec()]);
    }
}
