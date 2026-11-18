//! Counts a realm's live metadata documents and its groups.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::keyspaces::GROUP_KEYSPACE;
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{PrefixPages, decode};
use aruna_core::structs::identity::group::Group;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use thiserror::Error;

use crate::metadata::live_records::{LiveRecordsError, LiveRecordsOperation};

const GROUP_PAGE_SIZE: usize = 1_000;

#[derive(Debug, Error, PartialEq)]
pub enum RealmCountError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error(transparent)]
    Records(#[from] LiveRecordsError),
}

/// Realm-wide count of live metadata documents, unfiltered by readability and excluding
/// lifecycle-deleted ones. `None` without a metadata subsystem. Only document volume is
/// disclosed, never a per-caller glob evaluation.
#[derive(Debug, PartialEq)]
pub struct RealmDocumentsOperation {
    realm_id: RealmId,
    filtering: bool,
}

impl RealmDocumentsOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            realm_id,
            filtering: false,
        }
    }
}

impl Operation for RealmDocumentsOperation {
    type Output = Option<u64>;
    type Error = RealmCountError;

    fn first(&mut self) -> Result<Next<Option<u64>>, RealmCountError> {
        let cached = Effect::Metadata(MetadataEffect::CachedRecords);
        Ok(Next::Call(IoCall::from(cached)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<u64>>, RealmCountError> {
        if self.filtering {
            let live = result.child::<Vec<MetadataRegistryRecord>, LiveRecordsError>()??;
            let in_realm = live.iter().filter(|row| row.realm_id == self.realm_id);
            return Ok(Next::Done(Some(in_realm.count() as u64)));
        }
        match result.event()? {
            Event::Metadata(MetadataEvent::CachedRecords { records }) => {
                self.filtering = true;
                let live = LiveRecordsOperation::new(records.to_vec(), None);
                Ok(Next::Call(IoCall::child(live)))
            }
            Event::Metadata(MetadataEvent::Error {
                error: MetadataError::HandleMissing,
                ..
            }) => Ok(Next::Done(None)),
            Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error.into()),
            other => Err(IoError::Unexpected {
                expected: "MetadataEvent::CachedRecords",
                got: other.label(),
            }
            .into()),
        }
    }
}

/// Number of stored groups belonging to one realm, from bounded pages of one snapshot.
#[derive(Debug, PartialEq)]
pub struct RealmGroupsOperation {
    realm_id: RealmId,
    pages: PrefixPages,
    count: u64,
}

impl RealmGroupsOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            realm_id,
            pages: PrefixPages::new(GROUP_KEYSPACE, Vec::new(), GROUP_PAGE_SIZE),
            count: 0,
        }
    }
}

impl Operation for RealmGroupsOperation {
    type Output = u64;
    type Error = RealmCountError;

    fn first(&mut self) -> Result<Next<u64>, RealmCountError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<u64>, RealmCountError> {
        for (_, value) in self.pages.take(result)? {
            if decode::<Group>(&value)?.realm_id == self.realm_id {
                self.count += 1;
            }
        }
        if self.pages.done() {
            return Ok(Next::Done(self.count));
        }
        Ok(Next::Call(self.pages.call()))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::rows::encode;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::types::{Key, Value};
    use ulid::Ulid;

    use super::*;

    fn realm() -> RealmId {
        RealmId::from_bytes([7; 32])
    }

    fn other_realm() -> RealmId {
        RealmId::from_bytes([9; 32])
    }

    fn record(realm_id: RealmId) -> MetadataRegistryRecord {
        let document_id = Ulid::from_parts(1, 1);
        let group_id = Ulid::from_parts(1, 2);
        MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: "datasets/one".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                "datasets/one",
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::nil(),
            last_event_id: Ulid::nil(),
        }
    }

    fn group_row(realm_id: RealmId) -> (Key, Value) {
        let group = Group {
            display_name: "group".to_string(),
            group_id: Ulid::from_parts(1, 3),
            realm_id,
            roles: Default::default(),
            owner: UserId::nil(realm_id),
        };
        (Key::from(vec![1]), encode(&group).unwrap())
    }

    fn metadata(event: MetadataEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Metadata(event)))
    }

    #[test]
    fn documents_count_realm() {
        let mut count = RealmDocumentsOperation::new(realm());
        let cached = IoCall::from(Effect::Metadata(MetadataEffect::CachedRecords));
        assert_eq!(count.first(), Ok(Next::Call(cached)));
        let records = vec![record(realm()), record(other_realm())];
        let answer = count.next(metadata(MetadataEvent::CachedRecords {
            records: Arc::new(records.clone()),
        }));
        let live = LiveRecordsOperation::new(records.clone(), None);
        assert_eq!(answer, Ok(Next::Call(IoCall::child(live))));
        let live: Result<_, LiveRecordsError> = Ok(records);
        let answer = count.next(IoResult::Child(ChildOutcome::new(live)));
        assert_eq!(answer, Ok(Next::Done(Some(1))));
    }

    #[test]
    fn missing_handle_unknown() {
        // An absent count must stay distinguishable from zero documents.
        let mut count = RealmDocumentsOperation::new(realm());
        let answer = count.next(metadata(MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::HandleMissing,
        }));
        assert_eq!(answer, Ok(Next::Done(None)));
    }

    #[test]
    fn backend_failure_fails() {
        let mut count = RealmDocumentsOperation::new(realm());
        let answer = count.next(metadata(MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::ChannelClosed,
        }));
        let expected = RealmCountError::Metadata(MetadataError::ChannelClosed);
        assert_eq!(answer, Err(expected));
    }

    #[test]
    fn groups_span_pages() {
        let mut count = RealmGroupsOperation::new(realm());
        assert_eq!(count.transaction(), TransactionMode::ReadSnapshot);
        let first = IoCall::Page {
            space: GROUP_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: GROUP_PAGE_SIZE,
        };
        assert_eq!(count.first(), Ok(Next::Call(first)));
        let next = Key::from(vec![1]);
        let answer = count.next(IoResult::Page {
            rows: vec![group_row(realm()), group_row(other_realm())],
            next: Some(next.clone()),
        });
        let second = IoCall::Page {
            space: GROUP_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: Some(next),
            limit: GROUP_PAGE_SIZE,
        };
        assert_eq!(answer, Ok(Next::Call(second)));
        let answer = count.next(IoResult::Page {
            rows: vec![group_row(realm())],
            next: None,
        });
        assert_eq!(answer, Ok(Next::Done(2)));
    }

    #[test]
    fn broken_group_fails() {
        let mut count = RealmGroupsOperation::new(realm());
        let answer = count.next(IoResult::Page {
            rows: vec![(Key::from(vec![1]), Value::from(b"not a group".to_vec()))],
            next: None,
        });
        assert!(matches!(
            answer,
            Err(RealmCountError::Io(IoError::Codec(_)))
        ));
    }
}
