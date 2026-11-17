//! Adds external identifiers, such as a repository DOI, to a document's persistent id mapping.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::ID_MAPPING_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::placement::fence::{admits, fence_read};
use aruna_core::rows::{done, read_row};
use aruna_core::structs::secondary_id::{IdentifierOrigin, SecondaryIdentifier};
use aruna_core::structs::{PersistentIdMapping, persistent_id_key};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::metadata::call_results::read_value;
use crate::metadata::id_mapping::{
    MappingRoute, PersistentIdError, mapping_revision, transition_entries,
};

#[derive(Clone, Debug, PartialEq)]
pub struct AddIdentifiersInput {
    pub document_id: Ulid,
    pub identifiers: Vec<SecondaryIdentifier>,
    pub route: Option<MappingRoute>,
    pub occurred_at_ms: u64,
}

#[derive(Debug, PartialEq)]
enum Step {
    Read,
    Fence,
    Write,
}

type AddTurn = Result<Next<(PersistentIdMapping, bool)>, PersistentIdError>;

/// Unions identifiers into the existing row with its index, sync and outbox entries in one
/// transaction behind the bucket's write fence. A missing row is never created here.
#[derive(Debug, PartialEq)]
pub struct AddIdentifiersOperation {
    input: AddIdentifiersInput,
    step: Step,
    merged: Option<PersistentIdMapping>,
}

impl AddIdentifiersOperation {
    pub fn new(input: AddIdentifiersInput) -> Self {
        Self {
            input,
            step: Step::Read,
            merged: None,
        }
    }

    fn mapping_read(&mut self, result: IoResult) -> AddTurn {
        let mapping = read_value(result)?
            .map(|value| PersistentIdMapping::from_bytes(value.as_ref()))
            .transpose()
            .map_err(PersistentIdError::Conversion)?
            .ok_or(PersistentIdError::IntentMissing)?;
        if mapping.is_retired() {
            return Ok(Next::Done((mapping, false)));
        }
        let mut merged = mapping.clone();
        if !merged.add_identifiers(self.input.identifiers.iter().cloned()) {
            return Ok(Next::Done((mapping, false)));
        }
        // A new revision orders after the replaced one, so holders and sync accept the row.
        let occurred_at_ms = self
            .input
            .occurred_at_ms
            .max(mapping.revision.occurred_at_ms.saturating_add(1));
        merged.revision = mapping_revision(&self.input.route, occurred_at_ms);
        let fenced = self
            .input
            .route
            .as_ref()
            .filter(|route| route.generation > 0);
        let Some(route) = fenced else {
            return self.write(merged);
        };
        self.step = Step::Fence;
        self.merged = Some(merged);
        Ok(Next::Call(IoCall::Read(vec![fence_read(
            &route.realm_id,
            &route.placement,
        )])))
    }

    fn fence_answer(&mut self, result: IoResult) -> AddTurn {
        let generation = self
            .input
            .route
            .as_ref()
            .map_or(0, |route| route.generation);
        if !admits(read_value(result)?.as_ref(), generation) {
            return Err(PersistentIdError::PlacementFenced);
        }
        let merged = self
            .merged
            .take()
            .ok_or_else(|| PersistentIdError::Unavailable("merged row missing".into()))?;
        self.write(merged)
    }

    fn write(&mut self, merged: PersistentIdMapping) -> AddTurn {
        let writes = transition_entries(&self.input.route, &merged)?;
        self.step = Step::Write;
        self.merged = Some(merged);
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for AddIdentifiersOperation {
    type Output = (PersistentIdMapping, bool);
    type Error = PersistentIdError;

    fn transaction(&self) -> bool {
        true
    }

    fn first(&mut self) -> AddTurn {
        let key = persistent_id_key(self.input.document_id);
        Ok(Next::Call(read_row(ID_MAPPING_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> AddTurn {
        match self.step {
            Step::Read => self.mapping_read(result),
            Step::Fence => self.fence_answer(result),
            Step::Write => {
                done(result)?;
                let merged = self
                    .merged
                    .take()
                    .ok_or_else(|| PersistentIdError::Unavailable("merged row missing".into()))?;
                Ok(Next::Done((merged, true)))
            }
        }
    }
}

/// One readable document that holds a looked-up identifier.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentifierMatch {
    pub document_id: Ulid,
    /// The document's w3id PID once it is active.
    pub pid: Option<String>,
    pub origin: IdentifierOrigin,
}

/// Merges answers from several nodes: one entry per document, `Published` first, then by id.
pub(crate) fn merge_matches(
    answers: impl IntoIterator<Item = IdentifierMatch>,
) -> Vec<IdentifierMatch> {
    let mut merged: Vec<IdentifierMatch> = Vec::new();
    for answer in answers {
        match merged
            .iter_mut()
            .find(|known| known.document_id == answer.document_id)
        {
            Some(known) => {
                known.origin = known.origin.min(answer.origin);
                if known.pid.is_none() {
                    known.pid = answer.pid;
                }
            }
            None => merged.push(answer),
        }
    }
    merged.sort_by_key(|found| (found.origin, found.document_id));
    merged
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use aruna_core::calls::IoError;
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::SECONDARY_ID_KEYSPACE;
    use aruna_core::structs::PersistentIdRevision;
    use aruna_core::structs::execution::job::JobId;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::secondary_id::SecondaryIdKind;

    pub(crate) fn mapping(document_id: Ulid) -> PersistentIdMapping {
        PersistentIdMapping::requested(
            document_id,
            false,
            Default::default(),
            JobId::from_bytes([4; 16]),
            true,
            "/documents/test".into(),
            PersistentIdRevision {
                event_id: Ulid::from_bytes([1; 16]),
                actor: iroh::SecretKey::from_bytes(&[0u8; 32]).public(),
                occurred_at_ms: 50,
            },
        )
    }

    fn doi() -> SecondaryIdentifier {
        SecondaryIdentifier::new(
            SecondaryIdKind::Doi,
            "10.1/x",
            None,
            IdentifierOrigin::Published,
        )
        .unwrap()
    }

    fn operation(document_id: Ulid) -> AddIdentifiersOperation {
        AddIdentifiersOperation::new(AddIdentifiersInput {
            document_id,
            identifiers: vec![doi()],
            route: None,
            occurred_at_ms: 10,
        })
    }

    pub(crate) fn read_event(value: Option<&PersistentIdMapping>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: value.map(|mapping| mapping.to_bytes().unwrap().into()),
        })
    }

    fn stored(mapping: &PersistentIdMapping) -> IoResult {
        IoResult::Values(vec![Some(mapping.to_bytes().unwrap().into())])
    }

    #[test]
    fn writes_with_index() {
        let document_id = Ulid::from_bytes([3; 16]);
        let mut op = operation(document_id);
        assert!(op.transaction());
        assert_eq!(
            op.first(),
            Ok(Next::Call(read_row(
                ID_MAPPING_KEYSPACE,
                persistent_id_key(document_id)
            )))
        );
        let Ok(Next::Call(IoCall::Write(writes))) = op.next(stored(&mapping(document_id))) else {
            panic!("expected one write");
        };
        let row = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == ID_MAPPING_KEYSPACE)
            .map(|(_, _, value)| PersistentIdMapping::from_bytes(value).unwrap())
            .unwrap();
        assert!(row.secondary_identifiers.contains(&doi()));
        assert_eq!(row.revision.occurred_at_ms, 51);
        assert!(writes.iter().any(|(keyspace, key, value)| {
            keyspace == SECONDARY_ID_KEYSPACE
                && key.as_ref() == doi().index_key(document_id)
                && value.as_ref() == document_id.to_bytes()
        }));
        assert_eq!(op.next(IoResult::Done), Ok(Next::Done((row, true))));
    }

    #[test]
    fn missing_row_errors() {
        let document_id = Ulid::from_bytes([3; 16]);
        let mut op = operation(document_id);
        op.first().unwrap();
        assert_eq!(
            op.next(IoResult::Values(vec![None])),
            Err(PersistentIdError::IntentMissing)
        );
    }

    #[test]
    fn known_identifier_noop() {
        let document_id = Ulid::from_bytes([3; 16]);
        let mut known = mapping(document_id);
        known.secondary_identifiers.insert(doi());
        let mut op = operation(document_id);
        op.first().unwrap();
        assert_eq!(op.next(stored(&known)), Ok(Next::Done((known, false))));
    }

    #[test]
    fn conflict_reruns() {
        // A lost commit reads as the storage conflict and lets the runner rerun the operation.
        let conflict = PersistentIdError::from(IoError::Conflict);
        assert_eq!(
            conflict,
            PersistentIdError::Storage(StorageError::TransactionConflict)
        );
        assert!(AddIdentifiersOperation::is_conflict(&conflict));
        assert!(!AddIdentifiersOperation::is_conflict(
            &PersistentIdError::PlacementFenced
        ));
    }

    #[test]
    fn fence_rejects_add() {
        let document_id = Ulid::from_bytes([3; 16]);
        let route = MappingRoute {
            realm_id: RealmId([5; 32]),
            placement: aruna_core::structs::placement::record::PlacementRef {
                strategy_id: Ulid::from_bytes([6; 16]),
                shard: 1,
            },
            peers: Vec::new(),
            actor: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            generation: 3,
        };
        let mut op = AddIdentifiersOperation::new(AddIdentifiersInput {
            document_id,
            identifiers: vec![doi()],
            route: Some(route.clone()),
            occurred_at_ms: 10,
        });
        op.first().unwrap();
        assert_eq!(
            op.next(stored(&mapping(document_id))),
            Ok(Next::Call(IoCall::Read(vec![fence_read(
                &route.realm_id,
                &route.placement
            )])))
        );
        // A bucket closed at generation 5 no longer admits a write resolved at 3.
        let closed = IoResult::Values(vec![Some(5u64.to_be_bytes().to_vec().into())]);
        assert_eq!(op.next(closed), Err(PersistentIdError::PlacementFenced));
    }

    #[test]
    fn matches_published_first() {
        let found = |seed: u8, origin, pid: Option<&str>| IdentifierMatch {
            document_id: Ulid::from_bytes([seed; 16]),
            pid: pid.map(str::to_string),
            origin,
        };
        let merged = merge_matches([
            found(1, IdentifierOrigin::Imported, None),
            found(3, IdentifierOrigin::Published, None),
            found(2, IdentifierOrigin::Imported, None),
            found(2, IdentifierOrigin::Published, Some("pid-2")),
        ]);
        assert_eq!(
            merged,
            vec![
                found(2, IdentifierOrigin::Published, Some("pid-2")),
                found(3, IdentifierOrigin::Published, None),
                found(1, IdentifierOrigin::Imported, None),
            ]
        );
    }
}
