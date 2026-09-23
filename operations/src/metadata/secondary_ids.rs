//! Adds external identifiers, such as a repository DOI, to a document's persistent id mapping.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::SECONDARY_ID_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::PersistentIdMapping;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::secondary_id::{
    SecondaryIdKind, SecondaryIdentifier, secondary_id_prefix,
};
use aruna_core::types::{Effects, TxnId};
use byteview::ByteView;
use smallvec::smallvec;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::metadata::api::{MetadataApiError, can_read_record};
use crate::metadata::get_document::load_document_record;
use crate::metadata::persistent_id::{
    MappingRoute, PersistentIdError, mapping_revision, parse_mapping_read, read_mapping,
    read_mapping_effect, transition_entries,
};
use crate::storage_read::scan_all;

const COMMIT_ATTEMPTS: usize = 4;

#[derive(Clone, Debug, PartialEq)]
pub struct AddIdentifiersInput {
    pub document_id: Ulid,
    pub identifiers: Vec<SecondaryIdentifier>,
    pub route: Option<MappingRoute>,
    pub occurred_at_ms: u64,
}

#[derive(Debug, PartialEq)]
enum State {
    Init,
    Start,
    Read,
    Write,
    Commit,
    Aborting,
    Finish,
}

/// Unions identifiers into the existing row with its index, sync and outbox entries in one
/// transaction. Every document gets its row at creation, so a missing row is never created here.
#[derive(Debug, PartialEq)]
pub struct AddIdentifiersOperation {
    input: AddIdentifiersInput,
    state: State,
    attempts: usize,
    txn_id: Option<TxnId>,
    merged: Option<PersistentIdMapping>,
    output: Option<Result<(PersistentIdMapping, bool), PersistentIdError>>,
}

impl AddIdentifiersOperation {
    pub fn new(input: AddIdentifiersInput) -> Self {
        Self {
            input,
            state: State::Init,
            attempts: 0,
            txn_id: None,
            merged: None,
            output: None,
        }
    }

    fn begin(&mut self) -> Effects {
        self.attempts += 1;
        self.state = State::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn fail(&mut self, error: PersistentIdError) -> Effects {
        self.output = Some(Err(error));
        self.abort()
    }

    /// Ends without writing, keeping the stored row as the answer.
    fn unchanged(&mut self, mapping: PersistentIdMapping) -> Effects {
        self.output = Some(Ok((mapping, false)));
        self.abort()
    }

    fn handle_read(&mut self, event: Event) -> Effects {
        let mapping = match parse_mapping_read(event) {
            Ok(Some(mapping)) => mapping,
            Ok(None) => return self.fail(PersistentIdError::IntentMissing),
            Err(error) => return self.fail(error),
        };
        if mapping.is_retired() {
            return self.unchanged(mapping);
        }
        let mut merged = mapping.clone();
        merged
            .secondary_identifiers
            .extend(self.input.identifiers.iter().cloned());
        if merged == mapping {
            return self.unchanged(mapping);
        }
        // A new revision orders after the replaced one, so holders and sync accept the row.
        let occurred_at_ms = self
            .input
            .occurred_at_ms
            .max(mapping.revision.occurred_at_ms.saturating_add(1));
        merged.revision = mapping_revision(&self.input.route, occurred_at_ms);
        let writes = match transition_entries(&self.input.route, &merged) {
            Ok(writes) => writes,
            Err(error) => return self.fail(error),
        };
        self.merged = Some(merged);
        self.state = State::Write;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(PersistentIdError::Unavailable("transaction missing".into()));
        };
        self.state = State::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }
}

impl Operation for AddIdentifiersOperation {
    type Output = (PersistentIdMapping, bool);
    type Error = PersistentIdError;

    fn start(&mut self) -> Effects {
        self.begin()
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (State::Aborting, _) => {
                self.state = State::Finish;
                smallvec![]
            }
            (State::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = State::Read;
                smallvec![read_mapping_effect(self.input.document_id, Some(txn_id))]
            }
            (State::Read, event) => self.handle_read(event),
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => self.commit(),
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.state = State::Finish;
                self.txn_id = None;
                self.output = self.merged.take().map(|mapping| Ok((mapping, true)));
                smallvec![]
            }
            (
                State::Commit,
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                }),
            ) if self.attempts < COMMIT_ATTEMPTS => {
                self.txn_id = None;
                self.merged = None;
                self.begin()
            }
            (_, Event::Storage(StorageEvent::Error { error })) => {
                if self.state == State::Commit {
                    self.txn_id = None;
                }
                self.fail(PersistentIdError::Storage(error))
            }
            (state, event) => {
                let message = format!("unexpected event {event:?} in state {state:?}");
                self.fail(PersistentIdError::Unavailable(message))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == State::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(PersistentIdError::Unavailable(
            "identifier update did not finish".into(),
        )))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = State::Aborting;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => {
                self.state = State::Finish;
                smallvec![]
            }
        }
    }
}

/// Finds the first document holding the identifier that the caller may read, with its active PID.
/// Unreadable and deleted documents are skipped, so a miss never reveals that they exist.
pub async fn lookup_identifier(
    ctx: &DriverContext,
    realm_id: RealmId,
    auth: Option<&AuthContext>,
    kind: SecondaryIdKind,
    value: &str,
    endpoint: Option<&str>,
) -> Result<Option<(Ulid, Option<String>)>, MetadataApiError> {
    let candidates = match endpoint {
        Some(endpoint) => {
            let identifier = SecondaryIdentifier {
                kind,
                value: value.to_string(),
                endpoint: Some(endpoint.to_string()),
            };
            let event = ctx
                .storage_handle
                .send_effect(Effect::Storage(StorageEffect::Read {
                    key_space: SECONDARY_ID_KEYSPACE.to_string(),
                    key: ByteView::from(identifier.index_key()),
                    txn_id: None,
                }))
                .await;
            match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    value.into_iter().collect()
                }
                _ => return Err(MetadataApiError::ServiceUnavailable),
            }
        }
        None => scan_all(
            &ctx.storage_handle,
            SECONDARY_ID_KEYSPACE,
            Some(ByteView::from(secondary_id_prefix(kind, value))),
        )
        .await
        .map_err(MetadataApiError::Internal)?
        .into_iter()
        .map(|(_, document)| document)
        .collect::<Vec<_>>(),
    };
    for document in candidates {
        let Ok(bytes) = <[u8; 16]>::try_from(document.as_ref()) else {
            continue;
        };
        let document_id = Ulid::from_bytes(bytes);
        let record = load_document_record(ctx, document_id)
            .await
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        let Some(record) = record else {
            continue;
        };
        if !can_read_record(ctx, realm_id, auth, &record).await? {
            continue;
        }
        let pid = read_mapping(ctx, document_id)
            .await
            .map_err(|error| MetadataApiError::Internal(error.to_string()))?
            .filter(PersistentIdMapping::is_active)
            .map(|mapping| mapping.pid);
        return Ok(Some((document_id, pid)));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::keyspaces::ID_MAPPING_KEYSPACE;
    use aruna_core::structs::PersistentIdRevision;
    use aruna_core::structs::execution::job::JobId;

    fn mapping(document_id: Ulid) -> PersistentIdMapping {
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
        SecondaryIdentifier::new(SecondaryIdKind::Doi, "10.1/x", None).unwrap()
    }

    fn operation(document_id: Ulid) -> AddIdentifiersOperation {
        AddIdentifiersOperation::new(AddIdentifiersInput {
            document_id,
            identifiers: vec![doi()],
            route: None,
            occurred_at_ms: 10,
        })
    }

    fn read_event(value: Option<&PersistentIdMapping>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(Vec::new()),
            value: value.map(|mapping| ByteView::from(mapping.to_bytes().unwrap())),
        })
    }

    fn started(op: &mut AddIdentifiersOperation) -> Effects {
        op.start();
        op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([9; 16]),
        }))
    }

    #[test]
    fn writes_row_and_index() {
        let document_id = Ulid::from_bytes([3; 16]);
        let mut op = operation(document_id);
        started(&mut op);
        let effects = op.step(read_event(Some(&mapping(document_id))));
        let Some(Effect::Storage(StorageEffect::BatchWrite { writes, .. })) = effects.first()
        else {
            panic!("expected one batch write, got {effects:?}");
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
                && key.as_ref() == doi().index_key()
                && value.as_ref() == document_id.to_bytes()
        }));
        op.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        op.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: Ulid::from_bytes([9; 16]),
        }));
        let (stored, changed) = op.finalize().unwrap();
        assert!(changed);
        assert_eq!(stored, row);
    }

    #[test]
    fn missing_row_errors() {
        let document_id = Ulid::from_bytes([3; 16]);
        let mut op = operation(document_id);
        started(&mut op);
        let effects = op.step(read_event(None));
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::AbortTransaction { .. }))
        ));
        op.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: Ulid::from_bytes([9; 16]),
        }));
        assert_eq!(op.finalize().unwrap_err(), PersistentIdError::IntentMissing);
    }

    #[test]
    fn known_identifier_noop() {
        let document_id = Ulid::from_bytes([3; 16]);
        let mut stored = mapping(document_id);
        stored.secondary_identifiers.insert(doi());
        let mut op = operation(document_id);
        started(&mut op);
        op.step(read_event(Some(&stored)));
        op.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: Ulid::from_bytes([9; 16]),
        }));
        assert_eq!(op.finalize().unwrap(), (stored, false));
    }

    #[test]
    fn conflict_retries_then_fails() {
        let document_id = Ulid::from_bytes([3; 16]);
        let mut op = operation(document_id);
        for attempt in 1..=COMMIT_ATTEMPTS {
            if attempt == 1 {
                started(&mut op);
            } else {
                op.step(Event::Storage(StorageEvent::TransactionStarted {
                    txn_id: Ulid::from_bytes([9; 16]),
                }));
            }
            op.step(read_event(Some(&mapping(document_id))));
            op.step(Event::Storage(StorageEvent::BatchWriteResult {
                entries: Vec::new(),
            }));
            let effects = op.step(Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict,
            }));
            let restarted = matches!(
                effects.first(),
                Some(Effect::Storage(StorageEffect::StartTransaction { .. }))
            );
            assert_eq!(restarted, attempt < COMMIT_ATTEMPTS);
        }
        assert!(op.is_complete());
        assert_eq!(
            op.finalize().unwrap_err(),
            PersistentIdError::Storage(StorageError::TransactionConflict)
        );
    }
}
