//! Opens a new metadata history window once the current one fills up.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::metadata::{EVENT_LIMIT, RAW_BYTES_LIMIT};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::{NodeId, UserId};
use thiserror::Error;

use crate::metadata::checkpoint_window::{CheckpointWindowOperation, due};
use crate::metadata::update_document::{
    UpdateDocumentConfig, UpdateDocumentError, UpdateDocumentMutation, UpdateDocumentOperation,
};
use crate::metadata::update_entry::UpdateEntryOperation;

#[derive(Debug, Error, PartialEq)]
pub enum CheckpointError {
    #[error(transparent)]
    Window(#[from] IoError),
    #[error(transparent)]
    Update(#[from] UpdateDocumentError),
}

/// Checks the document's current window after an event materialized here and writes a
/// checkpoint when it is due. Nodes that are not origins of the document never write one.
#[derive(Debug, PartialEq)]
pub(crate) struct CheckpointOperation {
    local_node: Option<NodeId>,
    record: MetadataRegistryRecord,
    /// This node's place among the origins and their share of the window, once read.
    origin: Option<(NodeId, usize)>,
    updating: bool,
}

type CheckpointTurn = Result<Next<()>, CheckpointError>;

impl CheckpointOperation {
    pub(crate) fn new(local_node: Option<NodeId>, record: MetadataRegistryRecord) -> Self {
        Self {
            local_node,
            record,
            origin: None,
            updating: false,
        }
    }

    fn window_read(&mut self, result: IoResult) -> CheckpointTurn {
        let used = result.child::<(u64, u64), IoError>()??;
        let Some((node_id, position)) = self.origin else {
            return Ok(Next::Done(()));
        };
        let origins = self.record.holder_node_ids.len() as u64;
        let share = (
            u64::from(EVENT_LIMIT - 1) / origins,
            RAW_BYTES_LIMIT / origins,
        );
        if !due(used, share, position == 0) {
            return Ok(Next::Done(()));
        }
        let update = UpdateDocumentOperation::new(UpdateDocumentConfig {
            actor: Actor {
                node_id,
                user_id: UserId::nil(self.record.realm_id),
                realm_id: self.record.realm_id,
            },
            group_id: self.record.group_id,
            document_id: self.record.document_id,
            public: self.record.public,
            mutation: UpdateDocumentMutation::Checkpoint,
            expected_revision: None,
        });
        self.updating = true;
        Ok(Next::Call(IoCall::child(UpdateEntryOperation::new(
            update,
            self.local_node,
        ))))
    }
}

impl Operation for CheckpointOperation {
    type Output = ();
    type Error = CheckpointError;

    fn first(&mut self) -> CheckpointTurn {
        let Some(node_id) = self.local_node else {
            return Ok(Next::Done(()));
        };
        let Some(position) =
            (self.record.holder_node_ids.iter()).position(|holder| *holder == node_id)
        else {
            return Ok(Next::Done(()));
        };
        self.origin = Some((node_id, position));
        Ok(Next::Call(IoCall::child(CheckpointWindowOperation::new(
            self.record.document_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> CheckpointTurn {
        if !self.updating {
            return self.window_read(result);
        }
        result.child::<MetadataRegistryRecord, UpdateDocumentError>()??;
        Ok(Next::Done(()))
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn record(holders: Vec<NodeId>) -> MetadataRegistryRecord {
        let document_id = Ulid::from_parts(3, 3);
        MetadataRegistryRecord {
            realm_id: RealmId::from_bytes([1; 32]),
            group_id: Ulid::from_parts(2, 2),
            document_id,
            document_path: "datasets/window".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: String::new(),
            placement: aruna_core::structs::placement::record::PlacementRef::NIL,
            holder_node_ids: holders,
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::from_parts(1, 1),
            last_event_id: Ulid::from_parts(1, 1),
        }
    }

    fn used(events: u64, bytes: u64) -> IoResult {
        IoResult::Child(ChildOutcome::new(Ok::<_, IoError>((events, bytes))))
    }

    #[test]
    fn non_origin_skips() {
        let mut offline = CheckpointOperation::new(None, record(vec![node(1)]));
        assert_eq!(offline.first(), Ok(Next::Done(())));
        let mut outsider = CheckpointOperation::new(Some(node(2)), record(vec![node(1)]));
        assert_eq!(outsider.first(), Ok(Next::Done(())));
    }

    #[test]
    fn full_window_checkpoints() {
        let mut checkpoint = CheckpointOperation::new(Some(node(1)), record(vec![node(1)]));
        assert!(matches!(
            checkpoint.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let full = used(u64::from(EVENT_LIMIT), 0);
        assert!(matches!(
            checkpoint.next(full),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let done = IoResult::Child(ChildOutcome::new(Ok::<_, UpdateDocumentError>(record(
            vec![node(1)],
        ))));
        assert_eq!(checkpoint.next(done), Ok(Next::Done(())));
    }

    // Its update child holds the document; a hold here too would wait on itself forever.
    #[test]
    fn update_holds_document() {
        let checkpoint = CheckpointOperation::new(Some(node(1)), record(vec![node(1)]));
        assert_eq!(checkpoint.hold_scope(), None);
    }

    #[test]
    fn open_window_skips() {
        let mut checkpoint = CheckpointOperation::new(Some(node(1)), record(vec![node(1)]));
        checkpoint.first().unwrap();
        assert_eq!(checkpoint.next(used(1, 1)), Ok(Next::Done(())));
    }
}
