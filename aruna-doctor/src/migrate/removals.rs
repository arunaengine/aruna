//! Re-encodes queued realm config node removals from before they carried publisher cutoffs.
//! A relayed one keeps the origin signature over its old bytes, so it stops the migration.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::session_refs::{self, Layout};
use super::{Rewrites, decode_error, row_note};
use crate::explorer::ExplorerError;
use aruna_core::admin_documents::AdminDocumentOperation;
use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord};
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};

/// Node removals before the publisher cutoffs, stored with the node id only.
const REMOVAL: Layout = &[("node_id,cutoffs", 1)];

/// Reads a queued publish in its current shape or as one holding an old node removal.
pub(super) fn outbox_record(value: &[u8]) -> postcard::Result<DocumentOutboxRecord> {
    postcard::from_bytes(value).or_else(|_| session_refs::decode(value, REMOVAL))
}

/// Queued node removals this node originated, in the current shape. The node signs them when
/// it publishes, so empty cutoffs are right: no set had a publisher before.
pub(super) fn removal_rows(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    name: &str,
) -> Result<Rewrites, ExplorerError> {
    let mut scanned = 0;
    let mut rows = Vec::new();
    for entry in db.read_tx().iter(keyspace) {
        let (key, value) = entry.into_inner()?;
        scanned += 1;
        if session_refs::decode::<DocumentOutboxRecord>(&value, &[]).is_ok() {
            continue;
        }
        let Ok(record) = session_refs::decode::<DocumentOutboxRecord>(&value, REMOVAL) else {
            continue;
        };
        let DocumentOutboxEvent::AdminOperation {
            event,
            origin_signature,
        } = &record.event
        else {
            continue;
        };
        if !matches!(event.op, AdminDocumentOperation::ConfigNodeRemoved { .. }) {
            continue;
        }
        if origin_signature.is_some() {
            return Err(ExplorerError::Refused(row_note(
                name,
                &key,
                "a relayed realm config node removal from an earlier release waits for \
                 publishing, and its origin signature covers the old encoding. Start the node \
                 with the earlier release until its sync outbox has published the row, stop it, \
                 then run the migration again",
            )));
        }
        let bytes =
            postcard::to_allocvec(&record).map_err(|error| decode_error(name, &key, error))?;
        rows.push((key.to_vec(), bytes));
    }
    Ok(Rewrites { scanned, rows })
}

#[cfg(test)]
mod tests {
    use super::super::migrate_output;
    use super::super::tests::{read, strip, write};
    use super::*;
    use aruna_core::UserId;
    use aruna_core::admin_documents::{
        AdminDocumentClock, AdminDocumentEvent, AdminDocumentTarget,
    };
    use aruna_core::document::DocumentTarget;
    use aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_operations::sync::document_outbox::new_outbox_record;
    use ulid::Ulid;

    /// A queued node removal and its bytes before the publisher cutoffs.
    fn removal(origin_signature: Option<iroh::Signature>) -> (DocumentOutboxRecord, Vec<u8>) {
        let realm_id = RealmId([3; 32]);
        let origin = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let removed = iroh::SecretKey::from_bytes(&[9; 32]).public();
        let event = AdminDocumentEvent {
            event_id: Ulid::from(1),
            target: AdminDocumentTarget::RealmConfig { realm_id },
            origin_node_id: origin,
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor: Actor {
                node_id: origin,
                user_id: UserId::nil(realm_id),
                realm_id,
            },
            op: AdminDocumentOperation::ConfigNodeRemoved {
                node_id: removed,
                cutoffs: Vec::new(),
            },
        };
        let event = DocumentOutboxEvent::AdminOperation {
            event: Box::new(event),
            origin_signature,
        };
        let target = DocumentTarget::RealmConfig { realm_id };
        let record = new_outbox_record(origin, target, Vec::new(), event, PlacementRef::NIL, false);
        let current = postcard::to_allocvec(&record).expect("record encodes");
        let removed = [
            postcard::to_allocvec(&removed).expect("id encodes"),
            vec![0],
        ]
        .concat();
        let old = strip(&current, &removed, 1);
        (record, old)
    }

    #[test]
    fn rewrites_own_removals() {
        let (record, old) = removal(None);
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().join("db");
        let database = path.to_str().expect("path");
        write(&path, SYNC_OUTBOX_KEYSPACE, vec![(b"removal", old)]);

        let output = migrate_output(database).expect("migration runs");

        assert_eq!(output.outbox_removals_rewritten, 1);
        let rows = read(&path, SYNC_OUTBOX_KEYSPACE);
        let migrated: DocumentOutboxRecord =
            postcard::from_bytes(&rows[b"removal".as_slice()]).expect("row decodes");
        assert_eq!(migrated, record);
        let again = migrate_output(database).expect("migration repeats");
        assert_eq!(again.outbox_removals_rewritten, 0);
    }

    #[test]
    fn refuses_relayed_removals() {
        let signature = iroh::SecretKey::from_bytes(&[1; 32]).sign(b"removal");
        let (_, old) = removal(Some(signature));
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().join("db");
        write(&path, SYNC_OUTBOX_KEYSPACE, vec![(b"removal", old.clone())]);

        let error = migrate_output(path.to_str().expect("path")).expect_err("refused");

        assert!(matches!(error, ExplorerError::Refused(_)), "{error}");
        assert_eq!(
            read(&path, SYNC_OUTBOX_KEYSPACE)[b"removal".as_slice()],
            old
        );
    }
}
