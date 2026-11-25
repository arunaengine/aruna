//! Adds the node history entries a realm config state written before the history was kept lacks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::Rewrites;
use crate::explorer::ExplorerError;
use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::reducer::decode_reducer_state;
use aruna_core::storage_entries::reducer_state_key;
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};

/// Realm config states carry this key byte; other reducer states are not read.
const CONFIG_STATE: u8 = b'c';

/// The realm config reducer states that hold nodes without a history entry, each with the
/// entries their add events would have written. A state another node already backfilled, or a
/// rerun, rewrites nothing.
pub(super) fn legacy_states(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
) -> Result<Rewrites, ExplorerError> {
    let mut scanned = 0;
    let mut rows = Vec::new();
    for entry in db.read_tx().prefix(keyspace, [CONFIG_STATE]) {
        let (key, value) = entry.into_inner()?;
        let Ok(mut state) = decode_reducer_state(&value) else {
            continue;
        };
        let config = matches!(state.target, AdminDocumentTarget::RealmConfig { .. });
        if !config || reducer_state_key(&state.target).as_ref() != key.as_ref() {
            continue;
        }
        scanned += 1;
        if state.keep_legacy_history() == 0 {
            continue;
        }
        let bytes = postcard::to_allocvec(&state)
            .map_err(|error| ExplorerError::Decode(error.to_string()))?;
        rows.push((key.to_vec(), bytes));
    }
    Ok(Rewrites { scanned, rows })
}

#[cfg(test)]
mod tests {
    use super::super::migrate_output;
    use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
    use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
    use aruna_core::reducer::paths::NODE_HISTORY_PREFIX;
    use aruna_core::reducer::{AdminDocumentState, decode_reducer_state};
    use aruna_core::storage_entries::reducer_state_key;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::{RealmId, RealmNodeKind};
    use aruna_core::user::id::UserId;
    use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, Readable};
    use ulid::Ulid;

    // A realm config state from before the node history: the migration adds the entries the
    // add events write now, and a second run rewrites nothing.
    #[test]
    fn backfills_once() {
        let dir = tempfile::tempdir().unwrap();
        let realm_id = RealmId::from_bytes([3; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let actor = Actor {
            node_id,
            user_id: UserId::local(Ulid::from_parts(1, 1), realm_id),
            realm_id,
        };
        let mut state = AdminDocumentState::new(AdminDocumentTarget::RealmConfig { realm_id });
        let ensure = AdminDocumentOperation::ConfigNodeEnsured {
            node_id,
            kind: RealmNodeKind::Management,
        };
        state
            .apply_operation(&actor, ensure, Ulid::from_parts(1, 2))
            .unwrap();
        let kept = state.clone();
        state
            .user_subject_ids
            .retain(|path, _| !path.starts_with(NODE_HISTORY_PREFIX));
        let key = reducer_state_key(&state.target);
        {
            let db = OptimisticTxDatabase::builder(dir.path()).open().unwrap();
            let rows = db
                .keyspace(DOCUMENT_STATE_KEYSPACE, KeyspaceCreateOptions::default)
                .unwrap();
            let mut txn = db.write_tx().unwrap();
            txn.insert(rows, key.to_vec(), postcard::to_allocvec(&state).unwrap());
            txn.commit().unwrap().unwrap();
        }
        let database = dir.path().to_str().unwrap();
        let output = migrate_output(database).unwrap();
        assert_eq!(
            (
                output.node_histories_scanned,
                output.node_histories_rewritten
            ),
            (1, 1)
        );
        let again = migrate_output(database).unwrap();
        assert_eq!(again.node_histories_rewritten, 0);
        let db = OptimisticTxDatabase::builder(dir.path()).open().unwrap();
        let rows = db
            .keyspace(DOCUMENT_STATE_KEYSPACE, KeyspaceCreateOptions::default)
            .unwrap();
        let stored = db.read_tx().get(&rows, key.as_ref()).unwrap().unwrap();
        let stored = decode_reducer_state(&stored).unwrap();
        assert_eq!(stored, kept, "the entries match what the add event wrote");
        assert_eq!(stored.node_intervals().len(), 1);
    }
}
