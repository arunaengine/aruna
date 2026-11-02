//! Removes one document sync topic from a stopped node, so the node can join the realm's topic
//! again after it stored an incompatible genesis.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::info_topics::{self, InfoRows};
use crate::error::CliError;
use crate::explorer::ExplorerError;
use aruna_core::keyspaces::{
    APPLIED_OPS_KEYSPACE, QUARANTINE_USAGE_KEYSPACE, SYNC_OUTBOX_KEYSPACE, SYNC_QUARANTINE_KEYSPACE,
};
use aruna_core::structs::QUARANTINE_USAGE_KEY;
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase};
use irokle::{FjallStorage, Storage, TopicId};
use serde::Serialize;
use std::path::Path;

#[derive(Debug, Serialize)]
pub struct PurgeOutput {
    database_path: String,
    topic_id: String,
    applied: bool,
    cursors: usize,
    quarantine: usize,
    outbox: usize,
    /// Topic ops and fan-out cursors deleted; zero on a dry run.
    ops_deleted: usize,
    fanout_deleted: usize,
}

pub async fn purge_topic(
    database_path: String,
    sync_path: Option<String>,
    topic_id: String,
    apply: bool,
) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking(move || {
        let sync_path = sync_path
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| Path::new(&database_path).join("document-sync"));
        purge_output(&database_path, &sync_path, &topic_id, apply)
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn purge_output(
    database_path: &str,
    sync_path: &Path,
    topic_id: &str,
    apply: bool,
) -> Result<PurgeOutput, ExplorerError> {
    let topic = topic_id
        .parse::<TopicId>()
        .map_err(|error| ExplorerError::Refused(format!("topic id: {error}")))?;
    // Opening takes the store lock, so this refuses while the node runs.
    let db = OptimisticTxDatabase::builder(Path::new(database_path)).open()?;
    let realm_id = info_topics::node_realm(&db)?
        .ok_or_else(|| ExplorerError::Refused("node has no realm".to_string()))?;
    if !sync_path.exists()
        || FjallStorage::open(sync_path)?
            .topic_state(&topic)?
            .is_none()
    {
        return Err(ExplorerError::Refused(format!("topic {topic} not found")));
    }
    let applied = db.keyspace(APPLIED_OPS_KEYSPACE, KeyspaceCreateOptions::default)?;
    let quarantine = db.keyspace(SYNC_QUARANTINE_KEYSPACE, KeyspaceCreateOptions::default)?;
    let usage = db.keyspace(QUARANTINE_USAGE_KEYSPACE, KeyspaceCreateOptions::default)?;
    let outbox = db.keyspace(SYNC_OUTBOX_KEYSPACE, KeyspaceCreateOptions::default)?;
    let rows = InfoRows {
        applied: &applied,
        quarantine: &quarantine,
        usage: &usage,
        outbox: &outbox,
    };
    let cleanup = info_topics::topic_rows(&db, &[topic], rows, |record| {
        record.target.sync_topic_id(realm_id, &record.placement) == topic
    })?;
    let mut output = PurgeOutput {
        database_path: database_path.to_string(),
        topic_id: topic.to_string(),
        applied: apply,
        cursors: cleanup.cursors.len(),
        quarantine: cleanup.quarantine.len(),
        outbox: cleanup.outbox.len(),
        ops_deleted: 0,
        fanout_deleted: 0,
    };
    if !apply {
        return Ok(output);
    }
    let mut txn = db.write_tx()?;
    for (keyspace, keys) in [
        (&applied, &cleanup.cursors),
        (&quarantine, &cleanup.quarantine),
        (&outbox, &cleanup.outbox),
    ] {
        for key in keys {
            txn.remove(keyspace.clone(), key.clone());
        }
    }
    if let Some(value) = &cleanup.usage {
        txn.insert(usage.clone(), QUARANTINE_USAGE_KEY, value.clone());
    }
    txn.commit()?
        .map_err(|_| ExplorerError::Refused("conflicted with a running node".to_string()))?;
    // The node rows go first, so an interrupted purge still finds the topic and can repeat.
    (output.ops_deleted, output.fanout_deleted) =
        info_topics::purge_topics(sync_path, &cleanup.topics)?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::super::info_topics::tests::{genesis, node, outbox};
    use super::super::tests::{node_state, read, write};
    use super::*;
    use aruna_core::document::DocumentTarget;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use tempfile::tempdir;

    const REALM: RealmId = RealmId([3u8; 32]);

    struct Store {
        _temp: tempfile::TempDir,
        path: std::path::PathBuf,
        sync: std::path::PathBuf,
        purged: TopicId,
        kept: TopicId,
    }

    /// A node with two topics, each with an applied cursor and a queued outbox row.
    fn two_topics() -> Store {
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let sync = path.join("document-sync");
        node_state(&path);
        let usage = DocumentTarget::NodeUsage {
            realm_id: REALM,
            node_id: node(1),
            group_id: None,
        };
        let info = DocumentTarget::NodeInfo {
            realm_id: REALM,
            node_id: node(1),
        };
        let purged = usage.sync_topic_id(REALM, &PlacementRef::NIL);
        let kept = info.sync_topic_id(REALM, &PlacementRef::NIL);
        {
            let storage = FjallStorage::open(&sync).unwrap();
            genesis(&storage, purged);
            genesis(&storage, kept);
        }
        write(
            &path,
            SYNC_OUTBOX_KEYSPACE,
            vec![
                (b"usage".as_slice(), outbox(usage)),
                (b"info".as_slice(), outbox(info)),
            ],
        );
        let (gone, stay) = (
            info_topics::cursor_key(&purged),
            info_topics::cursor_key(&kept),
        );
        write(
            &path,
            APPLIED_OPS_KEYSPACE,
            vec![
                (gone.as_slice(), b"cursor".to_vec()),
                (stay.as_slice(), b"cursor".to_vec()),
            ],
        );
        Store {
            _temp: temp,
            path,
            sync,
            purged,
            kept,
        }
    }

    fn run(store: &Store, topic_id: &str, apply: bool) -> Result<PurgeOutput, ExplorerError> {
        purge_output(store.path.to_str().unwrap(), &store.sync, topic_id, apply)
    }

    fn topic_exists(store: &Store, topic_id: &TopicId) -> bool {
        let storage = FjallStorage::open(&store.sync).unwrap();
        storage.topic_state(topic_id).unwrap().is_some()
    }

    #[test]
    fn removes_one_topic() {
        let store = two_topics();

        let output = run(&store, &store.purged.to_string(), true).unwrap();

        assert_eq!((output.cursors, output.outbox), (1, 1));
        assert_eq!(output.ops_deleted, 1);
        assert!(!topic_exists(&store, &store.purged));
        assert!(topic_exists(&store, &store.kept));
        let outbox_rows = read(&store.path, SYNC_OUTBOX_KEYSPACE);
        assert_eq!(
            outbox_rows.into_keys().collect::<Vec<_>>(),
            vec![b"info".to_vec()]
        );
        let cursors = read(&store.path, APPLIED_OPS_KEYSPACE);
        assert_eq!(
            cursors.into_keys().collect::<Vec<_>>(),
            vec![info_topics::cursor_key(&store.kept)]
        );
    }

    #[test]
    fn dry_run_unchanged() {
        let store = two_topics();

        let output = run(&store, &store.purged.to_string(), false).unwrap();

        assert_eq!(
            (output.cursors, output.outbox, output.ops_deleted),
            (1, 1, 0)
        );
        assert!(topic_exists(&store, &store.purged));
        assert_eq!(read(&store.path, SYNC_OUTBOX_KEYSPACE).len(), 2);
        assert_eq!(read(&store.path, APPLIED_OPS_KEYSPACE).len(), 2);
    }

    #[test]
    fn refuses_missing_topic() {
        let store = two_topics();
        let missing = TopicId::hash(b"missing").to_string();

        let error = run(&store, &missing, true).unwrap_err();

        assert!(matches!(error, ExplorerError::Refused(_)));
        assert!(topic_exists(&store, &store.purged));
        assert_eq!(read(&store.path, SYNC_OUTBOX_KEYSPACE).len(), 2);
    }

    #[test]
    fn refuses_running_node() {
        let store = two_topics();
        let _running = OptimisticTxDatabase::builder(&store.path).open().unwrap();

        let error = run(&store, &store.purged.to_string(), true).unwrap_err();

        assert!(matches!(error, ExplorerError::Fjall(fjall::Error::Locked)));
        assert!(topic_exists(&store, &store.purged));
    }
}
