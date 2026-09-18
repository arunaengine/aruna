//! Opens the keyspaces read-only, summarizes stored rows and prints each summary as JSON.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::PendingShardPlacement;
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, KEYSPACE_CATALOG, NODE_STATE_KEYSPACE, STORAGE_BACKEND_KEYSPACE,
    SYNC_PLACEMENT_KEYSPACE,
};
use aruna_core::structs::placement::policy::attachment::{
    BULK_INTENT_KEYSPACE, BULK_RUN_KEYSPACE, POLICY_MUTATION_KEYSPACE,
};
use aruna_core::structs::storage::backends::BackendsFile;
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
use aruna_operations::sync::shard_placement::decode_placement;
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, Readable};
use std::collections::{BTreeSet, HashSet};
use std::path::Path;
use ulid::Ulid;

use super::ExplorerError;
use super::decode::decode_entry;
use super::present::{
    EntriesOutput, JsonPendingPlacement, KeyspaceEntry, KeyspacesOutput, LocationScanOutput,
    TopicListEntry, TopicPlacementsOutput, TopicStatusOutput, TopicsListOutput, UnresolvedLocation,
    placement_topic_id,
};
use crate::error::CliError;

pub async fn explore_keyspaces(database_path: String) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking({
        let database_path = database_path.clone();
        move || list_keyspaces(&database_path)
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

pub async fn explore_entries(database_path: String, keyspace: String) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking({
        let database_path = database_path.clone();
        let keyspace = keyspace.clone();
        move || list_entries(&database_path, &keyspace)
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

pub async fn print_node_state(database_path: String) -> Result<(), CliError> {
    explore_entries(database_path, NODE_STATE_KEYSPACE.to_string()).await
}

/// Reports locations whose recorded backend no longer resolves: node refs
/// against the operator's backends file, group refs against the stored tenant
/// records. Storage-side only, so the doctor needs no blob backend.
pub async fn scan_locations(
    database_path: String,
    backends_path: Option<String>,
) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking({
        let database_path = database_path.clone();
        move || location_scan(&database_path, backends_path.as_deref())
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

pub async fn print_topics_list(database_path: String) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking({
        let database_path = database_path.clone();
        move || topics_list_output(&database_path)
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

pub async fn print_topic_status(database_path: String, topic_id: String) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking({
        let database_path = database_path.clone();
        let topic_id = topic_id.clone();
        move || topic_status_output(&database_path, &topic_id)
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

pub async fn print_topic_placements(
    database_path: String,
    topic_id: Option<String>,
) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking({
        let database_path = database_path.clone();
        let topic_id = topic_id.clone();
        move || topic_placements_output(&database_path, topic_id.as_deref())
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn list_keyspaces(database_path: &str) -> Result<KeyspacesOutput, ExplorerError> {
    let db = OptimisticTxDatabase::builder(Path::new(database_path)).open()?;
    let mut keyspaces = db.list_keyspace_names();
    keyspaces.sort();
    let existing = keyspaces
        .iter()
        .map(|name| name.as_ref())
        .collect::<HashSet<_>>();
    let mut missing_keyspaces = defined_keyspaces()
        .into_iter()
        .filter(|name| !existing.contains(name))
        .map(|name| KeyspaceEntry {
            name: name.to_string(),
        })
        .collect::<Vec<_>>();
    missing_keyspaces.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(KeyspacesOutput {
        database_path: database_path.to_string(),
        keyspaces: keyspaces
            .into_iter()
            .map(|name| KeyspaceEntry {
                name: name.to_string(),
            })
            .collect(),
        missing_keyspaces,
    })
}

fn defined_keyspaces() -> Vec<&'static str> {
    let mut keyspaces = KEYSPACE_CATALOG.to_vec();
    keyspaces.extend([
        BULK_INTENT_KEYSPACE,
        BULK_RUN_KEYSPACE,
        POLICY_MUTATION_KEYSPACE,
    ]);
    keyspaces
}

fn list_entries(database_path: &str, keyspace_name: &str) -> Result<EntriesOutput, ExplorerError> {
    let db = OptimisticTxDatabase::builder(Path::new(database_path)).open()?;
    let keyspace_names = db.list_keyspace_names();
    if !keyspace_names
        .iter()
        .any(|name| name.as_ref() == keyspace_name)
    {
        return Err(ExplorerError::KeyspaceNotFound(keyspace_name.to_string()));
    }

    let keyspace = db.keyspace(keyspace_name, KeyspaceCreateOptions::default)?;
    let snapshot = db.read_tx();
    let mut entries = Vec::new();

    for entry in snapshot.iter(&keyspace) {
        let (key, value) = entry.into_inner()?;
        entries.push(decode_entry(keyspace_name, key.as_ref(), value.as_ref()));
    }

    Ok(EntriesOutput {
        database_path: database_path.to_string(),
        keyspace: keyspace_name.to_string(),
        entries,
    })
}

fn known_node_backends(backends_path: Option<&str>) -> Result<BTreeSet<String>, ExplorerError> {
    let Some(path) = backends_path else {
        return Ok(BTreeSet::from([BackendRef::DEFAULT_NODE_NAME.to_string()]));
    };
    let text = std::fs::read_to_string(path)?;
    let file =
        BackendsFile::parse(&text).map_err(|error| ExplorerError::Decode(error.to_string()))?;
    Ok(file.backend.into_keys().collect())
}

fn known_group_backends(
    db: &OptimisticTxDatabase,
    keyspaces: &[String],
) -> Result<BTreeSet<Ulid>, ExplorerError> {
    let mut known = BTreeSet::new();
    if !keyspaces
        .iter()
        .any(|name| name == STORAGE_BACKEND_KEYSPACE)
    {
        return Ok(known);
    }
    let keyspace = db.keyspace(STORAGE_BACKEND_KEYSPACE, KeyspaceCreateOptions::default)?;
    for entry in db.read_tx().iter(&keyspace) {
        let (key, _) = entry.into_inner()?;
        if let Ok(bytes) = <[u8; 16]>::try_from(key.as_ref()) {
            known.insert(Ulid::from_bytes(bytes));
        }
    }
    Ok(known)
}

fn location_scan(
    database_path: &str,
    backends_path: Option<&str>,
) -> Result<LocationScanOutput, ExplorerError> {
    let nodes = known_node_backends(backends_path)?;
    let db = OptimisticTxDatabase::builder(Path::new(database_path)).open()?;
    let keyspaces = db
        .list_keyspace_names()
        .iter()
        .map(|name| name.as_ref().to_string())
        .collect::<Vec<_>>();
    let groups = known_group_backends(&db, &keyspaces)?;

    let mut scanned = 0;
    let mut unresolved = Vec::new();
    if keyspaces.iter().any(|name| name == BLOB_LOCATIONS_KEYSPACE) {
        let keyspace = db.keyspace(BLOB_LOCATIONS_KEYSPACE, KeyspaceCreateOptions::default)?;
        for entry in db.read_tx().iter(&keyspace) {
            let (_, value) = entry.into_inner()?;
            let location = BackendLocation::from_bytes(value.as_ref())
                .map_err(|error| ExplorerError::Decode(error.to_string()))?;
            scanned += 1;
            let resolves = match &location.backend {
                BackendRef::Node(name) => nodes.contains(name),
                BackendRef::Group(id) => groups.contains(id),
            };
            if !resolves {
                unresolved.push(UnresolvedLocation {
                    backend: location.backend.to_string(),
                    storage_bucket: location.storage_bucket,
                    backend_path: location.backend_path,
                });
            }
        }
    }
    unresolved.sort();

    Ok(LocationScanOutput {
        database_path: database_path.to_string(),
        backends_path: backends_path.map(ToString::to_string),
        scanned,
        unresolved,
    })
}

fn topics_list_output(database_path: &str) -> Result<TopicsListOutput, ExplorerError> {
    let mut topics = load_pending_placements(database_path)?
        .into_iter()
        .map(|placement| TopicListEntry {
            topic_id: placement_topic_id(&placement),
            strategy_id: placement.placement.strategy_id.to_string(),
            shard: placement.placement.shard,
            status: "under_replicated",
            selected_peer_count: placement.selected_peers.len(),
        })
        .collect::<Vec<_>>();
    topics.sort_by(|left, right| left.topic_id.cmp(&right.topic_id));

    Ok(TopicsListOutput {
        database_path: database_path.to_string(),
        topics,
    })
}

fn topic_status_output(
    database_path: &str,
    topic_id: &str,
) -> Result<TopicStatusOutput, ExplorerError> {
    let pending_placement = load_pending_placements(database_path)?
        .into_iter()
        .find(|placement| placement_topic_id(placement) == topic_id)
        .map(JsonPendingPlacement);
    let status = if pending_placement.is_some() {
        "under_replicated"
    } else {
        "not_pending"
    };

    Ok(TopicStatusOutput {
        database_path: database_path.to_string(),
        topic_id: topic_id.to_string(),
        status,
        pending_placement,
    })
}

fn topic_placements_output(
    database_path: &str,
    topic_id: Option<&str>,
) -> Result<TopicPlacementsOutput, ExplorerError> {
    let mut placements = load_pending_placements(database_path)?;
    if let Some(topic_id) = topic_id {
        placements.retain(|placement| placement_topic_id(placement) == topic_id);
    }
    placements.sort_by_key(placement_topic_id);

    Ok(TopicPlacementsOutput {
        database_path: database_path.to_string(),
        topic_id: topic_id.map(str::to_string),
        placements: placements.into_iter().map(JsonPendingPlacement).collect(),
    })
}

fn load_pending_placements(
    database_path: &str,
) -> Result<Vec<PendingShardPlacement>, ExplorerError> {
    let db = OptimisticTxDatabase::builder(Path::new(database_path)).open()?;
    let keyspace_names = db.list_keyspace_names();
    if !keyspace_names
        .iter()
        .any(|name| name.as_ref() == SYNC_PLACEMENT_KEYSPACE)
    {
        return Ok(Vec::new());
    }

    let keyspace = db.keyspace(SYNC_PLACEMENT_KEYSPACE, KeyspaceCreateOptions::default)?;
    let snapshot = db.read_tx();
    let mut placements = Vec::new();
    for entry in snapshot.iter(&keyspace) {
        let (_, value) = entry.into_inner()?;
        placements.push(
            decode_placement(value.as_ref())
                .map_err(|error| ExplorerError::Decode(error.to_string()))?,
        );
    }
    Ok(placements)
}
#[cfg(test)]
mod tests {
    use super::super::present::{DecodedField, DecodedValue};
    use super::{list_entries, list_keyspaces, location_scan};
    use aruna_core::keyspaces::{
        BLOB_LOCATIONS_KEYSPACE, GROUP_KEYSPACE, KEYSPACE_CATALOG, STORAGE_BACKEND_KEYSPACE,
    };
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::group::Group;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::policy::attachment::{
        BULK_INTENT_KEYSPACE, BULK_RUN_KEYSPACE, POLICY_MUTATION_KEYSPACE,
    };
    use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
    use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn scan_location(backend: BackendRef) -> BackendLocation {
        BackendLocation {
            backend,
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "blob-bucket".to_string(),
            backend_path: "path/blob.bin".to_string(),
            ulid: Ulid::from_bytes([5_u8; 16]),
            compressed: false,
            encrypted: false,
            created_by: aruna_core::UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 11,
            hashes: HashMap::new(),
        }
    }
    #[test]
    fn reports_unknown_backend() {
        // A removed backend must be discoverable without a blob backend.
        let temp = tempdir().unwrap();
        let backends_path = temp.path().join("backends.toml");
        std::fs::write(
            &backends_path,
            "[backend.hot]\ntype = \"filesystem\"\nroot = \"/data/hot\"\nmultipart_bucket = \"parts\"\ndefault = true\n",
        )
        .unwrap();
        let group_id = Ulid::from_bytes([7_u8; 16]);
        {
            let db = OptimisticTxDatabase::builder(temp.path().join("db"))
                .open()
                .unwrap();
            let locations = db
                .keyspace(BLOB_LOCATIONS_KEYSPACE, KeyspaceCreateOptions::default)
                .unwrap();
            let group_backends = db
                .keyspace(STORAGE_BACKEND_KEYSPACE, KeyspaceCreateOptions::default)
                .unwrap();
            let mut txn = db.write_tx().unwrap();
            txn.insert(
                locations.clone(),
                vec![1_u8; 32],
                scan_location(BackendRef::Node("hot".to_string()))
                    .to_bytes()
                    .unwrap(),
            );
            txn.insert(
                locations.clone(),
                vec![2_u8; 32],
                scan_location(BackendRef::Node("gone".to_string()))
                    .to_bytes()
                    .unwrap(),
            );
            txn.insert(
                locations.clone(),
                vec![3_u8; 32],
                scan_location(BackendRef::Group(group_id))
                    .to_bytes()
                    .unwrap(),
            );
            txn.insert(
                locations,
                vec![4_u8; 32],
                scan_location(BackendRef::Group(Ulid::from_bytes([8_u8; 16])))
                    .to_bytes()
                    .unwrap(),
            );
            txn.insert(group_backends, group_id.to_bytes().to_vec(), vec![0_u8]);
            let _ = txn.commit().unwrap();
        }

        let output = location_scan(
            temp.path().join("db").to_str().unwrap(),
            backends_path.to_str(),
        )
        .unwrap();

        assert_eq!(output.scanned, 4);
        let named = output
            .unresolved
            .into_iter()
            .map(|entry| entry.backend)
            .collect::<Vec<_>>();
        assert_eq!(named.len(), 2);
        assert!(named.contains(&"node:gone".to_string()));
    }
    #[test]
    fn lists_sorted_keyspaces() {
        let temp = tempdir().unwrap();
        {
            let db = OptimisticTxDatabase::builder(temp.path()).open().unwrap();
            db.keyspace("zeta", KeyspaceCreateOptions::default).unwrap();
            db.keyspace("alpha", KeyspaceCreateOptions::default)
                .unwrap();
            db.keyspace(GROUP_KEYSPACE, KeyspaceCreateOptions::default)
                .unwrap();
        }

        let output = list_keyspaces(temp.path().to_str().unwrap()).unwrap();
        let names = output
            .keyspaces
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "alpha".to_string(),
                GROUP_KEYSPACE.to_string(),
                "zeta".to_string()
            ]
        );

        let missing = output
            .missing_keyspaces
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>();
        // The catalog plus the three bulk/mutation policy keyspaces is the
        // expectation; the created group keyspace is excluded.
        let mut expected_missing = KEYSPACE_CATALOG
            .iter()
            .copied()
            .chain([
                BULK_INTENT_KEYSPACE,
                BULK_RUN_KEYSPACE,
                POLICY_MUTATION_KEYSPACE,
            ])
            .filter(|name| *name != GROUP_KEYSPACE)
            .map(|name| name.to_string())
            .collect::<Vec<_>>();
        expected_missing.sort();
        assert_eq!(missing, expected_missing);
    }
    #[test]
    fn decodes_group_entries() {
        let temp = tempdir().unwrap();
        let group_id = Ulid::from_bytes([7_u8; 16]);
        let realm_id = RealmId::from_bytes([7_u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[9_u8; 32]).public(),
            user_id: aruna_core::UserId::local(Ulid::from_bytes([9_u8; 16]), realm_id),
            realm_id,
        };
        let group = Group {
            display_name: "Explorer Group".to_string(),
            group_id,
            realm_id,
            roles: Default::default(),
            owner: aruna_core::UserId::local(Ulid::from_bytes([10_u8; 16]), realm_id),
        };

        {
            let db = OptimisticTxDatabase::builder(temp.path()).open().unwrap();
            let keyspace = db
                .keyspace(GROUP_KEYSPACE, KeyspaceCreateOptions::default)
                .unwrap();
            let mut txn = db.write_tx().unwrap();
            txn.insert(
                keyspace,
                group_id.to_bytes().to_vec(),
                group.to_bytes(&actor).unwrap(),
            );
            let _ = txn.commit().unwrap();
        }

        let output = list_entries(temp.path().to_str().unwrap(), GROUP_KEYSPACE).unwrap();
        assert_eq!(output.entries.len(), 1);
        assert_eq!(
            output.entries[0].key,
            DecodedField::Ulid {
                value: group_id.to_string()
            }
        );
        match &output.entries[0].value {
            DecodedValue::Group { data } => assert_eq!(data.0.display_name, "Explorer Group"),
            other => panic!("expected group, got {other:?}"),
        }
    }
    #[test]
    fn missing_keyspace_errors() {
        let temp = tempdir().unwrap();
        let error = list_entries(temp.path().to_str().unwrap(), "missing").unwrap_err();
        assert!(error.to_string().contains("keyspace not found"));
    }
}
