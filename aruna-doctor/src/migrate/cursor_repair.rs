//! Moves one metadata graph's Craqle sync cursor onto its topic's current genesis on a stopped
//! node, refusing while an actor published on both the stale and the current genesis.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::CliError;
use crate::explorer::ExplorerError;
use craqle::{
    CraqleNode, CraqleOptions, GraphId, TopicCursorRepairAudit, VectorClock, topic_cursor_digest,
};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use irokle::{ActorClock, FjallStorage, OpId, Storage, TopicId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const CURSOR_VERSION: u8 = 2;
const CURSOR_PREFIX: u8 = b'C';
const BINDING_PREFIX: u8 = b'T';
const AUDIT_PREFIX: u8 = b'A';
const TOPIC_DOMAIN: &str = "craqle-graph-topic-v1";

/// Craqle's version 2 applied topic cursor, checked by version, topic and checksum.
#[derive(Serialize, Deserialize)]
struct CursorPayload {
    version: u8,
    topic: TopicId,
    epoch: u64,
    genesis: OpId,
    clock: ActorClock,
    target: Option<ActorClock>,
}

#[derive(Serialize, Deserialize)]
struct CursorEnvelope {
    payload: CursorPayload,
    checksum: [u8; 32],
}

/// One `metadata repair-cursor` invocation.
#[derive(Debug)]
pub struct CursorRepair {
    pub database_path: String,
    pub graph: String,
    /// Craqle store; defaults to `<database_path>/craqle`.
    pub craqle_path: Option<String>,
    /// Document sync store; defaults to `<database_path>/document-sync`.
    pub sync_path: Option<String>,
    pub apply: bool,
}

#[derive(Debug, Serialize)]
pub struct RepairOutput {
    graph: String,
    topic_id: String,
    stale: BranchView,
    current: BranchView,
    /// Every actor in the graph's applied clock by highest sequence; topic actors among them block.
    applied_actors: BTreeMap<String, u64>,
    /// Actors whose sequences can collide across the geneses, so a replay could skip records.
    overlap: Vec<ActorOverlap>,
    old_cursor_digest: String,
    replacement_cursor_digest: Option<String>,
    applied: bool,
}

#[derive(Debug, Serialize)]
struct BranchView {
    epoch: u64,
    genesis: String,
    actors: BTreeMap<String, u64>,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct ActorOverlap {
    actor: String,
    stale_cursor: u64,
    applied: u64,
    current: u64,
}

pub async fn repair_cursor(request: CursorRepair) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking(move || repair_output(&request))
        .await
        .map_err(std::io::Error::other)??;
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn repair_output(request: &CursorRepair) -> Result<RepairOutput, ExplorerError> {
    let storage = Path::new(&request.database_path);
    let craqle_path = request
        .craqle_path
        .as_ref()
        .map_or_else(|| storage.join("craqle"), PathBuf::from);
    let sync_path = request
        .sync_path
        .as_ref()
        .map_or_else(|| storage.join("document-sync"), PathBuf::from);
    for path in [&craqle_path, &sync_path] {
        if !path.exists() {
            return Err(ExplorerError::Refused(format!(
                "store {} not found",
                path.display()
            )));
        }
    }
    let graph = GraphId::new(&request.graph);
    // Each store is opened in turn; opening takes its lock, so this refuses while the node runs.
    let applied = applied_dots(&craqle_path, &graph)?;
    let topic = graph_topic(&graph);
    let bytes = {
        let (_db, graphs) = open_graphs(&craqle_path)?;
        let bound = graphs.get(store_key(BINDING_PREFIX, &topic))?;
        if bound.as_deref() != Some(request.graph.as_bytes()) {
            return Err(ExplorerError::Refused(format!(
                "topic {topic} is not bound to graph {graph}"
            )));
        }
        graphs
            .get(store_key(CURSOR_PREFIX, &topic))?
            .map(|value| value.to_vec())
            .ok_or_else(|| ExplorerError::Refused("graph has no applied sync cursor".to_string()))?
    };
    let stale = decode_cursor(&topic, &bytes)?;
    let view = FjallStorage::open(&sync_path)?
        .topic_view(&topic, None)?
        .ok_or_else(|| {
            ExplorerError::Refused(format!("topic {topic} is not in the local sync store"))
        })?;
    if stale.genesis == view.state.genesis {
        let reason = if stale.epoch == view.epoch {
            "the cursor already names the current topic branch"
        } else {
            // A reset clears actor tips, so a rejoin can bring back truncated history.
            "the topic kept the same genesis after a reset; a rejoin can reuse applied sequences, \
             and this command only repairs a replaced genesis"
        };
        return Err(ExplorerError::Refused(reason.to_string()));
    }
    // Members that never published still restart their sequences on the current genesis.
    let members = view
        .state
        .members
        .iter()
        .chain(view.state.membership_controls.keys())
        .map(|peer| irokle::actor_id_for(topic, *peer));
    let overlap = actor_overlap(&stale.clock, &applied, &view.clock, members);
    let applied_actors = applied
        .0
        .iter()
        .map(|(actor, sequence)| (hex::encode(actor.as_bytes()), *sequence))
        .collect();
    let mut output = RepairOutput {
        graph: request.graph.clone(),
        topic_id: topic.to_string(),
        stale: branch_view(stale.epoch, &stale.genesis, &stale.clock),
        current: branch_view(view.epoch, &view.state.genesis, &view.clock),
        applied_actors,
        overlap,
        old_cursor_digest: hex::encode(topic_cursor_digest(&bytes)),
        replacement_cursor_digest: None,
        applied: false,
    };
    if !request.apply {
        return Ok(output);
    }
    if !output.overlap.is_empty() {
        return Err(ExplorerError::Refused(format!(
            "actor sequences can collide across the geneses, so a replay could skip records: {}",
            serde_json::to_string(&output.overlap).unwrap_or_default()
        )));
    }
    let replacement = encode_cursor(CursorPayload {
        version: CURSOR_VERSION,
        topic,
        epoch: view.epoch,
        genesis: view.state.genesis,
        clock: ActorClock::default(),
        target: None,
    })?;
    let audit = replace_cursor(&craqle_path, &topic, &bytes, &replacement)?;
    output.replacement_cursor_digest = Some(hex::encode(audit.replacement_cursor_digest));
    output.applied = true;
    Ok(output)
}

/// Craqle's deterministic topic id for a graph IRI.
fn graph_topic(graph: &GraphId) -> TopicId {
    let mut hasher = blake3::Hasher::new_derive_key(TOPIC_DOMAIN);
    hasher.update(graph.as_str().as_bytes());
    TopicId::from_bytes(*hasher.finalize().as_bytes())
}

fn store_key(prefix: u8, topic: &TopicId) -> [u8; 33] {
    let mut key = [prefix; 33];
    key[1..].copy_from_slice(topic.as_bytes());
    key
}

fn open_graphs(craqle_path: &Path) -> Result<(Database, Keyspace), ExplorerError> {
    let db = Database::builder(craqle_path.join("store")).open()?;
    let graphs = db.keyspace("graphs", KeyspaceCreateOptions::default)?;
    Ok((db, graphs))
}

/// Compare-and-replace with an audit record, as Craqle's authorized cursor repair writes it.
/// That repair needs Craqle opened with sync, and its open-time reconcile fails on this cursor.
fn replace_cursor(
    craqle_path: &Path,
    topic: &TopicId,
    old: &[u8],
    replacement: &[u8],
) -> Result<TopicCursorRepairAudit, ExplorerError> {
    let (db, graphs) = open_graphs(craqle_path)?;
    let key = store_key(CURSOR_PREFIX, topic);
    if graphs.get(key)?.as_deref() != Some(old) {
        return Err(ExplorerError::Refused(
            "the cursor changed since it was read".to_string(),
        ));
    }
    let audit = TopicCursorRepairAudit {
        topic: *topic,
        old_cursor_digest: topic_cursor_digest(old),
        replacement_cursor_digest: topic_cursor_digest(replacement),
        repaired_at_unix_nanos: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX),
    };
    let mut audit_key = vec![AUDIT_PREFIX];
    audit_key.extend_from_slice(topic.as_bytes());
    audit_key.extend_from_slice(&audit.old_cursor_digest);
    audit_key.extend_from_slice(&audit.replacement_cursor_digest);
    let audit_value = postcard::to_allocvec(&audit)
        .map_err(|error| ExplorerError::Decode(format!("cursor audit: {error}")))?;
    let mut batch = db.batch();
    batch.insert(&graphs, key, replacement.to_vec());
    batch.insert(&graphs, audit_key, audit_value);
    batch.commit()?;
    db.persist(PersistMode::SyncAll)?;
    Ok(audit)
}

/// The dots the graph applied, read through Craqle without sync, which skips reconciliation.
fn applied_dots(craqle_path: &Path, graph: &GraphId) -> Result<VectorClock, ExplorerError> {
    let node = CraqleNode::open_with_options(craqle_path, CraqleOptions::new())?;
    if !node.contains_graph(graph)? {
        return Err(ExplorerError::Refused(format!("graph {graph} not found")));
    }
    Ok(node.vector_clock(graph)?)
}

fn encode_cursor(payload: CursorPayload) -> Result<Vec<u8>, ExplorerError> {
    let bytes = postcard::to_allocvec(&payload)
        .map_err(|error| ExplorerError::Decode(format!("applied cursor: {error}")))?;
    let checksum = *blake3::hash(&bytes).as_bytes();
    postcard::to_allocvec(&CursorEnvelope { payload, checksum })
        .map_err(|error| ExplorerError::Decode(format!("applied cursor: {error}")))
}

fn decode_cursor(topic: &TopicId, bytes: &[u8]) -> Result<CursorPayload, ExplorerError> {
    let envelope: CursorEnvelope = postcard::from_bytes(bytes)
        .map_err(|error| ExplorerError::Decode(format!("applied cursor: {error}")))?;
    let payload = postcard::to_allocvec(&envelope.payload)
        .map_err(|error| ExplorerError::Decode(format!("applied cursor: {error}")))?;
    if envelope.payload.version != CURSOR_VERSION
        || envelope.payload.topic != *topic
        || envelope.checksum != *blake3::hash(&payload).as_bytes()
    {
        return Err(ExplorerError::Refused(
            "applied cursor has another version, another topic or a bad checksum".to_string(),
        ));
    }
    Ok(envelope.payload)
}

/// A topic actor collides when the graph applied any of its records, or when both geneses have
/// it, since the current genesis keeps its actor id and restarts its sequences.
fn actor_overlap(
    stale: &ActorClock,
    applied: &VectorClock,
    current: &ActorClock,
    members: impl Iterator<Item = irokle::ActorId>,
) -> Vec<ActorOverlap> {
    let actors: BTreeMap<String, irokle::ActorId> = stale
        .iter()
        .chain(current.iter())
        .map(|(actor, _)| *actor)
        .chain(members)
        .map(|actor| (hex::encode(actor.as_bytes()), actor))
        .collect();
    actors
        .into_iter()
        .filter_map(|(name, actor)| {
            let (stale_cursor, current) = (stale.get(&actor), current.get(&actor));
            let applied = applied_sequence(applied, &actor);
            let collides = applied > 0 || (stale_cursor > 0 && current > 0);
            collides.then_some(ActorOverlap {
                actor: name,
                stale_cursor,
                applied,
                current,
            })
        })
        .collect()
}

fn applied_sequence(applied: &VectorClock, actor: &irokle::ActorId) -> u64 {
    applied
        .0
        .get(&craqle::ActorId::from_bytes(*actor.as_bytes()))
        .copied()
        .unwrap_or(0)
}

fn branch_view(epoch: u64, genesis: &OpId, clock: &ActorClock) -> BranchView {
    BranchView {
        epoch,
        genesis: hex::encode(genesis.as_bytes()),
        actors: clock
            .iter()
            .map(|(actor, sequence)| (hex::encode(actor.as_bytes()), *sequence))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use craqle::{
        AllowAllAuthorizer, CraqleIrokleOptions, CraqleRequestDurability, CreateCrateRequest,
        GraphPolicy, SearchStorage,
    };
    use irokle::{Ed25519Signer, Irokle, PeerId};
    use std::collections::BTreeSet;
    use tempfile::{TempDir, tempdir};

    const GRAPH: &str = "urn:test:doctor-repair-cursor";
    const REPLICA_SEED: u8 = 9;

    fn signed(seed: u8) -> Irokle {
        Irokle::builder()
            .with_signer(Ed25519Signer::from_bytes(&[seed; 32]))
            .build()
            .unwrap()
    }

    fn replica_irokle(path: &Path) -> Irokle<FjallStorage> {
        Irokle::builder()
            .with_signer(Ed25519Signer::from_bytes(&[REPLICA_SEED; 32]))
            .with_fjall_path(path.join("document-sync"))
            .unwrap()
            .build()
            .unwrap()
    }

    fn member<S: Storage>(path: &Path, irokle: &Irokle<S>, peers: &[PeerId]) -> CraqleNode {
        let sync = CraqleIrokleOptions::new().with_initial_peers(peers.iter().copied());
        let options = CraqleOptions::new()
            .with_search_storage(SearchStorage::Memory)
            .with_irokle(irokle.clone(), sync);
        CraqleNode::open_with_options(path, options).unwrap()
    }

    fn push<A: Storage, B: Storage>(from: &Irokle<A>, to: &Irokle<B>, topic: TopicId) {
        let summary = to.sync_summary(topic).unwrap();
        let data = from.plan_sync_data(to.peer_id(), &summary).unwrap();
        let ack = to.receive_sync_data_from(from.peer_id(), data).unwrap();
        let _ = from.apply_sync_ack(&ack.0);
    }

    fn policy() -> GraphPolicy {
        GraphPolicy {
            public: true,
            permission_paths: vec!["/t/x".to_string()],
        }
    }

    fn create(node: &CraqleNode, name: &str, durability: CraqleRequestDurability) {
        let request = CreateCrateRequest::new(
            GraphId::new(GRAPH),
            name,
            "description",
            "2025-01-01",
            None,
            policy(),
        );
        node.create_crate_with_durability(&AllowAllAuthorizer, request, durability)
            .unwrap();
    }

    /// A stopped replica whose cursor names a genesis minted by seed 1, while its sync store now
    /// holds a second genesis minted by `second_seed`. Without `first_writes` the replica's
    /// content is local and unpublished, as Aruna writes it, and the first genesis has a policy.
    /// With `own_write` the replica publishes once more and applies it before any reconcile.
    fn replaced_replica(
        second_seed: u8,
        first_writes: bool,
        own_write: bool,
    ) -> (TempDir, CursorRepair) {
        let temp = tempdir().unwrap();
        let (first, second) = (signed(1), signed(second_seed));
        let replica_sync = replica_irokle(temp.path());
        let replica_id = replica_sync.peer_id();
        let other = signed(8).peer_id();
        let first_node = member(&temp.path().join("first"), &first, &[replica_id]);
        let second_node = member(&temp.path().join("second"), &second, &[replica_id, other]);
        let replica = member(
            &temp.path().join("craqle"),
            &replica_sync,
            &[first.peer_id()],
        );
        let graph = GraphId::new(GRAPH);
        if first_writes {
            create(&first_node, "first", CraqleRequestDurability::Durable);
        } else {
            create(
                &replica,
                "first",
                CraqleRequestDurability::WalAlreadyDurable,
            );
            first_node
                .mint_irokle_topic(&graph, BTreeSet::from([replica_id]))
                .unwrap();
            first_node
                .set_graph_policy(&AllowAllAuthorizer, &graph, policy())
                .unwrap();
        }
        create(&second_node, "second", CraqleRequestDurability::Durable);
        second_node
            .add_data_entity(
                &AllowAllAuthorizer,
                &graph,
                "later.csv",
                "File",
                "on second",
            )
            .unwrap();
        let topic = first_node
            .irokle_topic_id(&GraphId::new(GRAPH))
            .unwrap()
            .unwrap();
        push(&first, &replica_sync, topic);
        replica.reconcile_irokle().unwrap();
        if own_write {
            replica
                .add_data_entity(&AllowAllAuthorizer, &graph, "own.csv", "File", "own")
                .unwrap();
        }
        replica_sync.storage().reset_topic(&topic).unwrap();
        push(&second, &replica_sync, topic);
        let request = dry_request(temp.path());
        (temp, request)
    }

    fn dry_request(path: &Path) -> CursorRepair {
        CursorRepair {
            database_path: path.display().to_string(),
            graph: GRAPH.to_string(),
            craqle_path: None,
            sync_path: None,
            apply: false,
        }
    }

    /// A stopped replica whose topic was reset and rejoined under the same genesis.
    fn rejoined_replica() -> (TempDir, CursorRepair) {
        let temp = tempdir().unwrap();
        let first = signed(1);
        let replica_sync = replica_irokle(temp.path());
        let first_node = member(
            &temp.path().join("first"),
            &first,
            &[replica_sync.peer_id()],
        );
        let replica = member(
            &temp.path().join("craqle"),
            &replica_sync,
            &[first.peer_id()],
        );
        create(&first_node, "first", CraqleRequestDurability::Durable);
        let topic = first_node
            .irokle_topic_id(&GraphId::new(GRAPH))
            .unwrap()
            .unwrap();
        push(&first, &replica_sync, topic);
        replica.reconcile_irokle().unwrap();
        replica_sync.storage().reset_topic(&topic).unwrap();
        push(&first, &replica_sync, topic);
        let request = dry_request(temp.path());
        (temp, request)
    }

    /// A reset under the same genesis can rejoin truncated history, so it is refused outright.
    #[test]
    fn refuses_same_genesis() {
        let (_temp, request) = rejoined_replica();
        let error = repair_output(&request).unwrap_err();
        assert!(
            matches!(&error, ExplorerError::Refused(reason) if reason.contains("same genesis")),
            "{error}"
        );
    }

    #[test]
    fn dry_run_reports() {
        let (_temp, request) = replaced_replica(2, false, false);
        let output = repair_output(&request).unwrap();
        assert_ne!(output.stale.genesis, output.current.genesis);
        // The local unpublished writes are listed but belong to no topic actor.
        assert!(!output.stale.actors.is_empty() && !output.applied_actors.is_empty());
        assert!(output.overlap.is_empty() && !output.applied);
        let again = repair_output(&request).unwrap();
        assert_eq!(again.old_cursor_digest, output.old_cursor_digest);
    }

    /// One signer minted both geneses, so its sequences collide and the cursor stays.
    #[test]
    fn refuses_shared_actor() {
        let (_temp, mut request) = replaced_replica(1, true, false);
        let output = repair_output(&request).unwrap();
        let [overlap] = output.overlap.as_slice() else {
            panic!("expected one shared actor, got {:?}", output.overlap);
        };
        assert!(overlap.stale_cursor > 0 && overlap.current > 0);
        request.apply = true;
        let error = repair_output(&request).unwrap_err();
        assert!(matches!(error, ExplorerError::Refused(_)));
        request.apply = false;
        let after = repair_output(&request).unwrap();
        assert_eq!(after.old_cursor_digest, output.old_cursor_digest);
    }

    /// The graph applied an old-genesis actor that has not published on the current genesis
    /// yet; its later records there would reuse those sequences, so the cursor stays.
    #[test]
    fn refuses_applied_actor() {
        let (_temp, mut request) = replaced_replica(2, true, false);
        let output = repair_output(&request).unwrap();
        let [overlap] = output.overlap.as_slice() else {
            panic!("expected one applied actor, got {:?}", output.overlap);
        };
        assert!(overlap.stale_cursor > 0 && overlap.applied > 0 && overlap.current == 0);
        assert_eq!(
            output.applied_actors.get(&overlap.actor),
            Some(&overlap.applied)
        );
        request.apply = true;
        let error = repair_output(&request).unwrap_err();
        assert!(matches!(error, ExplorerError::Refused(_)));
        request.apply = false;
        let after = repair_output(&request).unwrap();
        assert_eq!(after.old_cursor_digest, output.old_cursor_digest);
    }

    /// The replica applied its own record before the cursor took it, then rejoined a genesis it
    /// has not published on; its actor is in neither clock but its sequences would restart.
    #[test]
    fn refuses_member_actor() {
        let (_temp, mut request) = replaced_replica(2, false, true);
        let output = repair_output(&request).unwrap();
        let [overlap] = output.overlap.as_slice() else {
            panic!("expected the replica's actor, got {:?}", output.overlap);
        };
        assert!(overlap.applied > 0 && overlap.stale_cursor == 0 && overlap.current == 0);
        assert_eq!(
            output.applied_actors.get(&overlap.actor),
            Some(&overlap.applied)
        );
        request.apply = true;
        let error = repair_output(&request).unwrap_err();
        assert!(matches!(error, ExplorerError::Refused(_)));
    }

    /// After the repair Craqle opens with sync again, and the kept graph takes the new genesis.
    #[test]
    fn apply_repairs_cursor() {
        let (temp, mut request) = replaced_replica(2, false, false);
        request.apply = true;
        let output = repair_output(&request).unwrap();
        assert!(output.applied && output.replacement_cursor_digest.is_some());
        let error = repair_output(&request).unwrap_err();
        assert!(
            matches!(error, ExplorerError::Refused(_)),
            "a repeat changes nothing"
        );

        let replica_sync = replica_irokle(temp.path());
        let replica = member(&temp.path().join("craqle"), &replica_sync, &[]);
        replica.reconcile_irokle().unwrap();
        let graph = GraphId::new(GRAPH);
        let export = replica.export_rocrate(&AllowAllAuthorizer, &graph).unwrap();
        assert!(
            export.contains("first") && export.contains("on second"),
            "{export}"
        );
    }

    #[test]
    fn refuses_running_node() {
        let (temp, mut request) = replaced_replica(2, false, false);
        let craqle_path = temp.path().join("craqle");
        let running = CraqleNode::open_with_options(&craqle_path, CraqleOptions::new()).unwrap();
        request.apply = true;
        assert!(repair_output(&request).is_err());
        drop(running);
        request.apply = false;
        assert!(repair_output(&request).is_ok_and(|output| !output.applied));
    }
}
