//! Moves one metadata graph's Craqle sync cursor onto its topic's current genesis on a stopped
//! node, refusing while an actor published on both the stale and the current genesis.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::info_topics;
use crate::error::CliError;
use crate::explorer::ExplorerError;
use craqle::{GraphId, TopicCursorRepairAudit, VectorClock, topic_cursor_digest};
use fjall::OptimisticTxDatabase;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use irokle::{ActorClock, FjallStorage, OpId, Storage, TopicId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

const CURSOR_VERSION: u8 = 2;
const CURSOR_PREFIX: u8 = b'C';
const BINDING_PREFIX: u8 = b'T';
const AUDIT_PREFIX: u8 = b'A';
const TOPIC_DOMAIN: &str = "craqle-graph-topic-v1";
const META_PREFIX: u8 = b'M';
const CLOCK_PREFIX: u8 = b'K';
const TERM_DOMAIN: &[u8] = b"craqle-term/v1\0";

#[cfg(test)]
thread_local! {
    /// Runs right before the cursor write, so a test can check that both stores stay locked.
    static BEFORE_WRITE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

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
    /// Every actor in the graph's applied clock by highest sequence; all but this node's own block.
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
    let local = local_writer(storage)?;
    // Both stores stay open, and so locked, from validation through the durable write; the
    // locks also refuse while the node runs.
    let sync = FjallStorage::open(&sync_path)?;
    let (db, graphs) = open_graphs(&craqle_path)?;
    let topic = graph_topic(&graph);
    let applied = applied_dots(&db, &graphs, &graph)?;
    let bound = graphs.get(store_key(BINDING_PREFIX, &topic))?;
    if bound.as_deref() != Some(request.graph.as_bytes()) {
        return Err(ExplorerError::Refused(format!(
            "topic {topic} is not bound to graph {graph}"
        )));
    }
    let bytes = graphs
        .get(store_key(CURSOR_PREFIX, &topic))?
        .map(|value| value.to_vec())
        .ok_or_else(|| ExplorerError::Refused("graph has no applied sync cursor".to_string()))?;
    let stale = decode_cursor(&topic, &bytes)?;
    let view = sync.topic_view(&topic, None)?.ok_or_else(|| {
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
    // Every applied actor but this node's own unpublished writer can restart on the new genesis.
    let candidates: BTreeSet<[u8; 32]> = applied
        .0
        .keys()
        .map(|actor| *actor.as_bytes())
        .chain(stale.clock.iter().map(|(actor, _)| *actor.as_bytes()))
        .chain(view.clock.iter().map(|(actor, _)| *actor.as_bytes()))
        .chain(members.map(|actor| *actor.as_bytes()))
        .filter(|actor| actor != local.as_bytes())
        .collect();
    let overlap = actor_overlap(&candidates, &stale.clock, &applied, &view.clock);
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
    let audit = replace_cursor(&db, &graphs, &topic, &bytes, &replacement)?;
    drop((db, sync));
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
    let store = craqle_path.join("store");
    let not_craqle =
        || ExplorerError::Refused(format!("{} is not a Craqle store", store.display()));
    if !store.exists() {
        return Err(not_craqle());
    }
    let db = Database::builder(&store).open()?;
    // Opening a missing keyspace would create it, and an inspection must write nothing.
    if !db.keyspace_exists("graphs") || !db.keyspace_exists("terms") {
        return Err(not_craqle());
    }
    let graphs = db.keyspace("graphs", KeyspaceCreateOptions::default)?;
    Ok((db, graphs))
}

/// Compare-and-replace with an audit record, as Craqle's authorized cursor repair writes it.
/// That repair needs Craqle opened with sync, and its open-time reconcile fails on this cursor.
fn replace_cursor(
    db: &Database,
    graphs: &Keyspace,
    topic: &TopicId,
    old: &[u8],
    replacement: &[u8],
) -> Result<TopicCursorRepairAudit, ExplorerError> {
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
    #[cfg(test)]
    if let Some(hook) = BEFORE_WRITE.with(|hook| hook.borrow_mut().take()) {
        hook();
    }
    let mut batch = db.batch();
    batch.insert(graphs, key, replacement.to_vec());
    batch.insert(graphs, audit_key, audit_value);
    batch.commit()?;
    db.persist(PersistMode::SyncAll)?;
    Ok(audit)
}

/// The graph's applied clock, read from Craqle's keys: a node start would rebuild search,
/// repair diagnostics and settle validation work in the store.
fn applied_dots(
    db: &Database,
    graphs: &Keyspace,
    graph: &GraphId,
) -> Result<VectorClock, ExplorerError> {
    let term = format!("<{}>", graph.as_str());
    let mut hasher = blake3::Hasher::new();
    hasher.update(TERM_DOMAIN);
    hasher.update(term.as_bytes());
    let hash = hasher.finalize();
    let id = &hash.as_bytes()[..16];
    let graph_key = |prefix: u8| [&[prefix][..], id].concat();
    let terms = db.keyspace("terms", KeyspaceCreateOptions::default)?;
    if terms.get(id)?.as_deref() != Some(term.as_bytes())
        || graphs.get(graph_key(META_PREFIX))?.is_none()
    {
        return Err(ExplorerError::Refused(format!("graph {graph} not found")));
    }
    let clock = graphs
        .get(graph_key(CLOCK_PREFIX))?
        .ok_or_else(|| ExplorerError::Refused(format!("graph {graph} has no stored clock")))?;
    postcard::from_bytes(&clock)
        .map_err(|error| ExplorerError::Decode(format!("graph clock: {error}")))
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

/// An actor collides when the graph applied any of its records, or when both geneses have it,
/// since the current genesis keeps its actor id and restarts its sequences.
fn actor_overlap(
    candidates: &BTreeSet<[u8; 32]>,
    stale: &ActorClock,
    applied: &VectorClock,
    current: &ActorClock,
) -> Vec<ActorOverlap> {
    candidates
        .iter()
        .filter_map(|bytes| {
            let actor = irokle::ActorId::from_bytes(*bytes);
            let (stale_cursor, current) = (stale.get(&actor), current.get(&actor));
            let applied = applied_sequence(applied, &actor);
            let collides = applied > 0 || (stale_cursor > 0 && current > 0);
            collides.then_some(ActorOverlap {
                actor: hex::encode(bytes),
                stale_cursor,
                applied,
                current,
            })
        })
        .collect()
}

/// Aruna's own Craqle actor, the node id, which writes local metadata without publishing it.
fn local_writer(database_path: &Path) -> Result<craqle::ActorId, ExplorerError> {
    let db = OptimisticTxDatabase::builder(database_path).open()?;
    let state = info_topics::persisted_node_state(&db)?
        .ok_or_else(|| ExplorerError::Refused("node has no stored identity".to_string()))?;
    let node_id = iroh::SecretKey::from_bytes(&state.net_secret_key).public();
    Ok(craqle::ActorId::from_bytes(*node_id.as_bytes()))
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
        AllowAllAuthorizer, CraqleIrokleOptions, CraqleNode, CraqleOptions,
        CraqleRequestDurability, CreateCrateRequest, GraphPolicy, SearchStorage,
    };
    use irokle::{Ed25519Signer, Irokle, PeerId};
    use std::collections::BTreeSet;
    use tempfile::{TempDir, tempdir};

    const GRAPH: &str = "urn:test:doctor-repair-cursor";
    const REPLICA_SEED: u8 = 9;
    const NODE_SECRET: [u8; 32] = [21; 32];

    /// The Craqle actor Aruna writes local metadata with: the id of the node holding `NODE_SECRET`.
    fn node_writer() -> craqle::ActorId {
        let node_id = iroh::SecretKey::from_bytes(&NODE_SECRET).public();
        craqle::ActorId::from_bytes(*node_id.as_bytes())
    }

    /// Stores the node identity Aruna keeps beside the Craqle and sync stores.
    fn store_identity(path: &Path) {
        use aruna::identity::{
            BootOrigin, PersistedNodeIdentity, PersistedNodeState, PersistedNodeStatus,
        };
        use aruna_core::keyspaces::{NODE_STATE_KEY, NODE_STATE_KEYSPACE};
        let realm = aruna_core::structs::identity::realm::RealmId([3u8; 32]);
        let state = PersistedNodeState {
            boot_origin: BootOrigin::Onboarded,
            status: PersistedNodeStatus::PendingOnboarding,
            realm_id: realm,
            net_secret_key: NODE_SECRET,
            onboarding_phase: None,
            onboarding_sync_ticket: None,
            identity: PersistedNodeIdentity::User {
                owner: aruna_core::UserId::nil(realm),
            },
        };
        let value = postcard::to_allocvec(&state).unwrap();
        super::super::tests::write(path, NODE_STATE_KEYSPACE, vec![(NODE_STATE_KEY, value)]);
    }

    /// One `replaced_replica` scenario; the default is a safe repair of Aruna-style content.
    #[derive(Default)]
    struct Setup {
        /// One signer mints both geneses.
        shared_signer: bool,
        /// The first genesis carries a published crate the replica applies.
        first_writes: bool,
        /// The replica publishes once more and applies it before any reconcile.
        own_write: bool,
        /// The replica's local content is written by an actor that is not this node.
        stranger_writes: bool,
    }

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

    /// The replica's unpublished local crate, written as `actor`.
    fn create_local(node: &CraqleNode, actor: craqle::ActorId) {
        let request = CreateCrateRequest::new(
            GraphId::new(GRAPH),
            "first",
            "description",
            "2025-01-01",
            None,
            policy(),
        );
        let durability = CraqleRequestDurability::WalAlreadyDurable;
        node.create_crate_with_durability_as(&AllowAllAuthorizer, request, durability, Some(actor))
            .unwrap();
    }

    /// A stopped replica whose cursor names a genesis minted by seed 1, while its sync store now
    /// holds a second genesis for the same graph topic, shaped by `setup`.
    fn replaced_replica(setup: Setup) -> (TempDir, CursorRepair) {
        let temp = tempdir().unwrap();
        store_identity(temp.path());
        let second_seed = if setup.shared_signer { 1 } else { 2 };
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
        if setup.first_writes {
            create(&first_node, "first", CraqleRequestDurability::Durable);
        } else {
            let writer = if setup.stranger_writes {
                craqle::ActorId::from_bytes([77; 32])
            } else {
                node_writer()
            };
            create_local(&replica, writer);
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
        if setup.own_write {
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
        store_identity(temp.path());
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
        let (_temp, request) = replaced_replica(Setup::default());
        let output = repair_output(&request).unwrap();
        assert_ne!(output.stale.genesis, output.current.genesis);
        // The local unpublished writes are listed but belong to no topic actor.
        assert!(!output.stale.actors.is_empty() && !output.applied_actors.is_empty());
        assert!(output.overlap.is_empty() && !output.applied);
        let again = repair_output(&request).unwrap();
        assert_eq!(again.old_cursor_digest, output.old_cursor_digest);
    }

    /// Every key and value of the Craqle and sync stores, read without starting a node.
    fn store_state(path: &Path) -> BTreeMap<(String, Vec<u8>), Vec<u8>> {
        let mut state = BTreeMap::new();
        let craqle = Database::builder(path.join("craqle").join("store"))
            .open()
            .unwrap();
        for name in craqle.list_keyspace_names() {
            let keyspace = craqle
                .keyspace(&name, KeyspaceCreateOptions::default)
                .unwrap();
            for guard in keyspace.iter() {
                let (key, value) = guard.into_inner().unwrap();
                state.insert((format!("craqle {}", &*name), key.to_vec()), value.to_vec());
            }
        }
        drop(craqle);
        let sync = fjall::OptimisticTxDatabase::builder(path.join("document-sync"))
            .open()
            .unwrap();
        for name in sync.list_keyspace_names() {
            let keyspace = sync
                .inner()
                .keyspace(&name, KeyspaceCreateOptions::default)
                .unwrap();
            for guard in keyspace.iter() {
                let (key, value) = guard.into_inner().unwrap();
                state.insert((format!("sync {}", &*name), key.to_vec()), value.to_vec());
            }
        }
        state
    }

    /// A dry run starts no node, so it rebuilds, repairs or settles nothing in either store.
    #[test]
    fn dry_run_untouched() {
        let (temp, request) = replaced_replica(Setup::default());
        let before = store_state(temp.path());
        repair_output(&request).unwrap();
        assert_eq!(store_state(temp.path()), before);
        assert!(!temp.path().join("craqle").join("search").exists());
    }

    /// Both stores stay locked from validation through the write, so nothing changes between.
    #[test]
    fn holds_store_locks() {
        let (temp, mut request) = replaced_replica(Setup::default());
        let store = temp.path().join("craqle").join("store");
        let sync = temp.path().join("document-sync");
        let locked = std::rc::Rc::new(std::cell::Cell::new((false, false)));
        let seen = locked.clone();
        BEFORE_WRITE.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                let craqle = Database::builder(&store).open().is_err();
                seen.set((craqle, FjallStorage::open(&sync).is_err()));
            }));
        });
        request.apply = true;
        assert!(repair_output(&request).unwrap().applied);
        assert_eq!(locked.get(), (true, true), "craqle and sync store locks");
    }

    /// One signer minted both geneses, so its sequences collide and the cursor stays.
    #[test]
    fn refuses_shared_actor() {
        let (_temp, mut request) = replaced_replica(Setup {
            shared_signer: true,
            first_writes: true,
            ..Setup::default()
        });
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
        let (_temp, mut request) = replaced_replica(Setup {
            first_writes: true,
            ..Setup::default()
        });
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
        let (_temp, mut request) = replaced_replica(Setup {
            own_write: true,
            ..Setup::default()
        });
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

    /// Local content written by an actor that is not this node could be an old peer's record
    /// a crash left applied but unconsumed; only this node's own writer is exempt.
    #[test]
    fn refuses_unknown_actor() {
        let (_temp, mut request) = replaced_replica(Setup {
            stranger_writes: true,
            ..Setup::default()
        });
        let output = repair_output(&request).unwrap();
        let stranger = hex::encode([77; 32]);
        let [overlap] = output.overlap.as_slice() else {
            panic!("expected the stranger, got {:?}", output.overlap);
        };
        assert_eq!(overlap.actor, stranger);
        assert!(overlap.applied > 0 && overlap.stale_cursor == 0 && overlap.current == 0);
        request.apply = true;
        let error = repair_output(&request).unwrap_err();
        assert!(matches!(error, ExplorerError::Refused(_)));
    }

    /// After the repair Craqle opens with sync again, and the kept graph takes the new genesis.
    #[test]
    fn apply_repairs_cursor() {
        let (temp, mut request) = replaced_replica(Setup::default());
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
        let (temp, mut request) = replaced_replica(Setup::default());
        let craqle_path = temp.path().join("craqle");
        let running = CraqleNode::open_with_options(&craqle_path, CraqleOptions::new()).unwrap();
        request.apply = true;
        assert!(repair_output(&request).is_err());
        drop(running);
        request.apply = false;
        assert!(repair_output(&request).is_ok_and(|output| !output.applied));
    }
}
