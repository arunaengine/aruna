//! Moves one metadata graph's Craqle sync cursor onto its topic's current genesis on a stopped
//! node, refusing while an actor published on both the stale and the current genesis.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::info_topics;
use crate::error::CliError;
use crate::explorer::ExplorerError;
use aruna_core::keyspaces::EVENT_LOG_KEYSPACE;
use aruna_core::metadata::{
    MetadataActor, MetadataEventRecord, deterministic_materialization_actor,
};
use aruna_core::storage_entries::{event_log_key, event_log_prefix};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use craqle::{
    GraphId, GraphPolicy, PolicyTag, TopicCursorRepairAudit, VectorClock, topic_cursor_digest,
};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use fjall::{OptimisticTxDatabase, Readable};
use irokle::{ActorClock, FjallStorage, OpId, Storage, TopicId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use ulid::Ulid;

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

/// Craqle's stored graph metadata, field for field; its clock is the fallback clock.
#[derive(Serialize, Deserialize)]
struct GraphMeta {
    policy: GraphPolicy,
    policy_tag: PolicyTag,
    clock: VectorClock,
    irokle_topic: Option<[u8; 32]>,
    rocrate_context: Option<String>,
    rocrate_license: Option<String>,
    rocrate_license_digest: Option<[u8; 32]>,
    context_tag: ContextTag,
}

/// Craqle's ordering tag for stored RO-Crate render hints.
#[derive(Serialize, Deserialize)]
struct ContextTag {
    counter: u64,
    actor: craqle::ActorId,
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
    /// Every actor in the graph's applied clock; all but identified local writers block.
    applied_actors: BTreeMap<String, AppliedActor>,
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

#[derive(Debug, Serialize)]
struct AppliedActor {
    sequence: u64,
    /// How the event log or node identity names this local writer, or "unidentified".
    origin: String,
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
    // The document id names the event log that identifies the graph's local writers.
    let document_id = MetadataRegistryRecord::graph_document(&request.graph)
        .filter(|document| MetadataRegistryRecord::graph_iri_for(*document) == request.graph)
        .ok_or_else(|| {
            ExplorerError::Refused(format!(
                "{} is not an Aruna metadata graph IRI",
                request.graph
            ))
        })?;
    let storage = Path::new(&request.database_path);
    let craqle_path = request
        .craqle_path
        .as_ref()
        .map_or_else(|| storage.join("craqle"), PathBuf::from);
    let sync_path = request
        .sync_path
        .as_ref()
        .map_or_else(|| storage.join("document-sync"), PathBuf::from);
    // Opening a path that holds no database would create one, and an inspection writes nothing.
    for path in [
        storage.to_path_buf(),
        craqle_path.join("store"),
        sync_path.clone(),
    ] {
        if !fjall_database(&path) {
            return Err(ExplorerError::Refused(format!(
                "{} is not an existing database",
                path.display()
            )));
        }
    }
    let graph = GraphId::new(&request.graph);
    let writers = local_writers(storage, document_id)?;
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
        .filter(|actor| !writers.contains_key(actor))
        .collect();
    let overlap = actor_overlap(&candidates, &stale.clock, &applied, &view.clock);
    let applied_actors = applied
        .0
        .iter()
        .map(|(actor, sequence)| {
            let origin = writers
                .get(actor.as_bytes())
                .map_or("unidentified", String::as_str);
            let entry = AppliedActor {
                sequence: *sequence,
                origin: origin.to_string(),
            };
            (hex::encode(actor.as_bytes()), entry)
        })
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
    let known = terms.get(id)?.as_deref() == Some(term.as_bytes());
    let Some(meta) = graphs.get(graph_key(META_PREFIX))?.filter(|_| known) else {
        return Err(ExplorerError::Refused(format!("graph {graph} not found")));
    };
    match graphs.get(graph_key(CLOCK_PREFIX))? {
        Some(clock) => postcard::from_bytes(&clock)
            .map_err(|error| ExplorerError::Decode(format!("graph clock: {error}"))),
        // Craqle reads this fallback until a data mutation writes the separate clock key.
        None => metadata_clock(&meta),
    }
}

/// The clock inside Craqle's stored graph metadata; any other length is malformed.
fn metadata_clock(bytes: &[u8]) -> Result<VectorClock, ExplorerError> {
    let malformed = |reason: String| ExplorerError::Decode(format!("graph metadata: {reason}"));
    let (meta, rest) = postcard::take_from_bytes::<GraphMeta>(bytes)
        .map_err(|error| malformed(error.to_string()))?;
    if !rest.is_empty() {
        return Err(malformed(format!("{} trailing bytes", rest.len())));
    }
    Ok(meta.clock)
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

/// Fjall recovers a database only where its version marker exists and creates one anywhere else.
fn fjall_database(path: &Path) -> bool {
    path.join("version").is_file()
}

/// The actors Aruna writes this document's graph with locally, each with its derivation: the
/// node id, and per logged event its materialization actor and the document actor its node
/// starts there. These are domain-separated hashes, so none is an Irokle topic actor.
fn local_writers(
    database_path: &Path,
    document_id: Ulid,
) -> Result<BTreeMap<[u8; 32], String>, ExplorerError> {
    let db = OptimisticTxDatabase::builder(database_path).open()?;
    let state = info_topics::persisted_node_state(&db)?
        .ok_or_else(|| ExplorerError::Refused("node has no stored identity".to_string()))?;
    let node_id = iroh::SecretKey::from_bytes(&state.net_secret_key).public();
    let mut writers = BTreeMap::from([(*node_id.as_bytes(), "this node".to_string())]);
    // Opening a missing keyspace would create it.
    if !db.keyspace_exists(EVENT_LOG_KEYSPACE) {
        return Ok(writers);
    }
    let events = db.keyspace(EVENT_LOG_KEYSPACE, KeyspaceCreateOptions::default)?;
    let read = db.read_tx();
    for entry in read.prefix(&events, event_log_prefix(document_id)) {
        let (key, value) = entry.into_inner()?;
        let event: MetadataEventRecord = postcard::from_bytes(&value)
            .map_err(|error| ExplorerError::Decode(format!("metadata event: {error}")))?;
        let id = event.event_id;
        if key.as_ref() != event_log_key(document_id, id).as_ref() {
            return Err(ExplorerError::Decode(format!(
                "metadata event {id} is stored under another key"
            )));
        }
        writers
            .entry(deterministic_materialization_actor(id))
            .or_insert_with(|| format!("materialization of event {id}"));
        if let Some(actor) = MetadataActor::next(None, document_id, event.node_id, id) {
            writers.entry(actor.actor).or_insert_with(|| {
                format!("document actor of node {} from event {id}", event.node_id)
            });
        }
    }
    Ok(writers)
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
    use aruna_core::metadata::{MetadataBatch, MetadataBatchSource, MetadataEventPayload};
    use craqle::{
        AllowAllAuthorizer, CraqleIrokleOptions, CraqleNode, CraqleOptions,
        CraqleRequestDurability, CreateCrateRequest, GraphPolicy, SearchStorage,
    };
    use irokle::{Ed25519Signer, Irokle, PeerId};
    use std::collections::BTreeSet;
    use tempfile::{TempDir, tempdir};

    const GRAPH: &str = "https://w3id.org/aruna/0000000000000000000000001A";
    const REPLICA_SEED: u8 = 9;
    const NODE_SECRET: [u8; 32] = [21; 32];
    const OTHER_SECRET: [u8; 32] = [31; 32];
    const DEVICE_ACTOR: [u8; 32] = [88; 32];
    const EVENT_ONE: Ulid = Ulid(101);
    const EVENT_TWO: Ulid = Ulid(102);

    fn document() -> Ulid {
        MetadataRegistryRecord::graph_document(GRAPH).unwrap()
    }

    /// Logs one event of the document, as every holder keeps it in its event log.
    fn log_event(
        path: &Path,
        event_id: Ulid,
        node_id: iroh::PublicKey,
        payload: MetadataEventPayload,
    ) {
        use aruna_core::structs::placement::record::PlacementRef;
        let realm_id = aruna_core::structs::identity::realm::RealmId([3u8; 32]);
        let event = MetadataEventRecord {
            event_id,
            record: MetadataRegistryRecord {
                realm_id,
                group_id: Ulid(1),
                document_id: document(),
                document_path: "datasets/repair".into(),
                graph_iri: GRAPH.to_string(),
                public: false,
                permission_path: String::new(),
                placement: PlacementRef::NIL,
                holder_node_ids: vec![node_id],
                created_at_ms: 1,
                updated_at_ms: 1,
                establishing_event_id: event_id,
                last_event_id: event_id,
            },
            user_id: aruna_core::UserId::nil(realm_id),
            node_id,
            payload,
            occurred_at_ms: 1,
        };
        let (_, key, value) = aruna_core::storage_entries::create_event_entry(&event).unwrap();
        super::super::tests::write(
            path,
            EVENT_LOG_KEYSPACE,
            vec![(key.as_ref(), value.to_vec())],
        );
    }

    /// One local, unpublished data entity written as `actor`.
    fn patch_local(node: &CraqleNode, actor: [u8; 32]) {
        let entity = craqle::CreateEntityRequest {
            graph: GraphId::new(GRAPH),
            entity_id: "local.csv".to_string(),
            entity_type: "File".to_string(),
            name: "local".to_string(),
            additional_triples: Vec::new(),
        };
        let request = craqle::PatchEntityRequest {
            entity,
            replaced_predicates: Vec::new(),
        };
        let durability = CraqleRequestDurability::WalAlreadyDurable;
        let actor = Some(craqle::ActorId::from_bytes(actor));
        node.patch_data_with(&AllowAllAuthorizer, request, durability, actor)
            .unwrap();
    }

    fn other_node() -> iroh::PublicKey {
        iroh::SecretKey::from_bytes(&OTHER_SECRET).public()
    }

    /// The document actor another node starts at `EVENT_TWO`.
    fn other_actor() -> [u8; 32] {
        MetadataActor::next(None, document(), other_node(), EVENT_TWO)
            .unwrap()
            .actor
    }

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
        /// The replica's graph only has a local policy, so Craqle wrote no separate clock key.
        policy_only: bool,
        /// The storage root has no node database with an identity.
        without_identity: bool,
        /// The replica's content comes from a materialized event and another node's first batch.
        aruna_writers: bool,
        /// The replica merged a batch whose actor a device chose.
        device_writer: bool,
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

    fn scaffold() -> MetadataEventPayload {
        MetadataEventPayload::Scaffold {
            name: "first".to_string(),
            description: "description".to_string(),
            date_published: "2025-01-01".to_string(),
            license: None,
        }
    }

    /// A batch a device planned under an actor it chose itself.
    fn device_batch() -> MetadataEventPayload {
        let source = MetadataBatchSource::UpsertDataEntity {
            jsonld: "{}".to_string(),
        };
        MetadataEventPayload::ApplyBatch {
            batch: MetadataBatch {
                graph_iri: GRAPH.to_string(),
                actor: DEVICE_ACTOR,
                counter: 1,
                base_clock: VectorClock::new(),
                ops: Vec::new(),
                timestamp_millis: 0,
            },
            authored: source,
        }
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
        if !setup.without_identity {
            store_identity(temp.path());
        }
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
            if setup.aruna_writers {
                let writer = deterministic_materialization_actor(EVENT_ONE);
                create_local(&replica, craqle::ActorId::from_bytes(writer));
                patch_local(&replica, other_actor());
                let node = iroh::SecretKey::from_bytes(&NODE_SECRET).public();
                log_event(temp.path(), EVENT_ONE, node, scaffold());
                let upsert = MetadataEventPayload::UpsertDataEntity {
                    jsonld: "{}".to_string(),
                };
                log_event(temp.path(), EVENT_TWO, other_node(), upsert);
            } else if setup.device_writer {
                create_local(&replica, node_writer());
                patch_local(&replica, DEVICE_ACTOR);
                log_event(temp.path(), EVENT_TWO, other_node(), device_batch());
            } else if setup.policy_only {
                let write = craqle::PolicyWrite {
                    graph: &graph,
                    policy: policy(),
                    durability: CraqleRequestDurability::WalAlreadyDurable,
                };
                replica.set_policy_with(&AllowAllAuthorizer, write).unwrap();
            } else {
                create_local(&replica, writer);
            }
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

    /// Every key and value of the Craqle, sync and node stores, read without starting a node.
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
        for (label, store) in [("sync", path.join("document-sync")), ("node", path.into())] {
            let db = OptimisticTxDatabase::builder(store).open().unwrap();
            for name in db.list_keyspace_names() {
                let keyspace = db
                    .inner()
                    .keyspace(&name, KeyspaceCreateOptions::default)
                    .unwrap();
                for guard in keyspace.iter() {
                    let (key, value) = guard.into_inner().unwrap();
                    state.insert(
                        (format!("{label} {}", &*name), key.to_vec()),
                        value.to_vec(),
                    );
                }
            }
        }
        state
    }

    /// A dry run starts no node, so it rebuilds, repairs or settles nothing in any store.
    #[test]
    fn dry_run_untouched() {
        let (temp, request) = replaced_replica(Setup {
            aruna_writers: true,
            ..Setup::default()
        });
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
        let applied = output.applied_actors.get(&overlap.actor);
        assert_eq!(applied.map(|actor| actor.sequence), Some(overlap.applied));
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
        let applied = output.applied_actors.get(&overlap.actor);
        assert_eq!(applied.map(|actor| actor.sequence), Some(overlap.applied));
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
        assert_eq!(output.applied_actors[&stranger].origin, "unidentified");
        request.apply = true;
        let error = repair_output(&request).unwrap_err();
        assert!(matches!(error, ExplorerError::Refused(_)));
    }

    /// A graph that never had a data mutation keeps its clock in the graph metadata.
    #[test]
    fn reads_metadata_clock() {
        let (_temp, mut request) = replaced_replica(Setup {
            policy_only: true,
            ..Setup::default()
        });
        let output = repair_output(&request).unwrap();
        assert!(output.overlap.is_empty() && output.applied_actors.is_empty());
        request.apply = true;
        assert!(repair_output(&request).unwrap().applied);
    }

    /// A metadata record that is cut short or carries extra bytes is malformed, not a clock.
    #[test]
    fn rejects_malformed_metadata() {
        let mut clock = VectorClock::new();
        clock.0.insert(craqle::ActorId::from_bytes([5; 32]), 3);
        let meta = GraphMeta {
            policy: policy(),
            policy_tag: PolicyTag::GENESIS,
            clock: clock.clone(),
            irokle_topic: Some([1; 32]),
            rocrate_context: None,
            rocrate_license: Some("license".to_string()),
            rocrate_license_digest: None,
            context_tag: ContextTag {
                counter: 2,
                actor: craqle::ActorId::from_bytes([6; 32]),
            },
        };
        let bytes = postcard::to_allocvec(&meta).unwrap();
        assert_eq!(metadata_clock(&bytes).unwrap(), clock);
        let longer = [bytes.as_slice(), &[0]].concat();
        for malformed in [&bytes[..bytes.len() - 1], longer.as_slice()] {
            let error = metadata_clock(malformed).unwrap_err();
            assert!(matches!(error, ExplorerError::Decode(_)), "{error}");
        }
    }

    /// Every path below `path`, so a refused dry run can be shown to have created nothing.
    fn files(path: &Path) -> BTreeSet<PathBuf> {
        let mut found = BTreeSet::new();
        let mut pending = vec![path.to_path_buf()];
        while let Some(dir) = pending.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let entry = entry.unwrap().path();
                if entry.is_dir() {
                    pending.push(entry.clone());
                }
                found.insert(entry);
            }
        }
        found
    }

    /// A root, Craqle store or sync store that is not an existing database is refused before
    /// anything opens it, so a dry run creates no database or keyspace there.
    #[test]
    fn refuses_missing_stores() {
        let empty = tempdir().unwrap();
        assert!(repair_output(&dry_request(empty.path())).is_err());
        assert!(files(empty.path()).is_empty());

        let (temp, request) = replaced_replica(Setup {
            without_identity: true,
            ..Setup::default()
        });
        let before = files(temp.path());
        let error = repair_output(&request).unwrap_err();
        assert!(matches!(error, ExplorerError::Refused(_)), "{error}");
        assert_eq!(files(temp.path()), before);

        let bare = tempdir().unwrap();
        store_identity(bare.path());
        for store in ["craqle/store", "document-sync"] {
            std::fs::create_dir_all(bare.path().join(store)).unwrap();
        }
        let before = files(bare.path());
        let error = repair_output(&dry_request(bare.path())).unwrap_err();
        assert!(matches!(error, ExplorerError::Refused(_)), "{error}");
        assert_eq!(files(bare.path()), before);
    }

    /// Only an Aruna metadata graph IRI in its canonical form names a document to inspect.
    #[test]
    fn refuses_foreign_graph() {
        let (_temp, mut request) = replaced_replica(Setup::default());
        for graph in [
            "urn:test:doctor-repair-cursor",
            "https://w3id.org/aruna/0000000000000000000000001a",
        ] {
            request.graph = graph.to_string();
            let error = repair_output(&request).unwrap_err();
            assert!(
                matches!(&error, ExplorerError::Refused(reason) if reason.contains("metadata graph")),
                "{graph}: {error}"
            );
        }
    }

    /// A materialized event's actor and another node's first-batch actor are Aruna's own local
    /// writers, identified from the event log, so they do not block the repair.
    #[test]
    fn identifies_local_writers() {
        let (_temp, mut request) = replaced_replica(Setup {
            aruna_writers: true,
            ..Setup::default()
        });
        let output = repair_output(&request).unwrap();
        assert!(output.overlap.is_empty(), "{:?}", output.overlap);
        let origin = |actor: [u8; 32]| output.applied_actors[&hex::encode(actor)].origin.clone();
        let materialized = deterministic_materialization_actor(EVENT_ONE);
        assert_eq!(
            origin(materialized),
            format!("materialization of event {EVENT_ONE}")
        );
        assert_eq!(
            origin(other_actor()),
            format!(
                "document actor of node {} from event {EVENT_TWO}",
                other_node()
            )
        );
        request.apply = true;
        assert!(repair_output(&request).unwrap().applied);
    }

    /// A device chooses its batch actor itself; no derivation names it, so it still refuses.
    #[test]
    fn refuses_device_actor() {
        let (_temp, mut request) = replaced_replica(Setup {
            device_writer: true,
            ..Setup::default()
        });
        let output = repair_output(&request).unwrap();
        let device = hex::encode(DEVICE_ACTOR);
        assert_eq!(output.applied_actors[&device].origin, "unidentified");
        assert!(output.overlap.iter().any(|overlap| overlap.actor == device));
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
