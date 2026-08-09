//! Realm fixture with more sync-eligible nodes than the placement replication
//! factor, plus a User-kind node that holds nothing at all.
//!
//! Fixtures sized at or below the replication factor make every node a holder of
//! every bucket, which silently collapses non-holder coverage. [`Topology`] keeps
//! `management > replication_factor` and derives holders the way production does:
//! a create stamps the best-ranked bucket its origin already holds
//! ([`choose_origin_bucket`], DECISIONS D3), so holdership is proved against that
//! stamped [`PlacementRef`] and never against a blind document hash.
//!
//! Only metadata document buckets are replica-capped. Group, user, auth and
//! registry documents are bound to the `everywhere` strategy (DECISIONS B1), so
//! every sync-eligible node holds them and a non-holder of one is not a reachable
//! state: this fixture cannot express it and must not pretend to. A User-kind node
//! is never sync-eligible, holds no bucket of any strategy, and is therefore the
//! one origin that reaches the D10 forwarding path.

#![allow(dead_code)]

#[path = "../convergence/mod.rs"]
mod convergence;

use aruna_core::keys::generate_signing_key;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use aruna_core::admin_document_reducer::AdminDocumentReducerState;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::auth::TRUSTED_REALMS_LIST_KEY;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::document::{DocumentSyncEffect, DocumentSyncNetEvent, DocumentSyncPublish};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    API_STATE_KEYSPACE, AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::structs::{
    Actor, AuthContext, DocumentClass, GroupAuthorizationDocument, HandleRange,
    MetadataRegistryRecord, NodePlacementEntry, PlacementBinding, PlacementRef, PlacementScope,
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind, TokenClaims,
    TransitionLimits, band_start,
};
use aruna_core::structured_id::PlacementHandle;
use aruna_core::util::unix_timestamp_millis;
use aruna_core::{NodeId, UserId};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::add_user_to_group::{AddUserToGroupInput, AddUserToGroupOperation};
use aruna_operations::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::create_group::{CreateGroupConfig, CreateGroupOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::expand_placement::expand_realm_placement;
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::metadata::{MetadataAuthToken, MetadataHandle};
use aruna_operations::mutate_realm_placement::{
    MutateRealmPlacementConfig, RealmPlacementMutation, drive_realm_placement_mutation,
};
use aruna_operations::placement::transition::{TransitionRequest, plan_transition};
use aruna_operations::placement::{
    PlacementResolutionContext, choose_origin_bucket, meta_bucket_subject, resolve_shard_holders,
    strategy_for_target, transition_members,
};
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use futures_util::future::join_all;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use tempfile::TempDir;
use ulid::Ulid;

pub use convergence::{hang_cap, wait_for_convergence};

pub type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

const TOPOLOGY_SHARD_COUNT: u32 = 8;

/// Two stable locations, so `distinct_locations` strategies stay satisfiable
/// and location ranking is exercised rather than degenerate.
pub const LOCATIONS: [&str; 2] = ["eu", "us"];
pub const NODE_WEIGHT: u32 = 100;

pub struct TestNode {
    _temp_dir: TempDir,
    pub net: NetHandle,
    pub context: Arc<DriverContext>,
    pub kind: RealmNodeKind,
}

impl TestNode {
    pub fn node_id(&self) -> NodeId {
        self.net.node_id()
    }

    pub fn is_sync_eligible(&self) -> bool {
        self.kind.is_sync_eligible()
    }
}

pub struct Topology {
    pub realm_id: RealmId,
    /// Owner of every group this fixture seeds, and the subject of its tokens.
    pub user_id: UserId,
    pub replication_factor: u32,
    pub config: RealmConfigDocument,
    pub nodes: Vec<TestNode>,
    signing_key: SigningKey,
}

impl Topology {
    /// Spawns a meshed realm of `management` Management nodes and `users`
    /// User-kind nodes, with a default placement strategy of
    /// `replication_factor` holders.
    ///
    /// The realm id is a verifying key, so the fixture can mint the bearer tokens
    /// a forwarded write re-validates. Panics unless
    /// `management > replication_factor`: a fixture at or below the factor cannot
    /// express a non-holder and would quietly void every assertion this module
    /// exists to make.
    pub async fn spawn(
        management: usize,
        users: usize,
        replication_factor: u32,
    ) -> TestResult<Self> {
        Self::spawn_sharded(management, users, replication_factor, TOPOLOGY_SHARD_COUNT).await
    }

    /// [`Topology::spawn`] with an explicit bucket count per strategy. A
    /// transition scenario hands every bucket over one by one, so it pays for
    /// the realm's whole shard space; a smaller one exercises the same paths.
    pub async fn spawn_sharded(
        management: usize,
        users: usize,
        replication_factor: u32,
        shard_count: u32,
    ) -> TestResult<Self> {
        assert!(
            management > replication_factor as usize,
            "non-holder fixture needs more sync-eligible nodes than the replication factor: \
             management={management} replication_factor={replication_factor}"
        );

        let signing_key = generate_signing_key();
        let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
        let user_id = UserId::local(Ulid::generate(), realm_id);

        let mut nodes = Vec::with_capacity(management + users);
        for index in 0..(management + users) {
            let kind = if index < management {
                RealmNodeKind::Management
            } else {
                RealmNodeKind::User
            };
            nodes.push(spawn_node(realm_id, kind).await?);
        }
        mesh(&nodes).await;

        for node in &nodes {
            hang_cap(
                "announce realm presence",
                drive(
                    AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
                        realm_id,
                        node_id: node.node_id(),
                        schedule_refresh: true,
                    }),
                    node.context.as_ref(),
                ),
            )
            .await?;
        }
        wait_for_realm_nodes(&nodes, realm_id).await?;

        let config =
            install_realm_config(&nodes, realm_id, user_id, replication_factor, shard_count)
                .await?;

        Ok(Self {
            realm_id,
            user_id,
            replication_factor,
            config,
            nodes,
            signing_key,
        })
    }

    pub fn node(&self, index: usize) -> &TestNode {
        &self.nodes[index]
    }

    /// First User-kind node: sync-ineligible, so it holds no bucket at all.
    pub fn user_node(&self) -> &TestNode {
        self.nodes
            .iter()
            .find(|node| !node.is_sync_eligible())
            .expect("fixture was spawned with a User-kind node")
    }

    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.iter().map(TestNode::node_id).collect()
    }

    pub fn find(&self, node_id: NodeId) -> &TestNode {
        self.nodes
            .iter()
            .find(|node| node.node_id() == node_id)
            .expect("node id belongs to this topology")
    }

    pub fn actor(&self, node: &TestNode) -> Actor {
        Actor {
            node_id: node.node_id(),
            user_id: self.user_id,
            realm_id: self.realm_id,
        }
    }

    /// The unrestricted principal [`Topology::bearer_token`] authenticates as.
    pub fn auth_context(&self) -> AuthContext {
        AuthContext {
            user_id: self.user_id,
            realm_id: self.realm_id,
            path_restrictions: None,
        }
    }

    /// A bearer token for [`Topology::user_id`], signed by the realm key that the
    /// realm id is. A holder re-validates this before applying a forwarded write.
    pub fn bearer_token(&self) -> MetadataAuthToken {
        MetadataAuthToken::bearer(self.bearer_string()).expect("token is within the length bound")
    }

    /// A bearer token for another realm principal, for fixtures that need a second
    /// authorized caller.
    pub fn bearer_for(&self, user_id: UserId) -> MetadataAuthToken {
        MetadataAuthToken::bearer(self.bearer_string_for(user_id))
            .expect("token is within the length bound")
    }

    pub fn auth_for(&self, user_id: UserId) -> AuthContext {
        AuthContext {
            user_id,
            realm_id: self.realm_id,
            path_restrictions: None,
        }
    }

    /// Adds `user_id` to the group's `user` role, which carries WRITE on the
    /// group's metadata paths, and waits for the change to replicate.
    pub async fn grant_group_user(&self, group_id: Ulid, user_id: UserId) -> TestResult<()> {
        let bytes = read_group_auth(self.node(0), group_id)
            .await?
            .ok_or("the seeded group has an authorization document")?;
        let auth_doc: GroupAuthorizationDocument = postcard::from_bytes(&bytes)?;
        let role_ids = auth_doc
            .roles
            .iter()
            .filter_map(|(role_id, role)| (role.name == "user").then_some(*role_id))
            .collect::<HashSet<_>>();
        hang_cap(
            "grant_group_user",
            drive(
                AddUserToGroupOperation::new(AddUserToGroupInput {
                    actor: self.actor(self.node(0)),
                    group_id,
                    user_id,
                    role_ids,
                }),
                self.node(0).context.as_ref(),
            ),
        )
        .await?;
        wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
            "the group grant never reached every sync-eligible node",
            || async {
                let mut pending = 0;
                for node in self.nodes.iter().filter(|node| node.is_sync_eligible()) {
                    let visible = match read_group_auth(node, group_id).await? {
                        Some(bytes) => postcard::from_bytes::<GroupAuthorizationDocument>(&bytes)
                            .is_ok_and(|doc| {
                                doc.roles
                                    .values()
                                    .any(|role| role.assigned_users.contains(&user_id))
                            }),
                        None => false,
                    };
                    if !visible {
                        pending += 1;
                    }
                }
                Ok(pending)
            },
        )
        .await?;
        Ok(())
    }

    /// The raw signed JWT backing [`Topology::bearer_token`], for callers that pass
    /// a bearer string rather than a [`MetadataAuthToken`].
    pub fn bearer_string(&self) -> String {
        self.bearer_string_for(self.user_id)
    }

    fn bearer_string_for(&self, user_id: UserId) -> String {
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        let claims = TokenClaims {
            sub: user_id.to_string(),
            iss: self.realm_id.to_string(),
            iat: now,
            exp: now + 600,
            jti: Ulid::generate().to_string(),
            restrictions: None,
            issuer_pubkey: None,
            delegation_signature: None,
        };
        let key_pem = self
            .signing_key
            .to_pkcs8_pem(LineEnding::LF)
            .expect("realm key encodes");
        encode(
            &Header::new(Algorithm::EdDSA),
            &claims,
            &EncodingKey::from_ed_pem(key_pem.as_bytes()).expect("realm key is an ed25519 key"),
        )
        .expect("token signs")
    }

    /// A group owned by [`Topology::user_id`], replicated to every sync-eligible
    /// node. The holder of a forwarded write re-runs the caller's permission check
    /// against this group's authorization document, read from its own keyspace.
    pub async fn seed_group(&self) -> TestResult<Ulid> {
        let (group, _auth) = hang_cap(
            "seed_group create",
            drive(
                CreateGroupOperation::new(CreateGroupConfig {
                    actor: self.actor(self.node(0)),
                    display_name: "topology group".to_string(),
                    owner_cap: None,
                }),
                self.node(0).context.as_ref(),
            ),
        )
        .await?;

        wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
            "the seeded group never reached every sync-eligible node",
            || async {
                // Policy loading needs the group record as well as its auth
                // document, and the two replicate independently.
                let mut pending = 0;
                for node in self.nodes.iter().filter(|node| node.is_sync_eligible()) {
                    if read_group_auth(node, group.group_id).await?.is_none()
                        || read_group_record(node, group.group_id).await?.is_none()
                    {
                        pending += 1;
                    }
                }
                Ok(pending)
            },
        )
        .await?;
        Ok(group.group_id)
    }

    /// The bucket a create on `origin` stamps (D3): the best-ranked bucket the
    /// origin already holds, chosen on `(realm_id, group_id, path)`. The origin is
    /// therefore always a holder of what it creates.
    ///
    /// `None` when the origin holds no bucket of the governing strategy - a
    /// User-kind node, which is the only origin that reaches the D10 forward.
    pub fn origin_placement(
        &self,
        origin: &TestNode,
        group_id: Ulid,
        document_id: Ulid,
        document_path: &str,
    ) -> Option<PlacementRef> {
        let path = MetadataRegistryRecord::normalize_document_path(document_path);
        let target = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };
        let (strategy, _) = strategy_for_target(
            &self.config,
            &target,
            PlacementResolutionContext {
                group_id: Some(group_id),
                metadata_path: Some(path.as_str()),
            },
        )?;
        choose_origin_bucket(
            &self.config,
            strategy,
            origin.node_id(),
            &meta_bucket_subject(self.realm_id, group_id, &path),
        )
    }

    /// Rank-ordered holders of `placement`, exactly as every node derives them.
    pub fn holders(&self, placement: &PlacementRef) -> Vec<NodeId> {
        resolve_shard_holders(&self.config, placement)
    }

    pub fn is_holder(&self, node_id: NodeId, placement: &PlacementRef) -> bool {
        self.holders(placement).contains(&node_id)
    }

    /// Sync-eligible fixture nodes that hold nothing of `placement`. User-kind
    /// nodes are excluded: they hold nothing of anything by construction, so they
    /// prove nothing about a capped bucket.
    pub fn non_holder_ids(&self, placement: &PlacementRef) -> Vec<NodeId> {
        let holders = self.holders(placement);
        self.nodes
            .iter()
            .filter(|node| node.is_sync_eligible())
            .map(TestNode::node_id)
            .filter(|node_id| !holders.contains(node_id))
            .collect()
    }

    /// Proves `node_id` holds nothing of `placement` and returns the holder set.
    ///
    /// The proof is exact, not statistical: holders are a pure function of the
    /// replicated realm config and the stamped bucket, so the resolver is re-run
    /// here. It also asserts the strategy capped the holder set below the count of
    /// sync-eligible nodes, which is what makes "non-holder" meaningful at all.
    pub fn assert_not_holder(&self, node_id: NodeId, placement: &PlacementRef) -> Vec<NodeId> {
        let holders = self.holders(placement);
        assert!(
            holders.len() < self.sync_eligible_count(),
            "placement selected every sync-eligible node for {placement:?}; \
             non-holder coverage is void (holders={holders:?})"
        );
        assert!(
            !holders.contains(&node_id),
            "node {node_id} is a holder of {placement:?} (holders={holders:?})"
        );
        holders
    }

    pub fn assert_holder(&self, node_id: NodeId, placement: &PlacementRef) -> Vec<NodeId> {
        let holders = self.holders(placement);
        assert!(
            holders.contains(&node_id),
            "node {node_id} is not a holder of {placement:?} (holders={holders:?})"
        );
        holders
    }

    /// First sync-eligible fixture node that holds nothing of `placement`.
    pub fn non_holder(&self, placement: &PlacementRef) -> &TestNode {
        let node_id = *self
            .non_holder_ids(placement)
            .first()
            .expect("a realm above the replication factor always has a non-holder");
        self.assert_not_holder(node_id, placement);
        self.find(node_id)
    }

    /// Rank-0 holder of `placement`.
    pub fn holder(&self, placement: &PlacementRef) -> &TestNode {
        let node_id = *self
            .holders(placement)
            .first()
            .expect("placement resolves at least one holder");
        self.find(node_id)
    }

    /// Every sync-eligible node's own view of the holder set, from the config it
    /// replicated, for cross-node agreement checks.
    pub async fn holder_views(&self, placement: &PlacementRef) -> TestResult<Vec<Vec<NodeId>>> {
        let mut views = Vec::new();
        for node in self.nodes.iter().filter(|node| node.is_sync_eligible()) {
            let config = read_realm_config(node, self.realm_id).await?;
            views.push(resolve_shard_holders(&config, placement));
        }
        Ok(views)
    }

    pub fn sync_eligible_count(&self) -> usize {
        self.nodes
            .iter()
            .filter(|node| node.is_sync_eligible())
            .count()
    }

    /// Desired shard-topic membership for a bucket, as the reconciler derives it.
    pub fn members(&self, placement: &PlacementRef) -> Vec<NodeId> {
        transition_members(&self.config, placement, unix_timestamp_millis())
    }

    /// Every transition the realm has not finished yet.
    pub fn live_transitions(&self) -> Vec<Ulid> {
        self.config
            .placement_transitions
            .iter()
            .filter(|transition| !transition.is_terminal())
            .map(|transition| transition.plan.transition_id)
            .collect()
    }

    /// Every activated bucket of every strategy, in resolution order.
    pub fn holder_map(&self) -> BTreeMap<(Ulid, u32), Vec<NodeId>> {
        let mut holders = BTreeMap::new();
        for strategy in &self.config.strategies {
            for shard in 0..strategy.shard_count {
                let placement = PlacementRef {
                    strategy_id: strategy.strategy_id,
                    shard,
                };
                holders.insert(
                    (strategy.strategy_id, shard),
                    resolve_shard_holders(&self.config, &placement),
                );
            }
        }
        holders
    }

    /// Drives one placement mutation on a node, so control state reaches the
    /// other nodes through the admin-op reducer path rather than a raw write.
    pub async fn mutate(
        &mut self,
        node_index: usize,
        mutation: RealmPlacementMutation,
    ) -> TestResult<()> {
        let node = &self.nodes[node_index];
        let actor = self.actor(node);
        let config = hang_cap(
            "placement mutation",
            drive_realm_placement_mutation(
                MutateRealmPlacementConfig { actor, mutation },
                node.context.as_ref(),
            ),
        )
        .await?;
        self.config = config;
        Ok(())
    }

    /// Snapshots the current view as a new candidate map on every node.
    pub async fn publish_map(&mut self, node_index: usize) -> TestResult<u64> {
        let mut snapshot = self.config.clone();
        let epoch = snapshot.snapshot_candidate_map();
        let map = snapshot
            .candidate_map(epoch)
            .expect("the snapshot published one map")
            .clone();
        self.mutate(node_index, RealmPlacementMutation::PublishCandidateMap(map))
            .await?;
        self.await_config("candidate map replicates", move |config| {
            config.candidate_map(epoch).is_some()
        })
        .await?;
        Ok(epoch)
    }

    /// Starts a transition of `strategy_id` onto `target_epoch` and waits for
    /// every node to observe it - as the record, or as the cutover it already
    /// produced, since a zero grace releases the record with the last proof.
    pub async fn start_transition(
        &mut self,
        node_index: usize,
        strategy_id: Ulid,
        buckets: Vec<u32>,
        target_epoch: u64,
        limits: TransitionLimits,
    ) -> TestResult<Ulid> {
        let transition_id = Ulid::generate();
        let plan = plan_transition(
            &self.config,
            TransitionRequest {
                transition_id,
                strategy_id,
                buckets,
                target_map_epoch: target_epoch,
                limits,
                created_by: self.nodes[node_index].node_id(),
                created_at_ms: 1,
            },
        )?;
        let planned = plan.bucket_list();
        self.mutate(node_index, RealmPlacementMutation::StartTransition(plan))
            .await?;
        self.await_config("transition record replicates", move |config| {
            config.transition(&transition_id).is_some()
                || planned.iter().all(|bucket| {
                    config
                        .activation(&strategy_id, *bucket)
                        .is_some_and(|activation| activation.candidate_map_epoch == target_epoch)
                })
        })
        .await?;
        Ok(transition_id)
    }

    /// Runs the placement reconciler on every node until it reports clean, so
    /// barrier, pull, verify, and proof steps all get their turn.
    pub async fn run_placements(&self) -> TestResult<()> {
        let realm_id = self.realm_id;
        let nodes = &self.nodes;
        wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
            "placement reconciliation never reported clean",
            || async move {
                let mut pending = 0;
                for node in nodes {
                    if aruna_operations::process_placements::process_shard_placements(
                        &node.context,
                        realm_id,
                        node.node_id(),
                    )
                    .await
                    .retry_scheduled
                    {
                        pending += 1;
                    }
                }
                Ok(pending)
            },
        )
        .await
    }

    /// Drives the reconciler until every bucket of `transition_id` has cut over
    /// on every node.
    pub async fn await_transition(&mut self, transition_id: Ulid) -> TestResult<()> {
        let realm_id = self.realm_id;
        let nodes = &self.nodes;
        wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
            "placement transition never completed",
            || async move {
                for node in nodes {
                    aruna_operations::process_placements::process_shard_placements(
                        &node.context,
                        realm_id,
                        node.node_id(),
                    )
                    .await;
                }
                replicate_config(nodes, realm_id).await;
                // Count buckets, not nodes: the wait must see progress on every
                // cut-over, not only when the last one lands.
                let mut pending = 0;
                for node in nodes.iter().filter(|node| node.is_sync_eligible()) {
                    let config = read_realm_config(node, realm_id).await?;
                    // A record that is gone was released after cutting every
                    // bucket over; `start_transition` already waited for it to
                    // replicate, so absence here is completion.
                    if let Some(transition) = config.transition(&transition_id) {
                        pending += transition
                            .plan
                            .buckets
                            .iter()
                            .filter(|bucket| transition.completion(bucket.bucket).is_none())
                            .count();
                    }
                }
                Ok(pending)
            },
        )
        .await?;
        self.config = read_realm_config(self.node(0), self.realm_id).await?;
        Ok(())
    }

    /// Waits until every node's stored realm config satisfies `predicate`, then
    /// refreshes the fixture's own copy from node zero.
    pub async fn await_config<F>(&mut self, label: &str, predicate: F) -> TestResult<()>
    where
        F: Fn(&RealmConfigDocument) -> bool,
    {
        let realm_id = self.realm_id;
        let predicate = &predicate;
        let nodes = &self.nodes;
        wait_for_convergence::<_, _, Box<dyn std::error::Error>>(label, || async move {
            replicate_config(nodes, realm_id).await;
            let mut pending = 0;
            for node in nodes.iter().filter(|node| node.is_sync_eligible()) {
                if !predicate(&read_realm_config(node, realm_id).await?) {
                    pending += 1;
                }
            }
            Ok(pending)
        })
        .await?;
        self.config = read_realm_config(self.node(0), self.realm_id).await?;
        Ok(())
    }

    /// Spawns, meshes, announces, and registers one more node, then runs the
    /// production onboarding expansion: it publishes a map naming the joiner and
    /// starts a transition for every bucket that only grows. The joiner holds
    /// nothing until that transition completes.
    pub async fn spawn_late_node(&mut self, kind: RealmNodeKind) -> TestResult<NodeId> {
        let node = spawn_node(self.realm_id, kind.clone()).await?;
        let node_id = node.node_id();
        for existing in &self.nodes {
            existing.net.add_peer_addr(node.net.endpoint_addr()).await;
            node.net.add_peer_addr(existing.net.endpoint_addr()).await;
        }
        hang_cap(
            "announce late realm presence",
            drive(
                AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
                    realm_id: self.realm_id,
                    node_id,
                    schedule_refresh: true,
                }),
                node.context.as_ref(),
            ),
        )
        .await?;
        self.nodes.push(node);

        let mut config = self.config.clone();
        config.ensure_node(node_id, kind.clone());
        if kind.is_sync_eligible() {
            config.placement_map.push(NodePlacementEntry {
                node_id,
                location: LOCATIONS[self.nodes.len() % LOCATIONS.len()].to_string(),
                weight: NODE_WEIGHT,
                full: false,
                draining: false,
                labels: BTreeMap::new(),
            });
        }
        self.apply_config(config).await?;
        // The joiner runs the startup hook and joins the realm-config topic, as
        // a freshly started node does; without it no admin event reaches it.
        let node = self.find(node_id);
        aruna_operations::startup::restore_shard_subscriptions(
            &node.context,
            node_id,
            self.realm_id,
        )
        .await;
        let topic = DocumentSyncTarget::RealmConfig {
            realm_id: self.realm_id,
        }
        .sync_topic_id(self.realm_id, &PlacementRef::NIL);
        node.net
            .send_effect(Effect::Net(NetEffect::DocumentSync(
                DocumentSyncEffect::SyncDocuments {
                    topics: vec![topic],
                    peers: Vec::new(),
                },
            )))
            .await;

        let actor = self.actor(self.node(0));
        let started = hang_cap(
            "onboarding expansion",
            expand_realm_placement(self.nodes[0].context.as_ref(), &actor),
        )
        .await?;
        self.await_config("expansion transitions replicate", |config| {
            started
                .iter()
                .all(|transition_id| config.transition(transition_id).is_some())
        })
        .await?;
        for transition_id in &started {
            let transition = self
                .config
                .transition(transition_id)
                .expect("the started transition replicated");
            for bucket in &transition.plan.buckets {
                assert!(
                    bucket
                        .old_holders
                        .iter()
                        .all(|holder| bucket.target_holders.contains(holder)),
                    "onboarding issued a transition that moves a bucket off a holder"
                );
            }
        }
        Ok(node_id)
    }

    /// Reinstalls a mutated realm config on every node, as an admin change would.
    pub async fn apply_config(&mut self, config: RealmConfigDocument) -> TestResult<()> {
        for node in &self.nodes {
            let actor = Actor {
                node_id: node.node_id(),
                user_id: self.user_id,
                realm_id: self.realm_id,
            };
            write(
                node,
                REALM_CONFIG_KEYSPACE,
                self.realm_id.as_bytes().to_vec(),
                config.to_bytes(&actor)?,
            )
            .await?;
            node.net.refresh_realm_peers_from_document(&config).await?;
        }
        self.config = config;
        Ok(())
    }

    pub async fn shutdown(self) {
        for node in self.nodes {
            hang_cap("node shutdown", node.net.shutdown()).await;
        }
    }
}

async fn spawn_node(realm_id: RealmId, kind: RealmNodeKind) -> TestResult<TestNode> {
    let temp_dir = tempfile::tempdir()?;
    let storage = FjallStorage::open_test(temp_dir.path().to_str().ok_or("invalid temp path")?)?;
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            document_sync_storage_path: Some(temp_dir.path().join("document-sync")),
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await?;
    let task_handle = TaskHandle::new();
    let metadata_handle = MetadataHandle::new(
        temp_dir.path().join("metadata"),
        net.node_id(),
        storage.clone(),
        Some(net.clone()),
        Some(net.document_sync_node()),
        Some(net.document_sync_database()),
    )?;

    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: Some(metadata_handle),
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    });

    initialize_net_incoming(context.clone());
    initialize_task_incoming(
        context.clone(),
        task_handle,
        aruna_operations::jobs::runtime::JobsRuntime::new_paused(),
    )
    .await;

    Ok(TestNode {
        _temp_dir: temp_dir,
        net,
        context,
        kind,
    })
}

async fn mesh(nodes: &[TestNode]) {
    for left in 0..nodes.len() {
        for right in (left + 1)..nodes.len() {
            nodes[left]
                .net
                .add_peer_addr(nodes[right].net.endpoint_addr())
                .await;
            nodes[right]
                .net
                .add_peer_addr(nodes[left].net.endpoint_addr())
                .await;
        }
    }
}

async fn install_realm_config(
    nodes: &[TestNode],
    realm_id: RealmId,
    user_id: UserId,
    replication_factor: u32,
    shard_count: u32,
) -> TestResult<RealmConfigDocument> {
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), replication_factor);
    config.seed_default_placement();
    for strategy in &mut config.strategies {
        strategy.shard_count = shard_count;
    }
    let mut band = 0u32;
    for (index, node) in nodes.iter().enumerate() {
        let node_id = node.node_id();
        config.ensure_node(node_id, node.kind.clone());
        if !node.is_sync_eligible() {
            continue;
        }
        config.placement_map.push(NodePlacementEntry {
            node_id,
            location: LOCATIONS[index % LOCATIONS.len()].to_string(),
            weight: NODE_WEIGHT,
            full: false,
            draining: false,
            labels: BTreeMap::new(),
        });
        // Every sync-eligible node gets its onboarding band; the band's first
        // handle carries its immutable JobControl binding.
        let range = HandleRange {
            range_id: Ulid::from_bytes([band as u8 + 1; 16]),
            owner: node_id,
            start: band_start(band),
            end: band_start(band + 1),
        };
        config.placement_handle_ranges.push(range);
        config.placement_bindings.push(PlacementBinding {
            handle: PlacementHandle::new(range.start).expect("band start is a valid handle"),
            scope: PlacementScope::Realm(realm_id),
            document_class: DocumentClass::JobControl,
            strategy_id: config
                .default_strategy_id
                .expect("seeded config has a default strategy"),
            allocator_range_id: Some(range.range_id),
            allocated_by: Some(node_id),
            allocated_at_ms: Some(1),
        });
        band += 1;
    }

    // Freeze the assembled view as epoch 1 and activate it: holder resolution is
    // pinned to a published map, exactly as a bootstrapped realm is.
    config.snapshot_candidate_map();

    let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
    let trusted = HashSet::from([realm_id]);
    for node in nodes {
        let actor = Actor {
            node_id: node.node_id(),
            user_id,
            realm_id,
        };
        write(
            node,
            REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            config.to_bytes(&actor)?,
        )
        .await?;
        // The realm authorization document a permission check reads first, and the
        // trusted-realm list a forwarded caller's bearer token validates against:
        // both are per-node local state in production too.
        write(
            node,
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            realm_auth.to_bytes(&actor)?,
        )
        .await?;
        write(
            node,
            API_STATE_KEYSPACE,
            TRUSTED_REALMS_LIST_KEY.to_vec(),
            postcard::to_allocvec(&trusted)?,
        )
        .await?;
        node.net.refresh_realm_peers_from_document(&config).await?;
    }

    // Admin operations ride the shared realm-config topic, which needs a genesis
    // before anything can publish onto it. The last node seeds it so node zero,
    // which the tests mutate through, starts from an empty origin sequence.
    seed_config_topic(nodes, realm_id, &config).await?;

    // Activations must be reducer-owned to advance: a literal seed is only the
    // bootstrap value. One explicit admin op per strategy hands ownership over,
    // materializing the same epoch-1 activations the seed wrote.
    let strategy_ids: Vec<Ulid> = config
        .strategies
        .iter()
        .map(|strategy| strategy.strategy_id)
        .collect();
    for strategy_id in strategy_ids {
        hang_cap(
            "initialize activations",
            drive_realm_placement_mutation(
                MutateRealmPlacementConfig {
                    actor: Actor {
                        node_id: nodes[0].node_id(),
                        user_id,
                        realm_id,
                    },
                    mutation: RealmPlacementMutation::InitializeActivations {
                        strategy_id,
                        candidate_map_epoch: 1,
                    },
                },
                nodes[0].context.as_ref(),
            ),
        )
        .await?;
    }
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "activation initialization never replicated",
        || async {
            replicate_config(nodes, realm_id).await;
            let mut pending = 0;
            for node in nodes.iter().filter(|node| node.is_sync_eligible()) {
                let stored = read_realm_config(node, realm_id).await?;
                if stored.placement_activations.len() != config.placement_activations.len() {
                    pending += 1;
                }
            }
            Ok(pending)
        },
    )
    .await?;

    // The startup hook, exactly as the binary runs it after loading the config: it
    // joins the shared realm topics and reconciles the held shard topics. Nothing
    // can be published onto a shard topic before its rank-0 holder has minted the
    // genesis, so without this every write onto a bucket defers forever. A node
    // whose rank-0 co-holder has not minted one yet leaves it for the next pass, so
    // run until the reconciler reports clean rather than a fixed number of passes.
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "shard placement reconciliation never reported clean",
        || async {
            join_all(nodes.iter().map(|node| {
                aruna_operations::startup::restore_shard_subscriptions(
                    &node.context,
                    node.node_id(),
                    realm_id,
                )
            }))
            .await;
            let outcomes = join_all(nodes.iter().map(|node| {
                aruna_operations::process_placements::process_shard_placements(
                    &node.context,
                    realm_id,
                    node.node_id(),
                )
            }))
            .await;
            let pending = outcomes
                .iter()
                .filter(|outcome| outcome.retry_scheduled)
                .count();
            Ok(pending)
        },
    )
    .await?;
    Ok(config)
}

/// Creates the realm-config sync topic and joins every node to it.
async fn seed_config_topic(
    nodes: &[TestNode],
    realm_id: RealmId,
    config: &RealmConfigDocument,
) -> TestResult<()> {
    // A User-kind node seeds when the fixture has one: it never submits an admin
    // operation, so the genesis it publishes cannot collide with a later event
    // from the same actor log.
    let seeder = nodes
        .iter()
        .find(|node| !node.is_sync_eligible())
        .or_else(|| nodes.last())
        .ok_or("topology has no nodes")?;
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    let placement =
        aruna_operations::placement::placement_ref_for_target(config, &target, Default::default());
    let topic = target.sync_topic_id(realm_id, &placement);
    let actor = Actor {
        node_id: seeder.node_id(),
        user_id: aruna_core::UserId::nil(realm_id),
        realm_id,
    };
    let mut reducer_state =
        AdminDocumentReducerState::new(AdminDocumentTarget::RealmConfig { realm_id });
    let event = reducer_state.apply_operation(
        &actor,
        AdminDocumentOperation::RealmConfigNodePlacementSet {
            entry: config
                .placement_map
                .first()
                .ok_or("realm config has no placement entry")?
                .clone(),
        },
    )?;
    // Persist the state the seed event advanced, or the seeder's next mutation
    // would reuse origin sequence one and fork its own actor log.
    let (key_space, key, value) =
        aruna_core::storage_entries::admin_document_reducer_state_write_entry(&reducer_state)?;
    write(seeder, &key_space, key.to_vec(), value.to_vec()).await?;
    match seeder
        .net
        .send_effect(Effect::Net(NetEffect::DocumentSync(
            DocumentSyncEffect::PublishDocuments {
                documents: vec![DocumentSyncPublish::AdminOperation {
                    target: target.clone(),
                    event: Box::new(event),
                    placement,
                    allow_genesis: true,
                }],
                peers: Vec::new(),
            },
        )))
        .await
    {
        Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::DocumentsPublished { .. })) => {}
        other => return Err(format!("unexpected config topic seed publish: {other:?}").into()),
    }
    for node in nodes {
        if node.node_id() == seeder.node_id() || !node.is_sync_eligible() {
            continue;
        }
        match node
            .net
            .send_effect(Effect::Net(NetEffect::DocumentSync(
                DocumentSyncEffect::SyncDocuments {
                    topics: vec![topic],
                    peers: Vec::new(),
                },
            )))
            .await
        {
            Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::DocumentsReconciled {
                ..
            })) => {}
            other => return Err(format!("unexpected config topic seed sync: {other:?}").into()),
        }
    }
    Ok(())
}

/// Flushes every node's document-sync outbox and pulls the realm-config topic,
/// so an admin event converges on the next poll instead of waiting out the
/// drain timer and a gossip round.
async fn replicate_config(nodes: &[TestNode], realm_id: RealmId) {
    for node in nodes {
        aruna_operations::task_incoming::drive_document_sync_outbox_drain(node.context.clone())
            .await;
    }
    let topic =
        DocumentSyncTarget::RealmConfig { realm_id }.sync_topic_id(realm_id, &PlacementRef::NIL);
    for node in nodes.iter().filter(|node| node.is_sync_eligible()) {
        node.net
            .send_effect(Effect::Net(NetEffect::DocumentSync(
                DocumentSyncEffect::SyncDocuments {
                    topics: vec![topic],
                    peers: Vec::new(),
                },
            )))
            .await;
    }
}

async fn write(node: &TestNode, key_space: &str, key: Vec<u8>, value: Vec<u8>) -> TestResult<()> {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        other => Err(format!("unexpected write event in `{key_space}`: {other:?}").into()),
    }
}

async fn read_group_auth(node: &TestNode, group_id: Ulid) -> TestResult<Option<Vec<u8>>> {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: AUTH_KEYSPACE.to_string(),
            key: group_id.to_bytes().into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            Ok(value.map(|bytes| bytes.to_vec()))
        }
        other => Err(format!("unexpected group auth read event: {other:?}").into()),
    }
}

async fn read_group_record(node: &TestNode, group_id: Ulid) -> TestResult<Option<Vec<u8>>> {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: GROUP_KEYSPACE.to_string(),
            key: group_id.to_bytes().into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            Ok(value.map(|bytes| bytes.to_vec()))
        }
        other => Err(format!("unexpected group record read event: {other:?}").into()),
    }
}

async fn read_realm_config(node: &TestNode, realm_id: RealmId) -> TestResult<RealmConfigDocument> {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: (*realm_id.as_bytes()).into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => Ok(RealmConfigDocument::from_bytes(&value)?),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            Err("realm config missing on fixture node".into())
        }
        other => Err(format!("unexpected realm config read event: {other:?}").into()),
    }
}

/// Polls `predicate` until it holds, resilient to a slow-but-converging run via
/// the shared lost-progress wait rather than a fixed wall-clock budget.
pub async fn wait_until<F, Fut>(label: &str, node_id: NodeId, predicate: F) -> TestResult<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = bool>,
{
    let context = format!("{label} did not converge on node {node_id}");
    wait_for_convergence(&context, || async { Ok(usize::from(!predicate().await)) }).await
}
