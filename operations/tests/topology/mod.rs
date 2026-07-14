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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use aruna_core::auth::TRUSTED_REALMS_LIST_KEY;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{API_STATE_KEYSPACE, AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::structs::{
    Actor, MetadataRegistryRecord, NodePlacementEntry, PlacementRef, RealmAuthorizationDocument,
    RealmConfigDocument, RealmId, RealmNodeKind, TokenClaims,
};
use aruna_core::{NodeId, UserId};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::create_group::{CreateGroupConfig, CreateGroupOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::metadata::{MetadataAuthToken, MetadataHandle};
use aruna_operations::placement::{
    PlacementResolutionContext, choose_origin_bucket, meta_bucket_subject, resolve_shard_holders,
    strategy_for_target,
};
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use tempfile::TempDir;
use tokio::time::{Instant, sleep};
use ulid::Ulid;

pub type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

pub const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);

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
        assert!(
            management > replication_factor as usize,
            "non-holder fixture needs more sync-eligible nodes than the replication factor: \
             management={management} replication_factor={replication_factor}"
        );

        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
        let user_id = UserId::local(Ulid::r#gen(), realm_id);

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
            drive(
                AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
                    realm_id,
                    node_id: node.node_id(),
                    schedule_refresh: true,
                }),
                node.context.as_ref(),
            )
            .await?;
        }
        wait_for_realm_nodes(&nodes, realm_id).await?;

        let config = install_realm_config(&nodes, realm_id, user_id, replication_factor).await?;

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

    /// A bearer token for [`Topology::user_id`], signed by the realm key that the
    /// realm id is. A holder re-validates this before applying a forwarded write.
    pub fn bearer_token(&self) -> MetadataAuthToken {
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        let claims = TokenClaims {
            sub: self.user_id.to_string(),
            iss: self.realm_id.to_string(),
            iat: now,
            exp: now + 600,
            jti: Ulid::r#gen().to_string(),
            restrictions: None,
            issuer_pubkey: None,
            delegation_signature: None,
        };
        let key_pem = self
            .signing_key
            .to_pkcs8_pem(LineEnding::LF)
            .expect("realm key encodes");
        let token = encode(
            &Header::new(Algorithm::EdDSA),
            &claims,
            &EncodingKey::from_ed_pem(key_pem.as_bytes()).expect("realm key is an ed25519 key"),
        )
        .expect("token signs");
        MetadataAuthToken::bearer(token).expect("token is within the length bound")
    }

    /// A group owned by [`Topology::user_id`], replicated to every sync-eligible
    /// node. The holder of a forwarded write re-runs the caller's permission check
    /// against this group's authorization document, read from its own keyspace.
    pub async fn seed_group(&self) -> TestResult<Ulid> {
        let (group, _auth) = drive(
            CreateGroupOperation::new(CreateGroupConfig {
                actor: self.actor(self.node(0)),
                display_name: "topology group".to_string(),
                owner_cap: None,
            }),
            self.node(0).context.as_ref(),
        )
        .await?;

        let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
        loop {
            let mut pending = false;
            for node in self.nodes.iter().filter(|node| node.is_sync_eligible()) {
                if read_group_auth(node, group.group_id).await?.is_none() {
                    pending = true;
                }
            }
            if !pending {
                return Ok(group.group_id);
            }
            if Instant::now() >= deadline {
                return Err("the seeded group never reached every sync-eligible node".into());
            }
            sleep(Duration::from_millis(50)).await;
        }
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

    pub async fn shutdown(self) {
        for node in self.nodes {
            node.net.shutdown().await;
        }
    }
}

async fn spawn_node(realm_id: RealmId, kind: RealmNodeKind) -> TestResult<TestNode> {
    let temp_dir = tempfile::tempdir()?;
    let storage = FjallStorage::open(temp_dir.path().to_str().ok_or("invalid temp path")?)?;
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
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
    });

    initialize_net_incoming(context.clone());
    initialize_task_incoming(context.clone(), task_handle).await;

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
) -> TestResult<RealmConfigDocument> {
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), replication_factor);
    config.seed_default_placement();
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
    }

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

    // The startup hook, exactly as the binary runs it after loading the config: it
    // joins the shared realm topics and reconciles the held shard topics. Nothing
    // can be published onto a shard topic before its rank-0 holder has minted the
    // genesis, so without this every write onto a bucket defers forever. A node
    // whose rank-0 co-holder has not minted one yet leaves it for the next pass, so
    // run until the reconciler reports clean rather than a fixed number of passes.
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        for node in nodes {
            aruna_operations::startup::restore_shard_subscriptions(
                &node.context,
                node.node_id(),
                realm_id,
            )
            .await;
        }
        let mut retry = false;
        for node in nodes {
            retry |= aruna_operations::process_placements::process_shard_placements(
                &node.context,
                realm_id,
                node.node_id(),
            )
            .await
            .retry_scheduled;
        }
        if !retry {
            return Ok(config);
        }
        if Instant::now() >= deadline {
            return Err("shard placement reconciliation never reported clean".into());
        }
        sleep(Duration::from_millis(50)).await;
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

async fn wait_for_realm_nodes(nodes: &[TestNode], realm_id: RealmId) -> TestResult<()> {
    let expected: HashSet<NodeId> = nodes.iter().map(TestNode::node_id).collect();
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        let mut converged = true;
        for node in nodes {
            match drive(GetRealmNodesOperation::new(realm_id), node.context.as_ref()).await {
                Ok(realm_nodes) if realm_nodes == expected => {}
                _ => {
                    converged = false;
                    break;
                }
            }
        }
        if converged {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("realm nodes did not converge".into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// Polls `predicate` until it holds or the convergence budget expires.
pub async fn wait_until<F, Fut>(label: &str, node_id: NodeId, predicate: F) -> TestResult<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        if predicate().await {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("{label} did not converge on node {node_id}").into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}
