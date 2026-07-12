//! Reusable realm fixture with more sync-eligible nodes than the placement
//! replication factor, so holder and non-holder paths are distinguishable.
//!
//! Fixtures sized at or below the replication factor make every node a holder
//! of every subject, which silently collapses non-holder coverage. [`Topology`]
//! keeps `node_count > replication_factor` and exposes exact, resolver-derived
//! holder proofs instead of probabilistic ones.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::{Actor, NodePlacementEntry, RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_realm_nodes::GetRealmNodesOperation;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::placement::{
    PlacementResolutionContext, build_view, plan_target_placement, resolve_holders,
    strategy_for_target, subject_bytes,
};
use aruna_operations::sync_placement::sort_node_ids;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use tokio::time::{Instant, sleep};

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
}

impl TestNode {
    pub fn node_id(&self) -> NodeId {
        self.net.node_id()
    }
}

pub struct Topology {
    pub realm_id: RealmId,
    pub replication_factor: u32,
    pub config: RealmConfigDocument,
    pub nodes: Vec<TestNode>,
}

impl Topology {
    /// Spawns a meshed realm of `node_count` Management nodes whose default
    /// placement strategy replicates to `replication_factor` holders.
    ///
    /// Panics unless `node_count > replication_factor`: a fixture at or below
    /// the factor cannot express a non-holder and would quietly void every
    /// assertion this module exists to make.
    pub async fn spawn(
        realm_id: RealmId,
        node_count: usize,
        replication_factor: u32,
    ) -> TestResult<Self> {
        assert!(
            node_count > replication_factor as usize,
            "non-holder fixture needs more nodes than the replication factor: \
             node_count={node_count} replication_factor={replication_factor}"
        );

        let mut nodes = Vec::with_capacity(node_count);
        for _ in 0..node_count {
            nodes.push(spawn_node(realm_id).await?);
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

        let config = install_realm_config(&nodes, realm_id, replication_factor).await?;

        Ok(Self {
            realm_id,
            replication_factor,
            config,
            nodes,
        })
    }

    pub fn node(&self, index: usize) -> &TestNode {
        &self.nodes[index]
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

    pub fn actor(&self, node: &TestNode, user_id: aruna_core::UserId) -> Actor {
        Actor {
            node_id: node.node_id(),
            user_id,
            realm_id: self.realm_id,
        }
    }

    /// Holder set the placement resolver assigns to `target`, sorted.
    pub fn holders(
        &self,
        target: &DocumentSyncTarget,
        context: PlacementResolutionContext<'_>,
    ) -> Vec<NodeId> {
        let mut holders = plan_target_placement(&self.config, target, context)
            .expect("realm config resolves a placement strategy for the target")
            .holders;
        sort_node_ids(&mut holders);
        holders
    }

    pub fn is_holder(
        &self,
        node_id: NodeId,
        target: &DocumentSyncTarget,
        context: PlacementResolutionContext<'_>,
    ) -> bool {
        self.holders(target, context).contains(&node_id)
    }

    pub fn non_holder_ids(
        &self,
        target: &DocumentSyncTarget,
        context: PlacementResolutionContext<'_>,
    ) -> Vec<NodeId> {
        let holders = self.holders(target, context);
        self.node_ids()
            .into_iter()
            .filter(|node_id| !holders.contains(node_id))
            .collect()
    }

    /// Proves `node_id` holds nothing for `target` and returns the holder set.
    ///
    /// The proof is exact, not statistical: placement is a pure function of the
    /// replicated realm config, so the resolver is re-run here. It also asserts
    /// the strategy actually capped the holder set below the fixture size,
    /// which is what makes "non-holder" a meaningful state at all.
    pub fn assert_not_holder(
        &self,
        node_id: NodeId,
        target: &DocumentSyncTarget,
        context: PlacementResolutionContext<'_>,
    ) -> Vec<NodeId> {
        let holders = self.holders(target, context);
        assert!(
            holders.len() < self.nodes.len(),
            "placement selected every fixture node for {target:?}; \
             non-holder coverage is void (holders={holders:?})"
        );
        assert!(
            !holders.contains(&node_id),
            "node {node_id} is a holder of {target:?} (holders={holders:?})"
        );
        holders
    }

    pub fn assert_holder(
        &self,
        node_id: NodeId,
        target: &DocumentSyncTarget,
        context: PlacementResolutionContext<'_>,
    ) -> Vec<NodeId> {
        let holders = self.holders(target, context);
        assert!(
            holders.contains(&node_id),
            "node {node_id} is not a holder of {target:?} (holders={holders:?})"
        );
        holders
    }

    /// First fixture node that holds nothing for `target`, with the proof run.
    pub fn non_holder(
        &self,
        target: &DocumentSyncTarget,
        context: PlacementResolutionContext<'_>,
    ) -> &TestNode {
        let node_id = *self
            .non_holder_ids(target, context)
            .first()
            .expect("fixture above the replication factor always has a non-holder");
        self.assert_not_holder(node_id, target, context);
        self.find(node_id)
    }

    pub fn holder(
        &self,
        target: &DocumentSyncTarget,
        context: PlacementResolutionContext<'_>,
    ) -> &TestNode {
        let node_id = *self
            .holders(target, context)
            .first()
            .expect("placement resolves at least one holder");
        self.find(node_id)
    }

    /// Every node's own view of the holder set, for cross-node agreement checks.
    pub async fn holder_views(
        &self,
        target: &DocumentSyncTarget,
        context: PlacementResolutionContext<'_>,
    ) -> TestResult<Vec<Vec<NodeId>>> {
        let mut views = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            let config = read_realm_config(node, self.realm_id).await?;
            let view = build_view(&config);
            let (strategy, override_) = strategy_for_target(&config, target, context)
                .expect("replicated realm config resolves the same strategy");
            let mut holders =
                resolve_holders(&view, strategy, &subject_bytes(target), 0, override_);
            sort_node_ids(&mut holders);
            views.push(holders);
        }
        Ok(views)
    }

    pub async fn shutdown(self) {
        for node in self.nodes {
            node.net.shutdown().await;
        }
    }
}

async fn spawn_node(realm_id: RealmId) -> TestResult<TestNode> {
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
    replication_factor: u32,
) -> TestResult<RealmConfigDocument> {
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), replication_factor);
    config.seed_default_placement();
    for (index, node) in nodes.iter().enumerate() {
        let node_id = node.node_id();
        config.ensure_node(node_id, RealmNodeKind::Management);
        config.placement_map.push(NodePlacementEntry {
            node_id,
            location: LOCATIONS[index % LOCATIONS.len()].to_string(),
            weight: NODE_WEIGHT,
            full: false,
            draining: false,
            labels: BTreeMap::new(),
        });
    }

    for node in nodes {
        let actor = Actor {
            node_id: node.node_id(),
            user_id: aruna_core::UserId::nil(realm_id),
            realm_id,
        };
        let bytes = config.to_bytes(&actor)?;
        match node
            .context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: (*realm_id.as_bytes()).into(),
                value: bytes.into(),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => return Err(format!("unexpected realm config write event: {other:?}").into()),
        }
        node.net.refresh_realm_peers_from_document(&config).await?;
    }

    Ok(config)
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
