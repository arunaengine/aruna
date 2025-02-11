use std::{
    cmp::min,
    collections::VecDeque,
    sync::{atomic::AtomicU64, RwLockReadGuard},
};

use crate::{
    error::ArunaError,
    logerr,
    models::models::{EdgeType, MilliIdx, NodeVariant, Permission, RawRelation, RelationRange},
};
use milli::heed::{types::SerdeBincode, Database, RoTxn};
use milli::{ObkvCodec, BEU32, BEU64};
use petgraph::{
    graph::NodeIndex,
    visit::EdgeRef,
    Direction::{self, Incoming, Outgoing},
    Graph as PetGraph,
};
use tracing::error;

#[derive(Debug)]
pub(super) struct Graph {
    pub(super) graph: PetGraph<NodeVariant, EdgeType>,
    pub(super) idx_mappings: IdxMappings,
    pub(super) in_flight_tx: InFlightTxn,
}

pub struct GraphTxn<'a> {
    pub(super) state: RwLockReadGuard<'a, Graph>,
    pub(super) mode: Mode,
}

pub enum Mode {
    ReadTxn,
    WriteTxn,
}

#[derive(Debug)]
pub(super) struct IdxMappings {
    pub(super) graph_milli: Vec<u32>,
    pub(super) milli_graph: Vec<u32>,
}

#[derive(Default, Debug)]
pub(super) struct InFlightTxn {
    pub(super) nodes: Vec<u32>,
    pub(super) edges: Vec<u32>,
}

pub trait IndexHelper {
    fn as_u32(&self) -> u32;
}

impl IndexHelper for petgraph::graph::NodeIndex {
    fn as_u32(&self) -> u32 {
        self.index() as u32
    }
}

#[tracing::instrument(level = "trace", skip(rtxn, relations, documents))]
pub fn load_graph(
    rtxn: &RoTxn<'_>,
    relation_idx: &AtomicU64,
    relations: &Database<BEU64, SerdeBincode<RawRelation>>,
    documents: &Database<BEU32, ObkvCodec>,
) -> Result<Graph, ArunaError> {
    let mut graph = petgraph::graph::Graph::new();

    let mut milli_graph = Vec::new();
    let mut graph_milli = Vec::new();
    documents.iter(rtxn)?.try_for_each(|entry| {
        let (milli_idx, obkv) = entry.map_err(|e| ArunaError::ServerError(format!("{e}")))?;

        let node_type = obkv.get(1).ok_or_else(|| {
            ArunaError::ServerError("Node type not found in document".to_string())
        })?;

        let value = serde_json::from_slice::<serde_json::Number>(node_type).map_err(|_| {
            ArunaError::ConversionError {
                from: "&[u8]".to_string(),
                to: "serde_json::Number".to_string(),
            }
        })?;
        let variant = NodeVariant::try_from(value)?;

        let graph_idx = graph.add_node(variant).as_u32();
        milli_graph.insert(milli_idx as usize, graph_idx);
        graph_milli.insert(graph_idx as usize, milli_idx);

        Ok::<(), ArunaError>(())
    })?;

    // TODO: Not sure if this can produce interleaving idx invariants
    relation_idx.store(0, std::sync::atomic::Ordering::Relaxed);
    relations.iter(&rtxn)?.try_for_each(|entry| {
        let (_idx, relation) = entry.map_err(|e| ArunaError::ServerError(format!("{e}")))?;
        relation_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        graph.add_edge(
            relation.source.0.into(),
            relation.target.0.into(),
            relation.edge_type,
        );
        Ok::<(), ArunaError>(())
    })?;

    let idx_mappings = IdxMappings {
        graph_milli,
        milli_graph,
    };
    Ok(Graph {
        graph,
        idx_mappings,
        in_flight_tx: InFlightTxn::default(),
    })
}

impl GraphTxn<'_> {
    pub fn node_weight(&self, idx: MilliIdx) -> Option<NodeVariant> {
        self.state.graph.node_weight(idx.0.into()).cloned()
    }

    pub fn get_relation_range(
        &self,
        idx: MilliIdx,
        filter: Option<&[EdgeType]>,
        direction: Direction,
        range: RelationRange,
        // Tuple with (relations, last_entry)
    ) -> Result<(Vec<RawRelation>, u32), ArunaError> {
        let graph_idx = *self
            .state
            .idx_mappings
            .milli_graph
            .get(idx.0 as usize)
            .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?;

        let RelationRange {
            last_entry,
            page_size,
        } = range;
        let mut slice = Vec::new();
        let mut edge = match last_entry {
            Some(idx) => idx.into(),
            None => match self.state.graph.edges(graph_idx.into()).next() {
                Some(idx) => idx.id(),
                None => return Ok((vec![], 0)),
            },
        };
        for _ in 0..page_size {
            let edge_weight = match self.state.graph.edge_weight(edge) {
                Some(edge) => edge,
                None => continue,
            };
            let (source, target) = match self.state.graph.edge_endpoints(edge) {
                Some(refs) => refs,
                None => continue,
            };
            if let Some(filter) = filter {
                if !filter.contains(edge_weight) {
                    continue;
                }
            }
            if matches!(self.mode, Mode::ReadTxn) {
                if self
                    .state
                    .in_flight_tx
                    .edges
                    .contains(&(edge.index() as u32))
                {
                    continue;
                }
                if self.state.in_flight_tx.nodes.contains(&target.as_u32())
                    || self.state.in_flight_tx.nodes.contains(&source.as_u32())
                {
                    continue;
                }
            }
            let relation = match direction {
                Direction::Outgoing => RawRelation {
                    source: idx,
                    target: MilliIdx(
                        match self
                            .state
                            .idx_mappings
                            .graph_milli
                            .get(target.as_u32() as usize)
                        {
                            Some(i) => *i,
                            None => continue,
                        },
                    ),
                    edge_type: *edge_weight,
                },
                Direction::Incoming => RawRelation {
                    source: MilliIdx(
                        match self
                            .state
                            .idx_mappings
                            .graph_milli
                            .get(source.as_u32() as usize)
                        {
                            Some(i) => *i,
                            None => continue,
                        },
                    ),
                    target: idx,
                    edge_type: *edge_weight,
                },
            };
            slice.push(relation);
            edge = match self.state.graph.next_edge(edge, direction) {
                Some(edge) => edge,
                None => break,
            };
        }
        Ok((slice, edge.index() as u32))
    }

    pub fn get_relations(
        &self,
        idx: MilliIdx,
        filter: Option<&[EdgeType]>,
        direction: Direction,
    ) -> Result<Vec<RawRelation>, ArunaError> {
        let graph_idx = *self
            .state
            .idx_mappings
            .milli_graph
            .get(idx.0 as usize)
            .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?;

        Ok(self
            .state
            .graph
            .edges_directed(graph_idx.into(), direction)
            .filter_map(|e| {
                if let Some(filter) = filter {
                    if !filter.contains(e.weight()) {
                        return None;
                    }
                }
                if matches!(self.mode, Mode::ReadTxn) {
                    if self
                        .state
                        .in_flight_tx
                        .edges
                        .contains(&(e.id().index() as u32))
                    {
                        return None;
                    }
                    if self.state.in_flight_tx.nodes.contains(&e.target().as_u32())
                        || self.state.in_flight_tx.nodes.contains(&e.source().as_u32())
                    {
                        return None;
                    }
                }
                match direction {
                    Direction::Outgoing => Some(RawRelation {
                        source: idx,
                        target: MilliIdx(
                            *self
                                .state
                                .idx_mappings
                                .graph_milli
                                .get(e.target().as_u32() as usize)?,
                        ),
                        edge_type: *e.weight(),
                    }),
                    Direction::Incoming => Some(RawRelation {
                        source: MilliIdx(
                            *self
                                .state
                                .idx_mappings
                                .graph_milli
                                .get(e.source().as_u32() as usize)?,
                        ),
                        target: idx,
                        edge_type: *e.weight(),
                    }),
                }
            })
            .collect())
    }

    pub fn get_relatives(
        &self,
        idx: MilliIdx,
        direction: Direction,
    ) -> Result<Vec<MilliIdx>, ArunaError> {
        let graph_idx = *self
            .state
            .idx_mappings
            .milli_graph
            .get(idx.0 as usize)
            .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?;
        Ok(self
            .state
            .graph
            .edges_directed(graph_idx.into(), direction)
            .filter_map(|e| {
                if e.weight() == &0 {
                    let target_idx = match direction {
                        Direction::Outgoing => e.target(),
                        Direction::Incoming => e.source(),
                    };
                    if matches!(self.mode, Mode::ReadTxn) {
                        if self
                            .state
                            .in_flight_tx
                            .edges
                            .contains(&(e.id().index() as u32))
                        {
                            return None;
                        }
                        if self.state.in_flight_tx.nodes.contains(&e.target().as_u32())
                            || self.state.in_flight_tx.nodes.contains(&e.source().as_u32())
                        {
                            return None;
                        }
                    }

                    match self.state.graph.node_weight(target_idx) {
                        None => None,
                        Some(variant) => match variant {
                            NodeVariant::ResourceProject
                            | NodeVariant::ResourceFolder
                            | NodeVariant::ResourceObject => {
                                let target_milli_idx = MilliIdx(
                                    *self
                                        .state
                                        .idx_mappings
                                        .graph_milli
                                        .get(target_idx.as_u32() as usize)?,
                                );
                                Some(target_milli_idx)
                            }
                            _ => None,
                        },
                    }
                } else {
                    None
                }
            })
            .collect())
    }

    pub fn get_parent(&self, idx: MilliIdx) -> Option<MilliIdx> {
        let parents = self.get_relatives(idx, Incoming).ok()?;
        assert!(parents.len() <= 1);
        parents.first().copied()
    }

    #[allow(unused)]
    pub fn get_children(&self, idx: MilliIdx) -> Result<Vec<MilliIdx>, ArunaError> {
        self.get_relatives(idx, Outgoing)
    }

    /// Perform a breadth-first search for incoming edges to find the highest permission
    /// 1. Start at the resource node with a theoretical maximum permission
    /// 2. Iterate over all incoming edges
    /// 3. If the edge is a colored as a "permission" edge add the source node with the minimum permission to the queue
    /// 4. If the edge is a colored as a "hierarchy related non permission" edge add the source node with the previous current perm to the queue
    /// 5. If the source node is the target node, update the highest permission to the maximum of the current permission and the previous highest permission
    /// 6. If a constraint is reached, remove all other nodes from the queue and continue with the constraint node
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_permissions(
        &self,
        resource_idx: MilliIdx,
        identity: MilliIdx,
        constraint: Option<MilliIdx>,
    ) -> Result<Permission, ArunaError> {
        use crate::constants::relation_types::*;
        // Resource could be: Group, User, Projects, Folder, Object || TODO: Realm, ServiceAccount, Hooks, etc.

        let resource_idx = *self
            .state
            .idx_mappings
            .milli_graph
            .get(resource_idx.0 as usize)
            .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?;
        let identity = *self
            .state
            .idx_mappings
            .milli_graph
            .get(identity.0 as usize)
            .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?;

        // TODO: Brainfart here, there must be an easier way
        let constraint = if let Some(constraint) = constraint {
            Some(
                *self
                    .state
                    .idx_mappings
                    .milli_graph
                    .get(constraint.0 as usize)
                    .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?,
            )
        } else {
            None
        };

        let mut highest_perm: Option<u32> = None;
        let mut queue = VecDeque::new();
        let mut reached_constraint = !constraint.is_some();
        queue.push_back((resource_idx.into(), u32::MAX));
        while let Some((idx, current_perm)) = queue.pop_front() {
            // Iterate over all incoming edges
            for edge in self.state.graph.edges_directed(idx, Incoming) {
                match edge.weight() {
                    // If the edge is a "permission related" edge
                    &HAS_PART | &OWNS_PROJECT | &SHARES_PERMISSION | &GROUP_ADMINISTRATES_REALM => {
                        // If edge is part of in transition request -> skip
                        if self
                            .state
                            .in_flight_tx
                            .edges
                            .contains(&(edge.id().index() as u32))
                            || self
                                .state
                                .in_flight_tx
                                .nodes
                                .contains(&edge.target().as_u32())
                            || self
                                .state
                                .in_flight_tx
                                .nodes
                                .contains(&edge.source().as_u32())
                        {
                            continue;
                        }

                        if let Some(constraint) = constraint {
                            if edge.source().as_u32() == constraint {
                                reached_constraint = true;
                                queue.clear();
                            }
                        }

                        queue.push_back((edge.source(), current_perm));
                        if edge.source().as_u32() == identity {
                            if let Some(perm) = highest_perm.as_mut() {
                                if current_perm > *perm {
                                    *perm = current_perm;
                                }
                            } else {
                                highest_perm = Some(current_perm)
                            }
                        }
                    }
                    // If the edge is an explicit permission edge
                    got_perm @ PERMISSION_NONE..=PERMISSION_ADMIN => {
                        let perm_possible = min(*got_perm, current_perm);

                        if let Some(constraint) = constraint {
                            if edge.source().as_u32() == constraint {
                                reached_constraint = true;
                                queue.clear();
                            }
                        }
                        queue.push_back((edge.source(), perm_possible));

                        if edge.source().as_u32() == identity {
                            if let Some(perm) = highest_perm.as_mut() {
                                if perm_possible > *perm {
                                    *perm = perm_possible;
                                }
                            } else {
                                highest_perm = Some(perm_possible)
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        match highest_perm {
            None => {
                error!("No valid permission path found");
                Err(ArunaError::Forbidden("Permission denied".to_string()))
            }
            Some(p) => {
                if reached_constraint {
                    Ok(Permission::try_from(p).inspect_err(logerr!())?)
                } else {
                    error!("No valid permission path found");
                    Err(ArunaError::Forbidden("Permission denied".to_string()))
                }
            }
        }
    }

    #[allow(unused)]
    pub fn check_node_variant(
        &self,
        idx: MilliIdx,
        variant: &NodeVariant,
    ) -> Result<bool, ArunaError> {
        let idx = *self
            .state
            .idx_mappings
            .milli_graph
            .get(idx.0 as usize)
            .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?;

        if matches!(self.mode, Mode::ReadTxn) {
            if !self.state.in_flight_tx.nodes.contains(&idx) {
                Ok(self.state.graph.node_weight(idx.into()) == Some(variant))
            } else {
                Err(ArunaError::GraphError("Index not found".to_string()))
            }
        } else {
            Ok(self.state.graph.node_weight(idx.into()) == Some(variant))
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn has_relation(
        &self,
        source_idx: MilliIdx,
        target_idx: MilliIdx,
        edge_type: &[EdgeType],
    ) -> Result<bool, ArunaError> {
        let source_idx = *self
            .state
            .idx_mappings
            .milli_graph
            .get(source_idx.0 as usize)
            .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?;
        let target_idx = *self
            .state
            .idx_mappings
            .milli_graph
            .get(target_idx.0 as usize)
            .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?;

        if matches!(self.mode, Mode::ReadTxn) {
            if self.state.in_flight_tx.nodes.contains(&source_idx)
                || self.state.in_flight_tx.nodes.contains(&target_idx)
            {
                return Err(ArunaError::GraphError("Index not found".to_string()));
            }
        }
        Ok(self
            .state
            .graph
            .edges_connecting(source_idx.into(), target_idx.into())
            .any(|e| {
                if matches!(self.mode, Mode::ReadTxn)
                    && self
                        .state
                        .in_flight_tx
                        .edges
                        .contains(&(e.id().index() as u32))
                {
                    return false;
                }
                edge_type.contains(e.weight())
            }))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_related_user_or_groups(
        &self,
        resource_idx: MilliIdx,
    ) -> Result<Vec<MilliIdx>, ArunaError> {
        use crate::constants::relation_types::*;
        let mut user_or_group_idx = vec![];
        let mut queue = VecDeque::new();

        let resource_idx = *self
            .state
            .idx_mappings
            .milli_graph
            .get(resource_idx.0 as usize)
            .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?;
        queue.push_back(resource_idx.into());
        while let Some(idx) = queue.pop_front() {
            // Iterate over all incoming edges
            for edge in self.state.graph.edges_directed(idx, Incoming) {
                if matches!(self.mode, Mode::ReadTxn) {
                    if self
                        .state
                        .in_flight_tx
                        .edges
                        .contains(&(edge.id().index() as u32))
                    {
                        continue;
                    }
                    if self
                        .state
                        .in_flight_tx
                        .nodes
                        .contains(&edge.target().as_u32())
                        || self
                            .state
                            .in_flight_tx
                            .nodes
                            .contains(&edge.source().as_u32())
                    {
                        continue;
                    }
                }
                match edge.weight() {
                    // We know that this is a group or user
                    &OWNS_PROJECT | PERMISSION_READ..=PERMISSION_ADMIN => {
                        let user_or_group = MilliIdx(
                            *self
                                .state
                                .idx_mappings
                                .graph_milli
                                .get(edge.source().as_u32() as usize)
                                .ok_or_else(|| {
                                    ArunaError::GraphError("Index not found".to_string())
                                })?,
                        );
                        user_or_group_idx.push(user_or_group);
                    }
                    &HAS_PART => {
                        queue.push_back(edge.source());
                    }
                    _ => {}
                }
            }
        }
        Ok(user_or_group_idx)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_realm_and_groups(self, user_idx: MilliIdx) -> Result<Vec<MilliIdx>, ArunaError> {
        use crate::constants::relation_types::*;
        let user_graph_idx = *self
            .state
            .idx_mappings
            .milli_graph
            .get(user_idx.0 as usize)
            .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?;
        let mut indizes = vec![user_idx];
        let mut queue = VecDeque::new();
        queue.push_back(user_graph_idx.into());
        while let Some(idx) = queue.pop_front() {
            // Iterate over all incoming edges
            for edge in self.state.graph.edges_directed(idx, Outgoing) {
                if matches!(self.mode, Mode::ReadTxn) {
                    if self
                        .state
                        .in_flight_tx
                        .edges
                        .contains(&(edge.id().index() as u32))
                    {
                        continue;
                    }
                    if self
                        .state
                        .in_flight_tx
                        .nodes
                        .contains(&edge.target().as_u32())
                        || self
                            .state
                            .in_flight_tx
                            .nodes
                            .contains(&edge.source().as_u32())
                    {
                        continue;
                    }
                }
                match edge.weight() {
                    // The target is a group (but may have child groups)
                    PERMISSION_READ..=PERMISSION_ADMIN
                        if self.state.graph.node_weight(edge.target())
                            == Some(&NodeVariant::Group) =>
                    {
                        let group_idx = MilliIdx(
                            *self
                                .state
                                .idx_mappings
                                .graph_milli
                                .get(edge.target().as_u32() as usize)
                                .ok_or_else(|| {
                                    ArunaError::GraphError("Index not found".to_string())
                                })?,
                        );
                        indizes.push(group_idx);
                        // Loop detection
                        if !indizes.contains(&group_idx) {
                            queue.push_back(edge.source());
                        }
                    }
                    // The target is a realm
                    &GROUP_ADMINISTRATES_REALM | &GROUP_PART_OF_REALM => {
                        let realm_idx = MilliIdx(
                            *self
                                .state
                                .idx_mappings
                                .graph_milli
                                .get(edge.target().as_u32() as usize)
                                .ok_or_else(|| {
                                    ArunaError::GraphError("Index not found".to_string())
                                })?,
                        );
                        indizes.push(realm_idx);
                    }
                    _ => {}
                }
            }
        }
        Ok(indizes)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_realms(&self, user_idx: MilliIdx) -> Result<Vec<MilliIdx>, ArunaError> {
        use crate::constants::relation_types::*;
        let mut realms = vec![];
        let mut group_loop_detection = vec![];
        let mut queue = VecDeque::new();
        let user_graph_idx = *self
            .state
            .idx_mappings
            .milli_graph
            .get(user_idx.0 as usize)
            .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?;
        queue.push_back(user_graph_idx.into());
        while let Some(idx) = queue.pop_front() {
            // Iterate over all incoming edges
            for edge in self.state.graph.edges_directed(idx, Outgoing) {
                if matches!(self.mode, Mode::ReadTxn) {
                    if self
                        .state
                        .in_flight_tx
                        .edges
                        .contains(&(edge.id().index() as u32))
                    {
                        continue;
                    }
                    if self
                        .state
                        .in_flight_tx
                        .nodes
                        .contains(&edge.target().as_u32())
                        || self
                            .state
                            .in_flight_tx
                            .nodes
                            .contains(&edge.source().as_u32())
                    {
                        continue;
                    }
                }
                match edge.weight() {
                    // The target is a group (but may have child groups)
                    PERMISSION_READ..=PERMISSION_ADMIN
                        if self.state.graph.node_weight(edge.target())
                            == Some(&NodeVariant::Group) =>
                    {
                        // Loop detection
                        if !group_loop_detection.contains(&edge.target()) {
                            group_loop_detection.push(edge.target());
                            queue.push_back(edge.target());
                        }
                    }
                    // The target is a realm
                    &GROUP_ADMINISTRATES_REALM | &GROUP_PART_OF_REALM => {
                        let realm_idx = MilliIdx(
                            *self
                                .state
                                .idx_mappings
                                .graph_milli
                                .get(edge.target().as_u32() as usize)
                                .ok_or_else(|| {
                                    ArunaError::GraphError("Index not found".to_string())
                                })?,
                        );
                        realms.push(realm_idx);
                    }
                    _ => {}
                }
            }
        }
        Ok(realms)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_subtree(&self, node_idx: MilliIdx) -> Result<Vec<MilliIdx>, ArunaError> {
        //todo!("Remove tmp?");

        let mut subtree = vec![];

        let node_graph_idx = *self
            .state
            .idx_mappings
            .milli_graph
            .get(node_idx.0 as usize)
            .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?;

        let mut current_node_variant = self
            .state
            .graph
            .node_weight(node_graph_idx.into())
            .ok_or_else(|| {
                error!("Node with idx {} not found", node_idx.0);
                ArunaError::NotFound(format!("Node with idx {} not found", node_idx.0))
            })?;

        let mut current_node_id: NodeIndex = node_graph_idx.into();

        loop {
            // Not sure if this is wanted,
            // but for now this function is
            // only called with tmp objects anyway
            if matches!(&self.mode, Mode::ReadTxn) {
                if self
                    .state
                    .in_flight_tx
                    .nodes
                    .contains(&current_node_id.as_u32())
                {
                    break;
                }
            }
            let current_node_milli_idx = MilliIdx(
                *self
                    .state
                    .idx_mappings
                    .graph_milli
                    .get(current_node_id.as_u32() as usize)
                    .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?,
            );
            match current_node_variant {
                NodeVariant::ResourceProject => {
                    subtree.push(current_node_milli_idx);
                    break;
                }
                NodeVariant::ResourceFolder | NodeVariant::ResourceObject => {
                    subtree.push(current_node_milli_idx);
                    let Some(parent) = self.get_parent(current_node_milli_idx) else {
                        error!("Parent not found for {}", node_idx.0);
                        return Err(ArunaError::NotFound(format!(
                            "Parent not found for {}",
                            node_idx.0
                        )));
                    };

                    let parent_graph_idx = *self
                        .state
                        .idx_mappings
                        .milli_graph
                        .get(parent.0 as usize)
                        .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?;

                    current_node_id = parent_graph_idx.into();
                    current_node_variant = self
                        .state
                        .graph
                        .node_weight(current_node_id)
                        .ok_or_else(|| {
                            error!("Node with idx {:?} not found", current_node_id);
                            ArunaError::NotFound(format!(
                                "Node with idx {:?} not found",
                                current_node_id
                            ))
                        })?;
                }
                _ => {
                    subtree.push(current_node_milli_idx);
                    break;
                }
            }
        }

        Ok(subtree)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_all_children(&self, resource_idx: MilliIdx) -> Result<Vec<MilliIdx>, ArunaError> {
        let mut result = Vec::new();
        let mut queue = VecDeque::new();
        queue.push_front(resource_idx);
        while let Some(node_id) = queue.pop_front() {
            let children = self.get_children(node_id)?;
            result.extend(children.clone());
            queue.extend(children);
        }
        Ok(result)
    }
}
