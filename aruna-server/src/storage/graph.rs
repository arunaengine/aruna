use std::{cmp::min, collections::VecDeque, sync::atomic::AtomicU64};

use crate::{
    error::ArunaError,
    logerr,
    models::models::{EdgeType, NodeVariant, Permission, RawRelation},
};
use milli::heed::{types::SerdeBincode, Database, RoTxn};
use milli::{ObkvCodec, BEU32, BEU64};
use petgraph::{
    graph::NodeIndex,
    visit::EdgeRef,
    Direction::{self, Incoming, Outgoing},
    Graph,
};
use tracing::error;

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
) -> Result<petgraph::graph::Graph<NodeVariant, EdgeType>, ArunaError> {
    let mut graph = petgraph::graph::Graph::new();

    documents.iter(rtxn)?.try_for_each(|entry| {
        let (_id, obkv) = entry.map_err(|e| ArunaError::ServerError(format!("{e}")))?;

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

        graph.add_node(variant);

        Ok::<(), ArunaError>(())
    })?;

    // TODO: Not sure if this can produce interleaving idx invariants
    relation_idx.store(0, std::sync::atomic::Ordering::Relaxed);
    relations.iter(&rtxn)?.try_for_each(|entry| {
        let (_idx, relation) = entry.map_err(|e| ArunaError::ServerError(format!("{e}")))?;
        relation_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        graph.add_edge(
            relation.source.into(),
            relation.target.into(),
            relation.edge_type,
        );
        Ok::<(), ArunaError>(())
    })?;
    Ok(graph)
}

pub fn get_relations(
    graph: &Graph<NodeVariant, EdgeType>,
    idx: u32,
    filter: Option<&[EdgeType]>,
    direction: Direction,
) -> Vec<RawRelation> {
    let idx = idx.into();
    graph
        .edges_directed(idx, direction)
        .filter_map(|e| {
            if let Some(filter) = filter {
                if !filter.contains(e.weight()) {
                    return None;
                }
            }
            match direction {
                Direction::Outgoing => Some(RawRelation {
                    target: e.target().as_u32(),
                    source: idx.as_u32(),
                    edge_type: *e.weight(),
                }),
                Direction::Incoming => Some(RawRelation {
                    source: e.source().as_u32(),
                    target: idx.as_u32(),
                    edge_type: *e.weight(),
                }),
            }
        })
        .collect()
}

pub fn get_relatives(
    graph: &Graph<NodeVariant, EdgeType>,
    idx: u32,
    direction: Direction,
) -> Vec<u32> {
    let idx = idx.into();
    graph
        .edges_directed(idx, direction)
        .filter_map(|e| {
            if e.weight() == &0 {
                let target_idx = match direction {
                    Direction::Outgoing => e.target(),
                    Direction::Incoming => e.source(),
                };

                match graph.node_weight(target_idx) {
                    None => None,
                    Some(variant) => match variant {
                        NodeVariant::ResourceProject
                        | NodeVariant::ResourceFolder
                        | NodeVariant::ResourceObject => Some(target_idx.as_u32()),
                        _ => None,
                    },
                }
            } else {
                None
            }
        })
        .collect()
}

pub fn get_parent(graph: &Graph<NodeVariant, EdgeType>, idx: u32) -> Option<u32> {
    let parents = get_relatives(graph, idx, Incoming);
    assert!(parents.len() <= 1);
    parents.first().copied()
}

#[allow(unused)]
pub fn get_children(graph: &Graph<NodeVariant, EdgeType>, idx: u32) -> Vec<u32> {
    get_relatives(graph, idx, Outgoing)
}

/// Perform a breadth-first search for incoming edges to find the highest permission
/// 1. Start at the resource node with a theoretical maximum permission
/// 2. Iterate over all incoming edges
/// 3. If the edge is a colored as a "permission" edge add the source node with the minimum permission to the queue
/// 4. If the edge is a colored as a "hierarchy related non permission" edge add the source node with the previous current perm to the queue
/// 5. If the source node is the target node, update the highest permission to the maximum of the current permission and the previous highest permission
/// 6. If a constraint is reached, remove all other nodes from the queue and continue with the constraint node
#[tracing::instrument(level = "trace", skip(graph))]
pub fn get_permissions(
    graph: &Graph<NodeVariant, EdgeType>,
    resource_idx: u32,
    identity: u32,
    constraint: Option<u32>,
) -> Result<Permission, ArunaError> {
    use crate::constants::relation_types::*;
    // Resource could be: Group, User, Projects, Folder, Object || TODO: Realm, ServiceAccount, Hooks, etc.

    let mut highest_perm: Option<u32> = None;
    let mut queue = VecDeque::new();
    let mut reached_constraint = !constraint.is_some();
    queue.push_back((resource_idx.into(), u32::MAX));
    while let Some((idx, current_perm)) = queue.pop_front() {
        // Iterate over all incoming edges
        for edge in graph.edges_directed(idx, Incoming) {
            match edge.weight() {
                // If the edge is a "permission related" edge
                &HAS_PART | &OWNS_PROJECT | &SHARES_PERMISSION | &GROUP_ADMINISTRATES_REALM => {
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
    graph: &Graph<NodeVariant, EdgeType>,
    idx: u32,
    variant: &NodeVariant,
) -> bool {
    graph.node_weight(idx.into()) == Some(variant)
}

pub fn has_relation(
    graph: &Graph<NodeVariant, EdgeType>,
    source_idx: u32,
    target_idx: u32,
    edge_type: &[EdgeType],
) -> bool {
    let source_idx = source_idx.into();
    let target_idx = target_idx.into();
    graph
        .edges_connecting(source_idx, target_idx)
        .any(|e| edge_type.contains(e.weight()))
}

#[tracing::instrument(level = "trace", skip(graph))]
pub fn get_related_user_or_groups(
    graph: &Graph<NodeVariant, EdgeType>,
    resource_idx: u32,
) -> Result<Vec<u32>, ArunaError> {
    use crate::constants::relation_types::*;
    let mut user_or_group_idx = vec![];
    let mut queue = VecDeque::new();
    queue.push_back(resource_idx.into());
    while let Some(idx) = queue.pop_front() {
        // Iterate over all incoming edges
        for edge in graph.edges_directed(idx, Incoming) {
            match edge.weight() {
                // We know that this is a group or user
                &OWNS_PROJECT | PERMISSION_READ..=PERMISSION_ADMIN => {
                    user_or_group_idx.push(edge.source().as_u32());
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

#[tracing::instrument(level = "trace", skip(graph))]
pub fn get_realm_and_groups(
    graph: &Graph<NodeVariant, EdgeType>,
    user_idx: u32,
) -> Result<Vec<u32>, ArunaError> {
    use crate::constants::relation_types::*;
    let mut indizes = vec![user_idx];
    let mut queue = VecDeque::new();
    queue.push_back(user_idx.into());
    while let Some(idx) = queue.pop_front() {
        // Iterate over all incoming edges
        for edge in graph.edges_directed(idx, Outgoing) {
            match edge.weight() {
                // The target is a group (but may have child groups)
                PERMISSION_READ..=PERMISSION_ADMIN
                    if graph.node_weight(edge.target()) == Some(&NodeVariant::Group) =>
                {
                    indizes.push(edge.target().as_u32());
                    // Loop detection
                    if !indizes.contains(&edge.source().as_u32()) {
                        queue.push_back(edge.source());
                    }
                }
                // The target is a realm
                &GROUP_ADMINISTRATES_REALM | &GROUP_PART_OF_REALM => {
                    indizes.push(edge.target().as_u32());
                }
                _ => {}
            }
        }
    }
    Ok(indizes)
}

#[tracing::instrument(level = "trace", skip(graph))]
pub fn get_realms(
    graph: &Graph<NodeVariant, EdgeType>,
    user_idx: u32,
) -> Result<Vec<u32>, ArunaError> {
    use crate::constants::relation_types::*;
    let mut realms = vec![];
    let mut group_loop_detection = vec![];
    let mut queue = VecDeque::new();
    queue.push_back(user_idx.into());
    while let Some(idx) = queue.pop_front() {
        // Iterate over all incoming edges
        for edge in graph.edges_directed(idx, Outgoing) {
            match edge.weight() {
                // The target is a group (but may have child groups)
                PERMISSION_READ..=PERMISSION_ADMIN
                    if graph.node_weight(edge.target()) == Some(&NodeVariant::Group) =>
                {
                    // Loop detection
                    if !group_loop_detection.contains(&edge.target()) {
                        group_loop_detection.push(edge.target());
                        queue.push_back(edge.target());
                    }
                }
                // The target is a realm
                &GROUP_ADMINISTRATES_REALM | &GROUP_PART_OF_REALM => {
                    realms.push(edge.target().as_u32());
                }
                _ => {}
            }
        }
    }
    Ok(realms)
}

#[tracing::instrument(level = "trace", skip(graph))]
pub fn get_subtree(
    graph: &Graph<NodeVariant, EdgeType>,
    node_idx: u32,
) -> Result<Vec<u32>, ArunaError> {
    let mut subtree = vec![];

    let mut current_node_variant = graph.node_weight(node_idx.into()).ok_or_else(|| {
        error!("Node with idx {node_idx} not found");
        ArunaError::NotFound(format!("Node with idx {node_idx} not found"))
    })?;

    let mut current_node_id: NodeIndex = node_idx.into();

    loop {
        match current_node_variant {
            NodeVariant::ResourceProject => {
                subtree.push(current_node_id.index() as u32);
                break;
            }
            NodeVariant::ResourceFolder | NodeVariant::ResourceObject => {
                subtree.push(current_node_id.index() as u32);
                let Some(parent) = get_parent(graph, current_node_id.index() as u32) else {
                    error!("Parent not found for {node_idx}");
                    return Err(ArunaError::NotFound(format!(
                        "Parent not found for {node_idx}"
                    )));
                };
                current_node_id = parent.into();
                current_node_variant = graph.node_weight(current_node_id).ok_or_else(|| {
                    error!("Node with idx {:?} not found", current_node_id);
                    ArunaError::NotFound(format!("Node with idx {:?} not found", current_node_id))
                })?;
            }
            _ => {
                subtree.push(current_node_id.index() as u32);
                break;
            }
        }
    }

    Ok(subtree)
}


#[tracing::instrument(level = "trace", skip(graph))]
pub fn get_all_children(
    graph: &Graph<NodeVariant, EdgeType>,
    resource_idx: u32,
) -> Result<Vec<u32>, ArunaError> {
    let mut result = Vec::new();
    let mut queue = VecDeque::new();
    queue.push_front(resource_idx);
    while let Some(node_id) = queue.pop_front() {
        let children = get_children(graph, node_id);
        result.extend(children.clone());
        queue.extend(children);
    }
    Ok(result)
}
