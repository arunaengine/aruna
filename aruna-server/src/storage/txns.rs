use super::{
    graph::{Graph, GraphTxn, IndexHelper},
    store::single_entry_names,
};
use crate::{
    error::ArunaError,
    logerr,
    models::models::{EdgeType, MilliIdx, NodeVariant, Subscriber},
};
use ahash::HashSet;
use milli::heed::{
    byteorder::BigEndian,
    types::{SerdeBincode, Str, U128},
    Database, RoTxn, RwTxn, Unspecified,
};
use milli::BEU32;
use petgraph::graph::{EdgeIndex, NodeIndex};
use std::sync::RwLock;

pub trait Txn<'a> {
    fn get_ro_txn(&self) -> &RoTxn<'a>;
    fn get_ro_graph(&self) -> GraphTxn;
}

pub struct WriteTxn<'a> {
    pub(super) txn: Option<RwTxn<'a>>,
    pub(super) graph: &'a RwLock<Graph>,
    pub(super) events: &'a Database<BEU32, U128<BigEndian>>,
    pub(super) subscribers: &'a Database<U128<BigEndian>, SerdeBincode<Vec<u128>>>,
    pub(super) single_entry_database: &'a Database<Unspecified, Unspecified>,
    pub(super) nodes: Vec<NodeIndex>,
    pub(super) added_edges: Vec<EdgeIndex>,
    pub(super) removed_edges: Vec<(NodeIndex, NodeIndex, EdgeType)>,
    pub(super) committed: bool,
}

impl<'a> Txn<'a> for WriteTxn<'a> {
    fn get_ro_txn(&self) -> &RoTxn<'a> {
        self.txn.as_ref().expect("Transaction already committed")
    }
    fn get_ro_graph(&self) -> GraphTxn {
        GraphTxn {
            state: self.graph.read().expect("Poisoned lock"),
            mode: crate::storage::graph::Mode::WriteTxn,
        }
    }
}

impl<'a> WriteTxn<'a> {
    pub fn get_txn(&mut self) -> &mut RwTxn<'a> {
        self.txn.as_mut().expect("Transaction already committed")
    }

    pub(super) fn add_node(&mut self, milli_idx: MilliIdx, node: NodeVariant) -> NodeIndex {
        let mut lock = self.graph.write().expect("Poisoned lock");
        let idx = lock.graph.add_node(node);

        lock.idx_mappings
            .graph_milli
            .insert(idx.as_u32() as usize, milli_idx.0);
        lock.idx_mappings
            .milli_graph
            .insert(milli_idx.0 as usize, idx.as_u32());
        lock.in_flight_tx.nodes.push(milli_idx.0);
        self.nodes.push(idx);
        idx
    }

    pub(super) fn add_edge(
        &mut self,
        source: MilliIdx,
        target: MilliIdx,
        edge_type: EdgeType,
    ) -> Result<(), ArunaError> {
        let mut lock = self.graph.write().expect("Poisoned lock");
        let source_idx = *lock
            .idx_mappings
            .milli_graph
            .get(source.0 as usize)
            .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?;
        let target_idx = *lock
            .idx_mappings
            .milli_graph
            .get(target.0 as usize)
            .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?;
        let idx = lock
            .graph
            .add_edge(source_idx.into(), target_idx.into(), edge_type);

        lock.in_flight_tx.edges.push(idx.index() as u32);
        // TODO: Add to mappings?
        self.added_edges.push(idx);
        Ok(())
    }

    pub(super) fn remove_edge(&mut self, index: EdgeIndex) -> Option<u32> {
        let (from, to) = self
            .graph
            .write()
            .expect("Poisoned lock")
            .graph
            .edge_endpoints(index)?;
        let weight = self
            .graph
            .write()
            .expect("Poisoned lock")
            .graph
            .remove_edge(index)?;
        // TODO: Remove to mappings?
        self.removed_edges.push((from, to, weight));
        Some(weight)
    }

    // This is a read-only function that allows for the graph to be accessed
    // SAFETY: The caller must guarantee that the graph is not modified with this guard

    pub fn commit(
        mut self,
        event_id: u128,
        targets: &[MilliIdx],
        additional_affected: &[MilliIdx],
    ) -> Result<(), ArunaError> {
        let mut txn = self.txn.take().expect("Transaction already committed");

        let mut affected = HashSet::from_iter(targets.iter().cloned());
        affected.extend(additional_affected.iter().cloned());

        for target in targets {
            self.events
                .put(&mut txn, &target.0, &event_id)
                .inspect_err(logerr!())?;
            affected.extend(
                GraphTxn {
                    state: self.graph.read().expect("Poisoned lock"),
                    mode: super::graph::Mode::WriteTxn,
                }
                .get_subtree(
                    // not sure if we should
                    // use one lock over this loop
                    *target,
                )?,
            );
        }

        let mut graph_lock = self.graph.write().expect("Poisoned lock");

        graph_lock.in_flight_tx.nodes.clear();
        graph_lock.in_flight_tx.edges.clear();

        drop(graph_lock);

        let db = self
            .single_entry_database
            .remap_types::<Str, SerdeBincode<Vec<Subscriber>>>();

        let subscribers = db
            .get(&txn, single_entry_names::SUBSCRIBER_CONFIG)
            .inspect_err(logerr!())?;

        if let Some(subscribers) = subscribers {
            for subscriber in subscribers {
                if affected
                    .iter()
                    .any(|affected| subscriber.target_idx == affected.0)
                {
                    let mut subscriber_events = self
                        .subscribers
                        .get(&txn, &subscriber.id.0)
                        .inspect_err(logerr!())?
                        .unwrap_or_default();
                    subscriber_events.push(event_id);
                    self.subscribers
                        .put(&mut txn, &subscriber.id.0, &subscriber_events)
                        .inspect_err(logerr!())?;
                }
            }
        }

        txn.commit().inspect_err(logerr!())?;
        self.committed = true;
        Ok(())
    }
}

impl<'a> Drop for WriteTxn<'a> {
    fn drop(&mut self) {
        if !self.committed {
            let mut lock = self.graph.write().expect("Poisoned lock");

            lock.in_flight_tx.edges.clear();
            for idx in self.added_edges.drain(..) {
                lock.graph.remove_edge(idx);
            }

            lock.in_flight_tx.nodes.clear();
            for idx in self.nodes.drain(..) {
                lock.graph.remove_node(idx);
            }

            for (from, to, weight) in self.removed_edges.drain(..) {
                lock.graph.add_edge(from, to, weight);
            }
        }
    }
}

pub struct ReadTxn<'a> {
    pub(super) txn: milli::heed::RoTxn<'a>,
    pub(super) graph: &'a RwLock<Graph>,
}

impl<'a> ReadTxn<'a> {
    pub fn commit(self) -> Result<(), ArunaError> {
        self.txn.commit().inspect_err(logerr!());
        Ok(())
    }
}

impl<'a> Txn<'a> for ReadTxn<'a> {
    fn get_ro_txn(&self) -> &RoTxn<'a> {
        &self.txn
    }
    fn get_ro_graph(&self) -> GraphTxn {
        GraphTxn {
            state: self.graph.read().expect("Poisoned lock"),
            mode: crate::storage::graph::Mode::ReadTxn,
        }
    }
}
