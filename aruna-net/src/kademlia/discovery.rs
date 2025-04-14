use std::sync::Arc;

use iroh::{
    Endpoint, NodeId,
    discovery::{Discovery, DiscoveryItem},
    node_info::{NodeData, NodeInfo},
};

use crate::Kademlia;
use n0_future::boxed::BoxStream;

#[derive(Clone, Debug)]
pub struct KademliaArc {
    pub kademlia: Arc<Kademlia>,
}

impl Discovery for KademliaArc {
    fn publish(&self, _data: &NodeData) {}

    fn resolve(
        &self,
        _endpoint: Endpoint,
        node_id: NodeId,
    ) -> Option<BoxStream<anyhow::Result<DiscoveryItem>>> {
        let target = node_id.as_bytes().clone();
        let self_clone = self.clone();
        let fut = async move {
            let target = self_clone.kademlia.find(target, true).await?;
            Ok(DiscoveryItem::new(
                NodeInfo::from_parts(
                    node_id,
                    NodeData::from(
                        target
                            .value
                            .first()
                            .cloned()
                            .ok_or_else(|| anyhow::anyhow!("Node not found"))?,
                    ),
                ),
                "ARUNA_KADEMLIA",
                None,
            ))
        };

        let stream = n0_future::stream::once_future(fut);

        Some(Box::pin(stream))
    }
}
