use iroh::{
    Endpoint, NodeId,
    discovery::{Discovery, DiscoveryItem},
    node_info::{NodeData, NodeInfo},
};

use crate::Kademlia;
use n0_future::boxed::BoxStream;

impl Discovery for Kademlia {
    fn publish(&self, _data: &NodeData) {}

    fn resolve(
        &self,
        _endpoint: Endpoint,
        node_id: NodeId,
    ) -> Option<BoxStream<anyhow::Result<DiscoveryItem>>> {
        let target = node_id.as_bytes().clone();
        let arc_self = self.get_self();
        let fut = async move {
            let target = arc_self.find(target).await?;
            Ok(DiscoveryItem::new(
                NodeInfo::from_parts(node_id, NodeData::from(target.value.unwrap())),
                "ARUNA_KADEMLIA",
                None,
            ))
        };

        let stream = n0_future::stream::once_future(fut);

        Some(Box::pin(stream))
    }
}
