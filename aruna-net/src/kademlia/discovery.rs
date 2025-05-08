use iroh::{
    Endpoint, NodeId,
    discovery::{Discovery, DiscoveryItem},
    node_info::{NodeData, NodeInfo},
};

use n0_future::boxed::BoxStream;
use tracing::{error, trace};

use super::kademlia::Kademlia;

impl Discovery for Kademlia {
    fn publish(&self, _data: &NodeData) {}

    fn resolve(
        &self,
        _endpoint: Endpoint,
        node_id: NodeId,
    ) -> Option<BoxStream<anyhow::Result<DiscoveryItem>>> {
        trace!("resolve {:?}", node_id);

        let target = *node_id.as_bytes();
        let self_clone = self.clone();

        let fut = async move {
            trace!("trying to resolve {:?}", target);

            let result = match self_clone.find(target, true).await {
                Ok(find_result) => {
                    trace!("resolve found {:?}", target);
                    find_result
                }
                Err(e) => {
                    trace!("resolve error {:?}", e);
                    return Err(anyhow::anyhow!("resolve error {:?}", e));
                }
            };

            trace!("resolve result {:?}", result);
            let node_addr = result
                .value
                .first()
                .ok_or_else(|| {
                    error!("No nodes found for target {:?}", target);
                    anyhow::anyhow!("No nodes found for target {:?}", target)
                })?
                .addr();

            let node_data = NodeData::new(
                node_addr.relay_url.clone(),
                node_addr.direct_addresses.clone(),
            );

            Ok(DiscoveryItem::new(
                NodeInfo::from_parts(node_id, node_data),
                "ARUNA_KADEMLIA",
                None,
            ))
        };

        let stream = n0_future::stream::once_future(fut);

        Some(Box::pin(stream))
    }
}
