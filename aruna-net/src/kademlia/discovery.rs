use std::pin::Pin;

use futures::Stream;
use iroh::{
    NodeId,
    discovery::{Discovery, DiscoveryError, DiscoveryItem},
    node_info::{NodeData, NodeInfo},
};
use tracing::{error, trace};
use super::kademlia::Kademlia;

#[derive(Debug)]
pub struct ErrorWrapper {
    error: String,
}
impl std::fmt::Display for ErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let path = &self.error;
        write!(f, "unable to read configuration at {path}")
    }
}

impl std::error::Error for ErrorWrapper {}

impl Discovery for Kademlia {
    fn publish(&self, _data: &NodeData) {}

    fn resolve(
        &self,
        //_endpoint: Endpoint,
        node_id: NodeId,
    ) -> std::option::Option<
        Pin<
            Box<
                (
                    dyn Stream<Item = Result<DiscoveryItem, DiscoveryError>>
                        + std::marker::Send
                        + 'static
                ),
            >,
        >,
    > {
        trace!("resolve {:?}", node_id);

        let target = *node_id.as_bytes();
        let self_clone = self.clone();

        let fut = async move {
            trace!("trying to resolve {:?}", target);

            let result = match self_clone.find_value(target).await {
                Ok(find_result) => {
                    trace!("resolve found {:?}", target);
                    find_result
                }
                Err(e) => {
                    trace!("resolve error {:?}", e);
                    let err = ErrorWrapper {
                        error: e.to_string(),
                    };
                    return Err(DiscoveryError::from_err("anyhow", err));
                    //return Err(anyhow::anyhow!("resolve error {:?}", e));
                }
            };

            trace!("resolve result {:?}", result);
            let node_addr = result
                .first()
                .ok_or_else(|| {
                    error!("No nodes found for target {:?}", target);
                    let e = ErrorWrapper {
                        error: format!("No nodes found for target {:?}", target),
                    };
                    DiscoveryError::from_err("anyhow_error", e)
                })?
                .addr();

            let node_data = NodeData::new(
                node_addr.relay_url.clone(),
                node_addr.direct_addresses.clone(),
            );

            Ok::<DiscoveryItem, DiscoveryError>(DiscoveryItem::new(
                NodeInfo::from_parts(node_id, node_data),
                "ARUNA_KADEMLIA",
                None,
            ))
        };

        let stream = n0_future::stream::once_future(fut);

        Some(Box::pin(stream))
    }
}
