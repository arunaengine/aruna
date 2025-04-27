use anyhow::Result;
use aruna_net::KademliaActorHandle;
use iroh::NodeAddr;

pub async fn get_data_locations(
    kademlia: &KademliaActorHandle,
    target: [u8; 32],
) -> Result<Vec<NodeAddr>> {
    Ok(kademlia.find(target).await?.nodes)
}
