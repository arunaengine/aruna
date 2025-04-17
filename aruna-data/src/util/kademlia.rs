use anyhow::Result;
use aruna_net::Kademlia;
use iroh::NodeAddr;

pub async fn get_data_locations(kademlia: &Kademlia, target: [u8; 32]) -> Result<Vec<NodeAddr>> {
    Ok(kademlia.find(target, false).await?.nodes)
}
