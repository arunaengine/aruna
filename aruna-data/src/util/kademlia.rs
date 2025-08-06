use aruna_net::Kademlia;
use iroh::NodeAddr;

use crate::error::ArunaDataError;

pub async fn get_data_locations(
    kademlia: &Kademlia,
    target: [u8; 32],
) -> Result<Vec<NodeAddr>, ArunaDataError> {
    Ok(kademlia
        .find_value(target)
        .await
        .map_err(|e| ArunaDataError::ServerError(e.to_string()))?
        .into_iter()
        .map(|v| v.addr().clone())
        .collect())
}
