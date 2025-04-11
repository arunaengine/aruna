use std::net::{Ipv4Addr, SocketAddrV4};

use anyhow::Ok;
use aruna_net::connection_handler::ConnectionHandlerBuilder;
use log::debug;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let chandler1 = ConnectionHandlerBuilder::new(None)
        .add_bind_addr_v4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 31337))
        .build(vec![])
        .await
        .unwrap();

    let c1_addr = chandler1.get_node_addr().await?;
    debug!("Node 1 address: {:?}", c1_addr.node_id);

    let chander2 = ConnectionHandlerBuilder::new(None)
        .add_bind_addr_v4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 31338))
        .build(vec![c1_addr])
        .await
        .unwrap();
    let c2_addr = chander2.get_node_addr().await?;
    debug!("Node 2 address: {:?}", c2_addr.node_id);

    let result = chandler1.find(*c2_addr.node_id.as_bytes()).await?;

    debug!("Result: {:?}", result);
    assert_eq!(*result.value.first().unwrap(), c2_addr);
    Ok(())
}
