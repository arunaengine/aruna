use aruna_net::actor::NetworkActorBuilder;
use std::net::{Ipv4Addr, SocketAddrV4};

#[tokio::main]
pub async fn main() {
    let _chandler = NetworkActorBuilder::new(None)
        .await
        .add_bind_addr_v4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 31337))
        .build(vec![])
        .await
        .unwrap();

    //transfer_manager::init(chandler.clone()).await.unwrap();
}
