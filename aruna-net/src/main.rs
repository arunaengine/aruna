use std::net::{Ipv4Addr, SocketAddrV4};

use anyhow::Ok;
use aruna_net::connection_handler::ConnectionHandlerBuilder;
use tracing::debug;
use tracing_subscriber::EnvFilter;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("aruna-net=debug".parse()?);

    let subscriber = tracing_subscriber::fmt()
        //.with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        // Use a more compact, abbreviated log format
        .compact()
        // Set LOG_LEVEL to
        .with_env_filter(filter)
        // Display source code file paths
        .with_file(true)
        // Display source code line numbers
        .with_line_number(true)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

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
