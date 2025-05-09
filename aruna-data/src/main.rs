use anyhow::Result;
use aruna_data::{config::config::Config, util::opendal::get_operator};
use aruna_net::actor::NetworkActorBuilder;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;

use aruna_data::io::replication_handler::{REPLICATION_PROTOCOL_ID, ReplicationHandler};
use lazy_static::lazy_static;
use tracing::{Level, debug, error};
use tracing_subscriber::EnvFilter;
use ulid::Ulid;

lazy_static! {
    static ref CONFIG: Config = {
        dotenvy::from_filename(".env").ok();
        let config_file = dotenvy::var("CONFIG").unwrap_or("config.toml".to_string());
        let config: Config =
            toml::from_str(std::fs::read_to_string(config_file).unwrap().as_str()).unwrap();
        config
    };
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("aruna_data=trace".parse()?)
        .add_directive("aruna_net=trace".parse()?)
        .add_directive("iroh=info".parse()?);

    let subscriber = tracing_subscriber::fmt()
        //.with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        // Use a more compact, abbreviated log format
        .compact()
        .with_max_level(Level::DEBUG)
        .with_env_filter(filter)
        // Display source code file paths
        .with_file(true)
        // Display source code line numbers
        .with_line_number(true)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    debug!(config = ?*CONFIG);
    // Dummy access conf which is provided by user/request/node
    let conf = CONFIG.backend.access.clone();
    let operator = get_operator(&CONFIG.backend.backend_type, conf).await?;

    match operator.check().await {
        Ok(_) => debug!(
            "Connection to {} backend succeeded",
            CONFIG.backend.backend_type
        ),
        //Err(err) => info!(format!("Connection to backend failed: {}", err)),
        Err(err) => error!("Connection to backend failed: {}", err),
    }

    // Create an endpoint, it allows creating and accepting connections in the iroh p2p world
    let repl_handler_01 = Arc::new(ReplicationHandler::new(operator));
    let chandler1 = NetworkActorBuilder::new(None)
        .await
        .add_bind_addr_v4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 31337))
        .build(vec![])
        .await?;

    let node_addr = chandler1.get_node_addr().await?;
    let chandler1 = chandler1.new_actor_handle(REPLICATION_PROTOCOL_ID).await?;
    let kademlia = chandler1.get_kademlia_actor_handle().await?;

    repl_handler_01
        .add_connection_handler(node_addr.clone(), chandler1, kademlia.clone())
        .await;
    let _ = repl_handler_01.dummy_sync().await;

    debug!("Node 1 address: {:?}", node_addr);

    // Node 2
    let mut conf2 = CONFIG.backend.access.clone();
    conf2.insert("bucket".to_string(), "another-bucket".to_string());
    let operator2 = get_operator(&CONFIG.backend.backend_type, conf2).await?;
    let repl_handler_02 = Arc::new(ReplicationHandler::new(operator2));
    let chandler2 = NetworkActorBuilder::new(None)
        .await
        .add_bind_addr_v4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 31338))
        .build(vec![node_addr.clone()])
        .await?;

    let node_addr2 = chandler2.get_node_addr().await?;
    let chandler2 = chandler2.new_actor_handle(REPLICATION_PROTOCOL_ID).await?;
    let kademlia2 = chandler2.get_kademlia_actor_handle().await?;
    repl_handler_02
        .add_connection_handler(node_addr2.clone(), chandler2, kademlia2.clone())
        .await;

    debug!("Node 2 address: {:?}", node_addr2);

    let result = kademlia.find_value(*node_addr2.node_id.as_bytes()).await?;

    debug!("Result: {:?}", result);

    let hash = repl_handler_01
        .replicate_object_to_node(
            Ulid::new(),
            repl_handler_01
                .local_store
                .read()
                .await
                .get("360a1b94a7f997552cda3856869f6d24")
                .unwrap()
                .file_path
                .to_string(),
            node_addr2,
        )
        .await?;

    debug!(
        "Data location found in Node 1: {:?}",
        kademlia.find_value(hash).await?
    );
    debug!(
        "Data location found in Node 2: {:?}",
        kademlia2.find_value(hash).await?
    );

    Ok(())
}
