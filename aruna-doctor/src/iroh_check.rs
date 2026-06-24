use crate::error::CliError;
use crate::info::{default_info_url_from_env, fetch_info_url_with_timeout};
use aruna_api::routes::info::{
    InfoResponse, NetworkServiceStatus, PeerConnectionInfo, ServiceStatus,
};
use aruna_core::alpn::Alpn;
use aruna_core::telemetry::duration_ms;
use aruna_net::dht::rpc::{DhtRequest, DhtResponse, decode_response, encode_request};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, RelayMap, RelayMode, TransportAddr};
use serde::Serialize;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::{Duration, Instant};

const MAX_DHT_RESPONSE_SIZE: usize = 1024 * 1024;

#[derive(Debug, Serialize)]
struct IrohCheckOutput {
    status: &'static str,
    info_url: String,
    target: IrohCheckTarget,
    reported: ReportedNetDiagnostics,
    active: ActiveIrohCheck,
    checks: Vec<IrohCheckStep>,
}

#[derive(Debug, Serialize)]
struct IrohCheckTarget {
    realm_id: Option<String>,
    node_id: Option<String>,
    endpoint_addr: serde_json::Value,
    addresses: Vec<String>,
    has_direct_ip: bool,
    has_relay: bool,
}

#[derive(Debug, Serialize)]
struct ReportedNetDiagnostics {
    network: NetworkServiceStatus,
    connections: Vec<PeerConnectionInfo>,
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ActiveIrohCheck {
    doctor_node_id: String,
    remote_id: String,
    negotiated_alpn: String,
    rtt_ms: Option<u64>,
    selected_path: Option<IrohPathStatus>,
    paths: Vec<IrohPathStatus>,
}

#[derive(Debug, Clone, Serialize)]
struct IrohPathStatus {
    id: String,
    remote_addr: String,
    selected: bool,
    closed: bool,
    rtt_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
struct IrohCheckStep {
    name: &'static str,
    status: &'static str,
    duration_ms: u64,
}

pub async fn print_iroh_check(info_url: Option<String>, timeout_secs: u64) -> Result<(), CliError> {
    let _ = dotenvy::dotenv();
    let info_url = match info_url {
        Some(info_url) => info_url,
        None => default_info_url_from_env()?,
    };
    let output = run_iroh_check(info_url, Duration::from_secs(timeout_secs)).await?;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

async fn run_iroh_check(info_url: String, timeout: Duration) -> Result<IrohCheckOutput, CliError> {
    let mut checks = Vec::new();

    let info_started = Instant::now();
    let endpoint_info = fetch_info_url_with_timeout(&info_url, timeout).await?;
    checks.push(ok_step("info", info_started.elapsed()));

    if endpoint_info.services.network.status != ServiceStatus::Available {
        return Err(iroh_check_error(
            "info",
            format!(
                "network state is {:?}",
                endpoint_info.services.network.status
            ),
        ));
    }

    let endpoint_started = Instant::now();
    let endpoint_addr = endpoint_addr_from_info(&endpoint_info)?;
    let endpoint_addr = normalize_endpoint_addr(endpoint_addr, &info_url)?;
    checks.push(ok_step("endpoint_addr", endpoint_started.elapsed()));

    let active = active_iroh_check(endpoint_addr.clone(), timeout, &mut checks).await?;

    Ok(IrohCheckOutput {
        status: "ok",
        info_url,
        target: target_output(&endpoint_info, &endpoint_addr),
        reported: reported_diagnostics(&endpoint_info),
        active,
        checks,
    })
}

fn endpoint_addr_from_info(endpoint_info: &InfoResponse) -> Result<EndpointAddr, CliError> {
    let peer_id = endpoint_info
        .node
        .peer_id
        .parse::<iroh::PublicKey>()
        .map_err(|error| iroh_check_error("endpoint_addr", error))?;
    let addresses = endpoint_info
        .my_addresses
        .iter()
        .map(|address| transport_addr_from_info(address))
        .collect::<Result<Vec<_>, _>>()?;

    if addresses.is_empty() {
        return Err(iroh_check_error("endpoint_addr", "missing my_addresses"));
    }

    Ok(EndpointAddr::from_parts(peer_id, addresses))
}

fn transport_addr_from_info(address: &str) -> Result<TransportAddr, CliError> {
    if address.starts_with("http://") || address.starts_with("https://") {
        return address
            .parse::<iroh::RelayUrl>()
            .map(TransportAddr::Relay)
            .map_err(|error| iroh_check_error("endpoint_addr", error));
    }

    address
        .parse::<SocketAddr>()
        .map(TransportAddr::Ip)
        .map_err(|error| iroh_check_error("endpoint_addr", error))
}

fn normalize_endpoint_addr(
    endpoint_addr: EndpointAddr,
    info_url: &str,
) -> Result<EndpointAddr, CliError> {
    let replacement_ip = replacement_ip_from_url(info_url);
    let mut addrs = Vec::with_capacity(endpoint_addr.addrs.len());
    for addr in endpoint_addr.addrs {
        match addr {
            TransportAddr::Ip(socket) if socket.ip().is_unspecified() => {
                let Some(ip) = replacement_ip else {
                    return Err(iroh_check_error(
                        "endpoint_addr",
                        "target advertised a wildcard p2p address; use an IP-literal info URL or configure a dialable p2p address",
                    ));
                };
                addrs.push(TransportAddr::Ip(SocketAddr::new(ip, socket.port())));
            }
            addr => addrs.push(addr),
        }
    }

    Ok(EndpointAddr::from_parts(endpoint_addr.id, addrs))
}

async fn active_iroh_check(
    endpoint_addr: EndpointAddr,
    timeout: Duration,
    checks: &mut Vec<IrohCheckStep>,
) -> Result<ActiveIrohCheck, CliError> {
    let bind_addr = local_bind_addr_for(&endpoint_addr);
    let relay_mode = relay_mode_for_target(&endpoint_addr)?;
    let endpoint_builder = Endpoint::builder(presets::Minimal)
        .relay_mode(relay_mode)
        .alpns(vec![Alpn::Dht.as_bytes().to_vec()])
        .bind_addr(bind_addr)
        .map_err(|error| iroh_check_error("bind", error))?;
    let endpoint = endpoint_builder
        .bind()
        .await
        .map_err(|error| iroh_check_error("bind", error))?;

    let check = active_iroh_check_with_endpoint(&endpoint, endpoint_addr, timeout, checks).await;
    endpoint.close().await;
    check
}

async fn active_iroh_check_with_endpoint(
    endpoint: &Endpoint,
    endpoint_addr: EndpointAddr,
    timeout: Duration,
    checks: &mut Vec<IrohCheckStep>,
) -> Result<ActiveIrohCheck, CliError> {
    let connect_started = Instant::now();
    let conn = with_timeout(
        "iroh_connect",
        timeout,
        endpoint.connect(endpoint_addr, Alpn::Dht.as_bytes()),
    )
    .await?;
    checks.push(ok_step("iroh_connect", connect_started.elapsed()));

    let ping_started = Instant::now();
    let (mut send, mut recv) = with_timeout("open_dht_stream", timeout, conn.open_bi()).await?;
    let request =
        encode_request(&DhtRequest::Ping).map_err(|error| iroh_check_error("dht_ping", error))?;
    let len = (request.len() as u32).to_be_bytes();
    with_timeout("write_dht_ping", timeout, send.write_all(&len)).await?;
    with_timeout("write_dht_ping", timeout, send.write_all(&request)).await?;
    send.finish()
        .map_err(|error| iroh_check_error("write_dht_ping", error))?;

    let mut response_len = [0u8; 4];
    with_timeout("read_dht_pong", timeout, recv.read_exact(&mut response_len)).await?;
    let response_len = u32::from_be_bytes(response_len) as usize;
    if response_len > MAX_DHT_RESPONSE_SIZE {
        return Err(iroh_check_error(
            "read_dht_pong",
            format!("response too large: {response_len} bytes"),
        ));
    }

    let mut response = vec![0u8; response_len];
    with_timeout("read_dht_pong", timeout, recv.read_exact(&mut response)).await?;
    let response =
        decode_response(&response).map_err(|error| iroh_check_error("dht_ping", error))?;
    if !matches!(response, DhtResponse::Pong) {
        return Err(iroh_check_error(
            "dht_ping",
            format!("unexpected response: {response:?}"),
        ));
    }
    checks.push(ok_step("dht_ping", ping_started.elapsed()));

    let paths = path_statuses(&conn);
    let selected_path = paths.iter().find(|path| path.selected).cloned();
    let rtt_ms = selected_path.as_ref().and_then(|path| path.rtt_ms);
    let active = ActiveIrohCheck {
        doctor_node_id: endpoint.id().to_string(),
        remote_id: conn.remote_id().to_string(),
        negotiated_alpn: String::from_utf8_lossy(conn.alpn()).to_string(),
        rtt_ms,
        selected_path,
        paths,
    };
    conn.close(0u8.into(), b"aruna-doctor iroh check complete");

    Ok(active)
}

fn relay_mode_for_target(endpoint_addr: &EndpointAddr) -> Result<RelayMode, CliError> {
    let relay_urls = endpoint_addr
        .relay_urls()
        .map(|url| url.to_string())
        .collect::<Vec<_>>();
    if relay_urls.is_empty() {
        return Ok(RelayMode::Disabled);
    }

    let relays = RelayMap::try_from_iter(relay_urls.iter().map(|url| url.as_ref()))
        .map_err(|error| iroh_check_error("relay", error))?;
    Ok(RelayMode::Custom(relays))
}

async fn with_timeout<T, E>(
    step: &'static str,
    timeout: Duration,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, CliError>
where
    E: std::fmt::Display,
{
    let timeout = nonzero_timeout(timeout);
    match tokio::time::timeout(timeout, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(iroh_check_error(step, error)),
        Err(_) => Err(iroh_check_error(
            step,
            format!("timed out after {}s", timeout.as_secs_f64()),
        )),
    }
}

fn target_output(endpoint_info: &InfoResponse, endpoint_addr: &EndpointAddr) -> IrohCheckTarget {
    IrohCheckTarget {
        realm_id: Some(endpoint_info.node.realm_id.clone()),
        node_id: Some(endpoint_info.node.peer_id.clone()),
        endpoint_addr: serde_json::to_value(endpoint_addr).unwrap_or(serde_json::Value::Null),
        addresses: endpoint_addr
            .addrs
            .iter()
            .map(transport_addr_to_string)
            .collect(),
        has_direct_ip: endpoint_addr.addrs.iter().any(TransportAddr::is_ip),
        has_relay: endpoint_addr.addrs.iter().any(TransportAddr::is_relay),
    }
}

fn reported_diagnostics(endpoint_info: &InfoResponse) -> ReportedNetDiagnostics {
    ReportedNetDiagnostics {
        network: endpoint_info.services.network.clone(),
        connections: endpoint_info.connections.clone(),
        warnings: endpoint_info.warnings.clone(),
    }
}

fn path_statuses(conn: &iroh::endpoint::Connection) -> Vec<IrohPathStatus> {
    let paths = conn.paths();
    paths
        .iter()
        .map(|path| IrohPathStatus {
            id: format!("{:?}", path.id()),
            remote_addr: transport_addr_to_string(path.remote_addr()),
            selected: path.is_selected(),
            closed: false, // iroh currently does not provide a way to determine if a path is closed
            rtt_ms: Some(path.rtt().as_millis() as u64),
        })
        .collect()
}

fn local_bind_addr_for(endpoint_addr: &EndpointAddr) -> SocketAddr {
    let has_ipv4 = endpoint_addr.ip_addrs().any(SocketAddr::is_ipv4);
    let has_ipv6 = endpoint_addr.ip_addrs().any(SocketAddr::is_ipv6);
    if has_ipv6 && !has_ipv4 {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0)
    } else {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
    }
}

fn replacement_ip_from_url(info_url: &str) -> Option<IpAddr> {
    let url = reqwest::Url::parse(info_url).ok()?;
    let host = url.host_str()?;
    if host.eq_ignore_ascii_case("localhost") {
        return Some(IpAddr::V4(Ipv4Addr::LOCALHOST));
    }

    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host.parse().ok()
}

fn transport_addr_to_string(addr: &TransportAddr) -> String {
    match addr {
        TransportAddr::Ip(addr) => addr.to_string(),
        TransportAddr::Relay(url) => url.to_string(),
        _ => format!("{addr:?}"),
    }
}

fn ok_step(name: &'static str, duration: Duration) -> IrohCheckStep {
    IrohCheckStep {
        name,
        status: "ok",
        duration_ms: duration_ms(duration),
    }
}

fn nonzero_timeout(timeout: Duration) -> Duration {
    if timeout.is_zero() {
        Duration::from_millis(1)
    } else {
        timeout
    }
}

fn iroh_check_error(step: &'static str, message: impl std::fmt::Display) -> CliError {
    CliError::IrohCheck {
        step,
        message: message.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_api::routes::info::{
        BlobServiceStatus, DatabaseServiceStatus, InfoResponse, InterfaceServicesStatus,
        InterfaceStatus, NetworkServiceStatus, NodeCapabilityKind, NodeStatus, RequestSummary,
        ServicesStatus,
    };
    use aruna_core::structs::RealmId;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::FjallStorage;
    use tempfile::{TempDir, tempdir};

    fn test_info_response(endpoint_addr: Option<EndpointAddr>) -> InfoResponse {
        let node_id = endpoint_addr
            .as_ref()
            .map(|addr| addr.id)
            .unwrap_or_else(|| iroh::SecretKey::from_bytes(&[2u8; 32]).public());
        let my_addresses = endpoint_addr
            .as_ref()
            .map(|addr| addr.addrs.iter().map(transport_addr_to_string).collect())
            .unwrap_or_default();

        InfoResponse {
            node: NodeStatus {
                status: ServiceStatus::Available,
                realm_id: RealmId::from_bytes([1u8; 32]).to_string(),
                peer_id: node_id.to_string(),
                capabilities: NodeCapabilityKind::Management,
            },
            my_addresses,
            connections: Vec::new(),
            services: ServicesStatus {
                network: NetworkServiceStatus {
                    status: ServiceStatus::Available,
                    discovery: Vec::new(),
                    relay: Some("none".to_string()),
                    relay_urls: Vec::new(),
                    routing_table_size: None,
                    requests: RequestSummary {
                        total: 0,
                        failure_rate: 0.0,
                        last_error: None,
                    },
                },
                blob: BlobServiceStatus {
                    status: ServiceStatus::NotConfigured,
                    backend: None,
                    max_bucket_size: None,
                    multipart_bucket: None,
                    timeouts_secs: None,
                },
                database: DatabaseServiceStatus {
                    status: ServiceStatus::Available,
                    requests: RequestSummary {
                        total: 0,
                        failure_rate: 0.0,
                        last_error: None,
                    },
                },
                interfaces: InterfaceServicesStatus {
                    rest: InterfaceStatus {
                        status: ServiceStatus::Available,
                        bind: None,
                        url: None,
                    },
                    s3: InterfaceStatus {
                        status: ServiceStatus::Unavailable,
                        bind: None,
                        url: None,
                    },
                },
            },
            warnings: Vec::new(),
        }
    }

    async fn test_net_handle() -> (NetHandle, TempDir) {
        let temp_dir = tempdir().unwrap();
        let storage = FjallStorage::open(temp_dir.path().to_str().unwrap()).unwrap();
        let handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..Default::default()
            },
            storage,
        )
        .await
        .unwrap();

        (handle, temp_dir)
    }

    #[test]
    fn endpoint_addr_from_info_requires_endpoint_addr() {
        let error = endpoint_addr_from_info(&test_info_response(None)).unwrap_err();

        assert!(matches!(
            error,
            CliError::IrohCheck {
                step: "endpoint_addr",
                ..
            }
        ));
    }

    #[test]
    fn normalize_endpoint_addr_rewrites_unspecified_ipv4() {
        let node_id = iroh::SecretKey::from_bytes(&[3u8; 32]).public();
        let endpoint_addr =
            EndpointAddr::new(node_id).with_ip_addr("0.0.0.0:3001".parse().unwrap());

        let normalized =
            normalize_endpoint_addr(endpoint_addr, "http://127.0.0.1:3000/api/v1/info").unwrap();

        assert!(
            normalized
                .ip_addrs()
                .any(|addr| *addr == "127.0.0.1:3001".parse::<SocketAddr>().unwrap())
        );
    }

    #[test]
    fn normalize_endpoint_addr_rewrites_unspecified_ipv6() {
        let node_id = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let endpoint_addr = EndpointAddr::new(node_id).with_ip_addr("[::]:3001".parse().unwrap());

        let normalized =
            normalize_endpoint_addr(endpoint_addr, "http://[::1]:3000/api/v1/info").unwrap();

        assert!(
            normalized
                .ip_addrs()
                .any(|addr| *addr == "[::1]:3001".parse::<SocketAddr>().unwrap())
        );
    }

    #[test]
    fn normalize_endpoint_addr_rejects_dns_wildcard() {
        let node_id = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
        let endpoint_addr =
            EndpointAddr::new(node_id).with_ip_addr("0.0.0.0:3001".parse().unwrap());

        let error =
            normalize_endpoint_addr(endpoint_addr, "https://node.example/api/v1/info").unwrap_err();

        assert!(matches!(
            error,
            CliError::IrohCheck {
                step: "endpoint_addr",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn active_iroh_check_pings_live_node() {
        let (target, _dir) = test_net_handle().await;
        let mut checks = Vec::new();

        let active = active_iroh_check(target.endpoint_addr(), Duration::from_secs(5), &mut checks)
            .await
            .unwrap();

        assert_eq!(active.remote_id, target.node_id().to_string());
        assert_eq!(active.negotiated_alpn, Alpn::Dht.to_string());
        assert!(checks.iter().any(|check| check.name == "iroh_connect"));
        assert!(checks.iter().any(|check| check.name == "dht_ping"));
        target.shutdown().await;
    }

    #[tokio::test]
    async fn run_iroh_check_builds_output_from_info() {
        let (target, _dir) = test_net_handle().await;
        let output_endpoint =
            endpoint_addr_from_info(&test_info_response(Some(target.endpoint_addr()))).unwrap();
        let mut checks = Vec::new();

        let active = active_iroh_check(output_endpoint, Duration::from_secs(5), &mut checks)
            .await
            .unwrap();

        assert_eq!(active.remote_id, target.node_id().to_string());
        target.shutdown().await;
    }
}
