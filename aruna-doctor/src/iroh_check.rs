use crate::error::CliError;
use crate::info::{default_info_url_from_env, fetch_info_url_with_timeout};
use aruna_api::routes::info::{
    BootstrapDiagnosticsStatus, ConnectionMonitorStatus, InfoResponse, KnownPeerAddressStatus,
    PeerConnectivityStatus, ServiceStatus,
};
use aruna_core::alpn::Alpn;
use aruna_net::dht::rpc::{DhtRequest, DhtResponse, decode_response, encode_request};
use iroh::endpoint::{PathId, presets};
use iroh::{Endpoint, EndpointAddr, RelayMode, TransportAddr, Watcher};
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
    monitor: ConnectionMonitorStatus,
    bootstrap: BootstrapDiagnosticsStatus,
    peer_connectivity: Vec<PeerConnectivityStatus>,
    known_peer_addresses: Vec<KnownPeerAddressStatus>,
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

    if endpoint_info.net_state.status != ServiceStatus::Available {
        return Err(iroh_check_error(
            "info",
            format!("net state is {:?}", endpoint_info.net_state.status),
        ));
    }

    let endpoint_started = Instant::now();
    let endpoint_addr = endpoint_addr_from_info(&endpoint_info)?;
    let endpoint_addr = normalize_endpoint_addr(endpoint_addr, &info_url);
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
    let value = endpoint_info
        .net_state
        .endpoint_addr
        .clone()
        .ok_or_else(|| iroh_check_error("endpoint_addr", "missing net_state.endpoint_addr"))?;

    serde_json::from_value::<EndpointAddr>(value)
        .map_err(|error| iroh_check_error("endpoint_addr", error))
}

fn normalize_endpoint_addr(endpoint_addr: EndpointAddr, info_url: &str) -> EndpointAddr {
    let replacement_ip = replacement_ip_from_url(info_url);
    let addrs = endpoint_addr.addrs.into_iter().map(|addr| match addr {
        TransportAddr::Ip(socket) if socket.ip().is_unspecified() => {
            let ip = replacement_ip.unwrap_or_else(|| loopback_for(socket.ip()));
            TransportAddr::Ip(SocketAddr::new(ip, socket.port()))
        }
        addr => addr,
    });

    EndpointAddr::from_parts(endpoint_addr.id, addrs)
}

async fn active_iroh_check(
    endpoint_addr: EndpointAddr,
    timeout: Duration,
    checks: &mut Vec<IrohCheckStep>,
) -> Result<ActiveIrohCheck, CliError> {
    let bind_addr = local_bind_addr_for(&endpoint_addr);
    let endpoint_builder = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
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
    let active = ActiveIrohCheck {
        doctor_node_id: endpoint.id().to_string(),
        remote_id: conn.remote_id().to_string(),
        negotiated_alpn: String::from_utf8_lossy(conn.alpn()).to_string(),
        rtt_ms: conn.rtt(PathId::ZERO).map(duration_ms),
        selected_path,
        paths,
    };
    conn.close(0u8.into(), b"aruna-doctor iroh check complete");

    Ok(active)
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
        realm_id: endpoint_info.net_state.realm_id.clone(),
        node_id: endpoint_info.net_state.node_id.clone(),
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
        monitor: endpoint_info.net_state.monitor.clone(),
        bootstrap: endpoint_info.net_state.bootstrap.clone(),
        peer_connectivity: endpoint_info.net_state.peer_connectivity.clone(),
        known_peer_addresses: endpoint_info.net_state.known_peer_addresses.clone(),
        warnings: endpoint_info.net_state.warnings.clone(),
    }
}

fn path_statuses(conn: &iroh::endpoint::Connection) -> Vec<IrohPathStatus> {
    let mut paths = conn.paths();
    paths.update();
    paths
        .peek()
        .iter()
        .map(|path| IrohPathStatus {
            id: format!("{:?}", path.id()),
            remote_addr: transport_addr_to_string(path.remote_addr()),
            selected: path.is_selected(),
            closed: path.is_closed(),
            rtt_ms: path.rtt().map(duration_ms),
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

    host.parse().ok()
}

fn loopback_for(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
    }
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

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
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
        BlobStatus, InterfaceStatus, LocalNodeInfo, NetStatus, NodeCapabilityKind,
        RestInterfaceStatus, S3InterfaceStatus,
    };
    use aruna_core::structs::RealmId;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::FjallStorage;
    use tempfile::{TempDir, tempdir};

    fn test_info_response(endpoint_addr: Option<serde_json::Value>) -> InfoResponse {
        InfoResponse {
            node_info: LocalNodeInfo {
                realm_id: RealmId::from_bytes([1u8; 32]).to_string(),
                node_id: iroh::SecretKey::from_bytes(&[2u8; 32]).public().to_string(),
                capabilities: NodeCapabilityKind::Management,
            },
            net_state: NetStatus {
                status: ServiceStatus::Available,
                realm_id: Some(RealmId::from_bytes([1u8; 32]).to_string()),
                node_id: Some(iroh::SecretKey::from_bytes(&[2u8; 32]).public().to_string()),
                bootstrap_nodes: Vec::new(),
                endpoint_addr,
                monitor: ConnectionMonitorStatus::default(),
                bootstrap: BootstrapDiagnosticsStatus::default(),
                peer_connectivity: Vec::new(),
                known_peer_addresses: Vec::new(),
                warnings: Vec::new(),
            },
            blob_status: BlobStatus {
                status: ServiceStatus::NotConfigured,
                backend_type: None,
                max_bucket_size: None,
                multipart_bucket: None,
                timeouts: None,
            },
            interface_status: InterfaceStatus {
                rest: RestInterfaceStatus {
                    status: ServiceStatus::Available,
                    bind_address: None,
                    base_url: None,
                    api_base_url: None,
                    info_url: None,
                    swagger_ui_url: None,
                },
                s3: S3InterfaceStatus {
                    status: ServiceStatus::Unavailable,
                    bind_address: None,
                    base_url: None,
                },
            },
            database_status: aruna_api::routes::info::DatabaseStatus {
                status: ServiceStatus::Available,
                requests_total: 0,
                errors_total: 0,
                conflicts_total: 0,
                failed_total: 0,
                error_rate: 0.0,
                channel_closed: false,
                last_error: None,
            },
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
            normalize_endpoint_addr(endpoint_addr, "http://127.0.0.1:3000/api/v1/info");

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

        let normalized = normalize_endpoint_addr(endpoint_addr, "http://[::1]:3000/api/v1/info");

        assert!(
            normalized
                .ip_addrs()
                .any(|addr| *addr == "[::1]:3001".parse::<SocketAddr>().unwrap())
        );
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
        let endpoint_addr = serde_json::to_value(target.endpoint_addr()).unwrap();
        let output_endpoint =
            endpoint_addr_from_info(&test_info_response(Some(endpoint_addr))).unwrap();
        let mut checks = Vec::new();

        let active = active_iroh_check(output_endpoint, Duration::from_secs(5), &mut checks)
            .await
            .unwrap();

        assert_eq!(active.remote_id, target.node_id().to_string());
        target.shutdown().await;
    }
}
