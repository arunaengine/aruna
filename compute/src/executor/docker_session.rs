use std::net::Ipv4Addr;

use aruna_core::compute::BackendError;
use bollard::models::{Ipam, IpamConfig, NetworkCreateRequest};
use ipnet::Ipv4Net;

use super::super::config::SESSION_NETWORK;
use super::DockerBackend;
use super::classify;

impl DockerBackend {
    /// Creates the internal bridge sessions join, once per daemon. It has no
    /// external route: the node's S3 server on the gateway address is the only
    /// endpoint a session container can reach.
    pub async fn ensure_session_network(&self) -> Result<(), BackendError> {
        let gateway = session_gateway(&self.config.session_subnet)?;
        if let Ok(existing) = self.docker.inspect_network(SESSION_NETWORK, None).await {
            return check_session_subnet(&existing, &self.config.session_subnet);
        }
        let request = NetworkCreateRequest {
            name: SESSION_NETWORK.to_string(),
            driver: Some("bridge".to_string()),
            internal: Some(true),
            ipam: Some(Ipam {
                config: Some(vec![IpamConfig {
                    subnet: Some(self.config.session_subnet.clone()),
                    gateway: Some(gateway.to_string()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        match self.docker.create_network(request).await {
            Ok(_) => Ok(()),
            // Another attempt won the race, which is the same outcome.
            Err(error)
                if self
                    .docker
                    .inspect_network(SESSION_NETWORK, None)
                    .await
                    .is_ok() =>
            {
                tracing::debug!(error = %error, "session network already existed");
                Ok(())
            }
            Err(error) => Err(classify(&error)),
        }
    }
}

/// An existing session network must carry the configured subnet: a different
/// one would put containers on an address the node does not serve.
fn check_session_subnet(
    existing: &bollard::models::NetworkInspect,
    subnet: &str,
) -> Result<(), BackendError> {
    let configured = existing
        .ipam
        .as_ref()
        .and_then(|ipam| ipam.config.as_ref())
        .and_then(|config| config.first())
        .and_then(|entry| entry.subnet.clone())
        .unwrap_or_default();
    if configured == subnet {
        return Ok(());
    }
    Err(BackendError::InvalidSpec(format!(
        "network `{SESSION_NETWORK}` exists with subnet `{configured}`, not `{subnet}`"
    )))
}

/// The first host address of the session subnet, which Docker gives the bridge
/// and the node's S3 server binds.
pub fn session_gateway(subnet: &str) -> Result<Ipv4Addr, BackendError> {
    let network: Ipv4Net = subnet.parse().map_err(|_| {
        BackendError::InvalidSpec(format!("session subnet `{subnet}` is not an IPv4 CIDR"))
    })?;
    let first = u32::from(network.network()).checked_add(1).ok_or_else(|| {
        BackendError::InvalidSpec(format!("session subnet `{subnet}` has no host address"))
    })?;
    let gateway = Ipv4Addr::from(first);
    if !network.contains(&gateway) {
        return Err(BackendError::InvalidSpec(format!(
            "session subnet `{subnet}` has no host address"
        )));
    }
    Ok(gateway)
}
