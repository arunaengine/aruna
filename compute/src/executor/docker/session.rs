//! Creates and checks the internal bridge network that session containers join.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::Ipv4Addr;

use aruna_core::compute::BackendError;
use bollard::models::{Ipam, IpamConfig, NetworkCreateRequest, NetworkInspect};
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
            return check_session_network(&existing, &self.config.session_subnet, gateway);
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
            Err(error) => match self.docker.inspect_network(SESSION_NETWORK, None).await {
                // Another attempt won the race, but its network must still match.
                Ok(existing) => {
                    tracing::debug!(error = %error, "session network already existed");
                    check_session_network(&existing, &self.config.session_subnet, gateway)
                }
                Err(_) => Err(classify(&error)),
            },
        }
    }
}

/// A reused session network must be what creation requests: an internal bridge with one
/// IPAM entry for the configured subnet and gateway. Anything else could give sessions an
/// external route or an address the node does not serve.
fn check_session_network(
    existing: &NetworkInspect,
    subnet: &str,
    gateway: Ipv4Addr,
) -> Result<(), BackendError> {
    let reject = |detail: String| {
        Err(BackendError::InvalidSpec(format!(
            "network `{SESSION_NETWORK}` exists with {detail}"
        )))
    };
    let driver = existing.driver.as_deref();
    if driver != Some("bridge") {
        return reject(format!("driver {}, not `bridge`", quoted_or_unset(driver)));
    }
    if existing.internal != Some(true) {
        let internal = existing.internal.unwrap_or_default();
        return reject(format!("internal `{internal}`, not `true`"));
    }
    let configs = existing
        .ipam
        .as_ref()
        .and_then(|ipam| ipam.config.as_deref())
        .unwrap_or_default();
    let [config] = configs else {
        return reject(format!("{} IPAM configs, not one", configs.len()));
    };
    let found = config.subnet.as_deref();
    if found != Some(subnet) {
        return reject(format!("subnet {}, not `{subnet}`", quoted_or_unset(found)));
    }
    let found = config.gateway.as_deref();
    if found.and_then(|value| value.parse::<Ipv4Addr>().ok()) != Some(gateway) {
        return reject(format!(
            "gateway {}, not `{gateway}`",
            quoted_or_unset(found)
        ));
    }
    Ok(())
}

fn quoted_or_unset(value: Option<&str>) -> String {
    value.map_or_else(|| "unset".to_string(), |value| format!("`{value}`"))
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

#[cfg(test)]
mod tests {
    use super::*;

    const SUBNET: &str = "172.30.255.0/24";
    const GATEWAY: Ipv4Addr = Ipv4Addr::new(172, 30, 255, 1);

    // The inspect record Docker 29 returns for the network this module creates.
    fn created() -> NetworkInspect {
        NetworkInspect {
            name: Some(SESSION_NETWORK.to_string()),
            driver: Some("bridge".to_string()),
            internal: Some(true),
            ipam: Some(Ipam {
                driver: Some("default".to_string()),
                config: Some(vec![IpamConfig {
                    subnet: Some(SUBNET.to_string()),
                    gateway: Some(GATEWAY.to_string()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn rejection(network: &NetworkInspect) -> String {
        match check_session_network(network, SUBNET, GATEWAY) {
            Err(BackendError::InvalidSpec(message)) => message,
            other => panic!("expected an invalid spec rejection, got {other:?}"),
        }
    }

    fn ipam_entry(network: &mut NetworkInspect) -> &mut IpamConfig {
        &mut network.ipam.as_mut().unwrap().config.as_mut().unwrap()[0]
    }

    #[test]
    fn accepts_created_network() {
        check_session_network(&created(), SUBNET, GATEWAY).expect("created network matches");
    }

    #[test]
    fn rejects_other_driver() {
        let mut network = created();
        network.driver = Some("overlay".to_string());
        assert_eq!(
            rejection(&network),
            "network `aruna-sessions` exists with driver `overlay`, not `bridge`"
        );
        network.driver = None;
        assert_eq!(
            rejection(&network),
            "network `aruna-sessions` exists with driver unset, not `bridge`"
        );
    }

    #[test]
    fn rejects_external_network() {
        let mut network = created();
        network.internal = Some(false);
        let expected = "network `aruna-sessions` exists with internal `false`, not `true`";
        assert_eq!(rejection(&network), expected);
        network.internal = None;
        assert_eq!(rejection(&network), expected);
    }

    #[test]
    fn rejects_other_subnet() {
        let mut network = created();
        ipam_entry(&mut network).subnet = Some("10.0.0.0/24".to_string());
        assert_eq!(
            rejection(&network),
            "network `aruna-sessions` exists with subnet `10.0.0.0/24`, not `172.30.255.0/24`"
        );
    }

    #[test]
    fn rejects_other_gateway() {
        let mut network = created();
        ipam_entry(&mut network).gateway = Some("172.30.255.254".to_string());
        assert_eq!(
            rejection(&network),
            "network `aruna-sessions` exists with gateway `172.30.255.254`, not `172.30.255.1`"
        );
        ipam_entry(&mut network).gateway = Some("not-an-address".to_string());
        assert_eq!(
            rejection(&network),
            "network `aruna-sessions` exists with gateway `not-an-address`, not `172.30.255.1`"
        );
    }

    #[test]
    fn rejects_missing_settings() {
        let mut network = created();
        network.ipam = None;
        assert_eq!(
            rejection(&network),
            "network `aruna-sessions` exists with 0 IPAM configs, not one"
        );

        let mut network = created();
        network.ipam.as_mut().unwrap().config = Some(Vec::new());
        assert_eq!(
            rejection(&network),
            "network `aruna-sessions` exists with 0 IPAM configs, not one"
        );

        // A second pool could hand sessions an address outside the served subnet.
        let mut network = created();
        let entry = ipam_entry(&mut network).clone();
        network.ipam.as_mut().unwrap().config = Some(vec![entry.clone(), entry]);
        assert_eq!(
            rejection(&network),
            "network `aruna-sessions` exists with 2 IPAM configs, not one"
        );

        let mut network = created();
        ipam_entry(&mut network).subnet = None;
        assert_eq!(
            rejection(&network),
            "network `aruna-sessions` exists with subnet unset, not `172.30.255.0/24`"
        );

        let mut network = created();
        ipam_entry(&mut network).gateway = None;
        assert_eq!(
            rejection(&network),
            "network `aruna-sessions` exists with gateway unset, not `172.30.255.1`"
        );
    }
}
