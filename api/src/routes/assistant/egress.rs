//! Outbound egress policy for assistant provider connections. Only the assistant
//! client accepts a caller-supplied origin and resolves names; the OIDC validator
//! and management relay keep their own clients, so the rules stay separate.

use aruna_core::structs::identity::auth::NodeCapabilities;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// The client the assistant proxy and the ChatGPT login flow share. A server
/// node resolves through [`PublicDns`], so a stored base URL cannot reach a
/// private address; a user node deliberately may build local providers.
pub(crate) fn outbound_client(node_capabilities: &NodeCapabilities) -> Option<reqwest::Client> {
    let client = reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none());
    let client = if matches!(node_capabilities, NodeCapabilities::User { .. }) {
        client
    } else {
        client.no_proxy().dns_resolver(PublicDns)
    };
    client.build().ok()
}

/// Resolves provider names and rejects the whole answer unless every resolved
/// address is public. The check runs at the connection boundary, so a name that
/// later starts resolving to a private address is refused on the next connect.
#[derive(Debug)]
struct PublicDns;

impl Resolve for PublicDns {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addresses = tokio::net::lookup_host((host.as_str(), 0))
                .await
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?
                .collect::<Vec<_>>();
            if addresses.is_empty()
                || addresses
                    .iter()
                    .any(|address| !public_address(address.ip()))
            {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "assistant provider DNS resolved to a non-public address",
                ))
                    as Box<dyn std::error::Error + Send + Sync>);
            }
            Ok(Box::new(addresses.into_iter()) as Addrs)
        })
    }
}

pub(crate) fn public_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => public_ipv4(address),
        IpAddr::V6(address) => public_ipv6(address),
    }
}

fn public_ipv4(address: Ipv4Addr) -> bool {
    let [a, b, c, d] = address.octets();
    !(a == 0
        || address.is_private()
        || (a == 100 && b & 0xc0 == 0x40)
        || address.is_loopback()
        || address.is_link_local()
        || (a == 192 && b == 0 && c == 0 && d != 9 && d != 10)
        || address.is_documentation()
        || (a == 198 && b & 0xfe == 18)
        || address.is_multicast()
        || a & 0xf0 == 0xf0)
}

fn public_ipv6(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    // Server-side providers use currently assigned global unicast space only.
    segments[0] & 0xe000 == 0x2000
        && !matches!(segments, [0x2001, 0xdb8, ..] | [0x3fff, 0..=0x0fff, ..])
        && !matches!(segments, [0x2002, ..])
        && !(matches!(segments, [0x2001, b, ..] if b < 0x200)
            && !(u128::from_be_bytes(address.octets())
                == 0x2001_0001_0000_0000_0000_0000_0000_0001
                || u128::from_be_bytes(address.octets())
                    == 0x2001_0001_0000_0000_0000_0000_0000_0002
                || matches!(segments, [0x2001, 3, ..] | [0x2001, 4, 0x112, ..])
                || matches!(segments, [0x2001, b, ..] if (0x20..=0x3f).contains(&b))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_public_addresses() {
        assert!(public_address("8.8.8.8".parse().unwrap()));
        assert!(public_address("2001:4860:4860::8888".parse().unwrap()));
        for address in [
            "127.0.0.1",
            "100.64.0.1",
            "198.18.0.1",
            "::1",
            "fc00::1",
            "2001:db8::1",
            "::ffff:127.0.0.1",
        ] {
            assert!(!public_address(address.parse().unwrap()), "{address}");
        }
    }

    #[tokio::test]
    async fn dns_rejects_localhost() {
        assert!(
            PublicDns
                .resolve("localhost".parse().unwrap())
                .await
                .is_err()
        );
    }
}
