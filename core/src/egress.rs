//! Compiled-in egress policy for tenant-supplied endpoints.
//!
//! The deny table is a constant: no realm config, node config, or API input can
//! remove an entry from it. Every consumer of a tenant endpoint screens here.

use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum EgressError {
    #[error("address `{0}` is not a public unicast destination")]
    BlockedAddress(IpAddr),
    #[error("host `{0}` resolved to no allowed address")]
    NoAllowedAddress(String),
    #[error("endpoint `{0}` has no host to screen")]
    MissingHost(String),
    #[error("failed to resolve `{host}`: {reason}")]
    ResolveFailed { host: String, reason: String },
}

/// IPv4 ranges that are never a legitimate tenant destination.
const DENIED_V4: &[Ipv4Net] = &[
    Ipv4Net::new_assert(Ipv4Addr::new(0, 0, 0, 0), 8),
    Ipv4Net::new_assert(Ipv4Addr::new(10, 0, 0, 0), 8),
    Ipv4Net::new_assert(Ipv4Addr::new(100, 64, 0, 0), 10),
    Ipv4Net::new_assert(Ipv4Addr::new(127, 0, 0, 0), 8),
    Ipv4Net::new_assert(Ipv4Addr::new(169, 254, 0, 0), 16),
    Ipv4Net::new_assert(Ipv4Addr::new(172, 16, 0, 0), 12),
    Ipv4Net::new_assert(Ipv4Addr::new(192, 0, 0, 0), 24),
    Ipv4Net::new_assert(Ipv4Addr::new(192, 0, 2, 0), 24),
    Ipv4Net::new_assert(Ipv4Addr::new(192, 88, 99, 0), 24),
    Ipv4Net::new_assert(Ipv4Addr::new(192, 168, 0, 0), 16),
    Ipv4Net::new_assert(Ipv4Addr::new(198, 18, 0, 0), 15),
    Ipv4Net::new_assert(Ipv4Addr::new(198, 51, 100, 0), 24),
    Ipv4Net::new_assert(Ipv4Addr::new(203, 0, 113, 0), 24),
    Ipv4Net::new_assert(Ipv4Addr::new(224, 0, 0, 0), 4),
    Ipv4Net::new_assert(Ipv4Addr::new(240, 0, 0, 0), 4),
    Ipv4Net::new_assert(Ipv4Addr::new(255, 255, 255, 255), 32),
];

/// IPv6 ranges that are never a legitimate tenant destination: every IANA
/// special-purpose prefix that is not globally reachable, plus the tunnelling
/// prefixes whose reachability depends on a translator. `::/96` covers the
/// deprecated IPv4-compatible spellings that embed an arbitrary v4 address.
const DENIED_V6: &[Ipv6Net] = &[
    Ipv6Net::new_assert(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0), 96),
    Ipv6Net::new_assert(Ipv6Addr::new(0x0100, 0, 0, 0, 0, 0, 0, 0), 64),
    Ipv6Net::new_assert(Ipv6Addr::new(0x2001, 0, 0, 0, 0, 0, 0, 0), 32),
    Ipv6Net::new_assert(Ipv6Addr::new(0x2001, 0x0002, 0, 0, 0, 0, 0, 0), 48),
    Ipv6Net::new_assert(Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0), 32),
    Ipv6Net::new_assert(Ipv6Addr::new(0x2002, 0, 0, 0, 0, 0, 0, 0), 16),
    Ipv6Net::new_assert(Ipv6Addr::new(0x3fff, 0, 0, 0, 0, 0, 0, 0), 20),
    Ipv6Net::new_assert(Ipv6Addr::new(0x5f00, 0, 0, 0, 0, 0, 0, 0), 16),
    Ipv6Net::new_assert(Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 0), 7),
    Ipv6Net::new_assert(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 0), 10),
    Ipv6Net::new_assert(Ipv6Addr::new(0xfec0, 0, 0, 0, 0, 0, 0, 0), 10),
    Ipv6Net::new_assert(Ipv6Addr::new(0xff00, 0, 0, 0, 0, 0, 0, 0), 8),
];

const NAT64_WELL_KNOWN: Ipv6Net =
    Ipv6Net::new_assert(Ipv6Addr::new(0x0064, 0xff9b, 0, 0, 0, 0, 0, 0), 96);
const NAT64_LOCAL_USE: Ipv6Net =
    Ipv6Net::new_assert(Ipv6Addr::new(0x0064, 0xff9b, 0x0001, 0, 0, 0, 0, 0), 48);

/// Unwraps an IPv4 address carried inside an IPv6 address so a v6 spelling of a
/// blocked v4 destination cannot skip the v4 rows.
pub fn normalize(address: IpAddr) -> IpAddr {
    let IpAddr::V6(address) = address else {
        return address;
    };
    match embedded_v4(address) {
        Some(embedded) => IpAddr::V4(embedded),
        None => IpAddr::V6(address),
    }
}

fn embedded_v4(address: Ipv6Addr) -> Option<Ipv4Addr> {
    if let Some(mapped) = address.to_ipv4_mapped() {
        return Some(mapped);
    }
    let octets = address.octets();
    if NAT64_WELL_KNOWN.contains(&address) {
        return Some(Ipv4Addr::new(
            octets[12], octets[13], octets[14], octets[15],
        ));
    }
    // RFC 6052 /48 embedding straddles the reserved octet at bits 64..71.
    if NAT64_LOCAL_USE.contains(&address) {
        return Some(Ipv4Addr::new(octets[6], octets[7], octets[9], octets[10]));
    }
    None
}

fn denied(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => DENIED_V4.iter().any(|net| net.contains(&address)),
        IpAddr::V6(address) => DENIED_V6.iter().any(|net| net.contains(&address)),
    }
}

/// Decides whether the node may open a connection to an address.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EgressPolicy {
    loopback: bool,
    extra_deny: Arc<[IpNet]>,
}

impl Default for EgressPolicy {
    fn default() -> Self {
        Self::strict()
    }
}

impl EgressPolicy {
    /// Public unicast destinations only. Production wiring uses this and only
    /// this; no configuration surface selects anything weaker.
    pub fn strict() -> Self {
        Self {
            loopback: false,
            extra_deny: Arc::from([]),
        }
    }

    /// Additionally permits loopback destinations. Test fixtures pass this
    /// through the same constructor seam production uses.
    pub fn loopback() -> Self {
        Self {
            loopback: true,
            extra_deny: Arc::from([]),
        }
    }

    /// Node-local narrowing from the backends file. Denies only add; the
    /// compiled table below can never be reduced by configuration.
    pub fn with_deny(mut self, networks: Vec<IpNet>) -> Self {
        self.extra_deny = Arc::from(networks);
        self
    }

    pub fn check(&self, address: IpAddr) -> Result<(), EgressError> {
        let normalized = normalize(address);
        if self.extra_deny.iter().any(|net| net.contains(&normalized)) {
            return Err(EgressError::BlockedAddress(normalized));
        }
        if self.loopback && (address.is_loopback() || normalized.is_loopback()) {
            return Ok(());
        }
        if denied(normalized) {
            return Err(EgressError::BlockedAddress(normalized));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    const V4_ROWS: [&str; 16] = [
        "0.0.0.0/8",
        "10.0.0.0/8",
        "100.64.0.0/10",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "172.16.0.0/12",
        "192.0.0.0/24",
        "192.0.2.0/24",
        "192.88.99.0/24",
        "192.168.0.0/16",
        "198.18.0.0/15",
        "198.51.100.0/24",
        "203.0.113.0/24",
        "224.0.0.0/4",
        "240.0.0.0/4",
        "255.255.255.255/32",
    ];

    const V6_ROWS: [&str; 12] = [
        "::/96",
        "100::/64",
        "2001::/32",
        "2001:2::/48",
        "2001:db8::/32",
        "2002::/16",
        "3fff::/20",
        "5f00::/16",
        "fc00::/7",
        "fe80::/10",
        "fec0::/10",
        "ff00::/8",
    ];

    #[test]
    fn table_matches_spec() {
        // The table is the security contract; pin its exact rows.
        let v4: Vec<String> = DENIED_V4.iter().map(ToString::to_string).collect();
        let v6: Vec<String> = DENIED_V6.iter().map(ToString::to_string).collect();

        assert_eq!(v4, V4_ROWS);
        assert_eq!(v6, V6_ROWS);
    }

    #[test]
    fn denies_v4_rows() {
        let policy = EgressPolicy::strict();
        for row in V4_ROWS {
            let net = Ipv4Net::from_str(row).unwrap();
            for address in [net.network(), net.broadcast()] {
                assert_eq!(
                    policy.check(IpAddr::V4(address)),
                    Err(EgressError::BlockedAddress(IpAddr::V4(address))),
                    "{row}"
                );
            }
        }
    }

    #[test]
    fn denies_v6_rows() {
        let policy = EgressPolicy::strict();
        for row in V6_ROWS {
            let net = Ipv6Net::from_str(row).unwrap();
            let address = net.network();
            let normalized = normalize(IpAddr::V6(address));
            assert_eq!(
                policy.check(IpAddr::V6(address)),
                Err(EgressError::BlockedAddress(normalized)),
                "{row}"
            );
        }
    }

    #[test]
    fn allows_public_unicast() {
        let policy = EgressPolicy::strict();
        for address in [
            "1.1.1.1",
            "8.8.8.8",
            "203.0.114.1",
            "2606:4700:4700::1111",
            "2a00:1450:4001:80f::200e",
        ] {
            policy.check(IpAddr::from_str(address).unwrap()).unwrap();
        }
    }

    #[test]
    fn unwraps_mapped_v4() {
        let policy = EgressPolicy::strict();
        let metadata = IpAddr::from_str("::ffff:169.254.169.254").unwrap();

        assert_eq!(
            policy.check(metadata),
            Err(EgressError::BlockedAddress(
                IpAddr::from_str("169.254.169.254").unwrap()
            ))
        );
        policy
            .check(IpAddr::from_str("::ffff:1.1.1.1").unwrap())
            .unwrap();
        policy
            .check(IpAddr::from_str("::192.168.0.1").unwrap())
            .unwrap_err();
    }

    #[test]
    fn unwraps_nat64_prefixes() {
        // Well-known /96 and RFC 8215 /48 embeddings of 169.254.169.254.
        let policy = EgressPolicy::strict();
        let well_known = IpAddr::from_str("64:ff9b::169.254.169.254").unwrap();
        let local_use = IpAddr::V6(Ipv6Addr::new(
            0x0064, 0xff9b, 0x0001, 0xa9fe, 0x00a9, 0xfe00, 0, 0,
        ));
        let blocked = Err(EgressError::BlockedAddress(
            IpAddr::from_str("169.254.169.254").unwrap(),
        ));

        assert_eq!(policy.check(well_known), blocked);
        assert_eq!(policy.check(local_use), blocked);
        policy
            .check(IpAddr::from_str("64:ff9b::1.1.1.1").unwrap())
            .unwrap();
        policy
            .check(IpAddr::V6(Ipv6Addr::new(
                0x0064, 0xff9b, 0x0001, 0x0101, 0x0001, 0x0100, 0, 0,
            )))
            .unwrap();
    }

    #[test]
    fn loopback_stays_narrow() {
        // The fixture seam opens loopback and nothing else.
        let strict = EgressPolicy::strict();
        let fixture = EgressPolicy::loopback();
        for address in ["127.0.0.1", "::1", "::ffff:127.0.0.1"] {
            let address = IpAddr::from_str(address).unwrap();
            strict.check(address).unwrap_err();
            fixture.check(address).unwrap();
        }
        for address in ["169.254.169.254", "10.0.0.1", "fd00::1"] {
            fixture
                .check(IpAddr::from_str(address).unwrap())
                .unwrap_err();
        }
    }

    #[test]
    fn denies_only_narrow() {
        // A node-local deny adds rows; it can never open a compiled-in one.
        let narrowed =
            EgressPolicy::strict().with_deny(vec![IpNet::from_str("8.8.8.0/24").unwrap()]);

        narrowed
            .check(IpAddr::from_str("8.8.8.8").unwrap())
            .unwrap_err();
        narrowed
            .check(IpAddr::from_str("9.9.9.9").unwrap())
            .unwrap();

        let widened =
            EgressPolicy::loopback().with_deny(vec![IpNet::from_str("127.0.0.0/8").unwrap()]);
        widened
            .check(IpAddr::from_str("127.0.0.1").unwrap())
            .unwrap_err();
    }
}
