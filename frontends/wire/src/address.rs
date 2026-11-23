//! Builds the URLs clients use to reach a node from its bind address or advertised host.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::SocketAddr;

pub fn client_bind_url(bind_address: SocketAddr) -> String {
    format!(
        "http://{}:{}",
        host_for_ip(bind_address.ip()),
        bind_address.port()
    )
}

pub fn client_host_url(advertised_host: &str, bind_address: SocketAddr) -> String {
    let host = match advertised_host.trim() {
        "" => return client_bind_url(bind_address),
        host => {
            if host.contains("://") {
                return host.trim_end_matches('/').to_string();
            }

            if let Ok(addr) = host.parse::<SocketAddr>() {
                return format!("http://{}:{}", host_for_ip(addr.ip()), addr.port());
            }

            if let Ok(ip) = host.parse::<std::net::IpAddr>() {
                return format!("http://{}:{}", host_for_ip(ip), bind_address.port());
            }

            host
        }
    };

    format!("http://{host}")
}

fn host_for_ip(ip: std::net::IpAddr) -> String {
    match ip {
        std::net::IpAddr::V4(ip) if ip.is_unspecified() => {
            std::net::Ipv4Addr::LOCALHOST.to_string()
        }
        std::net::IpAddr::V6(ip) if ip.is_unspecified() => {
            format!("[{}]", std::net::Ipv6Addr::LOCALHOST)
        }
        std::net::IpAddr::V6(ip) => format!("[{ip}]"),
        std::net::IpAddr::V4(ip) => ip.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_ipv6_url() {
        assert_eq!(
            client_bind_url("[::]:3000".parse().unwrap()),
            "http://[::1]:3000"
        );
    }

    #[test]
    fn normalizes_s3_wildcards() {
        assert_eq!(
            client_host_url("0.0.0.0", "0.0.0.0:1337".parse().unwrap()),
            "http://127.0.0.1:1337"
        );
        assert_eq!(
            client_host_url("::", "[::]:1337".parse().unwrap()),
            "http://[::1]:1337"
        );
    }

    #[test]
    fn preserves_s3_authority() {
        assert_eq!(
            client_host_url("127.0.0.1:1337", "0.0.0.0:9999".parse().unwrap()),
            "http://127.0.0.1:1337"
        );
        assert_eq!(
            client_host_url(
                "s3.node-1.v3.aruna-engine.org",
                "0.0.0.0:1337".parse().unwrap()
            ),
            "http://s3.node-1.v3.aruna-engine.org"
        );
    }

    #[test]
    fn preserves_s3_scheme() {
        assert_eq!(
            client_host_url(
                "https://s3.node-1.v3.aruna-engine.org",
                "0.0.0.0:1337".parse().unwrap()
            ),
            "https://s3.node-1.v3.aruna-engine.org"
        );
        assert_eq!(
            client_host_url(
                "https://s3.node-1.v3.aruna-engine.org/",
                "0.0.0.0:1337".parse().unwrap()
            ),
            "https://s3.node-1.v3.aruna-engine.org"
        );
    }
}
