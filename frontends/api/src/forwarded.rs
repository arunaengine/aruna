//! Reads x-forwarded headers only from trusted proxies to find the client base URL and IP.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use axum::http::HeaderMap;
use ipnet::IpNet;
use std::net::IpAddr;

fn header_str<'headers>(headers: &'headers HeaderMap, name: &str) -> Option<&'headers str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    value.to_str().ok()
}

pub fn peer_is_trusted(trusted_proxies: &[IpNet], peer: IpAddr) -> bool {
    trusted_proxies.iter().any(|net| net.contains(&peer))
}

/// The configured API origin, attached to requests that arrived over native TLS.
#[derive(Clone, Debug)]
pub struct DirectOrigin {
    scheme: String,
    authority: String,
}

impl DirectOrigin {
    /// Takes an origin such as `https://api.example.org:8443`.
    pub fn new(origin: &str) -> Self {
        let (scheme, authority) = origin.split_once("://").unwrap_or(("https", origin));
        Self {
            scheme: scheme.to_string(),
            authority: authority.trim_end_matches('/').to_string(),
        }
    }
}

/// Base URL (`scheme://host`) as seen by the client. Forwarded scheme and host
/// are honored only from trusted proxies; otherwise a TLS request uses the
/// configured origin and a plain request `http` with its `Host`.
pub fn external_base_url(
    trusted_proxies: &[IpNet],
    peer: IpAddr,
    headers: &HeaderMap,
    direct: Option<&DirectOrigin>,
) -> String {
    let from_proxy = peer_is_trusted(trusted_proxies, peer);
    let scheme = from_proxy
        .then(|| header_str(headers, "x-forwarded-proto"))
        .flatten()
        .filter(|scheme| !scheme.contains(','))
        .filter(|scheme| *scheme == "http" || *scheme == "https")
        .or(direct.map(|direct| direct.scheme.as_str()))
        .unwrap_or("http");
    let host = from_proxy
        .then(|| header_str(headers, "x-forwarded-host"))
        .flatten()
        .filter(|host| !host.contains(','))
        .or_else(|| match direct {
            Some(direct) => Some(direct.authority.as_str()),
            None => header_str(headers, http::header::HOST.as_str()),
        })
        .unwrap_or("localhost");
    format!("{scheme}://{host}")
}

/// Client address for attribution. Behind trusted proxies, peel trusted hops
/// right-to-left; the first untrusted address is the client. Direct callers,
/// all-trusted chains, and malformed entries fall back to the transport address.
pub fn client_ip(trusted_proxies: &[IpNet], peer: IpAddr, headers: &HeaderMap) -> IpAddr {
    if !peer_is_trusted(trusted_proxies, peer) {
        return peer;
    }
    // Several header lines are one chain in order, exactly as if the proxies had
    // appended to a single comma-joined line.
    let mut chain = Vec::new();
    for value in headers.get_all("x-forwarded-for") {
        let Ok(value) = value.to_str() else {
            return peer;
        };
        chain.extend(value.split(','));
    }
    for entry in chain.into_iter().rev() {
        match entry.trim().parse::<IpAddr>() {
            Ok(addr) if peer_is_trusted(trusted_proxies, addr) => continue,
            Ok(addr) => return addr,
            Err(_) => return peer,
        }
    }
    peer
}

#[cfg(test)]
mod tests {
    use super::{DirectOrigin, client_ip, external_base_url};
    use axum::http::{HeaderMap, HeaderValue};
    use std::net::IpAddr;
    use std::str::FromStr;

    fn forwarded_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        headers.insert("x-forwarded-host", HeaderValue::from_static("drs.example"));
        headers.insert(
            http::header::HOST,
            HeaderValue::from_static("node.internal"),
        );
        headers
    }

    fn proxies() -> Vec<ipnet::IpNet> {
        vec![ipnet::IpNet::from_str("10.0.0.0/8").unwrap()]
    }

    #[test]
    fn ignores_untrusted_forwarded() {
        // A direct caller cannot forge its own origin via forwarded headers.
        let peer = IpAddr::from_str("203.0.113.9").unwrap();
        assert_eq!(
            external_base_url(&proxies(), peer, &forwarded_headers(), None),
            "http://node.internal"
        );
    }

    #[test]
    fn honors_trusted_proxy() {
        let peer = IpAddr::from_str("10.1.2.3").unwrap();
        assert_eq!(
            external_base_url(&proxies(), peer, &forwarded_headers(), None),
            "https://drs.example"
        );
    }

    #[test]
    fn rejects_xfh_duplicate() {
        let mut headers = forwarded_headers();
        headers.append(
            "x-forwarded-host",
            HeaderValue::from_static("attacker.example"),
        );
        let peer = IpAddr::from_str("10.1.2.3").unwrap();
        assert_eq!(
            external_base_url(&proxies(), peer, &headers, None),
            "https://node.internal"
        );
    }

    #[test]
    fn rejects_xfp_duplicate() {
        let mut headers = forwarded_headers();
        headers.append("x-forwarded-proto", HeaderValue::from_static("https"));
        let peer = IpAddr::from_str("10.1.2.3").unwrap();
        assert_eq!(
            external_base_url(&proxies(), peer, &headers, None),
            "http://drs.example"
        );
    }

    #[test]
    fn rejects_xfh_list() {
        let mut headers = forwarded_headers();
        headers.insert(
            "x-forwarded-host",
            HeaderValue::from_static("drs.example, attacker.example"),
        );
        let peer = IpAddr::from_str("10.1.2.3").unwrap();
        assert_eq!(
            external_base_url(&proxies(), peer, &headers, None),
            "https://node.internal"
        );
    }

    #[test]
    fn rejects_xfp_list() {
        let mut headers = forwarded_headers();
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https, http"));
        let peer = IpAddr::from_str("10.1.2.3").unwrap();
        assert_eq!(
            external_base_url(&proxies(), peer, &headers, None),
            "http://drs.example"
        );
    }

    #[test]
    fn rejects_bogus_scheme() {
        // A forwarded scheme outside http/https falls back instead of being
        // spliced into every absolute link.
        let peer = IpAddr::from_str("10.1.2.3").unwrap();
        let mut headers = forwarded_headers();
        headers.insert("x-forwarded-proto", HeaderValue::from_static("javascript"));
        assert_eq!(
            external_base_url(&proxies(), peer, &headers, None),
            "http://drs.example"
        );
    }

    #[test]
    fn attributes_client_ip() {
        // Direct callers cannot spoof their address; a trusted proxy's
        // appended hop is honored.
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("192.0.2.1, 198.51.100.7"),
        );
        let direct = IpAddr::from_str("203.0.113.9").unwrap();
        assert_eq!(client_ip(&proxies(), direct, &headers), direct);

        let proxy = IpAddr::from_str("10.1.2.3").unwrap();
        assert_eq!(
            client_ip(&proxies(), proxy, &headers),
            IpAddr::from_str("198.51.100.7").unwrap()
        );
        assert_eq!(client_ip(&proxies(), proxy, &HeaderMap::new()), proxy);
    }

    #[test]
    fn joins_xff_lines() {
        // Separate header lines form one chain: hops peel across them instead of
        // collapsing every client behind the proxy onto one attribution key.
        let mut headers = HeaderMap::new();
        headers.append("x-forwarded-for", HeaderValue::from_static("203.0.113.9"));
        headers.append("x-forwarded-for", HeaderValue::from_static("10.0.0.5"));
        let proxy = IpAddr::from_str("10.1.2.3").unwrap();
        assert_eq!(
            client_ip(&proxies(), proxy, &headers),
            IpAddr::from_str("203.0.113.9").unwrap()
        );

        // A malformed line still falls back to the transport address.
        headers.append("x-forwarded-for", HeaderValue::from_static("bogus"));
        assert_eq!(client_ip(&proxies(), proxy, &headers), proxy);
    }

    #[test]
    fn accepts_xff_chain() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("192.0.2.1, 198.51.100.7"),
        );
        let proxy = IpAddr::from_str("10.1.2.3").unwrap();
        assert_eq!(
            client_ip(&proxies(), proxy, &headers),
            IpAddr::from_str("198.51.100.7").unwrap()
        );
    }

    #[test]
    fn rejects_bad_xff() {
        let proxy = IpAddr::from_str("10.1.2.3").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", HeaderValue::from_bytes(b"\xff").unwrap());
        assert_eq!(client_ip(&proxies(), proxy, &headers), proxy);

        headers.insert("x-forwarded-for", HeaderValue::from_static("bogus"));
        assert_eq!(client_ip(&proxies(), proxy, &headers), proxy);
    }

    #[test]
    fn ignores_untrusted_xff() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("192.0.2.1, 198.51.100.7"),
        );
        let peer = IpAddr::from_str("203.0.113.9").unwrap();
        assert_eq!(client_ip(&proxies(), peer, &headers), peer);
    }

    #[test]
    fn peels_trusted_chain() {
        // Two trusted hops are peeled to the real client, not the last proxy.
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("203.0.113.9, 10.0.0.5"),
        );
        let proxy = IpAddr::from_str("10.1.2.3").unwrap();
        assert_eq!(
            client_ip(&proxies(), proxy, &headers),
            IpAddr::from_str("203.0.113.9").unwrap()
        );

        // An all-trusted chain and a malformed entry both fall back to peer.
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("10.0.0.1, 10.0.0.5"),
        );
        assert_eq!(client_ip(&proxies(), proxy, &headers), proxy);
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("bogus, 10.0.0.5"),
        );
        assert_eq!(client_ip(&proxies(), proxy, &headers), proxy);
    }

    #[test]
    fn defaults_without_headers() {
        let peer = IpAddr::from_str("127.0.0.1").unwrap();
        assert_eq!(
            external_base_url(&[], peer, &HeaderMap::new(), None),
            "http://localhost"
        );
    }

    #[test]
    fn tls_uses_origin() {
        // A direct TLS caller gets the configured origin, whatever its `Host` says.
        let direct = DirectOrigin::new("https://api.example.org:8443");
        let peer = IpAddr::from_str("203.0.113.9").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(http::header::HOST, HeaderValue::from_static("evil.test"));
        for headers in [headers, HeaderMap::new()] {
            assert_eq!(
                external_base_url(&proxies(), peer, &headers, Some(&direct)),
                "https://api.example.org:8443"
            );
        }
    }

    #[test]
    fn tls_keeps_proxy() {
        let direct = DirectOrigin::new("https://api.example.org");
        let peer = IpAddr::from_str("10.1.2.3").unwrap();
        assert_eq!(
            external_base_url(&proxies(), peer, &forwarded_headers(), Some(&direct)),
            "https://drs.example"
        );
        assert_eq!(
            external_base_url(&proxies(), peer, &HeaderMap::new(), Some(&direct)),
            "https://api.example.org"
        );
    }
}
