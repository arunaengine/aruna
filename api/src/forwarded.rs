//! Client-facing base URLs for absolute links in API responses. `x-forwarded-*`
//! headers are unauthenticated input, so they are honored only when the direct
//! peer is a configured trusted proxy.

use axum::http::HeaderMap;
use ipnet::IpNet;
use std::net::IpAddr;

fn header_str<'headers>(headers: &'headers HeaderMap, name: &str) -> Option<&'headers str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

pub fn peer_is_trusted(trusted_proxies: &[IpNet], peer: IpAddr) -> bool {
    trusted_proxies.iter().any(|net| net.contains(&peer))
}

/// Base URL (`scheme://host`) as the client sees it. Forwarded scheme and host
/// apply only from a trusted proxy, and the scheme only when it is a real HTTP
/// scheme; every other caller gets the transport-derived values.
pub fn external_base_url(trusted_proxies: &[IpNet], peer: IpAddr, headers: &HeaderMap) -> String {
    let from_proxy = peer_is_trusted(trusted_proxies, peer);
    let scheme = from_proxy
        .then(|| header_str(headers, "x-forwarded-proto"))
        .flatten()
        .filter(|scheme| *scheme == "http" || *scheme == "https")
        .unwrap_or("http");
    let host = from_proxy
        .then(|| header_str(headers, "x-forwarded-host"))
        .flatten()
        .or_else(|| header_str(headers, http::header::HOST.as_str()))
        .unwrap_or("localhost");
    format!("{scheme}://{host}")
}

/// Client address for attribution. Behind a trusted proxy the client is the
/// address that proxy appended to `x-forwarded-for`; a direct caller is its
/// own transport address, no matter what headers it sends.
pub fn client_ip(trusted_proxies: &[IpNet], peer: IpAddr, headers: &HeaderMap) -> IpAddr {
    if !peer_is_trusted(trusted_proxies, peer) {
        return peer;
    }
    header_str(headers, "x-forwarded-for")
        .and_then(|value| value.rsplit(',').next())
        .and_then(|entry| entry.trim().parse::<IpAddr>().ok())
        .unwrap_or(peer)
}

#[cfg(test)]
mod tests {
    use super::{client_ip, external_base_url};
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
            external_base_url(&proxies(), peer, &forwarded_headers()),
            "http://node.internal"
        );
    }

    #[test]
    fn honors_trusted_proxy() {
        let peer = IpAddr::from_str("10.1.2.3").unwrap();
        assert_eq!(
            external_base_url(&proxies(), peer, &forwarded_headers()),
            "https://drs.example"
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
            external_base_url(&proxies(), peer, &headers),
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
    fn defaults_without_headers() {
        let peer = IpAddr::from_str("127.0.0.1").unwrap();
        assert_eq!(
            external_base_url(&[], peer, &HeaderMap::new()),
            "http://localhost"
        );
    }
}
