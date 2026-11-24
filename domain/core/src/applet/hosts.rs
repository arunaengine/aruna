//! Host names, custom domains and embedding origins of applets, and their host index rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::IpAddr;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::types::Key;

/// Longest host name DNS allows, in bytes.
const MAX_HOST: usize = 253;
/// Longest slug, so `<slug>.<applet domain>` stays a short label.
const MAX_SLUG: usize = 40;
/// Longest single origin; the whole list is bounded by the node's setting.
pub const MAX_ORIGIN: usize = 255;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum HostError {
    #[error("slugs are 1 to 40 characters of a-z, 0-9 or '-', not starting or ending with '-'")]
    Slug,
    #[error("not a valid host name")]
    Host,
    #[error("a custom domain must be a subdomain outside the applet domain")]
    Domain,
    #[error("origins are exact https origins without path, wildcard or the applet's own host")]
    Origin,
    #[error("at most {0} embedding origins are allowed")]
    TooManyOrigins(usize),
}

/// What a host row points to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HostKind {
    Applet,
    Custom,
}

/// The value of a row in `APPLET_HOSTS_KEYSPACE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostRow {
    pub applet_id: Ulid,
    pub kind: HostKind,
}

/// The row of a normalized host in `APPLET_HOSTS_KEYSPACE`.
pub fn host_key(host: &str) -> Key {
    Key::from(host.as_bytes().to_vec())
}

fn label_valid(label: &str) -> bool {
    (1..=63).contains(&label.len())
        && !label.starts_with('-')
        && !label.ends_with('-')
        && label
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

/// A lower-case host name of at least two labels, without a trailing dot.
pub fn normalize_host(raw: &str) -> Result<String, HostError> {
    let host = raw.trim().trim_end_matches('.').to_ascii_lowercase();
    let labels: Vec<&str> = host.split('.').collect();
    if host.len() > MAX_HOST || labels.len() < 2 || !labels.iter().all(|label| label_valid(label)) {
        return Err(HostError::Host);
    }
    Ok(host)
}

/// The applet's own host: its slug as one label below the node's applet domain.
pub fn applet_host(slug: &str, applet_domain: &str) -> Result<String, HostError> {
    if slug.len() > MAX_SLUG || !label_valid(slug) {
        return Err(HostError::Slug);
    }
    normalize_host(&format!("{slug}.{applet_domain}"))
}

/// A custom domain: a subdomain (three labels or more) that is not below the applet domain.
pub fn custom_domain(raw: &str, applet_domain: &str) -> Result<String, HostError> {
    let host = normalize_host(raw)?;
    let below = host == applet_domain || host.ends_with(&format!(".{applet_domain}"));
    if host.split('.').count() < 3 || below {
        return Err(HostError::Domain);
    }
    Ok(host)
}

/// The DNS name whose TXT record proves control of `host`.
pub fn verify_name(host: &str) -> String {
    format!("_aruna-verify.{host}")
}

/// The TXT value `host` must publish for its nonce.
pub fn verify_value(nonce: &str) -> String {
    format!("aruna-applet={nonce}")
}

/// Whether DNS proves a custom domain: its `_aruna-verify` TXT names `nonce`, and the domain
/// resolves to at least one address of the applet host, through a CNAME or flattened records.
pub fn domain_proven(txt: &[String], nonce: &str, (domain, own): (&[IpAddr], &[IpAddr])) -> bool {
    let value = verify_value(nonce);
    txt.iter().any(|entry| entry.trim() == value) && domain.iter().any(|ip| own.contains(ip))
}

/// One embedding origin: `https://host[:port]`, lower case, and never the applet's own host.
pub fn embedding_origin(raw: &str, own_hosts: &[&str]) -> Result<String, HostError> {
    let raw = raw.trim();
    let rest = raw.strip_prefix("https://").ok_or(HostError::Origin)?;
    if raw.len() > MAX_ORIGIN || rest.contains(['/', '?', '#', '@', '*']) {
        return Err(HostError::Origin);
    }
    let (host, port) = match rest.rsplit_once(':') {
        Some((host, port)) => {
            let port: u16 = port.parse().map_err(|_| HostError::Origin)?;
            (host, Some(port).filter(|port| *port != 443))
        }
        None => (rest, None),
    };
    let host = normalize_host(host).map_err(|_| HostError::Origin)?;
    if own_hosts.contains(&host.as_str()) {
        return Err(HostError::Origin);
    }
    Ok(match port {
        Some(port) => format!("https://{host}:{port}"),
        None => format!("https://{host}"),
    })
}

/// Checks every origin of a list, removes duplicates and bounds its length.
pub fn embedding_origins(
    raw: &[String],
    own_hosts: &[&str],
    limit: usize,
) -> Result<Vec<String>, HostError> {
    let mut origins: Vec<String> = Vec::with_capacity(raw.len());
    for origin in raw {
        let origin = embedding_origin(origin, own_hosts)?;
        if !origins.contains(&origin) {
            origins.push(origin);
        }
    }
    if origins.len() > limit {
        return Err(HostError::TooManyOrigins(limit));
    }
    Ok(origins)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugs_and_hosts() {
        let host = applet_host("bakta-web", "apps.example.org").unwrap();
        assert_eq!(host, "bakta-web.apps.example.org");
        assert_eq!(applet_host("-x", "apps.example.org"), Err(HostError::Slug));
        assert_eq!(
            applet_host("Bakta", "apps.example.org"),
            Err(HostError::Slug)
        );
        assert_eq!(
            normalize_host("Lab.Example.ORG."),
            Ok("lab.example.org".into())
        );
        assert_eq!(normalize_host("localhost"), Err(HostError::Host));
    }

    #[test]
    fn custom_needs_subdomain() {
        let domain = "apps.example.org";
        assert_eq!(
            custom_domain("bakta.lab.org", domain),
            Ok("bakta.lab.org".into())
        );
        assert_eq!(custom_domain("lab.org", domain), Err(HostError::Domain));
        assert_eq!(
            custom_domain("x.apps.example.org", domain),
            Err(HostError::Domain)
        );
        assert_eq!(verify_name("bakta.lab.org"), "_aruna-verify.bakta.lab.org");
    }

    #[test]
    fn domains_need_both() {
        let (here, elsewhere): (IpAddr, IpAddr) = (
            "192.0.2.1".parse().unwrap(),
            "198.51.100.1".parse().unwrap(),
        );
        let txt = vec!["other".to_string(), verify_value("ab")];
        assert!(domain_proven(&txt, "ab", (&[elsewhere, here], &[here])));
        assert!(!domain_proven(&txt, "cd", (&[here], &[here])));
        assert!(!domain_proven(&txt, "ab", (&[elsewhere], &[here])));
        assert!(!domain_proven(&txt, "ab", (&[], &[])));
    }

    #[test]
    fn origins_are_exact() {
        let own = ["bakta.apps.example.org"];
        let origin = |raw: &str| embedding_origin(raw, &own);
        assert_eq!(
            origin("https://Lab.Example.org"),
            Ok("https://lab.example.org".into())
        );
        assert_eq!(origin("https://lab.org:443"), Ok("https://lab.org".into()));
        assert_eq!(
            origin("https://lab.org:8443"),
            Ok("https://lab.org:8443".into())
        );
        for refused in [
            "http://lab.org",
            "https://*.lab.org",
            "https://lab.org/page",
            "https://bakta.apps.example.org",
            "https://user@lab.org",
            "lab.org",
        ] {
            assert_eq!(origin(refused), Err(HostError::Origin), "{refused}");
        }
        let many = vec!["https://a.org".to_string(), "https://b.org".to_string()];
        assert_eq!(
            embedding_origins(&many, &own, 1),
            Err(HostError::TooManyOrigins(1))
        );
        let twice = vec!["https://a.org".to_string(), "https://A.org".to_string()];
        assert_eq!(embedding_origins(&twice, &own, 1).unwrap().len(), 1);
    }
}
