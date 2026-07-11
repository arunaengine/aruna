//! Default-deny SSRF protection for server-side fetches. The security boundary
//! is DNS-answer filtering at connect time ([`ValidatingResolver`]), which
//! defeats rebinding; IP literals are checked statically by [`validate_url`] and
//! per redirect hop. The realm allowlist ([`EgressPolicy`]) only relaxes the
//! built-in deny table for deliberate internal destinations.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use aruna_core::structs::{EgressAllowRule, EgressConfig, HostPattern};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use serde::Serialize;
use thiserror::Error;
use url::{Host, Url};

/// Schemes permitted for HTTP-family connectors and the operator fetch clients.
pub const HTTP_SCHEMES: &[&str] = &["http", "https"];
/// Schemes permitted for FTP connectors.
pub const FTP_SCHEMES: &[&str] = &["ftp"];

/// Structured record of a denied egress attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EgressDenial {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub scheme: Option<String>,
    pub reason: DenialReason,
}

/// Why an egress attempt was denied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DenialReason {
    /// The URL scheme is not permitted for this call site.
    Scheme { scheme: String },
    /// An explicit port `0` was requested.
    Port { port: u16 },
    /// A candidate address falls inside a built-in denied range.
    IpRange { addr: String, range: String },
    /// A redirect hop was denied.
    RedirectHop { hop_url: String },
    /// An FTP endpoint is not covered by an allowlist rule.
    FtpNotAllowlisted,
    /// A hostname resolved to a mix of permitted and denied addresses.
    MixedDnsAnswers,
    /// The endpoint could not be parsed or resolved.
    Unresolvable { message: String },
}

impl DenialReason {
    pub fn code(&self) -> &'static str {
        match self {
            DenialReason::Scheme { .. } => "scheme",
            DenialReason::Port { .. } => "port",
            DenialReason::IpRange { .. } => "ip_range",
            DenialReason::RedirectHop { .. } => "redirect_hop",
            DenialReason::FtpNotAllowlisted => "ftp_not_allowlisted",
            DenialReason::MixedDnsAnswers => "mixed_dns_answers",
            DenialReason::Unresolvable { .. } => "unresolvable",
        }
    }
}

impl std::fmt::Display for EgressDenial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.reason {
            DenialReason::Scheme { scheme } => {
                write!(f, "scheme `{scheme}` is not permitted")
            }
            DenialReason::Port { port } => write!(f, "port `{port}` is not permitted"),
            DenialReason::IpRange { addr, range } => {
                write!(f, "address {addr} is within denied range {range}")
            }
            DenialReason::RedirectHop { hop_url } => {
                write!(f, "redirect to {hop_url} is not permitted")
            }
            DenialReason::FtpNotAllowlisted => {
                write!(f, "FTP endpoints require an explicit egress allowlist rule")
            }
            DenialReason::MixedDnsAnswers => write!(
                f,
                "hostname resolved to a mix of permitted and denied addresses"
            ),
            DenialReason::Unresolvable { message } => {
                write!(f, "endpoint could not be validated: {message}")
            }
        }
    }
}

impl std::error::Error for EgressDenial {}

impl EgressDenial {
    fn from_url(url: &Url, reason: DenialReason) -> Self {
        Self {
            host: url.host_str().map(ToOwned::to_owned),
            port: url.port(),
            scheme: Some(url.scheme().to_string()),
            reason,
        }
    }

    /// Converts to the dependency-light core error carried by the staging path.
    pub fn into_staging_error(self) -> aruna_core::errors::StagingSourceError {
        let reason = self.to_string();
        aruna_core::errors::StagingSourceError::EgressDenied {
            host: self.host,
            port: self.port,
            scheme: self.scheme,
            reason,
        }
    }
}

/// Compiled realm allowlist snapshot layered on top of the built-in deny table.
#[derive(Debug, Clone, Default)]
pub struct EgressPolicy {
    rules: Vec<CompiledRule>,
}

#[derive(Debug, Clone)]
struct CompiledRule {
    host: CompiledHost,
    ports: Option<Vec<u16>>,
    schemes: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
enum CompiledHost {
    Host(String),
    Cidr(IpNet),
}

/// Error compiling an allowlist rule (surfaced by [`validate_egress_config`]).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EgressConfigError {
    #[error("invalid CIDR `{0}` in egress allowlist")]
    InvalidCidr(String),
    #[error("empty host pattern in egress allowlist")]
    EmptyHost,
    #[error("invalid port `0` in egress allowlist rule for `{0}`")]
    ZeroPort(String),
    #[error("invalid scheme `{scheme}` in egress allowlist rule for `{host}`")]
    InvalidScheme { host: String, scheme: String },
}

fn compile_rule(
    rule: &aruna_core::structs::EgressAllowRule,
) -> Result<CompiledRule, EgressConfigError> {
    let host = match &rule.host {
        HostPattern::Host(host) => {
            let host = host.trim();
            if host.is_empty() {
                return Err(EgressConfigError::EmptyHost);
            }
            CompiledHost::Host(host.to_ascii_lowercase())
        }
        HostPattern::Cidr(cidr) => {
            let net = cidr
                .trim()
                .parse::<IpNet>()
                .map_err(|_| EgressConfigError::InvalidCidr(cidr.clone()))?;
            CompiledHost::Cidr(net)
        }
    };
    if let Some(ports) = &rule.ports
        && ports.contains(&0)
    {
        return Err(EgressConfigError::ZeroPort(host_label(&rule.host)));
    }
    if let Some(schemes) = &rule.schemes {
        for scheme in schemes {
            let scheme = scheme.to_ascii_lowercase();
            if !HTTP_SCHEMES.contains(&scheme.as_str()) && !FTP_SCHEMES.contains(&scheme.as_str()) {
                return Err(EgressConfigError::InvalidScheme {
                    host: host_label(&rule.host),
                    scheme: scheme.clone(),
                });
            }
        }
    }
    Ok(CompiledRule {
        host,
        ports: rule.ports.clone(),
        schemes: rule
            .schemes
            .as_ref()
            .map(|schemes| schemes.iter().map(|s| s.to_ascii_lowercase()).collect()),
    })
}

fn host_label(host: &HostPattern) -> String {
    match host {
        HostPattern::Host(host) => host.clone(),
        HostPattern::Cidr(cidr) => cidr.clone(),
    }
}

/// Validates every rule in a realm egress config; used by the write path so a
/// bad rule is rejected before it is stored and replicated.
pub fn validate_egress_config(config: &EgressConfig) -> Result<(), EgressConfigError> {
    for rule in &config.allow {
        compile_rule(rule)?;
    }
    Ok(())
}

impl EgressPolicy {
    /// Compiles a realm egress config. Invalid rules are skipped (fail closed:
    /// a rule that does not compile simply does not relax the deny table).
    pub fn from_config(config: &EgressConfig) -> Self {
        let mut rules = Vec::new();
        for rule in &config.allow {
            match compile_rule(rule) {
                Ok(compiled) => rules.push(compiled),
                Err(error) => {
                    tracing::warn!(%error, "skipping invalid egress allow rule");
                }
            }
        }
        Self { rules }
    }

    /// A policy with no allowlist rules: the built-in deny table alone.
    pub fn deny_all() -> Self {
        Self::default()
    }

    /// Resolver-time admit check (host/CIDR only; port/scheme narrowing is in
    /// [`validate_url`], which the resolver cannot see).
    fn admits_ip_for_host(&self, ip: IpAddr, host: &str) -> bool {
        let ip = normalize_ip(ip);
        let host = host.to_ascii_lowercase();
        self.rules.iter().any(|rule| match &rule.host {
            CompiledHost::Host(rule_host) => *rule_host == host,
            CompiledHost::Cidr(net) => net.contains(&ip),
        })
    }

    /// Full admit check for an IP literal: host/CIDR match plus any port and
    /// scheme narrowing on the matching rule.
    fn admits_literal(&self, ip: IpAddr, host: &str, port: Option<u16>, scheme: &str) -> bool {
        let ip = normalize_ip(ip);
        let host = host.to_ascii_lowercase();
        let scheme = scheme.to_ascii_lowercase();
        self.rules.iter().any(|rule| {
            let host_ok = match &rule.host {
                CompiledHost::Host(rule_host) => *rule_host == host,
                CompiledHost::Cidr(net) => net.contains(&ip),
            };
            host_ok && rule_permits_port(rule, port) && rule_permits_scheme(rule, &scheme)
        })
    }

    /// Whether an FTP endpoint host (and its resolved addresses) is covered by
    /// an allowlist rule. FTP is allowlist-only, so this gates resolution.
    fn ftp_allowlisted(&self, host: &str, addrs: &[IpAddr]) -> bool {
        let host = host.to_ascii_lowercase();
        self.rules.iter().any(|rule| match &rule.host {
            CompiledHost::Host(rule_host) => *rule_host == host,
            CompiledHost::Cidr(net) => addrs.iter().any(|ip| net.contains(&normalize_ip(*ip))),
        })
    }
}

fn rule_permits_port(rule: &CompiledRule, port: Option<u16>) -> bool {
    match &rule.ports {
        None => true,
        Some(ports) => port.is_some_and(|port| ports.contains(&port)),
    }
}

fn rule_permits_scheme(rule: &CompiledRule, scheme: &str) -> bool {
    match &rule.schemes {
        None => true,
        Some(schemes) => schemes.iter().any(|s| s == scheme),
    }
}

// --- Built-in deny table ---------------------------------------------------

static DENIED_V4: LazyLock<Vec<Ipv4Net>> = LazyLock::new(|| {
    [
        ("0.0.0.0", 8),
        ("10.0.0.0", 8),
        ("100.64.0.0", 10),
        ("127.0.0.0", 8),
        ("169.254.0.0", 16),
        ("172.16.0.0", 12),
        ("192.0.0.0", 24),
        ("192.168.0.0", 16),
        ("198.18.0.0", 15),
        ("224.0.0.0", 4),
        ("240.0.0.0", 4),
        ("255.255.255.255", 32),
    ]
    .into_iter()
    .map(|(addr, prefix)| Ipv4Net::new(addr.parse().unwrap(), prefix).unwrap())
    .collect()
});

static DENIED_V6: LazyLock<Vec<Ipv6Net>> = LazyLock::new(|| {
    [
        ("::", 128),
        ("::1", 128),
        ("fe80::", 10),
        ("fc00::", 7),
        ("ff00::", 8),
    ]
    .into_iter()
    .map(|(addr, prefix)| Ipv6Net::new(addr.parse().unwrap(), prefix).unwrap())
    .collect()
});

/// Unwraps IPv4-mapped (`::ffff:a.b.c.d`) and NAT64 (`64:ff9b::/96`) IPv6
/// addresses to the embedded IPv4 address so they are never passed through.
fn normalize_ip(ip: IpAddr) -> IpAddr {
    let IpAddr::V6(v6) = ip else {
        return ip;
    };
    if let Some(v4) = v6.to_ipv4_mapped() {
        return IpAddr::V4(v4);
    }
    let segments = v6.segments();
    if segments[0] == 0x0064
        && segments[1] == 0xff9b
        && segments[2] == 0
        && segments[3] == 0
        && segments[4] == 0
        && segments[5] == 0
    {
        let octets = v6.octets();
        return IpAddr::V4(Ipv4Addr::new(
            octets[12], octets[13], octets[14], octets[15],
        ));
    }
    IpAddr::V6(v6)
}

/// Returns the denied range containing `ip` (after unwrapping), if any.
pub fn builtin_deny_range(ip: IpAddr) -> Option<String> {
    match normalize_ip(ip) {
        IpAddr::V4(v4) => DENIED_V4
            .iter()
            .find(|net| net.contains(&v4))
            .map(ToString::to_string),
        IpAddr::V6(v6) => DENIED_V6
            .iter()
            .find(|net| net.contains(&v6))
            .map(ToString::to_string),
    }
}

// --- Static URL validation -------------------------------------------------

/// Static pre-request validation: scheme, explicit port `0`, and IP-literal
/// hosts. Hostname hosts pass here and are gated at connect time by
/// [`ValidatingResolver`].
pub fn validate_url(
    policy: &EgressPolicy,
    allowed_schemes: &[&str],
    url: &Url,
) -> Result<(), EgressDenial> {
    let scheme = url.scheme();
    if !allowed_schemes
        .iter()
        .any(|s| s.eq_ignore_ascii_case(scheme))
    {
        return Err(EgressDenial::from_url(
            url,
            DenialReason::Scheme {
                scheme: scheme.to_string(),
            },
        ));
    }
    if url.port() == Some(0) {
        return Err(EgressDenial::from_url(url, DenialReason::Port { port: 0 }));
    }
    match url.host() {
        Some(Host::Ipv4(v4)) => check_literal(policy, url, IpAddr::V4(v4)),
        Some(Host::Ipv6(v6)) => check_literal(policy, url, IpAddr::V6(v6)),
        Some(Host::Domain(_)) | None => Ok(()),
    }
}

/// Parses and statically validates a URL string. Call sites that build a
/// hardened client but issue requests directly (OIDC, portal, onboarding) must
/// call this before each request: reqwest connects to IP-literal hosts without
/// consulting the resolver, so only this check blocks a literal metadata URL.
pub fn check_url(
    policy: &EgressPolicy,
    allowed_schemes: &[&str],
    url: &str,
) -> Result<(), EgressDenial> {
    let url = Url::parse(url).map_err(|error| EgressDenial {
        host: None,
        port: None,
        scheme: None,
        reason: DenialReason::Unresolvable {
            message: error.to_string(),
        },
    })?;
    validate_url(policy, allowed_schemes, &url)
}

fn check_literal(policy: &EgressPolicy, url: &Url, ip: IpAddr) -> Result<(), EgressDenial> {
    let Some(range) = builtin_deny_range(ip) else {
        return Ok(());
    };
    let host = url.host_str().unwrap_or_default();
    if policy.admits_literal(ip, host, url.port_or_known_default(), url.scheme()) {
        return Ok(());
    }
    Err(EgressDenial::from_url(
        url,
        DenialReason::IpRange {
            addr: normalize_ip(ip).to_string(),
            range,
        },
    ))
}

// --- Connect-time resolver -------------------------------------------------

/// The default system resolver used when no override is supplied.
#[derive(Debug, Default)]
struct SystemResolver;

impl Resolve for SystemResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addrs = tokio::net::lookup_host((host.as_str(), 0)).await?;
            let addrs: Addrs = Box::new(addrs.collect::<Vec<_>>().into_iter());
            Ok(addrs)
        })
    }
}

/// Wraps an inner resolver and fails closed if any answer is denied and not
/// allowlisted. reqwest can only connect to addresses the resolver returned, so
/// this holds per attempt, including retries and redirect hops.
#[derive(Clone)]
pub struct ValidatingResolver {
    inner: Arc<dyn Resolve>,
    policy: Arc<EgressPolicy>,
}

impl ValidatingResolver {
    pub fn new(inner: Arc<dyn Resolve>, policy: Arc<EgressPolicy>) -> Self {
        Self { inner, policy }
    }

    /// Wraps the system resolver.
    pub fn system(policy: Arc<EgressPolicy>) -> Self {
        Self::new(Arc::new(SystemResolver), policy)
    }
}

impl Resolve for ValidatingResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        let inner = self.inner.clone();
        let policy = self.policy.clone();
        Box::pin(async move {
            let resolved = inner.resolve(name).await?;
            let addrs: Vec<SocketAddr> = resolved.collect();
            match screen_addresses(&policy, &host, addrs.iter().map(SocketAddr::ip)) {
                Ok(()) => Ok(Box::new(addrs.into_iter()) as Addrs),
                Err(denial) => Err(Box::new(denial) as Box<dyn std::error::Error + Send + Sync>),
            }
        })
    }
}

/// Fails closed if any address is denied and not allowlisted; reports
/// `MixedDnsAnswers` when some answers are permitted and others are not.
fn screen_addresses(
    policy: &EgressPolicy,
    host: &str,
    addrs: impl Iterator<Item = IpAddr>,
) -> Result<(), EgressDenial> {
    let mut denied: Option<(IpAddr, String)> = None;
    let mut permitted = 0usize;
    for ip in addrs {
        match builtin_deny_range(ip) {
            Some(range) if !policy.admits_ip_for_host(ip, host) => {
                if denied.is_none() {
                    denied = Some((ip, range));
                }
            }
            _ => permitted += 1,
        }
    }
    let Some((ip, range)) = denied else {
        return Ok(());
    };
    let reason = if permitted > 0 {
        DenialReason::MixedDnsAnswers
    } else {
        DenialReason::IpRange {
            addr: normalize_ip(ip).to_string(),
            range,
        }
    };
    Err(EgressDenial {
        host: Some(host.to_string()),
        port: None,
        scheme: None,
        reason,
    })
}

/// Request-time pre-flight: static validation plus a screened resolution for
/// hostname hosts, yielding a precise [`EgressDenial`] before the request.
/// [`ValidatingResolver`] remains the connect-time boundary against rebinding.
pub async fn preflight_resolve(
    policy: &EgressPolicy,
    allowed_schemes: &[&str],
    url: &Url,
    resolver_override: Option<Arc<dyn Resolve>>,
) -> Result<(), EgressDenial> {
    validate_url(policy, allowed_schemes, url)?;
    let Some(Host::Domain(host)) = url.host() else {
        return Ok(());
    };
    let host = host.to_string();
    let name = Name::from_str(&host).map_err(|error| EgressDenial {
        host: Some(host.clone()),
        port: url.port(),
        scheme: Some(url.scheme().to_string()),
        reason: DenialReason::Unresolvable {
            message: error.to_string(),
        },
    })?;
    let resolver = resolver_override.unwrap_or_else(|| Arc::new(SystemResolver));
    let addrs = resolver.resolve(name).await.map_err(|error| EgressDenial {
        host: Some(host.clone()),
        port: url.port(),
        scheme: Some(url.scheme().to_string()),
        reason: DenialReason::Unresolvable {
            message: error.to_string(),
        },
    })?;
    screen_addresses(policy, &host, addrs.map(|addr| addr.ip()))
}

// --- Redirect policy -------------------------------------------------------

/// A redirect policy that re-validates each hop (scheme + IP literal) and caps
/// the chain at 10 URLs total (`previous()` includes the initial URL). Hostname
/// hops are still forced through [`ValidatingResolver`] at connect time.
pub fn redirect_policy(
    policy: Arc<EgressPolicy>,
    allowed_schemes: &'static [&'static str],
) -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(move |attempt| {
        let url = attempt.url().clone();
        let denial = EgressDenial::from_url(
            &url,
            DenialReason::RedirectHop {
                hop_url: url.to_string(),
            },
        );
        if attempt.previous().len() >= 10 {
            return attempt.error(denial);
        }
        match validate_url(&policy, allowed_schemes, &url) {
            Ok(()) => attempt.follow(),
            Err(_) => attempt.error(denial),
        }
    })
}

// --- Client constructor ----------------------------------------------------

/// Applies egress hardening (redirect re-validation + connect-time resolver) to
/// a caller-provided builder, so call sites keep their own timeouts and options.
/// `resolver_override` is the deterministic test hook; production passes `None`.
pub fn harden_builder(
    builder: reqwest::ClientBuilder,
    policy: Arc<EgressPolicy>,
    allowed_schemes: &'static [&'static str],
    resolver_override: Option<Arc<dyn Resolve>>,
) -> reqwest::ClientBuilder {
    let inner = resolver_override.unwrap_or_else(|| Arc::new(SystemResolver));
    let resolver = ValidatingResolver::new(inner, policy.clone());
    builder
        .redirect(redirect_policy(policy, allowed_schemes))
        .dns_resolver(resolver)
}

/// The single hardened reqwest client constructor used by every call site.
pub fn hardened_client(
    policy: Arc<EgressPolicy>,
    allowed_schemes: &'static [&'static str],
    resolver_override: Option<Arc<dyn Resolve>>,
) -> reqwest::Result<reqwest::Client> {
    harden_builder(
        reqwest::Client::builder(),
        policy,
        allowed_schemes,
        resolver_override,
    )
    .build()
}

/// Environment variable naming the pre-realm-config allowlist (onboarding and
/// portal fetches only; never merged into the realm egress policy).
pub const ENV_ALLOWLIST_VAR: &str = "ARUNA_EGRESS_ALLOW";

/// Parses a comma-separated list of hosts and CIDRs into an egress config.
pub fn parse_env_allowlist(value: &str) -> EgressConfig {
    let allow = value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            let host = if entry.parse::<IpNet>().is_ok() {
                HostPattern::Cidr(entry.to_string())
            } else {
                HostPattern::Host(entry.to_ascii_lowercase())
            };
            EgressAllowRule {
                host,
                ports: None,
                schemes: None,
                comment: None,
            }
        })
        .collect();
    EgressConfig { allow }
}

/// Builds the pre-realm-config policy from `ARUNA_EGRESS_ALLOW`, or deny-only.
pub fn env_allowlist_policy() -> EgressPolicy {
    match std::env::var(ENV_ALLOWLIST_VAR) {
        Ok(value) => EgressPolicy::from_config(&parse_env_allowlist(&value)),
        Err(_) => EgressPolicy::deny_all(),
    }
}

// --- FTP resolve-and-pin ---------------------------------------------------

/// Validates and pins an FTP endpoint. FTP is allowlist-only; on success the
/// host is rewritten to one validated IP literal so the control connection
/// cannot rebind (the PASV data channel remains server-controlled).
pub async fn resolve_and_pin_ftp(
    policy: &EgressPolicy,
    endpoint: &str,
    resolver_override: Option<&dyn FtpResolver>,
) -> Result<String, EgressDenial> {
    let url = Url::parse(endpoint).map_err(|error| EgressDenial {
        host: None,
        port: None,
        scheme: None,
        reason: DenialReason::Unresolvable {
            message: error.to_string(),
        },
    })?;
    if !FTP_SCHEMES
        .iter()
        .any(|s| s.eq_ignore_ascii_case(url.scheme()))
    {
        return Err(EgressDenial::from_url(
            &url,
            DenialReason::Scheme {
                scheme: url.scheme().to_string(),
            },
        ));
    }
    if url.port() == Some(0) {
        return Err(EgressDenial::from_url(&url, DenialReason::Port { port: 0 }));
    }
    let host = url
        .host_str()
        .ok_or_else(|| {
            EgressDenial::from_url(
                &url,
                DenialReason::Unresolvable {
                    message: "missing host".to_string(),
                },
            )
        })?
        .to_string();

    let addrs: Vec<IpAddr> = match url.host() {
        Some(Host::Ipv4(v4)) => vec![IpAddr::V4(v4)],
        Some(Host::Ipv6(v6)) => vec![IpAddr::V6(v6)],
        _ => {
            let port = url.port_or_known_default().unwrap_or(21);
            match resolver_override {
                Some(resolver) => resolver.lookup(&host, port).await,
                None => tokio::net::lookup_host((host.as_str(), port))
                    .await
                    .map(|iter| iter.map(|addr| addr.ip()).collect())
                    .map_err(|error| error.to_string()),
            }
            .map_err(|message| {
                EgressDenial::from_url(&url, DenialReason::Unresolvable { message })
            })?
        }
    };

    if addrs.is_empty() {
        return Err(EgressDenial::from_url(
            &url,
            DenialReason::Unresolvable {
                message: "no addresses resolved".to_string(),
            },
        ));
    }

    if !policy.ftp_allowlisted(&host, &addrs) {
        return Err(EgressDenial::from_url(
            &url,
            DenialReason::FtpNotAllowlisted,
        ));
    }

    // Pin to the first address that is either public or covered by the
    // allowlist; reject if any address is denied and not allowlisted.
    let mut pinned: Option<IpAddr> = None;
    for ip in &addrs {
        if let Some(range) = builtin_deny_range(*ip)
            && !policy.admits_ip_for_host(*ip, &host)
        {
            return Err(EgressDenial::from_url(
                &url,
                DenialReason::IpRange {
                    addr: normalize_ip(*ip).to_string(),
                    range,
                },
            ));
        }
        if pinned.is_none() {
            pinned = Some(normalize_ip(*ip));
        }
    }

    let mut pinned_url = url.clone();
    let literal = match pinned.unwrap() {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => format!("[{v6}]"),
    };
    pinned_url.set_host(Some(&literal)).map_err(|error| {
        EgressDenial::from_url(
            &url,
            DenialReason::Unresolvable {
                message: error.to_string(),
            },
        )
    })?;
    Ok(pinned_url.to_string())
}

/// Deterministic resolver hook for FTP tests. Returns resolved addresses or an
/// error message.
pub trait FtpResolver: Send + Sync {
    fn lookup<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<IpAddr>, String>> + Send + 'a>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{EgressAllowRule, EgressConfig, HostPattern};

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn cidr_policy(cidr: &str) -> EgressPolicy {
        EgressPolicy::from_config(&EgressConfig {
            allow: vec![EgressAllowRule {
                host: HostPattern::Cidr(cidr.to_string()),
                ports: None,
                schemes: None,
                comment: None,
            }],
        })
    }

    fn host_policy(host: &str) -> EgressPolicy {
        EgressPolicy::from_config(&EgressConfig {
            allow: vec![EgressAllowRule {
                host: HostPattern::Host(host.to_string()),
                ports: None,
                schemes: None,
                comment: None,
            }],
        })
    }

    #[test]
    fn denies_v4_ranges() {
        for addr in [
            "0.0.0.0",
            "10.0.0.5",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.0.5",
            "169.254.169.254",
            "172.16.5.5",
            "192.0.0.1",
            "192.168.1.1",
            "198.18.0.1",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
        ] {
            assert!(
                builtin_deny_range(ip(addr)).is_some(),
                "expected {addr} denied"
            );
        }
    }

    #[test]
    fn denies_v6_ranges() {
        for addr in [
            "::",
            "::1",
            "fe80::1",
            "fc00::1",
            "fd00:ec2::254",
            "ff00::1",
        ] {
            assert!(
                builtin_deny_range(ip(addr)).is_some(),
                "expected {addr} denied"
            );
        }
    }

    #[test]
    fn allows_public_addresses() {
        assert!(builtin_deny_range(ip("93.184.216.34")).is_none());
        assert!(builtin_deny_range(ip("2606:2800:220:1::1")).is_none());
        assert!(builtin_deny_range(ip("8.8.8.8")).is_none());
    }

    #[test]
    // Covers IPv4-mapped and NAT64 wrappings.
    fn unwraps_embedded_v4() {
        assert!(builtin_deny_range(ip("::ffff:127.0.0.1")).is_some());
        assert!(builtin_deny_range(ip("64:ff9b::7f00:1")).is_some());
        // A mapped/NAT64 wrapping of a public address stays permitted.
        assert!(builtin_deny_range(ip("::ffff:93.184.216.34")).is_none());
        assert!(builtin_deny_range(ip("64:ff9b::5db8:d822")).is_none());
    }

    #[test]
    fn encoded_literals_normalized() {
        let policy = EgressPolicy::deny_all();
        for endpoint in [
            "http://0x7f000001/",
            "http://0177.0.0.1/",
            "http://2130706433/",
            "http://127.1/",
            "http://[::ffff:127.0.0.1]/",
            "http://[64:ff9b::7f00:1]/",
        ] {
            let url = Url::parse(endpoint).unwrap();
            let result = validate_url(&policy, HTTP_SCHEMES, &url);
            assert!(
                result.is_err(),
                "expected {endpoint} denied, got {result:?}"
            );
            assert!(matches!(
                result.unwrap_err().reason,
                DenialReason::IpRange { .. }
            ));
        }
    }

    #[test]
    fn rejects_scheme_port() {
        let policy = EgressPolicy::deny_all();
        let scheme = validate_url(
            &policy,
            HTTP_SCHEMES,
            &Url::parse("ftp://example.org/").unwrap(),
        );
        assert!(matches!(
            scheme.unwrap_err().reason,
            DenialReason::Scheme { .. }
        ));
        let port = validate_url(
            &policy,
            HTTP_SCHEMES,
            &Url::parse("http://93.184.216.34:0/").unwrap(),
        );
        assert!(matches!(
            port.unwrap_err().reason,
            DenialReason::Port { .. }
        ));
    }

    #[test]
    fn cidr_admits_literal() {
        let policy = cidr_policy("10.0.0.0/8");
        let url = Url::parse("http://10.0.0.5/").unwrap();
        assert!(validate_url(&policy, HTTP_SCHEMES, &url).is_ok());
        // Outside the CIDR is still denied.
        let other = Url::parse("http://127.0.0.1/").unwrap();
        assert!(validate_url(&policy, HTTP_SCHEMES, &other).is_err());
    }

    #[test]
    fn narrows_port_scheme() {
        let policy = EgressPolicy::from_config(&EgressConfig {
            allow: vec![EgressAllowRule {
                host: HostPattern::Cidr("127.0.0.0/8".to_string()),
                ports: Some(vec![8443]),
                schemes: Some(vec!["https".to_string()]),
                comment: None,
            }],
        });
        assert!(
            validate_url(
                &policy,
                HTTP_SCHEMES,
                &Url::parse("https://127.0.0.1:8443/").unwrap()
            )
            .is_ok()
        );
        // Wrong port.
        assert!(
            validate_url(
                &policy,
                HTTP_SCHEMES,
                &Url::parse("https://127.0.0.1:9000/").unwrap()
            )
            .is_err()
        );
        // Wrong scheme.
        assert!(
            validate_url(
                &policy,
                HTTP_SCHEMES,
                &Url::parse("http://127.0.0.1:8443/").unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn hostname_admits_denied() {
        let policy = host_policy("rebind.test");
        assert!(policy.admits_ip_for_host(ip("127.0.0.1"), "rebind.test"));
        assert!(!policy.admits_ip_for_host(ip("127.0.0.1"), "other.test"));
    }

    #[test]
    fn screen_fails_closed() {
        let policy = EgressPolicy::deny_all();
        // Pure denied.
        let denied = screen_addresses(&policy, "evil.test", [ip("127.0.0.1")].into_iter());
        assert!(matches!(
            denied.unwrap_err().reason,
            DenialReason::IpRange { .. }
        ));
        // Mixed public + denied.
        let mixed = screen_addresses(
            &policy,
            "evil.test",
            [ip("93.184.216.34"), ip("127.0.0.1")].into_iter(),
        );
        assert!(matches!(
            mixed.unwrap_err().reason,
            DenialReason::MixedDnsAnswers
        ));
        // All public passes.
        assert!(screen_addresses(&policy, "ok.test", [ip("93.184.216.34")].into_iter()).is_ok());
    }

    #[test]
    fn screen_honors_allowlist() {
        let policy = host_policy("rebind.test");
        assert!(screen_addresses(&policy, "rebind.test", [ip("127.0.0.1")].into_iter()).is_ok());
        // A different host resolving to the same address is still denied.
        assert!(screen_addresses(&policy, "evil.test", [ip("127.0.0.1")].into_iter()).is_err());
    }

    #[test]
    fn parses_env_allowlist() {
        let config = parse_env_allowlist(" 10.0.0.0/8 , seed.internal ,, fd00::/8 ");
        assert_eq!(config.allow.len(), 3);
        assert!(matches!(config.allow[0].host, HostPattern::Cidr(ref c) if c == "10.0.0.0/8"));
        assert!(matches!(config.allow[1].host, HostPattern::Host(ref h) if h == "seed.internal"));
        assert!(matches!(config.allow[2].host, HostPattern::Cidr(ref c) if c == "fd00::/8"));
    }

    #[test]
    fn rejects_bad_cidr() {
        let bad = EgressConfig {
            allow: vec![EgressAllowRule {
                host: HostPattern::Cidr("not-a-cidr".to_string()),
                ports: None,
                schemes: None,
                comment: None,
            }],
        };
        assert!(matches!(
            validate_egress_config(&bad),
            Err(EgressConfigError::InvalidCidr(_))
        ));
    }

    struct ScriptedResolver {
        answers: Vec<Vec<IpAddr>>,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl Resolve for ScriptedResolver {
        fn resolve(&self, _name: Name) -> Resolving {
            let index = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let answer = self.answers.get(index).cloned().unwrap_or_default();
            Box::pin(async move {
                let addrs: Addrs = Box::new(
                    answer
                        .into_iter()
                        .map(|ip| SocketAddr::new(ip, 0))
                        .collect::<Vec<_>>()
                        .into_iter(),
                );
                Ok(addrs)
            })
        }
    }

    #[tokio::test]
    async fn resolver_blocks_rebinding() {
        let scripted = Arc::new(ScriptedResolver {
            answers: vec![vec![ip("93.184.216.34")], vec![ip("127.0.0.1")]],
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let resolver = ValidatingResolver::new(scripted, Arc::new(EgressPolicy::deny_all()));

        // First lookup: public address is allowed.
        let first = resolver.resolve("rebind.test".parse().unwrap()).await;
        assert!(first.is_ok());

        // Second lookup: rebound to loopback, must fail closed.
        let second = resolver.resolve("rebind.test".parse().unwrap()).await;
        assert!(second.is_err());
    }

    #[tokio::test]
    async fn ftp_requires_allowlist() {
        struct FixedFtpResolver(Vec<IpAddr>);
        impl FtpResolver for FixedFtpResolver {
            fn lookup<'a>(
                &'a self,
                _host: &'a str,
                _port: u16,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<Vec<IpAddr>, String>> + Send + 'a>,
            > {
                let addrs = self.0.clone();
                Box::pin(async move { Ok(addrs) })
            }
        }

        let resolver = FixedFtpResolver(vec![ip("10.0.0.7")]);

        // Without an allowlist rule, resolution is refused.
        let deny = EgressPolicy::deny_all();
        let denied = resolve_and_pin_ftp(&deny, "ftp://ftp.internal:21/", Some(&resolver)).await;
        assert!(matches!(
            denied.unwrap_err().reason,
            DenialReason::FtpNotAllowlisted
        ));

        // With a covering CIDR rule, the endpoint is pinned to the literal.
        let allow = cidr_policy("10.0.0.0/8");
        // Port 21 is the FTP default, so the URL parser omits it after pinning.
        let pinned = resolve_and_pin_ftp(&allow, "ftp://ftp.internal:21/", Some(&resolver))
            .await
            .unwrap();
        assert_eq!(pinned, "ftp://10.0.0.7/");
        // A non-default port is preserved on the pinned literal.
        let pinned_custom =
            resolve_and_pin_ftp(&allow, "ftp://ftp.internal:2121/", Some(&resolver))
                .await
                .unwrap();
        assert_eq!(pinned_custom, "ftp://10.0.0.7:2121/");
    }

    #[tokio::test]
    async fn redirect_metadata_blocked() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = axum::Router::new().fallback(|| async {
            axum::response::Redirect::temporary("http://169.254.169.254/latest/meta-data")
        });
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let client =
            hardened_client(Arc::new(cidr_policy("127.0.0.0/8")), HTTP_SCHEMES, None).unwrap();
        let result = client
            .get(format!("http://127.0.0.1:{}/", addr.port()))
            .send()
            .await;
        assert!(result.is_err(), "redirect to metadata IP must be blocked");
    }

    #[tokio::test]
    async fn redirect_allowlisted_followed() {
        let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_b = listener_b.local_addr().unwrap();
        let router_b = axum::Router::new().fallback(|| async { "ok" });
        tokio::spawn(async move {
            axum::serve(listener_b, router_b).await.unwrap();
        });

        let target = format!("http://127.0.0.1:{}/", addr_b.port());
        let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_a = listener_a.local_addr().unwrap();
        let router_a = axum::Router::new().fallback(move || {
            let target = target.clone();
            async move { axum::response::Redirect::temporary(&target) }
        });
        tokio::spawn(async move {
            axum::serve(listener_a, router_a).await.unwrap();
        });

        let client =
            hardened_client(Arc::new(cidr_policy("127.0.0.0/8")), HTTP_SCHEMES, None).unwrap();
        let response = client
            .get(format!("http://127.0.0.1:{}/", addr_a.port()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.text().await.unwrap(), "ok");
    }
}
