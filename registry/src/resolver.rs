//! Resolves `https://w3id.org/aruna/` paths to the realm or the legacy v2 site that holds them.
//! It only redirects: to approved realms, the fixed v2 sites and the project home.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::num::{NonZeroU32, NonZeroUsize};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aruna_blob::egress::EgressGuard;
use aruna_core::MetaResourceId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::replication::{
    ARUNA_DATA_PREFIX, ArunaArn, VersionedObjectArn, W3idIdentifier,
};
use aruna_core::time::unix_timestamp_secs;
use axum::Router;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures::StreamExt;
use governor::{DefaultDirectRateLimiter, DefaultKeyedRateLimiter, Quota, RateLimiter};
use lru::LruCache;
use percent_encoding::percent_decode_str;
use tracing::warn;
use ulid::Ulid;
use url::Url;

use crate::store::StoreError;
use crate::{RegistryState, resolve_target};

/// The project home, also the page of vocabulary terms until they have their own.
const HOME: &str = "https://aruna-engine.org/";
/// Paths of software vocabularies, which belong to no realm.
const VOCABULARIES: [&str; 3] = ["terms/", "invenio/", "profiles/repository/"];
/// The node route that picks the page of a w3id path, under the realm's API base path.
const NODE_ROUTE: &str = "w3id";
/// v2 minted `https://w3id.org/aruna/{ULID}` on both sites; the w3id rule sent it to the first.
const V2: Legacy = Legacy {
    page: "https://aruna-engine.org/objects/",
    api: "https://api.aruna-engine.org/v2/resources/",
};
const V2_DEV: Legacy = Legacy {
    page: "https://dev.aruna-engine.org/objects/",
    api: "https://api.dev.aruna-engine.org/v2/resources/",
};
/// Guesses are kept briefly, so a new or withdrawn identifier is seen within minutes.
const CACHE_SECS: u64 = 300;
const CACHE_ENTRIES: usize = 4096;
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// Sites asked at the same time for one guess.
const PROBE_SLOTS: usize = 8;
/// Guesses that ask other sites, per minute across all clients, and their burst.
const GUESSES_PER_MINUTE: u32 = 600;
const GUESS_BURST: u32 = 60;
/// The stricter share of one client, charged first, so one caller cannot use up the budget.
const CLIENT_GUESSES_PER_MINUTE: u32 = 60;
const CLIENT_BURST: u32 = 20;
/// Client limiter keys above which idle ones are dropped.
const CLIENT_ENTRIES: usize = 10_000;

/// A v2 site: its object page and the public API route that answers 404 for unknown ids.
struct Legacy {
    page: &'static str,
    api: &'static str,
}

/// What one asked site said about an identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Holding {
    Holds,
    Absent,
    Unavailable,
}

/// Realm nodes answer their w3id route, v2 sites their resources route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    Realm,
    Legacy,
}

/// One asked site: the redirect target if it holds the id, who it is, and what it said.
type Asked = (String, Source, Holding);

#[derive(Clone, Debug, PartialEq, Eq)]
enum Answer {
    Redirect(String),
    NotFound(&'static str),
    Conflict,
    Unavailable,
    Busy,
}

/// A w3id path parsed into a typed id; the request path is never used as a URL.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Shape {
    Home,
    Document(Ulid),
    Dev(Ulid),
    Profile(Ulid),
    /// A data id re-serialized canonically, with its realm when the id names one.
    Data(Option<RealmId>, String),
    Unknown,
}

/// A kept guess. A winning realm is kept by id and its target rebuilt from the live
/// registration on each use, so withdrawal, lease expiry and URL changes apply at once.
#[derive(Clone)]
enum Kept {
    Realm(RealmId),
    Fixed(Answer),
}

pub struct Resolver {
    registry: Arc<RegistryState>,
    /// Approved realms, the default realm first; only these are asked and redirected to.
    approved: Vec<RealmId>,
    cache: Mutex<LruCache<String, (Kept, u64)>>,
    guesses: DefaultDirectRateLimiter,
    clients: DefaultKeyedRateLimiter<IpAddr>,
    /// The header in which the ingress names the client address; the peer address otherwise.
    client_header: Option<HeaderName>,
}

impl Resolver {
    /// `default_realm` answers ids that name no realm; it counts as approved.
    pub fn new(
        registry: Arc<RegistryState>,
        approved: Vec<RealmId>,
        default_realm: Option<RealmId>,
        client_header: Option<HeaderName>,
    ) -> Self {
        let mut realms: Vec<RealmId> = default_realm.into_iter().collect();
        realms.extend(
            approved
                .into_iter()
                .filter(|realm| Some(*realm) != default_realm),
        );
        let quota = Quota::per_minute(NonZeroU32::new(GUESSES_PER_MINUTE).expect("nonzero rate"))
            .allow_burst(NonZeroU32::new(GUESS_BURST).expect("nonzero burst"));
        Self {
            registry,
            approved: realms,
            cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(CACHE_ENTRIES).expect("nonzero cache"),
            )),
            guesses: RateLimiter::direct(quota),
            clients: RateLimiter::keyed(
                Quota::per_minute(
                    NonZeroU32::new(CLIENT_GUESSES_PER_MINUTE).expect("nonzero rate"),
                )
                .allow_burst(NonZeroU32::new(CLIENT_BURST).expect("nonzero burst")),
            ),
            client_header,
        }
    }

    async fn answer<P, F>(&self, shape: Shape, now: u64, client: IpAddr, probe: P) -> Answer
    where
        P: Fn(Url, Source) -> F,
        F: Future<Output = Holding>,
    {
        match shape {
            Shape::Home => Answer::Redirect(HOME.to_string()),
            Shape::Unknown => Answer::NotFound("not an Aruna identifier"),
            Shape::Data(realm, path) => match realm.or(self.approved.first().copied()) {
                Some(realm) => self.direct(realm, &format!("data/{path}"), now),
                None => Answer::NotFound("no realm resolves this identifier"),
            },
            shape => self.guess(shape, now, client, probe).await,
        }
    }

    /// A redirect to an approved realm's node route, without asking it.
    fn direct(&self, realm: RealmId, path: &str, now: u64) -> Answer {
        if !self.approved.contains(&realm) {
            return Answer::NotFound("realm not resolved here");
        }
        match self.realm_target(realm, now, path) {
            Ok(Some(target)) => Answer::Redirect(target),
            Ok(None) => Answer::NotFound("realm not publicly reachable"),
            Err(error) => storage_failed(error),
        }
    }

    /// Asks the approved realms, and for a bare id the v2 sites, which one holds `shape`.
    async fn guess<P, F>(&self, shape: Shape, now: u64, client: IpAddr, probe: P) -> Answer
    where
        P: Fn(Url, Source) -> F,
        F: Future<Output = Holding>,
    {
        let (key, id, node_path) = match &shape {
            Shape::Document(id) => (id.to_string(), *id, id.to_string()),
            Shape::Dev(id) => (format!("dev/{id}"), *id, id.to_string()),
            Shape::Profile(id) => (format!("profile/{id}"), *id, format!("profile/{id}")),
            _ => return Answer::NotFound("not an Aruna identifier"),
        };
        if let Some(answer) = self.cached(&key, &node_path, now) {
            return answer;
        }
        let refused = self.clients.check_key(&client).is_err() || self.guesses.check().is_err();
        if self.clients.len() > CLIENT_ENTRIES {
            self.clients.retain_recent();
        }
        if refused {
            return Answer::Busy;
        }
        // A ULID with the reserved placement handle was never minted by a v3 realm.
        let structured = MetaResourceId::from_str(&id.to_string()).is_ok();
        let realms = match structured {
            true => match self.realm_targets(now, &node_path) {
                Ok(targets) => targets,
                Err(error) => return storage_failed(error),
            },
            false => Vec::new(),
        };
        let legacy = match shape {
            Shape::Document(_) => vec![V2, V2_DEV],
            _ => Vec::new(),
        };
        let mut asked = Vec::new();
        for (_, target) in &realms {
            if let Ok(url) = Url::parse(target) {
                asked.push((target.clone(), url, Source::Realm));
            }
        }
        for site in &legacy {
            if let Ok(url) = Url::parse(&format!("{}{id}", site.api)) {
                asked.push((format!("{}{id}", site.page), url, Source::Legacy));
            }
        }
        let probe = &probe;
        let answers: Vec<Asked> = futures::stream::iter(asked)
            .map(|(target, url, source)| async move { (target, source, probe(url, source).await) })
            .buffered(PROBE_SLOTS)
            .collect()
            .await;
        let answer = decide(&shape, &answers);
        if answer == Answer::Conflict {
            warn!(id = %id, "More than one approved realm claims one identifier");
        }
        let settled = answers
            .iter()
            .all(|(_, _, holding)| *holding != Holding::Unavailable);
        let kept = match &answer {
            Answer::Redirect(target) => realms
                .iter()
                .find(|(_, realm_target)| realm_target == target)
                .map_or(Kept::Fixed(answer.clone()), |(realm, _)| {
                    Kept::Realm(*realm)
                }),
            _ => Kept::Fixed(answer.clone()),
        };
        if settled
            && matches!(answer, Answer::Redirect(_) | Answer::NotFound(_))
            && let Ok(mut cache) = self.cache.lock()
        {
            cache.put(key, (kept, now + CACHE_SECS));
        }
        answer
    }

    /// A kept answer; a kept realm that is no longer redirectable is dropped and guessed again.
    fn cached(&self, key: &str, node_path: &str, now: u64) -> Option<Answer> {
        let kept = {
            let mut cache = self.cache.lock().ok()?;
            match cache.get(key) {
                Some((kept, expires_at)) if *expires_at > now => kept.clone(),
                Some(_) => {
                    cache.pop(key);
                    return None;
                }
                None => return None,
            }
        };
        match kept {
            Kept::Fixed(answer) => Some(answer),
            Kept::Realm(realm) => match self.direct(realm, node_path, now) {
                Answer::Redirect(target) => Some(Answer::Redirect(target)),
                _ => {
                    if let Ok(mut cache) = self.cache.lock() {
                        cache.pop(key);
                    }
                    None
                }
            },
        }
    }

    /// The node route for `path` of each approved realm with a live lease, in approval order.
    fn realm_targets(&self, now: u64, path: &str) -> Result<Vec<(RealmId, String)>, StoreError> {
        let mut targets = Vec::new();
        for realm in &self.approved {
            if let Some(target) = self.realm_target(*realm, now, path)? {
                targets.push((*realm, target));
            }
        }
        Ok(targets)
    }

    /// The node route for `path` of `realm` from its current registration, if leased.
    fn realm_target(
        &self,
        realm: RealmId,
        now: u64,
        path: &str,
    ) -> Result<Option<String>, StoreError> {
        let Some(entry) = self.registry.store.entry(&realm, now)? else {
            return Ok(None);
        };
        let api_url = &entry.registration.payload.descriptor.payload.api_url;
        // w3id.org would send the redirect back here; refusing it keeps every chain finite.
        if api_url.host_str() == Some("w3id.org") {
            return Ok(None);
        }
        Ok(resolve_target(api_url, NODE_ROUTE, path))
    }
}

fn storage_failed(error: StoreError) -> Answer {
    warn!(error = %error, "Registry storage failed");
    Answer::Unavailable
}

/// The guess from every answer: one realm holder wins, two conflict; then a v2 site that knows
/// the id; then the most likely page, unless an unreachable realm may hold the id.
fn decide(shape: &Shape, answers: &[Asked]) -> Answer {
    let said = |source, holding| {
        answers
            .iter()
            .filter(move |answer| answer.1 == source && answer.2 == holding)
            .map(|answer| &answer.0)
    };
    let holders: Vec<&String> = said(Source::Realm, Holding::Holds).collect();
    match holders.as_slice() {
        [one] => return Answer::Redirect((*one).clone()),
        [] => {}
        _ => return Answer::Conflict,
    }
    if let Some(page) = said(Source::Legacy, Holding::Holds).next() {
        return Answer::Redirect(page.clone());
    }
    let realm_down = said(Source::Realm, Holding::Unavailable).next().is_some();
    match shape {
        Shape::Dev(id) => Answer::Redirect(format!("{}{id}", V2_DEV.page)),
        _ if realm_down => Answer::Unavailable,
        Shape::Document(id) => Answer::Redirect(format!("{}{id}", V2.page)),
        _ => Answer::NotFound("no realm holds this identifier"),
    }
}

/// A realm holds an id when its node redirects to a page or reports it withdrawn; a v2 site
/// when it serves the resource or refuses it as not public.
pub fn holding(source: Source, status: StatusCode) -> Holding {
    match (source, status.as_u16()) {
        (Source::Realm, 301 | 302 | 303 | 307 | 308 | 410) => Holding::Holds,
        (Source::Legacy, 200 | 400) => Holding::Holds,
        (_, 404) => Holding::Absent,
        _ => Holding::Unavailable,
    }
}

/// Asks one site; no redirect is followed, so a probe never leaves the asked host.
pub async fn probe(egress: &EgressGuard, url: Url, source: Source) -> Holding {
    let Ok(request) = egress.hop_request(url, &[]) else {
        return Holding::Unavailable;
    };
    let accept = match source {
        Source::Realm => "application/ld+json",
        Source::Legacy => "application/json",
    };
    match request
        .header("accept", accept)
        .timeout(PROBE_TIMEOUT)
        .send()
        .await
    {
        Ok(response) => holding(source, response.status()),
        Err(_) => Holding::Unavailable,
    }
}

/// A canonical 26-character ULID; `from_string` alone would accept an overflowing first digit.
fn parse_ulid(value: &str) -> Option<Ulid> {
    let first = value.bytes().next()?;
    (value.len() == 26 && (b'0'..=b'7').contains(&first))
        .then(|| Ulid::from_string(value).ok())
        .flatten()
}

/// Parses the path after `/aruna/`. A path no rule names, the resolver's own prefixes among
/// them, is unknown, so a redirect never comes back here.
fn parse_path(path: &str) -> Shape {
    if path.is_empty() || VOCABULARIES.iter().any(|prefix| path.starts_with(prefix)) {
        return Shape::Home;
    }
    if let Some(tail) = path.strip_prefix("data/") {
        return match data_path(tail) {
            Some((realm, path)) => Shape::Data(realm, path),
            None => Shape::Unknown,
        };
    }
    if let Some(id) = path.strip_prefix("dev/") {
        return parse_ulid(id).map_or(Shape::Unknown, Shape::Dev);
    }
    if let Some(id) = path.strip_prefix("profile/") {
        return parse_ulid(id).map_or(Shape::Unknown, Shape::Profile);
    }
    parse_ulid(path).map_or(Shape::Unknown, Shape::Document)
}

/// The realm and canonical node path of a data id. w3id.org passes a path decoded once and
/// re-escaped in part, so the tail is decoded once and an ARN key encoded canonically again.
fn data_path(tail: &str) -> Option<(Option<RealmId>, String)> {
    let decoded = percent_decode_str(tail).decode_utf8().ok()?;
    if decoded.starts_with("arn:") {
        let (head, version) = decoded.rsplit_once('@')?;
        let arn = ArunaArn::parse(head).ok()?;
        let (bucket, key) = (arn.bucket()?, arn.key_prefix()?);
        let object =
            VersionedObjectArn::new(arn.realm_id, arn.node_id, bucket, key, parse_ulid(version)?)
                .ok()?;
        return Some((Some(object.realm_id), object.to_string()));
    }
    let identifier = W3idIdentifier::parse(&format!("{ARUNA_DATA_PREFIX}{decoded}")).ok()?;
    let realm = match &identifier {
        W3idIdentifier::ContentHash(_) => None,
        W3idIdentifier::Global(id) => Some(id.realm_id()),
        W3idIdentifier::VersionedObject(_) => return None,
    };
    let canonical = identifier.to_w3id();
    Some((
        realm,
        canonical.strip_prefix(ARUNA_DATA_PREFIX)?.to_string(),
    ))
}

/// A resolver answer: never cached by clients, readable from any origin.
fn respond(answer: Answer) -> Response {
    let (status, location, message) = match answer {
        Answer::Redirect(target) => (StatusCode::FOUND, Some(target), ""),
        Answer::NotFound(message) => (StatusCode::NOT_FOUND, None, message),
        Answer::Conflict => (
            StatusCode::CONFLICT,
            None,
            "more than one realm claims this identifier",
        ),
        Answer::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            None,
            "a realm that may hold this identifier is unreachable, retry later",
        ),
        Answer::Busy => (
            StatusCode::TOO_MANY_REQUESTS,
            None,
            "too many lookups, retry later",
        ),
    };
    let mut response = (status, message).into_response();
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    if let Some(value) = location.and_then(|target| HeaderValue::from_str(&target).ok()) {
        headers.insert(header::LOCATION, value);
    }
    response
}

pub fn router(resolver: Arc<Resolver>) -> Router {
    Router::new()
        .route("/aruna", get(resolve_shared))
        .route("/aruna/", get(resolve_shared))
        .route("/aruna/{*path}", get(resolve_shared))
        .route("/realm/{realm_id}/{*path}", get(resolve_owned))
        .with_state(resolver)
}

/// The shared `https://w3id.org/aruna/` namespace; the raw path keeps ARN keys encoded.
async fn resolve_shared(
    State(resolver): State<Arc<Resolver>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let path = uri.path().strip_prefix("/aruna").unwrap_or_default();
    let shape = parse_path(path.strip_prefix('/').unwrap_or(path));
    let client = client_address(&headers, resolver.client_header.as_ref(), peer.ip());
    let egress = &resolver.registry.egress;
    let answer = resolver
        .answer(shape, unix_timestamp_secs(), client, |url, source| {
            probe(egress, url, source)
        })
        .await;
    respond(answer)
}

/// The client as the ingress names it: the last address of the configured header, which the
/// closest proxy appended. Without a usable value the peer address counts.
fn client_address(headers: &HeaderMap, header: Option<&HeaderName>, peer: IpAddr) -> IpAddr {
    header
        .and_then(|name| headers.get_all(name).iter().next_back())
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.rsplit(',').next())
        .and_then(|address| address.trim().parse().ok())
        .unwrap_or(peer)
}

/// A namespace a realm owns at w3id.org, whose rule names the realm id in the target.
async fn resolve_owned(State(resolver): State<Arc<Resolver>>, uri: Uri) -> Response {
    let rest = uri.path().strip_prefix("/realm/").unwrap_or_default();
    let Some((realm, path)) = rest.split_once('/') else {
        return respond(Answer::NotFound("not an Aruna identifier"));
    };
    let Ok(realm) = RealmId::from_base64(realm) else {
        return respond(Answer::NotFound("path is not a realm id"));
    };
    let node_path = match parse_path(path) {
        Shape::Document(id) => id.to_string(),
        Shape::Profile(id) => format!("profile/{id}"),
        Shape::Data(_, path) => format!("data/{path}"),
        _ => return respond(Answer::NotFound("not an Aruna identifier")),
    };
    respond(resolver.direct(realm, &node_path, unix_timestamp_secs()))
}

#[cfg(test)]
#[path = "resolver_tests.rs"]
mod tests;
