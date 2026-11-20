//! Resolves `https://w3id.org/aruna/` paths to the realm or the legacy v2 site that holds them.
//! It only redirects: to approved realms, the fixed v2 sites and the project home.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::num::{NonZeroU32, NonZeroUsize};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aruna_blob::egress::EgressGuard;
use aruna_clock::unix_timestamp_secs;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::replication::{
    ARUNA_DATA_PREFIX, ArunaArn, VersionedObjectArn, W3idIdentifier,
};
use aruna_core::structured_id::MetaResourceId;
use axum::Router;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures::StreamExt;
use governor::clock::Clock;
use governor::middleware::NoOpMiddleware;
use governor::nanos::Nanos;
use governor::state::keyed::DashMapStateStore;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use ipnet::IpNet;
use lru::LruCache;
use percent_encoding::percent_decode_str;
use tracing::warn;
use ulid::Ulid;
use url::Url;

use crate::admin::ResolveGate;
use crate::store::{Entry, StoreError};
use crate::{RegistryState, resolve_target, retired_message};

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
/// One client's stricter share per minute, charged first, so it cannot use up the budget.
const CLIENT_GUESS_RATE: u32 = 60;
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
    /// A realm node answered a 503 marked `aruna-placement: unknown`: it can place no such id.
    Undecided,
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
    /// The realm that minted the id is retired; the message names it.
    Gone(String),
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

/// The limiters' clock: the latest request time the resolver was given, never going back, so
/// limits follow the same time as the cache and tests choose it.
#[derive(Clone, Debug, Default)]
struct RequestClock(Arc<AtomicU64>);

impl RequestClock {
    fn advance_to(&self, now_secs: u64) {
        let nanos = now_secs.saturating_mul(1_000_000_000);
        self.0.fetch_max(nanos, Ordering::Relaxed);
    }
}

impl Clock for RequestClock {
    type Instant = Nanos;

    fn now(&self) -> Nanos {
        Nanos::from(self.0.load(Ordering::Relaxed))
    }
}

type SharedLimiter = RateLimiter<NotKeyed, InMemoryState, RequestClock, NoOpMiddleware<Nanos>>;
type ClientLimiter =
    RateLimiter<IpAddr, DashMapStateStore<IpAddr>, RequestClock, NoOpMiddleware<Nanos>>;

/// What a realm node is sent: a document, a Profile or a canonical data id.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Item {
    Document(Ulid),
    Profile(Ulid),
    Data(String),
}

impl Item {
    /// The path below the node's w3id route; every node of a current realm serves it.
    fn route(&self) -> String {
        match self {
            Self::Document(id) => id.to_string(),
            Self::Profile(id) => format!("profile/{id}"),
            Self::Data(path) => format!("data/{path}"),
        }
    }
}

/// A kept guess. A winning realm is kept by id and its target rebuilt from the live
/// registration on each use, so withdrawal, lease expiry and URL changes apply at once.
#[derive(Clone)]
enum Kept {
    /// The realm whose node held the id.
    Realm(RealmId),
    Fixed(Answer),
}

pub struct Resolver {
    registry: Arc<RegistryState>,
    /// The realm of ids that name none, such as content hashes; used only while approved.
    default_realm: Option<RealmId>,
    cache: Mutex<LruCache<String, (Kept, u64)>>,
    clock: RequestClock,
    guesses: SharedLimiter,
    clients: ClientLimiter,
    client_source: ClientSource,
}

/// Where the client address comes from: a header the ingress sets, honoured only from the
/// trusted proxy addresses, else the peer address.
#[derive(Clone, Debug, Default)]
pub struct ClientSource {
    pub header: Option<HeaderName>,
    pub trusted_proxies: Vec<IpNet>,
}

impl ClientSource {
    /// The last address of the header, which the closest proxy appended; without a trusted
    /// peer or a usable value the peer address counts.
    fn address(&self, headers: &HeaderMap, peer: IpAddr) -> IpAddr {
        if !self.trusted_proxies.iter().any(|net| net.contains(&peer)) {
            return peer;
        }
        self.header
            .as_ref()
            .and_then(|name| headers.get_all(name).iter().next_back())
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.rsplit(',').next())
            .and_then(|address| address.trim().parse().ok())
            .unwrap_or(peer)
    }
}

/// Comma-separated proxy addresses or ranges, for example `10.0.0.0/8, 192.0.2.7`.
pub fn parse_proxies(value: &str) -> Result<Vec<IpNet>, ipnet::AddrParseError> {
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| match entry.parse::<IpAddr>() {
            Ok(address) => Ok(IpNet::from(address)),
            Err(_) => entry.parse::<IpNet>(),
        })
        .collect()
}

impl Resolver {
    /// Only realms a registry admin approved are asked and redirected to.
    pub fn new(
        registry: Arc<RegistryState>,
        default_realm: Option<RealmId>,
        client_source: ClientSource,
    ) -> Self {
        let clock = RequestClock::default();
        let quota = Quota::per_minute(NonZeroU32::new(GUESSES_PER_MINUTE).expect("nonzero rate"))
            .allow_burst(NonZeroU32::new(GUESS_BURST).expect("nonzero burst"));
        Self {
            registry,
            default_realm,
            cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(CACHE_ENTRIES).expect("nonzero cache"),
            )),
            guesses: RateLimiter::direct_with_clock(quota, clock.clone()),
            clients: RateLimiter::dashmap_with_clock(
                Quota::per_minute(NonZeroU32::new(CLIENT_GUESS_RATE).expect("nonzero rate"))
                    .allow_burst(NonZeroU32::new(CLIENT_BURST).expect("nonzero burst")),
                clock.clone(),
            ),
            clock,
            client_source,
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
            Shape::Data(..) if !self.admit(client, now) => Answer::Busy,
            Shape::Data(realm, path) => match realm.or(self.default_realm) {
                Some(realm) => {
                    let item = Item::Data(path);
                    self.direct(realm, &item, now).await
                }
                None => Answer::NotFound("no realm resolves this identifier"),
            },
            shape => self.guess(shape, now, client, probe).await,
        }
    }

    /// Charges one lookup to the client's share; idle client keys are dropped now and then.
    fn admit(&self, client: IpAddr, now: u64) -> bool {
        self.clock.advance_to(now);
        let admitted = self.clients.check_key(&client).is_ok();
        if self.clients.len() > CLIENT_ENTRIES {
            self.clients.retain_recent();
        }
        admitted
    }

    /// A redirect to an approved realm's node route, without asking it about the id.
    async fn direct(&self, realm: RealmId, item: &Item, now: u64) -> Answer {
        match self.registry.store.may_resolve(&realm) {
            Ok(ResolveGate::Allowed) => {}
            Ok(ResolveGate::Retired) => {
                return Answer::Gone(retired_message(&self.registry.store, &realm));
            }
            Ok(ResolveGate::Refused) => return Answer::NotFound("realm not approved"),
            Err(error) => return storage_failed(error),
        }
        let entry = match self.registry.store.entry(&realm, now) {
            Ok(Some(entry)) => entry,
            Ok(None) => return Answer::NotFound("realm not publicly reachable"),
            Err(error) => return storage_failed(error),
        };
        match entry_target(&entry, item) {
            Some(target) => Answer::Redirect(target),
            None => Answer::NotFound("realm not publicly reachable"),
        }
    }

    /// Asks the approved realms, and for a bare id the v2 sites, which one holds `shape`.
    async fn guess<P, F>(&self, shape: Shape, now: u64, client: IpAddr, probe: P) -> Answer
    where
        P: Fn(Url, Source) -> F,
        F: Future<Output = Holding>,
    {
        let (key, id, item) = match &shape {
            Shape::Document(id) => (id.to_string(), *id, Item::Document(*id)),
            Shape::Dev(id) => (format!("dev/{id}"), *id, Item::Document(*id)),
            Shape::Profile(id) => (format!("profile/{id}"), *id, Item::Profile(*id)),
            _ => return Answer::NotFound("not an Aruna identifier"),
        };
        if let Some(answer) = self.cached(&key, &item, now).await {
            return answer;
        }
        if !self.admit(client, now) || self.guesses.check().is_err() {
            return Answer::Busy;
        }
        // A ULID with the reserved placement handle was never minted by a v3 realm.
        let structured = MetaResourceId::from_str(&id.to_string()).is_ok();
        let realms = match structured {
            true => match self.realm_targets(now).await {
                Ok(targets) => targets,
                Err(error) => return storage_failed(error),
            },
            false => Vec::new(),
        };
        let legacy = match shape {
            Shape::Document(_) => vec![V2, V2_DEV],
            Shape::Dev(_) => vec![V2_DEV],
            _ => Vec::new(),
        };
        let probe = &probe;
        let asks: Vec<_> = realms
            .iter()
            .map(|(realm, entry)| ask_realm(*realm, entry, &item, probe))
            .collect();
        let realm_answers: Vec<(RealmId, String, Holding)> = futures::stream::iter(asks)
            .buffered(PROBE_SLOTS)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flatten()
            .collect();
        let mut asked = Vec::new();
        for site in &legacy {
            if let Ok(url) = Url::parse(&format!("{}{id}", site.api)) {
                asked.push((format!("{}{id}", site.page), url, Source::Legacy));
            }
        }
        let mut answers: Vec<Asked> = realm_answers
            .iter()
            .map(|(_, target, holding)| (target.clone(), Source::Realm, *holding))
            .collect();
        let legacy_answers: Vec<Asked> = futures::stream::iter(asked)
            .map(|(target, url, source)| async move { (target, source, probe(url, source).await) })
            .buffered(PROBE_SLOTS)
            .collect()
            .await;
        answers.extend(legacy_answers);
        let answer = decide(&answers);
        if answer == Answer::Conflict {
            warn!(id = %id, "More than one approved realm claims one identifier");
        }
        let settled = answers
            .iter()
            .all(|(_, _, holding)| matches!(holding, Holding::Holds | Holding::Absent));
        let kept = match &answer {
            Answer::Redirect(target) => realm_answers
                .iter()
                .find(|(_, realm_target, _)| realm_target == target)
                .map_or(Kept::Fixed(answer.clone()), |(realm, _, _)| {
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
    async fn cached(&self, key: &str, item: &Item, now: u64) -> Option<Answer> {
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
            Kept::Realm(realm) => {
                match self.direct(realm, item, now).await {
                    Answer::Redirect(target) => Some(Answer::Redirect(target)),
                    _ => {
                        if let Ok(mut cache) = self.cache.lock() {
                            cache.pop(key);
                        }
                        None
                    }
                }
            }
        }
    }

    /// Each approved realm with a live lease, the default first.
    async fn realm_targets(&self, now: u64) -> Result<Vec<(RealmId, Entry)>, StoreError> {
        // The store reads block, so they run on the blocking pool instead of a request worker.
        let registry = Arc::clone(&self.registry);
        let resolvable = tokio::task::spawn_blocking(move || registry.store.resolvable(now))
            .await
            .map_err(|_| StoreError::Poisoned)??;
        let mut approved: Vec<_> = (resolvable.into_iter())
            .map(|entry| (entry.registration.payload.descriptor.payload.realm_id, entry))
            .collect();
        approved.sort_by_key(|(realm, _)| Some(*realm) != self.default_realm);
        Ok(approved)
    }
}

/// Asks one realm's node about `item` on its w3id route.
async fn ask_realm<P, F>(
    realm: RealmId,
    entry: &Entry,
    item: &Item,
    probe: &P,
) -> Option<(RealmId, String, Holding)>
where
    P: Fn(Url, Source) -> F,
    F: Future<Output = Holding>,
{
    let target = entry_target(entry, item)?;
    let holding = probe(Url::parse(&target).ok()?, Source::Realm).await;
    Some((realm, target, holding))
}

/// The node's w3id route for `item` under the entry's API URL.
fn entry_target(entry: &Entry, item: &Item) -> Option<String> {
    let api_url = &entry.registration.payload.descriptor.payload.api_url;
    // w3id.org would send the redirect back here; refusing it keeps every chain finite.
    if api_url.host_str() == Some("w3id.org") {
        return None;
    }
    resolve_target(api_url, NODE_ROUTE, &item.route())
}

fn storage_failed(error: StoreError) -> Answer {
    warn!(error = %error, "Registry storage failed");
    Answer::Unavailable
}

/// The guess from every answer: one realm holder wins, two conflict; then a v2 site that knows
/// the id; an unreachable realm gives 503, an unreachable v2 site its page, nobody holding 404.
fn decide(answers: &[Asked]) -> Answer {
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
    if said(Source::Realm, Holding::Unavailable).next().is_some() {
        return Answer::Unavailable;
    }
    match said(Source::Legacy, Holding::Unavailable).next() {
        Some(page) => Answer::Redirect(page.clone()),
        None => Answer::NotFound("no realm or v2 site holds this identifier"),
    }
}

/// A realm holds an id when its node redirects to a page or reports it withdrawn; a v2 site
/// when it serves the resource or refuses it as not public. A 503, as during startup or an
/// authority failure, is no absence.
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
        Ok(response) => {
            let unplaced = response.status() == StatusCode::SERVICE_UNAVAILABLE
                && response
                    .headers()
                    .get("aruna-placement")
                    .is_some_and(|value| value == "unknown");
            match source {
                Source::Realm if unplaced => Holding::Undecided,
                _ => holding(source, response.status()),
            }
        }
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
        Answer::Redirect(target) => (StatusCode::FOUND, Some(target), "".to_string()),
        Answer::NotFound(message) => (StatusCode::NOT_FOUND, None, message.to_string()),
        Answer::Gone(message) => (StatusCode::GONE, None, message),
        Answer::Conflict => (
            StatusCode::CONFLICT,
            None,
            "more than one realm claims this identifier".to_string(),
        ),
        Answer::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            None,
            "a realm that may hold this identifier is unreachable, retry later".to_string(),
        ),
        Answer::Busy => (
            StatusCode::TOO_MANY_REQUESTS,
            None,
            "too many lookups, retry later".to_string(),
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
    let client = resolver.client_source.address(&headers, peer.ip());
    let egress = &resolver.registry.egress;
    let answer = resolver
        .answer(shape, unix_timestamp_secs(), client, |url, source| {
            probe(egress, url, source)
        })
        .await;
    respond(answer)
}

/// A namespace a realm owns at w3id.org, whose rule names the realm id in the target.
async fn resolve_owned(
    State(resolver): State<Arc<Resolver>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let rest = uri.path().strip_prefix("/realm/").unwrap_or_default();
    let client = resolver.client_source.address(&headers, peer.ip());
    let answer = resolver.owned(rest, unix_timestamp_secs(), client).await;
    respond(answer)
}

impl Resolver {
    /// `{realm-id}/{path}` of a namespace the realm owns, sent to that realm only.
    async fn owned(&self, rest: &str, now: u64, client: IpAddr) -> Answer {
        if !self.admit(client, now) {
            return Answer::Busy;
        }
        let Some((realm, path)) = rest.split_once('/') else {
            return Answer::NotFound("not an Aruna identifier");
        };
        let Ok(realm) = RealmId::from_base64(realm) else {
            return Answer::NotFound("path is not a realm id");
        };
        let item = match parse_path(path) {
            Shape::Document(id) => Item::Document(id),
            Shape::Profile(id) => Item::Profile(id),
            Shape::Data(_, path) => Item::Data(path),
            _ => return Answer::NotFound("not an Aruna identifier"),
        };
        self.direct(realm, &item, now).await
    }
}

#[cfg(test)]
#[path = "resolver_tests.rs"]
mod tests;
