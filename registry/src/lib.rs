//! Lists realms that registered themselves with a signed registration. A listing grants
//! nothing and proves no institutional identity; KPIs are self-reported.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod admin;
pub mod admin_api;
pub mod resolver;
pub mod store;
pub mod verify;

use std::num::NonZeroU32;
use std::sync::Arc;

use aruna_blob::egress::EgressGuard;
use aruna_core::federation::{
    FederationError, Registration, Signed, Withdrawal, valid_federation_url,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::replication::ArunaArn;
use aruna_core::time::unix_timestamp_secs;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::warn;

use crate::admin::{RealmStatus, ResolveGate};
use crate::admin_api::AdminAuth;
use crate::store::{Entry, Store, StoreError, is_stale};
use crate::verify::{RouteProblem, verify_routes};

/// Writes accepted per minute across all clients, and their burst.
const WRITES_PER_MINUTE: u32 = 60;
const WRITE_BURST: u32 = 20;
/// Registrations whose descriptor routes are fetched at the same time.
const VERIFY_SLOTS: usize = 4;
/// The registry route that resolves Aruna data w3ids.
const RESOLVE_PREFIX: &str = "/v1/resolve/";
/// The node route a resolved id goes to, under the realm's API base path.
const NODE_RESOLVE_ROUTE: &str = "resolve";

pub struct RegistryState {
    pub store: Store,
    pub egress: EgressGuard,
    pub admin_auth: AdminAuth,
    writes: DefaultDirectRateLimiter,
    verifications: Semaphore,
}

impl RegistryState {
    pub fn new(store: Store, egress: EgressGuard) -> Self {
        let quota = Quota::per_minute(NonZeroU32::new(WRITES_PER_MINUTE).expect("nonzero rate"))
            .allow_burst(NonZeroU32::new(WRITE_BURST).expect("nonzero burst"));
        Self {
            store,
            egress,
            admin_auth: AdminAuth::Disabled,
            writes: RateLimiter::direct(quota),
            verifications: Semaphore::new(VERIFY_SLOTS),
        }
    }

    pub fn with_admin(mut self, admin_auth: AdminAuth) -> Self {
        self.admin_auth = admin_auth;
        self
    }

    /// Counts one write; runs before any signature check or fetch.
    fn admit_write(&self) -> Result<(), RegistryError> {
        self.writes.check().map_err(|_| RegistryError::RateLimited)
    }
}

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("path is not a realm id")]
    BadRealm,
    #[error("descriptor URLs must be HTTPS, or HTTP to a loopback host")]
    BadUrl,
    #[error("invalid descriptor: {0}")]
    BadDescriptor(FederationError),
    #[error("too many registry writes, retry later")]
    RateLimited,
    #[error(transparent)]
    Signature(#[from] FederationError),
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl IntoResponse for RegistryError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::BadRealm | Self::BadUrl | Self::BadDescriptor(_) => StatusCode::BAD_REQUEST,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::Store(StoreError::NotFound) => StatusCode::NOT_FOUND,
            Self::Signature(_) => StatusCode::FORBIDDEN,
            Self::Store(StoreError::Replayed | StoreError::FutureIssued) => StatusCode::CONFLICT,
            Self::Store(error) => {
                warn!(error = %error, "Registry storage failed");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        (status, self.to_string()).into_response()
    }
}

/// One listed realm. `verified` means both descriptor routes were reachable from the registry
/// and served the registered descriptor.
#[derive(Debug, Serialize)]
pub struct Listing {
    pub realm_id: String,
    pub registration: Signed<Registration>,
    pub received_at: u64,
    pub verified: bool,
    /// Short reasons the descriptor routes did not verify, without addresses or error chains.
    pub problems: Vec<String>,
    /// The KPIs were observed more than 24 hours ago.
    pub stale: bool,
    /// Registry-owned status; `approved` realms are resolved through w3id.org.
    pub status: RealmStatus,
}

impl Listing {
    fn new(entry: Entry, now: u64, status: RealmStatus) -> Self {
        Self {
            realm_id: entry
                .registration
                .payload
                .descriptor
                .payload
                .realm_id
                .to_string(),
            stale: is_stale(&entry, now),
            received_at: entry.received_at,
            verified: entry.verified,
            problems: entry.problems.iter().map(RouteProblem::summary).collect(),
            registration: entry.registration,
            status,
        }
    }
}

pub fn router(state: Arc<RegistryState>) -> Router {
    Router::new()
        .route("/v1/realms", get(list_realms))
        .route(
            "/v1/realms/{realm_id}",
            get(get_realm).put(put_realm).delete(delete_realm),
        )
        .route("/v1/resolve/{*id}", get(resolve_id))
        .merge(admin_api::router())
        .with_state(state)
}

pub(crate) fn parse_realm(value: &str) -> Result<RealmId, RegistryError> {
    RealmId::from_base64(value).map_err(|_| RegistryError::BadRealm)
}

/// Public reads may come from any origin; writes get no CORS headers.
fn public(body: impl Serialize) -> Response {
    let mut response = Json(body).into_response();
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    response
}

/// The registration and its descriptor must both be signed for the path realm, the descriptor
/// URLs must follow the federation URL rule and its facts the realm settings bounds.
fn check_registration(
    realm_id: &RealmId,
    signed: &Signed<Registration>,
) -> Result<(), RegistryError> {
    signed.verify(realm_id)?;
    let descriptor = &signed.payload.descriptor;
    descriptor.verify(realm_id)?;
    if !valid_federation_url(&descriptor.payload.api_url)
        || !valid_federation_url(&descriptor.payload.portal_url)
    {
        return Err(RegistryError::BadUrl);
    }
    descriptor
        .payload
        .check_facts()
        .map_err(RegistryError::BadDescriptor)
}

/// Listed and approved realms, approved first; hidden and retired ones are left out.
async fn list_realms(State(state): State<Arc<RegistryState>>) -> Result<Response, RegistryError> {
    let now = unix_timestamp_secs();
    let mut listings = Vec::new();
    for entry in state.store.entries(now)? {
        let realm_id = entry.registration.payload.descriptor.payload.realm_id;
        let admin = state.store.admin(&realm_id)?;
        if admin.listed() {
            listings.push(Listing::new(entry, now, admin.status));
        }
    }
    listings.sort_by_key(|listing| listing.status != RealmStatus::Approved);
    Ok(public(listings))
}

async fn get_realm(
    State(state): State<Arc<RegistryState>>,
    Path(realm_id): Path<String>,
) -> Result<Response, RegistryError> {
    let now = unix_timestamp_secs();
    let realm_id = parse_realm(&realm_id)?;
    match state.store.entry(&realm_id, now)? {
        Some(entry) => {
            let status = state.store.admin(&realm_id)?.status;
            Ok(public(Listing::new(entry, now, status)))
        }
        None => Ok(StatusCode::NOT_FOUND.into_response()),
    }
}

/// The realm and the re-serialized node route path of a w3id data tail: a global id (version,
/// bucket or prefix form) or a node ARN. Anything else is refused.
fn parse_tail(tail: &str) -> Option<(RealmId, String)> {
    if tail.starts_with("arn:") {
        let arn = ArunaArn::parse(tail).ok()?;
        return Some((arn.realm_id, arn.to_string()));
    }
    let id = GlobalId::parse(&format!("/{tail}")).ok()?;
    let path = id.to_string();
    Some((id.realm_id(), path.trim_start_matches('/').to_string()))
}

/// The node `route` for `path` under the realm's API base path; only an `https` API URL without
/// user info, query or fragment qualifies, and the target never carries one either.
pub(crate) fn resolve_target(api_url: &url::Url, route: &str, path: &str) -> Option<String> {
    let usable = api_url.scheme() == "https"
        && api_url.host().is_some()
        && api_url.username().is_empty()
        && api_url.password().is_none()
        && api_url.query().is_none()
        && api_url.fragment().is_none();
    if !usable {
        return None;
    }
    let base = api_url.as_str().trim_end_matches('/');
    let target = format!("{base}/{route}/{path}");
    let parsed = url::Url::parse(&target).ok()?;
    (parsed.query().is_none() && parsed.fragment().is_none()).then_some(target)
}

/// A resolve answer; never cached, so withdrawals and endpoint changes apply at once.
fn uncached(status: StatusCode, location: Option<&str>, message: impl Into<String>) -> Response {
    let mut response = (status, message.into()).into_response();
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Some(value) = location.and_then(|location| HeaderValue::from_str(location).ok()) {
        headers.insert(header::LOCATION, value);
    }
    response
}

fn storage_unavailable(error: StoreError) -> Response {
    warn!(error = %error, "Registry storage failed");
    uncached(
        StatusCode::SERVICE_UNAVAILABLE,
        None,
        "registry unavailable",
    )
}

/// Why a retired realm's ids answer 410, naming the realm by its last known name.
pub(crate) fn retired_message(store: &Store, realm_id: &RealmId) -> String {
    match store.names(realm_id).unwrap_or_default().last() {
        Some(record) => format!("realm {} is retired", record.name),
        None => "realm is retired".to_string(),
    }
}

/// Sends a w3id data id to its realm: 307 to the realm API's resolve route for a leased,
/// approved realm, 410 for a retired realm, 404 otherwise.
async fn resolve_id(State(state): State<Arc<RegistryState>>, uri: Uri) -> Response {
    let tail = uri.path().strip_prefix(RESOLVE_PREFIX).unwrap_or_default();
    let Some((realm_id, path)) = parse_tail(tail) else {
        return uncached(StatusCode::NOT_FOUND, None, "not an Aruna data id");
    };
    match state.store.may_resolve(&realm_id) {
        Ok(ResolveGate::Allowed) => {}
        Ok(ResolveGate::Retired) => {
            let message = retired_message(&state.store, &realm_id);
            return uncached(StatusCode::GONE, None, message);
        }
        Ok(ResolveGate::Refused) => {
            return uncached(StatusCode::NOT_FOUND, None, "realm not approved");
        }
        Err(error) => return storage_unavailable(error),
    }
    let entry = match state.store.entry(&realm_id, unix_timestamp_secs()) {
        Ok(Some(entry)) => entry,
        Ok(None) => return uncached(StatusCode::NOT_FOUND, None, "realm not found"),
        Err(error) => return storage_unavailable(error),
    };
    let api_url = &entry.registration.payload.descriptor.payload.api_url;
    match resolve_target(api_url, NODE_RESOLVE_ROUTE, &path) {
        Some(target) => uncached(StatusCode::TEMPORARY_REDIRECT, Some(&target), ""),
        None => uncached(StatusCode::NOT_FOUND, None, "realm not publicly reachable"),
    }
}

async fn put_realm(
    State(state): State<Arc<RegistryState>>,
    Path(realm_id): Path<String>,
    Json(signed): Json<Signed<Registration>>,
) -> Result<Json<Listing>, RegistryError> {
    state.admit_write()?;
    let realm_id = parse_realm(&realm_id)?;
    check_registration(&realm_id, &signed)?;
    let now = unix_timestamp_secs();
    // Checked before the fetches too, so a replay costs no outbound requests.
    state
        .store
        .check_issued(&realm_id, signed.payload.issued_at, now)?;
    let problems = {
        let _slot = state
            .verifications
            .acquire()
            .await
            .map_err(|_| RegistryError::Store(StoreError::Poisoned))?;
        verify_routes(&state.egress, &signed.payload.descriptor).await
    };
    let entry = Entry {
        registration: signed,
        received_at: now,
        verified: problems.is_empty(),
        problems,
    };
    state.store.register(&realm_id, &entry, now)?;
    let status = state.store.admin(&realm_id)?.status;
    Ok(Json(Listing::new(entry, now, status)))
}

async fn delete_realm(
    State(state): State<Arc<RegistryState>>,
    Path(realm_id): Path<String>,
    Json(signed): Json<Signed<Withdrawal>>,
) -> Result<StatusCode, RegistryError> {
    state.admit_write()?;
    let realm_id = parse_realm(&realm_id)?;
    signed.verify(&realm_id)?;
    let now = unix_timestamp_secs();
    state
        .store
        .withdraw(&realm_id, signed.payload.issued_at, now)?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::AdminState;
    use crate::store::tests::{capabilities, realm_id, registration};
    use aruna_core::structs::identity::auth::NodeCapabilities;
    use ed25519_dalek::SigningKey;

    #[tokio::test]
    async fn writes_rate_limited() {
        // The limit applies before the signature check, so forged writes use it up too.
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(RegistryState::new(
            Store::open(dir.path()).unwrap(),
            EgressGuard::new(aruna_core::egress::EgressPolicy::strict()).unwrap(),
        ));
        let other = NodeCapabilities::management_node(SigningKey::from_bytes(&[8; 32])).unwrap();
        let forged = Signed::sign(
            Withdrawal {
                realm_id: realm_id(),
                issued_at: 10,
            },
            &other,
        )
        .unwrap();
        let path = realm_id().to_string();
        for _ in 0..WRITE_BURST {
            let result = delete_realm(
                State(state.clone()),
                Path(path.clone()),
                Json(forged.clone()),
            )
            .await;
            assert!(matches!(result, Err(RegistryError::Signature(_))));
        }
        let result = delete_realm(State(state), Path(path), Json(forged)).await;
        assert!(matches!(result, Err(RegistryError::RateLimited)));
    }

    #[test]
    fn accepts_old_registration() {
        // An older node signs its own layout, without contact and login providers.
        use aruna_core::federation::{DESCRIPTOR_DOMAIN, REGISTRATION_DOMAIN, RealmKpis, Signer};
        use ed25519_dalek::Signer as _;
        let key = SigningKey::from_bytes(&[7; 32]);
        let sign = |domain: &str, payload: &[u8]| {
            let mut bytes = postcard::to_allocvec(&domain).unwrap();
            bytes.extend_from_slice(payload);
            key.sign(&bytes).to_string()
        };
        let (api, portal) = (
            "https://realm.example.org/api/v1",
            "https://realm.example.org/",
        );
        let fields = (realm_id(), "Realm", "", api, portal, 1_u64);
        let signature = sign(DESCRIPTOR_DOMAIN, &postcard::to_allocvec(&fields).unwrap());
        let kpis = RealmKpis::default();
        let registration = (fields, Signer::Realm, &signature, kpis, 10_u64, 10_u64);
        let outer = sign(
            REGISTRATION_DOMAIN,
            &postcard::to_allocvec(&registration).unwrap(),
        );
        let wire = serde_json::json!({
            "payload": {
                "descriptor": {
                    "payload": {
                        "realm_id": realm_id(),
                        "name": "Realm",
                        "description": "",
                        "api_url": api,
                        "portal_url": portal,
                        "issued_at": 1,
                    },
                    "signer": "Realm",
                    "signature": signature,
                },
                "kpis": kpis,
                "observed_at": 10,
                "issued_at": 10,
            },
            "signer": "Realm",
            "signature": outer,
        });
        let signed: Signed<Registration> = serde_json::from_value(wire).unwrap();
        check_registration(&realm_id(), &signed).unwrap();
    }

    #[test]
    fn accepts_signed_registration() {
        let signed = registration("https://realm.example.org", 10);
        assert!(check_registration(&realm_id(), &signed).is_ok());
    }

    #[test]
    fn rejects_invalid_urls() {
        // Checked before any fetch or write; a private HTTPS URL still passes admission.
        for base in ["http://realm.example.org", "ftp://realm.example.org"] {
            let signed = registration(base, 10);
            assert!(matches!(
                check_registration(&realm_id(), &signed),
                Err(RegistryError::BadUrl)
            ));
        }
        let private = registration("https://10.0.0.5", 10);
        assert!(check_registration(&realm_id(), &private).is_ok());
    }

    #[test]
    fn rejects_oversized_facts() {
        // Signed descriptors still meet the settings bounds before any fetch or history write.
        let resign = |edit: &dyn Fn(&mut aruna_core::federation::RealmDescriptor)| {
            let mut payload = registration("https://realm.example.org", 10).payload;
            let mut descriptor = payload.descriptor.payload.clone();
            edit(&mut descriptor);
            payload.descriptor = Signed::sign(descriptor, &capabilities()).unwrap();
            Signed::sign(payload, &capabilities()).unwrap()
        };
        let long = aruna_core::federation::MAX_NAME_LEN + 1;
        let edits: [&dyn Fn(&mut aruna_core::federation::RealmDescriptor); 4] = [
            &|d| d.name = "a".repeat(long),
            &|d| d.name = " ".to_string(),
            &|d| d.description = "a".repeat(aruna_core::federation::MAX_DESCRIPTION_LEN + 1),
            &|d| d.contact = Some("not an address".to_string()),
        ];
        for edit in edits {
            assert!(matches!(
                check_registration(&realm_id(), &resign(edit)),
                Err(RegistryError::BadDescriptor(_))
            ));
        }
        let fits = resign(&|d| d.name = "a".repeat(long - 1));
        assert!(check_registration(&realm_id(), &fits).is_ok());
    }

    #[test]
    fn rejects_other_realm() {
        let signed = registration("https://realm.example.org", 10);
        let other = RealmId::from_bytes([3; 32]);
        assert!(matches!(
            check_registration(&other, &signed),
            Err(RegistryError::Signature(FederationError::RealmMismatch))
        ));
    }

    #[test]
    fn rejects_forged_descriptor() {
        // A realm-signed registration cannot carry a descriptor signed by another key.
        let mut payload = registration("https://realm.example.org", 10).payload;
        let other = NodeCapabilities::management_node(SigningKey::from_bytes(&[8; 32])).unwrap();
        payload.descriptor = Signed::sign(payload.descriptor.payload, &other).unwrap();
        let signed = Signed::sign(payload, &capabilities()).unwrap();
        assert!(matches!(
            check_registration(&realm_id(), &signed),
            Err(RegistryError::Signature(FederationError::BadSignature))
        ));
    }

    #[test]
    fn parses_resolve_tails() {
        let realm = realm_id();
        let version = format!("{realm}/01JMETADATA0123456789ABCDE");
        assert_eq!(parse_tail(&version), Some((realm, version.clone())));
        // Bucket and prefix forms keep their trailing segment.
        let bucket = format!("{realm}/01JMETADATA0123456789ABCDE/");
        assert_eq!(parse_tail(&bucket), Some((realm, bucket.clone())));
        let key = SigningKey::from_bytes(&[4; 32]).verifying_key();
        let node = aruna_core::NodeId::from_bytes(key.as_bytes()).unwrap();
        let arn = format!("arn:aruna:{realm}:{node}:s3/reads/a%20b@01JMETADATA0123456789ABCDE");
        assert_eq!(parse_tail(&arn), Some((realm, arn.clone())));
        for bad in [
            "",
            "abc",
            "../x",
            &format!("{realm}/not-a-ulid"),
            "https://evil.example",
        ] {
            assert_eq!(parse_tail(bad), None, "{bad}");
        }
    }

    #[test]
    fn https_targets_only() {
        let url = |value: &str| url::Url::parse(value).unwrap();
        assert_eq!(
            resolve_target(
                &url("https://realm.example.org/api/v1"),
                NODE_RESOLVE_ROUTE,
                "r/v"
            ),
            Some("https://realm.example.org/api/v1/resolve/r/v".to_string())
        );
        assert_eq!(
            resolve_target(
                &url("https://realm.example.org/api/v1/"),
                NODE_RESOLVE_ROUTE,
                "r/v"
            ),
            Some("https://realm.example.org/api/v1/resolve/r/v".to_string())
        );
        for refused in [
            "http://realm.example.org/api/v1",
            "https://user@realm.example.org/api/v1",
            "https://realm.example.org/api/v1?x=1",
            "https://realm.example.org/api/v1#top",
        ] {
            assert_eq!(
                resolve_target(&url(refused), NODE_RESOLVE_ROUTE, "r/v"),
                None,
                "{refused}"
            );
        }
    }

    #[tokio::test]
    async fn resolves_approved_realms() {
        // Only approval opens the resolver; verification is shown, never checked here.
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(RegistryState::new(
            Store::open(dir.path()).unwrap(),
            EgressGuard::new(aruna_core::egress::EgressPolicy::strict()).unwrap(),
        ));
        let now = unix_timestamp_secs();
        let tail = format!("{}/01JMETADATA0123456789ABCDE", realm_id());
        let uri: Uri = format!("{RESOLVE_PREFIX}{tail}").parse().unwrap();
        let missing = resolve_id(State(state.clone()), uri.clone()).await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert_eq!(missing.headers()[header::CACHE_CONTROL], "no-store");
        let entry = Entry {
            registration: registration("https://realm.example.org", now),
            received_at: now,
            verified: false,
            problems: Vec::new(),
        };
        state.store.register(&realm_id(), &entry, now).unwrap();
        let set = |status| {
            let next = AdminState {
                status,
                ..AdminState::default()
            };
            state
                .store
                .update_admin(&realm_id(), next, "test", now)
                .unwrap();
        };
        for status in [RealmStatus::Listed, RealmStatus::Hidden] {
            set(status);
            let refused = resolve_id(State(state.clone()), uri.clone()).await;
            assert_eq!(refused.status(), StatusCode::NOT_FOUND, "{status:?}");
        }
        set(RealmStatus::Approved);
        let found = resolve_id(State(state.clone()), uri.clone()).await;
        assert_eq!(found.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(found.headers()[header::CACHE_CONTROL], "no-store");
        let location = found.headers()[header::LOCATION]
            .to_str()
            .unwrap()
            .to_string();
        assert!(
            location.ends_with(&format!("/resolve/{tail}")),
            "{location}"
        );
        assert!(
            location.starts_with("https://realm.example.org"),
            "{location}"
        );
        // A retired realm answers 410 with its name, also after it withdrew.
        set(RealmStatus::Retired);
        state.store.withdraw(&realm_id(), now + 1, now + 1).unwrap();
        let gone = resolve_id(State(state), uri).await;
        assert_eq!(gone.status(), StatusCode::GONE);
        let text = axum::body::to_bytes(gone.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&text[..], b"realm Realm is retired");
    }

    async fn body(response: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX);
        serde_json::from_slice(&bytes.await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn listing_marks_stale() {
        // Both read routes mark KPIs observed 24 hours ago as stale and keep unknown ones null.
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(RegistryState::new(
            Store::open(dir.path()).unwrap(),
            EgressGuard::new(aruna_core::egress::EgressPolicy::strict()).unwrap(),
        ));
        let now = unix_timestamp_secs();
        let entry = |observed_at| Entry {
            registration: registration("https://realm.example.org", observed_at),
            received_at: now,
            verified: false,
            problems: Vec::new(),
        };
        let observed = now - crate::store::STALE_SECS;
        state
            .store
            .register(&realm_id(), &entry(observed), now)
            .unwrap();
        let listed = body(list_realms(State(state.clone())).await.unwrap()).await;
        assert_eq!(listed[0]["stale"], true);
        let kpis = &listed[0]["registration"]["payload"]["kpis"];
        assert_eq!(kpis["groups"], serde_json::Value::Null);
        assert_eq!(kpis["live_datasets"], 3);
        let path = Path(realm_id().to_string());
        let one = body(get_realm(State(state.clone()), path).await.unwrap()).await;
        assert_eq!(one["stale"], true);
        // A renewal with fresh KPIs clears the mark.
        state.store.register(&realm_id(), &entry(now), now).unwrap();
        let listed = body(list_realms(State(state)).await.unwrap()).await;
        assert_eq!(listed[0]["stale"], false);
    }
}
