//! Lists realms that registered themselves with a signed registration. A listing grants
//! nothing and proves no institutional identity; KPIs are self-reported.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod store;
pub mod verify;

use std::num::NonZeroU32;
use std::sync::Arc;

use aruna_blob::egress::EgressGuard;
use aruna_core::federation::{FederationError, Registration, Signed, Withdrawal};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::time::unix_timestamp_secs;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::warn;

use crate::store::{Entry, Store, StoreError, is_stale};
use crate::verify::verify_routes;

/// Writes accepted per minute across all clients, and their burst.
const WRITES_PER_MINUTE: u32 = 60;
const WRITE_BURST: u32 = 20;
/// Registrations whose descriptor routes are fetched at the same time.
const VERIFY_SLOTS: usize = 4;

pub struct RegistryState {
    pub store: Store,
    pub egress: EgressGuard,
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
            writes: RateLimiter::direct(quota),
            verifications: Semaphore::new(VERIFY_SLOTS),
        }
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
            Self::BadRealm => StatusCode::BAD_REQUEST,
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
    /// The KPIs were observed more than 24 hours ago.
    pub stale: bool,
}

impl Listing {
    fn new(entry: Entry, now: u64) -> Self {
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
            registration: entry.registration,
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
        .with_state(state)
}

fn parse_realm(value: &str) -> Result<RealmId, RegistryError> {
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

/// The registration and its descriptor must both be signed for the path realm.
fn check_registration(
    realm_id: &RealmId,
    signed: &Signed<Registration>,
) -> Result<(), RegistryError> {
    signed.verify(realm_id)?;
    signed.payload.descriptor.verify(realm_id)?;
    Ok(())
}

async fn list_realms(State(state): State<Arc<RegistryState>>) -> Result<Response, RegistryError> {
    let now = unix_timestamp_secs();
    let listings: Vec<Listing> = state
        .store
        .entries(now)?
        .into_iter()
        .map(|entry| Listing::new(entry, now))
        .collect();
    Ok(public(listings))
}

async fn get_realm(
    State(state): State<Arc<RegistryState>>,
    Path(realm_id): Path<String>,
) -> Result<Response, RegistryError> {
    let now = unix_timestamp_secs();
    match state.store.entry(&parse_realm(&realm_id)?, now)? {
        Some(entry) => Ok(public(Listing::new(entry, now))),
        None => Ok(StatusCode::NOT_FOUND.into_response()),
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
    let verified = {
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
        verified,
    };
    state.store.register(&realm_id, &entry, now)?;
    Ok(Json(Listing::new(entry, now)))
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
    fn accepts_signed_registration() {
        let signed = registration("https://realm.example.org", 10);
        assert!(check_registration(&realm_id(), &signed).is_ok());
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
}
