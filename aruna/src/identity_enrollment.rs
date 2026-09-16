//! Enrollment planning and HTTP transport for the persisted identity:
//! [`plan_enrollment`] decides whether a boot mints, bootstraps, refreshes, or
//! reuses an identity, and the clients run only when the plan asks for them.

use std::collections::BTreeMap;
use std::time::Duration;

use aruna_core::document::DocumentTarget;
use aruna_core::keys::generate_signing_key;
use aruna_core::onboarding::{
    BootstrapOnboardingRequest, BootstrapOnboardingResponse, OnboardingMode, OnboardingPhase,
    OnboardingSecret, OnboardingTicket, issuer_proof_message, node_proof_message,
};
use aruna_core::structs::identity::realm::{RealmId, StaticRealmEndpoint};
use aruna_core::time::unix_timestamp_secs;
use aruna_net::parse_endpoint_config;
use base64::Engine;
use crypto_box::{
    PublicKey as TransportPublicKey, SalsaBox, SecretKey as TransportSecretKey,
    aead::{Aead, OsRng as CryptoOsRng},
};
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::pkcs8::{DecodePrivateKey, EncodePrivateKey};
use ed25519_dalek::{Signer, SigningKey};
use iroh::EndpointAddr;

use super::IdentityError;
use super::{BootOrigin, PersistedNodeIdentity, PersistedNodeState, PersistedNodeStatus};

/// What this boot must do with the persisted identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnrollmentPlan {
    /// No identity exists and no secret is configured: mint a realm node.
    Generate,
    /// No identity exists and a secret is configured: enroll with the seed.
    Bootstrap,
    /// A pending bootstrapped enrollment must refresh its bootstrap material.
    Refresh,
    /// The stored identity is usable as-is.
    Ready,
}

/// The pure enrollment decision, testable without storage or HTTP.
pub fn plan_enrollment(
    state: Option<&PersistedNodeState>,
    has_onboarding_secret: bool,
) -> Result<EnrollmentPlan, IdentityError> {
    match state {
        None if has_onboarding_secret => Ok(EnrollmentPlan::Bootstrap),
        None => Ok(EnrollmentPlan::Generate),
        Some(state) if matches!(state.status, PersistedNodeStatus::PendingOnboarding) => {
            let phase = state
                .onboarding_phase
                .unwrap_or(OnboardingPhase::Bootstrapped);
            if matches!(phase, OnboardingPhase::Bootstrapped) {
                if has_onboarding_secret {
                    Ok(EnrollmentPlan::Refresh)
                } else {
                    Err(IdentityError::OnboardingBootstrapFailed(
                        "pending bootstrapped onboarding requires ONBOARDING_SECRET to refresh bootstrap material"
                            .to_string(),
                    ))
                }
            } else {
                Ok(EnrollmentPlan::Ready)
            }
        }
        Some(_) => Ok(EnrollmentPlan::Ready),
    }
}

const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct BootstrappedNodeState {
    pub(crate) node_state: PersistedNodeState,
    pub(crate) temporary_bootstrap_endpoint: EndpointAddr,
    pub(crate) realm_endpoints: Vec<EndpointAddr>,
}

/// Realm endpoints handed over at enrollment, so a joiner dials the realm
/// without a discovery read of its own. An entry that does not parse, or whose
/// address names another node, is dropped instead of failing the join.
pub(crate) fn onboarding_realm_endpoints(endpoints: &[StaticRealmEndpoint]) -> Vec<EndpointAddr> {
    endpoints
        .iter()
        .filter_map(
            |endpoint| match parse_endpoint_config(&endpoint.endpoint_addr) {
                Ok(endpoint_addr) if endpoint_addr.id.to_string() == endpoint.node_id => {
                    Some(endpoint_addr)
                }
                Ok(_) => {
                    tracing::warn!(
                        node_id = %endpoint.node_id,
                        "Enrollment endpoint address names a different node; ignoring it"
                    );
                    None
                }
                Err(message) => {
                    tracing::warn!(
                        node_id = %endpoint.node_id,
                        %message,
                        "Enrollment handed over an unusable realm endpoint; ignoring it"
                    );
                    None
                }
            },
        )
        .collect()
}

/// The decode half of the enrollment client, separated so response mapping is
/// testable without an HTTP server.
fn decode_bootstrap_response(body: &[u8]) -> Result<BootstrapOnboardingResponse, IdentityError> {
    serde_json::from_slice(body).map_err(|error| {
        IdentityError::OnboardingBootstrapFailed(format!("invalid bootstrap response: {error}"))
    })
}

pub(crate) async fn bootstrap_node_state(
    onboarding_secret: &str,
    node_location: Option<String>,
    node_weight: Option<u32>,
    node_labels: BTreeMap<String, String>,
    timeout: Duration,
) -> Result<BootstrappedNodeState, IdentityError> {
    let decoded_secret = OnboardingSecret::decode(onboarding_secret)?;
    let node_signing_key = generate_signing_key();
    let net_secret_key = node_signing_key.to_bytes();
    let node_id = iroh::SecretKey::from_bytes(&net_secret_key).public();

    let issuer_signing_key = if matches!(decoded_secret.mode, OnboardingMode::Server) {
        Some(generate_signing_key())
    } else {
        None
    };
    let issuer_public_key = issuer_signing_key.as_ref().map(|issuer_signing_key| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(issuer_signing_key.verifying_key().to_bytes())
    });
    let transport_secret_key = if matches!(decoded_secret.mode, OnboardingMode::Management) {
        Some(TransportSecretKey::generate(&mut CryptoOsRng))
    } else {
        None
    };
    let transport_public_key = transport_secret_key.as_ref().map(|transport_secret_key| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(transport_secret_key.public_key().as_bytes())
    });
    let node_id_string = node_id.to_string();
    let node_proof = node_signing_key
        .sign(&node_proof_message(
            onboarding_secret,
            &node_id_string,
            transport_public_key.as_deref(),
        ))
        .to_string();
    let issuer_proof = issuer_signing_key
        .as_ref()
        .zip(issuer_public_key.as_ref())
        .map(|(issuer_signing_key, issuer_public_key)| {
            issuer_signing_key
                .sign(&issuer_proof_message(
                    onboarding_secret,
                    &node_id_string,
                    issuer_public_key,
                ))
                .to_string()
        });

    let response = onboarding_bootstrap_client(timeout)?
        .post(format!(
            "{}/api/v1/access/onboarding/bootstrap",
            decoded_secret.seed_url.trim_end_matches('/'),
        ))
        .json(&BootstrapOnboardingRequest {
            onboarding_secret: onboarding_secret.to_string(),
            node_id: node_id_string,
            node_proof,
            transport_public_key,
            issuer_public_key,
            issuer_proof,
            node_location,
            node_weight,
            node_labels,
        })
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(IdentityError::OnboardingBootstrapFailed(format!(
            "bootstrap endpoint returned {}",
            response.status()
        )));
    }

    let body = response.bytes().await?;
    let response = decode_bootstrap_response(&body)?;
    if response.mode != decoded_secret.mode {
        return Err(IdentityError::OnboardingModeMismatch);
    }
    let realm_id = response.realm_id()?;
    validate_bootstrap_response(&response, decoded_secret.mode, realm_id, node_id)?;
    let temporary_bootstrap_endpoint = response.temporary_bootstrap_endpoint.clone();
    let realm_endpoints = onboarding_realm_endpoints(&response.realm_endpoints);
    let identity =
        match response.mode {
            OnboardingMode::Management => {
                let wrapped_key =
                    response
                        .wrapped_realm_key
                        .ok_or(IdentityError::MissingOnboardingMaterial(
                            OnboardingMode::Management,
                        ))?;
                let wrapped_nonce =
                    response
                        .wrapped_key_nonce
                        .ok_or(IdentityError::MissingOnboardingMaterial(
                            OnboardingMode::Management,
                        ))?;
                let wrapping_public_key = response.wrapping_public_key.ok_or(
                    IdentityError::MissingOnboardingMaterial(OnboardingMode::Management),
                )?;
                let transport_secret_key = transport_secret_key.ok_or(
                    IdentityError::MissingOnboardingMaterial(OnboardingMode::Management),
                )?;
                let wrapping_key_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(wrapping_public_key)
                    .map_err(IdentityError::Base64Error)?;
                let wrapping_public_key = TransportPublicKey::from(
                    <[u8; 32]>::try_from(wrapping_key_bytes.as_slice())
                        .map_err(IdentityError::FromSliceError)?,
                );
                let cipher = SalsaBox::new(&wrapping_public_key, &transport_secret_key);
                let nonce_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(wrapped_nonce)
                    .map_err(IdentityError::Base64Error)?;
                let nonce = crypto_box::Nonce::from(
                    <[u8; 24]>::try_from(nonce_bytes.as_slice())
                        .map_err(IdentityError::FromSliceError)?,
                );
                let ciphertext = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(wrapped_key)
                    .map_err(IdentityError::Base64Error)?;
                let realm_private_pem =
                    String::from_utf8(cipher.decrypt(&nonce, ciphertext.as_ref()).map_err(
                        |error| IdentityError::OnboardingBootstrapFailed(error.to_string()),
                    )?)?;

                PersistedNodeIdentity::Management { realm_private_pem }
            }
            OnboardingMode::Server => PersistedNodeIdentity::Server {
                private_key_pem: issuer_signing_key
                    .ok_or(IdentityError::MissingOnboardingMaterial(
                        OnboardingMode::Server,
                    ))?
                    .to_pkcs8_pem(LineEnding::default())?
                    .to_string(),
                delegation_signature: response.delegation_signature.ok_or(
                    IdentityError::MissingOnboardingMaterial(OnboardingMode::Server),
                )?,
            },
            // Devices carry no issuer keys. Their finalized membership grants owner authority,
            // and the saved owner identifies that authority before the record arrives.
            OnboardingMode::User { owner } => PersistedNodeIdentity::User { owner },
        };

    Ok(BootstrappedNodeState {
        temporary_bootstrap_endpoint,
        realm_endpoints,
        node_state: PersistedNodeState {
            boot_origin: BootOrigin::Onboarded,
            status: PersistedNodeStatus::PendingOnboarding,
            realm_id,
            net_secret_key,
            onboarding_phase: Some(OnboardingPhase::Bootstrapped),
            onboarding_sync_ticket: Some(response.onboarding_sync_ticket),
            identity,
        },
    })
}

pub(crate) async fn refresh_onboarding_bootstrap(
    onboarding_secret: &str,
    node_state: &PersistedNodeState,
    node_location: Option<String>,
    node_weight: Option<u32>,
    node_labels: BTreeMap<String, String>,
    timeout: Duration,
) -> Result<BootstrapOnboardingResponse, IdentityError> {
    let decoded_secret = OnboardingSecret::decode(onboarding_secret)?;
    let node_signing_key = SigningKey::from_bytes(&node_state.net_secret_key);
    let node_id = iroh::SecretKey::from_bytes(&node_state.net_secret_key).public();

    let mut transport_secret_key = None;
    let transport_public_key = if matches!(decoded_secret.mode, OnboardingMode::Management) {
        let secret_key = TransportSecretKey::generate(&mut CryptoOsRng);
        let public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(secret_key.public_key().as_bytes());
        transport_secret_key = Some(secret_key);
        Some(public_key)
    } else {
        None
    };

    let issuer_signing_key = match (&decoded_secret.mode, &node_state.identity) {
        (
            OnboardingMode::Server,
            PersistedNodeIdentity::Server {
                private_key_pem, ..
            },
        ) => Some(SigningKey::from_pkcs8_pem(private_key_pem)?),
        (OnboardingMode::Server, _) => {
            return Err(IdentityError::MissingOnboardingMaterial(
                OnboardingMode::Server,
            ));
        }
        _ => None,
    };
    let issuer_public_key = issuer_signing_key.as_ref().map(|issuer_signing_key| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(issuer_signing_key.verifying_key().to_bytes())
    });
    let node_id_string = node_id.to_string();
    let node_proof = node_signing_key
        .sign(&node_proof_message(
            onboarding_secret,
            &node_id_string,
            transport_public_key.as_deref(),
        ))
        .to_string();
    let issuer_proof = issuer_signing_key
        .as_ref()
        .zip(issuer_public_key.as_ref())
        .map(|(issuer_signing_key, issuer_public_key)| {
            issuer_signing_key
                .sign(&issuer_proof_message(
                    onboarding_secret,
                    &node_id_string,
                    issuer_public_key,
                ))
                .to_string()
        });

    let response = onboarding_bootstrap_client(timeout)?
        .post(format!(
            "{}/api/v1/access/onboarding/bootstrap",
            decoded_secret.seed_url.trim_end_matches('/'),
        ))
        .json(&BootstrapOnboardingRequest {
            onboarding_secret: onboarding_secret.to_string(),
            node_id: node_id_string,
            node_proof,
            transport_public_key,
            issuer_public_key,
            issuer_proof,
            node_location,
            node_weight,
            node_labels,
        })
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(IdentityError::OnboardingBootstrapFailed(format!(
            "bootstrap endpoint returned {}",
            response.status()
        )));
    }

    let body = response.bytes().await?;
    let response = decode_bootstrap_response(&body)?;
    if response.mode != decoded_secret.mode {
        return Err(IdentityError::OnboardingModeMismatch);
    }
    let response_realm_id = response.realm_id()?;
    if response_realm_id != node_state.realm_id {
        return Err(IdentityError::OnboardingBootstrapFailed(
            "bootstrap response realm does not match persisted node state".to_string(),
        ));
    }
    validate_bootstrap_response(&response, decoded_secret.mode, node_state.realm_id, node_id)?;

    drop(transport_secret_key);
    Ok(response)
}

fn onboarding_bootstrap_client(timeout: Duration) -> Result<reqwest::Client, IdentityError> {
    Ok(reqwest::Client::builder()
        .connect_timeout(HTTP_CONNECT_TIMEOUT)
        .timeout(timeout)
        .build()?)
}

fn validate_bootstrap_response(
    response: &BootstrapOnboardingResponse,
    expected_mode: OnboardingMode,
    expected_realm_id: RealmId,
    expected_node_id: iroh::PublicKey,
) -> Result<(), IdentityError> {
    if response.mode != expected_mode {
        return Err(IdentityError::OnboardingModeMismatch);
    }
    if response.realm_id()? != expected_realm_id {
        return Err(IdentityError::OnboardingBootstrapFailed(
            "bootstrap response realm does not match expected realm".to_string(),
        ));
    }

    let ticket = OnboardingTicket::decode(&response.onboarding_sync_ticket)?;
    if ticket.payload.realm_id != expected_realm_id.to_string() {
        return Err(IdentityError::OnboardingBootstrapFailed(
            "onboarding sync ticket realm does not match bootstrap response".to_string(),
        ));
    }
    if ticket.payload.node_id != expected_node_id.to_string() {
        return Err(IdentityError::OnboardingBootstrapFailed(
            "onboarding sync ticket node does not match local node".to_string(),
        ));
    }
    ticket.verify(
        expected_node_id,
        &DocumentTarget::RealmConfig {
            realm_id: expected_realm_id,
        },
        unix_timestamp_secs(),
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::keys::generate_signing_key;

    fn state(status: PersistedNodeStatus, phase: Option<OnboardingPhase>) -> PersistedNodeState {
        let realm_signing_key = generate_signing_key();
        PersistedNodeState {
            boot_origin: BootOrigin::Onboarded,
            status,
            realm_id: RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes()),
            net_secret_key: generate_signing_key().to_bytes(),
            onboarding_phase: phase,
            onboarding_sync_ticket: None,
            identity: PersistedNodeIdentity::User {
                owner: UserId::nil(RealmId::from_bytes([9u8; 32])),
            },
        }
    }

    #[test]
    fn rejects_bad_bootstrap() {
        assert!(matches!(
            decode_bootstrap_response(b"not json"),
            Err(IdentityError::OnboardingBootstrapFailed(_))
        ));
    }

    #[test]
    fn enrollment_plan_table() {
        // First boot: a secret decides bootstrap versus local generation.
        assert_eq!(
            plan_enrollment(None, false).unwrap(),
            EnrollmentPlan::Generate
        );
        assert_eq!(
            plan_enrollment(None, true).unwrap(),
            EnrollmentPlan::Bootstrap
        );

        // Pending bootstrapped enrollment refreshes with a secret and fails
        // without one.
        let pending = state(
            PersistedNodeStatus::PendingOnboarding,
            Some(OnboardingPhase::Bootstrapped),
        );
        assert_eq!(
            plan_enrollment(Some(&pending), true).unwrap(),
            EnrollmentPlan::Refresh
        );
        assert!(matches!(
            plan_enrollment(Some(&pending), false),
            Err(IdentityError::OnboardingBootstrapFailed(_))
        ));

        // A phase past the bootstrap refresh and a completed node are ready.
        let fetched = state(
            PersistedNodeStatus::PendingOnboarding,
            Some(OnboardingPhase::CoreDocumentsFetched),
        );
        assert_eq!(
            plan_enrollment(Some(&fetched), false).unwrap(),
            EnrollmentPlan::Ready
        );
        let complete = state(PersistedNodeStatus::Complete, None);
        assert_eq!(
            plan_enrollment(Some(&complete), false).unwrap(),
            EnrollmentPlan::Ready
        );
    }
}
