//! The signed login handoff a home realm gives its user for a session at another realm.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use ulid::Ulid;

use crate::UserId;
use crate::federation::{
    FederationError, FederationSettings, MAX_NAME_LEN, RealmDescriptor, Signable, Signed,
    valid_federation_url,
};
use crate::structs::identity::realm::RealmId;

pub const HANDOFF_DOMAIN: &str = "aruna-login-handoff-v1";
/// Longest lifetime of a handoff.
pub const MAX_HANDOFF_SECS: u64 = 60;
/// Largest accepted clock skew of the home realm ahead of the serving realm.
pub const HANDOFF_SKEW_SECS: u64 = 30;
/// Lifetime of the federated session a handoff opens; it is not renewable.
pub const FEDERATED_SESSION_SECS: u64 = 8 * 3600;

/// Signed by the home realm `issuer` for its own `user`, for one login at `audience`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginHandoff {
    pub issuer: RealmId,
    pub user: UserId,
    pub audience: RealmId,
    /// Hex SHA-256 of the audience's signed descriptor the handoff was made for.
    pub descriptor_digest: String,
    /// Hex SHA-256 of the secret the audience portal keeps in the browser.
    pub nonce: String,
    pub name: Option<String>,
    pub issued_at: u64,
    pub expires_at: u64,
    /// For audit only; handoffs are not single-use.
    pub handoff_id: Ulid,
}

impl Signable for LoginHandoff {
    const DOMAIN: &'static str = HANDOFF_DOMAIN;
    fn realm_id(&self) -> RealmId {
        self.issuer
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HandoffError {
    #[error(transparent)]
    Signature(#[from] FederationError),
    #[error("the user does not belong to the issuing realm")]
    ForeignUser,
    #[error("the handoff is for another realm")]
    WrongAudience,
    #[error("the handoff names a superseded or unknown descriptor")]
    StaleDescriptor,
    #[error("the browser secret does not match")]
    WrongSecret,
    #[error("the handoff lifetime is invalid or over")]
    BadLifetime,
    #[error("the issuing realm is not accepted here")]
    NotAccepted,
    #[error("the display name is too long")]
    NameTooLong,
}

/// Hex SHA-256 over the postcard encoding of the signed descriptor.
pub fn descriptor_digest(descriptor: &Signed<RealmDescriptor>) -> Result<String, FederationError> {
    let bytes = postcard::to_allocvec(descriptor)
        .map_err(|error| FederationError::Encoding(error.to_string()))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

/// The nonce a browser secret stands for.
pub fn secret_nonce(secret: &[u8]) -> String {
    hex::encode(Sha256::digest(secret))
}

impl LoginHandoff {
    /// A handoff for `user` of `issuer` to the realm of the verified `audience` descriptor.
    pub fn new(
        issuer: RealmId,
        user: UserId,
        audience: &Signed<RealmDescriptor>,
        nonce: String,
        name: Option<String>,
        now: u64,
    ) -> Result<Self, HandoffError> {
        let target = &audience.payload;
        audience.verify(&target.realm_id)?;
        if !valid_federation_url(&target.api_url) || !valid_federation_url(&target.portal_url) {
            return Err(FederationError::InvalidSettings("url must be https").into());
        }
        if user.realm_id != issuer || user.is_nil() {
            return Err(HandoffError::ForeignUser);
        }
        if target.realm_id == issuer {
            return Err(HandoffError::WrongAudience);
        }
        Ok(Self {
            issuer,
            user,
            audience: target.realm_id,
            descriptor_digest: descriptor_digest(audience)?,
            nonce,
            name: name.map(|name| name.chars().take(MAX_NAME_LEN).collect()),
            issued_at: now,
            expires_at: now.saturating_add(MAX_HANDOFF_SECS),
            handoff_id: Ulid::generate(),
        })
    }
}

/// Admits a handoff at the serving realm `local` against its current settings and the secret.
pub fn check_handoff(
    handoff: &Signed<LoginHandoff>,
    local: &RealmId,
    settings: &FederationSettings,
    secret: &[u8],
    now: u64,
) -> Result<(), HandoffError> {
    let payload = &handoff.payload;
    handoff.verify(&payload.issuer)?;
    if payload.user.realm_id != payload.issuer || payload.user.is_nil() {
        return Err(HandoffError::ForeignUser);
    }
    if payload.audience != *local || settings.descriptor.payload.realm_id != *local {
        return Err(HandoffError::WrongAudience);
    }
    if payload.descriptor_digest != descriptor_digest(&settings.descriptor)? {
        return Err(HandoffError::StaleDescriptor);
    }
    if secret_nonce(secret) != payload.nonce {
        return Err(HandoffError::WrongSecret);
    }
    let lifetime = payload.expires_at.checked_sub(payload.issued_at);
    if !lifetime.is_some_and(|secs| secs > 0 && secs <= MAX_HANDOFF_SECS)
        || now >= payload.expires_at
        || payload.issued_at > now.saturating_add(HANDOFF_SKEW_SECS)
    {
        return Err(HandoffError::BadLifetime);
    }
    if !settings.accepted_realms.admits(&payload.issuer) || payload.issuer == *local {
        return Err(HandoffError::NotAccepted);
    }
    if payload
        .name
        .as_ref()
        .is_some_and(|name| name.chars().count() > MAX_NAME_LEN)
    {
        return Err(HandoffError::NameTooLong);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::{AcceptedRealms, RegistrationMode};
    use crate::structs::identity::auth::NodeCapabilities;
    use ed25519_dalek::SigningKey;
    use url::Url;

    const NOW: u64 = 1_000;
    const SECRET: &[u8] = b"browser secret";

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn realm(seed: u8) -> RealmId {
        RealmId::from_bytes(key(seed).verifying_key().to_bytes())
    }

    fn capabilities(seed: u8) -> NodeCapabilities {
        NodeCapabilities::management_node(key(seed)).unwrap()
    }

    /// Settings of the serving realm 2, signed at `issued_at`.
    fn settings(accepted: AcceptedRealms, issued_at: u64) -> FederationSettings {
        let url = |value: &str| Url::parse(value).unwrap();
        let descriptor = RealmDescriptor {
            realm_id: realm(2),
            name: "Serving".to_string(),
            description: String::new(),
            api_url: url("https://b.example.org/api/v1"),
            portal_url: url("https://b.example.org"),
            issued_at,
        };
        FederationSettings {
            name: descriptor.name.clone(),
            api_url: descriptor.api_url.clone(),
            portal_url: descriptor.portal_url.clone(),
            registry_url: None,
            registration: RegistrationMode::Enabled,
            accepted_realms: accepted,
            descriptor: Signed::sign(descriptor, &capabilities(2)).unwrap(),
        }
    }

    fn accepting() -> FederationSettings {
        settings(AcceptedRealms::Only(vec![realm(1)]), 10)
    }

    fn user() -> UserId {
        UserId::new(Ulid::from_bytes([5; 16]), realm(1))
    }

    /// A handoff of realm 1 for the current serving descriptor, edited before signing.
    fn handoff(edit: impl FnOnce(&mut LoginHandoff)) -> Signed<LoginHandoff> {
        let mut payload = LoginHandoff::new(
            realm(1),
            user(),
            &accepting().descriptor,
            secret_nonce(SECRET),
            Some("Ada".to_string()),
            NOW,
        )
        .unwrap();
        edit(&mut payload);
        Signed::sign(payload, &capabilities(1)).unwrap()
    }

    fn check(
        handoff: &Signed<LoginHandoff>,
        settings: &FederationSettings,
    ) -> Result<(), HandoffError> {
        check_handoff(handoff, &realm(2), settings, SECRET, NOW + 1)
    }

    #[test]
    fn accepts_valid_handoff() {
        assert_eq!(check(&handoff(|_| {}), &accepting()), Ok(()));
        let any = settings(AcceptedRealms::Any, 10);
        assert_eq!(check(&handoff(|_| {}), &any), Ok(()));
    }

    #[test]
    fn rejects_wrong_signature() {
        // Signed by realm 3 while naming realm 1 as issuer.
        let payload = handoff(|_| {}).payload;
        let forged = Signed::sign(payload, &capabilities(3)).unwrap();
        assert!(matches!(
            check(&forged, &accepting()),
            Err(HandoffError::Signature(_))
        ));
    }

    #[test]
    fn rejects_foreign_user() {
        let other = UserId::new(Ulid::from_bytes([5; 16]), realm(3));
        let signed = handoff(|payload| payload.user = other);
        assert_eq!(check(&signed, &accepting()), Err(HandoffError::ForeignUser));
    }

    #[test]
    fn rejects_wrong_audience() {
        let signed = handoff(|payload| payload.audience = realm(3));
        assert_eq!(
            check(&signed, &accepting()),
            Err(HandoffError::WrongAudience)
        );
    }

    #[test]
    fn rejects_superseded_descriptor() {
        // A newer descriptor replaced the one the handoff was made for.
        let newer = settings(AcceptedRealms::Only(vec![realm(1)]), 11);
        assert_eq!(
            check(&handoff(|_| {}), &newer),
            Err(HandoffError::StaleDescriptor)
        );
        let signed = handoff(|payload| payload.descriptor_digest = "00".repeat(32));
        assert_eq!(
            check(&signed, &accepting()),
            Err(HandoffError::StaleDescriptor)
        );
    }

    #[test]
    fn rejects_wrong_secret() {
        let signed = handoff(|_| {});
        let result = check_handoff(&signed, &realm(2), &accepting(), b"other", NOW + 1);
        assert_eq!(result, Err(HandoffError::WrongSecret));
    }

    #[test]
    fn rejects_bad_lifetimes() {
        let long = handoff(|payload| payload.expires_at = payload.issued_at + MAX_HANDOFF_SECS + 1);
        assert_eq!(check(&long, &accepting()), Err(HandoffError::BadLifetime));
        let signed = handoff(|_| {});
        let expired = check_handoff(
            &signed,
            &realm(2),
            &accepting(),
            SECRET,
            NOW + MAX_HANDOFF_SECS,
        );
        assert_eq!(expired, Err(HandoffError::BadLifetime));
        let future = handoff(|payload| {
            payload.issued_at = NOW + 1 + HANDOFF_SKEW_SECS + 1;
            payload.expires_at = payload.issued_at + 10;
        });
        assert_eq!(check(&future, &accepting()), Err(HandoffError::BadLifetime));
    }

    #[test]
    fn rejects_unaccepted_realm() {
        let none = settings(AcceptedRealms::None, 10);
        assert_eq!(
            check(&handoff(|_| {}), &none),
            Err(HandoffError::NotAccepted)
        );
        let others = settings(AcceptedRealms::Only(vec![realm(3)]), 10);
        assert_eq!(
            check(&handoff(|_| {}), &others),
            Err(HandoffError::NotAccepted)
        );
    }

    #[test]
    fn rejects_long_name() {
        let signed = handoff(|payload| payload.name = Some("a".repeat(MAX_NAME_LEN + 1)));
        assert_eq!(check(&signed, &accepting()), Err(HandoffError::NameTooLong));
    }

    #[test]
    fn refuses_bad_audience() {
        // The home realm verifies the audience descriptor and never targets itself.
        let mut tampered = accepting().descriptor;
        tampered.payload.name = "Other".to_string();
        let result = LoginHandoff::new(realm(1), user(), &tampered, String::new(), None, NOW);
        assert!(matches!(result, Err(HandoffError::Signature(_))));
        let own = settings(AcceptedRealms::Any, 10).descriptor;
        let local = UserId::new(Ulid::from_bytes([5; 16]), realm(2));
        let result = LoginHandoff::new(realm(2), local, &own, String::new(), None, NOW);
        assert_eq!(result, Err(HandoffError::WrongAudience));
    }
}
