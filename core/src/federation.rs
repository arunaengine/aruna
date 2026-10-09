//! Federation contracts: the signed realm descriptor, realm-wide settings and registry payloads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::IpAddr;
use std::str::FromStr;

use base64::Engine;
use ed25519_dalek::{Signature, Signer as _, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::{Host, Url};

use crate::structs::identity::auth::NodeCapabilities;
use crate::structs::identity::realm::RealmId;

pub const DESCRIPTOR_DOMAIN: &str = "aruna-realm-descriptor-v1";
pub const REGISTRATION_DOMAIN: &str = "aruna-registry-registration-v1";
pub const WITHDRAWAL_DOMAIN: &str = "aruna-registry-withdrawal-v1";

/// Longest realm name a descriptor carries.
pub const MAX_NAME_LEN: usize = 128;
/// Most realms an `Only` admission list may name.
pub const MAX_ACCEPTED_REALMS: usize = 1024;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FederationError {
    #[error("payload names another realm")]
    RealmMismatch,
    #[error("signature does not verify")]
    BadSignature,
    #[error("realm delegation does not verify")]
    BadDelegation,
    #[error("this node holds no realm or delegated issuer key")]
    NoSigningKey,
    #[error("invalid federation settings: {0}")]
    InvalidSettings(&'static str),
    #[error("payload encoding failed: {0}")]
    Encoding(String),
}

/// A payload with its own signature domain, naming the realm that must sign it.
pub trait Signable: Serialize {
    const DOMAIN: &'static str;
    fn realm_id(&self) -> RealmId;
}

/// Public realm facts. A newer `issued_at` supersedes older descriptors.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealmDescriptor {
    pub realm_id: RealmId,
    pub name: String,
    pub description: String,
    pub api_url: Url,
    pub portal_url: Url,
    pub issued_at: u64,
}

impl Signable for RealmDescriptor {
    const DOMAIN: &'static str = DESCRIPTOR_DOMAIN;
    fn realm_id(&self) -> RealmId {
        self.realm_id
    }
}

/// Self-reported realm totals. `None` means unknown, never zero.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealmKpis {
    pub live_datasets: Option<u64>,
    pub groups: Option<u64>,
    pub nodes_configured: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Registration {
    pub descriptor: Signed<RealmDescriptor>,
    pub kpis: RealmKpis,
    pub observed_at: u64,
    pub issued_at: u64,
}

impl Signable for Registration {
    const DOMAIN: &'static str = REGISTRATION_DOMAIN;
    fn realm_id(&self) -> RealmId {
        self.descriptor.payload.realm_id
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Withdrawal {
    pub realm_id: RealmId,
    pub issued_at: u64,
}

impl Signable for Withdrawal {
    const DOMAIN: &'static str = WITHDRAWAL_DOMAIN;
    fn realm_id(&self) -> RealmId {
        self.realm_id
    }
}

/// The key behind a signature: the realm key itself, or an issuer key the realm delegated.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Signer {
    Realm,
    Delegated {
        issuer_pubkey: String,
        delegation_signature: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signed<T> {
    pub payload: T,
    pub signer: Signer,
    /// Hex ed25519 signature over the domain-tagged postcard payload.
    pub signature: String,
}

/// The domain-tagged bytes a signature covers.
pub fn signing_bytes<T: Signable>(payload: &T) -> Result<Vec<u8>, FederationError> {
    postcard::to_allocvec(&(T::DOMAIN, payload))
        .map_err(|error| FederationError::Encoding(error.to_string()))
}

impl<T: Signable> Signed<T> {
    /// Signs with the realm key on a management node or the delegated issuer key on a server.
    pub fn sign(payload: T, capabilities: &NodeCapabilities) -> Result<Self, FederationError> {
        let bytes = signing_bytes(&payload)?;
        let (key, signer) = match capabilities {
            NodeCapabilities::Management {
                realm_signing_key, ..
            } => (realm_signing_key, Signer::Realm),
            NodeCapabilities::Server {
                issuer_signing_key,
                delegation_signature,
                ..
            } => (
                issuer_signing_key,
                Signer::Delegated {
                    issuer_pubkey: base64::engine::general_purpose::URL_SAFE_NO_PAD
                        .encode(issuer_signing_key.verifying_key().to_bytes()),
                    delegation_signature: delegation_signature.clone(),
                },
            ),
            NodeCapabilities::User { .. } => return Err(FederationError::NoSigningKey),
        };
        Ok(Self {
            payload,
            signer,
            signature: sign_hex(key, &bytes),
        })
    }

    /// Accepts only a payload naming `realm_id`, signed by that realm's key or its delegate.
    pub fn verify(&self, realm_id: &RealmId) -> Result<(), FederationError> {
        if self.payload.realm_id() != *realm_id {
            return Err(FederationError::RealmMismatch);
        }
        let realm_key = verifying_key(realm_id.as_bytes())?;
        let key = match &self.signer {
            Signer::Realm => realm_key,
            Signer::Delegated {
                issuer_pubkey,
                delegation_signature,
            } => {
                let delegation = parse_signature(delegation_signature)
                    .map_err(|_| FederationError::BadDelegation)?;
                realm_key
                    .verify_strict(issuer_pubkey.as_bytes(), &delegation)
                    .map_err(|_| FederationError::BadDelegation)?;
                let issuer = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(issuer_pubkey)
                    .ok()
                    .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
                    .ok_or(FederationError::BadDelegation)?;
                verifying_key(&issuer)?
            }
        };
        let signature = parse_signature(&self.signature)?;
        key.verify_strict(&signing_bytes(&self.payload)?, &signature)
            .map_err(|_| FederationError::BadSignature)
    }
}

fn sign_hex(key: &SigningKey, bytes: &[u8]) -> String {
    key.sign(bytes).to_string()
}

fn parse_signature(value: &str) -> Result<Signature, FederationError> {
    Signature::from_str(value).map_err(|_| FederationError::BadSignature)
}

fn verifying_key(bytes: &[u8; 32]) -> Result<VerifyingKey, FederationError> {
    VerifyingKey::from_bytes(bytes).map_err(|_| FederationError::BadSignature)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegistrationMode {
    #[default]
    Enabled,
    Disabled,
}

/// Realms whose users may log in here as foreign users.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum AcceptedRealms {
    #[default]
    None,
    Only(Vec<RealmId>),
    Any,
}

impl AcceptedRealms {
    /// Whether logins of `realm_id` are admitted.
    pub fn admits(&self, realm_id: &RealmId) -> bool {
        match self {
            Self::None => false,
            Self::Only(realms) => realms.contains(realm_id),
            Self::Any => true,
        }
    }
}

/// Realm-wide federation settings with the descriptor signed from them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FederationSettings {
    pub name: String,
    pub api_url: Url,
    pub portal_url: Url,
    /// No URL means no registry traffic.
    pub registry_url: Option<Url>,
    pub registration: RegistrationMode,
    pub accepted_realms: AcceptedRealms,
    pub descriptor: Signed<RealmDescriptor>,
}

impl FederationSettings {
    /// Hex SHA-256 over the postcard encoding, so equal settings have equal digests.
    pub fn digest(&self) -> Result<String, FederationError> {
        crate::transfer::digest_of(self)
    }

    /// Checks bounds, URL rules and that the descriptor is signed for these values.
    pub fn validate(&self, realm_id: &RealmId) -> Result<(), FederationError> {
        if self.name.trim().is_empty() || self.name.chars().count() > MAX_NAME_LEN {
            return Err(FederationError::InvalidSettings("name length"));
        }
        let urls = [
            Some(&self.api_url),
            Some(&self.portal_url),
            self.registry_url.as_ref(),
        ];
        if !urls.into_iter().flatten().all(valid_federation_url) {
            return Err(FederationError::InvalidSettings("url must be https"));
        }
        if matches!(&self.accepted_realms, AcceptedRealms::Only(realms) if realms.len() > MAX_ACCEPTED_REALMS)
        {
            return Err(FederationError::InvalidSettings("too many accepted realms"));
        }
        let descriptor = &self.descriptor.payload;
        if descriptor.name != self.name
            || descriptor.api_url != self.api_url
            || descriptor.portal_url != self.portal_url
        {
            return Err(FederationError::InvalidSettings(
                "descriptor does not match",
            ));
        }
        self.descriptor.verify(realm_id)
    }
}

/// HTTPS, or plain HTTP to a loopback host for local development.
pub fn valid_federation_url(url: &Url) -> bool {
    match url.scheme() {
        "https" => url.host().is_some(),
        "http" => match url.host() {
            Some(Host::Domain(domain)) => domain == "localhost",
            Some(Host::Ipv4(address)) => IpAddr::V4(address).is_loopback(),
            Some(Host::Ipv6(address)) => IpAddr::V6(address).is_loopback(),
            None => false,
        },
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn realm_key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn realm_of(key: &SigningKey) -> RealmId {
        RealmId::from_bytes(key.verifying_key().to_bytes())
    }

    fn management(key: &SigningKey) -> NodeCapabilities {
        NodeCapabilities::management_node(key.clone()).unwrap()
    }

    fn descriptor(realm_id: RealmId) -> RealmDescriptor {
        RealmDescriptor {
            realm_id,
            name: "Realm".to_string(),
            description: String::new(),
            api_url: Url::parse("https://api.example.org").unwrap(),
            portal_url: Url::parse("https://portal.example.org").unwrap(),
            issued_at: 10,
        }
    }

    fn delegated(realm: &SigningKey, issuer: &SigningKey) -> NodeCapabilities {
        let pubkey = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(issuer.verifying_key().to_bytes());
        let delegation = sign_hex(realm, pubkey.as_bytes());
        NodeCapabilities::server_node(issuer.clone(), realm_of(realm), delegation).unwrap()
    }

    #[test]
    fn realm_key_verifies() {
        let key = realm_key(1);
        let signed = Signed::sign(descriptor(realm_of(&key)), &management(&key)).unwrap();
        assert_eq!(signed.verify(&realm_of(&key)), Ok(()));
    }

    #[test]
    fn delegated_key_verifies() {
        let key = realm_key(1);
        let caps = delegated(&key, &realm_key(2));
        let signed = Signed::sign(descriptor(realm_of(&key)), &caps).unwrap();
        assert_eq!(signed.verify(&realm_of(&key)), Ok(()));
    }

    #[test]
    fn rejects_wrong_key() {
        // Another realm's key cannot sign for this realm id.
        let key = realm_key(1);
        let other = realm_key(3);
        let signed = Signed::sign(descriptor(realm_of(&key)), &management(&other)).unwrap();
        assert_eq!(
            signed.verify(&realm_of(&key)),
            Err(FederationError::BadSignature)
        );
    }

    #[test]
    fn rejects_wrong_realm() {
        let key = realm_key(1);
        let signed = Signed::sign(descriptor(realm_of(&key)), &management(&key)).unwrap();
        assert_eq!(
            signed.verify(&realm_of(&realm_key(4))),
            Err(FederationError::RealmMismatch)
        );
    }

    #[test]
    fn rejects_undelegated_node() {
        // A node key without a realm delegation is not a realm signer.
        let key = realm_key(1);
        let node = realm_key(5);
        let forged = delegated(&node, &node);
        let signed = Signed::sign(descriptor(realm_of(&key)), &forged).unwrap();
        assert_eq!(
            signed.verify(&realm_of(&key)),
            Err(FederationError::BadDelegation)
        );
        let mut as_realm = signed;
        as_realm.signer = Signer::Realm;
        assert_eq!(
            as_realm.verify(&realm_of(&key)),
            Err(FederationError::BadSignature)
        );
    }

    #[derive(Serialize)]
    #[serde(transparent)]
    struct Mirror(Withdrawal);

    impl Signable for Mirror {
        const DOMAIN: &'static str = REGISTRATION_DOMAIN;
        fn realm_id(&self) -> RealmId {
            self.0.realm_id
        }
    }

    #[test]
    fn rejects_tag_confusion() {
        // Identical payload bytes under another domain tag do not verify.
        let key = realm_key(1);
        let realm_id = realm_of(&key);
        let payload = Withdrawal {
            realm_id,
            issued_at: 10,
        };
        let withdrawal = Signed::sign(payload.clone(), &management(&key)).unwrap();
        let confused = Signed {
            payload: Mirror(payload),
            signer: Signer::Realm,
            signature: withdrawal.signature,
        };
        assert_eq!(
            confused.verify(&realm_id),
            Err(FederationError::BadSignature)
        );
    }

    #[test]
    fn rejects_tampered_payload() {
        let key = realm_key(1);
        let mut signed = Signed::sign(descriptor(realm_of(&key)), &management(&key)).unwrap();
        signed.payload.portal_url = Url::parse("https://evil.example.org").unwrap();
        assert_eq!(
            signed.verify(&realm_of(&key)),
            Err(FederationError::BadSignature)
        );
    }

    #[test]
    fn settings_bind_descriptor() {
        // Settings whose values differ from the signed descriptor are refused.
        let key = realm_key(1);
        let realm_id = realm_of(&key);
        let signed = Signed::sign(descriptor(realm_id), &management(&key)).unwrap();
        let mut settings = FederationSettings {
            name: "Realm".to_string(),
            api_url: signed.payload.api_url.clone(),
            portal_url: signed.payload.portal_url.clone(),
            registry_url: None,
            registration: RegistrationMode::Enabled,
            accepted_realms: AcceptedRealms::None,
            descriptor: signed,
        };
        assert_eq!(settings.validate(&realm_id), Ok(()));
        settings.portal_url = Url::parse("https://other.example.org").unwrap();
        assert!(settings.validate(&realm_id).is_err());
    }

    #[test]
    fn url_rules() {
        let valid = |url: &str| valid_federation_url(&Url::parse(url).unwrap());
        assert!(valid("https://realm.example.org"));
        assert!(valid("http://localhost:8080"));
        assert!(valid("http://127.0.0.1:8080"));
        assert!(!valid("http://realm.example.org"));
        assert!(!valid("ftp://realm.example.org"));
    }
}
