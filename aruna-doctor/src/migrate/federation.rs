//! Re-encodes realm configs whose federation settings predate the description, contact and
//! login provider fields, keeping the descriptor signature of the older layout.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::session_refs;
use aruna_core::federation::{
    AcceptedRealms, FederationSettings, RealmDescriptor, RegistrationMode, Signed, Signer,
};
use aruna_core::request_policy::RequestPolicy;
use aruna_core::structs::identity::realm::{
    MetadataReplicationConfig, OidcProviderConfig, QuotaConfig, RealmConfigDocument,
    RealmDiscoveryConfig, RealmId, RealmNode, ReplicaQuota, TokenRevocation,
};
use aruna_core::structs::placement::compute_config::RealmComputeConfig;
use aruna_core::structs::placement::record::{
    BandPool, HandleRange, NodePlacementEntry, PlacementBinding, PlacementOverride,
    PlacementStrategy, StrategyBinding,
};
use aruna_core::structs::placement::transition::{
    CandidatePlacementMap, PlacementActivation, PlacementTransition,
};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ulid::Ulid;

/// The current encoding of a realm config stored with federation settings in the older layout,
/// which also lacked the replica quota; every byte of the row must be used.
pub(super) fn upgraded(value: &[u8]) -> Option<Vec<u8>> {
    let legacy = session_refs::decode::<LegacyConfig>(value, &[]).ok()?;
    postcard::to_allocvec(&RealmConfigDocument::from(legacy)).ok()
}

/// Previous shape of `RealmDescriptor`, before the contact and the login providers.
#[derive(Serialize, Deserialize)]
struct LegacyDescriptor {
    realm_id: RealmId,
    name: String,
    description: String,
    api_url: Url,
    portal_url: Url,
    issued_at: u64,
}

/// Previous shape of `Signed<RealmDescriptor>`.
#[derive(Serialize, Deserialize)]
struct LegacySigned {
    payload: LegacyDescriptor,
    signer: Signer,
    signature: String,
}

/// Previous shape of `FederationSettings`, before the description, contact and login providers.
#[derive(Serialize, Deserialize)]
struct LegacyFederation {
    name: String,
    api_url: Url,
    portal_url: Url,
    registry_url: Option<Url>,
    registration: RegistrationMode,
    accepted_realms: AcceptedRealms,
    descriptor: LegacySigned,
}

/// Previous shape of `RealmConfigDocument`: older federation settings, no replica quota.
#[derive(Serialize, Deserialize)]
struct LegacyConfig {
    realm_id: RealmId,
    metadata_replication: MetadataReplicationConfig,
    oidc_providers: Vec<OidcProviderConfig>,
    discovery: RealmDiscoveryConfig,
    nodes: Vec<RealmNode>,
    quota: QuotaConfig,
    description: String,
    placement_map: Vec<NodePlacementEntry>,
    strategies: Vec<PlacementStrategy>,
    default_strategy_id: Option<Ulid>,
    family_strategy_id: Ulid,
    strategy_bindings: Vec<StrategyBinding>,
    placement_overrides: Vec<PlacementOverride>,
    placement_bindings: Vec<PlacementBinding>,
    placement_handle_ranges: Vec<HandleRange>,
    band_pools: Vec<BandPool>,
    candidate_maps: Vec<CandidatePlacementMap>,
    placement_activations: Vec<PlacementActivation>,
    placement_transitions: Vec<PlacementTransition>,
    request_policies: Vec<RequestPolicy>,
    revoked_tokens: Vec<TokenRevocation>,
    revocation_floor: u64,
    compute: RealmComputeConfig,
    federation: Option<LegacyFederation>,
    portal_features: BTreeMap<String, bool>,
}

/// Without a contact or login providers, the signature over the older layout still verifies.
impl From<LegacySigned> for Signed<RealmDescriptor> {
    fn from(old: LegacySigned) -> Self {
        Self {
            payload: RealmDescriptor {
                realm_id: old.payload.realm_id,
                name: old.payload.name,
                description: old.payload.description,
                api_url: old.payload.api_url,
                portal_url: old.payload.portal_url,
                contact: None,
                login_providers: Vec::new(),
                issued_at: old.payload.issued_at,
            },
            signer: old.signer,
            signature: old.signature,
        }
    }
}

/// The settings take the description the descriptor was signed with, so both still match.
impl From<LegacyFederation> for FederationSettings {
    fn from(old: LegacyFederation) -> Self {
        Self {
            name: old.name,
            description: old.descriptor.payload.description.clone(),
            contact: None,
            login_providers: Vec::new(),
            api_url: old.api_url,
            portal_url: old.portal_url,
            registry_url: old.registry_url,
            registration: old.registration,
            accepted_realms: old.accepted_realms,
            descriptor: old.descriptor.into(),
        }
    }
}

impl From<LegacyConfig> for RealmConfigDocument {
    fn from(old: LegacyConfig) -> Self {
        Self {
            realm_id: old.realm_id,
            metadata_replication: old.metadata_replication,
            oidc_providers: old.oidc_providers,
            discovery: old.discovery,
            nodes: old.nodes,
            quota: old.quota,
            description: old.description,
            placement_map: old.placement_map,
            strategies: old.strategies,
            default_strategy_id: old.default_strategy_id,
            family_strategy_id: old.family_strategy_id,
            strategy_bindings: old.strategy_bindings,
            placement_overrides: old.placement_overrides,
            placement_bindings: old.placement_bindings,
            placement_handle_ranges: old.placement_handle_ranges,
            band_pools: old.band_pools,
            candidate_maps: old.candidate_maps,
            placement_activations: old.placement_activations,
            placement_transitions: old.placement_transitions,
            request_policies: old.request_policies,
            revoked_tokens: old.revoked_tokens,
            revocation_floor: old.revocation_floor,
            compute: old.compute,
            federation: old.federation.map(Into::into),
            portal_features: old.portal_features,
            replica_quota: ReplicaQuota::default(),
            email: None,
            vault: Vec::new(),
            secrets: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::migrate::migrate_output;
    use crate::migrate::tests::{read, write};
    use aruna_core::federation::{
        AcceptedRealms, DESCRIPTOR_DOMAIN, FederationSettings, RegistrationMode, Signer,
    };
    use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
    use aruna_core::realm_vault::RealmVaultRecord;
    use aruna_core::structs::execution::email::RealmEmailProfile;
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, ReplicaQuota};
    use ed25519_dalek::{Signer as _, SigningKey};
    use reqwest::Url;
    use serde::Serialize;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    /// `RealmDescriptor` as alpha.134 stored and signed it.
    #[derive(Serialize)]
    struct OldDescriptor {
        realm_id: RealmId,
        name: String,
        description: String,
        api_url: Url,
        portal_url: Url,
        issued_at: u64,
    }

    #[derive(Serialize)]
    struct OldSigned {
        payload: OldDescriptor,
        signer: Signer,
        signature: String,
    }

    /// `FederationSettings` as alpha.134 stored it.
    #[derive(Serialize)]
    struct OldFederation {
        name: String,
        api_url: Url,
        portal_url: Url,
        registry_url: Option<Url>,
        registration: RegistrationMode,
        accepted_realms: AcceptedRealms,
        descriptor: OldSigned,
    }

    /// Settings of a registered realm, signed by its realm key in the older layout.
    fn old_federation(key: &SigningKey, realm_id: RealmId) -> OldFederation {
        let api_url = Url::parse("https://api.example.org/api/v1").unwrap();
        let portal_url = Url::parse("https://portal.example.org/").unwrap();
        let payload = OldDescriptor {
            realm_id,
            name: "Realm".to_string(),
            description: "Production realm".to_string(),
            api_url: api_url.clone(),
            portal_url: portal_url.clone(),
            issued_at: 10,
        };
        let bytes = postcard::to_allocvec(&(DESCRIPTOR_DOMAIN, &payload)).unwrap();
        OldFederation {
            name: "Realm".to_string(),
            api_url,
            portal_url,
            registry_url: Some(Url::parse("https://registry.example.org/").unwrap()),
            registration: RegistrationMode::default(),
            accepted_realms: AcceptedRealms::Any,
            descriptor: OldSigned {
                signature: key.sign(&bytes).to_string(),
                signer: Signer::Realm,
                payload,
            },
        }
    }

    #[test]
    fn rewrites_old_federation() {
        // An alpha.134 config with federation settings becomes the current shape, its old
        // signature still verifies, and a second run changes nothing.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let key = SigningKey::from_bytes(&[9u8; 32]);
        let realm_id = RealmId::from_bytes(key.verifying_key().to_bytes());
        let current = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        let unfederated = postcard::to_allocvec(&current).unwrap();
        let features = postcard::to_allocvec(&BTreeMap::<String, bool>::new()).unwrap();
        let replicas = postcard::to_allocvec(&ReplicaQuota::default()).unwrap();
        let secrets = (
            None::<RealmEmailProfile>,
            Vec::<RealmVaultRecord>::new(),
            Vec::<aruna_core::secrets::SecretRecord>::new(),
        );
        let secrets = postcard::to_allocvec(&secrets).unwrap();
        // The config ends with federation, portal features, replica quota, email, vault and secrets.
        let tail = 1 + features.len() + replicas.len() + secrets.len();
        let head = &unfederated[..unfederated.len() - tail];
        let federation = postcard::to_allocvec(&Some(old_federation(&key, realm_id))).unwrap();
        let old = [head, &federation, &features].concat();
        write(&path, REALM_CONFIG_KEYSPACE, vec![(b"config", old)]);

        let output = migrate_output(path.to_str().unwrap()).unwrap();

        assert_eq!(
            (output.realm_configs_scanned, output.realm_configs_rewritten),
            (1, 1)
        );
        let rows = read(&path, REALM_CONFIG_KEYSPACE);
        let migrated = RealmConfigDocument::from_bytes(&rows[b"config".as_slice()]).unwrap();
        let settings: FederationSettings = migrated.federation.clone().expect("federation");
        settings
            .validate(&realm_id)
            .expect("old signature verifies");
        assert_eq!(settings.description, "Production realm");
        assert_eq!(
            (settings.contact, settings.login_providers.len()),
            (None, 0)
        );
        let mut rest = migrated;
        rest.federation = None;
        assert_eq!(rest, current);

        let again = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!(again.realm_configs_rewritten, 0);
        assert_eq!(read(&path, REALM_CONFIG_KEYSPACE), rows);
    }
}
