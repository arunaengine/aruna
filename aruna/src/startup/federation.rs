//! Creates the default federation settings at start, so a public realm registers on its own.
//! Stored settings are never changed, and a realm without public URLs never registers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_api::portal::api_base_url;
use aruna_core::UserId;
use aruna_core::egress::EgressPolicy;
use aruna_core::federation::{
    AcceptedRealms, MAX_NAME_LEN, RegistrationMode, valid_federation_url,
};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::time::unix_timestamp_secs;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::federation::publish::is_reporting;
use aruna_operations::realm::get_config::GetConfigOperation;
use aruna_operations::realm::set_federation::{
    SetFederationConfig, SetFederationError, SetFederationOperation,
};
use reqwest::Url;
use thiserror::Error;
use tracing::{info, warn};

use crate::config::Config;

/// Bound on resolving this node's own public host names.
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Why this node creates no default settings.
#[derive(Debug, Error, PartialEq, Eq)]
enum Skip {
    #[error("FEDERATION_REGISTRY_URL is empty")]
    RegistryOff,
    #[error("{0} is not set")]
    MissingUrl(&'static str),
    #[error("{0} must use HTTPS")]
    NotHttps(&'static str),
    #[error("{0} is not a public address")]
    PrivateUrl(&'static str),
}

/// The URLs of the default settings.
#[derive(Debug, PartialEq, Eq)]
struct EntryPoints {
    api_url: Url,
    portal_url: Url,
    registry_url: Url,
}

/// Creates the default settings when the realm has none and this node reports for it.
/// It never fails the start: a skip is one info line and a failure one warning.
pub(crate) async fn register_default(config: &Config, ctx: &DriverContext) {
    let realm = match drive(GetConfigOperation::new(config.realm_id), ctx).await {
        Ok(realm) => realm,
        Err(error) => {
            warn!(error = %error, "Failed to read the realm config for federation defaults");
            return;
        }
    };
    // Only the reporting node creates them, so management nodes do not race each other.
    if realm.federation.is_some() || !is_reporting(&realm, config.node_id) {
        return;
    }
    let policy = EgressPolicy::strict().with_deny(config.blob_backends.extra_deny.clone());
    let points = match entry_points(
        config.registry_url.as_ref(),
        config.api_public_url.as_deref(),
        config.portal_public_url.as_deref(),
        &policy,
    )
    .await
    {
        Ok(points) => points,
        Err(reason) => {
            info!(%reason, "This realm does not register with a registry");
            return;
        }
    };
    let registry_url = points.registry_url.clone();
    let operation = SetFederationOperation::new(SetFederationConfig {
        actor: node_actor(config),
        auth_context: None,
        node_capabilities: config.node_capabilities.clone(),
        name: config
            .realm_description
            .trim()
            .chars()
            .take(MAX_NAME_LEN)
            .collect(),
        api_url: points.api_url,
        portal_url: points.portal_url,
        registry_url: Some(points.registry_url),
        registration: RegistrationMode::Enabled,
        accepted_realms: AcceptedRealms::None,
        expected: None,
        now: unix_timestamp_secs(),
    });
    match drive(operation, ctx).await {
        Ok(_) => info!(registry = %registry_url, "Created the default federation settings"),
        // Settings stored in the meantime stay as they are.
        Err(SetFederationError::SettingsChanged) => {}
        Err(error) => warn!(error = %error, "Failed to create the default federation settings"),
    }
}

fn node_actor(config: &Config) -> Actor {
    Actor {
        node_id: config.node_id,
        user_id: UserId::nil(config.realm_id),
        realm_id: config.realm_id,
    }
}

async fn entry_points(
    registry_url: Option<&Url>,
    api_public_url: Option<&str>,
    portal_public_url: Option<&str>,
    policy: &EgressPolicy,
) -> Result<EntryPoints, Skip> {
    let registry_url = registry_url.ok_or(Skip::RegistryOff)?.clone();
    let api_base = api_public_url.map(api_base_url);
    let api_url = public_url("API_PUBLIC_URL", api_base.as_deref(), policy).await?;
    let portal_url = public_url("PORTAL_PUBLIC_URL", portal_public_url, policy).await?;
    Ok(EntryPoints {
        api_url,
        portal_url,
        registry_url,
    })
}

async fn public_url(
    key: &'static str,
    value: Option<&str>,
    policy: &EgressPolicy,
) -> Result<Url, Skip> {
    let url = value
        .and_then(|value| Url::parse(value).ok())
        .ok_or(Skip::MissingUrl(key))?;
    if !valid_federation_url(&url) {
        return Err(Skip::NotHttps(key));
    }
    match is_public(&url, policy).await {
        true => Ok(url),
        false => Err(Skip::PrivateUrl(key)),
    }
}

/// Whether the host resolves to an address the egress rules allow, as for any outgoing request.
async fn is_public(url: &Url, policy: &EgressPolicy) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let lookup = tokio::net::lookup_host((host, 0));
    match tokio::time::timeout(RESOLVE_TIMEOUT, lookup).await {
        Ok(Ok(mut addresses)) => addresses.any(|address| policy.check(address.ip()).is_ok()),
        Ok(Err(_)) | Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::resolve_settings;
    use crate::settings::read_settings_from;
    use aruna_core::federation::FederationSettings;
    use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    // Literal addresses, so no test resolves a name or contacts a registry.
    const API: &str = "https://30.255.255.1:8443";
    const PORTAL: &str = "https://30.255.255.1:8444/";
    const REGISTRY: &str = "https://registry.example.org/";

    /// A started management node of a new realm with public URLs and a test registry.
    async fn node(pairs: &[(&str, &str)]) -> (TempDir, Config, DriverContext) {
        let dir = tempfile::tempdir().unwrap();
        let mut env = BTreeMap::from([
            ("STORAGE_PATH", dir.path().to_str().unwrap()),
            ("SOCKET_ADDRESS", "127.0.0.1:0"),
            ("P2P_SOCKET_ADDRESS", "127.0.0.1:0"),
            ("S3_HOST", "127.0.0.1:0"),
            ("S3_ADDRESS", "127.0.0.1:0"),
            ("PORTAL_MODE", "disabled"),
            ("ARUNA_FJALL_PERSIST_MODE", "buffer"),
            ("API_PUBLIC_URL", API),
            ("PORTAL_PUBLIC_URL", PORTAL),
            ("FEDERATION_REGISTRY_URL", REGISTRY),
        ]);
        env.extend(pairs.iter().copied());
        let env: BTreeMap<String, String> = env
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        let (config, storage_handle) = resolve_settings(read_settings_from(&env).unwrap())
            .await
            .unwrap();
        let ctx = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: node_actor(&config),
                realm_description: config.realm_description.clone(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            &ctx,
        )
        .await
        .unwrap();
        (dir, config, ctx)
    }

    async fn stored(config: &Config, ctx: &DriverContext) -> Option<FederationSettings> {
        drive(GetConfigOperation::new(config.realm_id), ctx)
            .await
            .unwrap()
            .federation
    }

    #[tokio::test]
    async fn creates_default_settings() {
        // A realm without settings gets the node's public URLs, the registry and registration on.
        let description = "R".repeat(MAX_NAME_LEN + 10);
        let (_dir, config, ctx) = node(&[("REALM_DESCRIPTION", description.as_str())]).await;
        register_default(&config, &ctx).await;

        let settings = stored(&config, &ctx).await.expect("settings created");
        assert_eq!(settings.name, "R".repeat(MAX_NAME_LEN));
        assert_eq!(settings.api_url.as_str(), format!("{API}/api/v1"));
        assert_eq!(settings.portal_url.as_str(), PORTAL);
        assert_eq!(
            settings.registry_url.as_ref().map(Url::as_str),
            Some(REGISTRY)
        );
        assert_eq!(settings.registration, RegistrationMode::Enabled);
        assert_eq!(settings.accepted_realms, AcceptedRealms::None);
        assert_eq!(settings.descriptor.verify(&config.realm_id), Ok(()));
    }

    #[tokio::test]
    async fn keeps_stored_settings() {
        // A restart never replaces settings, also a cleared URL and a disabled registration.
        let (_dir, config, ctx) = node(&[]).await;
        let cleared = SetFederationConfig {
            actor: node_actor(&config),
            auth_context: None,
            node_capabilities: config.node_capabilities.clone(),
            name: "Chosen name".to_string(),
            api_url: Url::parse("https://api.example.org/api/v1").unwrap(),
            portal_url: Url::parse("https://portal.example.org/").unwrap(),
            registry_url: None,
            registration: RegistrationMode::Disabled,
            accepted_realms: AcceptedRealms::None,
            expected: None,
            now: 1,
        };
        drive(SetFederationOperation::new(cleared), &ctx)
            .await
            .unwrap();
        let before = stored(&config, &ctx).await;
        assert!(before.is_some());

        register_default(&config, &ctx).await;
        assert_eq!(stored(&config, &ctx).await, before);
    }

    #[tokio::test]
    async fn skips_each_reason() {
        // No registry, a missing URL, a public HTTP URL, or a URL the egress rules refuse never registers.
        let registry = Url::parse(REGISTRY).unwrap();
        let strict = EgressPolicy::strict();
        let denied = EgressPolicy::strict().with_deny(vec!["30.255.255.0/29".parse().unwrap()]);
        let cases = [
            (None, Some(API), Some(PORTAL), &strict, Skip::RegistryOff),
            (
                Some(&registry),
                None,
                Some(PORTAL),
                &strict,
                Skip::MissingUrl("API_PUBLIC_URL"),
            ),
            (
                Some(&registry),
                Some(API),
                None,
                &strict,
                Skip::MissingUrl("PORTAL_PUBLIC_URL"),
            ),
            (
                Some(&registry),
                Some("http://127.0.0.1:3000"),
                Some(PORTAL),
                &strict,
                Skip::PrivateUrl("API_PUBLIC_URL"),
            ),
            (
                Some(&registry),
                Some("http://30.255.255.1:3000"),
                Some(PORTAL),
                &strict,
                Skip::NotHttps("API_PUBLIC_URL"),
            ),
            (
                Some(&registry),
                Some(API),
                Some("https://10.0.0.5/"),
                &strict,
                Skip::PrivateUrl("PORTAL_PUBLIC_URL"),
            ),
            (
                Some(&registry),
                Some(API),
                Some("https://[::1]:8444/"),
                &strict,
                Skip::PrivateUrl("PORTAL_PUBLIC_URL"),
            ),
            (
                Some(&registry),
                Some(API),
                Some(PORTAL),
                &denied,
                Skip::PrivateUrl("API_PUBLIC_URL"),
            ),
        ];
        for (registry_url, api, portal, policy, reason) in cases {
            let result = entry_points(registry_url, api, portal, policy).await;
            assert_eq!(result, Err(reason));
        }
    }
}
