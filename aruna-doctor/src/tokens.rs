use crate::error::CliError;
use aruna::config::load;
use aruna_api::error::TokenError;
use aruna_api::routes::users::{GetTokenResponse, RegisterUserRequest, RegisterUserResponse};
use aruna_api::server_state::load_persisted_state;
use aruna_core::UserId;
use aruna_core::auth::{TOKEN_REVOCATION_LIST_KEY, TRUSTED_REALMS_LIST_KEY, bearer_token_hash};
use aruna_core::onboarding::{OnboardingMode, OnboardingSecret};
use aruna_core::structs::{Actor, OidcProviderConfig, RealmId, TokenClaims};
use aruna_operations::claim_initial_realm_admin::{
    ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
};
use aruna_operations::consume_onboarding_secret::{
    ConsumeOnboardingSecretInput, ConsumeOnboardingSecretOperation,
};
use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::inspect_onboarding_secret::{
    InspectOnboardingSecretInput, InspectOnboardingSecretOperation,
};
use aruna_operations::register_or_get_oidc_user::{
    RegisterOrGetOidcUserInput, RegisterOrGetOidcUserOperation,
};
use aruna_storage::storage;
use aruna_tasks::TaskHandle;
use base64::Engine;
use ed25519_dalek::VerifyingKey;
use ed25519_dalek::pkcs8::EncodePublicKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::{Signature, Verifier};
use jsonwebtoken::dangerous::insecure_decode;
use jsonwebtoken::{DecodingKey, Validation, decode};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use ulid::Ulid;

#[derive(Debug, Clone)]
struct OidcCliConfig {
    http_socket_addr: SocketAddr,
    oidc_providers: Vec<OidcProviderConfig>,
}

fn url_encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            b' ' => encoded.push('+'),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn oidc_password_grant_body(
    client_id: &str,
    username: &str,
    password: &str,
    scope: &str,
) -> String {
    [
        ("grant_type", "password"),
        ("client_id", client_id),
        ("username", username),
        ("password", password),
        ("scope", scope),
    ]
    .into_iter()
    .map(|(key, value)| {
        format!(
            "{}={}",
            url_encode_component(key),
            url_encode_component(value)
        )
    })
    .collect::<Vec<_>>()
    .join("&")
}

pub async fn create_local_bootstrap_token(
    oidc_username: String,
    oidc_password: String,
    oidc_scope: String,
    bootstrap_secret: String,
) -> Result<String, CliError> {
    let config = load_oidc_cli_config()?;
    let aruna_base_url = format!("http://{}", config.http_socket_addr);
    if config.oidc_providers.is_empty() {
        return create_direct_local_bootstrap_token(bootstrap_secret).await;
    }

    let oidc_token = create_oidc_token(oidc_username, oidc_password, oidc_scope, true).await?;

    exchange_bootstrap_token(
        &Client::new(),
        &aruna_base_url,
        &oidc_token,
        bootstrap_secret,
    )
    .await
}

async fn create_direct_local_bootstrap_token(bootstrap_secret: String) -> Result<String, CliError> {
    let (config, storage_handle) = load().await.map_err(Box::new)?;
    let driver_ctx = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(TaskHandle::new()),
    };

    let onboarding_secret = OnboardingSecret::decode(&bootstrap_secret)?;
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    let inspected = drive(
        InspectOnboardingSecretOperation::new(InspectOnboardingSecretInput {
            enrollment_id: onboarding_secret.enrollment_id,
            secret_hash: onboarding_secret.secret_hash(),
            now,
        }),
        &driver_ctx,
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;
    if inspected.mode != OnboardingMode::Local {
        return Err(
            std::io::Error::other("bootstrap secret is not a local onboarding secret").into(),
        );
    }

    let user_id = UserId::local(Ulid::new(), config.realm_id);
    drive(
        ConsumeOnboardingSecretOperation::new(ConsumeOnboardingSecretInput {
            enrollment_id: onboarding_secret.enrollment_id,
            secret_hash: onboarding_secret.secret_hash(),
            node_id: user_id.to_string(),
            now,
        }),
        &driver_ctx,
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    let actor = Actor {
        node_id: config.node_id,
        user_id,
        realm_id: config.realm_id,
    };
    let user = drive(
        RegisterOrGetOidcUserOperation::new(RegisterOrGetOidcUserInput {
            actor: actor.clone(),
            issuer: "aruna-local-bootstrap".to_string(),
            subject_id: onboarding_secret.enrollment_id.to_string(),
            name: "local-admin".to_string(),
            user_id,
        }),
        &driver_ctx,
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    drive(
        ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
            actor: Actor {
                user_id: user.user_id,
                ..actor
            },
        }),
        &driver_ctx,
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    let token = drive(
        CreateTokenOperation::new(CreateTokenConfig {
            time: now,
            expiry: None,
            user_id: user.user_id,
            realm_id: config.realm_id,
            node_capabilities: config.node_capabilities,
        })?,
        &driver_ctx,
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    Ok(token)
}

pub async fn create_oidc_token(
    username: String,
    password: String,
    scope: String,
    oidc_only: bool,
) -> Result<String, CliError> {
    let config = load_oidc_cli_config()?;
    let provider = config
        .oidc_providers
        .into_iter()
        .next()
        .ok_or_else(|| CliError::OidcProviderNotFound("No OIDC configured".to_string()))?;
    let aruna_base_url = format!("http://{}", config.http_socket_addr);

    let client = Client::builder().build()?;
    let oidc_token = request_oidc_token(&client, &provider, &username, &password, &scope).await?;
    if !oidc_only {
        exchange_oidc_token(&client, &aruna_base_url, &oidc_token).await
    } else {
        Ok(oidc_token)
    }
}

fn load_oidc_cli_config() -> Result<OidcCliConfig, CliError> {
    let _ = dotenvy::dotenv();
    let http_socket_addr = SocketAddr::from_str(&dotenvy::var("SOCKET_ADDRESS")?)?;

    Ok(OidcCliConfig {
        http_socket_addr,
        oidc_providers: load_oidc_providers_from_env()?,
    })
}

fn load_oidc_providers_from_env() -> Result<Vec<OidcProviderConfig>, CliError> {
    let Some(provider_ids) = dotenvy::var("OIDC_PROVIDER_IDS").ok() else {
        return Ok(Vec::new());
    };

    provider_ids
        .split(',')
        .map(str::trim)
        .filter(|provider_id| !provider_id.is_empty())
        .map(|provider_id: &str| {
            let env_prefix = provider_id.to_ascii_uppercase().replace('-', "_");
            Ok(OidcProviderConfig {
                id: provider_id.to_string(),
                issuer: dotenvy::var(format!("OIDC_{env_prefix}_ISSUER"))?,
                audience: dotenvy::var(format!("OIDC_{env_prefix}_AUDIENCE"))?,
                discovery_url: dotenvy::var(format!("OIDC_{env_prefix}_DISCOVERY_URL"))?,
            })
        })
        .collect()
}

async fn request_oidc_token(
    client: &Client,
    provider: &OidcProviderConfig,
    username: &str,
    password: &str,
    scope: &str,
) -> Result<String, CliError> {
    let discovery = client
        .get(&provider.discovery_url)
        .send()
        .await?
        .error_for_status()?
        .json::<OidcDiscoveryDocument>()
        .await?;

    let response = client
        .post(discovery.token_endpoint)
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .body(oidc_password_grant_body(
            &provider.audience,
            username,
            password,
            scope,
        ))
        .send()
        .await?
        .error_for_status()?
        .json::<OidcTokenResponse>()
        .await?;

    Ok(response.id_token.unwrap_or(response.access_token))
}

async fn exchange_oidc_token(
    client: &Client,
    aruna_base_url: &str,
    oidc_token: &str,
) -> Result<String, CliError> {
    // This returns a user if it already exists
    let _response = client
        .post(format!("{aruna_base_url}/api/v1/users/register"))
        .bearer_auth(oidc_token)
        .json(&RegisterUserRequest {
            onboarding_secret: None,
        })
        .send()
        .await?
        .error_for_status()?
        .json::<RegisterUserResponse>()
        .await?;

    let response = client
        .get(format!("{aruna_base_url}/api/v1/users/token"))
        .bearer_auth(oidc_token)
        .send()
        .await?
        .error_for_status()?
        .json::<GetTokenResponse>()
        .await?;

    Ok(response.token)
}

async fn exchange_bootstrap_token(
    client: &Client,
    aruna_base_url: &str,
    oidc_token: &str,
    onboarding_secret: String,
) -> Result<String, CliError> {
    let _response = client
        .post(format!("{aruna_base_url}/api/v1/users/register"))
        .bearer_auth(oidc_token)
        .json(&RegisterUserRequest {
            onboarding_secret: Some(onboarding_secret),
        })
        .send()
        .await?
        .error_for_status()?
        .json::<RegisterUserResponse>()
        .await?;

    let response = client
        .get(format!("{aruna_base_url}/api/v1/users/token"))
        .bearer_auth(oidc_token)
        .send()
        .await?
        .error_for_status()?
        .json::<GetTokenResponse>()
        .await?;
    Ok(response.token)
}

#[derive(Debug, Deserialize)]
struct OidcDiscoveryDocument {
    token_endpoint: String,
}

#[derive(Debug, Deserialize)]
struct OidcTokenResponse {
    access_token: String,
    #[serde(default)]
    id_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenView {
    pub claims: TokenClaims,
    pub is_blacklisted: bool,
    pub valid: Valid,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Valid {
    True,
    False { reason: String },
}

pub async fn view_token(token: String) -> Result<String, CliError> {
    let hash = bearer_token_hash(&token);
    let unvalidated_claims = insecure_decode::<TokenClaims>(&token)?;

    let (config, _) = load().await.unwrap();
    let storage_handle = storage::FjallStorage::open(&config.storage_path).unwrap();

    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
    });
    let persisted_blacklist = load_persisted_state::<HashSet<String, ahash::RandomState>>(
        &driver_ctx,
        TOKEN_REVOCATION_LIST_KEY,
    )
    .await
    .unwrap_or_default();

    let blacklisted = persisted_blacklist.contains(&hash);

    let trusted_realms = load_persisted_state::<HashSet<RealmId, ahash::RandomState>>(
        &driver_ctx,
        TRUSTED_REALMS_LIST_KEY,
    )
    .await
    .unwrap_or_default();

    let valid = match validate(unvalidated_claims.claims.clone(), token, trusted_realms).await {
        Ok(_) => Valid::True,
        Err(err) => Valid::False {
            reason: err.to_string(),
        },
    };

    let token_view = TokenView {
        claims: unvalidated_claims.claims,
        is_blacklisted: blacklisted,
        valid,
    };

    Ok(serde_json::to_string_pretty(&token_view)?)
}

async fn validate(
    claims: TokenClaims,
    token: String,
    trusted_realms: HashSet<RealmId, ahash::RandomState>,
) -> Result<(), TokenError> {
    let claims = match (claims.issuer_pubkey, claims.delegation_signature.is_some()) {
        (Some(issuer), true) => {
            let issuer_pubkey: [u8; 32] = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(issuer.clone())?
                .try_into()
                .map_err(|_| TokenError::InvalidIssuerKey)?;
            let pub_pem_key = VerifyingKey::from_bytes(&issuer_pubkey)?
                .to_public_key_pem(LineEnding::default())?;
            let decoding_key = DecodingKey::from_ed_pem(pub_pem_key.as_bytes())?;

            decode::<TokenClaims>(
                token,
                &decoding_key,
                &Validation::new(jsonwebtoken::Algorithm::EdDSA),
            )?
        }
        (_, _) => {
            let issuer_pubkey: [u8; 32] = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(claims.iss.clone())?
                .try_into()
                .map_err(|_| TokenError::InvalidIssuerKey)?;
            let pub_pem_key = VerifyingKey::from_bytes(&issuer_pubkey)?
                .to_public_key_pem(LineEnding::default())?;
            let decoding_key = DecodingKey::from_ed_pem(pub_pem_key.as_bytes())?;

            decode::<TokenClaims>(
                token,
                &decoding_key,
                &Validation::new(jsonwebtoken::Algorithm::EdDSA),
            )?
        }
    };

    let now = chrono::Utc::now().timestamp() as u64;
    if now > claims.claims.exp {
        return Err(TokenError::Expired);
    }

    if !trusted_realms.contains(
        &RealmId::from_base64(&claims.claims.iss).map_err(|_| TokenError::InvalidIssuerKey)?,
    ) {
        return Err(TokenError::RealmNotTrusted);
    }

    // Check server token claims
    match (
        &claims.claims.delegation_signature,
        &claims.claims.issuer_pubkey,
    ) {
        (Some(delegation_token), Some(issuer_pubkey)) => {
            // Check delegation signature
            let realm_key =
                base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(&claims.claims.iss)?;
            let realm_verifying_key = VerifyingKey::from_bytes(realm_key.as_slice().try_into()?)?;
            let signature = Signature::from_str(delegation_token)?;
            realm_verifying_key.verify(issuer_pubkey.as_bytes(), &signature)?;
            Ok(())
        }
        (None, None) => Ok(()),
        (_, _) => Err(TokenError::InvalidServerToken),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        create_local_bootstrap_token, create_oidc_token, load_oidc_providers_from_env,
        oidc_password_grant_body, request_oidc_token,
    };
    use crate::test_support::env_lock;
    use aruna::bootstrap::ensure_initial_local_onboarding_secret;
    use aruna_api::auth::OidcValidator;
    use aruna_api::routes::onboarding::ListOnboardingSecretsResponse;
    use aruna_api::server::{Server, ServerConfig};
    use aruna_api::server_state::ServerState;
    use aruna_core::UserId;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::{REALM_CONFIG_KEYSPACE, USER_KEYSPACE};
    use aruna_core::structs::{
        Actor, NodeCapabilities, OidcProviderConfig, RealmConfigDocument, TokenClaims, User,
    };
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_operations::announce_realm_presence::{
        AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
    };
    use aruna_operations::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_operations::incoming::initialize_net_incoming;
    use aruna_operations::task_incoming::initialize_task_incoming;
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use axum::extract::State;
    use axum::routing::{get, post};
    use axum::{Form, Json, Router};
    use base64::Engine;
    use byteview::ByteView;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::EncodePrivateKey;
    use jsonwebtoken::{Algorithm, EncodingKey, Header, dangerous::insecure_decode};
    use reqwest::Client;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use ulid::Ulid;

    fn restore_env(previous: Vec<(String, Option<String>)>) {
        for (key, value) in previous {
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }

    #[derive(Clone)]
    struct OidcTestState {
        discovery_url: String,
        expected_client_id: String,
        expected_username: String,
        expected_password: String,
        issued_id_token: String,
    }

    #[derive(Deserialize)]
    struct TokenForm {
        grant_type: String,
        client_id: String,
        username: String,
        password: String,
        scope: String,
    }

    #[derive(Clone)]
    struct OidcProviderState {
        issuer: String,
        token_endpoint: String,
        jwks_uri: String,
        jwks: serde_json::Value,
        token: String,
    }

    #[derive(Clone, Serialize, Deserialize)]
    struct TestOidcClaims {
        sub: String,
        iss: String,
        aud: String,
        exp: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    }

    struct TestNode {
        _temp_dir: TempDir,
        base_url: String,
        context: Arc<DriverContext>,
        net: NetHandle,
        server_task: JoinHandle<()>,
    }

    async fn discovery(
        State(state): State<Arc<OidcTestState>>,
    ) -> Json<HashMap<&'static str, String>> {
        Json(HashMap::from([(
            "token_endpoint",
            format!("{}/token", state.discovery_url),
        )]))
    }

    async fn token(
        State(state): State<Arc<OidcTestState>>,
        Form(form): Form<TokenForm>,
    ) -> Json<HashMap<&'static str, String>> {
        assert_eq!(form.grant_type, "password");
        assert_eq!(form.client_id, state.expected_client_id);
        assert_eq!(form.username, state.expected_username);
        assert_eq!(form.password, state.expected_password);
        assert_eq!(form.scope, "openid profile");

        Json(HashMap::from([
            ("access_token", "oidc-access-token".to_string()),
            ("id_token", state.issued_id_token.clone()),
        ]))
    }

    async fn oidc_discovery(State(state): State<OidcProviderState>) -> Json<serde_json::Value> {
        Json(serde_json::json!({
            "issuer": state.issuer,
            "jwks_uri": state.jwks_uri,
            "token_endpoint": state.token_endpoint,
        }))
    }

    async fn oidc_jwks(State(state): State<OidcProviderState>) -> Json<serde_json::Value> {
        Json(state.jwks)
    }

    async fn oidc_password_token(
        State(state): State<OidcProviderState>,
        Form(form): Form<TokenForm>,
    ) -> Json<HashMap<&'static str, String>> {
        assert_eq!(form.grant_type, "password");
        assert_eq!(form.client_id, "aruna-api");
        assert_eq!(form.username, "alice");
        assert_eq!(form.password, "alice-password");
        assert_eq!(form.scope, "openid profile");

        Json(HashMap::from([
            ("access_token", state.token.clone()),
            ("id_token", state.token.clone()),
        ]))
    }

    async fn spawn_oidc_provider(
        issuer: &str,
        kid: &str,
        signing_key: &SigningKey,
        token: String,
    ) -> (OidcProviderConfig, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let token_endpoint = format!("{base_url}/token");
        let jwks_uri = format!("{base_url}/jwks.json");
        let discovery_url = format!("{base_url}/.well-known/openid-configuration");
        let jwks = serde_json::json!({
            "keys": [{
                "kty": "OKP",
                "alg": "EdDSA",
                "use": "sig",
                "kid": kid,
                "crv": "Ed25519",
                "x": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signing_key.verifying_key().to_bytes()),
            }]
        });
        let router = Router::new()
            .route("/.well-known/openid-configuration", get(oidc_discovery))
            .route("/jwks.json", get(oidc_jwks))
            .route("/token", post(oidc_password_token))
            .with_state(OidcProviderState {
                issuer: issuer.to_string(),
                token_endpoint,
                jwks_uri: jwks_uri.clone(),
                jwks,
                token,
            });
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        (
            OidcProviderConfig {
                id: "main".to_string(),
                issuer: issuer.to_string(),
                audience: "aruna-api".to_string(),
                discovery_url,
            },
            task,
        )
    }

    fn sign_oidc_token(
        issuer: &str,
        kid: &str,
        signing_key: &SigningKey,
        subject: &str,
        name: Option<&str>,
    ) -> String {
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(kid.to_string());
        let claims = TestOidcClaims {
            sub: subject.to_string(),
            iss: issuer.to_string(),
            aud: "aruna-api".to_string(),
            exp: chrono::Utc::now().timestamp().max(0) as u64 + 600,
            name: name.map(str::to_string),
        };
        let key_pem = signing_key
            .to_pkcs8_pem(ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF)
            .unwrap();
        jsonwebtoken::encode(
            &header,
            &claims,
            &EncodingKey::from_ed_pem(key_pem.as_bytes()).unwrap(),
        )
        .unwrap()
    }

    async fn read_user(context: &DriverContext, user_id: UserId) -> User {
        match context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: USER_KEYSPACE.to_string(),
                key: ByteView::from(user_id.to_bytes()),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => User::from_bytes(&bytes).unwrap(),
            other => panic!("unexpected user read result: {other:?}"),
        }
    }

    async fn read_realm_config(
        context: &DriverContext,
        realm_id: &aruna_core::structs::RealmId,
    ) -> RealmConfigDocument {
        match context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: ByteView::from(realm_id.as_bytes().to_vec()),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => RealmConfigDocument::from_bytes(&bytes).unwrap(),
            other => panic!("unexpected realm config read result: {other:?}"),
        }
    }

    async fn spawn_test_node(provider: OidcProviderConfig, claim_admin: bool) -> TestNode {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(temp_dir.path().to_str().unwrap()).unwrap();
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .unwrap();
        let task_handle = TaskHandle::new();
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(task_handle.clone()),
        });
        initialize_net_incoming(context.clone());
        initialize_task_incoming(context.clone(), task_handle).await;

        let realm_signing_key =
            SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let realm_id =
            aruna_core::structs::RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let capabilities = NodeCapabilities::management_node(realm_signing_key).unwrap();
        let bootstrap_user = UserId::local(Ulid::new(), realm_id);
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: Actor {
                    node_id: net.node_id(),
                    user_id: bootstrap_user,
                    realm_id,
                },
                realm_description: "Test Realm".to_string(),
                oidc_providers: Vec::new(),
            }),
            context.as_ref(),
        )
        .await
        .unwrap();
        if claim_admin {
            drive(
                ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                    actor: Actor {
                        node_id: net.node_id(),
                        user_id: bootstrap_user,
                        realm_id,
                    },
                }),
                context.as_ref(),
            )
            .await
            .unwrap();
        }
        drive(
            AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
                realm_id,
                node_id: net.node_id(),
                schedule_refresh: false,
            }),
            context.as_ref(),
        )
        .await
        .unwrap();

        let mut config = read_realm_config(context.as_ref(), &realm_id).await;
        config.oidc_providers.push(provider);
        match context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: ByteView::from(realm_id.as_bytes().to_vec()),
                value: ByteView::from(
                    config
                        .to_bytes(&Actor {
                            node_id: net.node_id(),
                            user_id: UserId::nil(realm_id),
                            realm_id,
                        })
                        .unwrap(),
                ),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected realm config write result: {other:?}"),
        }

        let state = Arc::new(
            ServerState::new(
                context.clone(),
                realm_id,
                net.node_id(),
                capabilities.clone(),
                false,
                Some(Arc::new(OidcValidator::new().unwrap())),
            )
            .await,
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = Server::new(state, ServerConfig { http_addr: addr }).build_router();
        let server_task = tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });

        TestNode {
            _temp_dir: temp_dir,
            context,
            base_url: format!("http://{addr}"),
            net,
            server_task,
        }
    }

    fn set_oidc_env(
        base_url: &str,
        provider: &OidcProviderConfig,
    ) -> Vec<(String, Option<String>)> {
        let vars = [
            (
                "SOCKET_ADDRESS",
                base_url.trim_start_matches("http://").to_string(),
            ),
            ("OIDC_PROVIDER_IDS", provider.id.clone()),
            ("OIDC_MAIN_ISSUER", provider.issuer.clone()),
            ("OIDC_MAIN_AUDIENCE", provider.audience.clone()),
            ("OIDC_MAIN_DISCOVERY_URL", provider.discovery_url.clone()),
        ];
        let previous: Vec<_> = vars
            .iter()
            .map(|(key, _)| ((*key).to_string(), std::env::var(key).ok()))
            .collect();
        for (key, value) in &vars {
            unsafe { std::env::set_var(key, value) };
        }
        previous
    }

    fn decode_token_claims(token: &str) -> TokenClaims {
        insecure_decode::<TokenClaims>(token).unwrap().claims
    }

    async fn assert_regular_token_cannot_manage_onboarding(
        node: &TestNode,
        token: &str,
    ) -> Result<(), Box<dyn Error>> {
        let response = reqwest::Client::new()
            .get(format!("{}/api/v1/admin/onboarding/secrets", node.base_url))
            .bearer_auth(token)
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
        Ok(())
    }

    async fn list_onboarding_with_token(
        node: &TestNode,
        token: &str,
    ) -> Result<reqwest::StatusCode, Box<dyn Error>> {
        let response = reqwest::Client::new()
            .get(format!("{}/api/v1/admin/onboarding/secrets", node.base_url))
            .bearer_auth(token)
            .send()
            .await?;
        let status = response.status();
        if status == reqwest::StatusCode::OK {
            let body: ListOnboardingSecretsResponse = response.json().await?;
            assert!(!body.secrets.is_empty());
        }
        Ok(status)
    }

    #[tokio::test]
    async fn requests_oidc_token_via_discovery_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let state = Arc::new(OidcTestState {
            discovery_url: base_url.clone(),
            expected_client_id: "aruna-api".to_string(),
            expected_username: "alice".to_string(),
            expected_password: "alice-password".to_string(),
            issued_id_token: "oidc-id-token".to_string(),
        });
        let router = Router::new()
            .route("/.well-known/openid-configuration", get(discovery))
            .route("/token", post(token))
            .with_state(state);
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let provider = OidcProviderConfig {
            id: "main".to_string(),
            issuer: "http://issuer.example".to_string(),
            audience: "aruna-api".to_string(),
            discovery_url: format!("{base_url}/.well-known/openid-configuration"),
        };
        let token = request_oidc_token(
            &Client::new(),
            &provider,
            "alice",
            "alice-password",
            "openid profile",
        )
        .await
        .unwrap();

        assert_eq!(token, "oidc-id-token");
        server.abort();
    }

    #[tokio::test]
    async fn falls_back_to_access_token_when_id_token_is_missing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let state = Arc::new(OidcTestState {
            discovery_url: base_url.clone(),
            expected_client_id: "aruna-api".to_string(),
            expected_username: "alice".to_string(),
            expected_password: "alice-password".to_string(),
            issued_id_token: "unused-id-token".to_string(),
        });
        let router = Router::new()
            .route("/.well-known/openid-configuration", get(discovery))
            .route(
                "/token",
                post(
                    |State(state): State<Arc<OidcTestState>>, Form(form): Form<TokenForm>| async move {
                        assert_eq!(form.client_id, state.expected_client_id);
                        Json(HashMap::from([(
                            "access_token",
                            "oidc-access-token".to_string(),
                        )]))
                    },
                ),
            )
            .with_state(state);
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let provider = OidcProviderConfig {
            id: "main".to_string(),
            issuer: "http://issuer.example".to_string(),
            audience: "aruna-api".to_string(),
            discovery_url: format!("{base_url}/.well-known/openid-configuration"),
        };
        let token = request_oidc_token(
            &Client::new(),
            &provider,
            "alice",
            "alice-password",
            "openid profile",
        )
        .await
        .unwrap();

        assert_eq!(token, "oidc-access-token");
        server.abort();
    }

    #[test]
    fn oidc_password_grant_body_encodes_form_values() {
        let body = oidc_password_grant_body(
            "aruna-api",
            "alice@example.com",
            "p@ss word",
            "openid profile",
        );

        assert_eq!(
            body,
            "grant_type=password&client_id=aruna-api&username=alice%40example.com&password=p%40ss+word&scope=openid+profile"
        );
    }

    #[tokio::test]
    async fn loads_oidc_providers_from_environment() {
        let _guard = env_lock().lock().await;
        let vars = [
            ("OIDC_PROVIDER_IDS", "main".to_string()),
            ("OIDC_MAIN_ISSUER", "https://issuer.example".to_string()),
            ("OIDC_MAIN_AUDIENCE", "aruna-api".to_string()),
            (
                "OIDC_MAIN_DISCOVERY_URL",
                "https://issuer.example/.well-known/openid-configuration".to_string(),
            ),
        ];
        let previous: Vec<_> = vars
            .iter()
            .map(|(key, _)| ((*key).to_string(), std::env::var(key).ok()))
            .collect();
        for (key, value) in &vars {
            unsafe { std::env::set_var(key, value) };
        }

        let providers = load_oidc_providers_from_env().unwrap();

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, "main");
        assert_eq!(providers[0].audience, "aruna-api");

        restore_env(previous);
    }

    #[tokio::test]
    async fn create_oidc_token_registers_user_and_returns_aruna_token() -> Result<(), Box<dyn Error>>
    {
        let _guard = env_lock().lock().await;
        let issuer = "https://issuer.example";
        let kid = "main-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let oidc_token = sign_oidc_token(issuer, kid, &signing_key, "subject-123", Some("Alice"));
        let (provider, oidc_task) =
            spawn_oidc_provider(issuer, kid, &signing_key, oidc_token).await;
        let node = spawn_test_node(provider.clone(), true).await;
        let previous = set_oidc_env(&node.base_url, &provider);

        let token = create_oidc_token(
            "alice".to_string(),
            "alice-password".to_string(),
            "openid profile".to_string(),
            false,
        )
        .await?;

        let claims = decode_token_claims(&token);
        let user_id = UserId::from_string(&claims.sub).unwrap();
        let user = read_user(node.context.as_ref(), user_id).await;
        assert_eq!(user.name, "Alice");
        assert_regular_token_cannot_manage_onboarding(&node, &token).await?;

        restore_env(previous);
        node.server_task.abort();
        node.net.shutdown().await;
        oidc_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn create_local_bootstrap_token_claims_initial_admin_and_returns_aruna_token()
    -> Result<(), Box<dyn Error>> {
        let _guard = env_lock().lock().await;
        let issuer = "https://issuer.example";
        let kid = "main-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let oidc_token = sign_oidc_token(issuer, kid, &signing_key, "subject-admin", Some("Admin"));
        let (provider, oidc_task) =
            spawn_oidc_provider(issuer, kid, &signing_key, oidc_token).await;
        let node = spawn_test_node(provider.clone(), false).await;
        let onboarding_secret =
            ensure_initial_local_onboarding_secret(node.context.as_ref(), node.base_url.clone())
                .await?
                .encode()?;
        let previous = set_oidc_env(&node.base_url, &provider);

        let token = create_local_bootstrap_token(
            "alice".to_string(),
            "alice-password".to_string(),
            "openid profile".to_string(),
            onboarding_secret,
        )
        .await?;

        let claims = decode_token_claims(&token);
        let user_id = UserId::from_string(&claims.sub).unwrap();
        let user = read_user(node.context.as_ref(), user_id).await;
        assert_eq!(user.name, "Admin");
        let status = list_onboarding_with_token(&node, &token).await?;
        assert!(matches!(
            status,
            reqwest::StatusCode::OK | reqwest::StatusCode::FORBIDDEN
        ));

        restore_env(previous);
        node.server_task.abort();
        node.net.shutdown().await;
        oidc_task.abort();
        Ok(())
    }
}
