use crate::explorer::ExplorerError;
use crate::storage::SnapshotError;
use aruna::config::SetupError;
use aruna::portal::PortalArtifactError;
use aruna_core::onboarding::OnboardingSecretError;
use aruna_operations::create_token::CreateTokenError;
use aruna_storage::errors::StorageLibError;
use thiserror::Error;
use tokio::task::JoinError;

#[derive(Error, Debug)]
pub enum CliError {
    #[error(transparent)]
    UlidConversion(#[from] ulid::DecodeError),
    #[error(transparent)]
    CreateTokenError(#[from] CreateTokenError),
    #[error(transparent)]
    JwtTokenError(#[from] jsonwebtoken::errors::Error),
    #[error(transparent)]
    Base64DecodeError(#[from] base64::DecodeError),
    #[error("Cannot convert Vec into Slice")]
    IntoSliceError,
    #[error(transparent)]
    Ed25519Error(#[from] ed25519_dalek::ed25519::Error),
    #[error(transparent)]
    PCKSError(#[from] ed25519_dalek::pkcs8::Error),
    #[error(transparent)]
    SPKIError(#[from] ed25519_dalek::pkcs8::spki::Error),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),
    #[error("request to {url} failed")]
    InfoRequest {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("iroh check failed during {step}: {message}")]
    IrohCheck { step: &'static str, message: String },
    #[error(transparent)]
    DotenvError(#[from] dotenvy::Error),
    #[error(transparent)]
    OnboardingSecretError(#[from] OnboardingSecretError),
    #[error(transparent)]
    AddrParseError(#[from] std::net::AddrParseError),
    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error(transparent)]
    SetupError(#[from] Box<SetupError>),
    #[error(transparent)]
    FjallError(#[from] fjall::Error),
    #[error(transparent)]
    StorageError(#[from] StorageLibError),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    SnapshotError(#[from] SnapshotError),
    #[error(transparent)]
    TokioJoinError(#[from] JoinError),
    #[error(transparent)]
    ExplorerError(#[from] ExplorerError),
    #[error(transparent)]
    PortalArtifactError(#[from] PortalArtifactError),
    #[error("portal config value {0} is required")]
    MissingPortalConfig(&'static str),
    #[error("no prerelease in {repo} contains portal artifact {asset}")]
    MissingPortalWebsiteArtifact {
        repo: &'static str,
        asset: &'static str,
    },
    #[error("OIDC provider '{0}' is not configured")]
    OidcProviderNotFound(String),
    #[error("OIDC flow requires both --oidc-username and --oidc-password")]
    MissingOidcCredentials,
    #[error("OIDC flow cannot be combined with positional user_id or expiry")]
    InvalidOidcCreateTokenArgs,
    #[error("OIDC flow requires local Aruna HTTP address to be configured")]
    MissingArunaHttpAddress,
    #[error("Local bootstrap flow requires --name")]
    MissingBootstrapName,
    #[error("No initial local onboarding secret is available")]
    MissingInitialOnboardingSecret,
    #[error("Arbitrary user ids require --unsafe-arbitrary-user-id")]
    UnsafeUserIdRequired,
    #[error("invalid {key} value {value:?}: {message}")]
    InvalidConfigValue {
        key: &'static str,
        value: String,
        message: String,
    },
}
