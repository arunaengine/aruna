use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use base64::engine::general_purpose;
use base64::Engine;
use chrono::Utc;
use diesel_ulid::DieselUlid;
use hmac::{Hmac, Mac};
use jsonwebtoken::encode;
use jsonwebtoken::Algorithm;
use jsonwebtoken::DecodingKey;
use jsonwebtoken::EncodingKey;
use jsonwebtoken::Header;
use log::error;
use serde::Deserializer;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::error::Error;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::RwLock;

use crate::caching::cache::Cache;
use crate::caching::structs::PubKeyEnum;
use crate::database::connection::Database;
use crate::database::dsls::pub_key_dsl::PubKey as DbPubKey;
use crate::database::dsls::user_dsl::OIDCMapping;
use crate::database::enums::DbPermissionLevel;

use super::issuer_handler::IssuerType;

#[derive(Debug)]
pub enum OIDCError {
    NotFound(String),
}
impl Display for OIDCError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OIDCError::NotFound(err) => write!(f, "{err}"),
        }
    }
}
impl Error for OIDCError {}
/// This contains claims for ArunaTokens
/// containing 3 mandatory and 2 optional fields.
///
/// - iss: Token issuer which is currently 'aruna' everytime
/// - sub: User_ID or Endpoint_ID
/// - tid: UUID from the specific token
/// - exp: When this token expires (by default very large number)
/// - intent: Combination of specific endpoint and action.
///           Strongly restricts the usability of the token.
#[derive(Debug, Serialize, Deserialize)]
pub struct ArunaTokenClaims {
    pub iss: String, // Currently always 'aruna'
    pub sub: String, // User_ID / DataProxy_ID
    #[serde(skip_serializing_if = "Option::is_none")]
    aud: Option<Audience>, // Audience;
    exp: usize,      // Expiration timestamp
    // Token_ID; None if OIDC or ... ?
    #[serde(skip_serializing_if = "Option::is_none")]
    tid: Option<String>,
    // Intent: <endpoint-ulid>_<action>
    #[serde(skip_serializing_if = "Option::is_none")]
    it: Option<Intent>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
#[serde(untagged)]
enum Audience {
    String(String),
    Vec(Vec<String>),
}

#[repr(u8)]
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum Action {
    All = 0,
    CreateSecrets = 1,
    Impersonate = 2,
    FetchInfo = 3,
    //DpExchange = 4,
}
#[derive(Clone, Debug, Serialize)]
pub struct ProcessedToken {
    pub main_id: DieselUlid,       // User/Proxy Id
    pub token: Option<DieselUlid>, // Maybe token
    pub is_personal: bool,
    pub user_permissions: Vec<(DieselUlid, DbPermissionLevel)>,
    pub is_proxy: bool,
    pub proxy_intent: Option<Intent>,
}
impl From<u8> for Action {
    fn from(input: u8) -> Self {
        match input {
            0 => Action::All,
            1 => Action::CreateSecrets,
            2 => Action::Impersonate,
            3 => Action::FetchInfo,
            _ => panic!("Invalid action"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Intent {
    pub target: DieselUlid,
    pub action: Action,
}
type HmacSha256 = Hmac<Sha256>;

impl Serialize for Intent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer
            .serialize_str(format!("{}_{:?}", self.target, self.action.clone() as u8).as_str())
    }
}

impl Serialize for Action {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(format!("{:?}", self.clone() as u8).as_str())
    }
}

impl<'de> Deserialize<'de> for Intent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let temp = String::deserialize(deserializer)?;
        let split = temp.split('_').collect::<Vec<&str>>();

        Ok(Intent {
            target: DieselUlid::from_str(split[0])
                .map_err(|_| serde::de::Error::custom("Invalid UUID"))?,
            action: u8::from_str(split[1])
                .map_err(|_| serde::de::Error::custom("Invalid Action"))?
                .into(),
        })
    }
}

impl<'de> Deserialize<'de> for Action {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let deserialized_string = String::deserialize(deserializer)?;
        let action_byte = u8::from_str(&deserialized_string)
            .map_err(|_| serde::de::Error::custom("Conversion to u8 failed"))?;

        Ok(Action::from(action_byte))
    }
}

pub struct TokenHandler {
    cache: Arc<Cache>,
    signing_info: Arc<RwLock<(i16, EncodingKey, DecodingKey)>>, //<PublicKey Serial; PrivateKey; PublicKey>
}

impl TokenHandler {
    pub async fn new(
        cache: Arc<Cache>,
        database: Arc<Database>,
        encode_secret: String,
        decode_secret: String,
    ) -> Result<Self> {
        let private_pem =
            format!("-----BEGIN PRIVATE KEY-----{encode_secret}-----END PRIVATE KEY-----");
        let public_pem =
            format!("-----BEGIN PUBLIC KEY-----{decode_secret}-----END PUBLIC KEY-----");

        // Read encoding and decoding key; On error panic, we do not want malformed keys.
        let encoding_key = EncodingKey::from_ed_pem(private_pem.as_bytes())?;
        let decoding_key = DecodingKey::from_ed_pem(public_pem.as_bytes())?;

        // Check if public key already exists in database/cache
        let pubkey_serial = if let Some(key_serial) = cache.get_pubkey_serial(&decode_secret) {
            key_serial
        } else {
            // Add public key to database and cache
            let client = database.get_client().await?;
            let pub_key = DbPubKey::create_or_get_without_id(None, &decode_secret, &client).await?;

            cache.add_pubkey(
                pub_key.id,
                PubKeyEnum::Server((decode_secret, decoding_key.clone())), //ToDo: Server ID?
            );

            // Notification --> Announcement::PubKey::New?

            pub_key.id
        };

        // Return initialized TokenHandler
        Ok(TokenHandler {
            cache,
            signing_info: Arc::new(RwLock::new((pubkey_serial, encoding_key, decoding_key))),
        })
    }

    ///ToDo: Rust Doc
    pub fn get_current_pubkey_serial(&self) -> i16 {
        // Gets the signing key info -> if this returns a poison error this should also panic
        // We dont want to allow poisoned / malformed encoding keys and must crash at this point
        let signing_key = self.signing_info.read().unwrap();
        signing_key.0
    }

    ///ToDo: Rust Doc
    pub fn sign_user_token(
        &self,
        user_id: &DieselUlid,
        token_id: &DieselUlid,
        expires_at: Option<prost_wkt_types::Timestamp>,
    ) -> Result<String> {
        // Gets the signing key -> if this returns a poison error this should also panic
        // We dont want to allow poisoned / malformed encoding keys and must crash at this point
        let signing_key = self.signing_info.read().unwrap();

        let claims = ArunaTokenClaims {
            iss: "aruna".to_string(),
            sub: user_id.to_string(),
            exp: if let Some(expiration) = expires_at {
                expiration.seconds as usize
            } else {
                // Add 10 years to token lifetime if  expiry unspecified
                (Utc::now().timestamp() as usize) + 315360000
            },
            tid: Some(token_id.to_string()),
            it: None,
            aud: Some(Audience::String("aruna".to_string())),
        };

        let header = Header {
            kid: Some(format!("{}", signing_key.0)),
            alg: Algorithm::EdDSA,
            ..Default::default()
        };

        Ok(encode(&header, &claims, &signing_key.1)?)
    }

    /// Signing function to create a token that on lives only for a short period
    /// and is only applicable for a specific Endpoint. If an intent is provided
    /// the token is additionally restricted to the specific action.
    pub fn sign_dataproxy_slt(
        &self,
        user_id: &DieselUlid,     // User id of original
        token_id: Option<String>, // None if original request came with OIDC
        intent: Option<Intent>,   // Some Dataproxy action to restrict token usage scope
    ) -> Result<String> {
        // Gets the signing key -> if this returns a poison error this should also panic
        // We dont want to allow poisoned / malformed encoding keys and must crash at this point
        let signing_key = self.signing_info.read().unwrap();

        let claims = ArunaTokenClaims {
            iss: "aruna".to_string(),
            sub: user_id.to_string(),
            exp: (Utc::now().timestamp() as usize) + 86400, // One day for now.
            tid: token_id,
            it: intent,
            aud: Some(Audience::String("proxy".to_string())),
        };

        let header = Header {
            kid: Some(format!("{}", signing_key.0)),
            alg: Algorithm::EdDSA,
            ..Default::default()
        };

        Ok(encode(&header, &claims, &signing_key.1)?)
    }

    pub async fn process_token(&self, token: &str) -> Result<ProcessedToken> {
        let split = token
            .split('.')
            .nth(1)
            .ok_or_else(|| anyhow!("Invalid token"))?;
        let decoded = general_purpose::STANDARD_NO_PAD.decode(split)?;
        let claims: ArunaTokenClaims = serde_json::from_slice(&decoded)?;

        let issuer = self
            .cache
            .get_issuer(&claims.iss)
            .ok_or_else(|| anyhow!("Unknown issuer"))?;

        let (kid, validated_claims) = match issuer.check_token(token).await {
            Ok((kid, validated_claims)) => (kid, validated_claims),
            Err(e) => {
                error!("Possible invalid token: {e}");
                self.cache
                    .issuer_sender
                    .try_send(issuer.issuer_name.clone())?;
                bail!("Invalid token")
            }
        };

        match issuer.issuer_type {
            IssuerType::OIDC => self.validate_oidc_token(&validated_claims).await,
            IssuerType::ARUNA => self.validate_server_token(&validated_claims).await,
            IssuerType::DATAPROXY => self.validate_dataproxy_token(&validated_claims, &kid).await,
        }
    }

    ///ToDo: Rust Doc
    async fn validate_server_token(&self, claims: &ArunaTokenClaims) -> Result<ProcessedToken> {
        // Fetch user from cache
        let uid = DieselUlid::from_str(&claims.sub)?;
        let user = self.cache.get_user(&uid);

        // Convert token id if present
        let maybe_token = match &claims.tid {
            Some(token_id) => Some(DieselUlid::from_str(token_id)?),
            None => None,
        };

        // Fetch permissions associated with token
        if let Some(user) = user {
            let (perms, personal) = user.get_permissions(maybe_token)?;
            return Ok(ProcessedToken {
                main_id: user.id,
                token: maybe_token,
                is_personal: personal,
                user_permissions: perms,
                is_proxy: false,
                proxy_intent: None,
            });
        }
        bail!("Invalid user")
    }

    ///ToDo: Rust Doc
    async fn validate_dataproxy_token(
        &self,
        claims: &ArunaTokenClaims,
        kid: &str,
    ) -> Result<ProcessedToken> {
        // Fetch pubkey from cache
        let key = self
            .cache
            .get_pubkey(kid.parse::<i16>()?)
            .ok_or_else(|| anyhow!("Unspecified kid"))?
            .clone();

        // Check if pubkey is from ArunaServer or Dataproxy.
        match key {
            PubKeyEnum::DataProxy((_, _, endpoint_id)) => {
                // Intent is mandatory with Dataproxy signed tokens
                if let Some(intent) = &claims.it {
                    // Check if endpoint id matches the id associated with the pubkey
                    if endpoint_id != intent.target {
                        bail!("Invalid intent target id")
                    }

                    // Convert claims sub to ULID
                    let sub_id = DieselUlid::from_str(&claims.sub)?;

                    // Check if intent action is valid
                    match intent.action {
                        //Case 1: Dataproxy notification fetch
                        Action::FetchInfo => Ok(ProcessedToken {
                            main_id: sub_id,
                            token: None,
                            is_personal: false,
                            user_permissions: vec![],
                            is_proxy: true,
                            proxy_intent: Some(intent.clone()),
                        }),
                        //Case 2: Dataproxy user impersonation
                        Action::Impersonate => {
                            // Fetch user from cache
                            let user = self.cache.get_user(&sub_id);

                            // Convert token id if present
                            let token = match &claims.tid {
                                Some(token_id) => Some(DieselUlid::from_str(token_id)?),
                                None => None,
                            };

                            // Fetch permissions associated with token
                            if let Some(user) = user {
                                let perms = user.get_permissions(token)?;
                                return Ok(ProcessedToken {
                                    main_id: user.id,
                                    token,
                                    is_personal: false,
                                    user_permissions: perms.0,
                                    is_proxy: true,
                                    proxy_intent: Some(intent.clone()),
                                });
                            }
                            bail!("Invalid user provided")
                        }
                        _ => bail!("Invalid Dataproxy signed token intent"),
                    }
                } else {
                    bail!("Missing intent in Dataproxy signed token")
                }
            }
            PubKeyEnum::Server(_) => Err(anyhow::anyhow!("Token not signed from Dataproxy")),
        }
    }

    ///ToDo: Rust Doc
    async fn validate_oidc_token(&self, claims: &ArunaTokenClaims) -> Result<ProcessedToken> {
        let oidc_mapping = OIDCMapping {
            oidc_name: claims.iss.clone(),
            external_id: claims.sub.clone(),
        };

        // Fetch user from oidc provider
        let user = match self.cache.get_user_by_oidc(&oidc_mapping) {
            Some(u) => u,
            None => return Err(anyhow!(OIDCError::NotFound("Not registered".to_string()))),
        };
        let perms = user.get_permissions(None)?;

        Ok(ProcessedToken {
            main_id: user.id,
            token: None,
            is_personal: true,
            user_permissions: perms.0,
            is_proxy: false,
            proxy_intent: None,
        })
    }

    pub async fn sign_hook_secret(
        &self,
        cache: Arc<Cache>,
        object_id: DieselUlid,
        hook_id: DieselUlid,
    ) -> Result<(String, i16)> {
        let serial = self.get_current_pubkey_serial();
        let key = cache
            .get_pubkey(serial)
            .ok_or_else(|| anyhow!("Pubkey not found"))?
            .get_key_string();
        let mut mac = HmacSha256::new_from_slice(key.as_bytes())?;
        let sign = format!("{object_id}{hook_id}");
        mac.update(sign.as_bytes());
        Ok((
            general_purpose::STANDARD.encode(mac.finalize().into_bytes()),
            serial,
        ))
    }
    pub fn verify_hook_secret(
        &self,
        cache: Arc<Cache>,
        secret: String,
        object_id: DieselUlid,
        hook_id: DieselUlid,
        pubkey_serial: i16,
    ) -> Result<()> {
        let key = cache
            .get_pubkey(pubkey_serial)
            .ok_or_else(|| anyhow!("No pubkey found"))?
            .get_key_string();
        let mut mac = HmacSha256::new_from_slice(key.as_bytes())?;
        let sign = format!("{object_id}{hook_id}");
        mac.update(sign.as_bytes());
        let verify = general_purpose::STANDARD.decode(secret.as_bytes())?;
        mac.verify_slice(&verify).unwrap();
        Ok(())
    }
}

// Token tests
#[cfg(test)]
mod tests {}
