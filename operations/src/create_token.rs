use aruna_core::auth::valid_token_lifetime;
use aruna_core::operation::Operation;
use aruna_core::structs::{NodeCapabilities, RealmId, SessionRef, TokenClaims};
use aruna_core::types::UserId;
use base64::Engine;
use chrono::Months;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, PartialEq)]
pub struct CreateTokenConfig {
    pub time: u64,
    pub expiry: Option<u64>,
    pub user_id: UserId,
    pub realm_id: RealmId,
    pub node_capabilities: NodeCapabilities,
    pub session: Option<SessionRef>,
}

#[derive(Debug, PartialEq)]
pub struct CreateTokenOperation {
    config: CreateTokenConfig,
    state: CreateTokenState,
    output: Option<Result<String, CreateTokenError>>,
}

#[derive(Debug, PartialEq)]
pub enum CreateTokenState {
    Init,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateTokenError {
    #[error("Node has no capability to create tokens")]
    NotEnoughCapabilities,
    #[error("Creating Group did not finish")]
    NotFinished,
    #[error("Invalid timestamp")]
    InvalidTimestamp,
    #[error("Token lifetime exceeds the revocable maximum")]
    LifetimeTooLong,
    #[error(transparent)]
    EncodingError(#[from] jsonwebtoken::errors::Error),
}

pub fn mint_token(config: &CreateTokenConfig) -> Result<String, CreateTokenError> {
    let iat = config.time;
    let exp = match config.expiry {
        Some(exp) if exp > iat => exp,
        Some(_) => return Err(CreateTokenError::InvalidTimestamp),
        None => {
            let time = chrono::DateTime::from_timestamp_secs(iat as i64)
                .ok_or(CreateTokenError::InvalidTimestamp)?;
            time.checked_add_months(Months::new(12))
                .ok_or(CreateTokenError::InvalidTimestamp)?
                .timestamp() as u64
        }
    };
    if !valid_token_lifetime(iat, exp) {
        return Err(CreateTokenError::LifetimeTooLong);
    }

    let claims = |issuer_pubkey, delegation_signature| TokenClaims {
        sub: config.user_id.to_string(),
        iss: config.realm_id.to_string(),
        iat,
        exp,
        jti: Ulid::generate().to_string(),
        sid: config.session.as_ref().map(|session| session.sid.clone()),
        session_kind: config.session.as_ref().map(|session| session.kind),
        restrictions: None,
        issuer_pubkey,
        delegation_signature,
    };
    let header = Header::new(Algorithm::EdDSA);
    match &config.node_capabilities {
        NodeCapabilities::Management {
            realm_encoding_key, ..
        } => Ok(encode(
            &header,
            &claims(None, None),
            &EncodingKey::from_ed_pem(realm_encoding_key)?,
        )?),
        NodeCapabilities::Server {
            issuer_signing_key,
            issuer_encoding_key,
            delegation_signature,
            ..
        } => Ok(encode(
            &header,
            &claims(
                Some(
                    base64::engine::general_purpose::URL_SAFE_NO_PAD
                        .encode(issuer_signing_key.verifying_key().to_bytes()),
                ),
                Some(delegation_signature.clone()),
            ),
            &EncodingKey::from_ed_pem(issuer_encoding_key)?,
        )?),
        NodeCapabilities::User { .. } => Err(CreateTokenError::NotEnoughCapabilities),
    }
}

impl CreateTokenOperation {
    pub fn new(config: CreateTokenConfig) -> Result<Self, CreateTokenError> {
        if matches!(config.node_capabilities, NodeCapabilities::User { .. }) {
            Err(CreateTokenError::NotEnoughCapabilities)
        } else {
            Ok(CreateTokenOperation {
                config,
                state: CreateTokenState::Init,
                output: None,
            })
        }
    }
    pub fn emit_token(&mut self) -> Result<(), CreateTokenError> {
        self.output = Some(Ok(mint_token(&self.config)?));
        Ok(())
    }
}
impl Operation for CreateTokenOperation {
    type Output = String;

    type Error = CreateTokenError;

    fn start(&mut self) -> aruna_core::types::Effects {
        if let Err(err) = self.emit_token() {
            self.state = CreateTokenState::Error;
            self.output = Some(Err(err));
        } else {
            self.state = CreateTokenState::Finish;
        }
        smallvec![]
    }

    fn step(&mut self, _events: aruna_core::events::Event) -> aruna_core::types::Effects {
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateTokenState::Finish | CreateTokenState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(CreateTokenError::NotFinished)?
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod test {
    use crate::create_token::{
        CreateTokenConfig, CreateTokenError, CreateTokenOperation, mint_token,
    };
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::structs::{NodeCapabilities, RealmId, SessionKind, SessionRef, TokenClaims};
    use aruna_storage::storage;
    use ed25519_dalek::SigningKey;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_token_creation() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let signing_key: SigningKey = generate_signing_key();
        let pubkey = signing_key.verifying_key().to_bytes();
        let realm_id = RealmId::from_bytes(pubkey);
        let capabilities = NodeCapabilities::management_node(signing_key).unwrap();

        let token_config = CreateTokenConfig {
            time: chrono::Utc::now().timestamp() as u64,
            expiry: None,
            user_id: UserId::local(Ulid::generate(), realm_id),
            realm_id,
            node_capabilities: capabilities,

            session: None,
        };

        let token_operation = CreateTokenOperation::new(token_config.clone()).unwrap();
        drive(token_operation, &context).await.unwrap();
    }

    #[test]
    fn rejects_overlong_expiry() {
        // Minting must not hand out an expiry that validation rejects.
        let signing_key: SigningKey = generate_signing_key();
        let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
        let time = 1_000_000;
        let mut operation = CreateTokenOperation::new(CreateTokenConfig {
            time,
            expiry: Some(time + aruna_core::auth::MAX_BEARER_TOKEN_LIFETIME_SECS + 1),
            user_id: UserId::local(Ulid::generate(), realm_id),
            realm_id,
            node_capabilities: NodeCapabilities::management_node(signing_key).unwrap(),
            session: None,
        })
        .unwrap();

        assert_eq!(
            operation.emit_token(),
            Err(CreateTokenError::LifetimeTooLong)
        );
    }

    #[test]
    fn claims_carry_session() {
        let signing_key: SigningKey = generate_signing_key();
        let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
        let sid = Ulid::from_bytes([7; 16]).to_string();
        let token = mint_token(&CreateTokenConfig {
            time: 1_800_000_000,
            expiry: Some(1_800_000_600),
            user_id: UserId::local(Ulid::from_bytes([8; 16]), realm_id),
            realm_id,
            node_capabilities: NodeCapabilities::management_node(signing_key).unwrap(),
            session: Some(SessionRef {
                sid: sid.clone(),
                kind: SessionKind::Assistant,
            }),
        })
        .unwrap();
        let claims = jsonwebtoken::dangerous::insecure_decode::<TokenClaims>(&token)
            .unwrap()
            .claims;

        assert_eq!(claims.sid.as_deref(), Some(sid.as_str()));
        assert_eq!(claims.session_kind, Some(SessionKind::Assistant));
    }
}
