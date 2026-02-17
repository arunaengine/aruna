use aruna_core::operation::Operation;
use aruna_core::structs::{RealmId, TokenClaims};
use aruna_core::types::UserId;
use chrono::Months;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug)]
pub struct CreateTokenConfig {
    pub time: u64,
    pub expiry: Option<u64>,
    pub user_id: UserId,
    pub realm_id: RealmId,
    pub keypair: [u8; 64],
}

#[derive(Debug)]
pub struct CreateTokenOperation {
    config: CreateTokenConfig,
    state: CreateTokenState,
    output: Option<Result<String, CreateTokenError>>,
}

#[derive(Debug)]
pub enum CreateTokenState {
    Init,
    Finish,
    Error,
}

#[derive(Debug, Error)]
pub enum CreateTokenError {
    #[error("Creating Group did not finish")]
    NotFinished,
    #[error("Invalid timestamp")]
    InvalidTimestamp,
    #[error(transparent)]
    EncodingError(#[from] jsonwebtoken::errors::Error),
    #[error(transparent)]
    KeySerializationError(#[from] ed25519_dalek::ed25519::Error),
    #[error(transparent)]
    PKCSError(#[from] ed25519_dalek::pkcs8::Error),
}

impl CreateTokenOperation {
    pub fn new(config: CreateTokenConfig) -> Self {
        CreateTokenOperation {
            config,
            state: CreateTokenState::Init,
            output: None,
        }
    }
    pub fn emit_token(&mut self) -> Result<(), CreateTokenError> {
        let iat = self.config.time;
        let exp = match self.config.expiry {
            Some(exp) => {
                if exp > iat {
                    exp
                } else {
                    return Err(CreateTokenError::InvalidTimestamp);
                }
            }
            None => {
                let time = chrono::DateTime::from_timestamp_secs(iat as i64)
                    .ok_or_else(|| CreateTokenError::InvalidTimestamp)?;
                let new = time
                    .checked_add_months(Months::new(12))
                    .ok_or_else(|| CreateTokenError::InvalidTimestamp)?;
                new.timestamp() as u64
            }
        };

        let claims = TokenClaims {
            sub: format!(
                "{}@{}",
                self.config.user_id.to_string(),
                self.config.realm_id.to_string()
            ),
            iss: self.config.realm_id.to_string(),
            iat,
            exp,
            jti: Ulid::new().to_string(), // TODO: Save tokens somewhere
        };

        let signing_key = SigningKey::from_keypair_bytes(&self.config.keypair)?;
        let encoding_key = signing_key.to_pkcs8_pem(LineEnding::default())?;

        let token = encode(
            &Header::new(Algorithm::EdDSA),
            &claims,
            &EncodingKey::from_ed_pem(encoding_key.as_bytes())?,
        )?;
        self.output = Some(Ok(token));

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
        self.output.ok_or_else(|| CreateTokenError::NotFinished)?
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod test {
    use crate::create_token::{CreateTokenConfig, CreateTokenOperation};
    use crate::driver::{DriverContext, drive};
    use aruna_core::structs::RealmId;
    use aruna_storage::storage;
    use ed25519_dalek::SigningKey;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_token_creation() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(&random_path.path().to_str().unwrap()).unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: None,
        };

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let signing_key: SigningKey = SigningKey::generate(&mut csprng);
        let pubkey = signing_key.verifying_key().to_bytes();
        let realm_id = RealmId::from_bytes(pubkey);
        let keypair = signing_key.to_keypair_bytes();

        let token_config = CreateTokenConfig {
            time: chrono::Utc::now().timestamp() as u64,
            expiry: None,
            user_id: Ulid::new(),
            realm_id,
            keypair,
        };
        let token_operation = CreateTokenOperation::new(token_config.clone());
        drive(token_operation, &context).await.unwrap();
    }
}
