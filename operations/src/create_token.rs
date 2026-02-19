use aruna_core::operation::Operation;
use aruna_core::structs::{RealmId, TokenClaims};
use aruna_core::types::UserId;
use chrono::Months;
use jsonwebtoken::{EncodingKey, Header, encode};
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
    #[error("Invalid timestamp")]
    EncodingError(#[from] jsonwebtoken::errors::Error),
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
                    .ok_or(CreateTokenError::InvalidTimestamp)?;
                let new = time
                    .checked_add_months(Months::new(12))
                    .ok_or(CreateTokenError::InvalidTimestamp)?;
                new.timestamp() as u64
            }
        };

        let claims = TokenClaims {
            sub: format!(
                "{}@{}",
                self.config.user_id.to_string(),
                self.config.realm_id
            ),
            iss: self.config.realm_id.to_string(),
            iat,
            exp,
            jti: Ulid::new().to_string(), // TODO: Save tokens somewhere
        };
        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(&self.config.keypair),
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
        self.output.ok_or(CreateTokenError::NotFinished)?
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
            blob_handle: None,
        };

        let token_config = CreateTokenConfig {
            time: chrono::Utc::now().timestamp() as u64,
            expiry: None,
            user_id: Ulid::new(),
            realm_id: RealmId([0u8; 32]),
            keypair: [0u8; 64],
        };
        let token_operation = CreateTokenOperation::new(token_config.clone());
        drive(token_operation, &context).await.unwrap();
    }
}
