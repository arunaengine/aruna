use aruna_core::operation::Operation;
use aruna_core::structs::RealmId;
use aruna_core::types::UserId;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct CreateTokenConfig {
    pub user_id: UserId,
    pub realm_id: RealmId,
    pub display_name: String,
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
    CreateToken,
    Finish,
    Error,
}

#[derive(Debug, Error)]
pub enum CreateTokenError {
    #[error("Creating Group did not finish")]
    NotFinished,
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
        todo!()
    }
}
impl Operation for CreateTokenOperation {
    type Output = String;

    type Error = CreateTokenError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = CreateTokenState::CreateToken;
        self.emit_token();
        smallvec![]
    }

    fn step(&mut self, events: aruna_core::events::Event) -> aruna_core::types::Effects {
        match (events, &self.state) {
            (_, CreateTokenState::Error) => {
                smallvec![]
            }
            (_, CreateTokenState::Finish) => {
                smallvec![]
            }
            _ => {
                smallvec![]
            }
        }
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
    #[tokio::test]
    pub async fn test_token_creation() {
        todo!()
    }
}
