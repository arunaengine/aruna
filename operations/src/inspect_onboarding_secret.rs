use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
use aruna_core::onboarding::OnboardingSecretRecord;
use aruna_core::operation::Operation;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::create_onboarding_secret::secret_record_key;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InspectOnboardingSecretInput {
    pub enrollment_id: Ulid,
    pub secret_hash: String,
    pub now: u64,
}

#[derive(Debug, PartialEq)]
pub struct InspectOnboardingSecretOperation {
    input: InspectOnboardingSecretInput,
    state: InspectOnboardingSecretState,
    output: Option<Result<OnboardingSecretRecord, InspectOnboardingSecretError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum InspectOnboardingSecretState {
    Init,
    ReadRecord,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum InspectOnboardingSecretError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("onboarding secret not found")]
    NotFound,
    #[error("onboarding secret expired")]
    Expired,
    #[error("onboarding secret already consumed")]
    AlreadyConsumed,
    #[error("onboarding secret does not match")]
    InvalidSecret,
    #[error("inspecting onboarding secret did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl InspectOnboardingSecretOperation {
    pub fn new(input: InspectOnboardingSecretInput) -> Self {
        Self {
            input,
            state: InspectOnboardingSecretState::Init,
            output: None,
        }
    }
}

impl Operation for InspectOnboardingSecretOperation {
    type Output = OnboardingSecretRecord;
    type Error = InspectOnboardingSecretError;

    fn start(&mut self) -> Effects {
        self.state = InspectOnboardingSecretState::ReadRecord;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: ONBOARDING_KEYSPACE.to_string(),
            key: secret_record_key(self.input.enrollment_id),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => {
                self.state = InspectOnboardingSecretState::Error;
                self.output = Some(Err(InspectOnboardingSecretError::StorageError(error)));
                return smallvec![];
            }
            other => other,
        };

        match self.state {
            InspectOnboardingSecretState::ReadRecord => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    self.state = InspectOnboardingSecretState::Error;
                    self.output = Some(Err(InspectOnboardingSecretError::UnexpectedEvent {
                        state: "ReadRecord".to_string(),
                        expected: "read result",
                        got,
                    }));
                    return smallvec![];
                };

                let Some(value) = value else {
                    self.state = InspectOnboardingSecretState::Error;
                    self.output = Some(Err(InspectOnboardingSecretError::NotFound));
                    return smallvec![];
                };

                let record = match postcard::from_bytes::<OnboardingSecretRecord>(&value) {
                    Ok(record) => record,
                    Err(error) => {
                        self.state = InspectOnboardingSecretState::Error;
                        self.output = Some(Err(InspectOnboardingSecretError::ConversionError(
                            error.into(),
                        )));
                        return smallvec![];
                    }
                };

                let validation = if record.consumed {
                    Err(InspectOnboardingSecretError::AlreadyConsumed)
                } else if record.expires_at < self.input.now {
                    Err(InspectOnboardingSecretError::Expired)
                } else if record.secret_hash != self.input.secret_hash {
                    Err(InspectOnboardingSecretError::InvalidSecret)
                } else {
                    Ok(record)
                };

                match validation {
                    Ok(record) => {
                        self.state = InspectOnboardingSecretState::Finish;
                        self.output = Some(Ok(record));
                    }
                    Err(error) => {
                        self.state = InspectOnboardingSecretState::Error;
                        self.output = Some(Err(error));
                    }
                }
                smallvec![]
            }
            InspectOnboardingSecretState::Init
            | InspectOnboardingSecretState::Finish
            | InspectOnboardingSecretState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            InspectOnboardingSecretState::Finish | InspectOnboardingSecretState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or(InspectOnboardingSecretError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
