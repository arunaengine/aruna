use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
use aruna_core::onboarding::OnboardingSecretRecord;
use aruna_core::operation::Operation;
use aruna_core::types::Effects;
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Debug, PartialEq)]
pub struct ListOnboardingSecretsOperation {
    state: ListOnboardingSecretsState,
    output: Option<Result<Vec<OnboardingSecretRecord>, ListOnboardingSecretsError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum ListOnboardingSecretsState {
    Init,
    Iter,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListOnboardingSecretsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("listing onboarding secrets did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl Default for ListOnboardingSecretsOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl ListOnboardingSecretsOperation {
    pub fn new() -> Self {
        Self {
            state: ListOnboardingSecretsState::Init,
            output: None,
        }
    }
}

impl Operation for ListOnboardingSecretsOperation {
    type Output = Vec<OnboardingSecretRecord>;
    type Error = ListOnboardingSecretsError;

    fn start(&mut self) -> Effects {
        self.state = ListOnboardingSecretsState::Iter;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: ONBOARDING_KEYSPACE.to_string(),
            prefix: Some(ByteView::from(b"secret:".as_slice())),
            start_after: None,
            limit: usize::MAX,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => {
                self.state = ListOnboardingSecretsState::Error;
                self.output = Some(Err(ListOnboardingSecretsError::StorageError(error)));
                return smallvec![];
            }
            other => other,
        };

        match self.state {
            ListOnboardingSecretsState::Iter => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
                    self.state = ListOnboardingSecretsState::Error;
                    self.output = Some(Err(ListOnboardingSecretsError::UnexpectedEvent {
                        state: "Iter".to_string(),
                        expected: "storage iteration result",
                        got,
                    }));
                    return smallvec![];
                };

                let records = values
                    .into_iter()
                    .map(|(_, value)| postcard::from_bytes::<OnboardingSecretRecord>(&value))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(ConversionError::from);

                self.state = match records {
                    Ok(records) => {
                        self.output = Some(Ok(records));
                        ListOnboardingSecretsState::Finish
                    }
                    Err(error) => {
                        self.output = Some(Err(ListOnboardingSecretsError::ConversionError(error)));
                        ListOnboardingSecretsState::Error
                    }
                };
                smallvec![]
            }
            ListOnboardingSecretsState::Init
            | ListOnboardingSecretsState::Finish
            | ListOnboardingSecretsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListOnboardingSecretsState::Finish | ListOnboardingSecretsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ListOnboardingSecretsError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
