use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ONBOARDING_KEYSPACE;
use aruna_core::onboarding::{OnboardingSecretRecord, OnboardingSecretState};
use aruna_core::operation::Operation;
use aruna_core::types::Effects;
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

use crate::onboarding_secret_state::{resolve_secret_state, secret_state_key};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnboardingSecretListEntry {
    pub record: OnboardingSecretRecord,
    pub state: OnboardingSecretState,
}

#[derive(Debug, PartialEq)]
pub struct ListOnboardingSecretsOperation {
    state: ListOnboardingSecretsState,
    output: Option<Result<Vec<OnboardingSecretListEntry>, ListOnboardingSecretsError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum ListOnboardingSecretsState {
    Init,
    Iter,
    ReadStates {
        records: Vec<OnboardingSecretRecord>,
    },
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
    type Output = Vec<OnboardingSecretListEntry>;
    type Error = ListOnboardingSecretsError;

    fn start(&mut self) -> Effects {
        self.state = ListOnboardingSecretsState::Iter;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: ONBOARDING_KEYSPACE.to_string(),
            prefix: Some(ByteView::from(b"secret:".as_slice())),
            start: None,
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

        match self.state.clone() {
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
                        if records.is_empty() {
                            self.output = Some(Ok(Vec::new()));
                            ListOnboardingSecretsState::Finish
                        } else {
                            let reads = records
                                .iter()
                                .map(|record| {
                                    (
                                        ONBOARDING_KEYSPACE.to_string(),
                                        secret_state_key(record.enrollment_id),
                                    )
                                })
                                .collect();
                            self.state = ListOnboardingSecretsState::ReadStates { records };
                            return smallvec![Effect::Storage(StorageEffect::BatchRead {
                                reads,
                                txn_id: None,
                            })];
                        }
                    }
                    Err(error) => {
                        self.output = Some(Err(ListOnboardingSecretsError::ConversionError(error)));
                        ListOnboardingSecretsState::Error
                    }
                };
                smallvec![]
            }
            ListOnboardingSecretsState::ReadStates { records } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
                    self.state = ListOnboardingSecretsState::Error;
                    self.output = Some(Err(ListOnboardingSecretsError::UnexpectedEvent {
                        state: "ReadStates".to_string(),
                        expected: "batch read result",
                        got,
                    }));
                    return smallvec![];
                };
                if values.len() != records.len() {
                    self.state = ListOnboardingSecretsState::Error;
                    self.output = Some(Err(ListOnboardingSecretsError::UnexpectedEvent {
                        state: "ReadStates".to_string(),
                        expected: "state batch read result matching record count",
                        got: format!("{values:?}"),
                    }));
                    return smallvec![];
                }

                let entries = records
                    .into_iter()
                    .zip(values)
                    .map(|(record, (_, state_value))| {
                        resolve_secret_state(&record, state_value.as_ref())
                            .map(|state| OnboardingSecretListEntry { record, state })
                    })
                    .collect::<Result<Vec<_>, _>>();

                self.state = match entries {
                    Ok(entries) => {
                        self.output = Some(Ok(entries));
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
