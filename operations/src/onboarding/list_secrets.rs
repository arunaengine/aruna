//! Lists every stored onboarding secret record with its resolved state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

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

use crate::onboarding::secret_state::{resolve_secret_state, secret_state_key};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnboardingSecretEntry {
    pub record: OnboardingSecretRecord,
    pub state: OnboardingSecretState,
}

#[derive(Debug, PartialEq)]
pub struct ListSecretsOperation {
    state: ListSecretsState,
    output: Option<Result<Vec<OnboardingSecretEntry>, ListSecretsError>>,
}

#[derive(Clone, Debug, PartialEq)]
enum ListSecretsState {
    Init,
    Iter,
    ReadStates {
        records: Vec<OnboardingSecretRecord>,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListSecretsError {
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

impl Default for ListSecretsOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl ListSecretsOperation {
    pub fn new() -> Self {
        Self {
            state: ListSecretsState::Init,
            output: None,
        }
    }
}

impl Operation for ListSecretsOperation {
    type Output = Vec<OnboardingSecretEntry>;
    type Error = ListSecretsError;

    fn start(&mut self) -> Effects {
        self.state = ListSecretsState::Iter;
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
                self.state = ListSecretsState::Error;
                self.output = Some(Err(ListSecretsError::StorageError(error)));
                return smallvec![];
            }
            other => other,
        };

        match self.state.clone() {
            ListSecretsState::Iter => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
                    self.state = ListSecretsState::Error;
                    self.output = Some(Err(ListSecretsError::UnexpectedEvent {
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
                            ListSecretsState::Finish
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
                            self.state = ListSecretsState::ReadStates { records };
                            return smallvec![Effect::Storage(StorageEffect::BatchRead {
                                reads,
                                txn_id: None,
                            })];
                        }
                    }
                    Err(error) => {
                        self.output = Some(Err(ListSecretsError::ConversionError(error)));
                        ListSecretsState::Error
                    }
                };
                smallvec![]
            }
            ListSecretsState::ReadStates { records } => {
                let got = format!("{event:?}");
                let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
                    self.state = ListSecretsState::Error;
                    self.output = Some(Err(ListSecretsError::UnexpectedEvent {
                        state: "ReadStates".to_string(),
                        expected: "batch read result",
                        got,
                    }));
                    return smallvec![];
                };
                if values.len() != records.len() {
                    self.state = ListSecretsState::Error;
                    self.output = Some(Err(ListSecretsError::UnexpectedEvent {
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
                            .map(|state| OnboardingSecretEntry { record, state })
                    })
                    .collect::<Result<Vec<_>, _>>();

                self.state = match entries {
                    Ok(entries) => {
                        self.output = Some(Ok(entries));
                        ListSecretsState::Finish
                    }
                    Err(error) => {
                        self.output = Some(Err(ListSecretsError::ConversionError(error)));
                        ListSecretsState::Error
                    }
                };
                smallvec![]
            }
            ListSecretsState::Init | ListSecretsState::Finish | ListSecretsState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListSecretsState::Finish | ListSecretsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ListSecretsError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
