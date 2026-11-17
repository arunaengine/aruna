//! Loads and saves one postcard row of the node's API state, such as the trusted realm list.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::marker::PhantomData;
use std::mem;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::API_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, Value};
use serde::Serialize;
use serde::de::DeserializeOwned;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum StateError {
    #[error("API state storage failed: {0}")]
    Storage(StorageError),
    #[error("API state row is not readable: {0}")]
    Decode(String),
    #[error("API state row is not writable: {0}")]
    Encode(String),
    #[error("API state request got an unexpected event")]
    Unexpected,
    #[error("API state request did not finish")]
    Unfinished,
}

#[derive(Debug, PartialEq)]
enum Phase {
    Init,
    Waiting,
    Done(Option<Value>),
    Failed(StateError),
}

impl Phase {
    /// Moves to `Done` on the answer `expected` accepts, else fails.
    fn answer(
        &mut self,
        event: Event,
        expected: impl FnOnce(StorageEvent) -> Option<Option<Value>>,
    ) {
        *self = match (mem::replace(self, Phase::Init), event) {
            (Phase::Waiting, Event::Storage(StorageEvent::Error { error })) => {
                Phase::Failed(StateError::Storage(error))
            }
            (Phase::Waiting, Event::Storage(event)) => match expected(event) {
                Some(value) => Phase::Done(value),
                None => Phase::Failed(StateError::Unexpected),
            },
            _ => Phase::Failed(StateError::Unexpected),
        };
    }
}

/// Reads the row at `key`; outputs `None` when it is absent.
#[derive(Debug)]
pub struct LoadStateOperation<T> {
    key: Vec<u8>,
    phase: Phase,
    value: PhantomData<fn() -> T>,
}

impl<T> PartialEq for LoadStateOperation<T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.phase == other.phase
    }
}

impl<T> LoadStateOperation<T> {
    pub fn new(key: &[u8]) -> Self {
        Self {
            key: key.to_vec(),
            phase: Phase::Init,
            value: PhantomData,
        }
    }
}

impl<T: DeserializeOwned + std::fmt::Debug + Send> Operation for LoadStateOperation<T> {
    type Output = Option<T>;
    type Error = StateError;

    fn start(&mut self) -> Effects {
        self.phase = Phase::Waiting;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: API_STATE_KEYSPACE.to_string(),
            key: self.key.clone().into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.phase.answer(event, |event| match event {
            StorageEvent::ReadResult { value, .. } => Some(value),
            _ => None,
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        matches!(self.phase, Phase::Done(_) | Phase::Failed(_))
    }

    fn finalize(self) -> Result<Option<T>, StateError> {
        match self.phase {
            Phase::Done(None) => Ok(None),
            Phase::Done(Some(bytes)) => postcard::from_bytes(&bytes)
                .map(Some)
                .map_err(|error| StateError::Decode(error.to_string())),
            Phase::Failed(error) => Err(error),
            Phase::Init | Phase::Waiting => Err(StateError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Writes one value to the row at `key`.
#[derive(Debug, PartialEq)]
pub struct SaveStateOperation {
    key: Vec<u8>,
    bytes: Vec<u8>,
    phase: Phase,
}

impl SaveStateOperation {
    pub fn new<T: Serialize>(key: &[u8], value: &T) -> Result<Self, StateError> {
        let bytes =
            postcard::to_allocvec(value).map_err(|error| StateError::Encode(error.to_string()))?;
        Ok(Self {
            key: key.to_vec(),
            bytes,
            phase: Phase::Init,
        })
    }
}

impl Operation for SaveStateOperation {
    type Output = ();
    type Error = StateError;

    fn start(&mut self) -> Effects {
        self.phase = Phase::Waiting;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: API_STATE_KEYSPACE.to_string(),
            key: self.key.clone().into(),
            value: mem::take(&mut self.bytes).into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.phase.answer(event, |event| match event {
            StorageEvent::WriteResult { .. } => Some(None),
            _ => None,
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        matches!(self.phase, Phase::Done(_) | Phase::Failed(_))
    }

    fn finalize(self) -> Result<(), StateError> {
        match self.phase {
            Phase::Done(_) => Ok(()),
            Phase::Failed(error) => Err(error),
            Phase::Init | Phase::Waiting => Err(StateError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::operation_check::{rejects_events, sample_events};

    const KEY: &[u8] = b"flag";

    #[test]
    fn loads_present_row() {
        let mut operation = LoadStateOperation::<bool>::new(KEY);
        let effects = operation.start();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, key, .. })]
                if key_space == API_STATE_KEYSPACE && key.as_ref() == KEY
        ));
        let value = Some(postcard::to_allocvec(&true).unwrap().into());
        let key = KEY.to_vec().into();
        operation.step(Event::Storage(StorageEvent::ReadResult { key, value }));
        assert!(operation.is_complete());
        assert!(operation.abort().is_empty());
        assert_eq!(operation.finalize(), Ok(Some(true)));
    }

    #[test]
    fn loads_absent_row() {
        let mut operation = LoadStateOperation::<bool>::new(KEY);
        operation.start();
        let key = KEY.to_vec().into();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key,
            value: None,
        }));
        assert_eq!(operation.finalize(), Ok(None));
    }

    #[test]
    fn rejects_unreadable_row() {
        let mut operation = LoadStateOperation::<u64>::new(KEY);
        operation.start();
        let key = KEY.to_vec().into();
        let value = Some(vec![0xff; 11].into());
        operation.step(Event::Storage(StorageEvent::ReadResult { key, value }));
        assert!(matches!(operation.finalize(), Err(StateError::Decode(_))));
    }

    #[test]
    fn saves_encoded_value() {
        let mut operation = SaveStateOperation::new(KEY, &true).unwrap();
        let effects = operation.start();
        let bytes = postcard::to_allocvec(&true).unwrap();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { key, value, .. })]
                if key.as_ref() == KEY && value.as_ref() == bytes.as_slice()
        ));
        let key = KEY.to_vec().into();
        operation.step(Event::Storage(StorageEvent::WriteResult { key }));
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn reports_storage_error() {
        let mut operation = SaveStateOperation::new(KEY, &true).unwrap();
        operation.start();
        let error = StorageError::ReadError("boom".to_string());
        operation.step(Event::Storage(StorageEvent::Error {
            error: error.clone(),
        }));
        assert_eq!(operation.finalize(), Err(StateError::Storage(error)));
    }

    #[test]
    fn rejects_other_events() {
        let load = || {
            let mut operation = LoadStateOperation::<bool>::new(KEY);
            operation.start();
            operation
        };
        let read = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. })
            )
        };
        for (label, _, error) in rejects_events(load, sample_events, read) {
            assert_eq!(error, StateError::Unexpected, "{label}");
        }
        let save = || {
            let mut operation = SaveStateOperation::new(KEY, &true).unwrap();
            operation.start();
            operation
        };
        let written = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::WriteResult { .. } | StorageEvent::Error { .. })
            )
        };
        for (label, _, error) in rejects_events(save, sample_events, written) {
            assert_eq!(error, StateError::Unexpected, "{label}");
        }
    }

    #[test]
    fn rejects_early_finalize() {
        let mut operation = LoadStateOperation::<bool>::new(KEY);
        assert_eq!(operation.start().len(), 1);
        assert_eq!(operation.finalize(), Err(StateError::Unfinished));
    }
}
