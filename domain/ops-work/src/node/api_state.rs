//! Loads and saves one postcard row of the node's API state, such as the trusted realm list.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::marker::PhantomData;
use std::mem;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::API_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum StateError {
    #[error("API state storage failed: {0}")]
    Io(#[from] IoError),
    #[error("API state row is not readable: {0}")]
    Decode(String),
    #[error("API state row is not writable: {0}")]
    Encode(String),
}

/// Reads the row at `key`; outputs `None` when it is absent.
#[derive(Debug)]
pub struct LoadStateOperation<T> {
    key: Vec<u8>,
    value: PhantomData<fn() -> T>,
}

impl<T> PartialEq for LoadStateOperation<T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<T> LoadStateOperation<T> {
    pub fn new(key: &[u8]) -> Self {
        Self {
            key: key.to_vec(),
            value: PhantomData,
        }
    }
}

impl<T: DeserializeOwned + std::fmt::Debug + Send> Operation for LoadStateOperation<T> {
    type Output = Option<T>;
    type Error = StateError;

    fn first(&mut self) -> Result<Next<Option<T>>, StateError> {
        let read = (API_STATE_KEYSPACE.to_string(), self.key.clone().into());
        Ok(Next::Call(IoCall::Read(vec![read])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<T>>, StateError> {
        let IoResult::Values(values) = result else {
            return Err(IoError::Unexpected {
                expected: "IoResult::Values",
                got: "another result",
            }
            .into());
        };
        let Some(bytes) = values.into_iter().next().flatten() else {
            return Ok(Next::Done(None));
        };
        postcard::from_bytes(&bytes)
            .map(|value| Next::Done(Some(value)))
            .map_err(|error| StateError::Decode(error.to_string()))
    }
}

/// Writes one value to the row at `key`.
#[derive(Debug, PartialEq)]
pub struct SaveStateOperation {
    key: Vec<u8>,
    bytes: Vec<u8>,
}

impl SaveStateOperation {
    pub fn new<T: Serialize>(key: &[u8], value: &T) -> Result<Self, StateError> {
        let bytes =
            postcard::to_allocvec(value).map_err(|error| StateError::Encode(error.to_string()))?;
        Ok(Self {
            key: key.to_vec(),
            bytes,
        })
    }
}

impl Operation for SaveStateOperation {
    type Output = ();
    type Error = StateError;

    fn first(&mut self) -> Result<Next<()>, StateError> {
        let key = self.key.clone().into();
        let write = (
            API_STATE_KEYSPACE.to_string(),
            key,
            mem::take(&mut self.bytes).into(),
        );
        Ok(Next::Call(IoCall::Write(vec![write])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, StateError> {
        match result {
            IoResult::Done => Ok(Next::Done(())),
            _ => Err(IoError::Unexpected {
                expected: "IoResult::Done",
                got: "another result",
            }
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::types::Value;

    const KEY: &[u8] = b"flag";

    fn read() -> IoCall {
        IoCall::Read(vec![(API_STATE_KEYSPACE.to_string(), KEY.to_vec().into())])
    }

    fn row(bytes: Vec<u8>) -> IoResult {
        IoResult::Values(vec![Some(Value::from(bytes))])
    }

    #[test]
    fn loads_present_row() {
        let mut operation = LoadStateOperation::<bool>::new(KEY);
        assert_eq!(operation.first(), Ok(Next::Call(read())));
        let answer = operation.next(row(postcard::to_allocvec(&true).unwrap()));
        assert_eq!(answer, Ok(Next::Done(Some(true))));
        let mut absent = LoadStateOperation::<bool>::new(KEY);
        let answer = absent.next(IoResult::Values(vec![None]));
        assert_eq!(answer, Ok(Next::Done(None)));
    }

    #[test]
    fn rejects_bad_answers() {
        let mut unreadable = LoadStateOperation::<u64>::new(KEY);
        let answer = unreadable.next(row(vec![0xff; 11]));
        assert!(matches!(answer, Err(StateError::Decode(_))));
        let mut wrong = LoadStateOperation::<bool>::new(KEY);
        assert!(matches!(wrong.next(IoResult::Done), Err(StateError::Io(_))));
        let mut save = SaveStateOperation::new(KEY, &true).unwrap();
        let answer = save.next(IoResult::Values(Vec::new()));
        assert!(matches!(answer, Err(StateError::Io(_))));
    }

    #[test]
    fn saves_encoded_value() {
        let mut operation = SaveStateOperation::new(KEY, &true).unwrap();
        let bytes = postcard::to_allocvec(&true).unwrap();
        let write = (
            API_STATE_KEYSPACE.to_string(),
            KEY.to_vec().into(),
            bytes.into(),
        );
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Write(vec![write])))
        );
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
    }
}
