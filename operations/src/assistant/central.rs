//! Stores the realm's central assistant provider and charges each user's daily model calls.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{ASSISTANT_USAGE_KEYSPACE, CENTRAL_ASSISTANT_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows;
use aruna_core::structs::{
    AssistantUsage, CENTRAL_PROVIDER_ID, CentralAssistant, QuotaReached, charge,
};
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum CentralStoreError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("central assistant changed concurrently")]
    Stale,
    #[error("daily assistant limit reached")]
    QuotaReached(QuotaReached),
}

impl From<IoError> for CentralStoreError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

fn central_key() -> Key {
    ByteView::from(CENTRAL_PROVIDER_ID.as_bytes())
}

fn usage_key(user_id: UserId) -> Key {
    ByteView::from(user_id.to_storage_key())
}

fn central_row() -> (String, Key) {
    (CENTRAL_ASSISTANT_KEYSPACE.to_string(), central_key())
}

fn usage_row(user_id: UserId) -> (String, Key) {
    (ASSISTANT_USAGE_KEYSPACE.to_string(), usage_key(user_id))
}

fn decode_central(value: Option<&Value>) -> Result<Option<CentralAssistant>, ConversionError> {
    value
        .map(|bytes| CentralAssistant::from_bytes(bytes.as_ref()))
        .transpose()
}

fn decode_usage(value: Option<&Value>) -> Result<Option<AssistantUsage>, ConversionError> {
    value
        .map(|bytes| AssistantUsage::from_bytes(bytes.as_ref()))
        .transpose()
}

/// The central provider, if set up, and the caller's usage row.
pub type CentralRead = (Option<CentralAssistant>, Option<AssistantUsage>);

/// Reads the central provider together with the caller's usage row.
#[derive(Debug, PartialEq)]
pub struct GetCentralOperation {
    user_id: UserId,
}

impl GetCentralOperation {
    pub fn new(user_id: UserId) -> Self {
        Self { user_id }
    }
}

impl Operation for GetCentralOperation {
    type Output = CentralRead;
    type Error = CentralStoreError;

    fn first(&mut self) -> Result<Next<CentralRead>, CentralStoreError> {
        let reads = vec![central_row(), usage_row(self.user_id)];
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<CentralRead>, CentralStoreError> {
        let values = rows::all_values(result)?;
        let central = decode_central(values[0].as_ref())?;
        let usage = decode_usage(values[1].as_ref())?;
        Ok(Next::Done((central, usage)))
    }
}

/// Replaces or removes the central provider when the stored row still equals `expected`.
#[derive(Debug, PartialEq)]
pub struct SetCentralOperation {
    next: Option<CentralAssistant>,
    expected: Option<CentralAssistant>,
    /// Whether the change was sent and its answer ends the operation.
    written: bool,
}

impl SetCentralOperation {
    /// `next` of `None` removes the provider.
    pub fn new(next: Option<CentralAssistant>, expected: Option<CentralAssistant>) -> Self {
        Self {
            next,
            expected,
            written: false,
        }
    }
}

impl Operation for SetCentralOperation {
    type Output = ();
    type Error = CentralStoreError;

    fn first(&mut self) -> Result<Next<()>, CentralStoreError> {
        Ok(Next::Call(IoCall::Read(vec![central_row()])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, CentralStoreError> {
        if self.written {
            rows::done(result)?;
            return Ok(Next::Done(()));
        }
        if decode_central(rows::value(result)?.as_ref())? != self.expected {
            return Err(CentralStoreError::Stale);
        }
        let (space, key) = central_row();
        let call = match &self.next {
            Some(next) => IoCall::Write(vec![(space, key, next.to_bytes()?.into())]),
            None => IoCall::Delete(vec![(space, key)]),
        };
        self.written = true;
        Ok(Next::Call(call))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// Counts one central model call for a user on `today`; a refused call writes nothing.
#[derive(Debug, PartialEq)]
pub struct ChargeUsageOperation {
    user_id: UserId,
    today: u32,
    limit: Option<u32>,
    /// The charged row once its write was sent.
    charged: Option<AssistantUsage>,
}

impl ChargeUsageOperation {
    pub fn new(user_id: UserId, today: u32, limit: Option<u32>) -> Self {
        Self {
            user_id,
            today,
            limit,
            charged: None,
        }
    }
}

impl Operation for ChargeUsageOperation {
    type Output = AssistantUsage;
    type Error = CentralStoreError;

    fn first(&mut self) -> Result<Next<AssistantUsage>, CentralStoreError> {
        Ok(Next::Call(IoCall::Read(vec![usage_row(self.user_id)])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AssistantUsage>, CentralStoreError> {
        if let Some(charged) = self.charged.take() {
            rows::done(result)?;
            return Ok(Next::Done(charged));
        }
        let usage = decode_usage(rows::value(result)?.as_ref())?;
        let next =
            charge(usage, self.today, self.limit).map_err(CentralStoreError::QuotaReached)?;
        let (space, key) = usage_row(self.user_id);
        let call = IoCall::Write(vec![(space, key, next.to_bytes()?.into())]);
        self.charged = Some(next);
        Ok(Next::Call(call))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
#[path = "central_tests.rs"]
mod tests;
