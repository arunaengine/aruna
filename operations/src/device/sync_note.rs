//! Keeps the one row of this device's exchange with the realm: when the realm last answered,
//! when the last pass finished, and whether a pass is in flight.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::SYNC_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::done;
use aruna_core::types::Key;
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// How recently the realm must have answered for the device to call it
/// reachable. Two folder beats, so one missed pass does not flip the view.
const REALM_CONTACT_WINDOW: Duration = Duration::from_secs(90);

/// How long a run stays active without finishing. A crashed run must not block
/// the next one for longer than this.
const SYNC_RUN_LIFETIME: Duration = Duration::from_secs(120);

/// The one row this device keeps about its exchange with the realm.
const SYNC_STATE_KEY: [u8; 1] = [0];

/// When the realm last answered, when the last pass finished, and whether one
/// is in flight.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceSyncState {
    /// When the realm last answered. A failed contact attempt clears it, so the device shows
    /// disconnected at once and connected again after the next answer.
    pub last_contact_ms: Option<u64>,
    pub last_sync_ms: Option<u64>,
    pub run_started_ms: Option<u64>,
}

impl DeviceSyncState {
    pub fn realm_reachable(&self, now_ms: u64) -> bool {
        self.last_contact_ms.is_some_and(|contact| {
            now_ms.saturating_sub(contact) <= REALM_CONTACT_WINDOW.as_millis() as u64
        })
    }

    pub fn run_active(&self, now_ms: u64) -> bool {
        self.run_started_ms.is_some_and(|started| {
            now_ms.saturating_sub(started) <= SYNC_RUN_LIFETIME.as_millis() as u64
        })
    }
}

pub(crate) fn sync_state_key() -> Key {
    ByteView::from(SYNC_STATE_KEY.to_vec())
}

/// What the device records about its exchange with the realm.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncNote {
    /// The realm answered.
    Contact,
    /// The realm did not answer the last contact attempt.
    Failure,
    /// A sync pass finished, which also ends the run it belonged to.
    Finished,
}

#[derive(Debug, Error, PartialEq)]
pub enum NoteError {
    #[error(transparent)]
    Io(#[from] IoError),
}

/// Records one note in the device sync state at `now_ms`. A failed read or write fails the
/// note; the state is a view for the owner, and the next note writes it again.
#[derive(Debug, PartialEq)]
pub struct NoteSyncOperation {
    note: SyncNote,
    now_ms: u64,
    read: bool,
}

impl NoteSyncOperation {
    pub fn new(note: SyncNote, now_ms: u64) -> Self {
        Self {
            note,
            now_ms,
            read: false,
        }
    }

    /// The state after this note, or `None` when the note changes nothing.
    fn noted(&self, mut state: DeviceSyncState) -> Option<DeviceSyncState> {
        match self.note {
            SyncNote::Contact => state.last_contact_ms = Some(self.now_ms),
            SyncNote::Failure => {
                state.last_contact_ms.take()?;
            }
            SyncNote::Finished => {
                state.last_contact_ms = Some(self.now_ms);
                state.last_sync_ms = Some(self.now_ms);
                state.run_started_ms = None;
            }
        }
        Some(state)
    }
}

impl Operation for NoteSyncOperation {
    type Output = ();
    type Error = NoteError;

    fn first(&mut self) -> Result<Next<()>, NoteError> {
        let row = (SYNC_STATE_KEYSPACE.to_string(), sync_state_key());
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, NoteError> {
        if self.read {
            done(result)?;
            return Ok(Next::Done(()));
        }
        self.read = true;
        let stored = result.values()?.into_iter().next().flatten();
        let state = stored
            .and_then(|bytes| postcard::from_bytes(&bytes).ok())
            .unwrap_or_default();
        let Some(state) = self.noted(state) else {
            return Ok(Next::Done(()));
        };
        let Ok(bytes) = postcard::to_allocvec(&state) else {
            return Ok(Next::Done(()));
        };
        let row = (
            SYNC_STATE_KEYSPACE.to_string(),
            sync_state_key(),
            bytes.into(),
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(state: &DeviceSyncState) -> IoResult {
        IoResult::Values(vec![Some(postcard::to_allocvec(state).unwrap().into())])
    }

    fn write(state: &DeviceSyncState) -> Next<()> {
        let value = postcard::to_allocvec(state).unwrap().into();
        let row = (SYNC_STATE_KEYSPACE.to_string(), sync_state_key(), value);
        Next::Call(IoCall::Write(vec![row]))
    }

    // A finished pass records contact and sync time and ends the run.
    #[test]
    fn finished_ends_run() {
        let before = DeviceSyncState {
            run_started_ms: Some(5),
            ..DeviceSyncState::default()
        };
        let after = DeviceSyncState {
            last_contact_ms: Some(9),
            last_sync_ms: Some(9),
            run_started_ms: None,
        };
        let mut operation = NoteSyncOperation::new(SyncNote::Finished, 9);
        let read = IoCall::Read(vec![(SYNC_STATE_KEYSPACE.to_string(), sync_state_key())]);
        assert_eq!(operation.first(), Ok(Next::Call(read)));
        assert_eq!(operation.next(stored(&before)), Ok(write(&after)));
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
    }

    // A failed contact clears the last contact, and writes nothing when none was kept.
    #[test]
    fn failure_clears_contact() {
        let reached = DeviceSyncState {
            last_contact_ms: Some(3),
            ..DeviceSyncState::default()
        };
        let mut operation = NoteSyncOperation::new(SyncNote::Failure, 9);
        operation.first().unwrap();
        let cleared = write(&DeviceSyncState::default());
        assert_eq!(operation.next(stored(&reached)), Ok(cleared));
        let mut operation = NoteSyncOperation::new(SyncNote::Failure, 9);
        operation.first().unwrap();
        let empty = operation.next(stored(&DeviceSyncState::default()));
        assert_eq!(empty, Ok(Next::Done(())));
    }

    // An absent or undecodable row counts as empty; a result that answers no call fails.
    #[test]
    fn empty_row_counts() {
        let contact = DeviceSyncState {
            last_contact_ms: Some(4),
            ..DeviceSyncState::default()
        };
        let mut operation = NoteSyncOperation::new(SyncNote::Contact, 4);
        operation.first().unwrap();
        let garbage = IoResult::Values(vec![Some(vec![0xFF].into())]);
        assert_eq!(operation.next(garbage), Ok(write(&contact)));
        assert!(operation.next(IoResult::Woke).is_err());
    }

    #[test]
    fn windows_realm_contact() {
        // A device that has not heard from its realm in two beats is offline,
        // and a run that never finished stops blocking the next one.
        let now = 1_000_000;
        let state = DeviceSyncState {
            last_contact_ms: Some(now - 1_000),
            last_sync_ms: Some(now - 1_000),
            run_started_ms: Some(now - 1_000),
        };
        assert!(state.realm_reachable(now));
        assert!(state.run_active(now));

        let stale = DeviceSyncState {
            last_contact_ms: Some(now - REALM_CONTACT_WINDOW.as_millis() as u64 - 1),
            run_started_ms: Some(now - SYNC_RUN_LIFETIME.as_millis() as u64 - 1),
            ..state
        };
        assert!(!stale.realm_reachable(now));
        assert!(!stale.run_active(now));
        assert!(!DeviceSyncState::default().realm_reachable(now));
    }
}
