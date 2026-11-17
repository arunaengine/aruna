//! Keeps the one row of this device's exchange with the realm: when the realm last answered,
//! when the last pass finished, and whether a pass is in flight.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::SYNC_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, Key};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

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

/// The state a read answer holds; an absent, unreadable or undecodable row counts as empty.
pub(crate) fn read_state(event: &Event) -> DeviceSyncState {
    match event {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => postcard::from_bytes(bytes).unwrap_or_default(),
        _ => DeviceSyncState::default(),
    }
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
    #[error("unexpected {0} while noting the device sync state")]
    UnexpectedEvent(&'static str),
    #[error("noting the device sync state did not finish")]
    NotFinished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NoteState {
    Init,
    Read,
    Write,
    Finish,
    Error,
}

/// Records one note in the device sync state at `now_ms`. A failed write is only logged: the
/// state is a view for the owner, and the next note writes it again.
#[derive(Debug, PartialEq)]
pub struct NoteSyncOperation {
    note: SyncNote,
    now_ms: u64,
    state: NoteState,
    output: Option<Result<(), NoteError>>,
}

impl NoteSyncOperation {
    pub fn new(note: SyncNote, now_ms: u64) -> Self {
        Self {
            note,
            now_ms,
            state: NoteState::Init,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<(), NoteError>) -> Effects {
        self.state = match output {
            Ok(()) => NoteState::Finish,
            Err(_) => NoteState::Error,
        };
        self.output = Some(output);
        smallvec![]
    }

    fn write(&mut self, state: &DeviceSyncState) -> Effects {
        let Ok(bytes) = postcard::to_allocvec(state) else {
            return self.finish(Ok(()));
        };
        self.state = NoteState::Write;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: SYNC_STATE_KEYSPACE.to_string(),
            key: sync_state_key(),
            value: ByteView::from(bytes),
            txn_id: None,
        })]
    }
}

impl Operation for NoteSyncOperation {
    type Output = ();
    type Error = NoteError;

    fn start(&mut self) -> Effects {
        self.state = NoteState::Read;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: SYNC_STATE_KEYSPACE.to_string(),
            key: sync_state_key(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, &event) {
            (
                NoteState::Read,
                Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. }),
            ) => {
                let mut state = read_state(&event);
                match self.note {
                    SyncNote::Contact => state.last_contact_ms = Some(self.now_ms),
                    SyncNote::Failure => {
                        if state.last_contact_ms.take().is_none() {
                            return self.finish(Ok(()));
                        }
                    }
                    SyncNote::Finished => {
                        state.last_contact_ms = Some(self.now_ms);
                        state.last_sync_ms = Some(self.now_ms);
                        state.run_started_ms = None;
                    }
                }
                self.write(&state)
            }
            (NoteState::Write, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.finish(Ok(()))
            }
            (NoteState::Write, Event::Storage(StorageEvent::Error { error })) => {
                warn!(error = %error, "Failed to store the device sync state");
                self.finish(Ok(()))
            }
            _ => {
                let label = event.label();
                self.finish(Err(NoteError::UnexpectedEvent(label)))
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, NoteState::Finish | NoteState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(NoteError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_trace, rejects_events, rejects_finalize, sample_events,
    };

    fn stored(state: &DeviceSyncState) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: sync_state_key(),
            value: Some(postcard::to_allocvec(state).unwrap().into()),
        })
    }

    fn read() -> Effect {
        Effect::Storage(StorageEffect::Read {
            key_space: SYNC_STATE_KEYSPACE.to_string(),
            key: sync_state_key(),
            txn_id: None,
        })
    }

    fn write(state: &DeviceSyncState) -> Effect {
        Effect::Storage(StorageEffect::Write {
            key_space: SYNC_STATE_KEYSPACE.to_string(),
            key: sync_state_key(),
            value: postcard::to_allocvec(state).unwrap().into(),
            txn_id: None,
        })
    }

    fn written() -> Event {
        Event::Storage(StorageEvent::WriteResult {
            key: sync_state_key(),
        })
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
        check_trace(
            &mut operation,
            &[read()],
            vec![
                (stored(&before), vec![write(&after)]),
                (written(), Vec::new()),
            ],
        );
        assert_eq!(operation.finalize(), Ok(()));
    }

    // A failed contact clears the last contact, and writes nothing when none was kept.
    #[test]
    fn failure_clears_contact() {
        let reached = DeviceSyncState {
            last_contact_ms: Some(3),
            ..DeviceSyncState::default()
        };
        let mut operation = NoteSyncOperation::new(SyncNote::Failure, 9);
        check_trace(
            &mut operation,
            &[read()],
            vec![(stored(&reached), vec![write(&DeviceSyncState::default())])],
        );
        let mut operation = NoteSyncOperation::new(SyncNote::Failure, 9);
        operation.start();
        assert!(
            operation
                .step(stored(&DeviceSyncState::default()))
                .is_empty()
        );
        assert_eq!(operation.finalize(), Ok(()));
    }

    // An unreadable row counts as empty; a failed write is only logged.
    #[test]
    fn failures_tolerated() {
        let error = || {
            Event::Storage(StorageEvent::Error {
                error: StorageError::Timeout,
            })
        };
        let contact = DeviceSyncState {
            last_contact_ms: Some(4),
            ..DeviceSyncState::default()
        };
        let mut operation = NoteSyncOperation::new(SyncNote::Contact, 4);
        check_trace(
            &mut operation,
            &[read()],
            vec![(error(), vec![write(&contact)]), (error(), Vec::new())],
        );
        assert_eq!(operation.finalize(), Ok(()));
    }

    // Other events are refused; nothing is cleaned up and nothing finishes early.
    #[test]
    fn rejects_unexpected() {
        let fresh = || NoteSyncOperation::new(SyncNote::Contact, 1);
        let all = sample_events().len();
        assert_eq!(rejects_events(fresh, sample_events, |_| false).len(), all);
        rejects_finalize(fresh());
        assert!(fresh().abort().is_empty());
    }
}
