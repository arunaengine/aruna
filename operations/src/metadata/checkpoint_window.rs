//! Reads how much the busiest node wrote in a document's current history window.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{EVENT_SIZE_KEYSPACE, METADATA_CHECKPOINT_KEYSPACE};
use aruna_core::metadata::{EVENT_LIMIT, EventSize};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::event_log_prefix;
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

/// Whether this holder should checkpoint once one origin has used `used` of its `share`:
/// the first origin at half, any other origin at three quarters, so a missing first origin
/// never freezes the document.
pub fn due(used: (u64, u64), share: (u64, u64), first: bool) -> bool {
    let (part, of) = if first { (1u64, 2u64) } else { (3, 4) };
    used.0.saturating_mul(of) >= share.0.saturating_mul(part)
        || used.1.saturating_mul(of) >= share.1.saturating_mul(part)
}

#[derive(Debug, Error, PartialEq)]
pub enum CheckpointWindowError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("an event size row does not decode: {0}")]
    Decode(String),
    #[error("unexpected event while {state}: got {got}")]
    UnexpectedEvent {
        state: &'static str,
        got: &'static str,
    },
    #[error("the checkpoint window read did not finish")]
    Unfinished,
}

#[derive(Debug, PartialEq)]
enum WindowState {
    Init,
    ReadLast,
    ReadSizes,
    Done,
}

impl WindowState {
    fn name(&self) -> &'static str {
        match self {
            Self::Init => "starting",
            Self::ReadLast => "reading the last checkpoint",
            Self::ReadSizes => "reading the event sizes",
            Self::Done => "finished",
        }
    }
}

/// The most events and bytes any one node wrote since the document's last checkpoint.
#[derive(Debug, PartialEq)]
pub struct CheckpointWindowOperation {
    document_id: Ulid,
    state: WindowState,
    output: Option<Result<(u64, u64), CheckpointWindowError>>,
}

impl CheckpointWindowOperation {
    pub fn new(document_id: Ulid) -> Self {
        Self {
            document_id,
            state: WindowState::Init,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<(u64, u64), CheckpointWindowError>) -> Effects {
        self.state = WindowState::Done;
        self.output = Some(output);
        smallvec![]
    }

    fn read_sizes(&mut self, start: Option<IterStart>) -> Effects {
        self.state = WindowState::ReadSizes;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: EVENT_SIZE_KEYSPACE.to_string(),
            prefix: Some(event_log_prefix(self.document_id)),
            start,
            limit: EVENT_LIMIT as usize,
            txn_id: None,
        })]
    }
}

// Size rows carry each event's origin and size, so no event is decoded here.
fn busiest_origin(values: &[(Key, Value)]) -> Result<(u64, u64), CheckpointWindowError> {
    let mut used: BTreeMap<NodeId, (u64, u64)> = BTreeMap::new();
    for (_, value) in values {
        let size: EventSize = postcard::from_bytes(value)
            .map_err(|error| CheckpointWindowError::Decode(error.to_string()))?;
        let entry = used.entry(size.node_id).or_default();
        entry.0 += 1;
        entry.1 += size.bytes;
    }
    Ok(used.into_values().fold((0, 0), |most, used| {
        (most.0.max(used.0), most.1.max(used.1))
    }))
}

impl Operation for CheckpointWindowOperation {
    type Output = (u64, u64);
    type Error = CheckpointWindowError;

    fn start(&mut self) -> Effects {
        self.state = WindowState::ReadLast;
        smallvec![Effect::Storage(StorageEffect::Last {
            key_space: METADATA_CHECKPOINT_KEYSPACE.to_string(),
            prefix: Some(event_log_prefix(self.document_id)),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let reading = matches!(self.state, WindowState::ReadLast | WindowState::ReadSizes);
        match event {
            Event::Storage(StorageEvent::IterResult { values, .. })
                if self.state == WindowState::ReadLast =>
            {
                let start = values.first().map(|(key, _)| IterStart::At(key.clone()));
                self.read_sizes(start)
            }
            Event::Storage(StorageEvent::IterResult { values, .. })
                if self.state == WindowState::ReadSizes =>
            {
                let used = busiest_origin(&values);
                self.finish(used)
            }
            Event::Storage(StorageEvent::Error { error }) if reading => {
                self.finish(Err(error.into()))
            }
            event => {
                let error = CheckpointWindowError::UnexpectedEvent {
                    state: self.state.name(),
                    got: event.label(),
                };
                self.finish(Err(error))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == WindowState::Done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(CheckpointWindowError::Unfinished))
    }

    // Both reads run outside a transaction, so nothing is left to clean up.
    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };

    fn document() -> Ulid {
        Ulid::from_parts(7, 7)
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn size_row(node_id: NodeId, bytes: u64) -> (Key, Value) {
        let row = postcard::to_allocvec(&EventSize { node_id, bytes }).expect("size row encodes");
        (Vec::<u8>::new().into(), row.into())
    }

    fn iter_result(values: Vec<(Key, Value)>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        })
    }

    fn last_effect() -> Effect {
        Effect::Storage(StorageEffect::Last {
            key_space: METADATA_CHECKPOINT_KEYSPACE.to_string(),
            prefix: Some(event_log_prefix(document())),
            txn_id: None,
        })
    }

    fn sizes_effect(start: Option<IterStart>) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: EVENT_SIZE_KEYSPACE.to_string(),
            prefix: Some(event_log_prefix(document())),
            start,
            limit: EVENT_LIMIT as usize,
            txn_id: None,
        })
    }

    #[test]
    fn first_origin_leads() {
        let share = (340, 4000);
        assert!(due((170, 0), share, true));
        assert!(!due((169, 0), share, true));
        assert!(!due((170, 0), share, false));
        assert!(due((255, 0), share, false));
        assert!(due((0, 2000), share, true));
        assert!(!due((0, 2000), share, false));
    }

    #[test]
    fn window_after_checkpoint() {
        // The size scan starts at the last checkpoint key and the busiest node wins per measure.
        let checkpoint: Key = vec![1, 2, 3].into();
        let mut operation = CheckpointWindowOperation::new(document());
        check_trace(
            &mut operation,
            &[last_effect()],
            vec![
                (
                    iter_result(vec![(checkpoint.clone(), Vec::<u8>::new().into())]),
                    vec![sizes_effect(Some(IterStart::At(checkpoint)))],
                ),
                (
                    iter_result(vec![
                        size_row(node(1), 10),
                        size_row(node(1), 10),
                        size_row(node(2), 50),
                    ]),
                    vec![],
                ),
            ],
        );
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Ok((2, 50)));
    }

    #[test]
    fn window_without_checkpoint() {
        let mut operation = CheckpointWindowOperation::new(document());
        check_trace(
            &mut operation,
            &[last_effect()],
            vec![
                (iter_result(Vec::new()), vec![sizes_effect(None)]),
                (iter_result(Vec::new()), vec![]),
            ],
        );
        assert_eq!(operation.finalize(), Ok((0, 0)));
    }

    #[test]
    fn storage_errors_fail() {
        for read_sizes in [false, true] {
            let mut operation = CheckpointWindowOperation::new(document());
            operation.start();
            if read_sizes {
                operation.step(iter_result(Vec::new()));
            }
            let effects = operation.step(Event::Storage(StorageEvent::Error {
                error: StorageError::Timeout,
            }));
            assert!(effects.is_empty());
            assert_eq!(
                operation.finalize(),
                Err(CheckpointWindowError::Storage(StorageError::Timeout))
            );
        }
    }

    #[test]
    fn broken_size_fails() {
        let mut operation = CheckpointWindowOperation::new(document());
        operation.start();
        operation.step(iter_result(Vec::new()));
        operation.step(iter_result(vec![(
            Vec::<u8>::new().into(),
            vec![0xff].into(),
        )]));
        assert!(matches!(
            operation.finalize(),
            Err(CheckpointWindowError::Decode(_))
        ));
    }

    #[test]
    fn rejects_other_events() {
        let reading = || {
            let mut operation = CheckpointWindowOperation::new(document());
            operation.start();
            operation
        };
        let accepted = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
            )
        };
        for (_, effects, error) in rejects_events(reading, sample_events, accepted) {
            assert!(effects.is_empty());
            assert!(matches!(
                error,
                CheckpointWindowError::UnexpectedEvent { .. }
            ));
        }
        // A finished read accepts nothing more.
        let mut done = CheckpointWindowOperation::new(document());
        done.start();
        done.step(iter_result(Vec::new()));
        done.step(iter_result(Vec::new()));
        done.step(iter_result(Vec::new()));
        assert!(matches!(
            done.finalize(),
            Err(CheckpointWindowError::UnexpectedEvent { .. })
        ));
    }

    #[test]
    fn abort_cleans_nothing() {
        let mut operation = CheckpointWindowOperation::new(document());
        check_abort(&mut operation, &[]);
        operation.start();
        check_abort(&mut operation, &[]);
        operation.step(iter_result(Vec::new()));
        check_abort(&mut operation, &[]);
    }

    #[test]
    fn finalize_needs_result() {
        rejects_finalize(CheckpointWindowOperation::new(document()));
        let mut reading = CheckpointWindowOperation::new(document());
        reading.start();
        rejects_finalize(reading);
    }
}
