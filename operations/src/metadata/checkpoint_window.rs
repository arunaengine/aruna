//! Reads how much the busiest node wrote in a document's current history window.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::keyspaces::{EVENT_SIZE_KEYSPACE, METADATA_CHECKPOINT_KEYSPACE};
use aruna_core::metadata::{EVENT_LIMIT, EventSize};
use aruna_core::operation::Operation;
use aruna_core::rows::decode;
use aruna_core::storage_entries::event_log_prefix;
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use crate::metadata::call_results::page_rows;

/// Whether this holder should checkpoint once one origin has used `used` of its `share`:
/// the first origin at half, any other origin at three quarters, so a missing first origin
/// never freezes the document.
pub fn due(used: (u64, u64), share: (u64, u64), first: bool) -> bool {
    let (part, of) = if first { (1u64, 2u64) } else { (3, 4) };
    used.0.saturating_mul(of) >= share.0.saturating_mul(part)
        || used.1.saturating_mul(of) >= share.1.saturating_mul(part)
}

/// The most events and bytes any one node wrote since the document's last checkpoint: reads
/// the last checkpoint key, then the event sizes from it on. Both reads go out as effects,
/// since a page call can neither read the last row nor start at a key.
#[derive(Debug, PartialEq)]
pub struct CheckpointWindowOperation {
    document_id: Ulid,
    sizes: bool,
}

impl CheckpointWindowOperation {
    pub fn new(document_id: Ulid) -> Self {
        Self {
            document_id,
            sizes: false,
        }
    }

    fn storage(effect: StorageEffect) -> Result<Next<(u64, u64)>, IoError> {
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(
            effect,
        )))))
    }
}

// Size rows carry each event's origin and size, so no event is decoded here.
fn busiest_origin(values: &[(Key, Value)]) -> Result<(u64, u64), IoError> {
    let mut used: BTreeMap<NodeId, (u64, u64)> = BTreeMap::new();
    for (_, value) in values {
        let size: EventSize = decode(value)?;
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
    type Error = IoError;

    fn first(&mut self) -> Result<Next<(u64, u64)>, IoError> {
        Self::storage(StorageEffect::Last {
            key_space: METADATA_CHECKPOINT_KEYSPACE.to_string(),
            prefix: Some(event_log_prefix(self.document_id)),
            txn_id: None,
        })
    }

    fn next(&mut self, result: IoResult) -> Result<Next<(u64, u64)>, IoError> {
        let (values, _) = page_rows(result)?;
        if self.sizes {
            return Ok(Next::Done(busiest_origin(&values)?));
        }
        self.sizes = true;
        Self::storage(StorageEffect::Iter {
            key_space: EVENT_SIZE_KEYSPACE.to_string(),
            prefix: Some(event_log_prefix(self.document_id)),
            start: values.first().map(|(key, _)| IterStart::At(key.clone())),
            limit: EVENT_LIMIT as usize,
            txn_id: None,
        })
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};

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

    fn iter_result(values: Vec<(Key, Value)>) -> IoResult {
        IoResult::Event(Box::new(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        })))
    }

    fn last_call() -> Result<Next<(u64, u64)>, IoError> {
        CheckpointWindowOperation::storage(StorageEffect::Last {
            key_space: METADATA_CHECKPOINT_KEYSPACE.to_string(),
            prefix: Some(event_log_prefix(document())),
            txn_id: None,
        })
    }

    fn sizes_call(start: Option<IterStart>) -> Result<Next<(u64, u64)>, IoError> {
        CheckpointWindowOperation::storage(StorageEffect::Iter {
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
        assert_eq!(operation.first(), last_call());
        let last = iter_result(vec![(checkpoint.clone(), Vec::<u8>::new().into())]);
        assert_eq!(
            operation.next(last),
            sizes_call(Some(IterStart::At(checkpoint)))
        );
        let sizes = vec![
            size_row(node(1), 10),
            size_row(node(1), 10),
            size_row(node(2), 50),
        ];
        assert_eq!(operation.next(iter_result(sizes)), Ok(Next::Done((2, 50))));
    }

    #[test]
    fn window_without_checkpoint() {
        let mut operation = CheckpointWindowOperation::new(document());
        operation.first().unwrap();
        assert_eq!(operation.next(iter_result(Vec::new())), sizes_call(None));
        assert_eq!(
            operation.next(iter_result(Vec::new())),
            Ok(Next::Done((0, 0)))
        );
    }

    #[test]
    fn storage_errors_fail() {
        let error = || {
            IoResult::Event(Box::new(Event::Storage(StorageEvent::Error {
                error: StorageError::Timeout,
            })))
        };
        for read_sizes in [false, true] {
            let mut operation = CheckpointWindowOperation::new(document());
            operation.first().unwrap();
            if read_sizes {
                operation.next(iter_result(Vec::new())).unwrap();
            }
            assert_eq!(
                operation.next(error()),
                Err(IoError::Storage(StorageError::Timeout))
            );
        }
    }

    #[test]
    fn broken_size_fails() {
        let mut operation = CheckpointWindowOperation::new(document());
        operation.first().unwrap();
        operation.next(iter_result(Vec::new())).unwrap();
        let broken = iter_result(vec![(Vec::<u8>::new().into(), vec![0xff].into())]);
        assert!(matches!(operation.next(broken), Err(IoError::Codec(_))));
    }

    #[test]
    fn rejects_other_results() {
        let mut operation = CheckpointWindowOperation::new(document());
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(IoError::Unexpected { .. })
        ));
    }
}
