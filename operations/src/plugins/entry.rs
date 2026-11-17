//! Reads the current complete entry of one owner secret name on this node, as one operation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::secret_registers::{SecretEntry, SecretOwner};
use aruna_core::secrets::SecretName;
use aruna_core::structs::identity::realm::RealmId;

use crate::secrets::owner::{EntryReader, OwnerSecretError};

/// A failed entry read: the register reader refused an answer, or the runner refused a call.
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum EntryError {
    #[error(transparent)]
    Secret(#[from] OwnerSecretError),
    #[error(transparent)]
    Call(#[from] IoError),
}

/// The complete entry of `name`, or `None` when it is absent or incomplete.
#[derive(Debug, PartialEq)]
pub struct ReadEntry {
    reader: EntryReader,
}

impl ReadEntry {
    pub(crate) fn new(realm_id: RealmId, owner: &SecretOwner, name: &SecretName) -> Self {
        Self {
            reader: EntryReader::new(realm_id, owner, name),
        }
    }
}

impl Operation for ReadEntry {
    type Output = Option<SecretEntry>;
    type Error = EntryError;

    fn first(&mut self) -> Result<Next<Self::Output>, EntryError> {
        Ok(Next::Call(IoCall::Effect(Box::new(
            self.reader.start(None),
        ))))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<Self::Output>, EntryError> {
        let IoResult::Event(event) = answer else {
            let got = answer.label();
            return Err(OwnerSecretError::UnexpectedEvent {
                state: "ReadEntry",
                got,
            }
            .into());
        };
        Ok(match self.reader.step(*event, None)? {
            Some(effect) => Next::Call(IoCall::Effect(Box::new(effect))),
            None => Next::Done(self.reader.entry().filter(SecretEntry::complete)),
        })
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};

    use super::*;

    fn read() -> ReadEntry {
        let owner = SecretOwner::Group(ulid::Ulid::from_parts(1, 1));
        let name = SecretName::try_from("token".to_string()).unwrap();
        ReadEntry::new(RealmId::from_bytes([1; 32]), &owner, &name)
    }

    fn storage(event: StorageEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Storage(event)))
    }

    fn effect(next: Next<Option<SecretEntry>>) -> Effect {
        match next {
            Next::Call(IoCall::Effect(effect)) => *effect,
            other => panic!("expected an effect call, got {other:?}"),
        }
    }

    #[test]
    fn absent_entry() {
        let mut operation = read();
        let start = effect(operation.first().unwrap());
        assert!(matches!(start, Effect::Storage(StorageEffect::Read { .. })));
        let view = storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        });
        let page = effect(operation.next(view).unwrap());
        assert!(matches!(page, Effect::Storage(StorageEffect::Iter { .. })));
        let documents = storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        });
        assert_eq!(operation.next(documents), Ok(Next::Done(None)));
    }

    #[test]
    fn refuses_answers() {
        let mut operation = read();
        operation.first().unwrap();
        let failed = storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        assert!(matches!(
            operation.next(failed),
            Err(EntryError::Secret(OwnerSecretError::Storage(_)))
        ));
        let mut operation = read();
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(EntryError::Secret(OwnerSecretError::UnexpectedEvent { .. }))
        ));
    }
}
