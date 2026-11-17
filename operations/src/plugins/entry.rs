//! Reads the current complete entry of one owner secret name on this node, as one operation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::events::Event;
use aruna_core::operation::EffectOperation;
use aruna_core::secret_registers::{SecretEntry, SecretOwner};
use aruna_core::secrets::SecretName;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::Effects;
use smallvec::smallvec;

use crate::secrets::owner::{EntryReader, OwnerSecretError};

/// The complete entry of `name`, or `None` when it is absent or incomplete.
#[derive(Debug, PartialEq)]
pub struct ReadEntry {
    reader: EntryReader,
    output: Option<Result<Option<SecretEntry>, OwnerSecretError>>,
}

impl ReadEntry {
    pub(crate) fn new(realm_id: RealmId, owner: &SecretOwner, name: &SecretName) -> Self {
        Self {
            reader: EntryReader::new(realm_id, owner, name),
            output: None,
        }
    }
}

impl EffectOperation for ReadEntry {
    type Output = Option<SecretEntry>;
    type Error = OwnerSecretError;

    fn start(&mut self) -> Effects {
        smallvec![self.reader.start(None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.output.is_some() {
            let got = event.label();
            let state = "ReadEntry";
            self.output = Some(Err(OwnerSecretError::UnexpectedEvent { state, got }));
            return smallvec![];
        }
        match self.reader.step(event, None) {
            Ok(Some(effect)) => smallvec![effect],
            Ok(None) => {
                let entry = self.reader.entry().filter(SecretEntry::complete);
                self.output = Some(Ok(entry));
                smallvec![]
            }
            Err(error) => {
                self.output = Some(Err(error));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Option<SecretEntry>, OwnerSecretError> {
        self.output.unwrap_or(Err(OwnerSecretError::NotFinished))
    }

    /// Reads only, outside any transaction.
    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::effects::Effect;
    use aruna_core::errors::StorageError;
    use aruna_core::events::StorageEvent;
    use aruna_core::operation_check::{check_abort, rejects_events, rejects_finalize};

    use super::*;

    fn read() -> ReadEntry {
        let owner = SecretOwner::Group(ulid::Ulid::from_parts(1, 1));
        let name = SecretName::try_from("token".to_string()).unwrap();
        ReadEntry::new(RealmId::from_bytes([1; 32]), &owner, &name)
    }

    #[test]
    fn absent_entry() {
        let mut operation = read();
        let start = operation.start();
        assert!(matches!(
            start.as_slice(),
            [Effect::Storage(
                aruna_core::effects::StorageEffect::Read { .. }
            )]
        ));
        let view = Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        });
        let page = operation.step(view);
        assert!(matches!(
            page.as_slice(),
            [Effect::Storage(
                aruna_core::effects::StorageEffect::Iter { .. }
            )]
        ));
        rejects_finalize(read());
        let documents = Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        });
        assert!(operation.step(documents).is_empty());
        check_abort(&mut operation, &[]);
        assert_eq!(operation.finalize(), Ok(None));
    }

    #[test]
    fn refuses_events() {
        let failed = Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        let mut operation = read();
        operation.start();
        operation.step(failed);
        assert!(operation.finalize().is_err());
        let refusals = rejects_events(read, aruna_core::operation_check::sample_events, |event| {
            matches!(event, Event::Storage(StorageEvent::ReadResult { .. }))
        });
        assert!(refusals.iter().all(|(_, effects, _)| effects.is_empty()));
    }
}
