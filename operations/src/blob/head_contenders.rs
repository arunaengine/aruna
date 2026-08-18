//! Responder-local audit of the versions that claimed one head generation.
//!
//! The rows are written next to every contended head write. Reading them back
//! is what exposes "this object had concurrent versions" without putting a
//! policy or a second head into S3 GET/HEAD.

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_CONTENDER_KEYSPACE, BLOB_HEAD_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{BlobHeadKey, CurrentVersionPointer, HeadContenderKey, VersionKey};
use aruna_core::types::{Effects, Key, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

/// Rows read per scan round.
pub const CONTENDER_PAGE_LIMIT: usize = 256;
/// Rounds one audit may walk. A generation with more claimants than this is
/// reported truncated rather than read unbounded.
pub const MAX_CONTENDER_PAGES: usize = 16;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadContendersInput {
    pub version: VersionKey,
    /// Generation to audit. Absent audits whatever the current head names.
    pub generation: Option<u64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HeadContendersResult {
    pub generation: u64,
    /// Every VersionId this node observed claiming the generation, ascending.
    /// The audited version is included when it was itself a claimant.
    pub contenders: Vec<Ulid>,
    /// True when the generation holds more rows than the bound allows.
    pub truncated: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum HeadContendersError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event in state {0}")]
    InvalidEvent(&'static str),
}

#[derive(Debug, Eq, PartialEq)]
enum ContenderState {
    Init,
    ReadHead,
    Scan,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct HeadContendersOperation {
    input: HeadContendersInput,
    txn_id: Option<TxnId>,
    state: ContenderState,
    generation: u64,
    cursor: Option<Key>,
    pages: usize,
    contenders: Vec<Ulid>,
    truncated: bool,
    output: Option<Result<HeadContendersResult, HeadContendersError>>,
}

impl HeadContendersOperation {
    pub fn new(input: HeadContendersInput) -> Self {
        Self {
            input,
            txn_id: None,
            state: ContenderState::Init,
            generation: 0,
            cursor: None,
            pages: 0,
            contenders: Vec::new(),
            truncated: false,
            output: None,
        }
    }

    pub fn with_txn(mut self, txn_id: TxnId) -> Self {
        self.txn_id = Some(txn_id);
        self
    }

    fn scan(&mut self) -> Effects {
        let prefix = match HeadContenderKey::generation_prefix(
            &self.input.version.bucket,
            &self.input.version.key,
            self.generation,
        ) {
            Ok(prefix) => prefix,
            Err(error) => return self.fail(error.into()),
        };
        self.pages += 1;
        self.state = ContenderState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_HEAD_CONTENDER_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: self.cursor.clone().map(IterStart::After),
            limit: CONTENDER_PAGE_LIMIT,
            txn_id: self.txn_id,
        })]
    }

    fn handle_head(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(HeadContendersError::InvalidEvent("ReadHead"));
        };
        // No head means no generation was ever contended for this object.
        let Some(value) = value else {
            return self.complete();
        };
        match CurrentVersionPointer::from_bytes(value.as_ref()) {
            Ok(pointer) => {
                self.generation = pointer.generation;
                self.scan()
            }
            Err(error) => self.fail(error.into()),
        }
    }

    fn handle_page(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.fail(HeadContendersError::InvalidEvent("Scan"));
        };
        for (key, _) in values {
            match HeadContenderKey::from_bytes(key.as_ref()) {
                Ok(row) => self.contenders.push(row.version_id),
                Err(error) => return self.fail(error.into()),
            }
        }
        self.cursor = next_start_after;
        match self.cursor.is_some() {
            true if self.pages >= MAX_CONTENDER_PAGES => {
                self.truncated = true;
                self.complete()
            }
            true => self.scan(),
            false => self.complete(),
        }
    }

    fn complete(&mut self) -> Effects {
        self.output = Some(Ok(HeadContendersResult {
            generation: self.generation,
            contenders: std::mem::take(&mut self.contenders),
            truncated: self.truncated,
        }));
        self.state = ContenderState::Finish;
        smallvec![]
    }

    fn fail(&mut self, error: HeadContendersError) -> Effects {
        self.output = Some(Err(error));
        self.state = ContenderState::Error;
        smallvec![]
    }
}

impl Operation for HeadContendersOperation {
    type Output = HeadContendersResult;
    type Error = HeadContendersError;

    fn start(&mut self) -> Effects {
        let Some(generation) = self.input.generation else {
            let key = BlobHeadKey::new(&self.input.version.bucket, &self.input.version.key);
            let key = match key.to_bytes() {
                Ok(key) => key,
                Err(error) => return self.fail(error.into()),
            };
            self.state = ContenderState::ReadHead;
            return smallvec![Effect::Storage(StorageEffect::Read {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: key.into(),
                txn_id: self.txn_id,
            })];
        };
        self.generation = generation;
        self.scan()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ContenderState::ReadHead => self.handle_head(event),
            ContenderState::Scan => self.handle_page(event),
            ContenderState::Init | ContenderState::Finish | ContenderState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ContenderState::Finish | ContenderState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(HeadContendersError::InvalidEvent("Finish")))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{HeadContendersInput, HeadContendersOperation, MAX_CONTENDER_PAGES};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{CurrentVersionPointer, HeadContenderKey, VersionKey};
    use ulid::Ulid;

    fn version() -> VersionKey {
        VersionKey::new("bucket", "object.txt", Ulid::from_bytes([1u8; 16]))
    }

    fn operation(generation: Option<u64>) -> HeadContendersOperation {
        HeadContendersOperation::new(HeadContendersInput {
            version: version(),
            generation,
        })
    }

    fn page(ids: &[Ulid], cursor: bool) -> Event {
        let values: Vec<_> = ids
            .iter()
            .map(|id| {
                let key = HeadContenderKey::new("bucket", "object.txt", 4, *id);
                (
                    aruna_core::types::Key::from(key.to_bytes().expect("key encodes")),
                    aruna_core::types::Value::from(Vec::<u8>::new()),
                )
            })
            .collect();
        let next = values.last().map(|(key, _)| key.clone()).filter(|_| cursor);
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: next,
        })
    }

    #[test]
    fn reads_head_generation() {
        // Without an explicit generation the audit follows the current head.
        let mut operation = operation(None);
        let effects = operation.start();
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::Read { .. }))
        ));

        let pointer = CurrentVersionPointer::new_with_generation(Ulid::from_bytes([1u8; 16]), 4);
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(pointer.to_bytes().expect("pointer encodes").into()),
        }));
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::Iter { .. }))
        ));

        let first = Ulid::from_bytes([1u8; 16]);
        let second = Ulid::from_bytes([2u8; 16]);
        operation.step(page(&[first, second], false));
        let result = operation.finalize().expect("audit completes");
        assert_eq!(result.generation, 4);
        assert_eq!(result.contenders, vec![first, second]);
        assert!(!result.truncated);
    }

    #[test]
    fn missing_head_is_empty() {
        let mut operation = operation(None);
        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        }));
        assert!(
            operation
                .finalize()
                .expect("audit completes")
                .contenders
                .is_empty()
        );
    }

    #[test]
    fn bounds_the_walk() {
        // A generation with more rows than the bound is reported truncated
        // instead of scanned without end.
        let mut operation = operation(Some(4));
        operation.start();
        for _ in 0..MAX_CONTENDER_PAGES {
            operation.step(page(&[Ulid::from_bytes([7u8; 16])], true));
        }
        let result = operation.finalize().expect("audit completes");
        assert!(result.truncated);
        assert_eq!(result.contenders.len(), MAX_CONTENDER_PAGES);
    }

    #[test]
    fn rejects_wrong_event() {
        let mut operation = operation(Some(4));
        operation.start();
        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));
        assert!(operation.finalize().is_err());
    }
}
