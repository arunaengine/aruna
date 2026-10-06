//! Admits parameters beside a bucket generation in the caller's transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::compute::SharedSecret;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{ABE_EPOCH_KEYSPACE, ABE_PARAMETERS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::abe::{AbeEffect, AbeError, AbeEvent, AbeParameters};
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;

#[derive(Clone, Copy, Debug, PartialEq)]
enum State {
    Init,
    Crypto,
    Read,
    Write,
    Done,
}
#[derive(Debug, PartialEq)]
pub struct PrepareAbeOperation {
    effect: Option<AbeEffect>,
    txn: TxnId,
    parameters: Option<AbeParameters>,
    state: State,
    output: Option<Result<(), BlobError>>,
}

impl PrepareAbeOperation {
    pub fn new(
        realm: RealmId,
        node: NodeId,
        key: BucketKeyRef,
        private: SharedSecret,
        txn: TxnId,
    ) -> Self {
        Self {
            effect: Some(AbeEffect::Parameters {
                realm,
                node,
                key,
                private,
            }),
            txn,
            parameters: None,
            state: State::Init,
            output: None,
        }
    }
    fn fail(&mut self, error: BlobError) -> Effects {
        self.effect = None;
        self.output = Some(Err(error));
        self.state = State::Done;
        smallvec![]
    }
}
impl Operation for PrepareAbeOperation {
    type Output = ();
    type Error = BlobError;
    fn start(&mut self) -> Effects {
        let Some(effect) = self.effect.take() else {
            return self.fail(AbeError::Context.into());
        };
        self.state = State::Crypto;
        smallvec![Effect::Blob(BlobEffect::Abe(Box::new(effect)))]
    }
    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (State::Crypto, Event::Blob(BlobEvent::Abe(event))) => {
                let AbeEvent::Parameters(parameters) = *event else {
                    return self.fail(AbeError::Context.into());
                };
                let reads = vec![
                    (
                        ABE_PARAMETERS_KEYSPACE.to_string(),
                        parameters.key.key().into(),
                    ),
                    (
                        ABE_EPOCH_KEYSPACE.to_string(),
                        parameters.key.bucket_id.to_bytes().to_vec().into(),
                    ),
                ];
                self.parameters = Some(parameters);
                self.state = State::Read;
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads,
                    txn_id: Some(self.txn)
                })]
            }
            (State::Read, Event::Storage(StorageEvent::BatchReadResult { values }))
                if values.len() == 2 =>
            {
                let Some(parameters) = self.parameters.as_ref() else {
                    return self.fail(AbeError::Context.into());
                };
                if let Some(value) = &values[0].1
                    && AbeParameters::from_bytes(value).as_ref() != Ok(parameters)
                {
                    return self.fail(AbeError::Parameters.into());
                }
                let bytes = match parameters.to_bytes() {
                    Ok(bytes) => bytes,
                    Err(error) => return self.fail(error.into()),
                };
                let mut writes = vec![(
                    ABE_PARAMETERS_KEYSPACE.to_string(),
                    parameters.key.key().into(),
                    bytes.into(),
                )];
                match &values[1].1 {
                    None => writes.push((
                        ABE_EPOCH_KEYSPACE.to_string(),
                        parameters.key.bucket_id.to_bytes().to_vec().into(),
                        1u64.to_be_bytes().to_vec().into(),
                    )),
                    Some(value) if value.len() == 8 && value.as_ref() != [0; 8] => {}
                    _ => return self.fail(AbeError::Epoch.into()),
                }
                self.state = State::Write;
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes,
                    txn_id: Some(self.txn)
                })]
            }
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.state = State::Done;
                self.output = Some(Ok(()));
                smallvec![]
            }
            (_, Event::Blob(BlobEvent::Error(error))) => self.fail(error),
            (_, Event::Storage(StorageEvent::Error { .. })) => self.fail(AbeError::Stale.into()),
            _ => self.fail(AbeError::Context.into()),
        }
    }
    fn is_complete(&self) -> bool {
        self.state == State::Done
    }
    fn finalize(self) -> Result<(), BlobError> {
        self.output.unwrap_or(Err(AbeError::Context.into()))
    }
    fn abort(&mut self) -> Effects {
        self.effect = None;
        smallvec![]
    }
}

#[cfg(test)]
pub(crate) fn admit_parameters(operation: &mut impl Operation, effects: Effects) -> Effects {
    let [Effect::Blob(BlobEffect::Abe(effect))] = effects.as_slice() else {
        panic!("expected parameter derivation");
    };
    let AbeEffect::Parameters {
        realm,
        node,
        key,
        private,
    } = effect.as_ref()
    else {
        panic!("expected parameter derivation");
    };
    let parameters =
        aruna_core::structs::storage::abe::create_parameters(private.bytes(), *realm, *node, *key)
            .unwrap();
    let effects = operation.step(Event::Blob(BlobEvent::Abe(Box::new(AbeEvent::Parameters(
        parameters,
    )))));
    let [Effect::Storage(StorageEffect::BatchRead { reads, .. })] = effects.as_slice() else {
        panic!("expected parameter admission");
    };
    let values = reads.iter().map(|(_, key)| (key.clone(), None)).collect();
    operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
    operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }))
}
