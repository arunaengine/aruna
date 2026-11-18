//! Admits parameters beside a bucket generation in the caller's transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult};
use aruna_core::compute::SharedSecret;
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{ABE_EPOCH_KEYSPACE, ABE_PARAMETERS_KEYSPACE};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::abe::{AbeEffect, AbeError, AbeEvent, AbeParameters};
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::types::Value;

#[derive(Clone, Copy, Debug, PartialEq)]
enum State {
    Crypto,
    Read,
    Write,
}

/// Derives a generation's parameters and stores them. Its calls run in the transaction of the
/// operation that holds it.
#[derive(Debug, PartialEq)]
pub(crate) struct PrepareAbe {
    effect: Option<AbeEffect>,
    parameters: Option<AbeParameters>,
    state: State,
}

impl PrepareAbe {
    pub(crate) fn new(
        realm: RealmId,
        node: NodeId,
        key: BucketKeyRef,
        private: SharedSecret,
    ) -> Self {
        Self {
            effect: Some(AbeEffect::Parameters {
                realm,
                node,
                key,
                private,
            }),
            parameters: None,
            state: State::Crypto,
        }
    }

    pub(crate) fn first(&mut self) -> Result<IoCall, BlobError> {
        let effect = self.effect.take().ok_or(AbeError::Context)?;
        Ok(Effect::Blob(BlobEffect::Abe(Box::new(effect))).into())
    }

    /// The next call, or `None` once the parameters are written.
    pub(crate) fn next<E>(&mut self, result: IoResult) -> Result<Option<IoCall>, E>
    where
        E: From<IoError> + From<BlobError>,
    {
        match self.state {
            State::Crypto => Ok(Some(self.derived(result.event()?)?)),
            State::Read => Ok(Some(self.stored(result.values()?)?)),
            State::Write => {
                result.done()?;
                Ok(None)
            }
        }
    }

    fn derived(&mut self, event: Event) -> Result<IoCall, BlobError> {
        let parameters = match event {
            Event::Blob(BlobEvent::Abe(event)) => match *event {
                AbeEvent::Parameters(parameters) => parameters,
                _ => return Err(AbeError::Context.into()),
            },
            Event::Blob(BlobEvent::Error(error)) => return Err(error),
            _ => return Err(AbeError::Context.into()),
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
        Ok(IoCall::Read(reads))
    }

    fn stored(&mut self, values: Vec<Option<Value>>) -> Result<IoCall, BlobError> {
        let [stored, epoch] = <[Option<Value>; 2]>::try_from(values)
            .map_err(|_| BlobError::from(AbeError::Context))?;
        let parameters = self.parameters.as_ref().ok_or(AbeError::Context)?;
        if let Some(value) = &stored
            && AbeParameters::from_bytes(value).as_ref() != Ok(parameters)
        {
            return Err(AbeError::Parameters.into());
        }
        let mut writes = vec![(
            ABE_PARAMETERS_KEYSPACE.to_string(),
            parameters.key.key().into(),
            parameters.to_bytes()?.into(),
        )];
        match &epoch {
            None => writes.push((
                ABE_EPOCH_KEYSPACE.to_string(),
                parameters.key.bucket_id.to_bytes().to_vec().into(),
                1u64.to_be_bytes().to_vec().into(),
            )),
            Some(value) if value.len() == 8 && value.as_ref() != [0; 8] => {}
            _ => return Err(AbeError::Epoch.into()),
        }
        self.state = State::Write;
        Ok(IoCall::Write(writes))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use aruna_core::calls::Next;
    use aruna_core::compute::SecretBytes;
    use aruna_core::operation::Operation;
    use aruna_core::structs::storage::abe::create_parameters;
    use ulid::Ulid;

    /// Answers the admission an operation asked for in `turn` and returns its next turn.
    pub(crate) fn admit_parameters<O: Operation>(
        operation: &mut O,
        turn: Result<Next<O::Output>, O::Error>,
    ) -> Result<Next<O::Output>, O::Error> {
        let Ok(Next::Call(IoCall::Effect(effect))) = turn else {
            panic!("expected parameter derivation");
        };
        let Effect::Blob(BlobEffect::Abe(effect)) = *effect else {
            panic!("expected parameter derivation");
        };
        let AbeEffect::Parameters {
            realm,
            node,
            key,
            private,
        } = *effect
        else {
            panic!("expected parameter derivation");
        };
        let parameters = create_parameters(private.bytes(), realm, node, key).unwrap();
        let derived = Event::Blob(BlobEvent::Abe(Box::new(AbeEvent::Parameters(parameters))));
        let turn = operation.next(IoResult::Event(Box::new(derived)));
        let Ok(Next::Call(IoCall::Read(reads))) = turn else {
            panic!("expected parameter admission");
        };
        let values = reads.iter().map(|_| None).collect();
        let turn = operation.next(IoResult::Values(values));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Write(_)))));
        operation.next(IoResult::Done)
    }

    fn prepare() -> (PrepareAbe, IoResult) {
        let realm = RealmId::from_bytes([1; 32]);
        let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let key = BucketKeyRef::new(Ulid::from_parts(3, 3), 1);
        let secret = SecretBytes::new(vec![4; 32]);
        let parameters = create_parameters(&secret, realm, node, key).unwrap();
        let derived = Event::Blob(BlobEvent::Abe(Box::new(AbeEvent::Parameters(parameters))));
        let abe = PrepareAbe::new(realm, node, key, SharedSecret::new(secret));
        (abe, IoResult::Event(Box::new(derived)))
    }

    #[test]
    fn admits_new_parameters() {
        let (mut abe, derived) = prepare();
        assert!(matches!(abe.first(), Ok(IoCall::Effect(_))));
        let read = abe.next::<BlobIo>(derived).unwrap();
        assert!(matches!(read, Some(IoCall::Read(_))));
        let write = abe.next::<BlobIo>(IoResult::Values(vec![None, None]));
        let Ok(Some(IoCall::Write(rows))) = write else {
            panic!("expected the parameter and epoch rows");
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(abe.next::<BlobIo>(IoResult::Done).unwrap(), None);
    }

    #[test]
    fn refuses_other_parameters() {
        let (mut abe, derived) = prepare();
        abe.first().unwrap();
        abe.next::<BlobIo>(derived).unwrap();
        let answer = IoResult::Values(vec![Some(vec![1].into()), None]);
        assert_eq!(
            abe.next::<BlobIo>(answer),
            Err(BlobIo::Blob(AbeError::Parameters.into()))
        );
    }

    #[test]
    fn refuses_zero_epoch() {
        let (mut abe, derived) = prepare();
        abe.first().unwrap();
        abe.next::<BlobIo>(derived).unwrap();
        let answer = IoResult::Values(vec![None, Some(vec![0; 8].into())]);
        assert_eq!(
            abe.next::<BlobIo>(answer),
            Err(BlobIo::Blob(AbeError::Epoch.into()))
        );
    }

    #[derive(Debug, PartialEq)]
    enum BlobIo {
        Blob(BlobError),
        Io(IoError),
    }

    impl From<BlobError> for BlobIo {
        fn from(error: BlobError) -> Self {
            Self::Blob(error)
        }
    }

    impl From<IoError> for BlobIo {
        fn from(error: IoError) -> Self {
            Self::Io(error)
        }
    }
}
