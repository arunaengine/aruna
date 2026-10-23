//! Observes the source of a reference version for a refresh that was queued without one.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StagingSourceEffect, StorageEffect};
use aruna_core::errors::{
    ConversionError, SourceResolutionError, StagingSourceError, StorageError,
};
use aruna_core::events::{Event, StagingSourceEvent, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::storage::blob::{BlobVersion, BlobVersionState, VersionKey};
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

use crate::connectors::{ResolveBindingInput, resolve_binding_effect};

#[derive(Debug, Error, PartialEq)]
pub enum ObserveError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Resolve(#[from] SourceResolutionError),
    #[error(transparent)]
    Source(#[from] StagingSourceError),
    #[error("the version is no longer a reference")]
    NotReference,
    #[error("unexpected event in state {state:?}: {received}")]
    UnexpectedEvent {
        state: ObserveState,
        received: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObserveState {
    Init,
    ReadVersion,
    ResolveAccess,
    HeadSource,
    Finish,
    Error,
}

/// Reads a reference version, resolves its binding and heads the source once.
#[derive(Debug, PartialEq)]
pub(crate) struct ObserveSourceOperation {
    version_key: VersionKey,
    state: ObserveState,
    output: Option<Result<SourceMetadata, ObserveError>>,
}

impl ObserveSourceOperation {
    pub(crate) fn new(version_key: VersionKey) -> Self {
        Self {
            version_key,
            state: ObserveState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ObserveError) -> Effects {
        self.state = ObserveState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn step_inner(&mut self, event: Event) -> Result<Effects, ObserveError> {
        match (self.state, event) {
            (ObserveState::ReadVersion, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let version = value
                    .map(|value| BlobVersion::from_bytes(value.as_ref()))
                    .transpose()?;
                let Some(BlobVersion {
                    state: BlobVersionState::Reference { source, .. },
                    ..
                }) = version
                else {
                    return Err(ObserveError::NotReference);
                };
                self.state = ObserveState::ResolveAccess;
                Ok(smallvec![resolve_binding_effect(ResolveBindingInput {
                    source
                })])
            }
            (
                ObserveState::ResolveAccess,
                Event::SubOperation(SubOperationEvent::VersionAccessResolved { result }),
            ) => {
                self.state = ObserveState::HeadSource;
                Ok(smallvec![Effect::StagingSource(
                    StagingSourceEffect::Head { access: result? }
                )])
            }
            (
                ObserveState::HeadSource,
                Event::StagingSource(StagingSourceEvent::HeadResult { metadata }),
            ) => {
                self.state = ObserveState::Finish;
                self.output = Some(Ok(metadata));
                Ok(smallvec![])
            }
            (_, Event::StagingSource(StagingSourceEvent::Error { error })) => Err(error.into()),
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (state, event) => Err(ObserveError::UnexpectedEvent {
                state,
                received: format!("{event:?}"),
            }),
        }
    }
}

impl Operation for ObserveSourceOperation {
    type Output = SourceMetadata;
    type Error = ObserveError;

    fn start(&mut self) -> Effects {
        let key = match self.version_key.to_bytes() {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        self.state = ObserveState::ReadVersion;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if matches!(self.state, ObserveState::Finish | ObserveState::Error) {
            return smallvec![];
        }
        match self.step_inner(event) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ObserveState::Finish | ObserveState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(ObserveError::UnexpectedEvent {
            state: self.state,
            received: "no output".to_string(),
        }))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    // A failed head fails the observation, so the queued refresh is retried later.
    #[test]
    fn failed_head_retried() {
        let mut operation = ObserveSourceOperation::new(VersionKey::new("b", "k", Ulid::nil()));
        operation.start();
        operation.state = ObserveState::HeadSource;

        operation.step(Event::StagingSource(StagingSourceEvent::Error {
            error: StagingSourceError::ReadError("reset".to_string()),
        }));

        assert!(matches!(
            operation.finalize(),
            Err(ObserveError::Source(StagingSourceError::ReadError(_)))
        ));
    }
}
