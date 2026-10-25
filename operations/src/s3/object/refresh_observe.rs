//! Observes the source of a reference version for a refresh that was queued without one.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StagingSourceEffect, StorageEffect};
use aruna_core::errors::{
    ConversionError, SourceResolutionError, StagingSourceError, StorageError,
};
use aruna_core::events::{Event, StagingSourceEvent, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::storage::blob::{BlobVersion, BlobVersionState, BucketInfo, VersionKey};
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

impl ObserveError {
    /// The observed version is gone or can no longer be resolved; source and storage failures
    /// pass and are retried.
    pub(crate) fn is_obsolete(&self) -> bool {
        match self {
            Self::NotReference | Self::Conversion(_) => true,
            Self::Resolve(error) => !matches!(
                error,
                SourceResolutionError::StorageError(_) | SourceResolutionError::ResolveFailed
            ),
            Self::Storage(_) | Self::Source(_) | Self::UnexpectedEvent { .. } => false,
        }
    }
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
            (
                ObserveState::ReadVersion,
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => {
                let mut values = values.into_iter().map(|(_, value)| value);
                let (version, bucket) = (values.next().flatten(), values.next().flatten());
                let version = version
                    .map(|value| BlobVersion::from_bytes(value.as_ref()))
                    .transpose()?;
                // The bucket's group may read a reference through its own connector of the root.
                let group_id = bucket
                    .map(|value| BucketInfo::from_bytes(value.as_ref()))
                    .transpose()?
                    .map(|bucket| bucket.group_id);
                let Some(BlobVersion {
                    state: BlobVersionState::Reference { source, .. },
                    ..
                }) = version
                else {
                    return Err(ObserveError::NotReference);
                };
                self.state = ObserveState::ResolveAccess;
                Ok(smallvec![resolve_binding_effect(ResolveBindingInput {
                    source,
                    group_id,
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
        let bucket = self.version_key.bucket.as_bytes().to_vec();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (BLOB_VERSIONS_KEYSPACE.to_string(), key.into()),
                (S3_BUCKET_KEYSPACE.to_string(), bucket.into()),
            ],
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

    // A member's reference names no connector, so the bucket's group resolves its source.
    #[test]
    fn resolves_with_group() {
        use aruna_core::structs::execution::source_access::SourceMetadata;
        use aruna_core::structs::execution::source_connector::SourceConnectorKind;
        use aruna_core::structs::execution::staging::{
            PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
        };
        use aruna_core::structs::storage::format::Compression;
        use std::time::SystemTime;

        let group_id = Ulid::from_bytes([2; 16]);
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: Default::default(),
                source_path: "folder/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
            },
            connector_id: None,
        };
        let observation = SourceMetadata {
            content_length: 4,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        };
        let at = SystemTime::UNIX_EPOCH;
        let version = BlobVersion::reference(source, observation, at, Default::default(), at);
        let bucket = BucketInfo {
            group_id,
            created_at: at,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let mut operation = ObserveSourceOperation::new(VersionKey::new("b", "k", Ulid::nil()));
        operation.start();

        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    b"v".to_vec().into(),
                    Some(version.to_bytes().unwrap().into()),
                ),
                (
                    b"b".to_vec().into(),
                    Some(bucket.to_bytes().unwrap().into()),
                ),
            ],
        }));

        assert_eq!(operation.state, ObserveState::ResolveAccess);
        assert!(format!("{effects:?}").contains(&format!("group_id: Some({group_id:?})")));
    }

    // A failed head fails the observation, so the queued refresh is retried later.
    #[test]
    fn failed_head_retried() {
        let mut operation = ObserveSourceOperation::new(VersionKey::new("b", "k", Ulid::nil()));
        operation.start();
        operation.state = ObserveState::HeadSource;

        operation.step(Event::StagingSource(StagingSourceEvent::Error {
            error: StagingSourceError::ReadError("reset".to_string()),
        }));

        let error = operation.finalize().unwrap_err();
        assert!(matches!(
            error,
            ObserveError::Source(StagingSourceError::ReadError(_))
        ));
        assert!(!error.is_obsolete());
        assert!(ObserveError::NotReference.is_obsolete());
        assert!(
            !ObserveError::Resolve(SourceResolutionError::StorageError(StorageError::Timeout))
                .is_obsolete()
        );
    }
}
