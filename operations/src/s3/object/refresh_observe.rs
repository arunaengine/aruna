//! Observes the source of a reference version for a refresh that was queued without one.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::errors::{
    ConversionError, SourceResolutionError, StagingSourceError, StorageError,
};
use aruna_core::events::{Event, StagingSourceEvent};
use aruna_core::keyspaces::{BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_access::{ResolvedSourceAccess, SourceMetadata};
use aruna_core::structs::storage::blob::{BlobVersion, BlobVersionState, BucketInfo, VersionKey};
use thiserror::Error;

use crate::connectors::{ResolveBindingInput, ResolveBindingOperation};

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
    #[error("could not observe the source: {0}")]
    Io(IoError),
}

impl From<IoError> for ObserveError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
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
            Self::Storage(_) | Self::Source(_) | Self::Io(_) => false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObserveState {
    ReadVersion,
    ResolveAccess,
    HeadSource,
}

type Step = Result<Next<SourceMetadata>, ObserveError>;

/// Reads a reference version, resolves its binding and heads the source once.
#[derive(Debug, PartialEq)]
pub(crate) struct ObserveSourceOperation {
    version_key: VersionKey,
    state: ObserveState,
}

impl ObserveSourceOperation {
    pub(crate) fn new(version_key: VersionKey) -> Self {
        Self {
            version_key,
            state: ObserveState::ReadVersion,
        }
    }

    fn version_read(&mut self, result: IoResult) -> Step {
        let mut values = result.values()?.into_iter();
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
        let resolve = ResolveBindingOperation::new(ResolveBindingInput { source, group_id });
        Ok(Next::Call(IoCall::child(resolve)))
    }
}

impl Operation for ObserveSourceOperation {
    type Output = SourceMetadata;
    type Error = ObserveError;

    fn first(&mut self) -> Step {
        let key = self.version_key.to_bytes()?;
        let bucket = self.version_key.bucket.as_bytes().to_vec();
        Ok(Next::Call(IoCall::Read(vec![
            (BLOB_VERSIONS_KEYSPACE.to_string(), key.into()),
            (S3_BUCKET_KEYSPACE.to_string(), bucket.into()),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.state {
            ObserveState::ReadVersion => return self.version_read(result),
            ObserveState::ResolveAccess => {
                let access = result.child::<ResolvedSourceAccess, SourceResolutionError>()??;
                self.state = ObserveState::HeadSource;
                let head = StagingSourceEffect::Head { access };
                return Ok(Next::Call(IoCall::Effect(Box::new(Effect::StagingSource(
                    head,
                )))));
            }
            ObserveState::HeadSource => {}
        }
        match (self.state, result.event()?) {
            (
                ObserveState::HeadSource,
                Event::StagingSource(StagingSourceEvent::HeadResult { metadata }),
            ) => Ok(Next::Done(metadata)),
            (_, Event::StagingSource(StagingSourceEvent::Error { error })) => Err(error.into()),
            (_, event) => Err(IoError::Unexpected {
                expected: "the version access or the source head",
                got: event.label(),
            }
            .into()),
        }
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
                root: None,
            },
            connector_id: None,
            mirrors: Vec::new(),
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
        let _ = operation.first();

        let next = operation.next(IoResult::Values(vec![
            Some(version.to_bytes().unwrap().into()),
            Some(bucket.to_bytes().unwrap().into()),
        ]));

        assert_eq!(operation.state, ObserveState::ResolveAccess);
        assert!(format!("{next:?}").contains(&format!("group_id: Some({group_id:?})")));
    }

    // A failed head fails the observation, so the queued refresh is retried later.
    #[test]
    fn failed_head_retried() {
        let mut operation = ObserveSourceOperation::new(VersionKey::new("b", "k", Ulid::nil()));
        operation.state = ObserveState::HeadSource;

        let failed = Event::StagingSource(StagingSourceEvent::Error {
            error: StagingSourceError::ReadError("reset".to_string()),
        });
        let error = operation
            .next(IoResult::Event(Box::new(failed)))
            .unwrap_err();
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
