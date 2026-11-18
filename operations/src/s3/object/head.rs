//! Runs S3 HeadObject: resolves object metadata without reading data.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::connectors::{ResolveBindingInput, resolve_binding_effect};
use crate::s3::object::lookup::{FoundVersion, VersionLookup, VersionLookupError};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::errors::{
    ConversionError, SourceResolutionError, StagingSourceError, StorageError,
};
use aruna_core::events::{Event, StagingSourceEvent, SubOperationEvent};
use aruna_core::managed_copy::ManagedCopyError;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::multipart::MultipartChecksumType;
use std::collections::HashMap;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeadObjectState {
    Lookup,
    ResolveReferenceAccess,
    HeadReferenceSource,
}

#[derive(Debug, Error, PartialEq)]
pub enum HeadObjectError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received}]")]
    InvalidStateEvent {
        state: HeadObjectState,
        expected: &'static str,
        received: &'static str,
    },
    #[error("The specified key does not exist.")]
    NoSuchKey,
    #[error("The specified version does not exist.")]
    NoSuchVersion,
    #[error("The specified version is a delete marker.")]
    DeleteMarker,
    #[error(transparent)]
    ResolveReferenceError(#[from] SourceResolutionError),
    #[error(transparent)]
    StagingSourceError(#[from] StagingSourceError),
    #[error(transparent)]
    ManagedCopyError(#[from] ManagedCopyError),
    #[error("HeadObject failed")]
    HeadObjectFailed,
}

impl From<IoError> for HeadObjectError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

impl From<VersionLookupError> for HeadObjectError {
    fn from(error: VersionLookupError) -> Self {
        match error {
            VersionLookupError::Storage(error) => Self::StorageError(error),
            VersionLookupError::Conversion(error) => Self::ConversionError(error),
            VersionLookupError::Managed(error) => Self::ManagedCopyError(error),
            VersionLookupError::NoSuchKey => Self::NoSuchKey,
            VersionLookupError::NoSuchVersion => Self::NoSuchVersion,
            VersionLookupError::DeleteMarker => Self::DeleteMarker,
            VersionLookupError::Failed => Self::HeadObjectFailed,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct HeadObjectInput {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<Ulid>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HeadObjectResult {
    pub location: Option<BackendLocation>,
    pub metadata: HashMap<String, String>,
    pub source_metadata: Option<SourceMetadata>,
    pub last_refresh: Option<SystemTime>,
    pub version_created_at: Option<SystemTime>,
    pub version_id: Option<Ulid>,
    pub resolved_version_id: Option<Ulid>,
    pub checksum_type: MultipartChecksumType,
    pub composite_hashes: HashMap<String, Vec<u8>>,
    pub part_count: Option<usize>,
    /// Refs stored on the version that was described. A derived write unions
    /// them with its destination default and never drops one.
    pub source_policies: Vec<PlacementPolicyRef>,
    /// Where the version's bytes come from, for a reference and a snapshot alike.
    pub source_binding: Option<VersionSourceBinding>,
}

type HeadTurn = Result<Next<HeadObjectResult>, HeadObjectError>;

/// Reads the version in one snapshot in a child; a reference to another Aruna node then asks
/// that source for its current metadata, outside the snapshot.
#[derive(Debug, PartialEq)]
pub struct HeadObjectOperation {
    input: HeadObjectInput,
    state: HeadObjectState,
    found: Option<FoundVersion>,
}

impl HeadObjectOperation {
    pub fn new(input: HeadObjectInput) -> Self {
        Self {
            input,
            state: HeadObjectState::Lookup,
            found: None,
        }
    }

    fn result(&self, found: FoundVersion) -> HeadObjectResult {
        let summary = found.summary;
        HeadObjectResult {
            location: found.location,
            metadata: found.metadata,
            source_metadata: found.source_metadata,
            last_refresh: found.last_refresh,
            version_created_at: found.created_at,
            version_id: found.version_id.or(self.input.version_id),
            resolved_version_id: found.version_id,
            checksum_type: (summary.as_ref())
                .map_or(MultipartChecksumType::FullObject, |it| it.checksum_type),
            part_count: summary.as_ref().map(|summary| summary.part_count),
            composite_hashes: summary.map(|it| it.composite_hashes).unwrap_or_default(),
            source_policies: found.source_policies,
            source_binding: found.source_binding,
        }
    }

    fn invalid(&self, expected: &'static str, event: Event) -> HeadTurn {
        Err(HeadObjectError::InvalidStateEvent {
            state: self.state.clone(),
            expected,
            received: event.label(),
        })
    }

    fn looked_up(&mut self, outcome: Result<FoundVersion, VersionLookupError>) -> HeadTurn {
        let mut found = outcome?;
        let Some(source) = found.native_reference.take() else {
            return Ok(Next::Done(self.result(found)));
        };
        self.found = Some(found);
        self.state = HeadObjectState::ResolveReferenceAccess;
        let resolve = resolve_binding_effect(ResolveBindingInput {
            source,
            group_id: None,
        });
        Ok(Next::Call(resolve.into()))
    }

    fn access_resolved(&mut self, event: Event) -> HeadTurn {
            let error = self.lookup_error("Event::Storage(StorageEvent::BatchReadResult)", err);
            return self.emit_error(error);
        }
        self.state = HeadObjectState::GetBlobLocation;
        smallvec![pending_location_read(&archive, self.txn_id)]
    }

    fn commit_reference(&mut self, source: VersionSourceBinding) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(HeadObjectError::NoTransactionFound);
        };
        self.reference_source = Some(source);
        self.state = HeadObjectState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn read_blob_location(&mut self, key: BlobLocationKey) -> Effects {
        self.state = HeadObjectState::GetBlobLocation;
        smallvec![blob_location_read(&key, self.txn_id)]
    }

    /// A governed version is only serveable from a registered local copy, so an
    /// unregistered or quarantined copy fails closed before any metadata is served.
    fn check_managed_copy(&mut self, version_id: Ulid, location_key: BlobLocationKey) -> Effects {
        let check = match begin_copy_check(
            &self.input.bucket,
            &self.input.key,
            version_id,
            location_key,
            self.txn_id,
        ) {
            Ok(check) => check,
            Err(err) => return self.emit_error(err.into()),
        };
        self.pending_copy = Some(check.copy_key);
        self.pending_location = Some(check.location_key);
        self.state = HeadObjectState::CheckManagedCopy;
        smallvec![check.effect]
    }

    fn handle_managed_copy(&mut self, event: Event) -> Effects {
        let key = match finish_copy_check(
            event,
            &mut self.pending_copy,
            &mut self.pending_location,
            &self.source_policies,
            ExpectedNode::Subject,
        ) {
            Ok(key) => key,
            Err(err) => {
                let error = self.lookup_error("Event::Storage(StorageEvent::BatchReadResult)", err);
                return self.emit_error(error);
            }
        };
        self.read_blob_location(key)
    }

    fn location_read(&mut self, event: Event) -> Effects {
        let location = match location_from_read(event) {
            Ok(location) => location,
            Err(err) => {
                let error = self.lookup_error("Event::Storage(StorageEvent::ReadResult)", err);
                return self.emit_error(error);
            }
        };

        self.read_multipart_summary(location, self.resolved_version_id)
    }

    fn read_multipart_summary(
        &mut self,
        location: Option<BackendLocation>,
        resolved_version_id: Option<Ulid>,
    ) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(HeadObjectError::NoTransactionFound);
        };

        self.location = location;
        self.resolved_version_id = resolved_version_id;

        let Some(version_id) = resolved_version_id else {
            return self.finish_lookup();
        };

        let effect = match multipart_summary_read(version_id, Some(txn_id)) {
            Ok(effect) => effect,
            Err(err) => return self.emit_error(HeadObjectError::ConversionError(err)),
        };

        self.state = HeadObjectState::ReadMultipartSummary;
        smallvec![effect]
    }

    fn summary_read(&mut self, event: Event) -> Effects {
        let summary = match summary_from_read(event) {
            Ok(summary) => summary,
            Err(err) => {
                let error = self.lookup_error("Event::Storage(StorageEvent::ReadResult)", err);
                return self.emit_error(error);
            }
        };

        if let Some(summary) = summary {
            self.checksum_type = summary.checksum_type;
            self.composite_hashes = summary.composite_hashes;
            self.part_count = Some(summary.part_count);
        }

        self.finish_lookup()
    }

    fn finish_lookup(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(HeadObjectError::NoTransactionFound);
        };
        self.state = HeadObjectState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event {
            self.txn_id = None;
            if let Some(source) = self.reference_source.take() {
                self.state = HeadObjectState::ResolveReferenceAccess;
                return smallvec![resolve_binding_effect(ResolveBindingInput {
                    source,
                    group_id: None,
                },)];
            }
            self.state = HeadObjectState::Finish;
            self.output = Some(Ok(HeadObjectResult {
                location: self.location.clone(),
                metadata: self.metadata.clone(),
                source_metadata: self.source_metadata.clone(),
                last_refresh: self.last_refresh,
                version_created_at: self.version_created_at,
                version_id: self.resolved_version_id.or(self.input.version_id),
                resolved_version_id: self.resolved_version_id,
                checksum_type: self.checksum_type,
                composite_hashes: self.composite_hashes.clone(),
                part_count: self.part_count,
                source_policies: self.source_policies.clone(),
                source_binding: self.source_binding.clone(),
            }));
            smallvec![]
        } else {
            self.emit_error(HeadObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: (event).label(),
            })
        }
    }

    fn handle_reference_access(&mut self, event: Event) -> Effects {
        match event {
            Event::SubOperation(SubOperationEvent::VersionAccessResolved {
                result: Ok(access),
            }) => {
                self.state = HeadObjectState::HeadReferenceSource;
                smallvec![Effect::StagingSource(StagingSourceEffect::Head { access })]
            }
            Event::SubOperation(SubOperationEvent::VersionAccessResolved {
                result: Err(error),
            }) => self.emit_error(error.into()),
            other => self.emit_error(HeadObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::SubOperation(SubOperationEvent::VersionSourceAccessResolved)",
                received: (other).label(),
            }),
        }
    }

    fn handle_reference_head(&mut self, event: Event) -> Effects {
        match event {
            Event::StagingSource(StagingSourceEvent::HeadResult { metadata }) => {
                self.source_metadata = Some(metadata);
                self.last_refresh = Some(SystemTime::now());
                self.state = HeadObjectState::Finish;
                self.output = Some(Ok(HeadObjectResult {
                    location: None,
                    metadata: self.metadata.clone(),
                    source_metadata: self.source_metadata.clone(),
                    last_refresh: self.last_refresh,
                    version_created_at: None,
                    version_id: self.resolved_version_id.or(self.input.version_id),
                    resolved_version_id: self.resolved_version_id,
                    checksum_type: self.checksum_type,
                    composite_hashes: self.composite_hashes.clone(),
                    part_count: self.part_count,
                    source_policies: self.source_policies.clone(),
                    source_binding: self.source_binding.clone(),
                }));
                smallvec![]
            }
            Event::StagingSource(StagingSourceEvent::Error { error }) => {
                self.emit_error(error.into())
            }
            other => self.emit_error(HeadObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::StagingSource(StagingSourceEvent::HeadResult)",
                received: (other).label(),
            }),
        }
    }
}

impl EffectOperation for HeadObjectOperation {
    type Output = HeadObjectResult;
    type Error = HeadObjectError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.emit_error(HeadObjectError::StorageError(error));
        }

        match self.state {
            HeadObjectState::Init => self.handle_init(),
            HeadObjectState::StartTransaction => self.handle_transaction_started(event),
            HeadObjectState::GetVersion => self.handle_received_version(event),
            HeadObjectState::CheckManagedCopy => self.handle_managed_copy(event),
            HeadObjectState::CheckPendingCopy => self.handle_pending_copy(event),
            HeadObjectState::GetBlobLocation => self.location_read(event),
            HeadObjectState::GetCurrentVersion => self.current_version_received(event),
            HeadObjectState::ReadMultipartSummary => self.summary_read(event),
            HeadObjectState::CommitTransaction => self.handle_transaction_committed(event),
            HeadObjectState::ResolveReferenceAccess => self.handle_reference_access(event),
            HeadObjectState::HeadReferenceSource => self.handle_reference_head(event),
            HeadObjectState::Finish | HeadObjectState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, HeadObjectState::Finish | HeadObjectState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Err(HeadObjectError::NotFinished),
        }
    }

    // A missing key or version is a normal answer, for example when an import checks a key first.
    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            HeadObjectError::NoSuchKey
                | HeadObjectError::NoSuchVersion
                | HeadObjectError::DeleteMarker
        )
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_blob::blob::BlobHandler;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    };
    use aruna_core::structs::checksum::{HASH_BLAKE3, HASH_MD5};
    use aruna_core::structs::execution::source_access::{ResolvedSourceAccess, SourceMetadata};
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    use aruna_core::structs::storage::blob::BackendConfig;
    use aruna_core::structs::storage::blob::BackendLocation;
    use aruna_core::structs::storage::blob::{
        Backend, BackendRef, BlobHeadKey, BlobVersion, CurrentVersionPointer, VersionKey,
    };
    use aruna_core::structs::storage::format::EncodingClass;
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_net::{NetConfig, NetHandle};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::tempdir;

    fn location_with_hash() -> BackendLocation {
        let mut hashes = HashMap::new();
        hashes.insert(HASH_BLAKE3.to_string(), vec![2; 32]);
        hashes.insert(HASH_MD5.to_string(), vec![1; 16]);
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "mybucket".to_string(),
            backend_path: "hello.txt".to_string(),
            ulid: Ulid::generate(),
            format: StoredFormat::default(),
            created_at: SystemTime::now(),
            created_by: Default::default(),
            staging: false,
            partial: false,
            blob_size: 5,
            hashes,
        }
    }

    #[test]
    fn pending_reads_location() {
        use aruna_core::keyspaces::PENDING_LOCATION_KEYSPACE;
        use aruna_core::structs::placement::policy::PlacementPolicyRef;
        use aruna_core::structs::storage::blob::ArchiveKey;
        use aruna_core::structs::storage::encryption::BucketKeyRef;
        use aruna_core::structs::storage::format::PithosLayout;

        let mut location = location_with_hash();
        location.hashes.remove(HASH_BLAKE3);
        let layout = PithosLayout {
            stored_size: 70,
            metadata_digest: [3; 32],
            storage_generation: 0,
        };
        location.format = StoredFormat::pithos(layout, BucketKeyRef::new(Ulid::generate(), 1));
        let archive = ArchiveKey::of(&location);
        let version_id = Ulid::generate();
        let version = BlobVersion::pending(
            archive.clone(),
            location.created_at,
            location.created_by,
            None,
        );
        let input = || HeadObjectInput {
            bucket: "mybucket".to_string(),
            key: "hello.txt".to_string(),
            version_id: Some(version_id),
        };

        let mut operation = HeadObjectOperation::new(input());
        operation.txn_id = Some(Ulid::generate());
        let effects = operation.read_version(version_id, version.clone(), true);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, key, .. })]
                if key_space == PENDING_LOCATION_KEYSPACE && key.as_ref() == archive.to_bytes()
        ));
        let read = |value: Option<Vec<u8>>| {
            Event::Storage(StorageEvent::ReadResult {
                key: Vec::<u8>::new().into(),
                value: value.map(Into::into),
            })
        };
        operation.step(read(Some(location.to_bytes().unwrap())));
        operation.step(read(None));
        let txn_id = operation.txn_id.unwrap();
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        let found = matches!(&operation.output, Some(Ok(output)) if output.location == Some(location.clone()));
        assert!(found);

        // A governed archive is described only through the registration of exactly it.
        let refs = vec![PlacementPolicyRef {
            policy_id: Ulid::generate(),
            digest: [1; 32],
        }];
        let governed = version.with_policies(refs.clone()).unwrap();
        let version_key = VersionKey::new("mybucket", "hello.txt", version_id);
        let node_id = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let subject = aruna_core::structs::placement::node_subject::NodeSubjectRecord::seed(
            crate::tests::policy::subject(node_id, "eu"),
        )
        .unwrap();
        let registered = |location: &BackendLocation| {
            use aruna_core::structs::storage::blob::{ManagedCopyRecord, ManagedCopyState};
            let record = ManagedCopyRecord::new(
                version_key.clone(),
                subject.subject.node_id,
                location.clone(),
                refs.clone(),
                0,
                ManagedCopyState::Registered,
            )
            .unwrap()
            .stored_under(subject.subject.generation);
            Event::Storage(StorageEvent::BatchReadResult {
                values: vec![
                    (
                        Vec::<u8>::new().into(),
                        Some(record.to_bytes().unwrap().into()),
                    ),
                    (
                        Vec::<u8>::new().into(),
                        Some(subject.to_bytes().unwrap().into()),
                    ),
                ],
            })
        };
        let mut operation = HeadObjectOperation::new(input());
        operation.txn_id = Some(Ulid::generate());
        let effects = operation.read_version(version_id, governed.clone(), true);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchRead { .. })]
        ));
        let effects = operation.step(registered(&location));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == PENDING_LOCATION_KEYSPACE
        ));

        // A registration of another archive answers for nothing.
        let mut other = location.clone();
        other.ulid = Ulid::generate();
        let mut operation = HeadObjectOperation::new(input());
        operation.txn_id = Some(Ulid::generate());
        operation.read_version(version_id, governed, true);
        operation.step(registered(&other));
        assert!(matches!(
            operation.output,
            Some(Err(HeadObjectError::ManagedCopyError(
                ManagedCopyError::Mismatched
            )))
        ));
    }

    // A result exists only once the read transaction committed, so an aborted head never succeeds.
    #[test]
    fn output_after_commit() {
        use aruna_core::structs::storage::blob::ArchiveKey;
        use aruna_core::structs::storage::encryption::BucketKeyRef;
        use aruna_core::structs::storage::format::PithosLayout;

        let mut location = location_with_hash();
        location.hashes.remove(HASH_BLAKE3);
        let layout = PithosLayout {
            stored_size: 70,
            metadata_digest: [3; 32],
            storage_generation: 0,
        };
        location.format = StoredFormat::pithos(layout, BucketKeyRef::new(Ulid::generate(), 1));
        let archive = ArchiveKey::of(&location);
        let version_id = Ulid::generate();
        let version = BlobVersion::pending(archive, location.created_at, location.created_by, None);
        let read = |value: Option<Vec<u8>>| {
            Event::Storage(StorageEvent::ReadResult {
                key: Vec::<u8>::new().into(),
                value: value.map(Into::into),
            })
        };
        let looked_up = || {
            let mut operation = HeadObjectOperation::new(HeadObjectInput {
                bucket: "mybucket".to_string(),
                key: "hello.txt".to_string(),
                version_id: Some(version_id),
            });
            let txn_id = Ulid::generate();
            operation.txn_id = Some(txn_id);
            operation.read_version(version_id, version.clone(), true);
            operation.step(read(Some(location.to_bytes().unwrap())));
            let effects = operation.step(read(None));
            assert!(matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::CommitTransaction { txn_id: commit })]
                    if *commit == txn_id
            ));
            (operation, txn_id)
        };

        let (early, _) = looked_up();
        assert!(!early.is_complete());
        assert!(matches!(
            early.finalize(),
            Err(HeadObjectError::NotFinished)
        ));

        let (mut operation, txn_id) = looked_up();
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(operation.is_complete());
        assert_eq!(operation.finalize().unwrap().location, Some(location));
    }

    #[tokio::test]
    async fn current_version_read() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = aruna_storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: temp_root.to_string(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let location = location_with_hash();
        let version_id = Ulid::generate();
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("failed to start transaction");
        };

        let key = BlobHeadKey::new("mybucket", "hello.txt")
            .to_bytes()
            .unwrap();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: key.into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("mybucket", "hello.txt", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::materialized(
                    location.get_blake3().unwrap().try_into().unwrap(),
                    BackendRef::node_default(),
                    EncodingClass::Raw,
                    location.created_at,
                    location.created_by,
                    None,
                )
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: BlobLocationKey::from_blake3(
                    location.get_blake3().unwrap(),
                    EncodingClass::Raw,
                    location.backend.clone(),
                )
                .unwrap()
                .to_bytes()
                .into(),
                value: location.to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;

        let result = drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: "mybucket".to_string(),
                key: "hello.txt".to_string(),
                version_id: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        assert_eq!(result.location, Some(location));
        assert!(result.source_metadata.is_none());
        assert!(result.last_refresh.is_none());
        assert_eq!(result.version_id, Some(version_id));
        assert_eq!(result.checksum_type, MultipartChecksumType::FullObject);
    }

    #[tokio::test]
    async fn specific_version_read() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = aruna_storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: temp_root.to_string(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let location = location_with_hash();
        let version_id = Ulid::generate();
        let metadata = BlobVersion::materialized(
            location.get_blake3().unwrap().try_into().unwrap(),
            BackendRef::node_default(),
            EncodingClass::Raw,
            SystemTime::now(),
            Default::default(),
            None,
        );

        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("failed to start transaction");
        };

        let key = VersionKey::new("mybucket", "hello.txt", version_id)
            .to_bytes()
            .unwrap();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: key.into(),
                value: metadata.to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: BlobLocationKey::from_blake3(
                    location.get_blake3().unwrap(),
                    EncodingClass::Raw,
                    location.backend.clone(),
                )
                .unwrap()
                .to_bytes()
                .into(),
                value: location.to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;

        let result = drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: "mybucket".to_string(),
                key: "hello.txt".to_string(),
                version_id: Some(version_id),
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        assert_eq!(result.location, Some(location));
        assert!(result.source_metadata.is_none());
        assert!(result.last_refresh.is_none());
        assert_eq!(result.version_id, Some(version_id));
        assert_eq!(result.checksum_type, MultipartChecksumType::FullObject);
    }

    #[test]
    fn head_resolves_native() {
        let version_id = Ulid::generate();
        let relationship_id = Ulid::generate();
        let origin = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
        let cached_metadata = SourceMetadata {
            content_length: 11,
            content_type: Some("text/plain".to_string()),
            etag: Some("cached".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: None,
            digests: Default::default(),
        };
        let refreshed_metadata = SourceMetadata {
            content_length: 17,
            content_type: Some("application/octet-stream".to_string()),
            etag: Some("current".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: Some(version_id.to_string()),
            digests: Default::default(),
        };
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::ArunaNative,
                public_config: HashMap::from([(
                    "relationship_id".to_string(),
                    relationship_id.to_string(),
                )]),
                source_path: "source-bucket/folder/file.txt".to_string(),
                version_selector: Some(format!("version:{version_id}")),
                capabilities: Vec::new(),
                origin_node_id: Some(origin),
                namespace: None,
                root: None,
            },
            connector_id: None,
            mirrors: Vec::new(),
        };
        let version = BlobVersion::reference(
            source,
            cached_metadata,
            SystemTime::UNIX_EPOCH,
            Default::default(),
            SystemTime::UNIX_EPOCH,
        );
        let txn_id = Ulid::generate();
        let mut operation = HeadObjectOperation::new(HeadObjectInput {
            bucket: "target-bucket".to_string(),
            key: "folder/file.txt".to_string(),
            version_id: Some(version_id),
        });

        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(version.to_bytes().unwrap().into()),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: committed })]
                if *committed == txn_id
        ));

        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::ArunaNative,
            config: HashMap::new(),
            path: "source-bucket/folder/file.txt".to_string(),
            version: Some(version_id.to_string()),
        };
        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::VersionAccessResolved {
                result: Ok(access.clone()),
            },
        ));
        assert!(matches!(
            effects.as_slice(),
            [Effect::StagingSource(StagingSourceEffect::Head { access: resolved })]
                if resolved == &access
        ));

        assert!(
            operation
                .step(Event::StagingSource(StagingSourceEvent::HeadResult {
                    metadata: refreshed_metadata.clone(),
                }))
                .is_empty()
        );
        let result = operation.finalize().unwrap();
        assert_eq!(result.source_metadata, Some(refreshed_metadata));
        assert!(result.last_refresh.is_some());
        assert_eq!(result.version_id, Some(version_id));
    }

    #[tokio::test]
    async fn cached_metadata_returned() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = aruna_storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: temp_root.to_string(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let version_id = Ulid::generate();
        let cached_metadata = SourceMetadata {
            content_length: 11,
            content_type: Some("text/plain".to_string()),
            etag: Some("etag-123".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: None,
            digests: Default::default(),
        };
        let last_refresh = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(17);
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                source_path: "folder/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: Some(Ulid::generate()),
            mirrors: Vec::new(),
        };

        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("failed to start transaction");
        };

        let key = BlobHeadKey::new("mybucket", "hello.txt")
            .to_bytes()
            .unwrap();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: key.into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("mybucket", "hello.txt", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::reference(
                    source,
                    cached_metadata.clone(),
                    SystemTime::UNIX_EPOCH,
                    Default::default(),
                    last_refresh,
                )
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;

        let result = drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: "mybucket".to_string(),
                key: "hello.txt".to_string(),
                version_id: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        assert!(result.location.is_none());
        assert_eq!(result.source_metadata, Some(cached_metadata));
        assert_eq!(result.last_refresh, Some(last_refresh));
        assert_eq!(result.version_id, Some(version_id));
    }
}
