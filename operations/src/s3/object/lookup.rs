//! Reads one object version in a read snapshot: its row, copy gate, location and part summary.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, MANAGED_COPY_KEYSPACE,
    NODE_SUBJECT_KEYSPACE, OBJECT_METADATA_KEYSPACE, PENDING_LOCATION_KEYSPACE,
};
use aruna_core::managed_copy::{
    CopyRequest, ManagedCopyError, split_serve_reads, validate_registration,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::placement::node_subject::NODE_SUBJECT_KEY;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobHeadKey, BlobLocationKey, BlobVersion, BlobVersionState,
    CurrentVersionPointer, ManagedCopyKey, VersionKey,
};
use aruna_core::structs::storage::multipart::{
    MultipartObjectKey, MultipartObjectPart, MultipartObjectSummary,
};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

const PART_SCAN_LIMIT: usize = 10_000;

#[derive(Debug, PartialEq)]
pub(crate) enum LookupError {
    Conversion(ConversionError),
    Managed(ManagedCopyError),
    Missing,
}

/// Which node a registration must name: the reader's local node, or the node of
/// the subject row read alongside the registration.
pub(crate) enum ExpectedNode {
    Exact(NodeId),
    Subject,
}

/// The registration and subject rows of a copy check, in this order.
pub(crate) fn copy_check_call(copy_key: &ManagedCopyKey) -> Result<IoCall, ManagedCopyError> {
    Ok(IoCall::Read(vec![
        (
            MANAGED_COPY_KEYSPACE.to_string(),
            copy_key.to_bytes()?.into(),
        ),
        (
            NODE_SUBJECT_KEYSPACE.to_string(),
            Key::from(NODE_SUBJECT_KEY.to_vec()),
        ),
    ]))
}

fn keyed_values(values: Vec<Option<Value>>) -> Vec<(Key, Option<Value>)> {
    (values.into_iter())
        .map(|value| (Key::from(Vec::new()), value))
        .collect()
}

/// Checks the values [`copy_check_call`] read.
pub(crate) fn check_copy_values(
    values: Vec<Option<Value>>,
    pending_copy: &mut Option<ManagedCopyKey>,
    pending_location: &mut Option<BlobLocationKey>,
    refs: &[PlacementPolicyRef],
    node_id: ExpectedNode,
) -> Result<BlobLocationKey, LookupError> {
    let (copy, subject) = split_serve_reads(keyed_values(values)).map_err(LookupError::Managed)?;
    let (Some(copy_key), Some(location_key)) = (pending_copy.take(), pending_location.take())
    else {
        return Err(LookupError::Missing);
    };
    let node_id = match node_id {
        ExpectedNode::Exact(node_id) => Some(node_id),
        ExpectedNode::Subject => Some(subject.subject.node_id),
    };
    validate_registration(
        copy.as_deref(),
        &CopyRequest {
            key: &copy_key,
            node_id,
            blake3: Some(location_key.blake3_hash),
            refs,
            subject_generation: Some(subject.subject.generation),
        },
    )
    .map_err(LookupError::Managed)?;
    Ok(location_key)
}

/// The registration check of a governed version whose content hash is still pending. The
/// registered copy must name exactly `archive`; no hash is compared or assumed.
fn check_pending_values(
    values: Vec<Option<Value>>,
    copy_key: &ManagedCopyKey,
    archive: &ArchiveKey,
    refs: &[PlacementPolicyRef],
) -> Result<(), ManagedCopyError> {
    let (copy, subject) = split_serve_reads(keyed_values(values))?;
    let request = CopyRequest {
        key: copy_key,
        node_id: Some(subject.subject.node_id),
        blake3: None,
        refs,
        subject_generation: Some(subject.subject.generation),
    };
    let record = validate_registration(copy.as_deref(), &request)?;
    match &ArchiveKey::of(&record.location) == archive {
        true => Ok(()),
        false => Err(ManagedCopyError::Mismatched),
    }
}

pub(crate) fn location_from_value(
    value: Option<Value>,
) -> Result<Option<BackendLocation>, LookupError> {
    match value {
        Some(value) => Ok(Some(
            BackendLocation::from_bytes(value.as_ref()).map_err(LookupError::Conversion)?,
        )),
        None => Ok(None),
    }
}

/// The call that reads a version's multipart summary.
pub(crate) fn summary_call(version_id: Ulid) -> Result<IoCall, ConversionError> {
    let key = MultipartObjectKey::summary(version_id).to_bytes()?;
    Ok(read_row(OBJECT_METADATA_KEYSPACE, key))
}

pub(crate) fn summary_from_value(
    value: Option<Value>,
) -> Result<Option<MultipartObjectSummary>, LookupError> {
    value
        .map(|value| {
            MultipartObjectSummary::from_bytes(value.as_ref()).map_err(LookupError::Conversion)
        })
        .transpose()
}

#[derive(Debug, Error, PartialEq)]
pub(crate) enum VersionLookupError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Managed(#[from] ManagedCopyError),
    #[error("The specified key does not exist.")]
    NoSuchKey,
    #[error("The specified version does not exist.")]
    NoSuchVersion,
    #[error("The specified version is a delete marker.")]
    DeleteMarker,
    #[error("the version lookup lost its pending copy check")]
    Failed,
}

impl From<IoError> for VersionLookupError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

impl From<LookupError> for VersionLookupError {
    fn from(error: LookupError) -> Self {
        match error {
            LookupError::Conversion(error) => Self::Conversion(error),
            LookupError::Managed(error) => Self::Managed(error),
            LookupError::Missing => Self::Failed,
        }
    }
}

/// What one snapshot read found about a version.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct FoundVersion {
    pub location: Option<BackendLocation>,
    pub metadata: HashMap<String, String>,
    pub source_metadata: Option<SourceMetadata>,
    pub last_refresh: Option<SystemTime>,
    pub created_at: Option<SystemTime>,
    pub version_id: Option<Ulid>,
    pub summary: Option<MultipartObjectSummary>,
    pub parts: Vec<MultipartObjectPart>,
    pub source_policies: Vec<PlacementPolicyRef>,
    pub source_binding: Option<VersionSourceBinding>,
    /// A reference to another Aruna node, whose current metadata the caller asks for.
    pub native_reference: Option<VersionSourceBinding>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LookupState {
    ReadHead,
    ReadVersion,
    CheckCopy,
    CheckPending,
    ReadLocation,
    ReadSummary,
    ReadParts,
}

/// Resolves a version and everything a reader may learn about it from one read snapshot. A
/// governed version answers only through this node's registration of its copy.
#[derive(Debug, PartialEq)]
pub(crate) struct VersionLookup {
    bucket: String,
    key: String,
    version_id: Option<Ulid>,
    include_parts: bool,
    state: LookupState,
    found: FoundVersion,
    pending_copy: Option<ManagedCopyKey>,
    pending_location: Option<BlobLocationKey>,
    pending_archive: Option<ArchiveKey>,
}

type LookupTurn = Result<Next<FoundVersion>, VersionLookupError>;

impl VersionLookup {
    pub(crate) fn new(
        bucket: String,
        key: String,
        version_id: Option<Ulid>,
        include_parts: bool,
    ) -> Self {
        Self {
            bucket,
            key,
            version_id,
            include_parts,
            state: LookupState::ReadHead,
            found: FoundVersion::default(),
            pending_copy: None,
            pending_location: None,
            pending_archive: None,
        }
    }

    fn call(&mut self, state: LookupState, call: IoCall) -> LookupTurn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn read_version(&mut self, version_id: Ulid) -> LookupTurn {
        let key = VersionKey::new(&self.bucket, &self.key, version_id).to_bytes()?;
        let read = read_row(BLOB_VERSIONS_KEYSPACE, key);
        self.call(LookupState::ReadVersion, read)
    }

    fn head_read(&mut self, value: Option<Value>) -> LookupTurn {
        let value = value.ok_or(VersionLookupError::NoSuchKey)?;
        let pointer = CurrentVersionPointer::from_bytes(value.as_ref())?;
        self.found.version_id = Some(pointer.version_id);
        self.read_version(pointer.version_id)
    }

    fn version_read(&mut self, value: Option<Value>) -> LookupTurn {
        let Some(value) = value else {
            return Err(match self.version_id {
                Some(_) => VersionLookupError::NoSuchVersion,
                None => VersionLookupError::NoSuchKey,
            });
        };
        let version = BlobVersion::from_bytes(value.as_ref())?;
        let version_id =
            (self.found.version_id.or(self.version_id)).ok_or(VersionLookupError::Failed)?;
        self.describe(version_id, version)
    }

    fn describe(&mut self, version_id: Ulid, version: BlobVersion) -> LookupTurn {
        self.found.version_id = Some(version_id);
        self.found.metadata = version.metadata.clone();
        self.found.source_policies = version.placement_policies.clone();
        let governed = !version.placement_policies.is_empty();
        match version.state {
            BlobVersionState::Materialized {
                blob_hash,
                backend,
                encoding,
                source,
            } => {
                let location_key = BlobLocationKey::new(blob_hash, encoding, backend);
                self.found.source_binding = source;
                self.found.created_at = Some(version.created_at);
                match governed {
                    true => self.check_copy(version_id, location_key),
                    false => self.read_location(&location_key),
                }
            }
            BlobVersionState::Deleted => Err(match self.version_id {
                Some(_) => VersionLookupError::DeleteMarker,
                None => VersionLookupError::NoSuchKey,
            }),
            BlobVersionState::Reference {
                source,
                cached_metadata,
                last_refresh,
                ..
            } => {
                self.found.source_metadata = Some(cached_metadata);
                self.found.last_refresh = Some(last_refresh);
                self.found.source_binding = Some(source.clone());
                if source.descriptor.kind == SourceConnectorKind::ArunaNative {
                    self.found.native_reference = Some(source);
                }
                self.finish()
            }
            BlobVersionState::PendingContent { archive, source } => {
                self.found.source_binding = source;
                self.found.created_at = Some(version.created_at);
                match governed {
                    true => self.check_pending(version_id, archive),
                    false => self.read_pending_location(&archive),
                }
            }
        }
    }

    /// A governed pending archive answers only through this node's registration of it.
    fn check_pending(&mut self, version_id: Ulid, archive: ArchiveKey) -> LookupTurn {
        let version = VersionKey::new(&self.bucket, &self.key, version_id);
        let copy_key = ManagedCopyKey::new(version, archive.backend.clone());
        let call = copy_check_call(&copy_key)?;
        self.pending_copy = Some(copy_key);
        self.pending_archive = Some(archive);
        self.call(LookupState::CheckPending, call)
    }

    fn pending_checked(&mut self, values: Vec<Option<Value>>) -> LookupTurn {
        let (Some(copy_key), Some(archive)) =
            (self.pending_copy.take(), self.pending_archive.take())
        else {
            return Err(VersionLookupError::Failed);
        };
        let refs = &self.found.source_policies;
        check_pending_values(values, &copy_key, &archive, refs)?;
        self.read_pending_location(&archive)
    }

    fn read_pending_location(&mut self, archive: &ArchiveKey) -> LookupTurn {
        let read = read_row(PENDING_LOCATION_KEYSPACE, archive.to_bytes());
        self.call(LookupState::ReadLocation, read)
    }

    fn read_location(&mut self, key: &BlobLocationKey) -> LookupTurn {
        let read = read_row(BLOB_LOCATIONS_KEYSPACE, key.to_bytes());
        self.call(LookupState::ReadLocation, read)
    }

    /// A governed version is only described from a registered local copy, so an
    /// unregistered or quarantined copy fails closed before any metadata is served.
    fn check_copy(&mut self, version_id: Ulid, location_key: BlobLocationKey) -> LookupTurn {
        let version = VersionKey::new(&self.bucket, &self.key, version_id);
        let copy_key = ManagedCopyKey::new(version, location_key.backend.clone());
        let call = copy_check_call(&copy_key)?;
        self.pending_copy = Some(copy_key);
        self.pending_location = Some(location_key);
        self.call(LookupState::CheckCopy, call)
    }

    fn copy_checked(&mut self, values: Vec<Option<Value>>) -> LookupTurn {
        let key = check_copy_values(
            values,
            &mut self.pending_copy,
            &mut self.pending_location,
            &self.found.source_policies,
            ExpectedNode::Subject,
        )?;
        self.read_location(&key)
    }

    fn location_read(&mut self, value: Option<Value>) -> LookupTurn {
        self.found.location = location_from_value(value)?;
        let Some(version_id) = self.found.version_id else {
            return self.finish();
        };
        let call = summary_call(version_id)?;
        self.call(LookupState::ReadSummary, call)
    }

    fn summary_read(&mut self, value: Option<Value>) -> LookupTurn {
        self.found.summary = summary_from_value(value)?;
        let version_id = self.found.version_id.filter(|_| self.include_parts);
        let (Some(version_id), Some(_)) = (version_id, self.found.summary.as_ref()) else {
            return self.finish();
        };
        let scan = IoCall::Page {
            space: OBJECT_METADATA_KEYSPACE.to_string(),
            prefix: MultipartObjectKey::part_prefix(version_id)?.into(),
            after: None,
            limit: PART_SCAN_LIMIT,
        };
        self.call(LookupState::ReadParts, scan)
    }

    fn parts_read(&mut self, rows: Vec<(Key, Value)>) -> LookupTurn {
        let parts: Result<Vec<_>, _> = (rows.iter())
            .map(|(_, value)| MultipartObjectPart::from_bytes(value.as_ref()))
            .collect();
        self.found.parts = parts?;
        // Fixed big-endian part keys already iterate in ascending order; sort
        // defensively to stay independent of storage iteration guarantees.
        self.found.parts.sort_by_key(|part| part.part_number);
        self.finish()
    }

    fn finish(&mut self) -> LookupTurn {
        Ok(Next::Done(std::mem::take(&mut self.found)))
    }
}

impl Operation for VersionLookup {
    type Output = FoundVersion;
    type Error = VersionLookupError;

    fn first(&mut self) -> LookupTurn {
        if let Some(version_id) = self.version_id {
            return self.read_version(version_id);
        }
        let key = BlobHeadKey::new(&self.bucket, &self.key).to_bytes()?;
        let read = read_row(BLOB_HEAD_KEYSPACE, key);
        self.call(LookupState::ReadHead, read)
    }

    fn next(&mut self, result: IoResult) -> LookupTurn {
        match self.state {
            LookupState::ReadHead => self.head_read(result.value()?),
            LookupState::ReadVersion => self.version_read(result.value()?),
            LookupState::CheckCopy => self.copy_checked(result.values()?),
            LookupState::CheckPending => self.pending_checked(result.values()?),
            LookupState::ReadLocation => self.location_read(result.value()?),
            LookupState::ReadSummary => self.summary_read(result.value()?),
            LookupState::ReadParts => self.parts_read(result.page()?.0),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }

    // A missing key or version is a normal answer, for example when an import checks a key first.
    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            VersionLookupError::NoSuchKey
                | VersionLookupError::NoSuchVersion
                | VersionLookupError::DeleteMarker
        )
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::node_subject::NodeSubjectRecord;
    use aruna_core::structs::storage::blob::{BackendRef, ManagedCopyRecord, ManagedCopyState};
    use aruna_core::structs::storage::encryption::BucketKeyRef;
    use aruna_core::structs::storage::format::{EncodingClass, PithosLayout, StoredFormat};
    use aruna_core::structs::storage::multipart::MultipartChecksumType;

    fn row(value: Option<Vec<u8>>) -> IoResult {
        IoResult::Values(vec![value.map(Into::into)])
    }

    fn lookup(version_id: Option<Ulid>, parts: bool) -> VersionLookup {
        VersionLookup::new("bucket".to_string(), "key".to_string(), version_id, parts)
    }

    fn location() -> BackendLocation {
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "key".to_string(),
            ulid: Ulid::from_parts(2, 2),
            format: StoredFormat::default(),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::nil(RealmId::from_bytes([1; 32])),
            staging: false,
            partial: false,
            blob_size: 5,
            hashes: HashMap::new(),
        }
    }

    fn sealed() -> BackendLocation {
        let layout = PithosLayout {
            stored_size: 70,
            metadata_digest: [3; 32],
            storage_generation: 0,
        };
        let mut sealed = location();
        sealed.format = StoredFormat::pithos(layout, BucketKeyRef::new(Ulid::from_parts(3, 3), 1));
        sealed
    }

    fn materialized() -> BlobVersion {
        BlobVersion::materialized(
            [7; 32],
            BackendRef::node_default(),
            EncodingClass::Raw,
            SystemTime::UNIX_EPOCH,
            UserId::nil(RealmId::from_bytes([1; 32])),
            None,
        )
    }

    fn summary() -> MultipartObjectSummary {
        MultipartObjectSummary {
            checksum_type: MultipartChecksumType::Composite,
            part_count: 2,
            composite_hashes: Default::default(),
        }
    }

    fn found(turn: LookupTurn) -> FoundVersion {
        match turn {
            Ok(Next::Done(found)) => found,
            other => panic!("expected the finished lookup, got {other:?}"),
        }
    }

    fn reads_space(turn: &LookupTurn, space: &str) -> bool {
        matches!(turn, Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == space)
    }

    #[test]
    fn summary_decode_fails() {
        let bytes = summary().to_bytes().unwrap();
        assert_eq!(summary_from_value(Some(bytes.into())), Ok(Some(summary())));
        assert!(matches!(
            summary_from_value(Some(vec![0xff].into())),
            Err(LookupError::Conversion(_))
        ));
    }

    #[test]
    fn head_names_version() {
        // Without a version the head decides; the whole lookup reads one snapshot.
        let version_id = Ulid::from_parts(4, 4);
        let mut operation = lookup(None, false);
        assert_eq!(operation.transaction(), TransactionMode::ReadSnapshot);
        assert!(reads_space(&operation.first(), BLOB_HEAD_KEYSPACE));
        let pointer = CurrentVersionPointer::new(version_id).to_bytes().unwrap();
        let turn = operation.next(row(Some(pointer)));
        assert!(reads_space(&turn, BLOB_VERSIONS_KEYSPACE));
        let turn = operation.next(row(Some(materialized().to_bytes().unwrap())));
        assert!(reads_space(&turn, BLOB_LOCATIONS_KEYSPACE));
        let turn = operation.next(row(Some(location().to_bytes().unwrap())));
        assert!(reads_space(&turn, OBJECT_METADATA_KEYSPACE));
        let result = found(operation.next(row(None)));
        assert_eq!(result.location, Some(location()));
        assert_eq!(result.version_id, Some(version_id));
        assert_eq!(result.created_at, Some(SystemTime::UNIX_EPOCH));
    }

    #[test]
    fn missing_rows_refuse() {
        let mut no_head = lookup(None, false);
        no_head.first().unwrap();
        assert_eq!(no_head.next(row(None)), Err(VersionLookupError::NoSuchKey));
        let mut no_version = lookup(Some(Ulid::from_parts(4, 4)), false);
        no_version.first().unwrap();
        assert_eq!(
            no_version.next(row(None)),
            Err(VersionLookupError::NoSuchVersion)
        );
    }

    #[test]
    fn markers_refuse_reads() {
        // Asked by version a marker is named; through the head the key is just gone.
        let marker = BlobVersion::deleted(SystemTime::UNIX_EPOCH, Default::default());
        let bytes = marker.to_bytes().unwrap();
        let version_id = Ulid::from_parts(4, 4);
        let mut named = lookup(Some(version_id), false);
        named.first().unwrap();
        assert_eq!(
            named.next(row(Some(bytes.clone()))),
            Err(VersionLookupError::DeleteMarker)
        );
        let mut current = lookup(None, false);
        current.first().unwrap();
        let pointer = CurrentVersionPointer::new(version_id).to_bytes().unwrap();
        current.next(row(Some(pointer))).unwrap();
        assert_eq!(
            current.next(row(Some(bytes))),
            Err(VersionLookupError::NoSuchKey)
        );
    }

    #[test]
    fn parts_follow_summary() {
        // Parts are read only on request, and only for a multipart version.
        let version_id = Ulid::from_parts(4, 4);
        let part = |part_number| MultipartObjectPart {
            part_number,
            size: 5,
            hashes: HashMap::new(),
        };
        let mut operation = lookup(Some(version_id), true);
        operation.first().unwrap();
        operation
            .next(row(Some(materialized().to_bytes().unwrap())))
            .unwrap();
        operation
            .next(row(Some(location().to_bytes().unwrap())))
            .unwrap();
        let turn = operation.next(row(Some(summary().to_bytes().unwrap())));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Page { .. }))));
        let rows = [part(2), part(1)]
            .map(|part| (Key::from(vec![1]), Value::from(part.to_bytes().unwrap())));
        let page = IoResult::Page {
            rows: rows.to_vec(),
            next: None,
        };
        let result = found(operation.next(page));
        let numbers: Vec<_> = result.parts.iter().map(|part| part.part_number).collect();
        assert_eq!(numbers, [1, 2]);

        let mut plain = lookup(Some(version_id), true);
        plain.state = LookupState::ReadSummary;
        plain.found.version_id = Some(version_id);
        assert!(found(plain.next(row(None))).parts.is_empty());
    }

    #[test]
    fn pending_reads_location() {
        let archive = ArchiveKey::of(&sealed());
        let version_id = Ulid::from_parts(4, 4);
        let version = BlobVersion::pending(
            archive.clone(),
            SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
        );
        let mut operation = lookup(Some(version_id), false);
        operation.first().unwrap();
        let turn = operation.next(row(Some(version.to_bytes().unwrap())));
        assert_eq!(
            turn,
            Ok(Next::Call(read_row(
                PENDING_LOCATION_KEYSPACE,
                archive.to_bytes()
            )))
        );
        operation
            .next(row(Some(sealed().to_bytes().unwrap())))
            .unwrap();
        assert_eq!(found(operation.next(row(None))).location, Some(sealed()));
    }

    #[test]
    fn pending_needs_registration() {
        // A governed archive is described only through the registration of exactly it.
        let archive = ArchiveKey::of(&sealed());
        let version_id = Ulid::from_parts(4, 4);
        let refs = vec![PlacementPolicyRef {
            policy_id: Ulid::from_parts(5, 5),
            digest: [1; 32],
        }];
        let governed =
            BlobVersion::pending(archive, SystemTime::UNIX_EPOCH, Default::default(), None)
                .with_policies(refs.clone())
                .unwrap();
        let node_id = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let subject =
            NodeSubjectRecord::seed(crate::tests::policy::subject(node_id, "eu")).unwrap();
        let registered = |location: &BackendLocation| {
            let record = ManagedCopyRecord::new(
                VersionKey::new("bucket", "key", version_id),
                subject.subject.node_id,
                location.clone(),
                refs.clone(),
                0,
                ManagedCopyState::Registered,
            )
            .unwrap()
            .stored_under(subject.subject.generation);
            IoResult::Values(vec![
                Some(record.to_bytes().unwrap().into()),
                Some(subject.to_bytes().unwrap().into()),
            ])
        };
        let checking = || {
            let mut operation = lookup(Some(version_id), false);
            operation.first().unwrap();
            let turn = operation.next(row(Some(governed.to_bytes().unwrap())));
            assert!(reads_space(&turn, MANAGED_COPY_KEYSPACE));
            operation
        };
        let turn = checking().next(registered(&sealed()));
        assert!(reads_space(&turn, PENDING_LOCATION_KEYSPACE));
        // A registration of another archive answers for nothing.
        let mut other = sealed();
        other.ulid = Ulid::from_parts(6, 6);
        assert_eq!(
            checking().next(registered(&other)),
            Err(VersionLookupError::Managed(ManagedCopyError::Mismatched))
        );
    }
}
