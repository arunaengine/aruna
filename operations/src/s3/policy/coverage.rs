//! Scans this node's heads in pages and reports where a bucket default is not attached yet; in a
//! distributed bucket only the ranges this node writes. Historical rows are diagnostic only.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, MANAGED_COPY_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{read_row, values};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer, ManagedCopyKey,
    ManagedCopyRecord, VersionKey,
};
use aruna_core::structs::storage::distribution::{DistributionBinding, RouteError};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::distribution::guard::{GuardError, authority_reads, write_authority};

/// Upper bound on one page, so a scan can never walk a whole bucket at once.
const COVERAGE_PAGE_LIMIT: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoverageScope {
    /// The heads this responder currently stores.
    CurrentHeads,
    /// Versions that are not the current head. Diagnostic only.
    Historical,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttachmentGap {
    /// The version carries none of the target refs.
    Missing,
    /// The version carries some, but not all, of them.
    Partial,
}

/// Local copy state of one head, reported separately from its attachment: a
/// fully attached head can still have no serveable copy here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopyState {
    /// A serveable registration exists. It is NOT a compliance claim: only a
    /// subject revalidation decides whether the copy still satisfies its refs.
    Registered,
    Quarantined,
    /// No local registration for this version.
    Absent,
    /// A reference claims no Aruna-managed copy at all.
    ReferenceOnly,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CopyCounts {
    pub registered: usize,
    pub quarantined: usize,
    pub absent: usize,
    pub reference_only: usize,
}

/// What this report deliberately does not claim.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoverageLimit {
    /// Only this responder's local records were read.
    ResponderLocal,
    /// One bounded page; `cursor` resumes the scan.
    BoundedPage,
    /// Historical versions keep their original refs and are not scanned here.
    HistoricalExcluded,
    /// A write committed after a head was observed is not reflected.
    ConcurrentWrites,
    /// A distributed bucket: only the ranges this node writes, the other coordinators report theirs.
    CoordinatedRanges,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoverageGap {
    pub key: String,
    pub version_id: Ulid,
    pub attachment: AttachmentGap,
    /// `None` in the historical scope, where local copy state is not the subject.
    pub copy: Option<CopyState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoverageReport {
    pub bucket: String,
    pub scope: CoverageScope,
    /// The exact default this pass compared against.
    pub generation: u64,
    pub target_refs: Vec<PlacementPolicyRef>,
    pub observed: usize,
    /// Delete markers, which hold no bytes and mint no successor.
    pub deleted: usize,
    pub gaps: Vec<CoverageGap>,
    pub copies: CopyCounts,
    pub cursor: Option<Key>,
    /// True only when the bounded local iterator was exhausted in this pass.
    pub complete: bool,
    pub limits: Vec<CoverageLimit>,
    /// Entries of ranges another node writes; that node reports them.
    pub elsewhere: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoverageInput {
    pub bucket: String,
    pub scope: CoverageScope,
    pub start_after: Option<Key>,
    pub limit: usize,
    pub auth_context: AuthContext,
}

#[derive(Debug, Error, PartialEq)]
pub enum CoverageError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
    #[error("caller may not administer the realm configuration")]
    Unauthorized,
    #[error(transparent)]
    Distribution(#[from] GuardError),
}

impl From<IoError> for CoverageError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

type CoverageTurn = Result<Next<CoverageReport>, CoverageError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScanState {
    ReadBucket,
    /// Reads the set record and this node's fence, so only the keys it writes are counted.
    ReadRecord,
    ScanPage,
    ReadVersions,
    ReadHeads,
    ReadCopies,
}

/// One observed entry, carried between the page reads that classify it.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Observed {
    key: String,
    version_id: Ulid,
    version: Option<BlobVersion>,
}

/// Reports one page after checking that the caller may administer the realm's placement.
#[derive(Debug, PartialEq)]
pub struct PolicyCoverageOperation {
    input: CoverageInput,
    scanning: bool,
}

impl PolicyCoverageOperation {
    pub fn new(input: CoverageInput) -> Self {
        Self {
            input,
            scanning: false,
        }
    }

    fn authorized(&mut self, allowed: Result<bool, AuthorizationError>) -> CoverageTurn {
        match allowed {
            Ok(true) => {
                self.scanning = true;
                let scan = CoverageScan::new(self.input.clone());
                Ok(Next::Call(IoCall::child(scan)))
            }
            Ok(false) => Err(CoverageError::Unauthorized),
            Err(error) => {
                warn!(error = %error, "Coverage authorization check failed");
                Err(CoverageError::Unauthorized)
            }
        }
    }
}

impl Operation for PolicyCoverageOperation {
    type Output = CoverageReport;
    type Error = CoverageError;

    fn first(&mut self) -> CoverageTurn {
        let auth_context = self.input.auth_context.clone();
        let path = policy_admin_path(auth_context.realm_id);
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context,
            path,
            required_permission: Permission::READ,
        });
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> CoverageTurn {
        if self.scanning {
            let report = result.child::<CoverageReport, CoverageError>()??;
            return Ok(Next::Done(report));
        }
        self.authorized(result.child::<bool, AuthorizationError>()?)
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            CoverageError::Unauthorized | CoverageError::NoSuchBucket
        )
    }
}

/// Reads one page of heads or versions and what classifies them from one snapshot.
#[derive(Debug, PartialEq)]
struct CoverageScan {
    input: CoverageInput,
    state: ScanState,
    generation: u64,
    target_refs: Vec<PlacementPolicyRef>,
    entries: Vec<Observed>,
    observed: usize,
    deleted: usize,
    gaps: Vec<CoverageGap>,
    copies: CopyCounts,
    cursor: Option<Key>,
    /// The bucket's membership in a distributed set, and the set record and fence rows.
    binding: Option<DistributionBinding>,
    authority: (Option<Value>, Option<Value>),
    elsewhere: usize,
}

impl CoverageScan {
    fn new(input: CoverageInput) -> Self {
        Self {
            input,
            state: ScanState::ReadBucket,
            generation: 0,
            target_refs: Vec::new(),
            entries: Vec::new(),
            observed: 0,
            deleted: 0,
            gaps: Vec::new(),
            copies: CopyCounts::default(),
            cursor: None,
            binding: None,
            authority: (None, None),
            elsewhere: 0,
        }
    }

    fn call(&mut self, state: ScanState, call: IoCall) -> CoverageTurn {
        self.state = state;
        Ok(Next::Call(call))
    }

    /// Whether this report counts `key`: in a distributed bucket only while this node writes its
    /// range, or holds it fenced until the next coordinator takes over, so each key counts once.
    fn counts(&mut self, key: &str) -> Result<bool, CoverageError> {
        let Some(binding) = self.binding.as_ref() else {
            return Ok(true);
        };
        let (record, fence) = (self.authority.0.as_deref(), self.authority.1.as_deref());
        match write_authority(binding, key, record, fence) {
            Ok(_) | Err(GuardError::Route(RouteError::Moving(_))) => Ok(true),
            Err(GuardError::NotCoordinator { .. }) => {
                self.elsewhere += 1;
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn bucket_read(&mut self, value: Option<Value>) -> CoverageTurn {
        let value = value.ok_or(CoverageError::NoSuchBucket)?;
        let bucket = BucketInfo::from_bytes(value.as_ref())?;
        self.generation = bucket.placement_policy_generation;
        self.target_refs = bucket.placement_policies;
        self.binding = bucket.distribution;
        match self.binding.as_ref() {
            // Read in the same snapshot as the heads, so both describe one moment.
            Some(binding) => {
                let reads = authority_reads(binding.set_id);
                self.call(ScanState::ReadRecord, IoCall::Read(reads))
            }
            None => self.scan_page(),
        }
    }

    fn scan_page(&mut self) -> CoverageTurn {
        let bucket = &self.input.bucket;
        let (space, prefix) = match self.input.scope {
            CoverageScope::CurrentHeads => {
                (BLOB_HEAD_KEYSPACE, BlobHeadKey::bucket_prefix(bucket)?)
            }
            CoverageScope::Historical => {
                (BLOB_VERSIONS_KEYSPACE, VersionKey::bucket_prefix(bucket)?)
            }
        };
        let page = IoCall::Page {
            space: space.to_string(),
            prefix: prefix.into(),
            after: self.input.start_after.clone(),
            limit: self.input.limit.clamp(1, COVERAGE_PAGE_LIMIT),
        };
        self.call(ScanState::ScanPage, page)
    }

    fn page_read(&mut self, (rows, next): (Vec<(Key, Value)>, Option<Key>)) -> CoverageTurn {
        self.cursor = next;
        if rows.is_empty() {
            return Ok(Next::Done(self.report()));
        }
        match self.input.scope {
            CoverageScope::CurrentHeads => self.decode_heads(rows),
            CoverageScope::Historical => self.decode_versions(rows),
        }
    }

    /// Heads page: the version records they point at are read next.
    fn decode_heads(&mut self, rows: Vec<(Key, Value)>) -> CoverageTurn {
        let mut reads = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let head = BlobHeadKey::from_bytes(key.as_ref())?;
            if !self.counts(&head.key)? {
                continue;
            }
            let pointer = CurrentVersionPointer::from_bytes(value.as_ref())?;
            let version_key = VersionKey::new(&self.input.bucket, &head.key, pointer.version_id);
            reads.push((
                BLOB_VERSIONS_KEYSPACE.to_string(),
                version_key.to_bytes()?.into(),
            ));
            self.entries.push(Observed {
                key: head.key,
                version_id: pointer.version_id,
                version: None,
            });
        }
        if reads.is_empty() {
            return Ok(Next::Done(self.report()));
        }
        self.call(ScanState::ReadVersions, IoCall::Read(reads))
    }

    /// Versions page: the object heads decide which of them are historical.
    fn decode_versions(&mut self, rows: Vec<(Key, Value)>) -> CoverageTurn {
        let mut reads = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let version_key = VersionKey::from_bytes(key.as_ref())?;
            if !self.counts(&version_key.key)? {
                continue;
            }
            let version = BlobVersion::from_bytes(value.as_ref())?;
            let head = BlobHeadKey::new(&self.input.bucket, &version_key.key).to_bytes()?;
            reads.push((BLOB_HEAD_KEYSPACE.to_string(), head.into()));
            self.entries.push(Observed {
                key: version_key.key,
                version_id: version_key.version_id,
                version: Some(version),
            });
        }
        if reads.is_empty() {
            return Ok(Next::Done(self.report()));
        }
        self.call(ScanState::ReadHeads, IoCall::Read(reads))
    }

    fn check_answers(&self, read: &[Option<Value>]) -> Result<(), CoverageError> {
        if read.len() == self.entries.len() {
            return Ok(());
        }
        let got = "a different row count";
        let expected = "one row per entry";
        Err(IoError::Unexpected { expected, got }.into())
    }

    fn versions_read(&mut self, read: Vec<Option<Value>>) -> CoverageTurn {
        self.check_answers(&read)?;
        for (entry, value) in self.entries.iter_mut().zip(read) {
            entry.version = (value.as_deref())
                .map(BlobVersion::from_bytes)
                .transpose()?;
        }
        self.read_copies()
    }

    /// Historical scope: an entry that still is the head is not historical.
    fn heads_read(&mut self, read: Vec<Option<Value>>) -> CoverageTurn {
        self.check_answers(&read)?;
        let entries = std::mem::take(&mut self.entries);
        for (entry, value) in entries.into_iter().zip(read) {
            let pointer = (value.as_deref())
                .map(CurrentVersionPointer::from_bytes)
                .transpose()?;
            if pointer.is_some_and(|pointer| pointer.version_id == entry.version_id) {
                continue;
            }
            self.observed += 1;
            let Some(version) = entry.version.as_ref() else {
                continue;
            };
            if version.state == BlobVersionState::Deleted {
                self.deleted += 1;
                continue;
            }
            if let Some(attachment) = self.attachment_gap(&version.placement_policies) {
                self.gaps.push(CoverageGap {
                    key: entry.key,
                    version_id: entry.version_id,
                    attachment,
                    copy: None,
                });
            }
        }
        Ok(Next::Done(self.report()))
    }

    fn read_copies(&mut self) -> CoverageTurn {
        let mut reads = Vec::new();
        for entry in &self.entries {
            let Some(version) = entry.version.as_ref() else {
                continue;
            };
            let BlobVersionState::Materialized { backend, .. } = &version.state else {
                continue;
            };
            let key = ManagedCopyKey::new(
                VersionKey::new(&self.input.bucket, &entry.key, entry.version_id),
                backend.clone(),
            );
            reads.push((MANAGED_COPY_KEYSPACE.to_string(), key.to_bytes()?.into()));
        }
        if reads.is_empty() {
            return self.classify(Vec::new());
        }
        self.call(ScanState::ReadCopies, IoCall::Read(reads))
    }

    fn copies_read(&mut self, read: Vec<Option<Value>>) -> CoverageTurn {
        let mut states = Vec::with_capacity(read.len());
        for value in read {
            let record = (value.as_deref())
                .map(ManagedCopyRecord::from_bytes)
                .transpose()?;
            states.push(match record {
                Some(record) if record.state.is_serveable() => CopyState::Registered,
                Some(_) => CopyState::Quarantined,
                None => CopyState::Absent,
            });
        }
        self.classify(states)
    }

    /// Attachment and local copy state are recorded independently, so zero
    /// attachment gaps never implies that every registered copy is compliant.
    fn classify(&mut self, copy_states: Vec<CopyState>) -> CoverageTurn {
        let entries = std::mem::take(&mut self.entries);
        let mut copies = copy_states.into_iter();
        for entry in entries {
            self.observed += 1;
            let Some(version) = entry.version.as_ref() else {
                self.copies.absent += 1;
                self.gaps.push(CoverageGap {
                    key: entry.key,
                    version_id: entry.version_id,
                    attachment: AttachmentGap::Missing,
                    copy: Some(CopyState::Absent),
                });
                continue;
            };
            let copy = match version.state {
                BlobVersionState::Deleted => {
                    self.deleted += 1;
                    continue;
                }
                BlobVersionState::Reference { .. } => CopyState::ReferenceOnly,
                BlobVersionState::Materialized { .. } => copies.next().unwrap_or(CopyState::Absent),
                // No managed copy is read for an archive whose content hash is still pending.
                BlobVersionState::PendingContent { .. } => CopyState::Absent,
            };
            match copy {
                CopyState::Registered => self.copies.registered += 1,
                CopyState::Quarantined => self.copies.quarantined += 1,
                CopyState::Absent => self.copies.absent += 1,
                CopyState::ReferenceOnly => self.copies.reference_only += 1,
            }
            if let Some(attachment) = self.attachment_gap(&version.placement_policies) {
                self.gaps.push(CoverageGap {
                    key: entry.key,
                    version_id: entry.version_id,
                    attachment,
                    copy: Some(copy),
                });
            }
        }
        Ok(Next::Done(self.report()))
    }

    fn attachment_gap(&self, refs: &[PlacementPolicyRef]) -> Option<AttachmentGap> {
        let present = self
            .target_refs
            .iter()
            .filter(|target| refs.contains(target))
            .count();
        match present {
            _ if present == self.target_refs.len() => None,
            0 => Some(AttachmentGap::Missing),
            _ => Some(AttachmentGap::Partial),
        }
    }

    fn limits(&self) -> Vec<CoverageLimit> {
        let mut limits = vec![
            CoverageLimit::ResponderLocal,
            CoverageLimit::ConcurrentWrites,
        ];
        if self.cursor.is_some() {
            limits.push(CoverageLimit::BoundedPage);
        }
        if self.input.scope == CoverageScope::CurrentHeads {
            limits.push(CoverageLimit::HistoricalExcluded);
        }
        if self.binding.is_some() {
            limits.push(CoverageLimit::CoordinatedRanges);
        }
        limits
    }

    fn report(&mut self) -> CoverageReport {
        CoverageReport {
            bucket: self.input.bucket.clone(),
            scope: self.input.scope,
            generation: self.generation,
            target_refs: self.target_refs.clone(),
            observed: self.observed,
            deleted: self.deleted,
            gaps: std::mem::take(&mut self.gaps),
            copies: self.copies,
            cursor: self.cursor.clone(),
            complete: self.cursor.is_none(),
            limits: self.limits(),
            elsewhere: self.elsewhere,
        }
    }
}

impl Operation for CoverageScan {
    type Output = CoverageReport;
    type Error = CoverageError;

    fn first(&mut self) -> CoverageTurn {
        let key = self.input.bucket.as_bytes().to_vec();
        self.call(ScanState::ReadBucket, read_row(S3_BUCKET_KEYSPACE, key))
    }

    fn next(&mut self, result: IoResult) -> CoverageTurn {
        match self.state {
            ScanState::ReadBucket => self.bucket_read(result.value()?),
            ScanState::ReadRecord => {
                let [record, fence] = values::<2>(result)?;
                self.authority = (record, fence);
                self.scan_page()
            }
            ScanState::ScanPage => self.page_read(result.page()?),
            ScanState::ReadVersions => self.versions_read(result.values()?),
            ScanState::ReadHeads => self.heads_read(result.values()?),
            ScanState::ReadCopies => self.copies_read(result.values()?),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::id::NodeId;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::{BackendLocation, BackendRef, ManagedCopyState};
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::format::EncodingClass;
    use aruna_core::structs::storage::format::StoredFormat;
    use std::collections::HashMap;
    use std::time::UNIX_EPOCH;

    const BUCKET: &str = "bucket";

    fn node_id() -> NodeId {
        iroh::SecretKey::from_bytes(&[3u8; 32]).public()
    }

    fn user_id() -> UserId {
        UserId::nil(RealmId::from_bytes([1u8; 32]))
    }

    fn policy_ref(byte: u8) -> PlacementPolicyRef {
        PlacementPolicyRef {
            policy_id: Ulid::from_bytes([byte; 16]),
            digest: [byte; 32],
        }
    }

    fn bucket(policies: Vec<PlacementPolicyRef>) -> BucketInfo {
        BucketInfo {
            group_id: Ulid::from_bytes([2u8; 16]),
            created_at: UNIX_EPOCH,
            created_by: user_id(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: policies,
            placement_policy_generation: 7,
            compression: Compression::Off,
            distribution: None,
        }
    }

    fn version(refs: Vec<PlacementPolicyRef>) -> BlobVersion {
        BlobVersion::materialized(
            [6u8; 32],
            BackendRef::node_default(),
            EncodingClass::Raw,
            UNIX_EPOCH,
            user_id(),
            None,
        )
        .with_policies(refs)
        .expect("refs stored")
    }

    fn copy_record(state: ManagedCopyState, version_id: Ulid) -> ManagedCopyRecord {
        ManagedCopyRecord::new(
            VersionKey::new(BUCKET, "object", version_id),
            node_id(),
            BackendLocation {
                backend: BackendRef::node_default(),
                storage_class: None,
                root: "/data".to_string(),
                storage_bucket: "aruna".to_string(),
                backend_path: "objects/one".to_string(),
                ulid: Ulid::from_bytes([5u8; 16]),
                format: StoredFormat::default(),
                created_by: Default::default(),
                created_at: UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 3,
                hashes: HashMap::new(),
            },
            Vec::new(),
            7,
            state,
        )
        .expect("record builds")
    }

    fn input(scope: CoverageScope) -> CoverageInput {
        CoverageInput {
            bucket: BUCKET.to_string(),
            scope,
            start_after: None,
            limit: 16,
            auth_context: AuthContext {
                user_id: user_id(),
                realm_id: RealmId::from_bytes([1u8; 32]),
                path_restrictions: None,
                session: None,
            },
        }
    }

    /// A scan that read `bucket` and asks for what comes next.
    fn scan(scope: CoverageScope, bucket: &BucketInfo) -> (CoverageScan, CoverageTurn) {
        let mut scan = CoverageScan::new(input(scope));
        assert_eq!(scan.transaction(), TransactionMode::ReadSnapshot);
        scan.first().unwrap();
        let turn = scan.next(batch(vec![Some(bucket.to_bytes().expect("bucket"))]));
        (scan, turn)
    }

    fn report_of(turn: CoverageTurn) -> CoverageReport {
        match turn {
            Ok(Next::Done(report)) => report,
            other => panic!("expected the report, got {other:?}"),
        }
    }

    fn authorized(allowed: bool) -> IoResult {
        IoResult::Child(ChildOutcome::new(Ok::<_, AuthorizationError>(allowed)))
    }

    fn batch(read: Vec<Option<Vec<u8>>>) -> IoResult {
        IoResult::Values(
            read.into_iter()
                .map(|value| value.map(Into::into))
                .collect(),
        )
    }

    fn page(entries: Vec<(&str, Ulid)>) -> IoResult {
        IoResult::Page {
            rows: entries
                .into_iter()
                .map(|(key, version_id)| {
                    (
                        Key::from(BlobHeadKey::new(BUCKET, key).to_bytes().expect("key")),
                        Value::from(
                            CurrentVersionPointer::new(version_id)
                                .to_bytes()
                                .expect("pointer"),
                        ),
                    )
                })
                .collect(),
            next: None,
        }
    }

    /// One page of a version row, for the historical scope.
    fn version_page(version_id: Ulid) -> IoResult {
        let key = VersionKey::new(BUCKET, "object", version_id);
        let value = version(Vec::new()).to_bytes().expect("version");
        IoResult::Page {
            rows: vec![(key.to_bytes().expect("key").into(), value.into())],
            next: None,
        }
    }

    /// Runs a head scan over one page and returns the report.
    fn scan_heads(
        default: Vec<PlacementPolicyRef>,
        version: BlobVersion,
        copy: Option<ManagedCopyRecord>,
    ) -> CoverageReport {
        let version_id = Ulid::from_bytes([9u8; 16]);
        let (mut scan, _) = scan(CoverageScope::CurrentHeads, &bucket(default));
        scan.next(page(vec![("object", version_id)])).unwrap();
        scan.next(batch(vec![Some(version.to_bytes().expect("version"))]))
            .unwrap();
        report_of(scan.next(batch(vec![
            copy.map(|record| record.to_bytes().expect("record")),
        ])))
    }

    /// A head scan of a bucket bound to member 1 of the two-shard test set, which writes the keys
    /// below "m"; `fence` is this node's fence row.
    fn scan_distributed(
        entries: Vec<(&str, Ulid)>,
        fence: Option<Vec<u8>>,
        owned: usize,
    ) -> CoverageReport {
        use crate::distribution::test_record::{binding, record};
        let (snapshot, bytes) = record(true);
        let mut bound = bucket(vec![policy_ref(1)]);
        bound.distribution = Some(binding(&snapshot, 1, 5));
        let (mut scan, turn) = scan(CoverageScope::CurrentHeads, &bound);
        assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
        scan.next(batch(vec![Some(bytes), fence])).unwrap();
        let turn = scan.next(page(entries));
        if owned == 0 {
            return report_of(turn);
        }
        let version = version(vec![policy_ref(1)]).to_bytes().expect("version");
        scan.next(batch(vec![Some(version); owned])).unwrap();
        report_of(scan.next(batch(vec![None; owned])))
    }

    #[test]
    fn counts_coordinated_ranges() {
        // Member 1 writes "a" but not "x": the coordinator of "x" reports it, so it counts once.
        let report = scan_distributed(
            vec![
                ("a", Ulid::from_bytes([8; 16])),
                ("x", Ulid::from_bytes([9; 16])),
            ],
            None,
            1,
        );
        assert_eq!((report.observed, report.elsewhere), (1, 1));
        assert!(report.limits.contains(&CoverageLimit::CoordinatedRanges));

        let other = scan_distributed(vec![("x", Ulid::from_bytes([9; 16]))], None, 0);
        assert_eq!((other.observed, other.elsewhere), (0, 1));
        assert!(other.complete);
    }

    #[test]
    fn counts_fenced_range() {
        // A range fenced for a transfer stays this node's to report until the next one takes over.
        use aruna_core::structs::storage::distribution::fence::{LocalFence, RangeFence};
        let (snapshot, _) = crate::distribution::test_record::record(true);
        let mut fence = LocalFence::default();
        fence.fence_range(RangeFence {
            range_id: snapshot.config.ranges[0].range_id,
            transfer_id: Ulid::from_bytes([3; 16]),
            epoch: 1,
            since_ms: 0,
            uploads_open: false,
        });
        let fence = fence.to_bytes().expect("fence encodes");
        let report = scan_distributed(vec![("a", Ulid::from_bytes([8; 16]))], Some(fence), 1);
        assert_eq!((report.observed, report.elsewhere), (1, 0));
    }

    #[test]
    fn reports_target_limits() {
        // The report names the exact default it compared against and never
        // claims more than this responder observed.
        let report = scan_heads(
            vec![policy_ref(1)],
            version(vec![policy_ref(1)]),
            Some(copy_record(
                ManagedCopyState::Registered,
                Ulid::from_bytes([9u8; 16]),
            )),
        );

        assert_eq!(report.generation, 7);
        assert_eq!(report.target_refs, vec![policy_ref(1)]);
        assert_eq!(report.observed, 1);
        assert!(report.gaps.is_empty());
        assert!(report.complete);
        assert!(report.limits.contains(&CoverageLimit::ResponderLocal));
        assert!(report.limits.contains(&CoverageLimit::HistoricalExcluded));
        assert!(report.limits.contains(&CoverageLimit::ConcurrentWrites));
    }

    #[test]
    fn separates_copy_state() {
        // Zero attachment gaps must not hide a copy that cannot be served.
        let report = scan_heads(
            vec![policy_ref(1)],
            version(vec![policy_ref(1)]),
            Some(copy_record(
                ManagedCopyState::Quarantined(
                    aruna_core::structs::storage::blob::ManagedCopyQuarantine::Rejoin,
                ),
                Ulid::from_bytes([9u8; 16]),
            )),
        );

        assert!(report.gaps.is_empty());
        assert_eq!(report.copies.quarantined, 1);
        assert_eq!(report.copies.registered, 0);
    }

    #[test]
    fn flags_partial_attachment() {
        let report = scan_heads(
            vec![policy_ref(1), policy_ref(2)],
            version(vec![policy_ref(1)]),
            Some(copy_record(
                ManagedCopyState::Registered,
                Ulid::from_bytes([9u8; 16]),
            )),
        );

        assert_eq!(report.gaps.len(), 1);
        assert_eq!(report.gaps[0].attachment, AttachmentGap::Partial);
        assert_eq!(report.gaps[0].copy, Some(CopyState::Registered));
    }

    #[test]
    fn flags_absent_copy() {
        let report = scan_heads(vec![policy_ref(1)], version(Vec::new()), None);

        assert_eq!(report.gaps.len(), 1);
        assert_eq!(report.gaps[0].attachment, AttachmentGap::Missing);
        assert_eq!(report.gaps[0].copy, Some(CopyState::Absent));
        assert_eq!(report.copies.absent, 1);
    }

    #[test]
    fn historical_skips_head() {
        // A version that is still the head is not a historical gap.
        let version_id = Ulid::from_bytes([9u8; 16]);
        let (mut scan, _) = scan(CoverageScope::Historical, &bucket(vec![policy_ref(1)]));
        scan.next(version_page(version_id)).unwrap();
        let pointer = CurrentVersionPointer::new(version_id).to_bytes();
        let report = report_of(scan.next(batch(vec![Some(pointer.expect("pointer"))])));

        assert_eq!(report.observed, 0);
        assert!(report.gaps.is_empty());
        assert!(!report.limits.contains(&CoverageLimit::HistoricalExcluded));
    }

    #[test]
    fn denies_non_admin() {
        // Coverage names policy refs, so it is an administrative read.
        let mut operation = PolicyCoverageOperation::new(input(CoverageScope::CurrentHeads));
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(
            operation.next(authorized(false)),
            Err(CoverageError::Unauthorized)
        );
    }

    #[test]
    fn scans_when_allowed() {
        let mut operation = PolicyCoverageOperation::new(input(CoverageScope::CurrentHeads));
        operation.first().unwrap();
        assert!(matches!(
            operation.next(authorized(true)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
    }

    #[test]
    fn missing_bucket_refused() {
        let mut scan = CoverageScan::new(input(CoverageScope::CurrentHeads));
        scan.first().unwrap();
        assert_eq!(
            scan.next(batch(vec![None])),
            Err(CoverageError::NoSuchBucket)
        );
    }

    #[test]
    fn historical_flags_predecessor() {
        let (mut scan, _) = scan(CoverageScope::Historical, &bucket(vec![policy_ref(1)]));
        scan.next(version_page(Ulid::from_bytes([9u8; 16])))
            .unwrap();
        let pointer = CurrentVersionPointer::new(Ulid::from_bytes([8u8; 16])).to_bytes();
        let report = report_of(scan.next(batch(vec![Some(pointer.expect("pointer"))])));

        assert_eq!(report.observed, 1);
        assert_eq!(report.gaps.len(), 1);
        assert_eq!(report.gaps[0].copy, None);
    }
}
