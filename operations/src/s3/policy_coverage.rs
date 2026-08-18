//! Responder-local coverage of a bucket default.
//!
//! Setting a bucket default governs versions minted after it; it never rewrites
//! what is already stored. This scan therefore reports what THIS node observes
//! about its own current heads, with the exact refs and generation it compared
//! against, and says so in its limits. Historical versions are a separate,
//! diagnostic scope that no bulk action targets.

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, MANAGED_COPY_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    AuthContext, BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer,
    ManagedCopyKey, ManagedCopyRecord, Permission, PlacementPolicyRef, VersionKey,
    policy_admin_path,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};

/// Upper bound on one page, so a scan can never walk a whole bucket at once.
pub const COVERAGE_PAGE_LIMIT: usize = 256;

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
    #[error("unexpected event during the coverage scan")]
    InvalidEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ScanState {
    Init,
    Authorize,
    StartTransaction,
    ReadBucket,
    ScanPage,
    ReadVersions,
    ReadHeads,
    ReadCopies,
    CommitTransaction,
    Finish,
    Error,
}

/// One observed entry, carried between the page reads that classify it.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Observed {
    key: String,
    version_id: Ulid,
    version: Option<BlobVersion>,
}

#[derive(Debug, PartialEq)]
pub struct PolicyCoverageOperation {
    input: CoverageInput,
    state: ScanState,
    txn_id: Option<TxnId>,
    generation: u64,
    target_refs: Vec<PlacementPolicyRef>,
    entries: Vec<Observed>,
    observed: usize,
    deleted: usize,
    gaps: Vec<CoverageGap>,
    copies: CopyCounts,
    cursor: Option<Key>,
    output: Option<Result<CoverageReport, CoverageError>>,
}

impl PolicyCoverageOperation {
    pub fn new(input: CoverageInput) -> Self {
        Self {
            input,
            state: ScanState::Init,
            txn_id: None,
            generation: 0,
            target_refs: Vec::new(),
            entries: Vec::new(),
            observed: 0,
            deleted: 0,
            gaps: Vec::new(),
            copies: CopyCounts::default(),
            cursor: None,
            output: None,
        }
    }

    fn page_limit(&self) -> usize {
        self.input.limit.clamp(1, COVERAGE_PAGE_LIMIT)
    }

    fn fail(&mut self, error: CoverageError) -> Effects {
        self.state = ScanState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn scan_page(&mut self) -> Effects {
        let (key_space, prefix) = match self.input.scope {
            CoverageScope::CurrentHeads => (
                BLOB_HEAD_KEYSPACE.to_string(),
                BlobHeadKey::bucket_prefix(&self.input.bucket),
            ),
            CoverageScope::Historical => (
                BLOB_VERSIONS_KEYSPACE.to_string(),
                VersionKey::bucket_prefix(&self.input.bucket),
            ),
        };
        let prefix = match prefix {
            Ok(prefix) => prefix,
            Err(error) => return self.fail(error.into()),
        };
        self.state = ScanState::ScanPage;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space,
            prefix: Some(prefix.into()),
            start: self.input.start_after.clone().map(IterStart::After),
            limit: self.page_limit(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_page(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.fail(CoverageError::InvalidEvent);
        };
        self.cursor = next_start_after;
        if values.is_empty() {
            return self.commit();
        }
        match self.input.scope {
            CoverageScope::CurrentHeads => self.decode_heads(values),
            CoverageScope::Historical => self.decode_versions(values),
        }
    }

    /// Heads page: the version records they point at are read next.
    fn decode_heads(&mut self, values: Vec<(Key, Value)>) -> Effects {
        let mut reads = Vec::with_capacity(values.len());
        for (key, value) in values {
            let head = match BlobHeadKey::from_bytes(key.as_ref()) {
                Ok(head) => head,
                Err(error) => return self.fail(error.into()),
            };
            let pointer = match CurrentVersionPointer::from_bytes(value.as_ref()) {
                Ok(pointer) => pointer,
                Err(error) => return self.fail(error.into()),
            };
            let version_key = VersionKey::new(&self.input.bucket, &head.key, pointer.version_id);
            let encoded = match version_key.to_bytes() {
                Ok(encoded) => encoded,
                Err(error) => return self.fail(error.into()),
            };
            reads.push((BLOB_VERSIONS_KEYSPACE.to_string(), encoded.into()));
            self.entries.push(Observed {
                key: head.key,
                version_id: pointer.version_id,
                version: None,
            });
        }
        self.state = ScanState::ReadVersions;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })]
    }

    /// Versions page: the object heads decide which of them are historical.
    fn decode_versions(&mut self, values: Vec<(Key, Value)>) -> Effects {
        let mut reads = Vec::with_capacity(values.len());
        for (key, value) in values {
            let version_key = match VersionKey::from_bytes(key.as_ref()) {
                Ok(version_key) => version_key,
                Err(error) => return self.fail(error.into()),
            };
            let version = match BlobVersion::from_bytes(value.as_ref()) {
                Ok(version) => version,
                Err(error) => return self.fail(error.into()),
            };
            let head = match BlobHeadKey::new(&self.input.bucket, &version_key.key).to_bytes() {
                Ok(head) => head,
                Err(error) => return self.fail(error.into()),
            };
            reads.push((BLOB_HEAD_KEYSPACE.to_string(), head.into()));
            self.entries.push(Observed {
                key: version_key.key,
                version_id: version_key.version_id,
                version: Some(version),
            });
        }
        self.state = ScanState::ReadHeads;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })]
    }

    fn handle_versions(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.fail(CoverageError::InvalidEvent);
        };
        if values.len() != self.entries.len() {
            return self.fail(CoverageError::InvalidEvent);
        }
        for (entry, (_, value)) in self.entries.iter_mut().zip(values) {
            entry.version = match value
                .map(|value| BlobVersion::from_bytes(value.as_ref()))
                .transpose()
            {
                Ok(version) => version,
                Err(error) => {
                    self.state = ScanState::Error;
                    self.output = Some(Err(error.into()));
                    return self.abort();
                }
            };
        }
        self.read_copies()
    }

    /// Historical scope: an entry that still is the head is not historical.
    fn handle_heads(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.fail(CoverageError::InvalidEvent);
        };
        if values.len() != self.entries.len() {
            return self.fail(CoverageError::InvalidEvent);
        }
        let entries = std::mem::take(&mut self.entries);
        for (entry, (_, value)) in entries.into_iter().zip(values) {
            let pointer = match value
                .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
                .transpose()
            {
                Ok(pointer) => pointer,
                Err(error) => return self.fail(error.into()),
            };
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
        self.commit()
    }

    fn read_copies(&mut self) -> Effects {
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
            let encoded = match key.to_bytes() {
                Ok(encoded) => encoded,
                Err(error) => return self.fail(error.into()),
            };
            reads.push((MANAGED_COPY_KEYSPACE.to_string(), encoded.into()));
        }
        if reads.is_empty() {
            return self.classify(Vec::new());
        }
        self.state = ScanState::ReadCopies;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })]
    }

    fn handle_copies(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.fail(CoverageError::InvalidEvent);
        };
        let mut states = Vec::with_capacity(values.len());
        for (_, value) in values {
            let state = match value
                .map(|value| ManagedCopyRecord::from_bytes(value.as_ref()))
                .transpose()
            {
                Ok(Some(record)) if record.state.is_serveable() => CopyState::Registered,
                Ok(Some(_)) => CopyState::Quarantined,
                Ok(None) => CopyState::Absent,
                Err(error) => return self.fail(error.into()),
            };
            states.push(state);
        }
        self.classify(states)
    }

    /// Attachment and local copy state are recorded independently, so zero
    /// attachment gaps never implies that every registered copy is compliant.
    fn classify(&mut self, copy_states: Vec<CopyState>) -> Effects {
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
        self.commit()
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
        limits
    }

    fn commit(&mut self) -> Effects {
        self.output = Some(Ok(CoverageReport {
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
        }));
        match self.txn_id {
            Some(txn_id) => {
                self.state = ScanState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            None => {
                self.state = ScanState::Finish;
                smallvec![]
            }
        }
    }
}

impl Operation for PolicyCoverageOperation {
    type Output = CoverageReport;
    type Error = CoverageError;

    fn start(&mut self) -> Effects {
        self.state = ScanState::Authorize;
        let auth_config = CheckPermissionsConfig {
            auth_context: self.input.auth_context.clone(),
            path: policy_admin_path(self.input.auth_context.realm_id),
            required_permission: Permission::READ,
        };
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(auth_config),
            |result| Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: result
            }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state {
            ScanState::Init => self.start(),
            ScanState::Authorize => {
                let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event
                else {
                    return self.fail(CoverageError::InvalidEvent);
                };
                match allowed {
                    Ok(true) => {
                        self.state = ScanState::StartTransaction;
                        smallvec![Effect::Storage(StorageEffect::StartTransaction {
                            read: true
                        })]
                    }
                    Ok(false) => self.fail(CoverageError::Unauthorized),
                    Err(error) => {
                        warn!(error = %error, "Coverage authorization check failed");
                        self.fail(CoverageError::Unauthorized)
                    }
                }
            }
            ScanState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail(CoverageError::InvalidEvent);
                };
                self.txn_id = Some(txn_id);
                self.state = ScanState::ReadBucket;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.input.bucket.as_bytes().to_vec().into(),
                    txn_id: self.txn_id,
                })]
            }
            ScanState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(CoverageError::InvalidEvent);
                };
                let Some(value) = value else {
                    return self.fail(CoverageError::NoSuchBucket);
                };
                let bucket = match BucketInfo::from_bytes(value.as_ref()) {
                    Ok(bucket) => bucket,
                    Err(error) => return self.fail(error.into()),
                };
                self.generation = bucket.placement_policy_generation;
                self.target_refs = bucket.placement_policies;
                self.scan_page()
            }
            ScanState::ScanPage => self.handle_page(event),
            ScanState::ReadVersions => self.handle_versions(event),
            ScanState::ReadHeads => self.handle_heads(event),
            ScanState::ReadCopies => self.handle_copies(event),
            ScanState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail(CoverageError::InvalidEvent);
                };
                self.txn_id = None;
                self.state = ScanState::Finish;
                smallvec![]
            }
            ScanState::Finish => smallvec![],
            ScanState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ScanState::Finish | ScanState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(result) => result,
            None => Err(CoverageError::InvalidEvent),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            CoverageError::Unauthorized | CoverageError::NoSuchBucket
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AttachmentGap, CopyState, CoverageError, CoverageInput, CoverageLimit, CoverageReport,
        CoverageScope, PolicyCoverageOperation,
    };
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        AuthContext, BackendLocation, BackendRef, BlobHeadKey, BlobVersion, BucketInfo,
        CurrentVersionPointer, ManagedCopyRecord, ManagedCopyState, PlacementPolicyRef, RealmId,
        VersionKey,
    };
    use aruna_core::types::{Key, NodeId, UserId};
    use std::collections::HashMap;
    use std::time::UNIX_EPOCH;
    use ulid::Ulid;

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
        }
    }

    fn version(refs: Vec<PlacementPolicyRef>) -> BlobVersion {
        BlobVersion::materialized(
            [6u8; 32],
            BackendRef::node_default(),
            UNIX_EPOCH,
            user_id(),
            None,
        )
        .with_policies(refs)
        .expect("refs seal")
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
                compressed: false,
                encrypted: false,
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

    fn operation(scope: CoverageScope) -> PolicyCoverageOperation {
        PolicyCoverageOperation::new(CoverageInput {
            bucket: BUCKET.to_string(),
            scope,
            start_after: None,
            limit: 16,
            auth_context: AuthContext {
                user_id: user_id(),
                realm_id: RealmId::from_bytes([1u8; 32]),
                path_restrictions: None,
            },
        })
    }

    fn authorized(allowed: bool) -> Event {
        Event::SubOperation(SubOperationEvent::AuthorizationResult {
            allowed: Ok(allowed),
        })
    }

    fn read(value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: value.map(Into::into),
        })
    }

    fn batch(values: Vec<Option<Vec<u8>>>) -> Event {
        Event::Storage(StorageEvent::BatchReadResult {
            values: values
                .into_iter()
                .map(|value| (Key::from(Vec::new()), value.map(Into::into)))
                .collect(),
        })
    }

    fn page(entries: Vec<(&str, Ulid)>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: entries
                .into_iter()
                .map(|(key, version_id)| {
                    (
                        Key::from(BlobHeadKey::new(BUCKET, key).to_bytes().expect("key")),
                        Key::from(
                            CurrentVersionPointer::new(version_id)
                                .to_bytes()
                                .expect("pointer"),
                        ),
                    )
                })
                .collect(),
            next_start_after: None,
        })
    }

    /// Runs a head scan over one page and returns the report.
    fn scan_heads(
        default: Vec<PlacementPolicyRef>,
        version: BlobVersion,
        copy: Option<ManagedCopyRecord>,
    ) -> CoverageReport {
        let version_id = Ulid::from_bytes([9u8; 16]);
        let mut operation = operation(CoverageScope::CurrentHeads);
        operation.start();
        operation.step(authorized(true));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([4u8; 16]),
        }));
        operation.step(read(Some(bucket(default).to_bytes().expect("bucket"))));
        operation.step(page(vec![("object", version_id)]));
        operation.step(batch(vec![Some(version.to_bytes().expect("version"))]));
        operation.step(batch(vec![
            copy.map(|record| record.to_bytes().expect("record")),
        ]));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: Ulid::from_bytes([4u8; 16]),
        }));
        operation.finalize().expect("report returned")
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
                ManagedCopyState::Quarantined(aruna_core::structs::ManagedCopyQuarantine::Rejoin),
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
        let mut operation = operation(CoverageScope::Historical);
        operation.start();
        operation.step(authorized(true));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([4u8; 16]),
        }));
        operation.step(read(Some(
            bucket(vec![policy_ref(1)]).to_bytes().expect("bucket"),
        )));
        operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(
                Key::from(
                    VersionKey::new(BUCKET, "object", version_id)
                        .to_bytes()
                        .expect("key"),
                ),
                Key::from(version(Vec::new()).to_bytes().expect("version")),
            )],
            next_start_after: None,
        }));
        operation.step(batch(vec![Some(
            CurrentVersionPointer::new(version_id)
                .to_bytes()
                .expect("pointer"),
        )]));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: Ulid::from_bytes([4u8; 16]),
        }));
        let report = operation.finalize().expect("report returned");

        assert_eq!(report.observed, 0);
        assert!(report.gaps.is_empty());
        assert!(!report.limits.contains(&CoverageLimit::HistoricalExcluded));
    }

    #[test]
    fn denies_non_admin() {
        // Coverage names policy refs, so it is an administrative read.
        let mut operation = operation(CoverageScope::CurrentHeads);
        operation.start();
        let effects = operation.step(authorized(false));

        assert!(effects.is_empty(), "no transaction is opened");
        assert_eq!(operation.finalize(), Err(CoverageError::Unauthorized));
    }

    #[test]
    fn historical_flags_predecessor() {
        let mut operation = operation(CoverageScope::Historical);
        operation.start();
        operation.step(authorized(true));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([4u8; 16]),
        }));
        operation.step(read(Some(
            bucket(vec![policy_ref(1)]).to_bytes().expect("bucket"),
        )));
        operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(
                Key::from(
                    VersionKey::new(BUCKET, "object", Ulid::from_bytes([9u8; 16]))
                        .to_bytes()
                        .expect("key"),
                ),
                Key::from(version(Vec::new()).to_bytes().expect("version")),
            )],
            next_start_after: None,
        }));
        operation.step(batch(vec![Some(
            CurrentVersionPointer::new(Ulid::from_bytes([8u8; 16]))
                .to_bytes()
                .expect("pointer"),
        )]));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: Ulid::from_bytes([4u8; 16]),
        }));
        let report = operation.finalize().expect("report returned");

        assert_eq!(report.observed, 1);
        assert_eq!(report.gaps.len(), 1);
        assert_eq!(report.gaps[0].copy, None);
    }
}
