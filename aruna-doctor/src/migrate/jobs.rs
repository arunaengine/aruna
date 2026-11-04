//! Re-encodes job records and RO-Crate checkpoints from before repository transfers, job pauses
//! or input content, and job records whose session references predate the linked login.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::staging::{self, LegacyStaging};
use super::{Rewrites, decode_error, session_refs};
use crate::explorer::ExplorerError;
use aruna_core::repository::{ExportIdentity, LinkFailure, RepositoryRecord};
use aruna_core::structs::MintPersistentSpec;
use aruna_core::structs::execution::harvest::HarvestJobSpec;
use aruna_core::structs::execution::job::{
    ArtifactRef, AttemptIntent, CapturedInput, CopyJobSpec, ExecutionSpec, ExportOmissionCounts,
    ExportRoCrateResult, ExportRoCrateSpec, ImportRoCrateResult, ImportRoCrateSpec, InputContent,
    JobClaim, JobError, JobExecutionClass, JobId, JobPayload, JobProgress, JobRecord,
    JobResultPayload, JobState, OutputObject, RoCrateLimits, WorkspaceMode, encode_dedup_value,
    parse_dedup_value,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::secondary_id::{RegisterIdentifiersSpec, SecondaryIdentifier};
use aruna_core::structs::storage::backend_import::BackendImportSpec;
use aruna_core::structs::storage::blob::BucketIdentity;
use aruna_core::structs::storage::storage_purge::{StoragePurgeResult, StoragePurgeSpec};
use aruna_core::types::GroupId;
use aruna_core::{NodeId, UserId};
use aruna_operations::jobs::export::ExportCheckpoint;
use aruna_operations::jobs::import::ImportCheckpoint;
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ulid::Ulid;

/// Job records in an older layout. A record rewritten for its session references or its staging
/// spec gets the plan digest of its rewritten payload, so a retry with the same idempotency key
/// still matches. A staging spec takes the identity of the local bucket of its name.
pub(super) fn job_rows(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    name: &str,
    buckets: &BTreeMap<String, BucketIdentity>,
) -> Result<Rewrites, ExplorerError> {
    let mut scanned = 0;
    let mut rows = Vec::new();
    for entry in db.read_tx().iter(keyspace) {
        let (key, value) = entry.into_inner()?;
        scanned += 1;
        if round_trips::<JobRecord>(&value) {
            continue;
        }
        let mut record =
            upgraded(&value).ok_or_else(|| decode_error(name, &key, "unknown shape"))?;
        if let JobPayload::Staging(spec) = &mut record.payload {
            staging::bucket_identity(spec, buckets);
            record.plan_digest = record.plan_digest.map(|_| record.payload.plan_digest());
        }
        let bytes = record
            .to_bytes()
            .map_err(|error| decode_error(name, &key, error))?;
        rows.push((key.to_vec(), bytes));
    }
    Ok(Rewrites { scanned, rows })
}

/// A record in the current shape except for its staging spec.
type StagingJob = LegacyJob<ExportRoCrateSpec, ExportRoCrateResult>;

/// A record of alpha.134: a staging job, progress and captured inputs in their older shapes.
type InputJob = LegacyJob<ExportRoCrateSpec, ExportRoCrateResult, LegacyProgress, LegacyInput>;

/// The current record of an older row; every byte of the row must be used.
fn upgraded(value: &[u8]) -> Option<JobRecord> {
    // The round trip keeps a row of another shape from reading as one of alpha.134.
    if let Ok(old) = postcard::from_bytes::<InputJob>(value)
        && postcard::to_allocvec(&old).is_ok_and(|bytes| bytes == value)
    {
        return Some(input_record(old));
    }
    if let Ok(legacy) = session_refs::decode::<LegacyJob>(value, &[]) {
        return Some(legacy.into());
    }
    if let Ok(staging) = session_refs::decode::<StagingJob>(value, &[]) {
        return Some(staging.into());
    }
    for layout in [session_refs::NO_LOGIN, session_refs::NO_NAME] {
        let record = session_refs::decode::<JobRecord>(value, layout)
            .or_else(|_| session_refs::decode::<StagingJob>(value, layout).map(Into::into))
            .or_else(|_| session_refs::decode::<LegacyJob>(value, layout).map(Into::into));
        if let Ok(mut record) = record {
            record.plan_digest = record.plan_digest.map(|_| record.payload.plan_digest());
            return Some(record);
        }
    }
    None
}

/// The inputs of an alpha.134 execution take the group it runs in as their source group.
fn input_record(old: InputJob) -> JobRecord {
    let mut record = JobRecord::from(old);
    if let JobPayload::Execution(spec) = &record.payload {
        for input in &mut record.captured_inputs {
            input.source_group_id = spec.group_id;
        }
    }
    record
}

/// Dedup rows that name a rewritten job with another plan digest than the job now has.
pub(super) fn dedup_rows(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    jobs: &[(Vec<u8>, Vec<u8>)],
    name: &str,
) -> Result<Rewrites, ExplorerError> {
    let mut digests = BTreeMap::new();
    for (key, value) in jobs {
        let record =
            JobRecord::from_bytes(value).map_err(|error| decode_error(name, key, error))?;
        if let Some(digest) = record.plan_digest {
            digests.insert(record.job_id, digest);
        }
    }
    let mut scanned = 0;
    let mut rows = Vec::new();
    for entry in db.read_tx().iter(keyspace) {
        let (key, value) = entry.into_inner()?;
        scanned += 1;
        let (job_id, stored) =
            parse_dedup_value(&value).map_err(|error| decode_error(name, &key, error))?;
        match digests.get(&job_id) {
            Some(digest) if *digest != stored => {
                rows.push((key.to_vec(), encode_dedup_value(job_id, *digest)));
            }
            _ => {}
        }
    }
    Ok(Rewrites { scanned, rows })
}

/// Which checkpoint shape a job keeps under its id.
#[derive(Clone, Copy)]
pub(super) enum Checkpoint {
    Export,
    Import,
}

/// The checkpoint shape of every RO-Crate job, read from the records after their rewrite.
pub(super) fn checkpoint_kinds(
    db: &OptimisticTxDatabase,
    records: &OptimisticTxKeyspace,
    rewrites: &[(Vec<u8>, Vec<u8>)],
    name: &str,
) -> Result<BTreeMap<Vec<u8>, Checkpoint>, ExplorerError> {
    let rewrites: BTreeMap<&[u8], &[u8]> = rewrites
        .iter()
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect();
    let mut kinds = BTreeMap::new();
    for entry in db.read_tx().iter(records) {
        let (key, stored) = entry.into_inner()?;
        let value = rewrites.get(key.as_ref()).copied().unwrap_or(&stored);
        let record =
            JobRecord::from_bytes(value).map_err(|error| decode_error(name, &key, error))?;
        let kind = match record.payload {
            JobPayload::ExportRoCrate(_) => Checkpoint::Export,
            JobPayload::ImportRoCrate(_) => Checkpoint::Import,
            _ => continue,
        };
        kinds.insert(record.job_id.to_bytes().to_vec(), kind);
    }
    Ok(kinds)
}

/// Export checkpoints gained leading repository fields and import checkpoints trailing
/// ones. Their types are private, so the defaults are spliced in and checked by decoding.
pub(super) fn checkpoint_rows(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    kinds: &BTreeMap<Vec<u8>, Checkpoint>,
    name: &str,
) -> Result<Rewrites, ExplorerError> {
    let export_prefix = postcard::to_allocvec(&(
        false,
        false,
        None::<[u8; 32]>,
        None::<RepositoryRecord>,
        None::<String>,
        None::<LinkFailure>,
        Vec::<String>::new(),
        ExportIdentity::default(),
    ))
    .map_err(|error| ExplorerError::Decode(error.to_string()))?;
    // Empty identifiers and no pull progress.
    let import_suffix = postcard::to_allocvec(&(Vec::<SecondaryIdentifier>::new(), None::<()>))
        .map_err(|error| ExplorerError::Decode(error.to_string()))?;
    let mut scanned = 0;
    let mut rows = Vec::new();
    for entry in db.read_tx().iter(keyspace) {
        let (key, value) = entry.into_inner()?;
        let Some(kind) = kinds.get(key.as_ref()) else {
            continue;
        };
        scanned += 1;
        let (current, candidate) = match kind {
            Checkpoint::Export => (
                round_trips::<ExportCheckpoint>(&value),
                [export_prefix.as_slice(), &value].concat(),
            ),
            Checkpoint::Import => (
                round_trips::<ImportCheckpoint>(&value),
                [value.as_ref(), &import_suffix].concat(),
            ),
        };
        if current {
            continue;
        }
        let upgraded = match kind {
            Checkpoint::Export => round_trips::<ExportCheckpoint>(&candidate),
            Checkpoint::Import => round_trips::<ImportCheckpoint>(&candidate),
        };
        if !upgraded {
            return Err(decode_error(name, &key, "unknown checkpoint shape"));
        }
        rows.push((key.to_vec(), candidate));
    }
    Ok(Rewrites { scanned, rows })
}

fn round_trips<T: Serialize + for<'a> Deserialize<'a>>(value: &[u8]) -> bool {
    postcard::from_bytes::<T>(value)
        .ok()
        .and_then(|decoded| postcard::to_allocvec(&decoded).ok())
        .is_some_and(|bytes| bytes == value)
}

/// Previous shape of `ExportRoCrateSpec`, before the repository destination.
#[derive(Serialize, Deserialize)]
pub(super) struct LegacyExportSpec {
    pub(super) auth_context: AuthContext,
    pub(super) document_id: Ulid,
    pub(super) limits: RoCrateLimits,
}

/// Previous shape of `ExportRoCrateResult`, before the repository record.
#[derive(Serialize, Deserialize)]
pub(super) struct LegacyExportResult {
    pub(super) artifact: Option<ArtifactRef>,
    pub(super) included: u64,
    pub(super) omitted: ExportOmissionCounts,
    pub(super) report_digest: [u8; 32],
}

/// Previous shape of `JobPayload`: the staging and export variants differ.
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize)]
pub(super) enum LegacyPayload<E> {
    Probe {
        steps: u32,
        step_sleep_ms: u64,
        fail_at: Option<u32>,
        panic_at: Option<u32>,
        cleanup_marker: Option<String>,
    },
    Execution(ExecutionSpec),
    WriteRunCrate {
        for_job: JobId,
    },
    TerminalCleanup {
        for_job: JobId,
        attempt: Option<AttemptIntent>,
        access_key: String,
    },
    Staging(LegacyStaging),
    ImportRoCrate(ImportRoCrateSpec),
    ExportRoCrate(E),
    Harvest(HarvestJobSpec),
    MintPersistentId(MintPersistentSpec),
    StoragePurge(StoragePurgeSpec),
    CopyObject(CopyJobSpec),
    RegisterIdentifiers(RegisterIdentifiersSpec),
    BackendImport(BackendImportSpec),
    StoragePurgeFiles(StoragePurgeSpec),
}

/// Previous shape of `JobResultPayload`: only the export variant differs.
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize)]
pub(super) enum LegacyResult<R> {
    Probe {
        completed_steps: u32,
    },
    Execution {
        exit_code: Option<i32>,
        workspace_bucket: Option<String>,
        outputs: Vec<OutputObject>,
        stdout: String,
        stderr: String,
        output_digest: Option<[u8; 32]>,
    },
    RunCrate {
        resource: String,
    },
    Cleanup,
    Staging {
        completed_items: u64,
        failed_items: u64,
    },
    ImportRoCrate(ImportRoCrateResult),
    ExportRoCrate(R),
    Harvest {
        minted: u64,
        updated: u64,
        tombstoned: u64,
        skipped: u64,
    },
    PersistentId {
        pid: String,
        newly_minted: bool,
    },
    StoragePurge(StoragePurgeResult),
    CopyObject {
        version_id: String,
        bytes: u64,
        blake3: String,
    },
    Identifiers {
        changed: bool,
    },
    BackendImport {
        imported: u64,
        skipped: u64,
        failed: u64,
    },
}

/// Previous shape of `JobProgress`, before pauses.
#[derive(Serialize, Deserialize)]
struct LegacyProgress {
    current: u64,
    total: Option<u64>,
    unit: String,
}

/// Previous shape of `CapturedInput`, before the content identity and the source details.
#[derive(Serialize, Deserialize)]
struct LegacyInput {
    destination_key: String,
    source_node_id: NodeId,
    version_id: Ulid,
    blake3: [u8; 32],
    bytes: u64,
    policies: Vec<PlacementPolicyRef>,
}

/// Previous shape of `JobRecord`; with the current export shapes, the one before the staging
/// bucket identity.
#[derive(Serialize, Deserialize)]
pub(super) struct LegacyJob<
    E = LegacyExportSpec,
    R = LegacyExportResult,
    P = JobProgress,
    I = CapturedInput,
> {
    pub(super) job_id: JobId,
    pub(super) payload: LegacyPayload<E>,
    pub(super) state: JobState,
    pub(super) created_by: UserId,
    pub(super) owner_node_id: NodeId,
    pub(super) created_at_ms: u64,
    pub(super) started_at_ms: Option<u64>,
    pub(super) updated_at_ms: u64,
    pub(super) due_at_ms: u64,
    pub(super) finished_at_ms: Option<u64>,
    pub(super) attempts: u32,
    pub(super) next_attempt_epoch: u64,
    pub(super) has_run: bool,
    pub(super) last_error: Option<JobError>,
    pub(super) progress: P,
    pub(super) cancel_requested: bool,
    pub(super) claim: Option<JobClaim>,
    pub(super) dedup_key: Option<Vec<u8>>,
    pub(super) result: Option<LegacyResult<R>>,
    pub(super) execution_class: JobExecutionClass,
    pub(super) plan_digest: Option<[u8; 32]>,
    pub(super) attempt_intent: Option<AttemptIntent>,
    pub(super) workspace_bucket: Option<String>,
    pub(super) workspace_mode: WorkspaceMode,
    pub(super) captured_inputs: Vec<I>,
    pub(super) report_digest: Option<[u8; 32]>,
    pub(super) retention_ms: u64,
    pub(super) locally_exhausted: bool,
}

impl From<LegacyProgress> for JobProgress {
    fn from(old: LegacyProgress) -> Self {
        Self {
            current: old.current,
            total: old.total,
            unit: old.unit,
            paused_until: None,
        }
    }
}

/// The source group is unknown here; `input_record` sets it.
impl From<LegacyInput> for CapturedInput {
    fn from(old: LegacyInput) -> Self {
        Self {
            destination_key: old.destination_key,
            source_node_id: old.source_node_id,
            version_id: old.version_id,
            content: InputContent::Blake3(old.blake3),
            bytes: old.bytes,
            policies: old.policies,
            source_group_id: GroupId::default(),
            public: false,
            link: None,
            source_pin: None,
            served_by: None,
            database: None,
            releases: Vec::new(),
            releases_complete: false,
            grant: None,
        }
    }
}

impl From<LegacyExportSpec> for ExportRoCrateSpec {
    fn from(legacy: LegacyExportSpec) -> Self {
        Self {
            destination: None,
            auth_context: legacy.auth_context,
            document_id: legacy.document_id,
            limits: legacy.limits,
            selection: None,
        }
    }
}

impl From<LegacyExportResult> for ExportRoCrateResult {
    fn from(legacy: LegacyExportResult) -> Self {
        Self {
            repository: None,
            artifact: legacy.artifact,
            included: legacy.included,
            omitted: legacy.omitted,
            report_digest: legacy.report_digest,
        }
    }
}

impl<E: Into<ExportRoCrateSpec>> From<LegacyPayload<E>> for JobPayload {
    fn from(legacy: LegacyPayload<E>) -> Self {
        match legacy {
            LegacyPayload::Probe {
                steps,
                step_sleep_ms,
                fail_at,
                panic_at,
                cleanup_marker,
            } => Self::Probe {
                steps,
                step_sleep_ms,
                fail_at,
                panic_at,
                cleanup_marker,
            },
            LegacyPayload::Execution(spec) => Self::Execution(spec),
            LegacyPayload::WriteRunCrate { for_job } => Self::WriteRunCrate { for_job },
            LegacyPayload::TerminalCleanup {
                for_job,
                attempt,
                access_key,
            } => Self::TerminalCleanup {
                for_job,
                attempt,
                access_key,
            },
            LegacyPayload::Staging(spec) => Self::Staging(spec.into()),
            LegacyPayload::ImportRoCrate(spec) => Self::ImportRoCrate(spec),
            LegacyPayload::ExportRoCrate(spec) => Self::ExportRoCrate(spec.into()),
            LegacyPayload::Harvest(spec) => Self::Harvest(spec),
            LegacyPayload::MintPersistentId(spec) => Self::MintPersistentId(spec),
            LegacyPayload::StoragePurge(spec) => Self::StoragePurge(spec),
            LegacyPayload::CopyObject(spec) => Self::CopyObject(spec),
            LegacyPayload::RegisterIdentifiers(spec) => Self::RegisterIdentifiers(spec),
            LegacyPayload::BackendImport(spec) => Self::BackendImport(spec),
            LegacyPayload::StoragePurgeFiles(spec) => Self::StoragePurgeFiles(spec),
        }
    }
}

impl<R: Into<ExportRoCrateResult>> From<LegacyResult<R>> for JobResultPayload {
    fn from(legacy: LegacyResult<R>) -> Self {
        match legacy {
            LegacyResult::Probe { completed_steps } => Self::Probe { completed_steps },
            LegacyResult::Execution {
                exit_code,
                workspace_bucket,
                outputs,
                stdout,
                stderr,
                output_digest,
            } => Self::Execution {
                exit_code,
                workspace_bucket,
                outputs,
                stdout,
                stderr,
                output_digest,
            },
            LegacyResult::RunCrate { resource } => Self::RunCrate { resource },
            LegacyResult::Cleanup => Self::Cleanup,
            LegacyResult::Staging {
                completed_items,
                failed_items,
            } => Self::Staging {
                completed_items,
                failed_items,
            },
            LegacyResult::ImportRoCrate(result) => Self::ImportRoCrate(result),
            LegacyResult::ExportRoCrate(result) => Self::ExportRoCrate(result.into()),
            LegacyResult::Harvest {
                minted,
                updated,
                tombstoned,
                skipped,
            } => Self::Harvest {
                minted,
                updated,
                tombstoned,
                skipped,
            },
            LegacyResult::PersistentId { pid, newly_minted } => {
                Self::PersistentId { pid, newly_minted }
            }
            LegacyResult::StoragePurge(result) => Self::StoragePurge(result),
            LegacyResult::CopyObject {
                version_id,
                bytes,
                blake3,
            } => Self::CopyObject {
                version_id,
                bytes,
                blake3,
            },
            LegacyResult::Identifiers { changed } => Self::Identifiers { changed },
            LegacyResult::BackendImport {
                imported,
                skipped,
                failed,
            } => Self::BackendImport {
                imported,
                skipped,
                failed,
            },
        }
    }
}

impl<E, R, P, I> From<LegacyJob<E, R, P, I>> for JobRecord
where
    E: Into<ExportRoCrateSpec>,
    R: Into<ExportRoCrateResult>,
    P: Into<JobProgress>,
    I: Into<CapturedInput>,
{
    fn from(legacy: LegacyJob<E, R, P, I>) -> Self {
        Self {
            job_id: legacy.job_id,
            payload: legacy.payload.into(),
            state: legacy.state,
            created_by: legacy.created_by,
            owner_node_id: legacy.owner_node_id,
            created_at_ms: legacy.created_at_ms,
            started_at_ms: legacy.started_at_ms,
            updated_at_ms: legacy.updated_at_ms,
            due_at_ms: legacy.due_at_ms,
            finished_at_ms: legacy.finished_at_ms,
            attempts: legacy.attempts,
            next_attempt_epoch: legacy.next_attempt_epoch,
            has_run: legacy.has_run,
            last_error: legacy.last_error,
            progress: legacy.progress.into(),
            cancel_requested: legacy.cancel_requested,
            claim: legacy.claim,
            dedup_key: legacy.dedup_key,
            result: legacy.result.map(Into::into),
            execution_class: legacy.execution_class,
            plan_digest: legacy.plan_digest,
            attempt_intent: legacy.attempt_intent,
            workspace_bucket: legacy.workspace_bucket,
            workspace_mode: legacy.workspace_mode,
            captured_inputs: legacy.captured_inputs.into_iter().map(Into::into).collect(),
            report_digest: legacy.report_digest,
            retention_ms: legacy.retention_ms,
            locally_exhausted: legacy.locally_exhausted,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{LegacyExportResult, LegacyExportSpec, LegacyJob, LegacyPayload, LegacyResult};
    use crate::migrate::migrate_output;
    use crate::migrate::tests::{read, write};
    use aruna_core::NodeId;
    use aruna_core::UserId;
    use aruna_core::keyspaces::{JOB_KEYSPACE, JOB_STATE_KEYSPACE};
    use aruna_core::structs::execution::job::{
        CapturedInput, ExecutionSpec, ExportOmissionCounts, ExportRoCrateResult, ExportRoCrateSpec,
        ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateSpec, ImportRoCrateTarget,
        InputContent, JobId, JobPayload, JobProgress, JobRecord, JobResultPayload, JobState,
        RoCrateLimits, job_record_key,
    };
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::policy::PlacementPolicyRef;
    use aruna_core::structs::secondary_id::RegisterIdentifiersSpec;
    use aruna_core::structs::storage::backend_import::BackendImportSpec;
    use aruna_core::structs::storage::storage_purge::{StoragePurgeScope, StoragePurgeSpec};
    use aruna_operations::jobs::export::ExportCheckpoint;
    use aruna_operations::jobs::import::ImportCheckpoint;
    use serde::Serialize;
    use tempfile::tempdir;
    use ulid::Ulid;

    const REALM: RealmId = RealmId([1u8; 32]);

    fn auth() -> AuthContext {
        AuthContext {
            user_id: UserId::nil(REALM),
            realm_id: REALM,
            path_restrictions: None,
            session: None,
        }
    }

    /// A finished export in the current shape, the one the legacy row must become.
    fn export_job(job_id: JobId) -> JobRecord {
        let mut record = JobRecord::new(
            job_id,
            JobPayload::ExportRoCrate(ExportRoCrateSpec {
                selection: None,
                destination: None,
                auth_context: auth(),
                document_id: Ulid::from_bytes([2u8; 16]),
                limits: RoCrateLimits::default(),
            }),
            UserId::nil(REALM),
            iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
            5,
            5,
            Some(b"export-key".to_vec()),
        );
        record.state = JobState::Succeeded;
        record.result = Some(JobResultPayload::ExportRoCrate(ExportRoCrateResult {
            repository: None,
            artifact: None,
            included: 3,
            omitted: ExportOmissionCounts::default(),
            report_digest: [9u8; 32],
        }));
        record
    }

    /// The same export as origin/main stored it.
    fn legacy_job(record: JobRecord) -> LegacyJob {
        let JobPayload::ExportRoCrate(spec) = record.payload else {
            panic!("an export job");
        };
        let Some(JobResultPayload::ExportRoCrate(result)) = record.result else {
            panic!("an export result");
        };
        LegacyJob {
            job_id: record.job_id,
            payload: LegacyPayload::ExportRoCrate(LegacyExportSpec {
                auth_context: spec.auth_context,
                document_id: spec.document_id,
                limits: spec.limits,
            }),
            state: record.state,
            created_by: record.created_by,
            owner_node_id: record.owner_node_id,
            created_at_ms: record.created_at_ms,
            started_at_ms: record.started_at_ms,
            updated_at_ms: record.updated_at_ms,
            due_at_ms: record.due_at_ms,
            finished_at_ms: record.finished_at_ms,
            attempts: record.attempts,
            next_attempt_epoch: record.next_attempt_epoch,
            has_run: record.has_run,
            last_error: record.last_error,
            progress: record.progress,
            cancel_requested: record.cancel_requested,
            claim: record.claim,
            dedup_key: record.dedup_key,
            result: Some(LegacyResult::ExportRoCrate(LegacyExportResult {
                artifact: result.artifact,
                included: result.included,
                omitted: result.omitted,
                report_digest: result.report_digest,
            })),
            execution_class: record.execution_class,
            plan_digest: record.plan_digest,
            attempt_intent: record.attempt_intent,
            workspace_bucket: record.workspace_bucket,
            workspace_mode: record.workspace_mode,
            captured_inputs: record.captured_inputs,
            report_digest: record.report_digest,
            retention_ms: record.retention_ms,
            locally_exhausted: record.locally_exhausted,
        }
    }

    fn import_job(job_id: JobId) -> JobRecord {
        JobRecord::new(
            job_id,
            JobPayload::ImportRoCrate(ImportRoCrateSpec {
                auth_context: auth(),
                source: ImportRoCrateSource::Upload {
                    upload_id: Ulid::from_bytes([3u8; 16]),
                },
                target: ImportRoCrateTarget {
                    bucket: "target".to_string(),
                    prefix: "crate".to_string(),
                },
                metadata: ImportMetadataTarget {
                    group_id: Ulid::from_bytes([4u8; 16]),
                    path: "crate".to_string(),
                    public: false,
                },
                limits: RoCrateLimits::default(),
                document_id: Ulid::from_bytes([5u8; 16]),
            }),
            UserId::nil(REALM),
            iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
            5,
            5,
            None,
        )
    }

    #[test]
    fn rewrites_legacy_exports() {
        // Export records and results gain empty repository fields, both checkpoint
        // shapes gain their defaults and other rows stay as they are.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let export_id = JobId::from_bytes([1u8; 16]);
        let import_id = JobId::from_bytes([2u8; 16]);
        let current = export_job(export_id);
        let legacy = postcard::to_allocvec(&legacy_job(current.clone())).unwrap();
        let import = import_job(import_id).to_bytes().unwrap();
        write(
            &path,
            JOB_KEYSPACE,
            vec![
                (&job_record_key(export_id), legacy),
                (&job_record_key(import_id), import.clone()),
            ],
        );
        let export_state = postcard::to_allocvec(&ExportCheckpoint::default()).unwrap();
        let import_state = postcard::to_allocvec(&ImportCheckpoint::default()).unwrap();
        // Origin/main lacked the leading ten and the trailing two default bytes.
        let legacy_export = export_state[10..].to_vec();
        let legacy_import = import_state[..import_state.len() - 2].to_vec();
        write(
            &path,
            JOB_STATE_KEYSPACE,
            vec![
                (&export_id.to_bytes(), legacy_export),
                (&import_id.to_bytes(), legacy_import),
                (b"plan", vec![1, 2, 3]),
            ],
        );

        let output = migrate_output(path.to_str().unwrap()).unwrap();

        assert_eq!((output.jobs_scanned, output.jobs_rewritten), (2, 1));
        assert_eq!(
            (output.checkpoints_scanned, output.checkpoints_rewritten),
            (2, 2)
        );
        let jobs = read(&path, JOB_KEYSPACE);
        let migrated = JobRecord::from_bytes(&jobs[job_record_key(export_id).as_ref()]).unwrap();
        assert_eq!(migrated, current);
        assert_eq!(jobs[job_record_key(import_id).as_ref()], import);
        let states = read(&path, JOB_STATE_KEYSPACE);
        assert_eq!(states[export_id.to_bytes().as_slice()], export_state);
        assert_eq!(states[import_id.to_bytes().as_slice()], import_state);
        assert_eq!(states[b"plan".as_slice()], vec![1, 2, 3]);

        let again = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!((again.jobs_rewritten, again.checkpoints_rewritten), (0, 0));
    }

    /// `bytes` with the one occurrence of `from` replaced by `to`.
    fn splice(bytes: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
        let at = bytes
            .windows(from.len())
            .position(|window| window == from)
            .unwrap();
        [&bytes[..at], to, &bytes[at + from.len()..]].concat()
    }

    #[test]
    fn rewrites_old_sessions() {
        // Sessions stored without the display name, or without the linked login, gain empty
        // fields; the plan digest and the dedup row follow the rewritten payload.
        use aruna_core::keyspaces::DEDUP_INDEX_KEYSPACE;
        use aruna_core::structs::execution::job::{encode_dedup_value, parse_dedup_value};
        use aruna_core::structs::identity::auth::{SessionKind, SessionRef};
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let session = SessionRef {
            sid: Ulid::from_bytes([6u8; 16]).to_string(),
            kind: SessionKind::Portal,
            name: None,
            via: None,
        };
        let full = postcard::to_allocvec(&session).unwrap();
        let (mut jobs, mut dedup, mut current) = (Vec::new(), Vec::new(), Vec::new());
        // The layout of main lacks both fields; the layout before linked logins lacks one.
        for (seed, kept) in [(1u8, full.len() - 2), (2, full.len() - 1)] {
            let job_id = JobId::from_bytes([seed; 16]);
            let mut record = import_job(job_id);
            let JobPayload::ImportRoCrate(spec) = &mut record.payload else {
                panic!("an import job");
            };
            spec.auth_context.session = Some(session.clone());
            let payload = postcard::to_allocvec(&record.payload).unwrap();
            let old_payload = splice(&payload, &full, &full[..kept]);
            let old_digest = *blake3::hash(&old_payload).as_bytes();
            record.plan_digest = Some(old_digest);
            let stored = splice(&record.to_bytes().unwrap(), &full, &full[..kept]);
            jobs.push((job_record_key(job_id).to_vec(), stored));
            dedup.push((vec![seed], encode_dedup_value(job_id, old_digest)));
            record.plan_digest = Some(record.payload.plan_digest());
            current.push(record);
        }
        fn rows(rows: &[(Vec<u8>, Vec<u8>)]) -> Vec<(&[u8], Vec<u8>)> {
            rows.iter()
                .map(|(key, value)| (key.as_slice(), value.clone()))
                .collect()
        }
        write(&path, JOB_KEYSPACE, rows(&jobs));
        write(&path, DEDUP_INDEX_KEYSPACE, rows(&dedup));

        let output = migrate_output(path.to_str().unwrap()).unwrap();

        assert_eq!((output.jobs_rewritten, output.job_dedup_rewritten), (2, 2));
        let stored_jobs = read(&path, JOB_KEYSPACE);
        let stored_dedup = read(&path, DEDUP_INDEX_KEYSPACE);
        for (seed, record) in [1u8, 2].into_iter().zip(&current) {
            let key = job_record_key(record.job_id).to_vec();
            assert_eq!(JobRecord::from_bytes(&stored_jobs[&key]).unwrap(), *record);
            let (job_id, digest) = parse_dedup_value(&stored_dedup[&vec![seed]]).unwrap();
            assert_eq!((job_id, Some(digest)), (record.job_id, record.plan_digest));
        }
        let again = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!((again.jobs_rewritten, again.job_dedup_rewritten), (0, 0));
    }

    /// `CapturedInput` as alpha.134 stored it.
    #[derive(Serialize)]
    struct OldInput {
        destination_key: String,
        source_node_id: NodeId,
        version_id: Ulid,
        blake3: [u8; 32],
        bytes: u64,
        policies: Vec<PlacementPolicyRef>,
    }

    fn execution(group_id: Ulid) -> ExecutionSpec {
        ExecutionSpec {
            group_id,
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine".to_string(),
            entrypoint: None,
            command: vec!["true".to_string()],
            workdir: None,
            env: Default::default(),
            resources: Default::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        }
    }

    #[test]
    fn rewrites_hashed_inputs() {
        // An alpha.134 execution kept its progress without a pause and its inputs with a plain
        // hash. It gains both, its inputs take the execution's group, and a second run is a no-op.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let group = Ulid::from_bytes([7u8; 16]);
        let node = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
        let job_id = JobId::from_bytes([3u8; 16]);
        let policies = vec![PlacementPolicyRef {
            policy_id: Ulid::from_bytes([6u8; 16]),
            digest: [7u8; 32],
        }];
        let old_input = OldInput {
            destination_key: "inputs/table.csv".to_string(),
            source_node_id: node,
            version_id: Ulid::from_bytes([4u8; 16]),
            blake3: [5u8; 32],
            bytes: 42,
            policies: policies.clone(),
        };
        let input = CapturedInput {
            destination_key: old_input.destination_key.clone(),
            source_node_id: node,
            version_id: old_input.version_id,
            content: InputContent::Blake3([5u8; 32]),
            bytes: 42,
            policies,
            source_group_id: group,
            public: false,
            link: None,
            source_pin: None,
            served_by: None,
            database: None,
            releases: Vec::new(),
            releases_complete: false,
            grant: None,
        };
        let payload = JobPayload::Execution(execution(group));
        let mut record = JobRecord::new(job_id, payload, UserId::nil(REALM), node, 5, 5, None);
        record.progress = JobProgress::new("alpha-files");
        record.captured_inputs = vec![input.clone()];
        // Alpha.134 lacked the trailing pause byte and wrote each input in its older shape.
        let progress = postcard::to_allocvec(&record.progress).unwrap();
        let unpaused = &progress[..progress.len() - 1];
        let stored = splice(&record.to_bytes().unwrap(), &progress, unpaused);
        let new_input = postcard::to_allocvec(&input).unwrap();
        let stored = splice(
            &stored,
            &new_input,
            &postcard::to_allocvec(&old_input).unwrap(),
        );
        write(&path, JOB_KEYSPACE, vec![(&job_record_key(job_id), stored)]);

        let output = migrate_output(path.to_str().unwrap()).unwrap();

        assert_eq!((output.jobs_scanned, output.jobs_rewritten), (1, 1));
        let jobs = read(&path, JOB_KEYSPACE);
        let migrated = JobRecord::from_bytes(&jobs[job_record_key(job_id).as_ref()]).unwrap();
        assert_eq!(migrated, record);
        let again = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!(again.jobs_rewritten, 0);
        assert_eq!(read(&path, JOB_KEYSPACE), jobs);
    }

    #[test]
    fn rewrites_later_kinds() {
        // Identifier registrations, backend imports and file purges of alpha.134 differ from the
        // current shape only by the missing pause byte, and gain it.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let node = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
        let group = Ulid::from_bytes([7u8; 16]);
        let identifiers = JobPayload::RegisterIdentifiers(RegisterIdentifiersSpec {
            document_id: Ulid::from_bytes([2u8; 16]),
            identifiers: Vec::new(),
            auth_context: auth(),
        });
        let import = JobPayload::BackendImport(BackendImportSpec {
            auth_context: auth(),
            node_id: node,
            group_id: group,
            import_id: Ulid::from_bytes([3u8; 16]),
        });
        let purge = JobPayload::StoragePurgeFiles(StoragePurgeSpec {
            scope: StoragePurgeScope::Bucket {
                bucket: "linked".to_string(),
            },
            group_id: group,
            auth_context: auth(),
            node_id: node,
        });
        let results = [
            Some(JobResultPayload::Identifiers { changed: true }),
            Some(JobResultPayload::BackendImport {
                imported: 4,
                skipped: 1,
                failed: 0,
            }),
            None,
        ];
        let mut records = Vec::new();
        let mut rows = Vec::new();
        let payloads = [identifiers, import, purge].into_iter().zip(results);
        for (seed, (payload, result)) in (1u8..).zip(payloads) {
            let job_id = JobId::from_bytes([seed; 16]);
            let mut record = JobRecord::new(job_id, payload, UserId::nil(REALM), node, 5, 5, None);
            record.progress = JobProgress::new("later-kinds");
            record.result = result;
            let progress = postcard::to_allocvec(&record.progress).unwrap();
            let unpaused = &progress[..progress.len() - 1];
            let stored = splice(&record.to_bytes().unwrap(), &progress, unpaused);
            rows.push((job_record_key(job_id), stored));
            records.push(record);
        }
        let entries = rows
            .iter()
            .map(|(key, value)| (key.as_ref(), value.clone()));
        write(&path, JOB_KEYSPACE, entries.collect());

        let output = migrate_output(path.to_str().unwrap()).unwrap();

        assert_eq!((output.jobs_scanned, output.jobs_rewritten), (3, 3));
        let jobs = read(&path, JOB_KEYSPACE);
        for record in &records {
            let key = job_record_key(record.job_id);
            assert_eq!(JobRecord::from_bytes(&jobs[key.as_ref()]).unwrap(), *record);
        }
        let again = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!(again.jobs_rewritten, 0);
        assert_eq!(read(&path, JOB_KEYSPACE), jobs);
    }
}
