//! Re-encodes job records and RO-Crate checkpoints from before repository transfers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{Rewrites, decode_error};
use crate::explorer::ExplorerError;
use aruna_core::invenio::{ExportIdentity, InvenioRecord, LinkFailure};
use aruna_core::structs::MintPersistentSpec;
use aruna_core::structs::execution::harvest::HarvestJobSpec;
use aruna_core::structs::execution::job::{
    ArtifactRef, AttemptIntent, CapturedInput, CopyJobSpec, ExecutionSpec, ExportOmissionCounts,
    ExportRoCrateResult, ExportRoCrateSpec, ImportRoCrateResult, ImportRoCrateSpec, JobClaim,
    JobError, JobExecutionClass, JobId, JobPayload, JobProgress, JobRecord, JobResultPayload,
    JobState, OutputObject, RoCrateLimits, StagingJobSpec, WorkspaceMode,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::secondary_id::SecondaryIdentifier;
use aruna_core::structs::storage::storage_purge::{StoragePurgeResult, StoragePurgeSpec};
use aruna_core::{NodeId, UserId};
use aruna_operations::jobs::export::ExportCheckpoint;
use aruna_operations::jobs::import::ImportCheckpoint;
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ulid::Ulid;

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
        None::<InvenioRecord>,
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

/// Previous shape of `JobPayload`: only the export variant differs.
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize)]
pub(super) enum LegacyPayload {
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
    Staging(StagingJobSpec),
    ImportRoCrate(ImportRoCrateSpec),
    ExportRoCrate(LegacyExportSpec),
    Harvest(HarvestJobSpec),
    MintPersistentId(MintPersistentSpec),
    StoragePurge(StoragePurgeSpec),
    CopyObject(CopyJobSpec),
}

/// Previous shape of `JobResultPayload`: only the export variant differs.
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize)]
pub(super) enum LegacyResult {
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
    ExportRoCrate(LegacyExportResult),
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
}

/// Previous shape of `JobRecord`.
#[derive(Serialize, Deserialize)]
pub(super) struct LegacyJob {
    pub(super) job_id: JobId,
    pub(super) payload: LegacyPayload,
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
    pub(super) progress: JobProgress,
    pub(super) cancel_requested: bool,
    pub(super) claim: Option<JobClaim>,
    pub(super) dedup_key: Option<Vec<u8>>,
    pub(super) result: Option<LegacyResult>,
    pub(super) execution_class: JobExecutionClass,
    pub(super) plan_digest: Option<[u8; 32]>,
    pub(super) attempt_intent: Option<AttemptIntent>,
    pub(super) workspace_bucket: Option<String>,
    pub(super) workspace_mode: WorkspaceMode,
    pub(super) captured_inputs: Vec<CapturedInput>,
    pub(super) report_digest: Option<[u8; 32]>,
    pub(super) retention_ms: u64,
    pub(super) locally_exhausted: bool,
}

impl From<LegacyExportSpec> for ExportRoCrateSpec {
    fn from(legacy: LegacyExportSpec) -> Self {
        Self {
            destination: None,
            auth_context: legacy.auth_context,
            document_id: legacy.document_id,
            limits: legacy.limits,
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

impl From<LegacyPayload> for JobPayload {
    fn from(legacy: LegacyPayload) -> Self {
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
            LegacyPayload::Staging(spec) => Self::Staging(spec),
            LegacyPayload::ImportRoCrate(spec) => Self::ImportRoCrate(spec),
            LegacyPayload::ExportRoCrate(spec) => Self::ExportRoCrate(spec.into()),
            LegacyPayload::Harvest(spec) => Self::Harvest(spec),
            LegacyPayload::MintPersistentId(spec) => Self::MintPersistentId(spec),
            LegacyPayload::StoragePurge(spec) => Self::StoragePurge(spec),
            LegacyPayload::CopyObject(spec) => Self::CopyObject(spec),
        }
    }
}

impl From<LegacyResult> for JobResultPayload {
    fn from(legacy: LegacyResult) -> Self {
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
        }
    }
}

impl From<LegacyJob> for JobRecord {
    fn from(legacy: LegacyJob) -> Self {
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
            progress: legacy.progress,
            cancel_requested: legacy.cancel_requested,
            claim: legacy.claim,
            dedup_key: legacy.dedup_key,
            result: legacy.result.map(Into::into),
            execution_class: legacy.execution_class,
            plan_digest: legacy.plan_digest,
            attempt_intent: legacy.attempt_intent,
            workspace_bucket: legacy.workspace_bucket,
            workspace_mode: legacy.workspace_mode,
            captured_inputs: legacy.captured_inputs,
            report_digest: legacy.report_digest,
            retention_ms: legacy.retention_ms,
            locally_exhausted: legacy.locally_exhausted,
        }
    }
}
