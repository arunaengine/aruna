//! Builds the node report, one section per subject, from a stopped node's database.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::CliError;
use crate::explorer::ExplorerError;
use aruna_core::keyspaces::{
    ACTIVE_USER_KEYSPACE, ARTIFACT_TOMBSTONE_KEYSPACE, ATTEMPT_CONTROL_KEYSPACE,
    DEADLINE_INDEX_KEYSPACE, DEDUP_INDEX_KEYSPACE, FAMILY_ALIAS_KEYSPACE, FAMILY_CONFLICT_KEYSPACE,
    FAMILY_OUTBOX_KEYSPACE, FAMILY_PENDING_KEYSPACE, FAMILY_PROJECTION_KEYSPACE,
    FAMILY_RECORD_KEYSPACE, INPUT_PAGE_KEYSPACE, JOB_CLAIM_KEYSPACE, JOB_ENTRY_KEYSPACE,
    JOB_INDEX_KEYSPACE, JOB_KEYSPACE, JOB_RESERVATION_KEYSPACE, JOB_STATE_KEYSPACE,
    LAUNCH_RECORD_KEYSPACE, OUTPUT_RECORD_KEYSPACE, PLAN_EXPLAIN_KEYSPACE, PLUGIN_RUNS_KEYSPACE,
    PLUGIN_WORK_KEYSPACE, RUN_CRATE_KEYSPACE, RUN_LINK_KEYSPACE, SCHEDULE_INDEX_KEYSPACE,
    STAGING_MANIFEST_KEYSPACE, STAGING_QUEUE_KEYSPACE, STAGING_STATE_KEYSPACE,
    WITNESS_DEADLINE_KEYSPACE,
};
use aruna_core::plugin_runs::{PluginRun, RUN_ROW};
use aruna_core::structs::execution::job::record_rows::{
    OutboxEntry, PendingRecord, ProjectionCache,
};
use aruna_core::structs::execution::job::{
    DUE_INDEX_PREFIX, JobPayload, JobRecord, JobRecordKey, LEASE_INDEX_PREFIX, PRUNE_INDEX_PREFIX,
    cleanup_job_id, job_record_key, parse_schedule_key,
};
use aruna_operations::plugins::work::{WORK_ROW, WorkRow, WorkState, index_key};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

/// Every keyspace whose rows the report counts.
const COUNTED_KEYSPACES: &[&str] = &[
    JOB_KEYSPACE,
    SCHEDULE_INDEX_KEYSPACE,
    JOB_INDEX_KEYSPACE,
    ACTIVE_USER_KEYSPACE,
    DEDUP_INDEX_KEYSPACE,
    JOB_CLAIM_KEYSPACE,
    ATTEMPT_CONTROL_KEYSPACE,
    LAUNCH_RECORD_KEYSPACE,
    OUTPUT_RECORD_KEYSPACE,
    JOB_ENTRY_KEYSPACE,
    RUN_CRATE_KEYSPACE,
    RUN_LINK_KEYSPACE,
    ARTIFACT_TOMBSTONE_KEYSPACE,
    JOB_STATE_KEYSPACE,
    STAGING_STATE_KEYSPACE,
    STAGING_QUEUE_KEYSPACE,
    STAGING_MANIFEST_KEYSPACE,
    FAMILY_RECORD_KEYSPACE,
    INPUT_PAGE_KEYSPACE,
    FAMILY_PENDING_KEYSPACE,
    FAMILY_CONFLICT_KEYSPACE,
    FAMILY_ALIAS_KEYSPACE,
    FAMILY_PROJECTION_KEYSPACE,
    FAMILY_OUTBOX_KEYSPACE,
    JOB_RESERVATION_KEYSPACE,
    WITNESS_DEADLINE_KEYSPACE,
    DEADLINE_INDEX_KEYSPACE,
    PLAN_EXPLAIN_KEYSPACE,
    PLUGIN_WORK_KEYSPACE,
    PLUGIN_RUNS_KEYSPACE,
];

const HOUR_MS: u64 = 3_600_000;
const DAY_MS: u64 = 24 * HOUR_MS;
const WEEK_MS: u64 = 7 * DAY_MS;

/// The node report: where and when it was read, and one section per subject.
#[derive(Debug, Serialize)]
pub struct Report {
    pub database_path: String,
    pub now_ms: u64,
    pub sections: Vec<Section>,
}

/// One part of the report under its heading.
#[derive(Debug, Serialize)]
pub struct Section {
    pub heading: &'static str,
    pub content: serde_json::Value,
}

impl Report {
    /// Adds `content` as the section `heading`, in the order the sections are built.
    fn add(&mut self, heading: &'static str, content: &impl Serialize) -> Result<(), CliError> {
        let content = serde_json::to_value(content)?;
        self.sections.push(Section { heading, content });
        Ok(())
    }
}

/// The "Job records" section: job, family and plugin rows, overdue rows and held rows.
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct JobRecords {
    /// Rows per counted keyspace that exists on this node.
    pub rows: BTreeMap<String, u64>,
    /// Local job records by payload kind, then by state.
    pub jobs: BTreeMap<String, BTreeMap<String, u64>>,
    pub schedule: ScheduleRows,
    pub families: FamilyRows,
    pub outbox: OutboxRows,
    pub plugins: PluginRows,
    /// Overdue prune rows that the prune pass keeps, by hold reason.
    pub held: BTreeMap<String, u64>,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ScheduleRows {
    pub due: u64,
    pub lease: u64,
    pub prune: u64,
    /// Prune rows whose time has passed.
    pub overdue: u64,
    pub oldest_overdue_ms: Option<u64>,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FamilyRows {
    /// Accepted records by record kind.
    pub records: BTreeMap<String, u64>,
    /// Cached projections by logical state.
    pub projections: BTreeMap<String, u64>,
    pub pending: u64,
    pub oldest_pending_ms: Option<u64>,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct OutboxRows {
    pub entries: u64,
    pub oldest_queued_ms: Option<u64>,
    /// Entries by age: under an hour, a day, a week, or older.
    pub ages: BTreeMap<String, u64>,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PluginRows {
    /// Work rows by state.
    pub work: BTreeMap<String, u64>,
    /// Work rows whose end was not delivered yet.
    pub not_notified: u64,
    /// Runs by state.
    pub runs: BTreeMap<String, u64>,
}

pub async fn print_report(database_path: String) -> Result<(), CliError> {
    let now_ms = aruna_clock::unix_timestamp_millis();
    let output = tokio::task::spawn_blocking(move || build_report(&database_path, now_ms))
        .await
        .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

/// Reads the database at `database_path` at `now_ms` and adds every section in turn.
pub(crate) fn build_report(database_path: &str, now_ms: u64) -> Result<Report, CliError> {
    let database = Database::open(database_path)?;
    let mut report = Report {
        database_path: database_path.to_string(),
        now_ms,
        sections: Vec::new(),
    };
    job_records(&database, &mut report)?;
    Ok(report)
}

/// The file Fjall writes when it creates a database and checks before it recovers one
/// (`fjall::file::VERSION_MARKER`, which Fjall keeps private).
const FJALL_MARKER: &str = "version";

/// A stopped node's database, opened the way `explore` and `export` open it. The report writes no
/// records and opens only keyspaces that exist, so it creates none.
struct Database {
    db: OptimisticTxDatabase,
    existing: HashSet<String>,
}

impl Database {
    /// Refuses a path without a Fjall database, which Fjall would otherwise create empty.
    fn open(database_path: &str) -> Result<Self, ExplorerError> {
        if !Path::new(database_path).join(FJALL_MARKER).is_file() {
            return Err(ExplorerError::MissingDatabase(database_path.to_string()));
        }
        let db = OptimisticTxDatabase::builder(Path::new(database_path)).open()?;
        let existing = (db.list_keyspace_names().iter())
            .map(|name| name.as_ref().to_string())
            .collect();
        Ok(Self { db, existing })
    }

    fn keyspace(&self, name: &str) -> Result<Option<OptimisticTxKeyspace>, ExplorerError> {
        match self.existing.contains(name) {
            true => Ok(Some(
                self.db.keyspace(name, KeyspaceCreateOptions::default)?,
            )),
            false => Ok(None),
        }
    }
}

/// Adds the "Job records" section.
fn job_records(database: &Database, report: &mut Report) -> Result<(), CliError> {
    let now_ms = report.now_ms;
    let snapshot = database.db.read_tx();
    let mut records = JobRecords::default();

    for name in COUNTED_KEYSPACES {
        if let Some(keyspace) = database.keyspace(name)? {
            records
                .rows
                .insert(name.to_string(), snapshot.len(&keyspace)? as u64);
        }
    }
    let jobs = database.keyspace(JOB_KEYSPACE)?;
    let work = database.keyspace(PLUGIN_WORK_KEYSPACE)?;
    if let Some(keyspace) = &jobs {
        count_jobs(&snapshot, keyspace, &mut records)?;
    }
    if let Some(keyspace) = database.keyspace(SCHEDULE_INDEX_KEYSPACE)? {
        let holds = Holds {
            jobs: jobs.as_ref(),
            work: work.as_ref(),
        };
        count_schedule(&snapshot, &keyspace, &holds, now_ms, &mut records)?;
    }
    if let Some(keyspace) = database.keyspace(FAMILY_RECORD_KEYSPACE)? {
        for entry in snapshot.iter(&keyspace) {
            let key = entry.key()?;
            let kind = JobRecordKey::from_bytes(key.as_ref())
                .map_or("unreadable".to_string(), |key| {
                    format!("{:?}", key.kind).to_lowercase()
                });
            bump(&mut records.families.records, kind);
        }
    }
    if let Some(keyspace) = database.keyspace(FAMILY_PROJECTION_KEYSPACE)? {
        for entry in snapshot.iter(&keyspace) {
            let value = entry.value()?;
            let state = match ProjectionCache::decode(value.as_ref()) {
                Some(cache) => cache
                    .projection
                    .map_or("empty", |projection| projection.state.name()),
                None => "unreadable",
            };
            bump(&mut records.families.projections, state.to_string());
        }
    }
    if let Some(keyspace) = database.keyspace(FAMILY_PENDING_KEYSPACE)? {
        for entry in snapshot.iter(&keyspace) {
            let value = entry.value()?;
            records.families.pending += 1;
            if let Ok(row) = postcard::from_bytes::<PendingRecord>(value.as_ref()) {
                fold_oldest(&mut records.families.oldest_pending_ms, row.first_seen_ms);
            }
        }
    }
    if let Some(keyspace) = database.keyspace(FAMILY_OUTBOX_KEYSPACE)? {
        for entry in snapshot.iter(&keyspace) {
            let value = entry.value()?;
            records.outbox.entries += 1;
            if let Ok(row) = postcard::from_bytes::<OutboxEntry>(value.as_ref()) {
                fold_oldest(&mut records.outbox.oldest_queued_ms, row.queued_at_ms);
                let age = now_ms.saturating_sub(row.queued_at_ms);
                bump(&mut records.outbox.ages, age_bucket(age).to_string());
            }
        }
    }
    if let Some(keyspace) = &work {
        for entry in snapshot.prefix(keyspace, [WORK_ROW]) {
            let value = entry.value()?;
            let Ok(row) = postcard::from_bytes::<WorkRow>(value.as_ref()) else {
                bump(&mut records.plugins.work, "unreadable".to_string());
                continue;
            };
            bump(&mut records.plugins.work, work_state(row.state).to_string());
            records.plugins.not_notified += u64::from(!row.notified);
        }
    }
    if let Some(keyspace) = database.keyspace(PLUGIN_RUNS_KEYSPACE)? {
        for entry in snapshot.prefix(&keyspace, [RUN_ROW]) {
            let value = entry.value()?;
            let state = postcard::from_bytes::<PluginRun>(value.as_ref())
                .map_or("unreadable", |run| run.state.as_str());
            bump(&mut records.plugins.runs, state.to_string());
        }
    }
    report.add("Job records", &records)
}

/// The keyspaces the local prune holds read.
struct Holds<'a> {
    jobs: Option<&'a OptimisticTxKeyspace>,
    work: Option<&'a OptimisticTxKeyspace>,
}

fn count_jobs(
    snapshot: &impl Readable,
    jobs: &OptimisticTxKeyspace,
    records: &mut JobRecords,
) -> Result<(), ExplorerError> {
    for entry in snapshot.iter(jobs) {
        let value = entry.value()?;
        let (kind, state) = match JobRecord::from_bytes(value.as_ref()) {
            Ok(record) => (record.payload.kind(), record.state.name()),
            Err(_) => ("unreadable", "unreadable"),
        };
        let states = records.jobs.entry(kind.to_string()).or_default();
        bump(states, state.to_string());
    }
    Ok(())
}

fn count_schedule(
    snapshot: &impl Readable,
    schedule: &OptimisticTxKeyspace,
    holds: &Holds<'_>,
    now_ms: u64,
    records: &mut JobRecords,
) -> Result<(), ExplorerError> {
    records.schedule.due = snapshot.prefix(schedule, DUE_INDEX_PREFIX).count() as u64;
    records.schedule.lease = snapshot.prefix(schedule, LEASE_INDEX_PREFIX).count() as u64;
    for entry in snapshot.prefix(schedule, PRUNE_INDEX_PREFIX) {
        let key = entry.key()?;
        records.schedule.prune += 1;
        let Ok((expiry_ms, job_id)) = parse_schedule_key(key.as_ref()) else {
            continue;
        };
        if expiry_ms > now_ms {
            continue;
        }
        records.schedule.overdue += 1;
        fold_oldest(&mut records.schedule.oldest_overdue_ms, expiry_ms);
        let Some(jobs) = holds.jobs else {
            continue;
        };
        let Some(record) = read_job(snapshot, jobs, job_record_key(job_id).as_ref())? else {
            continue;
        };
        if let Some(reason) = hold_reason(snapshot, holds, &record)? {
            bump(&mut records.held, reason.to_string());
        }
    }
    Ok(())
}

/// The hold the local prune pass applies to `record`, read the way the pass reads it.
fn hold_reason(
    snapshot: &impl Readable,
    holds: &Holds<'_>,
    record: &JobRecord,
) -> Result<Option<&'static str>, ExplorerError> {
    match &record.payload {
        JobPayload::Execution(_) => {
            let Some(jobs) = holds.jobs else {
                return Ok(None);
            };
            let key = job_record_key(cleanup_job_id(record.job_id));
            let cleanup = read_job(snapshot, jobs, key.as_ref())?;
            Ok(cleanup
                .is_some_and(|cleanup| !cleanup.state.is_terminal())
                .then_some("cleanup_pending"))
        }
        JobPayload::PluginWork(spec) => {
            let Some(work) = holds.work else {
                return Ok(None);
            };
            let Some(row_key) = snapshot.get(work, index_key(spec.install_id, spec.work_id))?
            else {
                return Ok(None);
            };
            let row = snapshot
                .get(work, row_key)?
                .and_then(|bytes| postcard::from_bytes::<WorkRow>(bytes.as_ref()).ok());
            Ok(row
                .is_some_and(|row| row.work_id == spec.work_id && row.state != WorkState::Settled)
                .then_some("plugin_work"))
        }
        _ => Ok(None),
    }
}

fn read_job(
    snapshot: &impl Readable,
    jobs: &OptimisticTxKeyspace,
    key: &[u8],
) -> Result<Option<JobRecord>, ExplorerError> {
    Ok(snapshot
        .get(jobs, key)?
        .and_then(|bytes| JobRecord::from_bytes(bytes.as_ref()).ok()))
}

fn work_state(state: WorkState) -> &'static str {
    match state {
        WorkState::Reserved => "reserved",
        WorkState::Dispatched => "dispatched",
        WorkState::Settled => "settled",
    }
}

fn age_bucket(age_ms: u64) -> &'static str {
    match age_ms {
        age if age < HOUR_MS => "hour",
        age if age < DAY_MS => "day",
        age if age < WEEK_MS => "week",
        _ => "older",
    }
}

fn bump(counts: &mut BTreeMap<String, u64>, name: String) {
    *counts.entry(name).or_default() += 1;
}

fn fold_oldest(oldest: &mut Option<u64>, time_ms: u64) {
    *oldest = Some(oldest.map_or(time_ms, |current| current.min(time_ms)));
}

#[cfg(test)]
#[path = "report_tests.rs"]
mod tests;
