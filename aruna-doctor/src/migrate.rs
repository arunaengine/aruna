//! Re-encodes stored rows written before a field was added: job-family rows
//! that embed a physical execution result without stdout and stderr tails, and
//! realm configuration documents without the compute catch-up wait. Safe to
//! repeat: a row already in the current shape is left untouched.

use crate::error::CliError;
use crate::explorer::ExplorerError;
use aruna_core::keyspaces::{
    JOB_FAMILY_CONFLICT_KEYSPACE, JOB_FAMILY_PENDING_KEYSPACE, JOB_FAMILY_PROJECTION_KEYSPACE,
    JOB_FAMILY_RECORD_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::structs::{
    DEFAULT_CATCH_UP_AFTER_MS, ExecutionOutputRecord, ExecutionReceipt, ExecutionUpdate,
    JobCancelRecord, JobFamilyRecord, JobRecordEnvelope, LaunchIntent, LogicalJobSpec,
    PhysicalExecutionResult, PhysicalExecutionState, RealmConfigDocument, RealmId, ResultMessage,
    SubmissionClaim, SubmissionId, WitnessBudgetRecord,
};
use aruna_core::types::NodeId;
use aruna_operations::jobs::records::rows::{ConflictRecord, PendingNeed, PendingRecord};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use serde::{Deserialize, Serialize};
use std::path::Path;
use ulid::Ulid;

#[derive(Debug, Serialize)]
pub struct MigrateOutput {
    pub database_path: String,
    pub records_scanned: usize,
    pub records_rewritten: usize,
    pub pending_scanned: usize,
    pub pending_rewritten: usize,
    pub conflicts_scanned: usize,
    pub conflicts_rewritten: usize,
    /// Derived projection rows dropped. The reader rebuilds a missing one from
    /// the immutable records, so clearing them needs no re-encode.
    pub projections_cleared: usize,
    pub realm_configs_scanned: usize,
    pub realm_configs_rewritten: usize,
}

pub async fn migrate(database_path: String) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking({
        let database_path = database_path.clone();
        move || migrate_output(&database_path)
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn migrate_output(database_path: &str) -> Result<MigrateOutput, ExplorerError> {
    let db = OptimisticTxDatabase::builder(Path::new(database_path)).open()?;
    let record_rows = db.keyspace(JOB_FAMILY_RECORD_KEYSPACE, KeyspaceCreateOptions::default)?;
    let pending_rows = db.keyspace(JOB_FAMILY_PENDING_KEYSPACE, KeyspaceCreateOptions::default)?;
    let conflict_rows =
        db.keyspace(JOB_FAMILY_CONFLICT_KEYSPACE, KeyspaceCreateOptions::default)?;
    let cache_rows = db.keyspace(
        JOB_FAMILY_PROJECTION_KEYSPACE,
        KeyspaceCreateOptions::default,
    )?;
    let config_rows = db.keyspace(REALM_CONFIG_KEYSPACE, KeyspaceCreateOptions::default)?;

    let records = rewrites::<JobRecordEnvelope, LegacyEnvelope>(
        &db,
        &record_rows,
        JOB_FAMILY_RECORD_KEYSPACE,
    )?;
    let pending =
        rewrites::<PendingRecord, LegacyPending>(&db, &pending_rows, JOB_FAMILY_PENDING_KEYSPACE)?;
    let conflicts = rewrites::<ConflictRecord, LegacyConflict>(
        &db,
        &conflict_rows,
        JOB_FAMILY_CONFLICT_KEYSPACE,
    )?;
    let projections = keys(&db, &cache_rows)?;
    let configs = realm_configs(&db, &config_rows)?;

    let mut txn = db.write_tx()?;
    for (keyspace, rows) in [
        (&record_rows, &records.rows),
        (&pending_rows, &pending.rows),
        (&conflict_rows, &conflicts.rows),
        (&config_rows, &configs.rows),
    ] {
        for (key, value) in rows {
            txn.insert(keyspace.clone(), key.clone(), value.clone());
        }
    }
    for key in &projections {
        txn.remove(cache_rows.clone(), key.clone());
    }
    txn.commit()?.map_err(|_| {
        ExplorerError::Decode("migration conflicted with a running node".to_string())
    })?;

    Ok(MigrateOutput {
        database_path: database_path.to_string(),
        records_scanned: records.scanned,
        records_rewritten: records.rows.len(),
        pending_scanned: pending.scanned,
        pending_rewritten: pending.rows.len(),
        conflicts_scanned: conflicts.scanned,
        conflicts_rewritten: conflicts.rows.len(),
        projections_cleared: projections.len(),
        realm_configs_scanned: configs.scanned,
        realm_configs_rewritten: configs.rows.len(),
    })
}

/// Rows of one keyspace that must be written back, plus how many were read.
struct Rewrites {
    scanned: usize,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
}

fn rewrites<Current, Legacy>(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    name: &str,
) -> Result<Rewrites, ExplorerError>
where
    Current: Serialize + for<'a> Deserialize<'a> + From<Legacy>,
    Legacy: for<'a> Deserialize<'a>,
{
    let mut scanned = 0;
    let mut rows = Vec::new();
    for entry in db.read_tx().iter(keyspace) {
        let (key, value) = entry.into_inner()?;
        scanned += 1;
        match rewritten::<Current, Legacy>(value.as_ref()) {
            Ok(Some(bytes)) => rows.push((key.to_vec(), bytes)),
            Ok(None) => {}
            Err(error) => return Err(decode_error(name, &key, error)),
        }
    }
    Ok(Rewrites { scanned, rows })
}

/// Realm configuration documents gained the compute catch-up wait as their last
/// value. Postcard is positional, so an older row is the current row without
/// that trailing value and appending its default re-encodes the row.
fn realm_configs(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
) -> Result<Rewrites, ExplorerError> {
    let mut scanned = 0;
    let mut rows = Vec::new();
    for entry in db.read_tx().iter(keyspace) {
        let (key, value) = entry.into_inner()?;
        scanned += 1;
        if RealmConfigDocument::from_bytes(&value).is_ok() {
            continue;
        }
        let mut bytes = value.to_vec();
        bytes.extend_from_slice(
            &postcard::to_allocvec(&DEFAULT_CATCH_UP_AFTER_MS)
                .map_err(|error| decode_error(REALM_CONFIG_KEYSPACE, &key, error))?,
        );
        RealmConfigDocument::from_bytes(&bytes)
            .map_err(|error| decode_error(REALM_CONFIG_KEYSPACE, &key, error))?;
        rows.push((key.to_vec(), bytes));
    }
    Ok(Rewrites { scanned, rows })
}

fn keys(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
) -> Result<Vec<Vec<u8>>, ExplorerError> {
    let mut keys = Vec::new();
    for entry in db.read_tx().iter(keyspace) {
        keys.push(entry.into_inner()?.0.to_vec());
    }
    Ok(keys)
}

/// Rewrites one row into the current shape. `None` means the row already round
/// trips through the current type, so the migration leaves it alone.
fn rewritten<Current, Legacy>(value: &[u8]) -> Result<Option<Vec<u8>>, postcard::Error>
where
    Current: Serialize + for<'a> Deserialize<'a> + From<Legacy>,
    Legacy: for<'a> Deserialize<'a>,
{
    if let Ok(current) = postcard::from_bytes::<Current>(value)
        && postcard::to_allocvec(&current)? == value
    {
        return Ok(None);
    }
    let legacy: Legacy = postcard::from_bytes(value)?;
    Ok(Some(postcard::to_allocvec(&Current::from(legacy))?))
}

fn decode_error(name: &str, key: &[u8], error: impl std::fmt::Display) -> ExplorerError {
    ExplorerError::Decode(format!("{name}/{}: {error}", hex::encode(key)))
}

/// Previous shape of `PhysicalExecutionResult`, before the bounded stdout and
/// stderr tails were added.
#[derive(Serialize, Deserialize)]
struct LegacyResult {
    exit_code: Option<i32>,
    output_digest: Option<[u8; 32]>,
    message: Option<ResultMessage>,
}

/// Previous shape of `ExecutionUpdate`: identical except for its result.
#[derive(Serialize, Deserialize)]
struct LegacyUpdate {
    execution_id: Ulid,
    submission_id: SubmissionId,
    request_digest: [u8; 32],
    executor_node_id: NodeId,
    sequence: u64,
    previous_digest: [u8; 32],
    state: PhysicalExecutionState,
    observed_at_ms: u64,
    result: Option<LegacyResult>,
}

/// Previous shape of `JobFamilyRecord`: only the update variant differs.
#[derive(Serialize, Deserialize)]
enum LegacyFamilyRecord {
    Spec(Box<LogicalJobSpec>),
    Claim(SubmissionClaim),
    Budget(WitnessBudgetRecord),
    Launch(Box<LaunchIntent>),
    Receipt(Box<ExecutionReceipt>),
    Update(Box<LegacyUpdate>),
    Output(Box<ExecutionOutputRecord>),
    Cancel(JobCancelRecord),
}

/// Previous shape of `JobRecordEnvelope`.
#[derive(Serialize, Deserialize)]
struct LegacyEnvelope {
    realm_id: RealmId,
    record: LegacyFamilyRecord,
    published_by: NodeId,
    signature: iroh::Signature,
}

/// Previous shape of `PendingRecord`.
#[derive(Serialize, Deserialize)]
struct LegacyPending {
    envelope: LegacyEnvelope,
    need: PendingNeed,
    first_seen_ms: u64,
    attempts: u32,
}

/// Previous shape of `ConflictRecord`.
#[derive(Serialize, Deserialize)]
struct LegacyConflict {
    envelope: LegacyEnvelope,
    retained: [u8; 32],
    observed_at_ms: u64,
    relayed_by: Option<NodeId>,
}

impl From<LegacyResult> for PhysicalExecutionResult {
    fn from(legacy: LegacyResult) -> Self {
        Self {
            exit_code: legacy.exit_code,
            output_digest: legacy.output_digest,
            message: legacy.message,
            stdout: None,
            stderr: None,
        }
    }
}

impl From<LegacyUpdate> for ExecutionUpdate {
    fn from(legacy: LegacyUpdate) -> Self {
        Self {
            execution_id: legacy.execution_id,
            submission_id: legacy.submission_id,
            request_digest: legacy.request_digest,
            executor_node_id: legacy.executor_node_id,
            sequence: legacy.sequence,
            previous_digest: legacy.previous_digest,
            state: legacy.state,
            observed_at_ms: legacy.observed_at_ms,
            result: legacy.result.map(Into::into),
        }
    }
}

impl From<LegacyFamilyRecord> for JobFamilyRecord {
    fn from(legacy: LegacyFamilyRecord) -> Self {
        match legacy {
            LegacyFamilyRecord::Spec(spec) => Self::Spec(spec),
            LegacyFamilyRecord::Claim(claim) => Self::Claim(claim),
            LegacyFamilyRecord::Budget(budget) => Self::Budget(budget),
            LegacyFamilyRecord::Launch(launch) => Self::Launch(launch),
            LegacyFamilyRecord::Receipt(receipt) => Self::Receipt(receipt),
            LegacyFamilyRecord::Update(update) => Self::Update(Box::new((*update).into())),
            LegacyFamilyRecord::Output(output) => Self::Output(output),
            LegacyFamilyRecord::Cancel(cancel) => Self::Cancel(cancel),
        }
    }
}

impl From<LegacyEnvelope> for JobRecordEnvelope {
    fn from(legacy: LegacyEnvelope) -> Self {
        Self {
            realm_id: legacy.realm_id,
            record: legacy.record.into(),
            published_by: legacy.published_by,
            signature: legacy.signature,
        }
    }
}

impl From<LegacyPending> for PendingRecord {
    fn from(legacy: LegacyPending) -> Self {
        Self {
            envelope: legacy.envelope.into(),
            need: legacy.need,
            first_seen_ms: legacy.first_seen_ms,
            attempts: legacy.attempts,
        }
    }
}

impl From<LegacyConflict> for ConflictRecord {
    fn from(legacy: LegacyConflict) -> Self {
        Self {
            envelope: legacy.envelope.into(),
            retained: legacy.retained,
            observed_at_ms: legacy.observed_at_ms,
            relayed_by: legacy.relayed_by,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LegacyConflict, LegacyEnvelope, LegacyFamilyRecord, LegacyPending, LegacyResult,
        LegacyUpdate, migrate_output,
    };
    use aruna_core::keyspaces::{
        JOB_FAMILY_CONFLICT_KEYSPACE, JOB_FAMILY_PENDING_KEYSPACE, JOB_FAMILY_PROJECTION_KEYSPACE,
        JOB_FAMILY_RECORD_KEYSPACE, REALM_CONFIG_KEYSPACE,
    };
    use aruna_core::structs::{
        DEFAULT_CATCH_UP_AFTER_MS, ExecutionUpdate, JobFamilyRecord, JobRecordEnvelope,
        PhysicalExecutionState, RealmConfigDocument, RealmId, ResultMessage, SubmissionId,
    };
    use aruna_operations::jobs::records::rows::{PendingNeed, PendingRecord, ProjectionCache};
    use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, Readable};
    use std::collections::BTreeMap;
    use std::path::Path;
    use tempfile::tempdir;
    use ulid::Ulid;

    const REALM: RealmId = RealmId([3u8; 32]);

    fn secret() -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[7u8; 32])
    }

    /// A terminal update in the shape stored before the tails existed.
    fn legacy_update() -> LegacyUpdate {
        LegacyUpdate {
            execution_id: Ulid::from_bytes([1u8; 16]),
            submission_id: SubmissionId([2u8; 32]),
            request_digest: [3u8; 32],
            executor_node_id: secret().public(),
            sequence: 1,
            previous_digest: [4u8; 32],
            state: PhysicalExecutionState::Succeeded,
            observed_at_ms: 1_000,
            result: Some(LegacyResult {
                exit_code: Some(0),
                output_digest: Some([5u8; 32]),
                message: ResultMessage::tail("boom"),
            }),
        }
    }

    fn legacy_envelope() -> LegacyEnvelope {
        LegacyEnvelope {
            realm_id: REALM,
            record: LegacyFamilyRecord::Update(Box::new(legacy_update())),
            published_by: secret().public(),
            signature: secret().sign(b"legacy"),
        }
    }

    /// The same update in the current shape, so a current row is recognisable.
    fn current_envelope() -> JobRecordEnvelope {
        let update: ExecutionUpdate = legacy_update().into();
        JobRecordEnvelope::sign(REALM, JobFamilyRecord::Update(Box::new(update)), &secret())
            .expect("record signs")
    }

    fn write(path: &Path, keyspace: &str, entries: Vec<(&[u8], Vec<u8>)>) {
        let db = OptimisticTxDatabase::builder(path).open().unwrap();
        let keyspace = db
            .keyspace(keyspace, KeyspaceCreateOptions::default)
            .unwrap();
        let mut txn = db.write_tx().unwrap();
        for (key, value) in entries {
            txn.insert(keyspace.clone(), key, value);
        }
        txn.commit().unwrap().unwrap();
    }

    fn read(path: &Path, keyspace: &str) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let db = OptimisticTxDatabase::builder(path).open().unwrap();
        let keyspace = db
            .keyspace(keyspace, KeyspaceCreateOptions::default)
            .unwrap();
        db.read_tx()
            .iter(&keyspace)
            .map(|entry| {
                let (key, value) = entry.into_inner().unwrap();
                (key.to_vec(), value.to_vec())
            })
            .collect()
    }

    #[test]
    fn rewrites_legacy_updates() {
        // A hand-encoded legacy row must decode as the current shape with empty
        // tails, and a row already current must stay byte-identical.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let legacy = postcard::to_allocvec(&legacy_envelope()).unwrap();
        let current = postcard::to_allocvec(&current_envelope()).unwrap();
        write(
            &path,
            JOB_FAMILY_RECORD_KEYSPACE,
            vec![(b"old", legacy), (b"new", current.clone())],
        );

        let output = migrate_output(path.to_str().unwrap()).unwrap();

        assert_eq!((output.records_scanned, output.records_rewritten), (2, 1));
        let rows = read(&path, JOB_FAMILY_RECORD_KEYSPACE);
        assert_eq!(rows[b"new".as_slice()], current);
        let migrated: JobRecordEnvelope =
            postcard::from_bytes(&rows[b"old".as_slice()]).expect("row decodes");
        let JobFamilyRecord::Update(update) = &migrated.record else {
            panic!("the row is an update");
        };
        let result = update.result.as_ref().expect("terminal result");
        assert_eq!(result.exit_code, Some(0));
        assert_eq!(
            result.message.as_ref().map(ResultMessage::as_str),
            Some("boom")
        );
        assert!(result.stdout.is_none() && result.stderr.is_none());

        let again = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!(again.records_rewritten, 0);
    }

    #[test]
    fn rewrites_held_envelopes() {
        // Pending and conflict rows wrap the same envelope, so both shapes are
        // re-encoded too.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let pending = postcard::to_allocvec(&LegacyPending {
            envelope: legacy_envelope(),
            need: PendingNeed::LocalView,
            first_seen_ms: 9,
            attempts: 2,
        })
        .unwrap();
        let conflict = postcard::to_allocvec(&LegacyConflict {
            envelope: legacy_envelope(),
            retained: [6u8; 32],
            observed_at_ms: 11,
            relayed_by: None,
        })
        .unwrap();
        write(&path, JOB_FAMILY_PENDING_KEYSPACE, vec![(b"p", pending)]);
        write(&path, JOB_FAMILY_CONFLICT_KEYSPACE, vec![(b"c", conflict)]);

        let output = migrate_output(path.to_str().unwrap()).unwrap();

        assert_eq!((output.pending_scanned, output.pending_rewritten), (1, 1));
        assert_eq!(
            (output.conflicts_scanned, output.conflicts_rewritten),
            (1, 1)
        );
        let rows = read(&path, JOB_FAMILY_PENDING_KEYSPACE);
        let migrated: PendingRecord =
            postcard::from_bytes(&rows[b"p".as_slice()]).expect("row decodes");
        assert_eq!(migrated.attempts, 2);

        let again = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!((again.pending_rewritten, again.conflicts_rewritten), (0, 0));
    }

    #[test]
    fn clears_projection_cache() {
        // The projection row is derived, so the migration drops it and the node
        // rebuilds it from the immutable records.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let cache = postcard::to_allocvec(&ProjectionCache::invalidated(None)).unwrap();
        write(&path, JOB_FAMILY_PROJECTION_KEYSPACE, vec![(b"f", cache)]);

        let output = migrate_output(path.to_str().unwrap()).unwrap();

        assert_eq!(output.projections_cleared, 1);
        assert!(read(&path, JOB_FAMILY_PROJECTION_KEYSPACE).is_empty());
        let again = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!(again.projections_cleared, 0);
    }

    #[test]
    fn rewrites_realm_configs() {
        // A document stored before the catch-up wait must decode again with the
        // default, and a current document must stay byte-identical.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let document = RealmConfigDocument::new(REALM, Vec::new(), 3);
        let current = postcard::to_allocvec(&document).unwrap();
        let trailer = postcard::to_allocvec(&DEFAULT_CATCH_UP_AFTER_MS).unwrap();
        let legacy = current[..current.len() - trailer.len()].to_vec();
        write(
            &path,
            REALM_CONFIG_KEYSPACE,
            vec![(b"old", legacy), (b"new", current.clone())],
        );

        let output = migrate_output(path.to_str().unwrap()).unwrap();

        assert_eq!(
            (output.realm_configs_scanned, output.realm_configs_rewritten),
            (2, 1)
        );
        let rows = read(&path, REALM_CONFIG_KEYSPACE);
        assert_eq!(rows[b"new".as_slice()], current);
        let migrated = RealmConfigDocument::from_bytes(&rows[b"old".as_slice()]).unwrap();
        assert_eq!(
            migrated.compute.catch_up_after_ms,
            DEFAULT_CATCH_UP_AFTER_MS
        );

        let again = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!(again.realm_configs_rewritten, 0);
    }

    #[test]
    fn fails_unknown_row() {
        // A row that decodes with neither shape names its keyspace and key
        // rather than being skipped.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        write(
            &path,
            JOB_FAMILY_RECORD_KEYSPACE,
            vec![(b"bad", vec![0xff, 0xff, 0xff])],
        );

        let error = migrate_output(path.to_str().unwrap()).unwrap_err();

        assert!(error.to_string().contains(JOB_FAMILY_RECORD_KEYSPACE));
        assert!(error.to_string().contains(&hex::encode(b"bad")));
    }
}
