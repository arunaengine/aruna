//! Tests the workspace access checks and the credential mint by their calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::credential_encryption::EncryptedS3Secret;
use aruna_core::errors::StorageError;
use aruna_core::structs::execution::job::{
    AttemptIntent, JobErrorKind, JobPayload, workspace_credential_id,
};
use aruna_core::structs::storage::format::Compression;
use ulid::Ulid;

use super::*;

const NOW_MS: u64 = 1_000_000;

fn now() -> SystemTime {
    from_ms(NOW_MS)
}

fn spec() -> ExecutionSpec {
    ExecutionSpec {
        group_id: Ulid::from_bytes([2; 16]),
        name: None,
        description: None,
        tags: Default::default(),
        image: "alpine".to_string(),
        entrypoint: None,
        command: Vec::new(),
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

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[3; 32]).public()
}

fn record(spec: &ExecutionSpec) -> JobRecord {
    let realm_id = RealmId([1; 32]);
    let user_id = UserId::local(Ulid::from_bytes([4; 16]), realm_id);
    JobRecord::new(
        JobId::from_bytes([5; 16]),
        JobPayload::Execution(spec.clone()),
        user_id,
        node(),
        1,
        1,
        None,
    )
}

fn run(spec: &ExecutionSpec, record: &JobRecord) -> RunIdentity {
    RunIdentity::new(spec, record, RealmId([1; 32]), node())
}

fn key() -> CredentialEncryptionKey {
    CredentialEncryptionKey::derive(&[7; 32])
}

fn mint(spec: &ExecutionSpec, record: &JobRecord) -> MintCredentialOperation {
    let run = run(spec, record);
    MintCredentialOperation::workspace((spec, record), run, ("work", None), (Some(key()), now()))
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn allowed(allowed: bool) -> IoResult {
    child(Ok::<_, AuthorizationError>(allowed))
}

fn is_child<T>(next: &Result<Next<T>, JobError>) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Child(_))))
}

fn bucket(group_id: GroupId) -> BucketInfo {
    BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }
}

/// The stored credential the mint of `spec` and `record` would accept.
fn stored(spec: &ExecutionSpec, record: &JobRecord, expiry: SystemTime) -> UserAccess {
    let operation = mint(spec, record);
    let access_key = UserAccess::build_access_key(&workspace_credential_id(record.job_id)).unwrap();
    let mut access = UserAccess {
        access_key,
        user_identity: record.created_by,
        group_id: spec.group_id,
        secret: EncryptedS3Secret::empty(),
        expiry,
        path_restrictions: Some(operation.restrictions),
        issued_by: *node().as_bytes(),
        revoked_at: None,
    };
    access.encrypt_secret(&key(), "stored secret").unwrap();
    access
}

/// A mint that passed its group check and waits for the credential lookup.
fn at_lookup(spec: &ExecutionSpec, record: &JobRecord) -> MintCredentialOperation {
    let mut operation = mint(spec, record);
    assert!(is_child(&operation.first()));
    assert!(is_child(&operation.next(allowed(true))));
    operation
}

// A group check alone ends with its verdict.
#[test]
fn group_write_allowed() {
    let spec = spec();
    let record = record(&spec);
    let mut access = WorkspaceAccessOperation::group(run(&spec, &record));
    assert!(is_child(&access.first()));
    assert_eq!(access.next(allowed(true)), Ok(Next::Done(())));
}

// A refusal and a missing group are verdicts; a storage failure is not.
#[test]
fn group_failures_classified() {
    let spec = spec();
    let record = record(&spec);
    let answer = |result| {
        let mut access = WorkspaceAccessOperation::group(run(&spec, &record));
        access.first().unwrap();
        access.next(result).unwrap_err()
    };
    assert_eq!(answer(allowed(false)), denied());
    let missing = child(Err::<bool, _>(AuthorizationError::GroupNotFound));
    assert_eq!(answer(missing), denied());
    let failed = AuthorizationError::StorageError(StorageError::Timeout);
    let error = answer(child(Err::<bool, _>(failed)));
    assert_eq!(error.kind, JobErrorKind::Retryable);
}

// An existing bucket is read after the group check and then checked for writing.
#[test]
fn existing_bucket_checked() {
    let spec = spec();
    let record = record(&spec);
    let mut access = WorkspaceAccessOperation::existing(run(&spec, &record), "work");
    access.first().unwrap();
    assert!(is_child(&access.next(allowed(true))));
    let read = child(Ok::<_, GetBucketError>(bucket(spec.group_id)));
    assert!(is_child(&access.next(read)));
    assert_eq!(access.next(allowed(true)), Ok(Next::Done(())));
}

// A bucket of another group is refused before any write check.
#[test]
fn foreign_bucket_refused() {
    let spec = spec();
    let record = record(&spec);
    let mut access = WorkspaceAccessOperation::existing(run(&spec, &record), "work");
    access.first().unwrap();
    access.next(allowed(true)).unwrap();
    let other = bucket(Ulid::from_bytes([9; 16]));
    let error = access
        .next(child(Ok::<_, GetBucketError>(other)))
        .unwrap_err();
    assert_eq!(error.kind, JobErrorKind::Permanent);
}

// No credential yet: one is created for the run and its secret handed over once.
#[test]
fn mints_new_credential() {
    let spec = spec();
    let record = record(&spec);
    let mut operation = at_lookup(&spec, &record);
    let missing = child(Err::<UserAccess, _>(GetAccessError::NotFound));
    assert!(is_child(&operation.next(missing)));
    let expiry = now() + DEFAULT_WALLTIME + CREDENTIAL_SLACK;
    let access = stored(&spec, &record, expiry);
    let created = (
        access.access_key.clone(),
        Secret::new("new"),
        access.clone(),
    );
    let done = operation.next(child(Ok::<_, CreateUserError>(created)));
    assert_eq!(
        done,
        Ok(Next::Done(WorkspaceCredential {
            access_key: access.access_key,
            secret: "new".to_string(),
            expires_at_ms: expiry_ms(expiry),
        }))
    );
}

// A live credential of this run is read back instead of minted again.
#[test]
fn reuses_live_credential() {
    let spec = spec();
    let record = record(&spec);
    let mut operation = at_lookup(&spec, &record);
    let access = stored(&spec, &record, from_ms(NOW_MS + 1));
    let found = child(Ok::<_, GetAccessError>(access.clone()));
    assert_eq!(
        operation.next(found),
        Ok(Next::Done(WorkspaceCredential {
            access_key: access.access_key,
            secret: "stored secret".to_string(),
            expires_at_ms: NOW_MS + 1,
        }))
    );
}

// A credential that differs from what the run needs, or was revoked, is never used.
#[test]
fn refuses_foreign_credential() {
    let spec = spec();
    let record = record(&spec);
    let mut other = stored(&spec, &record, from_ms(NOW_MS + 1));
    other.group_id = Ulid::from_bytes([9; 16]);
    let mut revoked = stored(&spec, &record, from_ms(NOW_MS + 1));
    revoked.revoked_at = Some(now());
    for access in [other, revoked] {
        let mut operation = at_lookup(&spec, &record);
        let error = operation
            .next(child(Ok::<_, GetAccessError>(access)))
            .unwrap_err();
        assert_eq!(
            error,
            JobError::permanent("workspace credential is invalid")
        );
    }
}

// An expired credential is renewed before an attempt exists and is final after.
#[test]
fn expired_depends_on_intent() {
    let spec = spec();
    let mut record = record(&spec);
    let access = stored(&spec, &record, now());
    let mut fresh = at_lookup(&spec, &record);
    let found = child(Ok::<_, GetAccessError>(access.clone()));
    assert!(is_child(&fresh.next(found)));

    record.attempt_intent = Some(AttemptIntent {
        attempt_no: 0,
        external_name: "attempt".to_string(),
        executor_kind: "docker".to_string(),
        pinned_image: "alpine".to_string(),
        attempt_epoch: 1,
    });
    let mut attempted = at_lookup(&spec, &record);
    let error = attempted
        .next(child(Ok::<_, GetAccessError>(access)))
        .unwrap_err();
    assert_eq!(error, JobError::permanent("workspace credential expired"));
}

// A session credential ends with its walltime or its bearer, whichever comes first.
#[test]
fn session_expiry_bounded() {
    let walltime = Duration::from_secs(60);
    let bearer_ms = NOW_MS + 1_000;
    let bounded = CredentialLife::Session {
        bearer_ms: Some(bearer_ms),
    };
    assert_eq!(bounded.expiry(now(), walltime), from_ms(bearer_ms));
    let open = CredentialLife::Session { bearer_ms: None };
    assert_eq!(open.expiry(now(), walltime), now() + walltime);
    let staged = CredentialLife::Staged.expiry(now(), walltime);
    assert_eq!(staged, now() + walltime + CREDENTIAL_SLACK);
}

// Twenty-five mounted buckets fit the limit and fail only for the missing key; one more is
// refused outright. An input credential needs no group check.
#[test]
fn input_mount_limits() {
    let spec = spec();
    let record = record(&spec);
    let buckets = |count: usize| -> BTreeSet<String> {
        (0..count).map(|index| format!("bucket-{index}")).collect()
    };
    let first = |count| {
        let run = run(&spec, &record);
        let secret = (None, now());
        MintCredentialOperation::inputs((&spec, &record), run, &buckets(count), secret).first()
    };
    assert_eq!(
        first(25),
        Err(JobError::permanent(
            "workspace credential needs a net handle"
        ))
    );
    let over = first(26).unwrap_err();
    assert_eq!(over.kind, JobErrorKind::Permanent);
    assert!(
        over.message
            .starts_with("workspace credential restrictions invalid")
    );
}
