//! Checks a run's write access to its workspace and mints the S3 credential it works with.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::time::{Duration, SystemTime};

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::Secret;
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::errors::AuthorizationError;
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::permission_path::validate_restriction_limits;
use aruna_core::structs::execution::job::{
    ExecutionSpec, JobError, JobId, JobRecord, workspace_credential_id,
};
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    BucketInfo, UserAccess, bucket_permission_path, group_permission_path,
};
use aruna_core::types::GroupId;

use super::DEFAULT_WALLTIME;
use super::workspace::{authorization_error, bucket_lookup_error};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::jobs::lifecycle::ids::session_of;
use crate::s3::access::create::{CreateUserConfig, CreateUserError, CreateUserOperation};
use crate::s3::access::get::{GetAccessError, GetAccessOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

/// Credential lifetime past the walltime so a slow finalize still authorizes.
const CREDENTIAL_SLACK: Duration = Duration::from_secs(6 * 60 * 60);

/// Minted workspace S3 credential handed to the container.
#[derive(PartialEq)]
pub struct WorkspaceCredential {
    pub access_key: String,
    pub secret: String,
    /// Wall-clock milliseconds the credential stops working at.
    pub expires_at_ms: u64,
}

impl std::fmt::Debug for WorkspaceCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkspaceCredential")
            .field("access_key", &self.access_key)
            .field("expires_at_ms", &self.expires_at_ms)
            .finish_non_exhaustive()
    }
}

/// Who a run acts as and where: its creator, its group, the serving realm and this node.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RunIdentity {
    pub job_id: JobId,
    pub user_id: UserId,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
}

impl RunIdentity {
    pub fn new(
        spec: &ExecutionSpec,
        record: &JobRecord,
        realm_id: RealmId,
        node_id: NodeId,
    ) -> Self {
        Self {
            job_id: record.job_id,
            user_id: record.created_by,
            group_id: spec.group_id,
            realm_id,
            node_id,
        }
    }

    fn write_check(&self, path: String) -> CheckPermissionsOperation {
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: AuthContext {
                user_id: self.user_id,
                realm_id: self.realm_id,
                path_restrictions: None,
                session: None,
            },
            path,
            required_permission: Permission::WRITE,
        })
    }

    fn group_check(&self) -> CheckPermissionsOperation {
        self.write_check(group_permission_path(
            self.realm_id,
            self.group_id,
            self.node_id,
        ))
    }

    fn bucket_path(&self, bucket: &str) -> String {
        bucket_permission_path(self.realm_id, self.group_id, self.node_id, bucket)
    }
}

fn denied() -> JobError {
    JobError::permanent("workspace write access denied")
}

/// The verdict of the check on the run's group: a missing target denies the write.
fn group_verdict(result: IoResult) -> Result<(), JobError> {
    let allowed = result
        .child::<bool, AuthorizationError>()?
        .map_err(|error| match error {
            AuthorizationError::InvalidRealmId
            | AuthorizationError::InvalidGroupId
            | AuthorizationError::GroupNotFound
            | AuthorizationError::DocNotFound => denied(),
            other => JobError::retryable(format!("workspace authorization failed: {other}")),
        })?;
    allowed.then_some(()).ok_or_else(denied)
}

#[derive(Debug, PartialEq)]
enum AccessStep {
    Group,
    Bucket,
    BucketWrite,
}

/// Checks that the run's creator may write to its group and, for a run inside an existing
/// bucket, that the bucket belongs to that group and is writable. A run never creates a bucket.
#[derive(Debug, PartialEq)]
pub struct WorkspaceAccessOperation {
    run: RunIdentity,
    bucket: Option<String>,
    step: AccessStep,
}

impl WorkspaceAccessOperation {
    pub fn group(run: RunIdentity) -> Self {
        Self {
            run,
            bucket: None,
            step: AccessStep::Group,
        }
    }

    pub fn existing(run: RunIdentity, bucket: &str) -> Self {
        Self {
            run,
            bucket: Some(bucket.to_string()),
            step: AccessStep::Group,
        }
    }
}

impl Operation for WorkspaceAccessOperation {
    type Output = ();
    type Error = JobError;

    fn first(&mut self) -> Result<Next<()>, JobError> {
        Ok(Next::Call(IoCall::child(self.run.group_check())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, JobError> {
        match self.step {
            AccessStep::Group => {
                group_verdict(result)?;
                let Some(bucket) = &self.bucket else {
                    return Ok(Next::Done(()));
                };
                let read = GetBucketOperation::new(bucket.clone());
                self.step = AccessStep::Bucket;
                Ok(Next::Call(IoCall::child(read)))
            }
            AccessStep::Bucket => {
                let info = result
                    .child::<BucketInfo, GetBucketError>()?
                    .map_err(|error| bucket_lookup_error("workspace", error))?;
                if info.group_id != self.run.group_id {
                    return Err(JobError::permanent(
                        "existing workspace bucket is outside the execution group",
                    ));
                }
                let bucket = self.bucket.as_deref().unwrap_or_default();
                let check = self.run.write_check(self.run.bucket_path(bucket));
                self.step = AccessStep::BucketWrite;
                Ok(Next::Call(IoCall::child(check)))
            }
            AccessStep::BucketWrite => {
                let allowed = result
                    .child::<bool, AuthorizationError>()?
                    .map_err(|error| authorization_error("workspace", error))?;
                allowed.then_some(Next::Done(())).ok_or_else(denied)
            }
        }
    }
}

/// How long a new credential lasts.
#[derive(Clone, Copy, Debug, PartialEq)]
enum CredentialLife {
    /// A staged run keeps the slack its capture needs.
    Staged,
    /// A session's credential outlives neither its job nor the bearer that asked for it.
    Session { bearer_ms: Option<u64> },
}

impl CredentialLife {
    fn of(spec: &ExecutionSpec) -> Self {
        match session_of(spec) {
            Some(session) => Self::Session {
                bearer_ms: session.expires_at_ms,
            },
            None => Self::Staged,
        }
    }

    fn expiry(self, now: SystemTime, walltime: Duration) -> SystemTime {
        match self {
            Self::Staged => now + walltime + CREDENTIAL_SLACK,
            Self::Session { bearer_ms: None } => now + walltime,
            Self::Session {
                bearer_ms: Some(bearer),
            } => (now + walltime).min(from_ms(bearer)),
        }
    }
}

#[derive(Debug, PartialEq)]
enum MintStep {
    Group,
    Lookup {
        key_id: String,
        access_key: String,
        encryption_key: CredentialEncryptionKey,
    },
    Create,
}

/// Mints the run's path-restricted `UserAccess`, issued by the serving node so the container's
/// SigV4 requests authorize locally; a credential already minted for the run is read back.
#[derive(Debug, PartialEq)]
pub struct MintCredentialOperation {
    run: RunIdentity,
    restrictions: Vec<PathRestriction>,
    walltime: Duration,
    life: CredentialLife,
    has_intent: bool,
    check_group: bool,
    encryption_key: Option<CredentialEncryptionKey>,
    now: SystemTime,
    step: MintStep,
}

impl MintCredentialOperation {
    fn new(
        (spec, record): (&ExecutionSpec, &JobRecord),
        run: RunIdentity,
        restrictions: Vec<PathRestriction>,
        check_group: bool,
        (encryption_key, now): (Option<CredentialEncryptionKey>, SystemTime),
    ) -> Self {
        let walltime = spec
            .resources
            .max_walltime_ms
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_WALLTIME);
        Self {
            run,
            restrictions,
            walltime,
            life: CredentialLife::of(spec),
            has_intent: record.attempt_intent.is_some(),
            check_group,
            encryption_key,
            now,
            step: MintStep::Group,
        }
    }

    /// The credential confined to the workspace bucket, with read access to the run's input
    /// view when a session also mounts inputs. WRITE on the bucket and its subtree also
    /// satisfies READ without matching siblings.
    pub fn workspace(
        job: (&ExecutionSpec, &JobRecord),
        run: RunIdentity,
        (bucket, view): (&str, Option<&str>),
        secret: (Option<CredentialEncryptionKey>, SystemTime),
    ) -> Self {
        let bucket_path = run.bucket_path(bucket);
        let mut restrictions = vec![
            PathRestriction {
                pattern: bucket_path.clone(),
                permission: Permission::WRITE,
            },
            PathRestriction {
                pattern: format!("{bucket_path}/**"),
                permission: Permission::WRITE,
            },
        ];
        if let Some(view) = view {
            restrictions.extend(read_restrictions(&run, view));
        }
        Self::new(job, run, restrictions, true, secret)
    }

    /// One read-only credential restricted to the input buckets the job mounts.
    pub fn inputs(
        job: (&ExecutionSpec, &JobRecord),
        run: RunIdentity,
        buckets: &BTreeSet<String>,
        secret: (Option<CredentialEncryptionKey>, SystemTime),
    ) -> Self {
        let restrictions = buckets
            .iter()
            .flat_map(|bucket| read_restrictions(&run, bucket))
            .collect();
        Self::new(job, run, restrictions, false, secret)
    }

    /// Refuses an oversized mount set permanently before any attempt intent, so a job cannot
    /// loop retrying a credential the evaluator would reject, then looks the credential up.
    fn lookup(&mut self) -> Result<Next<WorkspaceCredential>, JobError> {
        validate_restriction_limits(&self.restrictions).map_err(|error| {
            JobError::permanent(format!(
                "workspace credential restrictions invalid: {error}"
            ))
        })?;
        let key_id = workspace_credential_id(self.run.job_id);
        let access_key = UserAccess::build_access_key(&key_id).map_err(|error| {
            JobError::permanent(format!("workspace credential key failed: {error}"))
        })?;
        // Issued and consumed on this node, so this node's issuer-local key seals the secret.
        let encryption_key = self
            .encryption_key
            .take()
            .ok_or_else(|| JobError::permanent("workspace credential needs a net handle"))?;
        let read = GetAccessOperation::new(access_key.clone());
        self.step = MintStep::Lookup {
            key_id,
            access_key,
            encryption_key,
        };
        Ok(Next::Call(IoCall::child(read)))
    }

    /// The stored credential when it still serves this run; `None` when a new one is due.
    fn reuse(
        &self,
        access: UserAccess,
        access_key: &str,
        encryption_key: &CredentialEncryptionKey,
    ) -> Result<Option<WorkspaceCredential>, JobError> {
        let matches_job = access.access_key == access_key
            && access.user_identity == self.run.user_id
            && access.group_id == self.run.group_id
            && access.issued_by == *self.run.node_id.as_bytes()
            && access.path_restrictions.as_ref() == Some(&self.restrictions);
        if !matches_job || access.is_revoked() {
            return Err(JobError::permanent("workspace credential is invalid"));
        }
        if !access.is_expired(self.now) {
            let secret = access.open_secret(encryption_key).map_err(|error| {
                JobError::permanent(format!("workspace credential decryption failed: {error}"))
            })?;
            let expires_at_ms = expiry_ms(access.expiry);
            return Ok(Some(WorkspaceCredential {
                access_key: access.access_key,
                secret,
                expires_at_ms,
            }));
        }
        match self.has_intent {
            true => Err(JobError::permanent("workspace credential expired")),
            false => Ok(None),
        }
    }

    fn create(
        &mut self,
        key_id: String,
        encryption_key: CredentialEncryptionKey,
    ) -> Next<WorkspaceCredential> {
        let create = CreateUserOperation::new_with_key(
            CreateUserConfig {
                user_identity: self.run.user_id,
                group_id: self.run.group_id,
                expiry: self.life.expiry(self.now, self.walltime),
                path_restrictions: Some(std::mem::take(&mut self.restrictions)),
                issued_by: *self.run.node_id.as_bytes(),
            },
            key_id,
            encryption_key,
        );
        self.step = MintStep::Create;
        Next::Call(IoCall::child(create))
    }
}

impl Operation for MintCredentialOperation {
    type Output = WorkspaceCredential;
    type Error = JobError;

    fn first(&mut self) -> Result<Next<WorkspaceCredential>, JobError> {
        match self.check_group {
            true => Ok(Next::Call(IoCall::child(self.run.group_check()))),
            false => self.lookup(),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<WorkspaceCredential>, JobError> {
        match std::mem::replace(&mut self.step, MintStep::Create) {
            MintStep::Group => {
                group_verdict(result)?;
                self.lookup()
            }
            MintStep::Lookup {
                key_id,
                access_key,
                encryption_key,
            } => match result.child::<UserAccess, GetAccessError>()? {
                Ok(access) => match self.reuse(access, &access_key, &encryption_key)? {
                    Some(credential) => Ok(Next::Done(credential)),
                    None => Ok(self.create(key_id, encryption_key)),
                },
                Err(GetAccessError::NotFound) => Ok(self.create(key_id, encryption_key)),
                Err(error) => Err(JobError::retryable(format!(
                    "workspace credential lookup failed: {error}"
                ))),
            },
            MintStep::Create => {
                let (_, secret, access) = result
                    .child::<(String, Secret, UserAccess), CreateUserError>()?
                    .map_err(|error| {
                        JobError::retryable(format!("workspace credential mint failed: {error}"))
                    })?;
                Ok(Next::Done(WorkspaceCredential {
                    access_key: access.access_key,
                    secret: secret.expose().to_string(),
                    expires_at_ms: expiry_ms(access.expiry),
                }))
            }
        }
    }
}

/// READ on one bucket and everything below it.
fn read_restrictions(run: &RunIdentity, bucket: &str) -> [PathRestriction; 2] {
    let path = run.bucket_path(bucket);
    [
        PathRestriction {
            pattern: path.clone(),
            permission: Permission::READ,
        },
        PathRestriction {
            pattern: format!("{path}/**"),
            permission: Permission::READ,
        },
    ]
}

/// Wall-clock milliseconds of a stored expiry.
fn expiry_ms(expiry: SystemTime) -> u64 {
    expiry
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|since| since.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or_default()
}

fn from_ms(millis: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_millis(millis)
}

#[cfg(test)]
#[path = "credential_tests.rs"]
mod tests;
