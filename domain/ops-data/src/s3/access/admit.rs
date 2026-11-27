//! Admits a stored S3 credential and its owner, and builds the permission path of a request.
//! S3, SMB and NFS share these rules, so every frontend refuses the same credentials.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::revoke::{RevokeUserError, RevokeUserOperation};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::credential_encryption::EncryptedS3Secret;
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::identity::s3_session::{S3Session, SESSION_ACCESS_PREFIX};
use aruna_core::structs::storage::blob::{
    BucketInfo, UserAccess, bucket_permission_path, group_permission_path, object_permission_path,
};
use aruna_core::user::id::UserId;
use aruna_ops_identity::auth::bearer_token::{AccountDeactivatedOperation, UserCutoffOperation};
use aruna_ops_identity::auth::error::ArunaBearerError;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::time::SystemTime;
use thiserror::Error;
use tracing::warn;

/// The access an S3 operation needs on its permission path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Action {
    Read,
    Write,
}

impl Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Action::Read => write!(f, "read"),
            Action::Write => write!(f, "write"),
        }
    }
}

/// Why a looked-up credential can no longer be used.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum AdmissionFailure {
    #[error("Credential has been revoked")]
    Revoked,
    #[error("Credential has expired")]
    Expired,
}

/// Refuses a revoked or expired credential. Frontends call it right after the lookup, so an
/// unusable credential never spends its owner's request budget.
pub fn check_credential_usable(
    access: &UserAccess,
    now: SystemTime,
) -> Result<(), AdmissionFailure> {
    if access.is_revoked() {
        return Err(AdmissionFailure::Revoked);
    }
    if access.is_expired(now) {
        return Err(AdmissionFailure::Expired);
    }
    Ok(())
}

/// Whether `access` is a stored, signing S3 credential of a real user. A temporary
/// `S3Session`, a share link's delegated id, a forwarded or anonymous principal and any record
/// without a sealed secret are refused. A request token never becomes a `UserAccess`.
pub fn is_local_access(access: &UserAccess) -> bool {
    UserAccess::build_access_key(&access.access_key).is_ok()
        && !S3Session::is_session_key(&access.access_key)
        && access.secret != EncryptedS3Secret::empty()
        && !access.user_identity.is_nil()
}

/// The Unix second a credential's ULID key id was minted, or 0 for a key id without one.
fn issued_secs(access_key: &str) -> u64 {
    let key_id = access_key
        .strip_prefix(SESSION_ACCESS_PREFIX)
        .unwrap_or(access_key);
    ulid::Ulid::from_string(key_id).map_or(0, |id| id.timestamp_ms() / 1000)
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdmitOwnerConfig {
    pub realm_id: RealmId,
    pub access_key: String,
    pub user_id: UserId,
    /// The node whose local decryption proved the credential.
    pub issued_by: [u8; 32],
    /// Deletes a stored credential whose owner is refused; a session is never deleted here.
    pub revoke: bool,
}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum AdmitOwnerError {
    #[error("Credential issuer not in realm")]
    IssuerUnknown,
    #[error("Failed to load realm config")]
    ConfigUnavailable,
    #[error("Malformed realm node id")]
    MalformedNode,
    #[error("Credential has been revoked")]
    Revoked,
    #[error("Account is deactivated")]
    Deactivated,
    #[error("Revocation state is unavailable")]
    RevocationUnavailable,
    #[error("Account state is unavailable")]
    AccountUnavailable,
    #[error("owner admission failed: {0}")]
    Io(IoError),
}

impl From<IoError> for AdmitOwnerError {
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug, PartialEq)]
enum Stage {
    Issuer,
    Cutoff,
    Status,
    Revoke(AdmitOwnerError),
}

type AdmitTurn = Result<Next<()>, AdmitOwnerError>;

/// Admits the owner of a credential: its issuer must still be a realm node, the credential must
/// predate no cutoff of its owner, and the owner must not be deactivated. A refused stored
/// credential is deleted at once, so it stays refused after the cutoff ages out.
#[derive(Debug, PartialEq)]
pub struct AdmitOwnerOperation {
    config: AdmitOwnerConfig,
    stage: Stage,
}

impl AdmitOwnerOperation {
    pub fn new(config: AdmitOwnerConfig) -> Self {
        Self {
            config,
            stage: Stage::Issuer,
        }
    }

    fn issuer_read(&mut self, result: IoResult) -> AdmitTurn {
        let config = (result.child::<RealmConfigDocument, GetConfigError>()?)
            .map_err(|_| AdmitOwnerError::ConfigUnavailable)?;
        let node_ids = config
            .node_ids()
            .map_err(|_| AdmitOwnerError::MalformedNode)?;
        if !node_ids
            .iter()
            .any(|node_id| *node_id.as_bytes() == self.config.issued_by)
        {
            return Err(AdmitOwnerError::IssuerUnknown);
        }
        self.stage = Stage::Cutoff;
        let cutoff = UserCutoffOperation::new(self.config.realm_id, &self.config.user_id);
        Ok(Next::Call(IoCall::child(cutoff)))
    }

    fn cutoff_read(&mut self, result: IoResult) -> AdmitTurn {
        let cutoff = (result.child::<Option<u64>, ArunaBearerError>()?)
            .map_err(|_| AdmitOwnerError::RevocationUnavailable)?;
        if cutoff.is_some_and(|cutoff| issued_secs(&self.config.access_key) < cutoff) {
            return self.refuse(AdmitOwnerError::Revoked);
        }
        self.stage = Stage::Status;
        let status = AccountDeactivatedOperation::new(&self.config.user_id);
        Ok(Next::Call(IoCall::child(status)))
    }

    fn status_read(&mut self, result: IoResult) -> AdmitTurn {
        let deactivated = (result.child::<bool, ArunaBearerError>()?)
            .map_err(|_| AdmitOwnerError::AccountUnavailable)?;
        match deactivated {
            true => self.refuse(AdmitOwnerError::Deactivated),
            false => Ok(Next::Done(())),
        }
    }

    fn refuse(&mut self, denial: AdmitOwnerError) -> AdmitTurn {
        if !self.config.revoke {
            return Err(denial);
        }
        self.stage = Stage::Revoke(denial);
        let revoke = RevokeUserOperation::new(self.config.access_key.clone());
        Ok(Next::Call(IoCall::child(revoke)))
    }

    /// A failed deletion keeps the denial; the next request refuses the credential again.
    fn revoked(denial: AdmitOwnerError, result: IoResult) -> AdmitTurn {
        match result.child::<UserAccess, RevokeUserError>()? {
            Ok(_) | Err(RevokeUserError::NotFound) => {}
            Err(error) => warn!(error = %error, "Failed to delete a refused credential"),
        }
        Err(denial)
    }
}

impl Operation for AdmitOwnerOperation {
    type Output = ();
    type Error = AdmitOwnerError;

    fn first(&mut self) -> AdmitTurn {
        let read = GetConfigOperation::new(self.config.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> AdmitTurn {
        match &self.stage {
            Stage::Issuer => self.issuer_read(result),
            Stage::Cutoff => self.cutoff_read(result),
            Stage::Status => self.status_read(result),
            Stage::Revoke(denial) => Self::revoked(denial.clone(), result),
        }
    }

    fn expected_error(error: &AdmitOwnerError) -> bool {
        matches!(
            error,
            AdmitOwnerError::IssuerUnknown
                | AdmitOwnerError::Revoked
                | AdmitOwnerError::Deactivated
        )
    }
}

/// Why a request has no permission path.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum PathError {
    #[error("Bucket belongs to a different group")]
    OtherGroup,
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
}

/// One request's addressed bucket and key on this node, with the caller's credential.
#[derive(Clone, Copy, Debug)]
pub struct PathRequest<'a> {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub access: &'a UserAccess,
    pub bucket: Option<&'a str>,
    /// The stored bucket, looked up by the frontend; `None` when it does not exist.
    pub bucket_info: Option<&'a BucketInfo>,
    pub key: Option<&'a str>,
    /// The S3 operation name, such as `GetObject`.
    pub operation: &'a str,
}

/// The permission path a request is checked on and the principal checked there.
#[derive(Clone, Debug, PartialEq)]
pub struct AuthorizationPath {
    pub path: String,
    pub context: AuthContext,
    /// The caller as handlers see it: Everyone in the bucket's group after a group swap.
    pub access: UserAccess,
    /// The caller reads another group's bucket as Everyone in that group.
    pub swapped: bool,
}

/// Builds the permission path of a request. Without a bucket it is the credential group's data
/// path. Another group's bucket admits only the public reads, checked as Everyone in that group,
/// so a part forwarded to another node asks as Everyone too.
pub fn build_authorization_path(request: PathRequest<'_>) -> Result<AuthorizationPath, PathError> {
    let PathRequest {
        realm_id,
        node_id,
        access,
        bucket,
        bucket_info,
        key,
        operation,
    } = request;
    let mut access = access.clone();
    let mut context = AuthContext {
        user_id: access.user_identity,
        realm_id,
        path_restrictions: access.path_restrictions.clone(),
        session: None,
    };
    let Some(bucket) = bucket else {
        let path = group_permission_path(realm_id, access.group_id, node_id);
        return Ok(AuthorizationPath {
            path,
            context,
            access,
            swapped: false,
        });
    };
    let mut swapped = false;
    let group_id = match bucket_info {
        Some(info) if info.group_id != access.group_id => {
            let read = operation_permission(operation) == Some(Action::Read);
            if !read || !public_read_allowed(operation, key.is_some()) {
                return Err(PathError::OtherGroup);
            }
            context = AuthContext::anonymous(realm_id);
            access.user_identity = UserId::nil(realm_id);
            access.group_id = info.group_id;
            access.path_restrictions = None;
            swapped = true;
            info.group_id
        }
        Some(info) => info.group_id,
        None if operation == "CreateBucket" && key.is_none() => access.group_id,
        None => return Err(PathError::NoSuchBucket),
    };
    let path = match key {
        Some(key) => object_permission_path(realm_id, group_id, node_id, bucket, key),
        None => bucket_permission_path(realm_id, group_id, node_id, bucket),
    };
    Ok(AuthorizationPath {
        path,
        context,
        access,
        swapped,
    })
}

/// Reads a signed caller may make on another group's bucket, as Everyone.
pub fn public_read_allowed(operation_name: &str, has_key: bool) -> bool {
    match has_key {
        true => matches!(operation_name, "GetObject" | "HeadObject"),
        false => matches!(
            operation_name,
            "HeadBucket" | "GetBucketLocation" | "ListObjects" | "ListObjectsV2"
        ),
    }
}

pub fn operation_permission(operation_name: &str) -> Option<Action> {
    match operation_name {
        // Write operations (operations that modify state/data)
        "AbortMultipartUpload" => Some(Action::Write),
        "CompleteMultipartUpload" => Some(Action::Write),
        "CopyObject" => Some(Action::Write),
        "CreateBucket" => Some(Action::Write),
        "CreateBucketMetadataTableConfiguration" => Some(Action::Write),
        "CreateMultipartUpload" => Some(Action::Write),
        "DeleteBucket" => Some(Action::Write),
        "DeleteBucketAnalyticsConfiguration" => Some(Action::Write),
        "DeleteBucketCors" => Some(Action::Write),
        "DeleteBucketEncryption" => Some(Action::Write),
        "DeleteBucketIntelligentTieringConfiguration" => Some(Action::Write),
        "DeleteBucketInventoryConfiguration" => Some(Action::Write),
        "DeleteBucketLifecycle" => Some(Action::Write),
        "DeleteBucketMetadataTableConfiguration" => Some(Action::Write),
        "DeleteBucketMetricsConfiguration" => Some(Action::Write),
        "DeleteBucketOwnershipControls" => Some(Action::Write),
        "DeleteBucketPolicy" => Some(Action::Write),
        "DeleteBucketReplication" => Some(Action::Write),
        "DeleteBucketTagging" => Some(Action::Write),
        "DeleteBucketWebsite" => Some(Action::Write),
        "DeleteObject" => Some(Action::Write),
        "DeleteObjectTagging" => Some(Action::Write),
        "DeleteObjects" => Some(Action::Write),
        "DeletePublicAccessBlock" => Some(Action::Write),
        "PutBucketAccelerateConfiguration" => Some(Action::Write),
        "PutBucketAcl" => Some(Action::Write),
        "PutBucketAnalyticsConfiguration" => Some(Action::Write),
        "PutBucketCors" => Some(Action::Write),
        "PutBucketEncryption" => Some(Action::Write),
        "PutBucketIntelligentTieringConfiguration" => Some(Action::Write),
        "PutBucketInventoryConfiguration" => Some(Action::Write),
        "PutBucketLifecycleConfiguration" => Some(Action::Write),
        "PutBucketLogging" => Some(Action::Write),
        "PutBucketMetricsConfiguration" => Some(Action::Write),
        "PutBucketNotificationConfiguration" => Some(Action::Write),
        "PutBucketOwnershipControls" => Some(Action::Write),
        "PutBucketPolicy" => Some(Action::Write),
        "PutBucketReplication" => Some(Action::Write),
        "PutBucketRequestPayment" => Some(Action::Write),
        "PutBucketTagging" => Some(Action::Write),
        "PutBucketVersioning" => Some(Action::Write),
        "PutBucketWebsite" => Some(Action::Write),
        "PutObject" => Some(Action::Write),
        "PutObjectAcl" => Some(Action::Write),
        "PutObjectLegalHold" => Some(Action::Write),
        "PutObjectLockConfiguration" => Some(Action::Write),
        "PutObjectRetention" => Some(Action::Write),
        "PutObjectTagging" => Some(Action::Write),
        "PutPublicAccessBlock" => Some(Action::Write),
        "RestoreObject" => Some(Action::Write),
        "UploadPart" => Some(Action::Write),
        "UploadPartCopy" => Some(Action::Write),
        "WriteGetObjectResponse" => Some(Action::Write),

        // Read operations (operations that only retrieve/read data)
        "GetBucketAccelerateConfiguration" => Some(Action::Read),
        "GetBucketAcl" => Some(Action::Read),
        "GetBucketAnalyticsConfiguration" => Some(Action::Read),
        "GetBucketCors" => Some(Action::Read),
        "GetBucketEncryption" => Some(Action::Read),
        "GetBucketIntelligentTieringConfiguration" => Some(Action::Read),
        "GetBucketInventoryConfiguration" => Some(Action::Read),
        "GetBucketLifecycleConfiguration" => Some(Action::Read),
        "GetBucketLocation" => Some(Action::Read),
        "GetBucketLogging" => Some(Action::Read),
        "GetBucketMetadataTableConfiguration" => Some(Action::Read),
        "GetBucketMetricsConfiguration" => Some(Action::Read),
        "GetBucketNotificationConfiguration" => Some(Action::Read),
        "GetBucketOwnershipControls" => Some(Action::Read),
        "GetBucketPolicy" => Some(Action::Read),
        "GetBucketPolicyStatus" => Some(Action::Read),
        "GetBucketReplication" => Some(Action::Read),
        "GetBucketRequestPayment" => Some(Action::Read),
        "GetBucketTagging" => Some(Action::Read),
        "GetBucketVersioning" => Some(Action::Read),
        "GetBucketWebsite" => Some(Action::Read),
        "GetObject" => Some(Action::Read),
        "GetObjectAcl" => Some(Action::Read),
        "GetObjectAttributes" => Some(Action::Read),
        "GetObjectLegalHold" => Some(Action::Read),
        "GetObjectLockConfiguration" => Some(Action::Read),
        "GetObjectRetention" => Some(Action::Read),
        "GetObjectTagging" => Some(Action::Read),
        "GetObjectTorrent" => Some(Action::Read),
        "GetPublicAccessBlock" => Some(Action::Read),
        "HeadBucket" => Some(Action::Read),
        "HeadObject" => Some(Action::Read),
        "ListBucketAnalyticsConfigurations" => Some(Action::Read),
        "ListBucketIntelligentTieringConfigurations" => Some(Action::Read),
        "ListBucketInventoryConfigurations" => Some(Action::Read),
        "ListBucketMetricsConfigurations" => Some(Action::Read),
        "ListBuckets" => Some(Action::Read),
        "ListMultipartUploads" => Some(Action::Read),
        "ListObjectVersions" => Some(Action::Read),
        "ListObjects" => Some(Action::Read),
        "ListObjectsV2" => Some(Action::Read),
        "ListParts" => Some(Action::Read),
        "SelectObjectContent" => Some(Action::Read),

        // Unknown operation
        _ => None,
    }
}

#[cfg(test)]
#[path = "admit_tests.rs"]
mod tests;
