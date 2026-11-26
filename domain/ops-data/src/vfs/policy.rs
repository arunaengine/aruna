//! Admits one file server action like the S3 request it stands for: a live local credential,
//! its owner, a local ordinary bucket, the write guards, then RBAC and CEL on the exact path.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::VfsError;
use super::path::export_holds;
use crate::s3::access::admit::{
    AdmitOwnerConfig, AdmitOwnerError, AdmitOwnerOperation, PathError, PathRequest,
    build_authorization_path, check_credential_usable, is_local_access,
};
use crate::s3::access::get::{GetAccessError, GetAccessOperation};
use crate::s3::bucket::write_guard::{BucketGuardError, BucketGuardOperation};
use aruna_core::bucket_read::{GetBucketError, GetBucketOperation};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{BUCKET_CREATION_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::request_policy::{PolicyRequestExtras, policy_request_with};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess, incarnation};
use aruna_core::structs::storage::encryption::{BucketEncryption, EncryptionMode};
use aruna_core::structs::storage::vfs::NfsMountTarget;
use aruna_core::types::Key;
use aruna_core::user::id::UserId;
use aruna_core::vfs_effects::VfsProtocol;
use aruna_ops_identity::auth::enforcement::EnforcePolicyOperation;
use aruna_ops_identity::auth::error::{AuthorizeError, PolicyEnforcementError};
use aruna_ops_identity::auth::permission_rules::PermissionRules;
use aruna_ops_identity::auth::request_authorization::{AuthorizeOperation, rules_operation};
use aruna_ops_identity::auth::scope::SubpathScope;
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};
use ulid::Ulid;

/// The S3 action a file server action is checked as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VfsAccess {
    /// Traverse, stat and access queries.
    Head,
    /// Directory enumeration.
    List,
    /// Reading bytes or copying base data.
    Get,
    /// Create, write, resize, flush, publish and parent markers.
    Put,
    /// Remove, and the source of a rename.
    Delete,
    /// The destination of a rename.
    Copy,
}

impl VfsAccess {
    /// The S3 operation name the permission path and public reads are decided on.
    pub fn operation(self) -> &'static str {
        match self {
            Self::Head => "HeadObject",
            Self::List => "ListObjectsV2",
            Self::Get => "GetObject",
            Self::Put => "PutObject",
            Self::Delete => "DeleteObject",
            Self::Copy => "CopyObject",
        }
    }

    pub fn writes(self) -> bool {
        matches!(self, Self::Put | Self::Delete | Self::Copy)
    }

    fn permission(self) -> Permission {
        match self.writes() {
            true => Permission::WRITE,
            false => Permission::READ,
        }
    }
}

/// The policy inputs of a file server action: the exact S3 operation, its query parameters and
/// only headers whose value the action really has. No body is ever invented.
pub fn vfs_policy_extras(
    access: VfsAccess,
    prefix: Option<&str>,
    version_id: Option<Ulid>,
    length: Option<u64>,
) -> PolicyRequestExtras {
    let mut params = BTreeMap::new();
    if let Some(prefix) = prefix.filter(|_| access == VfsAccess::List) {
        params.insert("prefix".to_string(), prefix.to_string());
        params.insert("list-type".to_string(), "2".to_string());
    }
    if let Some(version_id) = version_id {
        params.insert("versionId".to_string(), version_id.to_string());
    }
    let mut headers = BTreeMap::new();
    if let Some(length) = length.filter(|_| access == VfsAccess::Put) {
        headers.insert("content-length".to_string(), length.to_string());
    }
    PolicyRequestExtras {
        operation: format!("s3.{}", access.operation()),
        params,
        headers,
        body: None,
    }
}

/// Who asks, through which export, at which time.
#[derive(Clone, Debug, PartialEq)]
pub struct VfsAccessContext {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    /// The live `UserAccess` whose grants admit the request; never its secret.
    pub access_key: String,
    pub protocol: VfsProtocol,
    /// The NFS binding target or SMB share the request came through.
    pub target: NfsMountTarget,
    pub read_only: bool,
    pub now_ms: u64,
}

impl VfsAccessContext {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(self.now_ms)
    }

    /// The bucket and incarnation of a bucket or folder export.
    pub fn bucket_scope(&self) -> Option<(&str, u128)> {
        match &self.target {
            NfsMountTarget::Bucket {
                bucket,
                bucket_incarnation,
            }
            | NfsMountTarget::Folder {
                bucket,
                bucket_incarnation,
                ..
            } => Some((bucket.as_str(), *bucket_incarnation)),
            NfsMountTarget::JobInputView { .. } => None,
        }
    }
}

/// One action on one exact resolved path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsRequest {
    pub access: VfsAccess,
    pub bucket: String,
    /// The exact object or marker key; `None` checks the bucket.
    pub key: Option<String>,
    pub version_id: Option<Ulid>,
    /// The resulting logical length of a write.
    pub length: Option<u64>,
    /// The listed directory prefix of an enumeration.
    pub prefix: Option<String>,
}

impl VfsRequest {
    pub fn new(access: VfsAccess, bucket: &str, key: Option<&str>) -> Self {
        Self {
            access,
            bucket: bucket.to_string(),
            key: key.map(str::to_string),
            version_id: None,
            length: None,
            prefix: None,
        }
    }
}

/// What an admitted action may use: the caller as handlers see it, the live bucket and its
/// settings, and the subpaths a partly admitted listing is narrowed to.
#[derive(Clone, Debug, PartialEq)]
pub struct VfsGrant {
    pub access: UserAccess,
    pub owner: UserId,
    pub bucket: BucketInfo,
    pub incarnation: u128,
    pub encryption: BucketEncryption,
    pub path: String,
    pub context: AuthContext,
    pub scope: Option<SubpathScope>,
}

impl VfsGrant {
    /// Whether a listed key shows to the caller.
    pub fn shows(&self, key: &str) -> bool {
        self.scope
            .as_ref()
            .is_none_or(|scope| scope.allows_key(key))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AuthStep {
    Access,
    Owner,
    Bucket,
    Rows,
    Guard,
    Authorize,
    Rules,
    Enforce,
}

type AuthTurn = Result<Next<VfsGrant>, VfsError>;

/// Admits one action again from live state: an open handle, a cached decision, a mount or a
/// filename never carries authority. Runs without a transaction and opens none.
#[derive(Debug, PartialEq)]
pub struct AuthorizeVfsOperation {
    context: VfsAccessContext,
    request: VfsRequest,
    step: AuthStep,
    access: Option<UserAccess>,
    bucket: Option<BucketInfo>,
    incarnation: u128,
    encryption: BucketEncryption,
    built: Option<(String, AuthContext, UserAccess)>,
    scope: Option<SubpathScope>,
}

impl AuthorizeVfsOperation {
    pub fn new(context: VfsAccessContext, request: VfsRequest) -> Self {
        Self {
            context,
            request,
            step: AuthStep::Access,
            access: None,
            bucket: None,
            incarnation: 0,
            encryption: BucketEncryption::default(),
            built: None,
            scope: None,
        }
    }

    fn call(&mut self, step: AuthStep, call: IoCall) -> AuthTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn extras(&self) -> PolicyRequestExtras {
        let request = &self.request;
        let prefix = request.prefix.as_deref();
        vfs_policy_extras(request.access, prefix, request.version_id, request.length)
    }

    /// The export confines every key it serves by whole components.
    fn confined(&self) -> Result<(), VfsError> {
        let Some((bucket, _)) = self.context.bucket_scope() else {
            return Err(VfsError::Denied);
        };
        if bucket != self.request.bucket {
            return Err(VfsError::CrossExport);
        }
        let key = self.request.key.as_deref();
        let key = key.or(self.request.prefix.as_deref()).unwrap_or_default();
        let root = matches!(self.context.target, NfsMountTarget::Bucket { .. });
        match root || export_holds(&self.context.target, bucket, key) {
            true => Ok(()),
            false => Err(VfsError::CrossExport),
        }
    }

    fn access_read(&mut self, result: IoResult) -> AuthTurn {
        let access = match result.child::<UserAccess, GetAccessError>()? {
            Ok(access) => access,
            Err(GetAccessError::NotFound) => return Err(VfsError::Denied),
            Err(error) => return Err(VfsError::Storage(error.to_string())),
        };
        let usable = check_credential_usable(&access, self.context.now()).is_ok();
        if !is_local_access(&access) || !usable {
            return Err(VfsError::Denied);
        }
        let admit = AdmitOwnerOperation::new(AdmitOwnerConfig {
            realm_id: self.context.realm_id,
            access_key: access.access_key.clone(),
            user_id: access.user_identity,
            issued_by: access.issued_by,
            revoke: true,
        });
        self.access = Some(access);
        self.call(AuthStep::Owner, IoCall::child(admit))
    }

    fn owner_admitted(&mut self, result: IoResult) -> AuthTurn {
        match result.child::<(), AdmitOwnerError>()? {
            Ok(()) => {}
            Err(
                AdmitOwnerError::IssuerUnknown
                | AdmitOwnerError::Revoked
                | AdmitOwnerError::Deactivated,
            ) => return Err(VfsError::Denied),
            Err(error) => return Err(VfsError::Unavailable(error.to_string())),
        }
        let read = GetBucketOperation::new(self.request.bucket.clone());
        self.call(AuthStep::Bucket, IoCall::child(read))
    }

    fn bucket_read(&mut self, result: IoResult) -> AuthTurn {
        let bucket = match result.child::<BucketInfo, GetBucketError>()? {
            Ok(bucket) => bucket,
            Err(GetBucketError::NotFound) => return Err(VfsError::StaleHandle),
            Err(error) => return Err(VfsError::Storage(error.to_string())),
        };
        if bucket.distribution.is_some() {
            return Err(VfsError::Distributed);
        }
        self.bucket = Some(bucket);
        let key = || Key::from(self.request.bucket.as_bytes().to_vec());
        let reads = vec![
            (BUCKET_CREATION_KEYSPACE.to_string(), key()),
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), key()),
        ];
        self.call(AuthStep::Rows, IoCall::Read(reads))
    }

    /// The bucket must still be the incarnation the export bound, and writes must be allowed.
    fn rows_read(&mut self, result: IoResult) -> AuthTurn {
        let [creation, settings] = aruna_core::rows::values::<2>(result)?;
        self.incarnation = incarnation(creation.as_deref());
        let bound = self.context.bucket_scope().map(|(_, bound)| bound);
        if bound != Some(self.incarnation) {
            return Err(VfsError::StaleHandle);
        }
        self.encryption = BucketEncryption::from_row(settings.as_deref())?;
        if !self.request.access.writes() {
            return self.authorize();
        }
        if self.context.read_only {
            return Err(VfsError::ReadOnly);
        }
        if self.encryption.mode == EncryptionMode::VaultLocked {
            return Err(VfsError::VaultLocked);
        }
        let guard = BucketGuardOperation::new(self.request.bucket.as_str());
        self.call(AuthStep::Guard, IoCall::child(guard))
    }

    fn guard_read(&mut self, result: IoResult) -> AuthTurn {
        match result.child::<(), BucketGuardError>()? {
            Ok(()) => self.authorize(),
            Err(BucketGuardError::ReadOnly(_)) => Err(VfsError::ReadOnly),
            Err(error) => Err(VfsError::Storage(error.to_string())),
        }
    }

    fn authorize(&mut self) -> AuthTurn {
        let access = self.access.as_ref().ok_or(VfsError::Denied)?;
        let built = build_authorization_path(PathRequest {
            realm_id: self.context.realm_id,
            node_id: self.context.node_id,
            access,
            bucket: Some(&self.request.bucket),
            bucket_info: self.bucket.as_ref(),
            key: self.request.key.as_deref(),
            operation: self.request.access.operation(),
        });
        let built = match built {
            Ok(built) => built,
            Err(PathError::OtherGroup) => return Err(VfsError::Denied),
            Err(PathError::NoSuchBucket) => return Err(VfsError::StaleHandle),
        };
        let permission = self.request.access.permission();
        let check = AuthorizeOperation::new(
            self.context.realm_id,
            &built.context,
            &built.path,
            &permission,
            self.extras(),
        );
        self.built = Some((built.path, built.context, built.access));
        self.call(AuthStep::Authorize, IoCall::child(check))
    }

    fn authorized(&mut self, result: IoResult) -> AuthTurn {
        match result.child::<(), AuthorizeError>()? {
            Ok(()) => self.granted(),
            Err(AuthorizeError::PermissionDenied) if self.request.access == VfsAccess::List => {
                let (path, context, _) = self.built.as_ref().ok_or(VfsError::Denied)?;
                let rules = rules_operation(context, path);
                self.call(AuthStep::Rules, IoCall::child(rules))
            }
            Err(error) => Err(authorize_error(error)),
        }
    }

    /// A listing the caller reaches only in part is narrowed to the subpaths it may read; the
    /// policies still run on the listed path.
    fn rules_read(&mut self, result: IoResult) -> AuthTurn {
        let rules = result.child::<PermissionRules, AuthorizationError>()?;
        let (path, context, access) = self.built.as_ref().ok_or(VfsError::Denied)?;
        let restrictions = access.path_restrictions.as_deref();
        let scope = SubpathScope::resolve(rules, restrictions, path)
            .map_err(|error| VfsError::Storage(error.to_string()))?;
        let prefix = self.request.prefix.as_deref().unwrap_or_default();
        if scope.is_empty() || !scope.allows_prefix(prefix) {
            return Err(VfsError::Denied);
        }
        let request = policy_request_with(path, &Permission::READ, Some(context), self.extras());
        let enforce = EnforcePolicyOperation::new(self.context.realm_id, request);
        self.scope = Some(scope);
        self.call(AuthStep::Enforce, IoCall::child(enforce))
    }

    fn enforced(&mut self, result: IoResult) -> AuthTurn {
        match result.child::<(), PolicyEnforcementError>()? {
            Ok(()) => self.granted(),
            Err(error) => Err(authorize_error(AuthorizeError::Policy(error))),
        }
    }

    fn granted(&mut self) -> AuthTurn {
        let (path, context, access) = self.built.take().ok_or(VfsError::Denied)?;
        let owner = self.access.as_ref().map(|access| access.user_identity);
        Ok(Next::Done(VfsGrant {
            access,
            owner: owner.ok_or(VfsError::Denied)?,
            bucket: self.bucket.take().ok_or(VfsError::StaleHandle)?,
            incarnation: self.incarnation,
            encryption: std::mem::take(&mut self.encryption),
            path,
            context,
            scope: self.scope.take(),
        }))
    }
}

/// RBAC and policy denials stay indistinguishable; control-plane faults stay faults.
fn authorize_error(error: AuthorizeError) -> VfsError {
    match error {
        AuthorizeError::Storage(error) => VfsError::Storage(error.to_string()),
        AuthorizeError::CheckFailed(reason) => VfsError::Unavailable(reason),
        _ => VfsError::Denied,
    }
}

impl Operation for AuthorizeVfsOperation {
    type Output = VfsGrant;
    type Error = VfsError;

    fn first(&mut self) -> AuthTurn {
        self.confined()?;
        let read = GetAccessOperation::new(self.context.access_key.clone());
        self.call(AuthStep::Access, IoCall::child(read))
    }

    fn next(&mut self, result: IoResult) -> AuthTurn {
        match self.step {
            AuthStep::Access => self.access_read(result),
            AuthStep::Owner => self.owner_admitted(result),
            AuthStep::Bucket => self.bucket_read(result),
            AuthStep::Rows => self.rows_read(result),
            AuthStep::Guard => self.guard_read(result),
            AuthStep::Authorize => self.authorized(result),
            AuthStep::Rules => self.rules_read(result),
            AuthStep::Enforce => self.enforced(result),
        }
    }

    fn expected_error(error: &VfsError) -> bool {
        matches!(
            error,
            VfsError::Denied | VfsError::ReadOnly | VfsError::CrossExport | VfsError::StaleHandle
        )
    }
}

/// The result of an [`AuthorizeVfsOperation`] child.
pub(crate) fn grant_of(result: IoResult) -> Result<VfsGrant, VfsError> {
    result.child::<VfsGrant, VfsError>()?
}

/// The child call that admits `request` for `context`.
pub(crate) fn authorize_call(context: &VfsAccessContext, request: VfsRequest) -> IoCall {
    IoCall::child(AuthorizeVfsOperation::new(context.clone(), request))
}
