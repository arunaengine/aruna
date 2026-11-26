//! Live admission of SMB credentials and shares: the exact local `UserAccess`, its issuer,
//! owner, expiry and revocation, and the local ordinary bucket a share names.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::structs::storage::blob::UserAccess;
use aruna_ops_data::s3::access::admit::{
    AdmitOwnerConfig, AdmitOwnerError, AdmitOwnerOperation, check_credential_usable,
    is_local_access,
};
use aruna_ops_data::s3::access::get::{GetAccessError, GetAccessOperation};
use aruna_ops_data::vfs::policy::{VfsAccess, VfsRequest};
use aruna_ops_data::vfs::{
    AuthorizeVfsOperation, BucketScopeOperation, VfsAccessContext, VfsError,
};
use aruna_runner::driver::drive;
use zeroize::Zeroizing;

use super::{ArunaSmbBackend, vfs_failure};
use crate::backend::{BackendError, Credential, CredentialSecret, TreeGrant};

/// The capacity a share reports; group quotas, not the volume, bound what a client may write.
const SHARE_BYTES: u64 = 1 << 50;

impl ArunaSmbBackend {
    /// The live local credential stored under exactly `access_key`; another spelling of the
    /// same key is another, absent credential.
    async fn live_access(&self, access_key: &str) -> Result<Option<UserAccess>, BackendError> {
        let read = GetAccessOperation::new(access_key.to_string());
        let access = match drive(read, &self.driver).await {
            Ok(access) => access,
            Err(GetAccessError::NotFound) => return Ok(None),
            Err(error) => return Err(BackendError::Internal(error.to_string())),
        };
        let usable = check_credential_usable(&access, SystemTime::now()).is_ok();
        let issued_here = access.issued_by == *self.node_id.as_bytes();
        if access.access_key != access_key || !is_local_access(&access) || !usable || !issued_here {
            return Ok(None);
        }
        let owner = AdmitOwnerOperation::new(AdmitOwnerConfig {
            realm_id: self.realm_id,
            access_key: access.access_key.clone(),
            user_id: access.user_identity,
            issued_by: access.issued_by,
            revoke: true,
        });
        match drive(owner, &self.driver).await {
            Ok(()) => Ok(Some(access)),
            Err(
                AdmitOwnerError::IssuerUnknown
                | AdmitOwnerError::Revoked
                | AdmitOwnerError::Deactivated,
            ) => Ok(None),
            Err(error) => Err(BackendError::Internal(error.to_string())),
        }
    }

    /// Opens the issued secret and wraps it at once, so the NTLM check drops it zeroed.
    pub(super) async fn secret_of(
        &self,
        access_key: &str,
    ) -> Result<Option<CredentialSecret>, BackendError> {
        let Some(access) = self.live_access(access_key).await? else {
            return Ok(None);
        };
        let Ok(secret) = access.open_secret(&self.credential_key) else {
            return Ok(None);
        };
        Ok(Some(CredentialSecret {
            principal: access.user_identity,
            secret: Zeroizing::new(secret),
        }))
    }

    pub(super) async fn admit_live(&self, credential: &Credential) -> Result<(), BackendError> {
        match self.live_access(&credential.access_key).await? {
            Some(access) if access.user_identity == credential.principal => Ok(()),
            _ => Err(BackendError::Revoked),
        }
    }

    /// Runs one live admission of `request`; a refusal is `false`, a fault an error.
    pub(super) async fn allows(
        &self,
        context: &VfsAccessContext,
        request: VfsRequest,
    ) -> Result<bool, BackendError> {
        let check = AuthorizeVfsOperation::new(context.clone(), request);
        match drive(check, &self.driver).await {
            Ok(_) => Ok(true),
            Err(
                VfsError::Denied
                | VfsError::ReadOnly
                | VfsError::VaultLocked
                | VfsError::CrossExport
                | VfsError::KeyRequired
                | VfsError::Quarantined,
            ) => Ok(false),
            Err(error) => Err(vfs_failure(error)),
        }
    }

    /// The current incarnation of a local ordinary bucket; a distributed one is refused.
    pub(super) async fn incarnation(
        &self,
        access_key: &str,
        bucket: &str,
    ) -> Result<u128, BackendError> {
        let known = (self.shares.lock())
            .get(&(access_key.to_string(), bucket.to_string()))
            .copied();
        if let Some(incarnation) = known {
            return Ok(incarnation);
        }
        self.bucket_scope(bucket).await
    }

    async fn bucket_scope(&self, bucket: &str) -> Result<u128, BackendError> {
        match drive(BucketScopeOperation::new(bucket), &self.driver).await {
            Ok(scope) => Ok(scope.incarnation),
            Err(VfsError::NotFound) => Err(BackendError::NotFound),
            Err(error) => Err(vfs_failure(error)),
        }
    }

    /// Admits an ordinary local bucket share: it must exist here, not be distributed, and
    /// the caller must reach some path of it. Writes stay decided per path.
    pub(super) async fn connect_bucket(
        &self,
        credential: &Credential,
        bucket: &str,
    ) -> Result<TreeGrant, BackendError> {
        let incarnation = self.bucket_scope(bucket).await?;
        self.admit_live(credential).await?;
        let context = self.context(&credential.access_key, bucket, incarnation);
        let mut list = VfsRequest::new(VfsAccess::List, bucket, None);
        list.prefix = Some(String::new());
        if !self.allows(&context, list).await? {
            return Err(BackendError::Denied);
        }
        let put = VfsRequest::new(VfsAccess::Put, bucket, None);
        let check = AuthorizeVfsOperation::new(context, put);
        let read_only = match drive(check, &self.driver).await {
            Ok(_) | Err(VfsError::Denied) => false,
            Err(VfsError::ReadOnly | VfsError::VaultLocked) => true,
            Err(error) => return Err(vfs_failure(error)),
        };
        let share = (credential.access_key.clone(), bucket.to_string());
        self.shares.lock().insert(share, incarnation);
        Ok(TreeGrant {
            read_only,
            capacity: SHARE_BYTES,
            available: SHARE_BYTES,
        })
    }
}
