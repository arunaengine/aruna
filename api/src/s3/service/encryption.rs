//! Serves the S3 bucket encryption configuration: encrypted buckets report `AES256`, and
//! `AES256` enables node-managed encryption with group admin rights.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::ArunaS3Service;
use crate::routes::storage::encryption::{EncryptionRequest, enable_bucket};
use crate::s3::auth::map_authorize_error;
use crate::s3::error::IntoS3Error;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::placement::policy::document::group_admin_path;
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::structs::storage::encryption::{BucketKeyError, EncryptionMode};
use aruna_operations::auth::request_authorization::authorize;
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::driver::drive;
use aruna_operations::s3::bucket::encryption::EnableError;
use aruna_operations::s3::bucket::get::GetBucketOperation;
use aruna_operations::s3::key_status::{KeyStatusOperation, bucket_settings};
use s3s::dto::{
    ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
    ServerSideEncryptionRule,
};
use s3s::{S3Error, S3ErrorCode, S3Result, s3_error};

impl ArunaS3Service {
    pub(super) async fn bucket_encrypted(&self, bucket: &str) -> S3Result<bool> {
        bucket_settings(&self.state, bucket)
            .await
            .map(|settings| settings.is_encrypted())
            .map_err(|error| s3_error!(InternalError, "{}", error))
    }

    pub(super) async fn encryption_config(
        &self,
        bucket: &str,
    ) -> S3Result<ServerSideEncryptionConfiguration> {
        drive(GetBucketOperation::new(bucket.to_string()), &self.state)
            .await
            .map_err(IntoS3Error::into_s3_error)?;
        match self.bucket_encrypted(bucket).await? {
            true => Ok(aes_config()),
            false => Err(config_missing()),
        }
    }

    /// Enables `node_managed` for exactly `AES256`; an encrypted bucket keeps its mode.
    pub(super) async fn enable_encryption(
        &self,
        bucket: &str,
        config: &ServerSideEncryptionConfiguration,
        user_access: &UserAccess,
        extras: PolicyRequestExtras,
    ) -> S3Result<()> {
        requested_aes(config)?;
        let info = drive(GetBucketOperation::new(bucket.to_string()), &self.state)
            .await
            .map_err(IntoS3Error::into_s3_error)?;
        let auth = AuthContext {
            user_id: user_access.user_identity,
            realm_id: user_access.user_identity.realm_id,
            path_restrictions: user_access.path_restrictions.clone(),
            session: None,
        };
        let path = group_admin_path(self.realm_id, info.group_id);
        authorize(
            &self.state,
            self.realm_id,
            &auth,
            &path,
            &Permission::WRITE,
            extras,
        )
        .await
        .map_err(map_authorize_error)?;
        let operation = KeyStatusOperation::new(bucket.to_string(), self.realm_id, info.group_id);
        let snapshot = drive(operation, &self.state)
            .await
            .map_err(|error| s3_error!(InternalError, "{}", error))?;
        if snapshot.settings.is_encrypted() {
            return Ok(());
        }
        let request = EncryptionRequest {
            mode: EncryptionMode::NodeManaged,
            cipher: None,
            block_keys: None,
            max_unlock_ms: None,
            expected_generation: snapshot.settings.storage_generation,
        };
        let target = (self.realm_id, self.node_id, info.group_id);
        enable_bucket(
            &self.state,
            target,
            auth.user_id,
            bucket,
            &snapshot,
            &request,
        )
        .await
        .map_err(enable_error)
    }
}

fn aes_config() -> ServerSideEncryptionConfiguration {
    let rule = ServerSideEncryptionRule {
        apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
            sse_algorithm: ServerSideEncryption::from_static(ServerSideEncryption::AES256),
            kms_master_key_id: None,
        }),
        ..Default::default()
    };
    ServerSideEncryptionConfiguration { rules: vec![rule] }
}

fn config_missing() -> S3Error {
    let mut error = S3Error::with_message(
        S3ErrorCode::ServerSideEncryptionConfigurationNotFoundError,
        "The server side encryption configuration was not found",
    );
    error.set_status_code(http::StatusCode::NOT_FOUND);
    error
}

/// Only one rule with `AES256` and no KMS key or bucket key setting describes Aruna encryption.
fn requested_aes(config: &ServerSideEncryptionConfiguration) -> S3Result<()> {
    let [rule] = config.rules.as_slice() else {
        return Err(s3_error!(
            MalformedXML,
            "Exactly one encryption rule is supported"
        ));
    };
    let Some(default) = rule.apply_server_side_encryption_by_default.as_ref() else {
        return Err(s3_error!(
            MalformedXML,
            "The rule names no default encryption"
        ));
    };
    let aes = default.sse_algorithm.as_str() == ServerSideEncryption::AES256;
    if !aes || default.kms_master_key_id.is_some() || rule.bucket_key_enabled == Some(true) {
        return Err(s3_error!(
            NotImplemented,
            "Only AES256 server-side encryption is supported"
        ));
    }
    Ok(())
}

fn enable_error(error: EnableError) -> S3Error {
    match error {
        EnableError::OpenUploads => s3_error!(
            OperationAborted,
            "Complete or abort the open multipart uploads before enabling encryption"
        ),
        EnableError::RecoveryUnmet => s3_error!(
            InvalidRequest,
            "The bucket key holders do not meet the recovery rule"
        ),
        EnableError::Key(BucketKeyError::StaleGeneration { .. })
        | EnableError::AlreadyEncrypted => {
            s3_error!(
                OperationAborted,
                "The bucket encryption changed during the request"
            )
        }
        EnableError::NotAdmin => s3_error!(AccessDenied, "The caller is no group admin"),
        other => s3_error!(InternalError, "{}", other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(algorithm: &'static str, kms: Option<&str>) -> ServerSideEncryptionConfiguration {
        ServerSideEncryptionConfiguration {
            rules: vec![ServerSideEncryptionRule {
                apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                    sse_algorithm: ServerSideEncryption::from_static(algorithm),
                    kms_master_key_id: kms.map(str::to_string),
                }),
                ..Default::default()
            }],
        }
    }

    #[test]
    fn accepts_only_aes() {
        assert!(requested_aes(&config(ServerSideEncryption::AES256, None)).is_ok());
        assert!(requested_aes(&aes_config()).is_ok());
        let kms = requested_aes(&config(ServerSideEncryption::AWS_KMS, Some("key")));
        assert_eq!(kms.unwrap_err().code(), &S3ErrorCode::NotImplemented);
        let keyed = requested_aes(&config(ServerSideEncryption::AES256, Some("key")));
        assert_eq!(keyed.unwrap_err().code(), &S3ErrorCode::NotImplemented);
        let empty = ServerSideEncryptionConfiguration { rules: Vec::new() };
        assert_eq!(
            requested_aes(&empty).unwrap_err().code(),
            &S3ErrorCode::MalformedXML
        );
    }

    #[test]
    fn plain_bucket_missing() {
        let error = config_missing();
        assert_eq!(
            error.code(),
            &S3ErrorCode::ServerSideEncryptionConfigurationNotFoundError
        );
        assert_eq!(error.status_code(), Some(http::StatusCode::NOT_FOUND));
    }
}
