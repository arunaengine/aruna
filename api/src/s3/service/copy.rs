//! Authorizes copy sources and maps copy conditions and copy results for the S3 adapter.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{ArunaS3Service, reference_etag};
use crate::s3::auth::map_authorize_error;
use crate::s3::checksum::{ApplyChecksums, ChecksumSelection, encode_checksums};
use crate::s3::error::IntoS3Error;
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess, object_permission_path};
use aruna_operations::auth::request_authorization::authorize;
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::driver::drive;
use aruna_operations::s3::bucket::get::GetBucketOperation;
use aruna_operations::s3::multipart::part_copy::PartCopyResult;
use aruna_operations::s3::object::copy::{CopyResultData, CopySourceConditions};
use s3s::dto::{
    ChecksumType, CopyObjectOutput, CopyObjectResult, CopyPartResult, ETag, ETagCondition,
    Timestamp, TimestampFormat, UploadPartCopyOutput,
};
use s3s::{S3Response, S3Result, s3_error};
use std::time::SystemTime;

impl ArunaS3Service {
    /// Resolves the source bucket and authorizes a read there, because the auth
    /// layer only authorized the destination path.
    pub(super) async fn authorize_copy_source(
        &self,
        user_access: &UserAccess,
        source_bucket: String,
        source_key: &str,
        extras: PolicyRequestExtras,
    ) -> S3Result<(BucketInfo, AuthContext)> {
        let source_bucket_info = drive(GetBucketOperation::new(source_bucket.clone()), &self.state)
            .await
            .map_err(IntoS3Error::into_s3_error)?;

        let source_auth_context = if source_bucket_info.group_id == user_access.group_id {
            AuthContext {
                user_id: user_access.user_identity,
                realm_id: user_access.user_identity.realm_id,
                path_restrictions: user_access.path_restrictions.clone(),
                session: None,
            }
        } else {
            AuthContext::anonymous(self.realm_id)
        };
        authorize(
            &self.state,
            self.realm_id,
            &source_auth_context,
            &object_permission_path(
                self.realm_id,
                source_bucket_info.group_id,
                self.node_id,
                &source_bucket,
                source_key,
            ),
            &Permission::READ,
            extras,
        )
        .await
        .map_err(map_authorize_error)?;

        Ok((source_bucket_info, source_auth_context))
    }
}

pub(super) fn copy_object_response(result: CopyResultData) -> S3Response<CopyObjectOutput> {
    let e_tag = match &result.location {
        Some(location) => location
            .hashes
            .get(HASH_MD5)
            .map(|value| ETag::Strong(hex::encode(value))),
        None => result.source_metadata.as_ref().map(reference_etag),
    };
    let mut copy_object_result = CopyObjectResult {
        e_tag,
        last_modified: Some(result.created_at.into()),
        ..Default::default()
    };
    if let Some(location) = &result.location {
        copy_object_result.apply_checksums(encode_checksums(
            &location.hashes,
            ChecksumSelection::AllStored,
            ChecksumType::from_static(ChecksumType::FULL_OBJECT),
            None,
        ));
    }

    S3Response::new(CopyObjectOutput {
        copy_object_result: Some(copy_object_result),
        version_id: Some(result.version_id.to_string()),
        copy_source_version_id: result
            .source_version_id
            .map(|version_id| version_id.to_string()),
        ..Default::default()
    })
}

pub(super) fn copy_part_response(result: PartCopyResult) -> S3Response<UploadPartCopyOutput> {
    let mut copy_part_result = CopyPartResult {
        e_tag: result
            .part_location
            .hashes
            .get(HASH_MD5)
            .map(|value| ETag::Strong(hex::encode(value))),
        last_modified: Some(result.part_location.created_at.into()),
        ..Default::default()
    };
    copy_part_result.apply_checksums(encode_checksums(
        &result.part_location.hashes,
        ChecksumSelection::AllStored,
        ChecksumType::from_static(ChecksumType::FULL_OBJECT),
        None,
    ));

    S3Response::new(UploadPartCopyOutput {
        copy_part_result: Some(copy_part_result),
        copy_source_version_id: result
            .source_version_id
            .map(|version_id| version_id.to_string()),
        ..Default::default()
    })
}

fn etag_condition_value(condition: &ETagCondition) -> String {
    match condition {
        ETagCondition::Any => "*".to_string(),
        ETagCondition::ETag(etag) => etag.value().to_string(),
    }
}

fn timestamp_system_time(timestamp: &Timestamp) -> S3Result<SystemTime> {
    let mut rendered = Vec::new();
    timestamp
        .format(TimestampFormat::DateTime, &mut rendered)
        .map_err(|_| s3_error!(InvalidArgument, "Invalid timestamp"))?;
    let rendered =
        String::from_utf8(rendered).map_err(|_| s3_error!(InvalidArgument, "Invalid timestamp"))?;
    let parsed = chrono::DateTime::parse_from_rfc3339(&rendered)
        .map_err(|_| s3_error!(InvalidArgument, "Invalid timestamp"))?;
    Ok(parsed.with_timezone(&chrono::Utc).into())
}

pub(super) fn copy_source_conditions(
    if_match: Option<&ETagCondition>,
    if_none_match: Option<&ETagCondition>,
    if_modified_since: Option<&Timestamp>,
    if_unmodified_since: Option<&Timestamp>,
) -> S3Result<CopySourceConditions> {
    Ok(CopySourceConditions {
        if_match: if_match.map(etag_condition_value),
        if_none_match: if_none_match.map(etag_condition_value),
        if_modified_since: if_modified_since.map(timestamp_system_time).transpose()?,
        if_unmodified_since: if_unmodified_since.map(timestamp_system_time).transpose()?,
    })
}
