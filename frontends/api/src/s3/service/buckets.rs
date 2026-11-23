//! Serves the bucket calls of the S3 adapter: create, head, versioning, cors, encryption,
//! deletion and replication.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::ArunaS3Service;
use super::view::InputView;
use crate::s3::cors::{map_bucket_cors, parse_bucket_cors};
use crate::s3::error::IntoS3Error;
use aruna_core::bucket_read::GetBucketOperation;
use aruna_core::request_policy::PolicyRequestExtras;
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::mirror_repair::SyncMirrorIntent;
use aruna_core::structs::storage::replication::ArunaArn;
use aruna_core::structs::sync_relationship::{
    SyncMode, SyncRelationship, SyncState, SyncStatusSnapshot,
};
use aruna_ops_data::replication::mirror_repair::kick::KickRepairOperation;
use aruna_ops_data::replication::mirror_repair::record::MirrorRecordOperation;
use aruna_ops_data::replication::sync_relationship::SyncRelationshipDirection;
use aruna_ops_data::replication::sync_relationship::create::CreateRelationshipOperation;
use aruna_ops_data::s3::bucket::cors::{DeleteCorsOperation, GetCorsOperation, PutCorsOperation};
use aruna_ops_data::s3::bucket::create::CreateBucketOperation;
use aruna_ops_data::s3::bucket::delete::DeleteBucketOperation;
use aruna_ops_data::s3::bucket::list::ListBucketsOperation;
use aruna_runner::driver::{drive, drive_retry};
use s3s::dto::{
    Bucket, CreateBucketInput, CreateBucketOutput, DeleteBucketCorsInput, DeleteBucketCorsOutput,
    DeleteBucketInput, DeleteBucketOutput, DeleteBucketReplicationInput,
    DeleteBucketReplicationOutput, GetBucketCorsInput, GetBucketCorsOutput,
    GetBucketEncryptionInput, GetBucketEncryptionOutput, GetBucketLocationInput,
    GetBucketLocationOutput, GetBucketReplicationInput, GetBucketReplicationOutput,
    HeadBucketInput, HeadBucketOutput, ListBucketsInput, ListBucketsOutput, PutBucketCorsInput,
    PutBucketCorsOutput, PutBucketEncryptionInput, PutBucketEncryptionOutput,
    PutBucketReplicationInput, PutBucketReplicationOutput,
};
use s3s::{S3Request, S3Response, S3Result, s3_error};
use std::time::SystemTime;
use tracing::{debug, error};

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn create_bucket(
    adapter: &ArunaS3Service,
    req: S3Request<CreateBucketInput>,
) -> S3Result<S3Response<CreateBucketOutput>> {
    debug!(bucket = %req.input.bucket, "Received CREATE BUCKET Request");

    let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;

    let operation = CreateBucketOperation::new(
        req.input.bucket.clone(),
        BucketInfo {
            group_id: user_access.group_id,
            created_at: SystemTime::now(),
            created_by: user_access.user_identity,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        },
    )
    .mapped();

    drive(operation, &adapter.state)
        .await
        .map_err(IntoS3Error::into_s3_error)?;

    Ok(S3Response::new(CreateBucketOutput::default()))
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn head_bucket(
    adapter: &ArunaS3Service,
    req: S3Request<HeadBucketInput>,
) -> S3Result<S3Response<HeadBucketOutput>> {
    if let Some(view) = req.extensions.get::<InputView>().copied() {
        return adapter.view_head_bucket(view).await;
    }
    debug!(bucket = %req.input.bucket, "Received HEAD BUCKET Request");

    let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;

    drive(GetBucketOperation::new(req.input.bucket), &adapter.state)
        .await
        .map_err(IntoS3Error::into_s3_error)?;

    Ok(S3Response::new(HeadBucketOutput::default()))
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn get_bucket_location(
    adapter: &ArunaS3Service,
    req: S3Request<GetBucketLocationInput>,
) -> S3Result<S3Response<GetBucketLocationOutput>> {
    debug!(bucket = %req.input.bucket, "Received GET BUCKET LOCATION Request");

    let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;

    drive(GetBucketOperation::new(req.input.bucket), &adapter.state)
        .await
        .map_err(IntoS3Error::into_s3_error)?;

    // No region is configured for the node, so report the default location
    // constraint (an empty constraint denotes the us-east-1 default region).
    Ok(S3Response::new(GetBucketLocationOutput {
        location_constraint: None,
    }))
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn list_buckets(
    adapter: &ArunaS3Service,
    req: S3Request<ListBucketsInput>,
) -> S3Result<S3Response<ListBucketsOutput>> {
    debug!("Received LIST BUCKETS Request");

    let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    let extras = req
        .extensions
        .get::<PolicyRequestExtras>()
        .cloned()
        .ok_or_else(|| {
            error!(error = "Missing policy context");
            s3_error!(InternalError, "Missing policy context")
        })?;

    let result = drive(
        ListBucketsOperation::new(aruna_ops_data::s3::bucket::list::ListBucketsInput {
            group_id: user_access.group_id,
            prefix: req.input.prefix.clone(),
            continuation_token: req.input.continuation_token.clone(),
            max_buckets: req
                .input
                .max_buckets
                .and_then(|max_buckets| usize::try_from(max_buckets).ok()),
        }),
        &adapter.state,
    )
    .await
    .map_err(IntoS3Error::into_s3_error)?;

    let mut buckets = Vec::new();
    for (bucket, bucket_info) in result.buckets {
        if adapter
            .can_access_bucket(&user_access, &bucket, &bucket_info, &extras)
            .await?
        {
            buckets.push(Bucket {
                bucket_arn: None,
                bucket_region: None,
                creation_date: Some(bucket_info.created_at.into()),
                name: Some(bucket),
            });
        }
    }

    Ok(S3Response::new(ListBucketsOutput {
        buckets: Some(buckets),
        continuation_token: result.continuation_token,
        owner: None,
        prefix: req.input.prefix,
    }))
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn put_bucket_cors(
    adapter: &ArunaS3Service,
    req: S3Request<PutBucketCorsInput>,
) -> S3Result<S3Response<PutBucketCorsOutput>> {
    debug!(bucket = %req.input.bucket, "Received PUT BUCKET CORS Request");

    let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    let config = parse_bucket_cors(req.input.cors_configuration.clone())?;

    drive(
        PutCorsOperation::new(req.input.bucket.clone(), config),
        &adapter.state,
    )
    .await
    .map_err(IntoS3Error::into_s3_error)?;

    Ok(S3Response::new(PutBucketCorsOutput::default()))
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn get_bucket_cors(
    adapter: &ArunaS3Service,
    req: S3Request<GetBucketCorsInput>,
) -> S3Result<S3Response<GetBucketCorsOutput>> {
    debug!(bucket = %req.input.bucket, "Received GET BUCKET CORS Request");

    let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    let config = drive(
        GetCorsOperation::new(req.input.bucket.clone()),
        &adapter.state,
    )
    .await
    .map_err(IntoS3Error::into_s3_error)?;

    Ok(S3Response::new(map_bucket_cors(config)))
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn delete_bucket_cors(
    adapter: &ArunaS3Service,
    req: S3Request<DeleteBucketCorsInput>,
) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
    debug!(bucket = %req.input.bucket, "Received DELETE BUCKET CORS Request");

    let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;

    drive(
        DeleteCorsOperation::new(req.input.bucket.clone()),
        &adapter.state,
    )
    .await
    .map_err(IntoS3Error::into_s3_error)?;

    Ok(S3Response::new(DeleteBucketCorsOutput::default()))
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn get_bucket_encryption(
    adapter: &ArunaS3Service,
    req: S3Request<GetBucketEncryptionInput>,
) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
    let config = adapter.encryption_config(&req.input.bucket).await?;
    Ok(S3Response::new(GetBucketEncryptionOutput {
        server_side_encryption_configuration: Some(config),
    }))
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn put_bucket_encryption(
    adapter: &ArunaS3Service,
    req: S3Request<PutBucketEncryptionInput>,
) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
    let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    let extras = req
        .extensions
        .get::<PolicyRequestExtras>()
        .cloned()
        .ok_or_else(|| s3_error!(InternalError, "Missing policy context"))?;
    let config = &req.input.server_side_encryption_configuration;
    adapter
        .enable_encryption(&req.input.bucket, config, &user_access, extras)
        .await?;
    Ok(S3Response::new(PutBucketEncryptionOutput::default()))
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn delete_bucket(
    adapter: &ArunaS3Service,
    req: S3Request<DeleteBucketInput>,
) -> S3Result<S3Response<DeleteBucketOutput>> {
    debug!(bucket = %req.input.bucket, "Received DELETE BUCKET Request");

    let delete =
        DeleteBucketOperation::new(req.input.bucket).purge_notices(adapter.state.node_and_realm());
    drive(delete, &adapter.state)
        .await
        .map_err(IntoS3Error::into_s3_error)?;

    Ok(S3Response::new(DeleteBucketOutput::default()))
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn put_bucket_replication(
    adapter: &ArunaS3Service,
    req: S3Request<PutBucketReplicationInput>,
) -> S3Result<S3Response<PutBucketReplicationOutput>> {
    let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    let extras = req
        .extensions
        .get::<PolicyRequestExtras>()
        .cloned()
        .ok_or_else(|| {
            error!(error = "Missing policy context");
            s3_error!(InternalError, "Missing policy context")
        })?;
    let bucket = req.input.bucket;
    let targets =
        adapter.parse_replication_targets(&bucket, &req.input.replication_configuration)?;
    let old_relationships = adapter.list_xml_relationships(&bucket).await?;
    let source = ArunaArn::s3_bucket(adapter.realm_id, adapter.node_id, bucket.clone())
        .map_err(|error| s3_error!(InvalidArgument, "{}", error.to_string()))?;
    let created_at = SystemTime::now();
    let relationships = targets
        .into_iter()
        .map(|(target, replicate_deletes)| SyncRelationship {
            id: ulid::Ulid::generate(),
            source: source.clone(),
            target,
            mode: SyncMode::Continuous,
            reference_handling: Default::default(),
            reference_serving: false,
            replicate_deletes,
            created_by: user_access.user_identity,
            created_at,
            state: SyncState::Enabled,
            status: SyncStatusSnapshot::default(),
        })
        .collect::<Vec<_>>();

    let mut stored: Vec<SyncRelationship> = Vec::new();
    for relationship in &relationships {
        drive(
            MirrorRecordOperation::stage(relationship.clone(), SyncMirrorIntent::Reconcile),
            &adapter.state,
        )
        .await
        .map_err(|error| s3_error!(InternalError, "{}", error))?;
        if let Err(error) = adapter
            .create_sync_mirror(&user_access, relationship, &extras)
            .await
        {
            let _ = drive(KickRepairOperation::new(), &adapter.state).await;
            for created in &stored {
                if drive(
                    MirrorRecordOperation::stage(created.clone(), SyncMirrorIntent::Delete),
                    &adapter.state,
                )
                .await
                .is_ok()
                {
                    let _ = adapter
                        .delete_sync_relationship(
                            created.clone(),
                            SyncRelationshipDirection::Outgoing,
                        )
                        .await;
                    let _ = drive(KickRepairOperation::new(), &adapter.state).await;
                    if adapter.remove_sync_mirror(created).await {
                        adapter
                            .clear_mirror_repair(created, SyncMirrorIntent::Delete)
                            .await;
                    }
                }
            }
            return Err(error);
        }
        if let Err(error) = drive_retry(
            || CreateRelationshipOperation::activate(relationship.clone()),
            &adapter.state,
            &relationship.id.to_bytes(),
        )
        .await
        .map_err(|error| s3_error!(InternalError, "{}", error.to_string()))
        {
            let mut rollback = stored.clone();
            rollback.push(relationship.clone());
            for created in &rollback {
                if drive(
                    MirrorRecordOperation::stage(created.clone(), SyncMirrorIntent::Delete),
                    &adapter.state,
                )
                .await
                .is_ok()
                {
                    let _ = adapter
                        .delete_sync_relationship(
                            created.clone(),
                            SyncRelationshipDirection::Outgoing,
                        )
                        .await;
                    let _ = drive(KickRepairOperation::new(), &adapter.state).await;
                    if adapter.remove_sync_mirror(created).await {
                        adapter
                            .clear_mirror_repair(created, SyncMirrorIntent::Delete)
                            .await;
                    }
                }
            }
            return Err(error);
        }
        adapter
            .clear_mirror_repair(relationship, SyncMirrorIntent::Reconcile)
            .await;
        stored.push(relationship.clone());
    }

    for relationship in old_relationships {
        drive(
            MirrorRecordOperation::stage(relationship.clone(), SyncMirrorIntent::Delete),
            &adapter.state,
        )
        .await
        .map_err(|error| s3_error!(InternalError, "{}", error))?;
        adapter
            .delete_sync_relationship(relationship.clone(), SyncRelationshipDirection::Outgoing)
            .await?;
        let _ = drive(KickRepairOperation::new(), &adapter.state).await;
        if adapter.remove_sync_mirror(&relationship).await {
            adapter
                .clear_mirror_repair(&relationship, SyncMirrorIntent::Delete)
                .await;
        }
    }

    Ok(S3Response::new(PutBucketReplicationOutput::default()))
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn get_bucket_replication(
    adapter: &ArunaS3Service,
    req: S3Request<GetBucketReplicationInput>,
) -> S3Result<S3Response<GetBucketReplicationOutput>> {
    let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;

    let relationships = adapter.list_xml_relationships(&req.input.bucket).await?;
    if relationships.is_empty() {
        return Err(s3_error!(
            ReplicationConfigurationNotFoundError,
            "Replication configuration not found"
        ));
    }

    Ok(S3Response::new(GetBucketReplicationOutput {
        replication_configuration: Some(adapter.build_replication_configuration(&relationships)),
    }))
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn delete_bucket_replication(
    adapter: &ArunaS3Service,
    req: S3Request<DeleteBucketReplicationInput>,
) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
    let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    let relationships = adapter.list_xml_relationships(&req.input.bucket).await?;

    for relationship in relationships {
        drive(
            MirrorRecordOperation::stage(relationship.clone(), SyncMirrorIntent::Delete),
            &adapter.state,
        )
        .await
        .map_err(|error| s3_error!(InternalError, "{}", error))?;
        adapter
            .delete_sync_relationship(relationship.clone(), SyncRelationshipDirection::Outgoing)
            .await?;
        let _ = drive(KickRepairOperation::new(), &adapter.state).await;
        if adapter.remove_sync_mirror(&relationship).await {
            adapter
                .clear_mirror_repair(&relationship, SyncMirrorIntent::Delete)
                .await;
        }
    }

    Ok(S3Response::new(DeleteBucketReplicationOutput::default()))
}
