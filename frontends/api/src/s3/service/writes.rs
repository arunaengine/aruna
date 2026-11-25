//! Serves the S3 object writes: put, copy and single and bulk deletions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::copy::{copy_object_response, copy_source_conditions};
use super::forward::copy::CopySource;
use super::response::object_metadata;
use super::{ArunaS3Service, forward, payload};
use crate::s3::auth::DelegatedRequest;
use crate::s3::checksum::{
    parse_upload_checksum, validate_delete_checksum, validate_trailing_checksum,
    verify_trailer_stream,
};
use crate::s3::error::{IntoS3Error, gate_context_error, routing_inputs_error};
use crate::s3::server::body::DeleteObjectsBody;
use crate::s3::util::{
    check_sse, convert_input, declared_trailer_algorithm, parse_copy_source, parse_version_id,
    validate_object_key,
};
use aruna_clock::unix_timestamp_millis;
use aruna_core::request_policy::PolicyRequestExtras;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess, object_permission_path};
use aruna_core::structs::storage::origin::VersionOrigin;
use aruna_ops_data::s3::object::delete::DeleteObjectOperation;
use aruna_ops_data::s3::object::delete::bulk::{
    BulkDeleteEntry, BulkDeleteInput, BulkDeleteOperation,
};
use aruna_ops_data::s3::object::member::read::{MemberPayload, MemberRequest};
use aruna_ops_data::s3::object::member::write::CopyPath;
use aruna_ops_data::s3::object::put::{PutObjectConfig, PutObjectOperation};
use aruna_ops_data::transfer::copy::CopyObjectInput as CopyObjectData;
use aruna_ops_data::transfer::copy::{CopyObjectOperation, CopyReferences};
use aruna_ops_identity::auth::check_permissions::{
    CheckPermissionsConfig, CheckPermissionsOperation,
};
use aruna_ops_identity::auth::enforcement::PolicyLoadOperation;
use aruna_ops_identity::groups::routing::{BucketRules, SnapshotOperation};
use aruna_ops_identity::placement::policy::gate::GateOperation;
use aruna_runner::driver::{drive, drive_retry, node_routing};
use s3s::dto::Error as S3DeleteError;
use s3s::dto::{
    ChecksumType, CopyObjectInput, CopyObjectOutput, DeleteObjectInput, DeleteObjectOutput,
    DeleteObjectsInput, DeleteObjectsOutput, MetadataDirective, PutObjectInput, PutObjectOutput,
};
use s3s::{S3Request, S3Response, S3Result, s3_error};
use tracing::{debug, error};

#[tracing::instrument(err, skip(adapter, req))]
#[allow(clippy::blocks_in_conditions)]
pub(super) async fn put_object(
    adapter: &ArunaS3Service,
    mut req: S3Request<PutObjectInput>,
) -> S3Result<S3Response<PutObjectOutput>> {
    debug!(
        bucket = %req.input.bucket,
        key = %req.input.key,
        content_length = ?req.input.content_length,
        "Received PUT Request"
    );

    let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    let encrypted = adapter.bucket_encrypted(&req.input.bucket).await?;
    check_sse(
        req.input.server_side_encryption.as_ref(),
        req.input.ssekms_key_id.is_some()
            || req.input.ssekms_encryption_context.is_some()
            || req.input.bucket_key_enabled.is_some()
            || req.input.sse_customer_algorithm.is_some()
            || req.input.sse_customer_key.is_some()
            || req.input.sse_customer_key_md5.is_some(),
        encrypted,
    )?;
    validate_object_key(&req.input.key)?;
    let bucket_info = req.extensions.get::<BucketInfo>().cloned();
    // A forwarded body carries the client's trailing checksum in its signed trailer.
    let delegated_trailer = forward::delegated_trailer(&req.extensions)?;
    let trailer_algorithm = match delegated_trailer {
        Some(algorithm) => Some(algorithm),
        None => declared_trailer_algorithm(
            &req.headers,
            req.trailing_headers.is_some(),
            req.input.checksum_algorithm.as_ref(),
        )?,
    };
    let checksum_request = parse_upload_checksum(&req.headers, trailer_algorithm)?;
    let trailing_headers = req.trailing_headers.clone();
    let replication_auth = AuthContext {
        user_id: user_access.user_identity,
        realm_id: adapter.realm_id,
        path_restrictions: user_access.path_restrictions.clone(),
        session: None,
    };
    let replication_bucket = req.input.bucket.clone();
    let replication_key = req.input.key.clone();

    let group_id = bucket_info
        .as_ref()
        .map(|bucket_info| bucket_info.group_id)
        .unwrap_or(user_access.group_id);
    let quota_ceiling = adapter.resolve_quota_ceiling(group_id).await?;
    if let (Some(algorithm), Some(headers)) = (trailer_algorithm, trailing_headers.clone())
        && let Some(body) = req.input.body.take()
    {
        req.input.body = Some(verify_trailer_stream(
            body,
            algorithm,
            checksum_request.checksum_type.as_str() == ChecksumType::COMPOSITE,
            move || headers.read(Clone::clone),
        ));
    }
    // A member that does not coordinate the key's range forwards the write.
    if let Some(forward) = adapter
        .forward_target(&req.extensions, bucket_info.as_ref(), &req.input.key)
        .await?
    {
        let request = MemberRequest {
            method: aruna_core::member_read::MemberMethod::Put,
            operation: "PutObject",
            bucket: req.input.bucket.clone(),
            key: req.input.key.clone(),
            query: forward::passed_query(req.uri.query()),
            headers: forward::passed_headers(&req.headers, req.input.content_length),
            body: match req.input.body.take() {
                Some(body) => forward::stream_body(
                    body,
                    forward::trailer_checksum(trailer_algorithm, trailing_headers.clone()),
                ),
                None => MemberPayload::Empty,
            },
        };
        let response = adapter
            .forward_write(&user_access, &forward, request)
            .await?;
        return Ok(ArunaS3Service::forwarded_put(&response));
    }
    // A forwarded body must end with the issuer's signed trailer.
    if let Some(delegated) = req.extensions.get::<DelegatedRequest>() {
        let composite = checksum_request.checksum_type.as_str() == ChecksumType::COMPOSITE;
        let check = payload::BodyCheck {
            issuer: delegated.issuer,
            nonce: delegated.nonce,
            checksum: delegated_trailer.map(|algorithm| (algorithm, composite)),
        };
        req.input.body = req
            .input
            .body
            .take()
            .map(|body| payload::unframe_body(body, check));
        req.input.content_length = payload::unframed_length(req.input.content_length)?;
    }
    let metadata = object_metadata(
        req.input.metadata.clone().unwrap_or_default(),
        req.input.content_type.as_deref(),
    );
    // A forwarded copy keeps its source's placement refs and origin, as a local copy does.
    let copied = forward::copy_context(&req.extensions, &req.headers)?;
    let create_only = (req.input.if_none_match.as_ref()).is_some_and(|tag| tag.is_any());
    let routing = match bucket_info.as_ref() {
        Some(info) => {
            drive(
                SnapshotOperation::new(
                    &node_routing(&adapter.state),
                    (info).group_id,
                    BucketRules::of(info),
                ),
                &adapter.state,
            )
            .await
        }
        None => {
            drive(
                SnapshotOperation::new(
                    &node_routing(&adapter.state),
                    group_id,
                    BucketRules::Named(replication_bucket.to_string()),
                ),
                &adapter.state,
            )
            .await
        }
    }
    .map_err(routing_inputs_error)?;
    let input = convert_input(req.input)?;
    let gate = drive(
        GateOperation::new(adapter.realm_id, unix_timestamp_millis()),
        &adapter.state,
    )
    .await
    .map_err(gate_context_error)?;
    let mut operation = PutObjectOperation::new(PutObjectConfig {
        user_id: user_access.user_identity,
        group_id,
        realm_id: adapter.realm_id,
        node_id: adapter.node_id,
        request: input,
        expected_checksums: checksum_request.expected.clone(),
        checksum_type: Some(checksum_request.checksum_type.as_str().to_string()),
        version_source: None,
        preassigned_version_id: None,
        exists: false,
        quota_ceiling,
        routing,
    })
    .with_rocrate_limits(adapter.rocrate_limits.clone())
    .with_metadata(metadata)
    .with_restrictions(replication_auth.path_restrictions.clone());
    operation = match copied {
        Some(copied) => operation
            .with_inherited_policies(copied.policies)
            .with_version_origin(copied.origin),
        None => operation.with_version_origin(Some(VersionOrigin::s3_write(
            adapter.node_id,
            &user_access.access_key,
            user_access.user_identity,
        ))),
    };
    if let Some(gate) = gate {
        operation = operation.with_gate(gate);
    }
    if create_only {
        operation = operation.create_only();
    }

    let result = drive(operation, &adapter.state)
        .await
        .map_err(IntoS3Error::into_s3_error)?;
    // The unframed stream already checked a forwarded trailing checksum.
    validate_trailing_checksum(
        trailer_algorithm.filter(|_| delegated_trailer.is_none()),
        &checksum_request.checksum_type,
        trailing_headers.as_ref(),
        &result.location.hashes,
    )?;
    adapter
        .record_touch(
            &user_access.access_key,
            &replication_bucket,
            &replication_key,
            "write",
        )
        .await;

    adapter
        .put_object_response(
            &checksum_request,
            replication_auth,
            group_id,
            replication_bucket,
            replication_key,
            result,
        )
        .await
}

#[tracing::instrument(err, skip(adapter, req))]
#[allow(clippy::blocks_in_conditions)]
pub(super) async fn copy_object(
    adapter: &ArunaS3Service,
    req: S3Request<CopyObjectInput>,
) -> S3Result<S3Response<CopyObjectOutput>> {
    debug!(
        bucket = %req.input.bucket,
        key = %req.input.key,
        "Received COPY OBJECT Request"
    );

    let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    let encrypted = adapter.bucket_encrypted(&req.input.bucket).await?;
    check_sse(
        req.input.server_side_encryption.as_ref(),
        req.input.ssekms_key_id.is_some()
            || req.input.ssekms_encryption_context.is_some()
            || req.input.bucket_key_enabled.is_some()
            || req.input.sse_customer_algorithm.is_some()
            || req.input.sse_customer_key.is_some()
            || req.input.sse_customer_key_md5.is_some()
            || req.input.copy_source_sse_customer_algorithm.is_some()
            || req.input.copy_source_sse_customer_key.is_some()
            || req.input.copy_source_sse_customer_key_md5.is_some(),
        encrypted,
    )?;
    validate_object_key(&req.input.key)?;
    let dest_bucket_info = req.extensions.get::<BucketInfo>().cloned();

    let (source_bucket, source_key, source_version_id) = parse_copy_source(&req.input.copy_source)?;
    // A link copy writes a link to the source's exact version instead of a copy.
    let mode = req
        .headers
        .get(aruna_core::structs::storage::link::COPY_MODE_HEADER);
    if mode.and_then(|value| value.to_str().ok()) == Some("link") {
        let source = (source_bucket, source_key, source_version_id);
        return adapter
            .copy_link(&req, &user_access, dest_bucket_info.as_ref(), source)
            .await;
    }
    let source_extras = req
        .extensions
        .get::<PolicyRequestExtras>()
        .cloned()
        .ok_or_else(|| {
            error!(error = "Missing policy context");
            s3_error!(InternalError, "Missing policy context")
        })?;
    let (source_bucket_info, source_auth_context) = adapter
        .authorize_copy_source(
            &user_access,
            source_bucket.clone(),
            &source_key,
            source_extras,
        )
        .await?;

    let dest_bucket = req.input.bucket.clone();
    let dest_key = req.input.key.clone();

    let metadata_replace = req
        .input
        .metadata_directive
        .as_ref()
        .map(MetadataDirective::as_str)
        == Some(MetadataDirective::REPLACE);
    if source_bucket == dest_bucket
        && source_key == dest_key
        && source_version_id.is_none()
        && !metadata_replace
    {
        return Err(s3_error!(
            InvalidRequest,
            "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes."
        ));
    }

    let conditions = copy_source_conditions(
        req.input.copy_source_if_match.as_ref(),
        req.input.copy_source_if_none_match.as_ref(),
        req.input.copy_source_if_modified_since.as_ref(),
        req.input.copy_source_if_unmodified_since.as_ref(),
    )?;
    let token = adapter.token_read(&req.extensions);
    // A destination another member coordinates takes the copy there.
    if let Some(forward) = adapter
        .forward_target(&req.extensions, dest_bucket_info.as_ref(), &dest_key)
        .await?
    {
        return match adapter
            .copy_path(
                &source_bucket_info,
                (&source_key, source_version_id.is_some()),
                &forward,
            )
            .await?
        {
            CopyPath::Stream => {
                let source = CopySource {
                    bucket: &source_bucket_info,
                    name: source_bucket,
                    key: source_key,
                    version_id: source_version_id,
                    range: None,
                    auth: &source_auth_context,
                    conditions: &conditions,
                };
                let metadata = metadata_replace.then(|| {
                    object_metadata(
                        req.input.metadata.clone().unwrap_or_default(),
                        req.input.content_type.as_deref(),
                    )
                });
                let target = (dest_bucket, dest_key);
                adapter
                    .stream_copy(
                        &user_access,
                        &forward,
                        target,
                        source,
                        metadata,
                        token.as_ref(),
                    )
                    .await
            }
            CopyPath::Relay(holder) => {
                let source = (
                    source_bucket.as_str(),
                    source_key.as_str(),
                    source_version_id,
                );
                let read =
                    (adapter.read_through(&user_access, &holder, source, &req.headers)).await?;
                let metadata = metadata_replace.then(|| {
                    object_metadata(
                        req.input.metadata.clone().unwrap_or_default(),
                        req.input.content_type.as_deref(),
                    )
                });
                let target = (dest_bucket, dest_key);
                adapter
                    .relay_copy(&user_access, &forward, target, read, metadata)
                    .await
            }
            CopyPath::Delegate => {
                let request = MemberRequest {
                    method: aruna_core::member_read::MemberMethod::Put,
                    operation: "CopyObject",
                    bucket: dest_bucket,
                    key: dest_key,
                    query: forward::passed_query(req.uri.query()),
                    headers: forward::passed_headers(&req.headers, None),
                    body: MemberPayload::Empty,
                };
                let response = adapter
                    .forward_write(&user_access, &forward, request)
                    .await?;
                adapter.delegated_copy(response).await
            }
        };
    }
    adapter
        .ensure_source(
            &source_bucket_info,
            (&source_key, source_version_id.is_some()),
        )
        .await?;
    let dest_group_id = dest_bucket_info
        .as_ref()
        .map(|bucket_info| bucket_info.group_id)
        .unwrap_or(user_access.group_id);
    let quota_ceiling = adapter.resolve_quota_ceiling(dest_group_id).await?;
    let replication_auth = AuthContext {
        user_id: user_access.user_identity,
        realm_id: adapter.realm_id,
        path_restrictions: user_access.path_restrictions.clone(),
        session: None,
    };

    let result = drive(
        CopyObjectOperation::new(
            CopyObjectData {
                source_bucket,
                source_key,
                source_version_id,
                source_group_id: source_bucket_info.group_id,
                dest_bucket: dest_bucket.clone(),
                dest_key: dest_key.clone(),
                user_id: user_access.user_identity,
                group_id: dest_group_id,
                realm_id: adapter.realm_id,
                node_id: adapter.node_id,
                quota_ceiling,
                conditions,
                metadata: metadata_replace.then(|| {
                    object_metadata(
                        req.input.metadata.clone().unwrap_or_default(),
                        req.input.content_type.as_deref(),
                    )
                }),
                source_auth_context,
                restrictions: replication_auth.path_restrictions.clone(),
                references: CopyReferences::Preserve,
                job_id: VersionOrigin::copy_job(&user_access.access_key),
            },
            node_routing(&adapter.state),
        )
        .with_token(token),
        &adapter.state,
    )
    .await
    .map_err(IntoS3Error::into_s3_error)?;

    adapter
        .queue_live_replication(
            replication_auth,
            dest_bucket,
            dest_key,
            result.version_id,
            false,
        )
        .await;

    Ok(copy_object_response(result))
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn delete_object(
    adapter: &ArunaS3Service,
    req: S3Request<DeleteObjectInput>,
) -> S3Result<S3Response<DeleteObjectOutput>> {
    debug!(
        bucket = %req.input.bucket,
        key = %req.input.key,
        "Received DELETE Request"
    );

    let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    let version_id = parse_version_id(req.input.version_id)?;
    // A delete marker for a key another member coordinates is forwarded there.
    let bucket_info = req.extensions.get::<BucketInfo>();
    if version_id.is_none()
        && let Some(forward) = adapter
            .forward_target(&req.extensions, bucket_info, &req.input.key)
            .await?
    {
        let request = MemberRequest {
            method: aruna_core::member_read::MemberMethod::Delete,
            operation: "DeleteObject",
            bucket: req.input.bucket.clone(),
            key: req.input.key.clone(),
            query: forward::passed_query(req.uri.query()),
            headers: Vec::new(),
            body: MemberPayload::Empty,
        };
        let response = adapter
            .forward_write(&user_access, &forward, request)
            .await?;
        return Ok(ArunaS3Service::forwarded_delete(&response));
    }
    let replication_auth = AuthContext {
        user_id: user_access.user_identity,
        realm_id: adapter.realm_id,
        path_restrictions: user_access.path_restrictions.clone(),
        session: None,
    };
    // A version a release pins stays; a delete marker is still allowed.
    let guards = match version_id {
        Some(version_id) => {
            let place = (req.input.bucket.as_str(), bucket_info);
            let version = (req.input.key.as_str(), version_id);
            adapter
                .refuse_pinned(place, &replication_auth, version)
                .await?
        }
        None => Vec::new(),
    };
    let replication_bucket = req.input.bucket.clone();
    let replication_key = req.input.key.clone();
    let replicate_latest_delete = version_id.is_none();

    let input = aruna_ops_data::s3::object::delete::DeleteObjectInput {
        bucket: req.input.bucket,
        key: req.input.key,
        version_id,
        group_id: user_access.group_id,
        realm_id: adapter.realm_id,
        node_id: adapter.node_id,
        deleted_by: user_access.user_identity,
    };
    let restrictions = replication_auth.path_restrictions.clone();
    let seed = input.key.clone().into_bytes();
    // A lost commit conflict deletes again after a backoff; nothing moves before the commit.
    let build = || {
        let operation =
            DeleteObjectOperation::new(input.clone()).with_restrictions(restrictions.clone());
        match guards.is_empty() {
            true => operation,
            false => operation.with_fence(guards.clone(), Vec::new()),
        }
    };
    let result = drive_retry(build, &adapter.state, &seed)
        .await
        .map_err(IntoS3Error::into_s3_error)?;

    adapter
        .delete_object_response(
            replication_auth,
            replication_bucket,
            replication_key,
            replicate_latest_delete,
            result,
        )
        .await
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn delete_objects(
    adapter: &ArunaS3Service,
    req: S3Request<DeleteObjectsInput>,
) -> S3Result<S3Response<DeleteObjectsOutput>> {
    debug!(
        bucket = %req.input.bucket,
        objects = req.input.delete.objects.len(),
        "Received DELETE OBJECTS Request"
    );

    let body = req
        .extensions
        .get::<DeleteObjectsBody>()
        .ok_or_else(|| s3_error!(InternalError, "Missing DeleteObjects request body"))?;
    if body.exceeded() {
        return Err(s3_error!(
            MaxMessageLengthExceeded,
            "DeleteObjects request body exceeds 2 MiB"
        ));
    }
    let body = body.take_bytes();
    validate_delete_checksum(&req.headers, &body)?;

    let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    // Deferred per-object policy uses the stashed request context, so
    // each entry is decided against the real query and headers.
    let extras = req
        .extensions
        .get::<PolicyRequestExtras>()
        .cloned()
        .ok_or_else(|| {
            error!(error = "Missing policy context");
            s3_error!(InternalError, "Missing policy context")
        })?;

    if req.input.delete.objects.len() > 1000 {
        return Err(s3_error!(
            MalformedXML,
            "The number of keys in a delete request must not exceed 1000."
        ));
    }

    let quiet = req.input.delete.quiet.unwrap_or(false);
    let bucket_info = req.extensions.get::<BucketInfo>().cloned();
    let replication_auth = AuthContext {
        user_id: user_access.user_identity,
        realm_id: adapter.realm_id,
        path_restrictions: user_access.path_restrictions.clone(),
        session: None,
    };
    let bucket = req.input.bucket;

    // DeleteObjects has no object key in its path: load the realm and
    // group policy sets once and decide every entry against them.
    let policy_evaluator = drive(
        PolicyLoadOperation::new(adapter.realm_id, Some(user_access.group_id)),
        &adapter.state,
    )
    .await
    .map_err(|_| s3_error!(AccessDenied, "Request denied by policy"))?;

    let mut entries = Vec::with_capacity(req.input.delete.objects.len());
    let mut errors: Vec<S3DeleteError> = Vec::new();
    for object in req.input.delete.objects {
        let version_id = match parse_version_id(object.version_id.clone()) {
            Ok(version_id) => version_id,
            Err(_) => {
                errors.push(S3DeleteError {
                    code: Some("NoSuchVersion".to_string()),
                    key: Some(object.key),
                    version_id: object.version_id,
                    message: Some("The specified version does not exist.".to_string()),
                });
                continue;
            }
        };

        let object_path = object_permission_path(
            adapter.realm_id,
            user_access.group_id,
            adapter.node_id,
            &bucket,
            &object.key,
        );
        let allowed = drive(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: replication_auth.clone(),
                path: object_path.clone(),
                required_permission: Permission::WRITE,
            }),
            &adapter.state,
        )
        .await
        .map_err(|err| s3_error!(InternalError, "{}", err.to_string()))?;
        let policy_request = aruna_core::request_policy::policy_request_with(
            &object_path,
            &Permission::WRITE,
            Some(&replication_auth),
            extras.clone(),
        );
        if !allowed || policy_evaluator.evaluate(&policy_request).is_err() {
            errors.push(S3DeleteError {
                code: Some("AccessDenied".to_string()),
                key: Some(object.key),
                version_id: object.version_id,
                message: Some("Access Denied".to_string()),
            });
            continue;
        }

        entries.push(BulkDeleteEntry {
            key: object.key,
            version_id,
        });
    }
    let place = (bucket.as_str(), bucket_info.as_ref());
    let (entries, pinned, guards) = adapter
        .drop_pinned(place, &replication_auth, entries)
        .await?;
    errors.extend(pinned);
    // Delete markers for keys another member coordinates are forwarded there one by one.
    let (entries, forwarded, forward_errors) = adapter
        .forward_deletes(
            &req.extensions,
            (&bucket, bucket_info.as_ref()),
            &user_access,
            entries,
        )
        .await;
    errors.extend(forward_errors);

    let outcomes = drive(
        BulkDeleteOperation::new(BulkDeleteInput {
            bucket: bucket.clone(),
            entries,
            group_id: user_access.group_id,
            realm_id: adapter.realm_id,
            node_id: adapter.node_id,
            deleted_by: user_access.user_identity,
            restrictions: replication_auth.path_restrictions.clone(),
            guards,
        }),
        &adapter.state,
    )
    .await
    .map_err(IntoS3Error::into_s3_error)?;

    Ok(adapter
        .delete_objects_response(
            quiet,
            bucket,
            replication_auth,
            (forwarded, errors),
            outcomes,
        )
        .await)
}
