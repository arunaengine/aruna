//! Serves the S3 multipart calls: create, part upload and copy, completion, abort and
//! the part and upload listings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::copy::{copy_part_response, copy_source_conditions};
use super::forward::copy::CopySource;
use super::listing::{list_parts_output, list_uploads_output};
use super::multipart::parse_upload_marker;
use super::response::object_metadata;
use super::{ArunaS3Service, COMPLETION_DEADLINE, forward, payload};
use crate::s3::auth::DelegatedRequest;
use crate::s3::checksum::{
    ApplyChecksums, ChecksumSelection, encode_checksums, parse_completion_checksum,
    parse_upload_checksum, validate_part_count, validate_trailing_checksum, verify_trailer_stream,
};
use crate::s3::error::{IntoS3Error, gate_context_error, routing_inputs_error};
use crate::s3::multipart_join::{
    CompletionFailure, CompletionRequest, DeclaredChecksum, await_completion,
    conflicting_completion,
};
use crate::s3::util::{
    check_sse, checked_size, declared_trailer_algorithm, map_checksum_type, parse_checksum_hint,
    parse_checksum_type, parse_completed_part, parse_copy_source, parse_part_number,
    parse_source_range, reject_sse, sse_header, stored_encrypted, validate_object_key,
};
use aruna_clock::unix_timestamp_millis;
use aruna_core::request_policy::PolicyRequestExtras;
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess};
use aruna_core::structs::storage::distribution::delegation::{Payload, body_digest};
use aruna_core::structs::storage::distribution::upload::UploadTag;
use aruna_core::structs::storage::origin::VersionOrigin;
use aruna_ops_data::s3::multipart::abort::{AbortUploadInput, AbortUploadOperation};
use aruna_ops_data::s3::multipart::complete::{CompleteUploadInput, CompleteUploadOperation};
use aruna_ops_data::s3::multipart::create::{CreateMultipartInput, CreateMultipartOperation};
use aruna_ops_data::s3::multipart::part_upload::UploadPartOperation;
use aruna_ops_data::s3::multipart::parts::ListPartsOperation;
use aruna_ops_data::s3::multipart::uploads::{ListUploadsInput, ListUploadsOperation};
use aruna_ops_data::s3::object::member::read::{MemberPayload, MemberRequest};
use aruna_ops_data::s3::object::member::write::CopyPath;
use aruna_ops_data::transfer::part_copy::PartCopyInput as UploadPartCopyData;
use aruna_ops_data::transfer::part_copy::{KeepPoliciesOperation, PartCopyOperation};
use aruna_ops_identity::groups::routing::{BucketRules, SnapshotOperation};
use aruna_ops_identity::placement::policy::gate::GateOperation;
use aruna_runner::driver::{drive, drive_until, node_routing};
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, ChecksumType,
    CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CreateMultipartUploadInput,
    CreateMultipartUploadOutput, ETag, ListMultipartUploadsInput, ListMultipartUploadsOutput,
    ListPartsInput, ListPartsOutput, UploadPartCopyInput, UploadPartCopyOutput, UploadPartInput,
    UploadPartOutput,
};
use s3s::{S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::sync::Arc;
use tracing::{Instrument, debug, error};

#[tracing::instrument(err, skip(adapter, req))]
#[allow(clippy::blocks_in_conditions)]
pub(super) async fn create_multipart_upload(
    adapter: &ArunaS3Service,
    req: S3Request<CreateMultipartUploadInput>,
) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
    debug!(
        bucket = %req.input.bucket,
        key = %req.input.key,
        "Received CREATE MULTIPART Request"
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
    // Uploads run on the coordinator of the key's range.
    if let Some(forward) = adapter
        .forward_target(&req.extensions, bucket_info.as_ref(), &req.input.key)
        .await?
    {
        let request = MemberRequest {
            method: aruna_core::member_read::MemberMethod::Post,
            operation: "CreateMultipartUpload",
            bucket: req.input.bucket.clone(),
            key: req.input.key.clone(),
            query: forward::passed_query(req.uri.query()),
            headers: forward::passed_headers(&req.headers, None),
            body: MemberPayload::Empty,
        };
        let response = adapter
            .forward_write(&user_access, &forward, request)
            .await?;
        return adapter
            .forwarded_create(response, req.input.bucket, req.input.key)
            .await;
    }
    let checksum_hint = parse_checksum_hint(&req.input)?;

    let group_id = bucket_info
        .as_ref()
        .map(|bucket_info| bucket_info.group_id)
        .unwrap_or(user_access.group_id);
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
                    BucketRules::Named(req.input.bucket.to_string()),
                ),
                &adapter.state,
            )
            .await
        }
    }
    .map_err(routing_inputs_error)?;
    let gate = drive(
        GateOperation::new(adapter.realm_id, unix_timestamp_millis()),
        &adapter.state,
    )
    .await
    .map_err(gate_context_error)?;
    let mut operation = CreateMultipartOperation::new(CreateMultipartInput {
        bucket: req.input.bucket.clone(),
        key: req.input.key.clone(),
        group_id,
        created_by: user_access.user_identity,
        checksum_hint: checksum_hint.clone(),
        routing,
    })
    .with_metadata(object_metadata(
        req.input.metadata.clone().unwrap_or_default(),
        req.input.content_type.as_deref(),
    ));
    if let Some(gate) = gate {
        operation = operation.with_gate(gate);
    }

    let result = drive(operation, &adapter.state)
        .await
        .map_err(IntoS3Error::into_s3_error)?;

    let upload_id = adapter
        .shown_upload(
            bucket_info.as_ref(),
            &req.input.key,
            result.record.upload_id,
        )
        .await?;
    Ok(S3Response::new(CreateMultipartUploadOutput {
        bucket: Some(req.input.bucket),
        key: Some(req.input.key),
        upload_id: Some(upload_id),
        checksum_algorithm: req.input.checksum_algorithm,
        checksum_type: checksum_hint.map(|hint| map_checksum_type(hint.checksum_type)),
        server_side_encryption: sse_header(encrypted),
        ..Default::default()
    }))
}

#[tracing::instrument(err, skip(adapter, req))]
#[allow(clippy::blocks_in_conditions)]
pub(super) async fn upload_part(
    adapter: &ArunaS3Service,
    mut req: S3Request<UploadPartInput>,
) -> S3Result<S3Response<UploadPartOutput>> {
    debug!(
        bucket = %req.input.bucket,
        key = %req.input.key,
        part_number = req.input.part_number,
        content_length = ?req.input.content_length,
        "Received UPLOAD PART Request"
    );

    let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    reject_sse(
        req.input.sse_customer_algorithm.is_some()
            || req.input.sse_customer_key.is_some()
            || req.input.sse_customer_key_md5.is_some(),
    )?;
    validate_object_key(&req.input.key)?;
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
    let bucket_info = req.extensions.get::<BucketInfo>().cloned();
    let bucket_info = bucket_info.as_ref();
    if let Some(forward) = adapter
        .forward_upload(&req.extensions, bucket_info, &req.input.key)
        .await?
    {
        let body =
            req.input
                .body
                .take()
                .map(|body| match (trailer_algorithm, trailing_headers.clone()) {
                    (Some(algorithm), Some(headers)) => verify_trailer_stream(
                        body,
                        algorithm,
                        checksum_request.checksum_type.as_str() == ChecksumType::COMPOSITE,
                        move || headers.read(Clone::clone),
                    ),
                    _ => body,
                });
        let request = MemberRequest {
            method: aruna_core::member_read::MemberMethod::Put,
            operation: "UploadPart",
            bucket: req.input.bucket.clone(),
            key: req.input.key.clone(),
            query: forward::passed_query(req.uri.query()),
            headers: forward::passed_headers(&req.headers, req.input.content_length),
            body: match body {
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
        return Ok(ArunaS3Service::forwarded_part(&response));
    }
    let upload_id = adapter
        .upload_id(bucket_info, &req.input.key, &req.input.upload_id)
        .await?;
    // A part copied on another member keeps its source's placement refs, as one copied here.
    if let Some(copied) = forward::copy_context(&req.extensions, &req.headers)? {
        let upload = (adapter.realm_id, upload_id);
        let target = (req.input.bucket.as_str(), req.input.key.as_str());
        let keep = KeepPoliciesOperation::new(upload, target, &copied.policies);
        drive(keep, &adapter.state)
            .await
            .map_err(IntoS3Error::into_s3_error)?;
    }
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
    let body = req
        .input
        .body
        .take()
        .map(|body| match (trailer_algorithm, trailing_headers.clone()) {
            (Some(algorithm), Some(headers)) => verify_trailer_stream(
                body,
                algorithm,
                checksum_request.checksum_type.as_str() == ChecksumType::COMPOSITE,
                move || headers.read(Clone::clone),
            ),
            _ => body,
        })
        .map(BackendStream::new_from_boxed)
        .ok_or_else(|| s3_error!(InvalidRequest, "Missing body"))?;

    let operation = UploadPartOperation::new(
        aruna_ops_data::s3::multipart::part_upload::UploadPartInput {
            bucket: req.input.bucket,
            key: req.input.key,
            upload_id,
            part_number: parse_part_number(req.input.part_number, S3ErrorCode::InvalidArgument)?,
            content_length: req.input.content_length.map(checked_size).transpose()?,
            body: Some(body),
            created_by: user_access.user_identity,
            expected_checksums: checksum_request.expected.clone(),
        },
    );

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

    let mut output = UploadPartOutput {
        e_tag: result
            .location
            .hashes
            .get(HASH_MD5)
            .map(|value| ETag::Strong(hex::encode(value))),
        server_side_encryption: sse_header(stored_encrypted(&result.location.format)),
        ..Default::default()
    };
    output.apply_checksums(encode_checksums(
        &result.location.hashes,
        ChecksumSelection::Requested(checksum_request.response_algorithm),
        checksum_request.checksum_type,
        None,
    ));

    Ok(S3Response::new(output))
}

#[tracing::instrument(err, skip(adapter, req))]
#[allow(clippy::blocks_in_conditions)]
pub(super) async fn upload_part_copy(
    adapter: &ArunaS3Service,
    req: S3Request<UploadPartCopyInput>,
) -> S3Result<S3Response<UploadPartCopyOutput>> {
    debug!(
        bucket = %req.input.bucket,
        key = %req.input.key,
        part_number = req.input.part_number,
        "Received UPLOAD PART COPY Request"
    );

    let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    reject_sse(
        req.input.sse_customer_algorithm.is_some()
            || req.input.sse_customer_key.is_some()
            || req.input.sse_customer_key_md5.is_some()
            || req.input.copy_source_sse_customer_algorithm.is_some()
            || req.input.copy_source_sse_customer_key.is_some()
            || req.input.copy_source_sse_customer_key_md5.is_some(),
    )?;
    validate_object_key(&req.input.key)?;

    let part_number = parse_part_number(req.input.part_number, S3ErrorCode::InvalidArgument)?;
    let range = parse_source_range(req.input.copy_source_range.as_deref())?;
    let conditions = copy_source_conditions(
        req.input.copy_source_if_match.as_ref(),
        req.input.copy_source_if_none_match.as_ref(),
        req.input.copy_source_if_modified_since.as_ref(),
        req.input.copy_source_if_unmodified_since.as_ref(),
    )?;

    let (source_bucket, source_key, source_version_id) = parse_copy_source(&req.input.copy_source)?;
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

    let token = adapter.token_read(&req.extensions);
    // The upload belongs to the coordinator of its key's range.
    let dest_info = req.extensions.get::<BucketInfo>().cloned();
    if let Some(forward) = adapter
        .forward_upload(&req.extensions, dest_info.as_ref(), &req.input.key)
        .await?
    {
        let upload: String =
            url::form_urlencoded::byte_serialize(req.input.upload_id.as_bytes()).collect();
        let query = format!("partNumber={}&uploadId={upload}", req.input.part_number);
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
                    range,
                    auth: &source_auth_context,
                    conditions: &conditions,
                };
                let target = (req.input.bucket, req.input.key, query);
                adapter
                    .stream_part(&user_access, &forward, target, source, token.as_ref())
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
                let target = (req.input.bucket, req.input.key, query);
                adapter
                    .relay_part(&user_access, &forward, target, read)
                    .await
            }
            CopyPath::Delegate => {
                let request = MemberRequest {
                    method: aruna_core::member_read::MemberMethod::Put,
                    operation: "UploadPartCopy",
                    bucket: req.input.bucket,
                    key: req.input.key,
                    query: forward::passed_query(req.uri.query()),
                    headers: forward::passed_headers(&req.headers, None),
                    body: MemberPayload::Empty,
                };
                let response = adapter
                    .forward_write(&user_access, &forward, request)
                    .await?;
                adapter.delegated_part(response).await
            }
        };
    }
    adapter
        .ensure_source(
            &source_bucket_info,
            (&source_key, source_version_id.is_some()),
        )
        .await?;
    let upload_id = adapter
        .upload_id(dest_info.as_ref(), &req.input.key, &req.input.upload_id)
        .await?;
    let result = drive(
        PartCopyOperation::new(
            UploadPartCopyData {
                source_bucket,
                source_key,
                source_version_id,
                source_group_id: source_bucket_info.group_id,
                dest_bucket: req.input.bucket,
                dest_key: req.input.key,
                upload_id,
                part_number,
                range,
                user_id: user_access.user_identity,
                node_id: adapter.node_id,
                conditions,
                source_auth_context,
            },
            token,
        ),
        &adapter.state,
    )
    .await
    .map_err(IntoS3Error::into_s3_error)?;

    Ok(copy_part_response(result))
}

#[tracing::instrument(err, skip(adapter, req))]
#[allow(clippy::blocks_in_conditions)]
pub(super) async fn complete_multipart_upload(
    adapter: &ArunaS3Service,
    req: S3Request<CompleteMultipartUploadInput>,
) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
    debug!(
        bucket = %req.input.bucket,
        key = %req.input.key,
        "Received COMPLETE MULTIPART Request"
    );

    let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    reject_sse(
        req.input.sse_customer_algorithm.is_some()
            || req.input.sse_customer_key.is_some()
            || req.input.sse_customer_key_md5.is_some(),
    )?;
    validate_object_key(&req.input.key)?;
    let bucket_info = req.extensions.get::<BucketInfo>().cloned();
    let group_id = bucket_info
        .as_ref()
        .map(|bucket_info| bucket_info.group_id)
        .unwrap_or(user_access.group_id);
    if let Some(forward) = adapter
        .forward_upload(&req.extensions, bucket_info.as_ref(), &req.input.key)
        .await?
    {
        let body = forward::parts::completion_body(req.input.multipart_upload.as_ref());
        let mut headers = forward::passed_headers(&req.headers, Some(body.len() as i64));
        headers.retain(|(name, _)| name != "content-type");
        headers.push(("content-type".to_string(), "application/xml".to_string()));
        let request = MemberRequest {
            method: aruna_core::member_read::MemberMethod::Post,
            operation: "CompleteMultipartUpload",
            bucket: req.input.bucket.clone(),
            key: req.input.key.clone(),
            query: forward::passed_query(req.uri.query()),
            headers,
            body: MemberPayload::Known(bytes::Bytes::from(body)),
        };
        let response = adapter
            .forward_write(&user_access, &forward, request)
            .await?;
        return adapter.forwarded_complete(response).await;
    }
    // A forwarded completion lists exactly the parts the issuer bound.
    if let Some(delegated) = req.extensions.get::<DelegatedRequest>() {
        let body = forward::parts::completion_body(req.input.multipart_upload.as_ref());
        let bound = matches!(delegated.payload, Payload::Digest(digest)
            if digest == body_digest(body.as_bytes()));
        if !bound {
            return Err(s3_error!(AccessDenied, "Delegation refused: other parts"));
        }
    }
    let quota_ceiling = adapter.resolve_quota_ceiling(group_id).await?;
    let checksum_request = parse_completion_checksum(&req.headers)?;
    let upload_id = adapter
        .upload_id(bucket_info.as_ref(), &req.input.key, &req.input.upload_id)
        .await?;
    let replication_auth = AuthContext {
        user_id: user_access.user_identity,
        realm_id: adapter.realm_id,
        path_restrictions: user_access.path_restrictions.clone(),
        session: None,
    };
    let completed_parts = req
        .input
        .multipart_upload
        .as_ref()
        .and_then(|multipart| multipart.parts.as_ref())
        .map(|parts| {
            parts
                .iter()
                .map(parse_completed_part)
                .collect::<S3Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();
    validate_part_count(&checksum_request, completed_parts.len())?;

    let gate = drive(
        GateOperation::new(adapter.realm_id, unix_timestamp_millis()),
        &adapter.state,
    )
    .await
    .map_err(gate_context_error)?;
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
                    BucketRules::Named(req.input.bucket.to_string()),
                ),
                &adapter.state,
            )
            .await
        }
    }
    .map_err(routing_inputs_error)?;
    let checksum_type = parse_checksum_type(&checksum_request.checksum_type);
    let object_size = req.input.mpu_object_size.map(checked_size).transpose()?;
    let declared = DeclaredChecksum {
        algorithm: checksum_request.response_algorithm,
        checksum_type,
        explicit: checksum_request.checksum_type_declared,
    };
    let request = CompletionRequest::new(
        &completed_parts,
        &checksum_request.expected,
        declared,
        object_size,
    );
    let mut operation = CompleteUploadOperation::new(CompleteUploadInput {
        bucket: req.input.bucket.clone(),
        key: req.input.key.clone(),
        upload_id,
        realm_id: adapter.realm_id,
        node_id: adapter.node_id,
        completed_parts,
        expected_checksums: checksum_request.expected.clone(),
        checksum_algorithm: checksum_request.response_algorithm,
        checksum_type,
        checksum_type_explicit: checksum_request.checksum_type_declared,
        object_size,
        created_by: user_access.user_identity,
        quota_ceiling,
        now_ms: unix_timestamp_millis(),
    })
    .with_rocrate_limits(adapter.rocrate_limits.clone())
    .with_restrictions(replication_auth.path_restrictions.clone())
    .with_routing(routing)
    .with_version_origin(VersionOrigin::s3_write(
        adapter.node_id,
        &user_access.access_key,
        user_access.user_identity,
    ));
    if let Some(gate) = gate {
        operation = operation.with_gate(gate);
    }

    // The completion outlives its request: a dropped connection must not
    // cancel it, and a retry joins it instead of racing a second one.
    let bucket = req.input.bucket.clone();
    let key = req.input.key.clone();
    let service = adapter.clone();
    let deadline = tokio::time::Instant::now() + COMPLETION_DEADLINE;
    let work = async move {
        let outcome = match drive_until(operation, &service.state, deadline).await {
            Ok(result) => {
                service
                    .complete_put(
                        replication_auth,
                        group_id,
                        bucket,
                        key,
                        result.version_id,
                        result.location.blob_size,
                    )
                    .await;
                Ok(result)
            }
            Err(error) => Err(CompletionFailure::new(&error.into_s3_error())),
        };
        Arc::new(outcome)
    };
    // A request may only join or replay the completion of exactly what it asks for.
    let Ok(joined) = adapter.completions.join_as(
        (req.input.bucket.clone(), req.input.key.clone(), upload_id),
        request,
        work.instrument(tracing::Span::current()),
    ) else {
        return Err(conflicting_completion());
    };

    match await_completion(joined).await.as_ref() {
        Ok(result) => Ok(adapter.complete_upload_response(
            req.input.bucket,
            req.input.key,
            &checksum_request,
            result.clone(),
        )),
        Err(failure) => Err(failure.to_s3_error()),
    }
}

#[tracing::instrument(err, skip(adapter, req))]
#[allow(clippy::blocks_in_conditions)]
pub(super) async fn abort_multipart_upload(
    adapter: &ArunaS3Service,
    req: S3Request<AbortMultipartUploadInput>,
) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
    debug!(
        bucket = %req.input.bucket,
        key = %req.input.key,
        "Received ABORT MULTIPART Request"
    );

    let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    let bucket_info = req.extensions.get::<BucketInfo>();
    if let Some(forward) = adapter
        .forward_upload(&req.extensions, bucket_info, &req.input.key)
        .await?
    {
        let request = MemberRequest {
            method: aruna_core::member_read::MemberMethod::Delete,
            operation: "AbortMultipartUpload",
            bucket: req.input.bucket.clone(),
            key: req.input.key.clone(),
            query: forward::passed_query(req.uri.query()),
            headers: Vec::new(),
            body: MemberPayload::Empty,
        };
        adapter
            .forward_write(&user_access, &forward, request)
            .await?;
        return Ok(S3Response::new(AbortMultipartUploadOutput::default()));
    }
    let upload_id = adapter
        .upload_id(bucket_info, &req.input.key, &req.input.upload_id)
        .await?;
    let operation = AbortUploadOperation::new(AbortUploadInput {
        bucket: req.input.bucket,
        key: req.input.key,
        upload_id,
        now_ms: unix_timestamp_millis(),
    });

    // A dropped connection must not stop the abort between its transactions.
    let state = adapter.state.clone();
    tokio::spawn(async move { drive(operation, &state).await }.in_current_span())
        .await
        .map_err(|error| s3_error!(InternalError, "{}", error))?
        .map_err(IntoS3Error::into_s3_error)?;

    Ok(S3Response::new(AbortMultipartUploadOutput::default()))
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn list_parts(
    adapter: &ArunaS3Service,
    req: S3Request<ListPartsInput>,
) -> S3Result<S3Response<ListPartsOutput>> {
    debug!(
        bucket = %req.input.bucket,
        key = %req.input.key,
        max_parts = ?req.input.max_parts,
        "Received LIST PARTS Request"
    );

    let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    reject_sse(
        req.input.sse_customer_algorithm.is_some()
            || req.input.sse_customer_key.is_some()
            || req.input.sse_customer_key_md5.is_some(),
    )?;
    // Uploads live on the coordinator of the key's range, so their parts are listed there.
    let bucket_info = req.extensions.get::<BucketInfo>();
    if let Some(forward) = adapter
        .forward_upload(&req.extensions, bucket_info, &req.input.key)
        .await?
    {
        let request = MemberRequest {
            method: aruna_core::member_read::MemberMethod::Get,
            operation: "ListParts",
            bucket: req.input.bucket.clone(),
            key: req.input.key.clone(),
            query: forward::passed_query(req.uri.query()),
            headers: Vec::new(),
            body: MemberPayload::Empty,
        };
        let response = adapter
            .forward_write(&user_access, &forward, request)
            .await?;
        return adapter
            .forwarded_list(response, req.input.bucket, req.input.key)
            .await;
    }
    let upload_id = adapter
        .upload_id(bucket_info, &req.input.key, &req.input.upload_id)
        .await?;
    let part_number_marker = match req.input.part_number_marker {
        None => None,
        Some(marker) if marker < 0 => {
            return Err(s3_error!(InvalidArgument, "Invalid part-number-marker"));
        }
        Some(marker) => Some(u16::try_from(marker).unwrap_or(u16::MAX)),
    };
    let max_parts = match req.input.max_parts {
        None => ListPartsOperation::DEFAULT_MAX_PARTS,
        Some(max_parts) => usize::try_from(max_parts)
            .map_err(|_| s3_error!(InvalidArgument, "max-parts must be non-negative"))?
            .min(ListPartsOperation::DEFAULT_MAX_PARTS),
    };

    let result = drive(
        ListPartsOperation::new(aruna_ops_data::s3::multipart::parts::ListPartsInput {
            bucket: req.input.bucket.clone(),
            key: req.input.key.clone(),
            upload_id,
            part_number_marker,
            max_parts,
        }),
        &adapter.state,
    )
    .await
    .map_err(IntoS3Error::into_s3_error)?;

    Ok(list_parts_output(req.input, max_parts, result))
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn list_multipart_uploads(
    adapter: &ArunaS3Service,
    req: S3Request<ListMultipartUploadsInput>,
) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
    debug!(
        bucket = %req.input.bucket,
        prefix = ?req.input.prefix,
        "Received LIST MULTIPART UPLOADS Request"
    );

    let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
        error!(error = "Missing user context");
        s3_error!(UnexpectedContent, "Missing user context")
    })?;
    let bucket = req.input.bucket.clone();
    let prefix = req.input.prefix.clone();
    let delimiter = req.input.delimiter.clone();
    let key_marker = req.input.key_marker.clone();
    let bucket_info = req.extensions.get::<BucketInfo>().cloned();
    let bounds = adapter
        .delegated_listing(&req.extensions, bucket_info.as_ref(), req.uri.query())
        .await?;
    // A distributed bucket shows tagged upload ids; the marker names the stored one.
    let requested_upload_marker = req.input.upload_id_marker.as_deref().map(|marker| {
        UploadTag::parse(marker)
            .map(|tag| tag.upload_id.to_string())
            .unwrap_or_else(|| marker.to_string())
    });
    let upload_id_marker =
        parse_upload_marker(key_marker.as_deref(), requested_upload_marker.as_deref())?;
    let max_uploads = match req.input.max_uploads {
        None => ListUploadsOperation::DEFAULT_MAX_UPLOADS,
        Some(max_uploads) => usize::try_from(max_uploads)
            .map_err(|_| s3_error!(InvalidArgument, "max-uploads must be non-negative"))?
            .min(ListUploadsOperation::DEFAULT_MAX_UPLOADS),
    };
    // A distributed bucket's uploads live on the coordinator of each key's range.
    if let Some(binding) = (bucket_info.as_ref()).and_then(|info| info.distribution.clone()) {
        let shape = (upload_id_marker, max_uploads);
        return match bounds {
            Some(bounds) => {
                adapter
                    .range_uploads(&binding, req.input, bounds, shape)
                    .await
            }
            None => {
                let group_id =
                    (bucket_info.as_ref()).map_or(user_access.group_id, |info| info.group_id);
                let shape = (group_id, upload_id_marker, max_uploads);
                adapter
                    .merged_uploads(&user_access, &binding, req.input, shape)
                    .await
            }
        };
    }

    let result = drive(
        ListUploadsOperation::new(ListUploadsInput {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            delimiter: delimiter.clone(),
            key_marker: key_marker.clone(),
            upload_id_marker,
            max_uploads,
        }),
        &adapter.state,
    )
    .await
    .map_err(IntoS3Error::into_s3_error)?;
    let listed: Vec<(String, ulid::Ulid)> = (result.uploads.iter())
        .map(|record| (record.key.clone(), record.upload_id))
        .collect();
    let shown = adapter.shown_uploads(bucket_info.as_ref(), &listed).await?;
    let mut response = list_uploads_output(req.input, max_uploads, result);
    let output = &mut response.output;
    let uploads = output.uploads.iter_mut().flatten();
    let ids = uploads
        .map(|upload| &mut upload.upload_id)
        .chain([&mut output.next_upload_id_marker]);
    for id in ids.flatten() {
        if let Some(tagged) = shown.get(id.as_str()) {
            *id = tagged.clone();
        }
    }
    Ok(response)
}
