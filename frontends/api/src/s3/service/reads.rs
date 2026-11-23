//! Serves the S3 object reads: get, head and the object attributes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::attributes::{
    RequestedAttributes, attributes_checksum, attributes_output, attributes_parts, parse_max_parts,
    parse_part_marker,
};
use super::object::object_range_request;
use super::response::{
    attach_reference_refresh, full_object_checksums, reference_metadata_refresh,
};
use super::view::InputView;
use super::{ArunaS3Service, forward};
use crate::object_key::ObjectKey;
use crate::s3::browse::wants_index;
use crate::s3::checksum::{
    ApplyChecksums, ChecksumSelection, checksum_mode_enabled, encode_checksums,
};
use crate::s3::error::IntoS3Error;
use crate::s3::util::{
    checksum_response_hashes, location_sse, map_checksum_type, parse_version_id, reject_sse,
};
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess};
use aruna_core::structs::storage::distribution::delegation::COPY_CONTEXT_HEADER;
use aruna_ops_data::s3::object::attributes::{GetAttributesInput, GetAttributesOperation};
use aruna_ops_data::s3::object::head::HeadObjectOperation;
use aruna_ops_data::s3::object::member::read::MemberRead;
use aruna_ops_data::transfer::read::{GetRoutedOperation, KeyedReadOperation, ObjectInfoOperation};
use aruna_runner::driver::drive;
use s3s::dto::{
    GetObjectAttributesInput, GetObjectAttributesOutput, GetObjectInput, GetObjectOutput,
    HeadObjectInput, HeadObjectOutput, StreamingBlob,
};
use s3s::{S3Request, S3Response, S3Result, s3_error};
use tracing::{debug, error};

#[tracing::instrument(err, skip(adapter, req))]
#[allow(clippy::blocks_in_conditions)]
pub(super) async fn get_object(
    adapter: &ArunaS3Service,
    req: S3Request<GetObjectInput>,
) -> S3Result<S3Response<GetObjectOutput>> {
    if let Some(view) = req.extensions.get::<InputView>().copied() {
        return adapter.view_get(view, req.input).await;
    }
    debug!(
        bucket = %req.input.bucket,
        key = %req.input.key,
        "Received GET Request"
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
    let bucket_info = req.extensions.get::<BucketInfo>().cloned();
    if user_access.user_identity.is_nil() && wants_index(&req.headers, &req.input.key) {
        let group_id = bucket_info
            .as_ref()
            .map(|bucket_info| bucket_info.group_id)
            .unwrap_or(user_access.group_id);
        return adapter
            .folder_index(&req.extensions, group_id, &req.input.bucket, &req.input.key)
            .await;
    }
    let requested_range = req.input.range;
    let link_read = (requested_range, req.input.if_match.clone());
    let version_id = parse_version_id(req.input.version_id)?;
    let bucket = req.input.bucket;
    let key = req.input.key;
    let response_bucket = bucket.clone();
    let response_key = key.clone();
    let member_key = forward::reads::member_key(
        false,
        (&bucket, &key, version_id),
        req.uri.query(),
        &req.headers,
    );
    let answer = adapter
        .member_read(
            &req.extensions,
            &user_access,
            bucket_info.as_ref(),
            member_key.clone(),
        )
        .await?;
    let stale = match answer {
        MemberRead::Remote { body, stale } => {
            let mut response = forward::reads::forwarded_get(body);
            forward::reads::mark_stale(&mut response.headers, stale.as_slice());
            return Ok(response);
        }
        MemberRead::Local(stale) => stale,
    };
    let catching_up = adapter
        .read_scope(bucket_info.as_ref(), &key, false)
        .await?;

    let range_request = requested_range.map(object_range_request);

    let input = aruna_ops_data::s3::object::get::GetObjectInput {
        bucket,
        key,
        version_id,
        range: range_request,
        group_id: bucket_info
            .as_ref()
            .map(|bucket_info| bucket_info.group_id)
            .unwrap_or(user_access.group_id),
        user_identity: user_access.user_identity,
        realm_id: adapter.realm_id,
        node_id: adapter.node_id,
    };

    // A device holds version records without their bytes, so a local miss
    // continues against the realm's holders instead of failing here.
    let restrictions = user_access.path_restrictions.clone();
    let token = adapter.token_read(&req.extensions);
    let result = match req.extensions.get::<ObjectKey>() {
        Some(ObjectKey(Ok(Some(private)))) => {
            let (private, limits) = (private.clone(), &adapter.rocrate_limits);
            let read = KeyedReadOperation::new(input, restrictions, private, limits);
            drive(read, &adapter.state).await
        }
        _ => {
            let read = GetRoutedOperation::new(input, restrictions, token);
            drive(read, &adapter.state).await
        }
    };
    // A link is read at its target, under the caller's own rights there.
    if let Err(aruna_ops_data::s3::object::get::GetObjectError::Link(linked)) = &result {
        let bucket = response_bucket.as_str();
        return adapter
            .follow_get(&user_access, bucket, linked, link_read)
            .await;
    }
    let result = result.map_err(IntoS3Error::into_s3_error);
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            if let Some(range_id) = stale {
                return Err(forward::reads::stale_error(error, range_id));
            }
            let caller = (&user_access, bucket_info.as_ref());
            let miss = adapter.missed_read(&req.extensions, caller, member_key, &error);
            return match miss.await? {
                MemberRead::Remote { body, .. } => Ok(forward::reads::forwarded_get(body)),
                // While the coordinator cannot answer, this copy's miss stands, marked.
                MemberRead::Local(Some(range_id)) => {
                    Err(forward::reads::stale_error(error, range_id))
                }
                MemberRead::Local(None) => Err(forward::catch_up_miss(
                    error,
                    catching_up,
                    bucket_info.as_ref(),
                )),
            };
        }
    };
    adapter
        .record_touch(
            &user_access.access_key,
            &response_bucket,
            &response_key,
            "read",
        )
        .await;

    let version_id = result.version_id;
    let resolved_range = result.resolved_range.clone();
    let reader = aruna_core::structs::identity::auth::AuthContext {
        user_id: user_access.user_identity,
        realm_id: adapter.realm_id,
        path_restrictions: user_access.path_restrictions.clone(),
        session: None,
    };
    let reference_refresh = reference_metadata_refresh(
        response_bucket,
        response_key,
        &result,
        adapter.node_id,
        reader,
    );
    let response_fields = adapter.build_response_fields(
        result.location.as_ref(),
        Some(&result.info),
        Some(&result.metadata),
        result.source_metadata.as_ref(),
        result.last_refresh,
        None,
    );
    let blob = if let Some(refresh) = reference_refresh {
        attach_reference_refresh(result.blob, adapter.state.clone(), refresh)
    } else {
        result.blob
    };
    let content = StreamingBlob::wrap(blob);
    let mut output = GetObjectOutput {
        server_side_encryption: location_sse(result.location.as_ref()),
        body: Some(content),
        accept_ranges: resolved_range.as_ref().map(|_| "bytes".to_string()),
        content_length: resolved_range
            .as_ref()
            .map(|range| range.content_length)
            .or(response_fields.content_length),
        content_range: resolved_range
            .as_ref()
            .map(|range| range.content_range.clone()),
        content_type: response_fields.content_type,
        e_tag: response_fields.e_tag,
        last_modified: response_fields.last_modified,
        metadata: response_fields.metadata,
        version_id: version_id.map(|version_id| version_id.to_string()),
        ..Default::default()
    };
    if checksum_mode_enabled(&req.headers) {
        full_object_checksums(&mut output, &result.info);
    }

    let mut response = match resolved_range.is_some() {
        true => S3Response::with_status(output, http::StatusCode::PARTIAL_CONTENT),
        false => S3Response::new(output),
    };
    // A member copying this version through here keeps its placement refs.
    if let Some(value) = forward::copy_policies(&req.extensions, &result.source_policies) {
        (response.headers).insert(COPY_CONTEXT_HEADER, value);
    }
    forward::reads::mark_stale(&mut response.headers, stale.as_slice());
    Ok(response)
}

#[tracing::instrument(err, skip(adapter, req))]
#[allow(clippy::blocks_in_conditions)]
pub(super) async fn get_object_attributes(
    adapter: &ArunaS3Service,
    req: S3Request<GetObjectAttributesInput>,
) -> S3Result<S3Response<GetObjectAttributesOutput>> {
    debug!(
        bucket = %req.input.bucket,
        key = %req.input.key,
        "Received GET OBJECT ATTRIBUTES Request"
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

    let requested = RequestedAttributes::from_request(&req.input.object_attributes)?;
    let requested_part_marker = req.input.part_number_marker;
    let part_number_marker = parse_part_marker(requested_part_marker)?;
    let max_parts = parse_max_parts(req.input.max_parts)?;
    let version_id = parse_version_id(req.input.version_id)?;
    let bucket = req.input.bucket.clone();
    let key = req.input.key.clone();
    let group_id = req
        .extensions
        .get::<BucketInfo>()
        .map(|info| info.group_id)
        .unwrap_or(user_access.group_id);

    let result = drive(
        GetAttributesOperation::new(GetAttributesInput {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id,
            include_parts: requested.object_parts,
        }),
        &adapter.state,
    )
    .await
    .map_err(IntoS3Error::into_s3_error)?;

    // A reference answers from its source observation without opening a source read.
    let remote_info = if result.location.is_none() && result.source_metadata.is_none() {
        Some(
            drive(
                ObjectInfoOperation::new(
                    aruna_ops_data::s3::object::get::GetObjectInput {
                        bucket,
                        key,
                        version_id,
                        range: None,
                        group_id,
                        user_identity: user_access.user_identity,
                        realm_id: adapter.realm_id,
                        node_id: adapter.node_id,
                    },
                    user_access.path_restrictions.clone(),
                ),
                &adapter.state,
            )
            .await
            .map_err(IntoS3Error::into_s3_error)?,
        )
    } else {
        None
    };

    let response_fields = adapter.build_response_fields(
        result.location.as_ref(),
        remote_info.as_ref(),
        None,
        result.source_metadata.as_ref(),
        None,
        result.version_created_at,
    );

    let checksum = requested
        .checksum
        .then(|| attributes_checksum(remote_info.as_ref(), &result))
        .flatten();

    let object_parts = requested
        .object_parts
        .then(|| {
            attributes_parts(
                &result,
                requested_part_marker,
                part_number_marker,
                max_parts,
            )
        })
        .flatten();

    Ok(S3Response::new(attributes_output(
        requested,
        response_fields,
        &result,
        checksum,
        object_parts,
    )))
}

#[tracing::instrument(err, skip(adapter, req))]
pub(super) async fn head_object(
    adapter: &ArunaS3Service,
    req: S3Request<HeadObjectInput>,
) -> S3Result<S3Response<HeadObjectOutput>> {
    if let Some(view) = req.extensions.get::<InputView>().copied() {
        return adapter.view_head(view, req.input).await;
    }
    debug!(
        bucket = %req.input.bucket,
        key = %req.input.key,
        "Received HEAD Request"
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
    let version_id = parse_version_id(req.input.version_id)?;
    let bucket = req.input.bucket.clone();
    let key = req.input.key.clone();
    let bucket_info = req.extensions.get::<BucketInfo>().cloned();
    let group_id = bucket_info
        .as_ref()
        .map(|info| info.group_id)
        .unwrap_or(user_access.group_id);
    let member_key = forward::reads::member_key(
        true,
        (&bucket, &key, version_id),
        req.uri.query(),
        &req.headers,
    );
    let answer = adapter
        .member_read(
            &req.extensions,
            &user_access,
            bucket_info.as_ref(),
            member_key.clone(),
        )
        .await?;
    let stale = match answer {
        MemberRead::Remote { body, stale } => {
            let mut response = forward::reads::forwarded_head(&body);
            forward::reads::mark_stale(&mut response.headers, stale.as_slice());
            return Ok(response);
        }
        MemberRead::Local(stale) => stale,
    };
    let catching_up = adapter
        .read_scope(bucket_info.as_ref(), &key, false)
        .await?;
    let operation = HeadObjectOperation::new(aruna_ops_data::s3::object::head::HeadObjectInput {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id,
    });

    let result = match drive(operation, &adapter.state).await {
        Ok(result) => result,
        Err(error) => {
            let error = error.into_s3_error();
            if let Some(range_id) = stale {
                return Err(forward::reads::stale_error(error, range_id));
            }
            let caller = (&user_access, bucket_info.as_ref());
            let miss = adapter.missed_read(&req.extensions, caller, member_key, &error);
            return match miss.await? {
                MemberRead::Remote { body, .. } => Ok(forward::reads::forwarded_head(&body)),
                // While the coordinator cannot answer, this copy's miss stands, marked.
                MemberRead::Local(Some(range_id)) => {
                    Err(forward::reads::stale_error(error, range_id))
                }
                MemberRead::Local(None) => Err(forward::catch_up_miss(
                    error,
                    catching_up,
                    bucket_info.as_ref(),
                )),
            };
        }
    };

    // A link answers from its recorded target only to a caller who may read the target.
    if let Some(binding) = result.source_binding.as_ref() {
        adapter.check_link(&user_access, &bucket, binding).await?;
    }
    // A reference answers from its source observation without opening a source read.
    let remote_info = if result.location.is_none() && result.source_metadata.is_none() {
        Some(
            drive(
                ObjectInfoOperation::new(
                    aruna_ops_data::s3::object::get::GetObjectInput {
                        bucket,
                        key,
                        version_id,
                        range: None,
                        group_id,
                        user_identity: user_access.user_identity,
                        realm_id: adapter.realm_id,
                        node_id: adapter.node_id,
                    },
                    user_access.path_restrictions.clone(),
                ),
                &adapter.state,
            )
            .await
            .map_err(IntoS3Error::into_s3_error)?,
        )
    } else {
        None
    };

    let response_fields = adapter.build_response_fields(
        result.location.as_ref(),
        remote_info.as_ref(),
        Some(&result.metadata),
        result.source_metadata.as_ref(),
        result.last_refresh,
        result.version_created_at,
    );
    let mut output = HeadObjectOutput {
        server_side_encryption: location_sse(result.location.as_ref()),
        content_length: response_fields.content_length,
        content_type: response_fields.content_type,
        e_tag: response_fields.e_tag,
        version_id: result.version_id.map(|version_id| version_id.to_string()),
        last_modified: response_fields.last_modified,
        metadata: response_fields.metadata,
        ..Default::default()
    };

    if checksum_mode_enabled(&req.headers)
        && let Some(info) = remote_info.as_ref()
    {
        output.apply_checksums(encode_checksums(
            checksum_response_hashes(info.checksum_type, &info.hashes, &info.composite_hashes),
            ChecksumSelection::AllStored,
            map_checksum_type(info.checksum_type),
            info.part_count,
        ));
    } else if checksum_mode_enabled(&req.headers)
        && let Some(location) = result.location.as_ref()
    {
        output.apply_checksums(encode_checksums(
            checksum_response_hashes(
                result.checksum_type,
                &location.hashes,
                &result.composite_hashes,
            ),
            ChecksumSelection::AllStored,
            map_checksum_type(result.checksum_type),
            result.part_count,
        ));
    }

    let mut response = S3Response::new(output);
    forward::reads::mark_stale(&mut response.headers, stale.as_slice());
    Ok(response)
}
