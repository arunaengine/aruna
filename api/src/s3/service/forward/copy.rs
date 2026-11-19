//! Copies into a distributed bucket across ranges: a member keeping the source streams it to the
//! destination's coordinator, else that coordinator copies from its own records (D33).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Nothing is queued.

use aruna_operations::driver::drive;
use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::NodeId;
use aruna_core::jobs::ByteStream;
use aruna_core::member_read::{MemberBody, MemberMethod};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess};
use aruna_core::structs::storage::distribution::delegation::{COPY_CONTEXT_HEADER, CopyContext};
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::origin::{OriginVersion, VersionOrigin};
use aruna_operations::s3::object::copy::{CopySourceConditions, ReadCopyOperation, source_origin};
use aruna_operations::s3::object::get::{GetObjectInput, GetObjectResult, ObjectRangeRequest};
pub(in crate::s3::service) use aruna_operations::s3::object::member_write::CopyPath;
use aruna_operations::s3::object::member_write::{
    CopyPathError, CopyPathOperation, answered_metadata, answered_policies, copy_headers,
    source_read,
};
use s3s::dto::{
    CopyObjectOutput, CopyObjectResult, CopyPartResult, ETag, Timestamp, TimestampFormat,
    UploadPartCopyOutput,
};
use s3s::{S3Error, S3ErrorCode, S3Response, S3Result, s3_error};

use super::{Forward, ForwardBody, ForwardRequest, header, passed_headers, xml_field};
use crate::s3::error::{IntoS3Error, guard_error};
use crate::s3::service::ArunaS3Service;

/// The source a member streams: one whole version, or one byte range for a part.
pub(in crate::s3::service) struct CopySource<'a> {
    pub bucket: &'a BucketInfo,
    pub name: String,
    pub key: String,
    pub version_id: Option<ulid::Ulid>,
    pub range: Option<ObjectRangeRequest>,
    pub auth: &'a AuthContext,
    pub conditions: &'a CopySourceConditions,
}

/// Neither this member nor the coordinator keeps the source; the hint names who does.
fn source_elsewhere(set_id: ulid::Ulid, holders: &[NodeId]) -> S3Error {
    let holders: Vec<String> = holders.iter().map(ToString::to_string).collect();
    let mut error = S3Error::with_message(
        S3ErrorCode::Custom("CopySourceNotHeld".into()),
        format!(
            "Neither this node nor the coordinator of the destination keeps the records of the \
             copy source's range in distributed bucket {set_id}. Send the copy to a node that \
             keeps them: {}.",
            holders.join(", ")
        ),
    );
    error.set_status_code(http::StatusCode::SERVICE_UNAVAILABLE);
    error
}

/// The header that carries what a streamed copy keeps of its source; none for a plain source.
fn context_header(context: &CopyContext) -> S3Result<Option<(String, String)>> {
    if *context == CopyContext::default() {
        return Ok(None);
    }
    let encoded = (context.encode())
        .map_err(|error| s3_error!(InternalError, "The copy context does not encode: {error}"))?;
    Ok(Some((COPY_CONTEXT_HEADER.to_string(), encoded)))
}

/// The placement refs a holder answered a copy source read with.
fn holder_policies(
    response: &MemberBody,
) -> S3Result<Vec<aruna_core::structs::placement::policy::PlacementPolicyRef>> {
    answered_policies(response)
        .ok_or_else(|| s3_error!(ServiceUnavailable, "The holder named unreadable placement"))
}

fn copied_at(text: Option<String>) -> Timestamp {
    text.and_then(|text| Timestamp::parse(TimestampFormat::DateTime, &text).ok())
        .unwrap_or_else(|| SystemTime::now().into())
}

fn e_tag(text: Option<String>) -> Option<ETag> {
    text.and_then(|value| ETag::parse_http_header(value.as_bytes()).ok())
}

impl ArunaS3Service {
    /// How a copy reaches its source; `dest` names the destination's coordinator elsewhere.
    async fn copy_decision(
        &self,
        bucket: &BucketInfo,
        source: (&str, bool),
        dest: Option<NodeId>,
    ) -> S3Result<CopyPath> {
        let Some(binding) = bucket.distribution.as_ref() else {
            return Ok(CopyPath::Stream);
        };
        let decide = CopyPathOperation::new(binding, source, self.node_id, dest);
        (drive(decide, self.state.as_ref()).await).map_err(|error| match error {
            CopyPathError::Route(error) => guard_error(&error),
            CopyPathError::Elsewhere { set_id, holders } => source_elsewhere(set_id, &holders),
        })
    }

    /// Refuses a copy this node writes itself when it does not keep the source.
    pub(in crate::s3::service) async fn ensure_source(
        &self,
        bucket: &BucketInfo,
        source: (&str, bool),
    ) -> S3Result<()> {
        self.copy_decision(bucket, source, None).await.map(|_| ())
    }

    /// How a copy whose destination `forward` coordinates reaches it.
    pub(in crate::s3::service) async fn copy_path(
        &self,
        bucket: &BucketInfo,
        source: (&str, bool),
        forward: &Forward,
    ) -> S3Result<CopyPath> {
        self.copy_decision(bucket, source, Some(forward.node_id))
            .await
    }

    /// Reads a copy source through the member that keeps it, under the caller's authority
    /// there; a failed condition fails the copy.
    pub(in crate::s3::service) async fn read_through(
        &self,
        user: &UserAccess,
        holder: &Forward,
        source: (&str, &str, Option<ulid::Ulid>),
        headers: &http::HeaderMap,
    ) -> S3Result<MemberBody> {
        let request = source_read(source, &passed_headers(headers, None));
        match self.forward_read(user, holder, request).await {
            Err(error) if error.status_code() == Some(http::StatusCode::NOT_MODIFIED) => Err(
                s3s::s3_error!(PreconditionFailed, "The copy source changed"),
            ),
            answer => answer,
        }
    }

    /// Streams a source read through its holder to the destination's coordinator.
    pub(in crate::s3::service) async fn relay_copy(
        &self,
        user: &UserAccess,
        forward: &Forward,
        target: (String, String),
        read: MemberBody,
        metadata: Option<HashMap<String, String>>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let version = header(&read, "x-amz-version-id");
        let length = (read
            .header("content-length")
            .and_then(|value| value.parse().ok()))
        .ok_or_else(|| s3s::s3_error!(ServiceUnavailable, "The holder named no length"))?;
        let metadata = metadata.unwrap_or_else(|| answered_metadata(&read));
        // A relayed source is always a distributed version: its global id is the origin.
        let origin = read.version().map(|version_id| VersionOrigin::Copy {
            from: OriginVersion::Global(GlobalId::Version {
                realm_id: self.realm_id,
                version_id,
            }),
            job_id: VersionOrigin::copy_job(&user.access_key),
        });
        let context = CopyContext {
            policies: holder_policies(&read)?,
            origin,
        };
        let mut headers = copy_headers(&metadata, length);
        headers.extend(context_header(&context)?);
        self.send_copy(user, forward, target, (headers, read.stream), version)
            .await
    }

    /// Streams a source range read through its holder to the upload's coordinator.
    pub(in crate::s3::service) async fn relay_part(
        &self,
        user: &UserAccess,
        forward: &Forward,
        target: (String, String, String),
        read: MemberBody,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let version = header(&read, "x-amz-version-id");
        let length = header(&read, "content-length")
            .ok_or_else(|| s3s::s3_error!(ServiceUnavailable, "The holder named no length"))?;
        let context = CopyContext {
            policies: holder_policies(&read)?,
            origin: None,
        };
        let mut headers = vec![("content-length".to_string(), length)];
        headers.extend(context_header(&context)?);
        self.send_part(user, forward, target, (headers, read.stream), version)
            .await
    }

    /// Reads the source here, its conditions checked first, through a holder when this member
    /// keeps only its record.
    async fn read_copy(
        &self,
        source: CopySource<'_>,
        token: Option<&aruna_operations::s3::object::read::TokenRead>,
    ) -> S3Result<GetObjectResult> {
        let input = GetObjectInput {
            bucket: source.name,
            key: source.key,
            version_id: source.version_id,
            range: source.range,
            group_id: source.bucket.group_id,
            user_identity: source.auth.user_id,
            realm_id: self.realm_id,
            node_id: self.node_id,
        };
        let restrictions = source.auth.path_restrictions.clone();
        let conditions = source.conditions;
        let read = ReadCopyOperation::new(input, restrictions, conditions, token.cloned());
        drive(read, &self.state)
            .await
            .map_err(IntoS3Error::into_s3_error)
    }

    /// Streams a source this member keeps to the destination's coordinator as its write.
    pub(in crate::s3::service) async fn stream_copy(
        &self,
        user: &UserAccess,
        forward: &Forward,
        (bucket, key): (String, String),
        source: CopySource<'_>,
        metadata: Option<HashMap<String, String>>,
        token: Option<&aruna_operations::s3::object::read::TokenRead>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let (name, key_of) = (source.name.clone(), source.key.clone());
        let distributed = source.bucket.distribution.is_some();
        let read = self.read_copy(source, token).await?;
        let version = read.resolved_version_id.or(read.version_id);
        let mut headers = copy_headers(metadata.as_ref().unwrap_or(&read.metadata), read.info.size);
        let context = CopyContext {
            policies: read.source_policies.clone(),
            origin: version.and_then(|id| {
                let local = (self.realm_id, self.node_id);
                let job_id = VersionOrigin::copy_job(&user.access_key);
                source_origin(local, (&name, &key_of), (id, job_id), distributed)
            }),
        };
        headers.extend(context_header(&context)?);
        let version = version.map(|version| version.to_string());
        self.send_copy(user, forward, (bucket, key), (headers, read.blob), version)
            .await
    }

    /// Writes a copy's bytes at the destination's coordinator.
    async fn send_copy(
        &self,
        user: &UserAccess,
        forward: &Forward,
        (bucket, key): (String, String),
        (headers, body): (Vec<(String, String)>, ByteStream),
        version: Option<String>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let request = ForwardRequest {
            method: MemberMethod::Put,
            operation: "PutObject",
            bucket,
            key,
            query: None,
            headers,
            body: ForwardBody::Stream(body, None),
        };
        let response = self.forward_write(user, forward, request).await?;
        let put = Self::forwarded_put(&response).output;
        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: put.e_tag,
                last_modified: Some(SystemTime::now().into()),
                checksum_crc32: put.checksum_crc32,
                checksum_crc32c: put.checksum_crc32c,
                checksum_crc64nvme: put.checksum_crc64nvme,
                checksum_sha1: put.checksum_sha1,
                checksum_sha256: put.checksum_sha256,
                ..Default::default()
            }),
            version_id: put.version_id,
            copy_source_version_id: version,
            ..Default::default()
        }))
    }

    /// Streams a source range this member keeps to the upload's coordinator as one part.
    pub(in crate::s3::service) async fn stream_part(
        &self,
        user: &UserAccess,
        forward: &Forward,
        (bucket, key, query): (String, String, String),
        source: CopySource<'_>,
        token: Option<&aruna_operations::s3::object::read::TokenRead>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let read = self.read_copy(source, token).await?;
        let version = read.resolved_version_id.or(read.version_id);
        let length = (read.resolved_range.as_ref())
            .map_or(read.info.size, |range| range.range.end - range.range.start);
        let mut headers = vec![("content-length".to_string(), length.to_string())];
        let context = CopyContext {
            policies: read.source_policies.clone(),
            origin: None,
        };
        headers.extend(context_header(&context)?);
        let version = version.map(|version| version.to_string());
        self.send_part(
            user,
            forward,
            (bucket, key, query),
            (headers, read.blob),
            version,
        )
        .await
    }

    /// Writes one part's bytes at the upload's coordinator.
    async fn send_part(
        &self,
        user: &UserAccess,
        forward: &Forward,
        (bucket, key, query): (String, String, String),
        (headers, body): (Vec<(String, String)>, ByteStream),
        version: Option<String>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let request = ForwardRequest {
            method: MemberMethod::Put,
            operation: "UploadPart",
            bucket,
            key,
            query: Some(query),
            headers,
            body: ForwardBody::Stream(body, None),
        };
        let response = self.forward_write(user, forward, request).await?;
        let part = Self::forwarded_part(&response).output;
        Ok(S3Response::new(UploadPartCopyOutput {
            copy_part_result: Some(CopyPartResult {
                e_tag: part.e_tag,
                last_modified: Some(SystemTime::now().into()),
                checksum_crc32: part.checksum_crc32,
                checksum_crc32c: part.checksum_crc32c,
                checksum_crc64nvme: part.checksum_crc64nvme,
                checksum_sha1: part.checksum_sha1,
                checksum_sha256: part.checksum_sha256,
                ..Default::default()
            }),
            copy_source_version_id: version,
            ..Default::default()
        }))
    }

    /// The coordinator's answer to a delegated CopyObject.
    pub(in crate::s3::service) async fn delegated_copy(
        &self,
        response: MemberBody,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let version_id = header(&response, "x-amz-version-id");
        let source_version = header(&response, "x-amz-copy-source-version-id");
        let body = self.answer_text(response).await?;
        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: e_tag(xml_field(&body, "ETag")),
                last_modified: Some(copied_at(xml_field(&body, "LastModified"))),
                checksum_crc32: xml_field(&body, "ChecksumCRC32"),
                checksum_crc32c: xml_field(&body, "ChecksumCRC32C"),
                checksum_crc64nvme: xml_field(&body, "ChecksumCRC64NVME"),
                checksum_sha1: xml_field(&body, "ChecksumSHA1"),
                checksum_sha256: xml_field(&body, "ChecksumSHA256"),
                ..Default::default()
            }),
            version_id,
            copy_source_version_id: source_version,
            ..Default::default()
        }))
    }

    /// The coordinator's answer to a delegated UploadPartCopy.
    pub(in crate::s3::service) async fn delegated_part(
        &self,
        response: MemberBody,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let source_version = header(&response, "x-amz-copy-source-version-id");
        let body = self.answer_text(response).await?;
        Ok(S3Response::new(UploadPartCopyOutput {
            copy_part_result: Some(CopyPartResult {
                e_tag: e_tag(xml_field(&body, "ETag")),
                last_modified: Some(copied_at(xml_field(&body, "LastModified"))),
                checksum_crc32: xml_field(&body, "ChecksumCRC32"),
                checksum_crc32c: xml_field(&body, "ChecksumCRC32C"),
                checksum_crc64nvme: xml_field(&body, "ChecksumCRC64NVME"),
                checksum_sha1: xml_field(&body, "ChecksumSHA1"),
                checksum_sha256: xml_field(&body, "ChecksumSHA256"),
                ..Default::default()
            }),
            copy_source_version_id: source_version,
            ..Default::default()
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refusal_names_holders() {
        let node = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let error = source_elsewhere(ulid::Ulid::from_bytes([1; 16]), &[node]);
        assert_eq!(
            error.status_code(),
            Some(http::StatusCode::SERVICE_UNAVAILABLE)
        );
        assert!(
            error
                .message()
                .unwrap_or_default()
                .contains(&node.to_string())
        );
    }
}
