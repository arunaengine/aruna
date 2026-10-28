//! Follows links in S3 reads: a link answers only to a caller who may read its target, and its
//! bytes come from the target's exact version under that caller's own rights.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::credential_job_id;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::structs::storage::link::LinkTarget;
use aruna_operations::jobs::source_read::HEAD_OPERATION;
use aruna_operations::jobs::store::read_job_record;
use aruna_operations::s3::object::get::GetObjectError;
use aruna_operations::s3::object::link::{
    LinkCaller, LinkError, LinkSource, authorize_target, create_link, read_target, target_visible,
};
use s3s::dto::{ETag, GetObjectOutput, StreamingBlob};
use s3s::{S3Response, S3Result, s3_error};

use super::ArunaS3Service;
use super::object::object_range_request;

fn link_error(error: LinkError) -> s3s::S3Error {
    match error {
        LinkError::Denied => s3_error!(AccessDenied, "The link target may not be read"),
        LinkError::NotFound => s3_error!(NoSuchKey, "The link target does not exist."),
        LinkError::Unavailable(_) => {
            s3_error!(ServiceUnavailable, "The link target is unavailable")
        }
    }
}

impl ArunaS3Service {
    /// The caller a link is followed for. A run's own credential reads the links of its own
    /// workspace as the run's creator was admitted, which no path restriction narrowed.
    async fn link_caller(&self, access: &UserAccess, bucket: &str) -> LinkCaller {
        let mut auth = AuthContext {
            user_id: access.user_identity,
            realm_id: self.realm_id,
            path_restrictions: access.path_restrictions.clone(),
            session: None,
        };
        if let Some(job_id) = credential_job_id(&access.access_key)
            && let Ok(Some(record)) =
                read_job_record(&self.state.storage_handle, job_id, None).await
            && record.created_by == access.user_identity
            && record.workspace_bucket.as_deref() == Some(bucket)
        {
            auth.path_restrictions = None;
        }
        LinkCaller::of_access(auth, access)
    }

    /// Refuses a link the caller may not read the target of; other versions pass.
    pub(super) async fn check_link(
        &self,
        access: &UserAccess,
        bucket: &str,
        binding: &VersionSourceBinding,
    ) -> S3Result<()> {
        let Some(target) = LinkTarget::of(binding, self.realm_id) else {
            return Ok(());
        };
        let caller = self.link_caller(access, bucket).await;
        authorize_target(&self.state, &caller.auth, &target, HEAD_OPERATION)
            .await
            .map_err(link_error)
    }

    /// A GetObject of a link: the target's bytes, or the asked range of them.
    pub(super) async fn follow_get(
        &self,
        access: &UserAccess,
        bucket: &str,
        linked: &(VersionSourceBinding, SourceMetadata),
        range: Option<s3s::dto::Range>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let (binding, recorded) = linked;
        let target = LinkTarget::of(binding, self.realm_id)
            .ok_or_else(|| s3_error!(InternalError, "The link record is malformed"))?;
        let caller = self.link_caller(access, bucket).await;
        let resolved = range
            .map(object_range_request)
            .map(|range| range.resolve(recorded.content_length))
            .transpose()
            .map_err(|error| match error {
                GetObjectError::InvalidRange => {
                    s3_error!(InvalidRange, "The requested range is not satisfiable")
                }
                _ => s3_error!(InvalidArgument, "The range is invalid"),
            })?;
        let asked = resolved.as_ref().map(|resolved| resolved.range.clone());
        let blob = read_target(&self.state, &caller, &target, (self.node_id, asked))
            .await
            .map_err(link_error)?;
        let length = match &resolved {
            Some(resolved) => resolved.content_length,
            None => i64::try_from(recorded.content_length).unwrap_or(i64::MAX),
        };
        let output = GetObjectOutput {
            body: Some(StreamingBlob::wrap(blob)),
            accept_ranges: Some("bytes".to_string()),
            content_length: Some(length),
            content_range: resolved
                .as_ref()
                .map(|resolved| resolved.content_range.clone()),
            content_type: recorded.content_type.clone(),
            e_tag: recorded
                .etag
                .clone()
                .map(|etag| ETag::Strong(etag.trim_matches('"').to_string())),
            ..Default::default()
        };
        Ok(match resolved.is_some() {
            true => S3Response::with_status(output, http::StatusCode::PARTIAL_CONTENT),
            false => S3Response::new(output),
        })
    }
}

impl ArunaS3Service {
    /// CopyObject with the link mode: a link at the destination to the source's exact version,
    /// on this node or on the node `x-aruna-copy-source-node` names.
    pub(super) async fn copy_link(
        &self,
        req: &s3s::S3Request<s3s::dto::CopyObjectInput>,
        access: &UserAccess,
        dest: Option<&aruna_core::structs::storage::blob::BucketInfo>,
        (bucket, key, version): (String, String, Option<ulid::Ulid>),
    ) -> S3Result<S3Response<s3s::dto::CopyObjectOutput>> {
        let dest =
            dest.ok_or_else(|| s3_error!(NoSuchBucket, "The specified bucket does not exist."))?;
        if dest.distribution.is_some() {
            return Err(s3_error!(
                NotImplemented,
                "Links in a distributed bucket are not supported"
            ));
        }
        let header = aruna_core::structs::storage::link::COPY_SOURCE_NODE_HEADER;
        let node_id = match req.headers.get(header) {
            Some(value) => value
                .to_str()
                .ok()
                .and_then(|value| std::str::FromStr::from_str(value.trim()).ok())
                .ok_or_else(|| {
                    s3_error!(InvalidArgument, "The copy source node is not a node id")
                })?,
            None => self.node_id,
        };
        let caller = self.link_caller(access, &req.input.bucket).await;
        let source = LinkSource {
            node_id,
            bucket,
            key,
            version,
        };
        let destination = (
            req.input.bucket.as_str(),
            req.input.key.as_str(),
            dest.group_id,
        );
        let version = create_link(&self.state, &caller, destination, (self.node_id, source))
            .await
            .map_err(link_error)?;
        Ok(S3Response::new(s3s::dto::CopyObjectOutput {
            copy_object_result: Some(s3s::dto::CopyObjectResult {
                e_tag: Some(ETag::Strong(version.to_string())),
                ..Default::default()
            }),
            version_id: Some(version.to_string()),
            ..Default::default()
        }))
    }
}

impl ArunaS3Service {
    /// Whether a listed entry may show: every entry but a link whose target the reader may not
    /// read. A request without a credential reads as the Everyone principal.
    pub(super) async fn link_listable(
        &self,
        reader: Option<&UserAccess>,
        bucket: &str,
        object: &aruna_operations::s3::object::list::ListedObject,
    ) -> S3Result<bool> {
        let Some(binding) = object.link.as_ref() else {
            return Ok(true);
        };
        let Some(target) = LinkTarget::of(binding, self.realm_id) else {
            return Ok(false);
        };
        let caller = match reader {
            Some(access) => self.link_caller(access, bucket).await,
            None => LinkCaller {
                auth: AuthContext::anonymous(self.realm_id),
                credential: (String::new(), ulid::Ulid::nil(), 0),
            },
        };
        target_visible(&self.state, &caller, &target)
            .await
            .map_err(link_error)
    }
}

impl ArunaS3Service {
    /// The version page without links whose target the reader may not read.
    pub(super) async fn visible_versions(
        &self,
        access: &UserAccess,
        bucket: &str,
        mut result: aruna_operations::s3::object::versions::ListVersionsResult,
    ) -> S3Result<aruna_operations::s3::object::versions::ListVersionsResult> {
        use aruna_operations::s3::object::versions::ListVersionsItem;
        let caller = self.link_caller(access, bucket).await;
        let mut items = Vec::with_capacity(result.items.len());
        for item in std::mem::take(&mut result.items) {
            let target = match &item {
                ListVersionsItem::Version {
                    link: Some(binding),
                    ..
                } => LinkTarget::of(binding, self.realm_id),
                _ => {
                    items.push(item);
                    continue;
                }
            };
            let visible = match target {
                Some(target) => target_visible(&self.state, &caller, &target)
                    .await
                    .map_err(link_error)?,
                None => false,
            };
            if visible {
                items.push(item);
            }
        }
        result.items = items;
        Ok(result)
    }
}
