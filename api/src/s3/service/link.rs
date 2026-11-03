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
use aruna_operations::driver::drive;
use aruna_operations::jobs::source_read::READ_OPERATION;
use aruna_operations::jobs::store::read_job_record;
use aruna_operations::s3::object::get::GetObjectError;
use aruna_operations::s3::object::link::{
    LinkCaller, LinkError, LinkSource, authorize_target, create_link, head_target, read_target,
    target_visible,
};
use aruna_operations::s3::object::versions::{
    ListVersionsInput, ListVersionsItem, ListVersionsOperation, ListVersionsResult,
};
use aruna_operations::s3::object::visible::{SCAN_BUDGET, seal_cursor};
use s3s::dto::{ETag, ETagCondition, GetObjectOutput, StreamingBlob};
use s3s::{S3Response, S3Result, s3_error};

use super::ArunaS3Service;
use super::object::object_range_request;
use crate::s3::error::IntoS3Error;

fn link_error(error: LinkError) -> s3s::S3Error {
    match error {
        LinkError::Denied => s3_error!(AccessDenied, "The link target may not be read"),
        LinkError::NotFound => s3_error!(NoSuchKey, "The link target does not exist."),
        LinkError::Unavailable(_) => {
            s3_error!(ServiceUnavailable, "The link target is unavailable")
        }
        LinkError::Invalid(reason) => s3_error!(InvalidArgument, "{reason}"),
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
        head_target(&self.state, &caller, &target, self.node_id)
            .await
            .map_err(link_error)
    }

    /// A GetObject of a link: the target's bytes, or the asked range of them. `If-Match` holds
    /// against the recorded target, so a read after a relink fails instead of mixing targets.
    pub(super) async fn follow_get(
        &self,
        access: &UserAccess,
        bucket: &str,
        linked: &(VersionSourceBinding, SourceMetadata),
        (range, if_match): (Option<s3s::dto::Range>, Option<ETagCondition>),
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let (binding, recorded) = linked;
        let target = LinkTarget::of(binding, self.realm_id)
            .ok_or_else(|| s3_error!(InternalError, "The link record is malformed"))?;
        let caller = self.link_caller(access, bucket).await;
        let recorded_etag = recorded.etag.as_deref().map(|etag| etag.trim_matches('"'));
        let held = match &if_match {
            None | Some(ETagCondition::Any) => true,
            Some(ETagCondition::ETag(etag)) => recorded_etag == Some(etag.value()),
        };
        if !held {
            // Only a caller the target's GET admits learns that its ETag differs, before any
            // range is weighed: here the local check decides; elsewhere the target opens a read.
            match target.arn.node_id == self.node_id {
                true => authorize_target(&self.state, &caller.auth, &target, READ_OPERATION).await,
                false => read_target(&self.state, &caller, &target, (self.node_id, None))
                    .await
                    .map(drop),
            }
            .map_err(link_error)?;
            return Err(s3_error!(PreconditionFailed, "The ETag does not match"));
        }
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
            // No credential expires, so only the delegation's own lifetime bounds a remote HEAD.
            None => LinkCaller {
                auth: AuthContext::anonymous(self.realm_id),
                credential: (String::new(), ulid::Ulid::nil(), u64::MAX),
            },
        };
        target_visible(&self.state, &caller, &target, self.node_id)
            .await
            .map_err(link_error)
    }
}

/// Where a version listing stands among hidden versions: its markers, and a common prefix whose
/// shown version is still sought, with that search's own markers.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) struct VersionCursor {
    pub(super) key_marker: Option<String>,
    pub(super) version_marker: Option<ulid::Ulid>,
    pub(super) pending: Option<(String, Option<String>, Option<ulid::Ulid>)>,
}

/// The key an item sorts by and the markers that resume after it.
fn item_marker(item: &ListVersionsItem) -> (String, Option<ulid::Ulid>) {
    match item {
        ListVersionsItem::Version {
            key, version_id, ..
        }
        | ListVersionsItem::DeleteMarker {
            key, version_id, ..
        } => (key.clone(), Some(*version_id)),
    }
}

enum VersionProbe {
    Shown,
    Empty,
    Stopped(VersionCursor),
}

impl ArunaS3Service {
    /// Whether a listed version may show: every one but a link the reader may not follow.
    async fn version_shows(&self, caller: &LinkCaller, item: &ListVersionsItem) -> S3Result<bool> {
        let ListVersionsItem::Version {
            link: Some(binding),
            ..
        } = item
        else {
            return Ok(true);
        };
        match LinkTarget::of(binding, self.realm_id) {
            Some(target) => target_visible(&self.state, caller, &target, self.node_id)
                .await
                .map_err(link_error),
            None => Ok(false),
        }
    }

    async fn list_versions(&self, input: &ListVersionsInput) -> S3Result<ListVersionsResult> {
        drive(ListVersionsOperation::new(input.clone()), &self.state)
            .await
            .map_err(IntoS3Error::into_s3_error)
    }

    /// Seeks one shown version below `group` from `at`, spending the budget on hidden ones.
    async fn probe_versions(
        &self,
        (caller, input): (&LinkCaller, &ListVersionsInput),
        (group, at): (&str, (Option<String>, Option<ulid::Ulid>)),
        budget: &mut usize,
    ) -> S3Result<VersionProbe> {
        // The first page holds one version, as the first one of a prefix usually shows.
        let mut probe = ListVersionsInput {
            prefix: Some(group.to_string()),
            delimiter: None,
            key_marker: at.0,
            version_id_marker: at.1,
            max_keys: Some(1),
            ..input.clone()
        };
        loop {
            let page = self.list_versions(&probe).await?;
            for item in &page.items {
                if self.version_shows(caller, item).await? {
                    return Ok(VersionProbe::Shown);
                }
                *budget = budget.saturating_sub(1);
                if *budget == 0 {
                    let (key, version) = item_marker(item);
                    return Ok(VersionProbe::Stopped(VersionCursor {
                        pending: Some((group.to_string(), Some(key), version)),
                        ..Default::default()
                    }));
                }
            }
            if !page.is_truncated {
                return Ok(VersionProbe::Empty);
            }
            (probe.key_marker, probe.version_id_marker) =
                (page.next_key_marker, page.next_version_marker);
            probe.max_keys = None;
        }
    }

    /// One version page the reader may see, from `input`'s markers or a sealed `hidden` cursor.
    /// A link version shows only with its target and a prefix once a version below it does; the
    /// markers name only shown entries, or are sealed when the page stopped among hidden ones.
    pub(super) async fn visible_versions(
        &self,
        access: &UserAccess,
        mut input: ListVersionsInput,
        hidden: Option<VersionCursor>,
    ) -> S3Result<ListVersionsResult> {
        let caller = self.link_caller(access, &input.bucket).await;
        let max = input
            .max_keys
            .unwrap_or(ListVersionsOperation::DEFAULT_MAX_KEYS);
        let mut shown = ListVersionsResult {
            items: Vec::new(),
            common_prefixes: Vec::new(),
            is_truncated: false,
            next_key_marker: None,
            next_version_marker: None,
        };
        let mut budget = SCAN_BUDGET;
        let mut last: Option<(String, Option<ulid::Ulid>)> = None;
        let seal = |shown: &mut ListVersionsResult, cursor: &VersionCursor| -> S3Result<()> {
            let sealed = seal_cursor(cursor)
                .ok_or_else(|| s3_error!(InternalError, "The listing cursor was not sealed"))?;
            (shown.is_truncated, shown.next_key_marker) = (true, Some(sealed));
            Ok(())
        };
        let after = |shown: &mut ListVersionsResult, last: &(String, Option<ulid::Ulid>)| {
            shown.is_truncated = true;
            (shown.next_key_marker, shown.next_version_marker) = (Some(last.0.clone()), last.1);
        };
        if max == 0 {
            return Ok(shown);
        }
        if let Some(hidden) = hidden {
            (input.key_marker, input.version_id_marker) =
                (hidden.key_marker, hidden.version_marker);
            if let Some((group, key, version)) = hidden.pending {
                let at = (key, version);
                match self
                    .probe_versions((&caller, &input), (&group, at), &mut budget)
                    .await?
                {
                    VersionProbe::Shown => {
                        shown.common_prefixes.push(group.clone());
                        last = Some((group.clone(), None));
                        // A page the prefix fills ends here, and the next one starts past it.
                        if shown.common_prefixes.len() >= max {
                            after(&mut shown, &(group, None));
                            return Ok(shown);
                        }
                    }
                    VersionProbe::Empty => {}
                    VersionProbe::Stopped(stopped) => {
                        let cursor = VersionCursor {
                            key_marker: input.key_marker.clone(),
                            version_marker: input.version_id_marker,
                            ..stopped
                        };
                        seal(&mut shown, &cursor)?;
                        return Ok(shown);
                    }
                }
                (input.key_marker, input.version_id_marker) = (Some(group), None);
            }
        }
        loop {
            let page = self.list_versions(&input).await?;
            let mut entries: Vec<(String, Option<ListVersionsItem>)> = (page.items.into_iter())
                .map(|item| (item_marker(&item).0, Some(item)))
                .chain(page.common_prefixes.into_iter().map(|group| (group, None)))
                .collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let count = entries.len();
            for (index, (name, item)) in entries.into_iter().enumerate() {
                let position = match &item {
                    Some(item) => item_marker(item),
                    None => (name.clone(), None),
                };
                // A hidden version costs one; a prefix pays for the hidden ones its probe met.
                let (visible, cost) = match item {
                    Some(item) => match self.version_shows(&caller, &item).await? {
                        true => {
                            shown.items.push(item);
                            (true, 0)
                        }
                        false => (false, 1),
                    },
                    None => {
                        let at = (None, None);
                        match self
                            .probe_versions((&caller, &input), (&name, at), &mut budget)
                            .await?
                        {
                            VersionProbe::Shown => {
                                shown.common_prefixes.push(name.clone());
                                (true, 0)
                            }
                            VersionProbe::Empty => (false, 0),
                            VersionProbe::Stopped(stopped) => {
                                match &last {
                                    Some(last) => after(&mut shown, last),
                                    None => seal(
                                        &mut shown,
                                        &VersionCursor {
                                            key_marker: input.key_marker.clone(),
                                            version_marker: input.version_id_marker,
                                            ..stopped
                                        },
                                    )?,
                                }
                                return Ok(shown);
                            }
                        }
                    }
                };
                budget = budget.saturating_sub(cost);
                if visible {
                    last = Some(position.clone());
                }
                let rest = index + 1 < count || page.is_truncated;
                let full = shown.items.len() + shown.common_prefixes.len() >= max;
                if (full || budget == 0) && rest {
                    match &last {
                        Some(last) => after(&mut shown, last),
                        None => {
                            let cursor = VersionCursor {
                                key_marker: Some(position.0),
                                version_marker: position.1,
                                pending: None,
                            };
                            seal(&mut shown, &cursor)?;
                        }
                    }
                    return Ok(shown);
                }
                if !visible {
                    (input.key_marker, input.version_id_marker) = (Some(position.0), position.1);
                }
            }
            match (page.is_truncated, &last) {
                (false, _) => return Ok(shown),
                (true, Some(last)) => {
                    after(&mut shown, last);
                    return Ok(shown);
                }
                (true, None) => {
                    (input.key_marker, input.version_id_marker) =
                        (page.next_key_marker, page.next_version_marker);
                }
            }
        }
    }
}
