//! Answers S3 reads of a run's input view: listings, heads and ranged gets of the exact
//! versions the run captured. The view takes no writes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::{JobId, JobRecord};
use aruna_operations::jobs::store::read_job_record;
use aruna_operations::jobs::workflow::input_view::{
    MAX_VIEW_KEYS, ViewEntry, ViewError, decode_token, encode_token, head_entry, list_visible,
    read_entry, view_bucket, view_entries,
};
use aruna_operations::s3::object::get::GetObjectError;
use percent_encoding::utf8_percent_encode;
use s3s::dto::{
    CommonPrefix, ETag, ETagCondition, EncodingType, GetObjectInput, GetObjectOutput,
    HeadBucketOutput, HeadObjectInput, HeadObjectOutput, ListObjectsV2Input, ListObjectsV2Output,
    Object, ObjectStorageClass, StreamingBlob, Timestamp,
};
use s3s::{S3Response, S3Result, s3_error};

use super::object::object_range_request;
use super::{ArunaS3Service, URL_ENCODE_SET};

/// The run whose input view a request reads; the access check sets it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct InputView {
    pub(crate) job_id: JobId,
}

fn view_error(error: ViewError) -> s3s::S3Error {
    match error {
        ViewError::NotFound => s3_error!(NoSuchKey, "The specified key does not exist."),
        ViewError::Denied => s3_error!(AccessDenied, "The input read is denied"),
        ViewError::Unavailable(_) => {
            s3_error!(ServiceUnavailable, "The input source is unavailable")
        }
    }
}

/// `If-Match` holds for `*` or for the entry's own ETag.
fn matches(condition: Option<&ETagCondition>, entry: &ViewEntry) -> bool {
    match condition {
        None | Some(ETagCondition::Any) => true,
        Some(ETagCondition::ETag(etag)) => etag.value() == entry.etag(),
    }
}

impl ArunaS3Service {
    async fn view_record(&self, view: InputView) -> S3Result<(JobRecord, Vec<ViewEntry>)> {
        let record = read_job_record(&self.state.storage_handle, view.job_id, None)
            .await
            .map_err(|_| s3_error!(ServiceUnavailable, "The run could not be read"))?
            .ok_or_else(|| s3_error!(NoSuchBucket, "The specified bucket does not exist."))?;
        let entries = view_entries(&record)
            .map_err(|_| s3_error!(NoSuchBucket, "The specified bucket does not exist."))?;
        Ok((record, entries))
    }

    fn view_entry<'a>(
        entries: &'a [ViewEntry],
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<&'a ViewEntry> {
        let entry = entries
            .iter()
            .find(|entry| entry.key == key)
            .ok_or_else(|| s3_error!(NoSuchKey, "The specified key does not exist."))?;
        match version_id {
            Some(version) if version != entry.captured.version_id.to_string() => Err(s3_error!(
                NoSuchVersion,
                "The specified version does not exist."
            )),
            _ => Ok(entry),
        }
    }

    fn view_modified(record: &JobRecord) -> Timestamp {
        let created =
            std::time::UNIX_EPOCH + std::time::Duration::from_millis(record.created_at_ms);
        Timestamp::from(created)
    }

    pub(super) async fn view_head_bucket(
        &self,
        view: InputView,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.view_record(view).await?;
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    pub(super) async fn view_list(
        &self,
        view: InputView,
        input: ListObjectsV2Input,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let (record, entries) = self.view_record(view).await?;
        let bucket = view_bucket(view.job_id);
        let prefix = input.prefix.clone().unwrap_or_default();
        let max_keys = match input.max_keys {
            Some(max_keys) => usize::try_from(max_keys)
                .map_err(|_| s3_error!(InvalidArgument, "max-keys must be non-negative"))?
                .min(MAX_VIEW_KEYS),
            None => MAX_VIEW_KEYS,
        };
        let after =
            match input.continuation_token.as_deref() {
                Some(token) => Some(decode_token(&bucket, token).ok_or_else(|| {
                    s3_error!(InvalidArgument, "The continuation token is invalid")
                })?),
                None => input.start_after.clone(),
            };
        // Entries are authorized before paging, so cursors and prefixes come from visible ones.
        let mut visible = Vec::with_capacity(entries.len());
        for entry in &entries {
            let shown = match entry.key.starts_with(&prefix) {
                false => false,
                true => match head_entry(&self.state, &record, entry, self.node_id).await {
                    Ok(()) => true,
                    Err(ViewError::Denied | ViewError::NotFound) => false,
                    Err(error) => return Err(view_error(error)),
                },
            };
            visible.push(shown);
        }
        let scope = (prefix.as_str(), input.delimiter.as_deref());
        let page = list_visible(&entries, &visible, scope, after.as_deref(), max_keys);
        let encode = |value: &str| match &input.encoding_type {
            Some(kind) if kind.as_str() == EncodingType::URL => {
                utf8_percent_encode(value, URL_ENCODE_SET).to_string()
            }
            _ => value.to_string(),
        };
        let mut contents = Vec::with_capacity(page.objects.len());
        for index in &page.objects {
            let entry = &entries[*index];
            contents.push(Object {
                key: Some(encode(&entry.key)),
                size: i64::try_from(entry.captured.bytes).ok(),
                e_tag: Some(ETag::Strong(entry.etag())),
                last_modified: Some(Self::view_modified(&record)),
                storage_class: Some(ObjectStorageClass::from_static(
                    ObjectStorageClass::STANDARD,
                )),
                ..Default::default()
            });
        }
        let prefixes: Vec<CommonPrefix> = page
            .prefixes
            .iter()
            .map(|prefix| CommonPrefix {
                prefix: Some(encode(prefix)),
            })
            .collect();
        let key_count = contents.len() + prefixes.len();
        let output = ListObjectsV2Output {
            name: Some(bucket.clone()),
            prefix: Some(encode(&prefix)),
            delimiter: input.delimiter.clone(),
            encoding_type: input.encoding_type.clone(),
            max_keys: i32::try_from(max_keys).ok(),
            key_count: i32::try_from(key_count).ok(),
            is_truncated: Some(page.next.is_some()),
            continuation_token: input.continuation_token.clone(),
            next_continuation_token: page.next.map(|next| encode_token(&bucket, &next)),
            start_after: input.start_after.clone(),
            contents: Some(contents),
            common_prefixes: Some(prefixes),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    pub(super) async fn view_head(
        &self,
        view: InputView,
        input: HeadObjectInput,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let (record, entries) = self.view_record(view).await?;
        let entry = Self::view_entry(&entries, &input.key, input.version_id.as_deref())?;
        if !matches(input.if_match.as_ref(), entry) {
            return Err(s3_error!(PreconditionFailed, "The ETag does not match"));
        }
        head_entry(&self.state, &record, entry, self.node_id)
            .await
            .map_err(view_error)?;
        Ok(S3Response::new(HeadObjectOutput {
            accept_ranges: Some("bytes".to_string()),
            content_length: i64::try_from(entry.captured.bytes).ok(),
            e_tag: Some(ETag::Strong(entry.etag())),
            last_modified: Some(Self::view_modified(&record)),
            version_id: Some(entry.captured.version_id.to_string()),
            ..Default::default()
        }))
    }

    pub(super) async fn view_get(
        &self,
        view: InputView,
        input: GetObjectInput,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let (record, entries) = self.view_record(view).await?;
        let entry = Self::view_entry(&entries, &input.key, input.version_id.as_deref())?;
        if !matches(input.if_match.as_ref(), entry) {
            return Err(s3_error!(PreconditionFailed, "The ETag does not match"));
        }
        let resolved = input
            .range
            .map(object_range_request)
            .map(|range| range.resolve(entry.captured.bytes))
            .transpose()
            .map_err(|error| match error {
                GetObjectError::InvalidRange => {
                    s3_error!(InvalidRange, "The requested range is not satisfiable")
                }
                _ => s3_error!(InvalidArgument, "The range is invalid"),
            })?;
        let range = resolved.as_ref().map(|resolved| resolved.range.clone());
        let blob = read_entry(&self.state, &record, entry, self.node_id, range)
            .await
            .map_err(view_error)?;
        let length = match &resolved {
            Some(resolved) => resolved.content_length,
            None => i64::try_from(entry.captured.bytes).unwrap_or(i64::MAX),
        };
        let output = GetObjectOutput {
            body: Some(StreamingBlob::wrap(blob)),
            accept_ranges: Some("bytes".to_string()),
            content_length: Some(length),
            content_range: resolved
                .as_ref()
                .map(|resolved| resolved.content_range.clone()),
            e_tag: Some(ETag::Strong(entry.etag())),
            last_modified: Some(Self::view_modified(&record)),
            version_id: Some(entry.captured.version_id.to_string()),
            ..Default::default()
        };
        Ok(match resolved.is_some() {
            true => S3Response::with_status(output, http::StatusCode::PARTIAL_CONTENT),
            false => S3Response::new(output),
        })
    }
}
