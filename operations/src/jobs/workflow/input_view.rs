//! Serves a run's mounted inputs as a read-only S3 bucket on the node that runs it. Each key
//! names the exact version the run captured, and every read is authorized at its source.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use aruna_core::NodeId;
use aruna_core::compute::CACHE_MANIFEST_KEY;
use aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE;
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::job::{
    CapturedInput, InputContent, InputMode, InputSelection, InputSource, JobId, JobPayload,
    JobRecord, credential_job_id,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures_util::future::BoxFuture;
use thiserror::Error;

use crate::driver::{DriverContext, drive};
use crate::jobs::key_wake::read_row;
use crate::jobs::source_read::{HEAD_OPERATION, READ_OPERATION, SourceReadError, authorize_read};
use crate::replication::bao_read::{BaoReadError, BaoReadOutput, managed_read};
use crate::replication::protocol::{BaoReadRefusal, BaoReadRequest, BaoReadTarget};
use crate::s3::object::get::{GetObjectInput, GetObjectOperation, ObjectRangeRequest};
use crate::s3::object::visible::SCAN_BUDGET;
use crate::staging::native_source::{body_stream, granted_read};

/// Keys one listing page returns at most, as S3 does.
pub const MAX_VIEW_KEYS: usize = 1000;

pub type ViewStream = BackendStream<Result<bytes::Bytes, aruna_core::stream::StreamError>>;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ViewError {
    #[error("the input view has no such key")]
    NotFound,
    #[error("the input read is denied")]
    Denied,
    #[error("the input view is unavailable: {0}")]
    Unavailable(String),
}

/// The read-only bucket a run's mounted inputs appear in.
pub fn view_bucket(job_id: JobId) -> String {
    format!("in-{}", job_id.to_string().to_lowercase())
}

/// The run a request reads the input view of: its own credential naming its own view bucket.
pub fn view_job(access_key: &str, bucket: &str) -> Option<JobId> {
    let job_id = credential_job_id(access_key)?;
    (view_bucket(job_id) == bucket).then_some(job_id)
}

/// Whether an input is mounted: a `Mount` input whose every ranged read binds its version.
pub fn mounted(input: &InputSelection, captured: &CapturedInput) -> bool {
    input.mode == InputMode::Mount
        && match &captured.content {
            InputContent::Blake3(_) => true,
            InputContent::Reference { range_bound, .. } => *range_bound,
        }
}

/// One input in the view, under its container path without the leading `/`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewEntry {
    pub key: String,
    pub input: InputSelection,
    pub captured: CapturedInput,
}

impl ViewEntry {
    /// The ETag value, unquoted: the version id names the bytes for the whole run.
    pub fn etag(&self) -> String {
        self.captured.version_id.to_string()
    }
}

/// The mounted inputs of one run in key order, while the run is not finished.
pub fn view_entries(record: &JobRecord) -> Result<Vec<ViewEntry>, ViewError> {
    if record.state.is_terminal() {
        return Err(ViewError::NotFound);
    }
    let JobPayload::Execution(spec) = &record.payload else {
        return Err(ViewError::NotFound);
    };
    let mut entries = Vec::new();
    for input in &spec.inputs {
        let Some(captured) = record
            .captured_inputs
            .iter()
            .find(|captured| captured.destination_key == input.dest_key)
        else {
            continue;
        };
        let Some(path) = input.container_path.as_deref() else {
            continue;
        };
        if mounted(input, captured) {
            entries.push(ViewEntry {
                key: path.trim_start_matches('/').to_string(),
                input: input.clone(),
                captured: captured.clone(),
            });
        }
    }
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(entries)
}

/// The keys of entries a worker cache may keep after the run: stored copies on this node with
/// no placement policy, from buckets that do not encrypt, under keys KubeDAL reads as written.
pub async fn cached_keys(
    context: &DriverContext,
    entries: &[ViewEntry],
    local: NodeId,
) -> Result<BTreeSet<String>, ViewError> {
    let mut keys = BTreeSet::new();
    if entries.iter().any(|entry| entry.key == CACHE_MANIFEST_KEY) {
        return Ok(keys);
    }
    let mut encrypted: BTreeMap<&str, bool> = BTreeMap::new();
    for entry in entries {
        let captured = &entry.captured;
        let candidate = captured.content.blake3().is_some()
            && captured.source_node_id == local
            && captured.policies.is_empty()
            && captured.link.is_none()
            && exact_key(&entry.key);
        if !candidate {
            continue;
        }
        let (bucket, _) = source_of(entry);
        let sealed = match encrypted.get(bucket) {
            Some(sealed) => *sealed,
            None => {
                let row = bucket.as_bytes().to_vec();
                let row = read_row(&context.storage_handle, BUCKET_ENCRYPTION_KEYSPACE, row)
                    .await
                    .map_err(ViewError::Unavailable)?;
                let settings = BucketEncryption::from_row(row.as_deref())
                    .map_err(|error| ViewError::Unavailable(error.to_string()))?;
                *encrypted.entry(bucket).or_insert(settings.is_encrypted())
            }
        };
        if !sealed {
            keys.insert(entry.key.clone());
        }
    }
    Ok(keys)
}

/// Whether storage reads `key` exactly as written: no white space at its ends, no empty, "."
/// or ".." parts. KubeDAL refuses a file list with any other key.
fn exact_key(key: &str) -> bool {
    key.trim() == key
        && key
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

/// Whether a view read of `key` is for the cache file list; an input at that key wins.
pub fn names_manifest(entries: &[ViewEntry], key: &str) -> bool {
    key == CACHE_MANIFEST_KEY && !entries.iter().any(|entry| entry.key == key)
}

/// The KubeDAL file list of a run's cached entries, once its creator may still read
/// (GetObject) each of them, since a worker cache serves them without another read here.
/// `None` when the run caches nothing.
pub async fn read_manifest(
    context: &DriverContext,
    record: &JobRecord,
    entries: &[ViewEntry],
    local: NodeId,
) -> Result<Option<Vec<u8>>, ViewError> {
    manifest_for(context, &creator(record, context), entries, local).await
}

async fn manifest_for(
    context: &DriverContext,
    auth: &AuthContext,
    entries: &[ViewEntry],
    local: NodeId,
) -> Result<Option<Vec<u8>>, ViewError> {
    let cached = cached_keys(context, entries, local).await?;
    if cached.is_empty() {
        return Ok(None);
    }
    // Cached entries are stored copies on this node, so they are authorized here.
    for entry in entries.iter().filter(|entry| cached.contains(&entry.key)) {
        authorize_read(context, auth, local, source_of(entry), READ_OPERATION)
            .await
            .map_err(source_error)?;
    }
    Ok(Some(cache_manifest(entries, &cached)))
}

/// The KubeDAL file list of the cached entries, each checked against its BLAKE3 and size.
pub fn cache_manifest(entries: &[ViewEntry], cached: &BTreeSet<String>) -> Vec<u8> {
    let files = entries
        .iter()
        .filter(|entry| cached.contains(&entry.key))
        .filter_map(|entry| {
            let blake3 = entry.captured.content.blake3()?;
            Some(serde_json::json!({
                "path":entry.key,
                "source":entry.key,
                "blake3":hex::encode(blake3),
                "size":entry.captured.bytes
            }))
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&serde_json::json!({"version":1,"files":files})).unwrap_or_default()
}

/// Where a view page resumes: after a marker, or among hidden entries after the last one
/// examined, inside the common prefix whose shown entry is still sought.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ViewFrom {
    After(String),
    Hidden { at: String, pending: Option<String> },
}

/// One ListObjectsV2 page: entry indices, common prefixes, and where the next page resumes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ViewListing {
    pub objects: Vec<usize>,
    pub prefixes: Vec<String>,
    pub next: Option<ViewFrom>,
}

/// Lists `entries` (in key order) below `prefix` from `from`, asking `shows` about each
/// candidate in turn, so nothing before the position or past the page is checked. A common
/// prefix shows once one of its entries does and counts once toward `max_keys`.
pub async fn list_shown<'s, E, F>(
    entries: &[ViewEntry],
    (prefix, delimiter): (&str, Option<&str>),
    from: Option<ViewFrom>,
    max_keys: usize,
    mut shows: F,
) -> Result<ViewListing, E>
where
    F: FnMut(&ViewEntry) -> BoxFuture<'s, Result<bool, E>>,
{
    let mut listing = ViewListing::default();
    let max_keys = max_keys.min(MAX_VIEW_KEYS);
    let mut budget = SCAN_BUDGET;
    let (after, marker, pending) = match from {
        None => (None, false, None),
        Some(ViewFrom::After(after)) => (Some(after), true, None),
        Some(ViewFrom::Hidden { at, pending }) => (Some(at), false, pending),
    };
    // The common prefix last met, and whether it showed.
    let mut current: Option<(String, bool)> = pending.map(|group| (group, false));
    let mut last: Option<String> = None;
    for (index, entry) in entries.iter().enumerate() {
        let Some(rest) = entry.key.strip_prefix(prefix) else {
            continue;
        };
        if after.as_deref().is_some_and(|after| {
            entry.key.as_str() <= after
                || (marker && is_prefix(after, delimiter) && entry.key.starts_with(after))
        }) {
            continue;
        }
        let group = delimiter
            .filter(|delimiter| !delimiter.is_empty())
            .and_then(|delimiter| rest.find(delimiter).map(|at| at + delimiter.len()))
            .map(|end| format!("{prefix}{}", &rest[..end]));
        if group.is_some()
            && current.as_ref().is_some_and(|(shown_group, shown)| {
                *shown && group.as_deref() == Some(shown_group.as_str())
            })
        {
            continue;
        }
        if listing.objects.len() + listing.prefixes.len() == max_keys {
            listing.next = last.map(ViewFrom::After);
            return Ok(listing);
        }
        let shown = shows(entry).await?;
        match (&group, shown) {
            (Some(group), true) => {
                listing.prefixes.push(group.clone());
                last = Some(group.clone());
            }
            (None, true) => {
                listing.objects.push(index);
                last = Some(entry.key.clone());
            }
            (_, false) => budget = budget.saturating_sub(1),
        }
        if let Some(group) = &group {
            current = Some((group.clone(), shown));
        }
        if budget == 0 {
            listing.next = Some(match &last {
                Some(last) => ViewFrom::After(last.clone()),
                None => ViewFrom::Hidden {
                    at: entry.key.clone(),
                    pending: group,
                },
            });
            return Ok(listing);
        }
    }
    Ok(listing)
}

fn is_prefix(marker: &str, delimiter: Option<&str>) -> bool {
    delimiter.is_some_and(|delimiter| !delimiter.is_empty() && marker.ends_with(delimiter))
}

/// An opaque continuation token bound to one view bucket.
pub fn encode_token(bucket: &str, marker: &str) -> String {
    URL_SAFE_NO_PAD.encode(format!("{bucket}\0{marker}"))
}

/// The marker a token of this view bucket names; a token of another bucket names none.
pub fn decode_token(bucket: &str, token: &str) -> Option<String> {
    let bytes = URL_SAFE_NO_PAD.decode(token).ok()?;
    let text = String::from_utf8(bytes).ok()?;
    let (owner, marker) = text.split_once('\0')?;
    (owner == bucket).then(|| marker.to_string())
}

/// The run creator as admitted: submissions refuse path restricted credentials.
fn creator(record: &JobRecord, context: &DriverContext) -> AuthContext {
    AuthContext {
        user_id: record.created_by,
        realm_id: context.serving_realm(record.created_by.realm_id),
        path_restrictions: None,
        session: None,
    }
}

fn source_of(entry: &ViewEntry) -> (&str, &str) {
    let InputSource::S3 { bucket, key, .. } = &entry.input.source;
    (bucket, key)
}

fn source_error(error: SourceReadError) -> ViewError {
    match error {
        SourceReadError::NotFound => ViewError::NotFound,
        SourceReadError::Denied => ViewError::Denied,
        SourceReadError::Unavailable(error) => ViewError::Unavailable(error),
    }
}

fn managed_error(error: BaoReadError) -> ViewError {
    match error {
        BaoReadError::Refused(BaoReadRefusal::ReadDenied) | BaoReadError::GovernedUnavailable => {
            ViewError::Denied
        }
        BaoReadError::Refused(BaoReadRefusal::NotFound) => ViewError::NotFound,
        other => ViewError::Unavailable(other.to_string()),
    }
}

fn granted_error(error: aruna_core::errors::StagingSourceError) -> ViewError {
    use aruna_core::errors::StagingSourceError;
    match error {
        StagingSourceError::AccessDenied => ViewError::Denied,
        StagingSourceError::NotFound => ViewError::NotFound,
        other => ViewError::Unavailable(other.to_string()),
    }
}

/// Checks the creator may still read one entry, at its source: here, or at its holder.
pub async fn head_entry(
    context: &DriverContext,
    record: &JobRecord,
    entry: &ViewEntry,
    local: NodeId,
) -> Result<(), ViewError> {
    let source = entry.captured.source_node_id;
    if source == local {
        let auth = creator(record, context);
        return authorize_read(context, &auth, local, source_of(entry), HEAD_OPERATION)
            .await
            .map(|_| ())
            .map_err(source_error);
    }
    match entry.captured.content.blake3() {
        Some(blake3) => {
            let request = managed_request(context, record, entry, Some(blake3), true, None);
            managed_read(context, source, request)
                .await
                .map(|_| ())
                .map_err(managed_error)
        }
        None => {
            let grant = entry.captured.grant.as_ref().ok_or(ViewError::Denied)?;
            granted_read(context, source, grant, None, true)
                .await
                .map(|_| ())
                .map_err(granted_error)
        }
    }
}

/// The bytes of one entry, or of `range` of it, at its exact version from its source.
pub async fn read_entry(
    context: &DriverContext,
    record: &JobRecord,
    entry: &ViewEntry,
    local: NodeId,
    range: Option<Range<u64>>,
) -> Result<ViewStream, ViewError> {
    let source = entry.captured.source_node_id;
    if source == local {
        let auth = creator(record, context);
        let group_id = authorize_read(context, &auth, local, source_of(entry), READ_OPERATION)
            .await
            .map_err(source_error)?;
        let (bucket, key) = source_of(entry);
        let get = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: Some(entry.captured.version_id),
                range: range.map(|range| ObjectRangeRequest::StartEnd {
                    start: range.start,
                    end: range.end.saturating_sub(1),
                }),
                group_id,
                user_identity: record.created_by,
                realm_id: auth.realm_id,
                node_id: local,
            }),
            context,
        )
        .await
        .map_err(|error| ViewError::Unavailable(error.to_string()))?;
        return Ok(get.blob);
    }
    match entry.captured.content.blake3() {
        Some(blake3) => {
            let request = managed_request(context, record, entry, Some(blake3), false, range);
            match managed_read(context, source, request).await {
                Ok(BaoReadOutput::Stream { blob, .. }) => Ok(blob),
                Ok(BaoReadOutput::Metadata { .. }) => Err(ViewError::Unavailable(
                    "the holder sent no bytes".to_string(),
                )),
                Err(error) => Err(managed_error(error)),
            }
        }
        None => {
            let grant = entry.captured.grant.as_ref().ok_or(ViewError::Denied)?;
            let (_, stream) = granted_read(context, source, grant, range, false)
                .await
                .map_err(granted_error)?;
            Ok(body_stream(stream))
        }
    }
}

/// A managed read of the exact captured version for the creator, as admitted.
fn managed_request(
    context: &DriverContext,
    record: &JobRecord,
    entry: &ViewEntry,
    blake3: Option<[u8; 32]>,
    metadata_only: bool,
    range: Option<Range<u64>>,
) -> BaoReadRequest {
    let auth = creator(record, context);
    let (bucket, key) = source_of(entry);
    BaoReadRequest {
        realm_id: auth.realm_id,
        target: BaoReadTarget::ExactVersion(VersionedObjectArn {
            realm_id: auth.realm_id,
            node_id: entry.captured.source_node_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            version: entry.captured.version_id,
        }),
        auth_context: auth,
        expected_blake3: blake3,
        metadata_only,
        destination: None,
        known_refs: Vec::new(),
        range,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    fn entry(key: &str) -> ViewEntry {
        ViewEntry {
            key: key.to_string(),
            input: InputSelection {
                source: InputSource::S3 {
                    bucket: "b".to_string(),
                    key: key.to_string(),
                    version_id: None,
                },
                source_node_id: None,
                dest_key: key.to_string(),
                mode: InputMode::Mount,
                container_path: Some(format!("/{key}")),
                name: None,
                description: None,
            },
            captured: CapturedInput {
                destination_key: key.to_string(),
                source_node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
                version_id: Ulid::from_parts(1, 1),
                content: InputContent::Blake3([1; 32]),
                bytes: 1,
                policies: Vec::new(),
                source_group_id: Ulid::from_parts(2, 2),
                public: false,
                link: None,
                source_pin: None,
                database: None,
                releases: Vec::new(),
                releases_complete: false,
                grant: None,
                served_by: None,
            },
        }
    }

    fn entries() -> Vec<ViewEntry> {
        [
            "db/a.dmnd",
            "db/b.sqlite",
            "db/sub/c",
            "db/sub/d",
            "genome.fna",
        ]
        .into_iter()
        .map(entry)
        .collect()
    }

    fn keys(entries: &[ViewEntry], listing: &ViewListing) -> Vec<String> {
        listing
            .objects
            .iter()
            .map(|index| entries[*index].key.clone())
            .collect()
    }

    /// One page of `entries`, asking `shows` which ones the caller may see.
    async fn page(
        entries: &[ViewEntry],
        scope: (&str, Option<&str>),
        from: Option<ViewFrom>,
        max_keys: usize,
        shows: impl Fn(&ViewEntry) -> bool,
    ) -> ViewListing {
        let shows = |entry: &ViewEntry| -> BoxFuture<'static, Result<bool, ()>> {
            let shown = shows(entry);
            Box::pin(async move { Ok(shown) })
        };
        list_shown(entries, scope, from, max_keys, shows)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn pages_across_prefixes() {
        // One key per page walks every key and common prefix exactly once, in order.
        let entries = entries();
        let mut seen = Vec::new();
        let mut from: Option<ViewFrom> = None;
        loop {
            let listed = page(&entries, ("db/", Some("/")), from.take(), 1, |_| true).await;
            assert!(listed.objects.len() + listed.prefixes.len() <= 1);
            seen.extend(keys(&entries, &listed));
            seen.extend(listed.prefixes.clone());
            match listed.next {
                Some(next) => from = Some(next),
                None => break,
            }
        }
        assert_eq!(seen, ["db/a.dmnd", "db/b.sqlite", "db/sub/"]);
        let flat = page(&entries, ("", None), None, 2, |_| true).await;
        assert_eq!(keys(&entries, &flat), ["db/a.dmnd", "db/b.sqlite"]);
        let after = Some(ViewFrom::After("db/b.sqlite".to_string()));
        assert_eq!(flat.next, after);
        let rest = page(&entries, ("", None), after, 1000, |_| true).await;
        assert_eq!(
            keys(&entries, &rest),
            ["db/sub/c", "db/sub/d", "genome.fna"]
        );
        assert!(rest.next.is_none());
    }

    #[tokio::test]
    async fn groups_root_prefixes() {
        // At the root one prefix stands for all its keys, and a marker resumes after it.
        let entries = entries();
        let root = page(&entries, ("", Some("/")), None, 1000, |_| true).await;
        assert_eq!(root.prefixes, ["db/"]);
        assert_eq!(keys(&entries, &root), ["genome.fna"]);
        let first = page(&entries, ("", Some("/")), None, 1, |_| true).await;
        assert_eq!(first.prefixes, ["db/"]);
        assert_eq!(first.next, Some(ViewFrom::After("db/".to_string())));
        let second = page(&entries, ("", Some("/")), first.next, 1, |_| true).await;
        assert_eq!(keys(&entries, &second), ["genome.fna"]);
        assert!(second.next.is_none());
    }

    #[tokio::test]
    async fn denied_stays_unlisted() {
        // A denied entry is never a key, a prefix or a cursor, on any page.
        let entries = entries();
        let denied = ["db/b.sqlite", "db/sub/c", "db/sub/d"];
        let shows = |entry: &ViewEntry| !denied.contains(&entry.key.as_str());
        for (prefix, expected) in [("", vec!["db/", "genome.fna"]), ("db/", vec!["db/a.dmnd"])] {
            let mut seen = Vec::new();
            let mut from: Option<ViewFrom> = None;
            loop {
                let listed = page(&entries, (prefix, Some("/")), from.take(), 1, shows).await;
                seen.extend(keys(&entries, &listed));
                seen.extend(listed.prefixes.clone());
                let Some(next) = listed.next else { break };
                if let ViewFrom::After(next) = &next {
                    assert!(!denied.iter().any(|key| next.starts_with(key)), "{next}");
                }
                from = Some(next);
            }
            assert_eq!(seen, expected);
        }
    }

    #[tokio::test]
    async fn authorizes_from_position() {
        // A page checks only entries after its position and stops at the first it can return;
        // past the scan budget of hidden entries it resumes from a hidden position.
        let entries = entries();
        let mut checked = Vec::new();
        let from = Some(ViewFrom::After("db/b.sqlite".to_string()));
        let shows = |entry: &ViewEntry| -> BoxFuture<'static, Result<bool, ()>> {
            checked.push(entry.key.clone());
            Box::pin(async { Ok(true) })
        };
        let listed = list_shown(&entries, ("", None), from, 1, shows)
            .await
            .unwrap();
        assert_eq!(keys(&entries, &listed), ["db/sub/c"]);
        assert_eq!(checked, ["db/sub/c"]);
        let many: Vec<ViewEntry> = (0..=SCAN_BUDGET)
            .map(|index| entry(&format!("h/{index:05}")))
            .chain([entry("z.txt")])
            .collect();
        let shows = |entry: &ViewEntry| entry.key == "z.txt";
        let first = page(&many, ("", Some("/")), None, 10, shows).await;
        assert!(first.objects.is_empty() && first.prefixes.is_empty());
        let Some(ViewFrom::Hidden { pending, .. }) = &first.next else {
            panic!("a hidden position: {:?}", first.next);
        };
        assert_eq!(pending.as_deref(), Some("h/"));
        let second = page(&many, ("", Some("/")), first.next, 10, shows).await;
        assert_eq!(keys(&many, &second), ["z.txt"]);
        assert!(second.prefixes.is_empty() && second.next.is_none());
    }

    #[test]
    fn binds_tokens_and_views() {
        // A token and a view bucket belong to one run only.
        let token = encode_token("in-a", "db/sub/");
        assert_eq!(decode_token("in-a", &token).as_deref(), Some("db/sub/"));
        assert_eq!(decode_token("in-b", &token), None);
        let job = JobId::from_bytes([3; 16]);
        let access = aruna_core::structs::execution::job::workspace_credential_id(job);
        assert_eq!(view_job(&access, &view_bucket(job)), Some(job));
        assert_eq!(view_job(&access, "in-other"), None);
        assert_eq!(view_job("AKIAOTHER", &view_bucket(job)), None);
    }

    #[tokio::test]
    async fn caches_plain_local_copies() {
        use aruna_core::effects::StorageEffect;
        use aruna_core::structs::storage::encryption::EncryptionMode;

        let dir = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let locked = BucketEncryption {
            mode: EncryptionMode::NodeManaged,
            bucket_id: Some(Ulid::from_parts(3, 3)),
            key_generation: 1,
            ..Default::default()
        };
        let effect = StorageEffect::Write {
            key_space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            key: b"locked".to_vec().into(),
            value: locked.to_bytes().unwrap().into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(effect).await;
        let local = entry("a").captured.source_node_id;
        let mut sealed = entry("sealed");
        sealed.input.source = InputSource::S3 {
            bucket: "locked".to_string(),
            key: "sealed".to_string(),
            version_id: None,
        };
        let mut remote = entry("remote");
        remote.captured.source_node_id = iroh::SecretKey::from_bytes(&[9; 32]).public();
        let mut reference = entry("reference");
        reference.captured.content = InputContent::Reference {
            etag: None,
            range_bound: true,
        };
        let entries = [entry("a"), sealed, remote, reference, entry("b ")];

        let keys = cached_keys(&context, &entries, local).await.unwrap();
        assert_eq!(keys, BTreeSet::from(["a".to_string()]));
        let manifest: serde_json::Value =
            serde_json::from_slice(&cache_manifest(&entries, &keys)).unwrap();
        let file = serde_json::json!({
            "path":"a","source":"a","blake3":hex::encode([1; 32]),"size":1
        });
        assert_eq!(manifest, serde_json::json!({"version":1,"files":[file]}));

        // An input at the manifest key turns the cache off and is read as that input.
        let taken = [entry("a"), entry(CACHE_MANIFEST_KEY)];
        assert!(
            cached_keys(&context, &taken, local)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(!names_manifest(&taken, CACHE_MANIFEST_KEY));
        assert!(names_manifest(&entries, CACHE_MANIFEST_KEY));
        assert!(!names_manifest(&entries, "a"));
    }

    #[tokio::test]
    async fn manifest_needs_object_reads() {
        // A rule that denies only GetObject keeps an input out of the file list.
        use aruna_core::effects::StorageEffect;
        use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
        use aruna_core::request_policy::{PolicyKind, RequestPolicy};
        use aruna_core::structs::storage::blob::BucketInfo;
        use aruna_core::structs::storage::format::Compression;

        let (_directory, storage) = crate::tests::s3::test_storage();
        let context = crate::tests::s3::test_context(storage);
        let realm_id = aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]);
        let group_id = Ulid::from_parts(2, 2);
        let local = entry("a").captured.source_node_id;
        let policy = RequestPolicy {
            policy_id: Ulid::from_parts(3, 3),
            name: "no-object-reads".to_string(),
            kind: PolicyKind::Deny,
            when: None,
            expression: "operation == 's3.GetObject' && path.endsWith('/blocked')".to_string(),
            enabled: true,
        };
        let scope = (realm_id, group_id, local);
        let owner =
            crate::tests::access::seed_open_access(&context, scope, "none/**", None, vec![policy])
                .await;
        let info = BucketInfo {
            group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let effect = StorageEffect::Write {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: b"b".to_vec().into(),
            value: info.to_bytes().unwrap().into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(effect).await;

        let open = [entry("a")];
        let manifest = manifest_for(&context, &owner, &open, local).await.unwrap();
        assert!(manifest.is_some());
        let blocked = [entry("a"), entry("blocked")];
        let head = authorize_read(&context, &owner, local, ("b", "blocked"), HEAD_OPERATION);
        assert!(head.await.is_ok());
        let denied = manifest_for(&context, &owner, &blocked, local).await;
        assert_eq!(denied, Err(ViewError::Denied));
    }

    #[test]
    fn mounts_bound_reads() {
        // Only a mounted input whose ranges bind its version enters the view.
        let mut bound = entry("a");
        assert!(mounted(&bound.input, &bound.captured));
        bound.captured.content = InputContent::Reference {
            etag: None,
            range_bound: false,
        };
        assert!(!mounted(&bound.input, &bound.captured));
        let mut staged = entry("b");
        staged.input.mode = InputMode::Snapshot;
        assert!(!mounted(&staged.input, &staged.captured));
    }
}
