//! Serves a run's mounted inputs as a read-only S3 bucket on the node that runs it. Each key
//! names the exact version the run captured, and every read is authorized at its source.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::CACHE_MANIFEST_KEY;
use aruna_core::cursor;
use aruna_core::errors::StagingSourceError;
use aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE;
use aruna_core::native_reference::GrantedBody;
use aruna_core::operation::Operation;
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::job::{
    CapturedInput, InputContent, InputMode, InputSelection, InputSource, JobId, JobPayload,
    JobRecord, credential_job_id,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::types::{GroupId, Key};
use thiserror::Error;

use crate::jobs::read_access::SourceReadError;
use crate::jobs::read_access::SourceReadOperation;
use crate::replication::bao_read::{BaoReadError, BaoReadOutput};
use crate::replication::managed_read::ManagedReadOperation;
use crate::replication::protocol::{BaoReadRefusal, BaoReadRequest, BaoReadTarget};
use crate::s3::object::get::{
    GetObjectError, GetObjectInput, GetObjectOperation, GetObjectResult, ObjectRangeRequest,
};
use crate::s3::object::visible::SCAN_BUDGET;
use crate::staging::granted_read::GrantedReadOperation;
use aruna_core::structs::storage::read_grant::{HEAD_OPERATION, READ_OPERATION};

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

impl From<IoError> for ViewError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
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

/// Whether a worker cache may keep `entry`, unless its bucket encrypts: a stored copy on this
/// node with no placement policy, under a key KubeDAL reads as written.
fn cache_candidate(entry: &ViewEntry, local: NodeId) -> bool {
    let captured = &entry.captured;
    captured.content.blake3().is_some()
        && captured.source_node_id == local
        && captured.policies.is_empty()
        && captured.link.is_none()
        && exact_key(&entry.key)
}

/// Finds the keys of entries a worker cache may keep after the run: the candidates whose source
/// bucket does not encrypt. The encryption rows of those buckets are read together.
#[derive(Debug, PartialEq)]
pub struct CachedKeysOperation {
    entries: Vec<ViewEntry>,
    local: NodeId,
    buckets: Vec<String>,
}

impl CachedKeysOperation {
    pub fn new(entries: Vec<ViewEntry>, local: NodeId) -> Self {
        Self {
            entries,
            local,
            buckets: Vec::new(),
        }
    }
}

impl Operation for CachedKeysOperation {
    type Output = BTreeSet<String>;
    type Error = ViewError;

    fn first(&mut self) -> Result<Next<Self::Output>, ViewError> {
        if self
            .entries
            .iter()
            .any(|entry| entry.key == CACHE_MANIFEST_KEY)
        {
            return Ok(Next::Done(BTreeSet::new()));
        }
        let buckets: BTreeSet<&str> = (self.entries.iter())
            .filter(|entry| cache_candidate(entry, self.local))
            .map(|entry| source_of(entry).0)
            .collect();
        self.buckets = buckets.into_iter().map(str::to_string).collect();
        if self.buckets.is_empty() {
            return Ok(Next::Done(BTreeSet::new()));
        }
        let rows = (self.buckets.iter())
            .map(|bucket| {
                let key = Key::from(bucket.as_bytes().to_vec());
                (BUCKET_ENCRYPTION_KEYSPACE.to_string(), key)
            })
            .collect();
        Ok(Next::Call(IoCall::Read(rows)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ViewError> {
        let mut encrypted = BTreeMap::new();
        for (bucket, row) in self.buckets.iter().zip(result.values()?) {
            let settings = BucketEncryption::from_row(row.as_deref())
                .map_err(|error| ViewError::Unavailable(error.to_string()))?;
            encrypted.insert(bucket.as_str(), settings.is_encrypted());
        }
        let keys = (self.entries.iter())
            .filter(|entry| cache_candidate(entry, self.local))
            .filter(|entry| encrypted.get(source_of(entry).0) == Some(&false))
            .map(|entry| entry.key.clone())
            .collect();
        Ok(Next::Done(keys))
    }
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

#[derive(Debug, PartialEq)]
enum ManifestStep {
    Cached,
    Reads {
        cached: BTreeSet<String>,
        checked: usize,
    },
}

/// Builds the KubeDAL file list of a run's cached entries, once its creator may still read
/// (GetObject) each of them, since a worker cache serves them without another read here.
/// `None` when the run caches nothing.
#[derive(Debug, PartialEq)]
pub struct ReadManifestOperation {
    auth: AuthContext,
    entries: Vec<ViewEntry>,
    local: NodeId,
    step: ManifestStep,
}

impl ReadManifestOperation {
    pub fn new(auth: AuthContext, entries: Vec<ViewEntry>, local: NodeId) -> Self {
        Self {
            auth,
            entries,
            local,
            step: ManifestStep::Cached,
        }
    }

    /// Checks the next cached entry after the first `checked`, or ends with the file list.
    /// Cached entries are stored copies on this node, so they are authorized here.
    fn advance(
        &mut self,
        cached: BTreeSet<String>,
        checked: usize,
    ) -> Result<Next<Option<Vec<u8>>>, ViewError> {
        let found = (self.entries.iter().enumerate().skip(checked))
            .find(|(_, entry)| cached.contains(&entry.key));
        let Some((index, entry)) = found else {
            return Ok(Next::Done(Some(cache_manifest(&self.entries, &cached))));
        };
        let read = SourceReadOperation::of_object(
            &self.auth,
            self.local,
            source_of(entry),
            READ_OPERATION,
        );
        self.step = ManifestStep::Reads {
            cached,
            checked: index + 1,
        };
        Ok(Next::Call(IoCall::child(read)))
    }
}

impl Operation for ReadManifestOperation {
    type Output = Option<Vec<u8>>;
    type Error = ViewError;

    fn first(&mut self) -> Result<Next<Self::Output>, ViewError> {
        let keys = CachedKeysOperation::new(self.entries.clone(), self.local);
        Ok(Next::Call(IoCall::child(keys)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ViewError> {
        match std::mem::replace(&mut self.step, ManifestStep::Cached) {
            ManifestStep::Cached => {
                let cached = result.child::<BTreeSet<String>, ViewError>()??;
                match cached.is_empty() {
                    true => Ok(Next::Done(None)),
                    false => self.advance(cached, 0),
                }
            }
            ManifestStep::Reads { cached, checked } => {
                (result.child::<GroupId, SourceReadError>()?).map_err(source_error)?;
                self.advance(cached, checked)
            }
        }
    }
}

/// The KubeDAL file list of the cached entries, each checked against its BLAKE3 and size.
fn cache_manifest(entries: &[ViewEntry], cached: &BTreeSet<String>) -> Vec<u8> {
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

/// Lists `entries` (in key order) below a prefix from a position; the caller checks each
/// candidate, and nothing before the position or past the page is checked. A common prefix
/// shows once one of its entries does and counts once toward `max_keys`.
#[derive(Debug)]
pub struct ViewLister<'a> {
    entries: &'a [ViewEntry],
    prefix: &'a str,
    delimiter: Option<&'a str>,
    max_keys: usize,
    budget: usize,
    after: Option<String>,
    marker: bool,
    /// The common prefix last met, and whether it showed.
    current: Option<(String, bool)>,
    last: Option<String>,
    index: usize,
    /// The common prefix of the candidate waiting for its answer.
    group: Option<String>,
    listing: ViewListing,
    finished: bool,
}

impl<'a> ViewLister<'a> {
    pub fn new(
        entries: &'a [ViewEntry],
        (prefix, delimiter): (&'a str, Option<&'a str>),
        from: Option<ViewFrom>,
        max_keys: usize,
    ) -> Self {
        let (after, marker, pending) = match from {
            None => (None, false, None),
            Some(ViewFrom::After(after)) => (Some(after), true, None),
            Some(ViewFrom::Hidden { at, pending }) => (Some(at), false, pending),
        };
        Self {
            entries,
            prefix,
            delimiter,
            max_keys: max_keys.min(MAX_VIEW_KEYS),
            budget: SCAN_BUDGET,
            after,
            marker,
            current: pending.map(|group| (group, false)),
            last: None,
            index: 0,
            group: None,
            listing: ViewListing::default(),
            finished: false,
        }
    }

    /// The next entry to check, or `None` once the page is complete.
    pub fn candidate(&mut self) -> Option<&'a ViewEntry> {
        if self.finished {
            return None;
        }
        let entries = self.entries;
        while let Some(entry) = entries.get(self.index) {
            let Some(rest) = entry.key.strip_prefix(self.prefix) else {
                self.index += 1;
                continue;
            };
            if self.after.as_deref().is_some_and(|after| {
                entry.key.as_str() <= after
                    || (self.marker
                        && is_prefix(after, self.delimiter)
                        && entry.key.starts_with(after))
            }) {
                self.index += 1;
                continue;
            }
            let group = (self.delimiter)
                .filter(|delimiter| !delimiter.is_empty())
                .and_then(|delimiter| rest.find(delimiter).map(|at| at + delimiter.len()))
                .map(|end| format!("{}{}", self.prefix, &rest[..end]));
            if group.is_some()
                && self.current.as_ref().is_some_and(|(shown_group, shown)| {
                    *shown && group.as_deref() == Some(shown_group.as_str())
                })
            {
                self.index += 1;
                continue;
            }
            if self.listing.objects.len() + self.listing.prefixes.len() == self.max_keys {
                self.listing.next = self.last.clone().map(ViewFrom::After);
                self.finished = true;
                return None;
            }
            self.group = group;
            return Some(entry);
        }
        self.finished = true;
        None
    }

    /// Takes the verdict on the entry [`candidate`](Self::candidate) named last.
    pub fn answer(&mut self, shown: bool) {
        let Some(entry) = self.entries.get(self.index) else {
            return;
        };
        let group = self.group.take();
        match (&group, shown) {
            (Some(group), true) => {
                self.listing.prefixes.push(group.clone());
                self.last = Some(group.clone());
            }
            (None, true) => {
                self.listing.objects.push(self.index);
                self.last = Some(entry.key.clone());
            }
            (_, false) => self.budget = self.budget.saturating_sub(1),
        }
        if let Some(group) = &group {
            self.current = Some((group.clone(), shown));
        }
        self.index += 1;
        if self.budget == 0 {
            self.listing.next = Some(match &self.last {
                Some(last) => ViewFrom::After(last.clone()),
                None => ViewFrom::Hidden {
                    at: entry.key.clone(),
                    pending: group,
                },
            });
            self.finished = true;
        }
    }

    /// The page found so far; complete once `candidate` answered `None`.
    pub fn finish(self) -> ViewListing {
        self.listing
    }
}

fn is_prefix(marker: &str, delimiter: Option<&str>) -> bool {
    delimiter.is_some_and(|delimiter| !delimiter.is_empty() && marker.ends_with(delimiter))
}

/// An opaque continuation token bound to one view bucket.
pub fn encode_token(bucket: &str, marker: &str) -> String {
    cursor::encode(format!("{bucket}\0{marker}"))
}

/// The marker a token of this view bucket names; a token of another bucket names none.
pub fn decode_token(bucket: &str, token: &str) -> Option<String> {
    let bytes = cursor::decode(token).ok()?;
    let text = String::from_utf8(bytes).ok()?;
    let (owner, marker) = text.split_once('\0')?;
    (owner == bucket).then(|| marker.to_string())
}

/// The run creator as admitted: submissions refuse path restricted credentials.
pub fn creator(record: &JobRecord, realm_id: RealmId) -> AuthContext {
    AuthContext {
        user_id: record.created_by,
        realm_id,
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

fn granted_error(error: StagingSourceError) -> ViewError {
    match error {
        StagingSourceError::AccessDenied => ViewError::Denied,
        StagingSourceError::NotFound => ViewError::NotFound,
        other => ViewError::Unavailable(other.to_string()),
    }
}

#[derive(Debug, PartialEq)]
enum EntryStep {
    Authorize,
    Read,
    Managed,
    Granted,
}

/// Reads one entry at its exact version from its source, here or at its holder, for the run's
/// creator. A head only checks the creator may still read it and answers no bytes.
#[derive(Debug, PartialEq)]
pub struct ViewEntryOperation {
    auth: AuthContext,
    entry: ViewEntry,
    local: NodeId,
    head: bool,
    range: Option<Range<u64>>,
    step: EntryStep,
}

type EntryNext = Result<Next<Option<ViewStream>>, ViewError>;

impl ViewEntryOperation {
    pub fn head(auth: AuthContext, entry: ViewEntry, local: NodeId) -> Self {
        Self::new(auth, entry, local, true, None)
    }

    /// The bytes of the entry, or of `range` of it.
    pub fn read(
        auth: AuthContext,
        entry: ViewEntry,
        local: NodeId,
        range: Option<Range<u64>>,
    ) -> Self {
        Self::new(auth, entry, local, false, range)
    }

    fn new(
        auth: AuthContext,
        entry: ViewEntry,
        local: NodeId,
        head: bool,
        range: Option<Range<u64>>,
    ) -> Self {
        Self {
            auth,
            entry,
            local,
            head,
            range,
            step: EntryStep::Authorize,
        }
    }

    /// A managed read of the exact captured version for the creator, as admitted.
    fn managed_request(&self, blake3: [u8; 32]) -> BaoReadRequest {
        let (bucket, key) = source_of(&self.entry);
        BaoReadRequest {
            realm_id: self.auth.realm_id,
            target: BaoReadTarget::ExactVersion(VersionedObjectArn {
                realm_id: self.auth.realm_id,
                node_id: self.entry.captured.source_node_id,
                bucket: bucket.to_string(),
                key: key.to_string(),
                version: self.entry.captured.version_id,
            }),
            auth_context: self.auth.clone(),
            expected_blake3: Some(blake3),
            metadata_only: self.head,
            destination: None,
            known_refs: Vec::new(),
            range: self.range.clone(),
        }
    }
}

impl Operation for ViewEntryOperation {
    type Output = Option<ViewStream>;
    type Error = ViewError;

    fn first(&mut self) -> EntryNext {
        let source = self.entry.captured.source_node_id;
        if source == self.local {
            let operation = match self.head {
                true => HEAD_OPERATION,
                false => READ_OPERATION,
            };
            let object = source_of(&self.entry);
            let read = SourceReadOperation::of_object(&self.auth, self.local, object, operation);
            return Ok(Next::Call(IoCall::child(read)));
        }
        if let Some(blake3) = self.entry.captured.content.blake3() {
            self.step = EntryStep::Managed;
            let read = ManagedReadOperation::new(source, self.managed_request(blake3));
            return Ok(Next::Call(IoCall::child(read)));
        }
        let grant = self
            .entry
            .captured
            .grant
            .as_ref()
            .ok_or(ViewError::Denied)?;
        let ids = Some((self.local, self.auth.realm_id));
        let read = GrantedReadOperation::new(ids, source, grant, self.range.clone(), self.head);
        self.step = EntryStep::Granted;
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> EntryNext {
        match self.step {
            EntryStep::Authorize => {
                let group_id =
                    (result.child::<GroupId, SourceReadError>()?).map_err(source_error)?;
                if self.head {
                    return Ok(Next::Done(None));
                }
                let (bucket, key) = source_of(&self.entry);
                let get = GetObjectOperation::new(GetObjectInput {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    version_id: Some(self.entry.captured.version_id),
                    range: self
                        .range
                        .clone()
                        .map(|range| ObjectRangeRequest::StartEnd {
                            start: range.start,
                            end: range.end.saturating_sub(1),
                        }),
                    group_id,
                    user_identity: self.auth.user_id,
                    realm_id: self.auth.realm_id,
                    node_id: self.local,
                });
                self.step = EntryStep::Read;
                Ok(Next::Call(IoCall::child(get)))
            }
            EntryStep::Read => {
                let get = (result.child::<GetObjectResult, GetObjectError>()?)
                    .map_err(|error| ViewError::Unavailable(error.to_string()))?;
                Ok(Next::Done(Some(get.blob)))
            }
            EntryStep::Managed => {
                let read =
                    (result.child::<BaoReadOutput, BaoReadError>()?).map_err(managed_error)?;
                match (read, self.head) {
                    (_, true) => Ok(Next::Done(None)),
                    (BaoReadOutput::Stream { blob, .. }, false) => Ok(Next::Done(Some(blob))),
                    (BaoReadOutput::Metadata { .. }, false) => Err(ViewError::Unavailable(
                        "the holder sent no bytes".to_string(),
                    )),
                }
            }
            EntryStep::Granted => {
                let body =
                    (result.child::<GrantedBody, StagingSourceError>()?).map_err(granted_error)?;
                Ok(Next::Done((!self.head).then_some(body.blob)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::storage::encryption::EncryptionMode;
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
    fn page(
        entries: &[ViewEntry],
        scope: (&str, Option<&str>),
        from: Option<ViewFrom>,
        max_keys: usize,
        mut shows: impl FnMut(&ViewEntry) -> bool,
    ) -> ViewListing {
        let mut lister = ViewLister::new(entries, scope, from, max_keys);
        while let Some(entry) = lister.candidate() {
            lister.answer(shows(entry));
        }
        lister.finish()
    }

    #[test]
    fn pages_across_prefixes() {
        // One key per page walks every key and common prefix exactly once, in order.
        let entries = entries();
        let mut seen = Vec::new();
        let mut from: Option<ViewFrom> = None;
        loop {
            let listed = page(&entries, ("db/", Some("/")), from.take(), 1, |_| true);
            assert!(listed.objects.len() + listed.prefixes.len() <= 1);
            seen.extend(keys(&entries, &listed));
            seen.extend(listed.prefixes.clone());
            match listed.next {
                Some(next) => from = Some(next),
                None => break,
            }
        }
        assert_eq!(seen, ["db/a.dmnd", "db/b.sqlite", "db/sub/"]);
        let flat = page(&entries, ("", None), None, 2, |_| true);
        assert_eq!(keys(&entries, &flat), ["db/a.dmnd", "db/b.sqlite"]);
        let after = Some(ViewFrom::After("db/b.sqlite".to_string()));
        assert_eq!(flat.next, after);
        let rest = page(&entries, ("", None), after, 1000, |_| true);
        assert_eq!(
            keys(&entries, &rest),
            ["db/sub/c", "db/sub/d", "genome.fna"]
        );
        assert!(rest.next.is_none());
    }

    #[test]
    fn groups_root_prefixes() {
        // At the root one prefix stands for all its keys, and a marker resumes after it.
        let entries = entries();
        let root = page(&entries, ("", Some("/")), None, 1000, |_| true);
        assert_eq!(root.prefixes, ["db/"]);
        assert_eq!(keys(&entries, &root), ["genome.fna"]);
        let first = page(&entries, ("", Some("/")), None, 1, |_| true);
        assert_eq!(first.prefixes, ["db/"]);
        assert_eq!(first.next, Some(ViewFrom::After("db/".to_string())));
        let second = page(&entries, ("", Some("/")), first.next, 1, |_| true);
        assert_eq!(keys(&entries, &second), ["genome.fna"]);
        assert!(second.next.is_none());
    }

    #[test]
    fn denied_stays_unlisted() {
        // A denied entry is never a key, a prefix or a cursor, on any page.
        let entries = entries();
        let denied = ["db/b.sqlite", "db/sub/c", "db/sub/d"];
        let shows = |entry: &ViewEntry| !denied.contains(&entry.key.as_str());
        for (prefix, expected) in [("", vec!["db/", "genome.fna"]), ("db/", vec!["db/a.dmnd"])] {
            let mut seen = Vec::new();
            let mut from: Option<ViewFrom> = None;
            loop {
                let listed = page(&entries, (prefix, Some("/")), from.take(), 1, shows);
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

    #[test]
    fn authorizes_from_position() {
        // A page checks only entries after its position and stops at the first it can return;
        // past the scan budget of hidden entries it resumes from a hidden position.
        let entries = entries();
        let mut checked = Vec::new();
        let from = Some(ViewFrom::After("db/b.sqlite".to_string()));
        let listed = page(&entries, ("", None), from, 1, |entry| {
            checked.push(entry.key.clone());
            true
        });
        assert_eq!(keys(&entries, &listed), ["db/sub/c"]);
        assert_eq!(checked, ["db/sub/c"]);
        let many: Vec<ViewEntry> = (0..=SCAN_BUDGET)
            .map(|index| entry(&format!("h/{index:05}")))
            .chain([entry("z.txt")])
            .collect();
        let shows = |entry: &ViewEntry| entry.key == "z.txt";
        let first = page(&many, ("", Some("/")), None, 10, shows);
        assert!(first.objects.is_empty() && first.prefixes.is_empty());
        let Some(ViewFrom::Hidden { pending, .. }) = &first.next else {
            panic!("a hidden position: {:?}", first.next);
        };
        assert_eq!(pending.as_deref(), Some("h/"));
        let second = page(&many, ("", Some("/")), first.next, 10, shows);
        assert_eq!(keys(&many, &second), ["z.txt"]);
        assert!(second.prefixes.is_empty() && second.next.is_none());
    }

    #[test]
    fn tokens_and_views() {
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

    fn encryption_row(mode: EncryptionMode) -> Option<aruna_core::types::Value> {
        let settings = BucketEncryption {
            mode,
            bucket_id: Some(Ulid::from_parts(3, 3)),
            key_generation: 1,
            ..Default::default()
        };
        Some(settings.to_bytes().unwrap().into())
    }

    fn in_bucket(mut entry: ViewEntry, bucket: &str) -> ViewEntry {
        let key = entry.key.clone();
        entry.input.source = InputSource::S3 {
            bucket: bucket.to_string(),
            key,
            version_id: None,
        };
        entry
    }

    #[test]
    fn caches_local_copies() {
        let local = entry("a").captured.source_node_id;
        let mut remote = entry("remote");
        remote.captured.source_node_id = iroh::SecretKey::from_bytes(&[9; 32]).public();
        let mut reference = entry("reference");
        reference.captured.content = InputContent::Reference {
            etag: None,
            range_bound: true,
        };
        let sealed = in_bucket(entry("sealed"), "locked");
        let entries = vec![entry("a"), sealed, remote, reference, entry("b ")];

        // One read asks for the encryption of each candidate's bucket, in bucket order.
        let mut keys = CachedKeysOperation::new(entries.clone(), local);
        let rows = |bucket: &str| {
            (
                BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                Key::from(bucket.as_bytes().to_vec()),
            )
        };
        assert_eq!(
            keys.first(),
            Ok(Next::Call(IoCall::Read(vec![rows("b"), rows("locked")])))
        );
        let answer = IoResult::Values(vec![None, encryption_row(EncryptionMode::NodeManaged)]);
        let cached = BTreeSet::from(["a".to_string()]);
        assert_eq!(keys.next(answer), Ok(Next::Done(cached.clone())));

        let manifest: serde_json::Value =
            serde_json::from_slice(&cache_manifest(&entries, &cached)).unwrap();
        let file = serde_json::json!({
            "path":"a","source":"a","blake3":hex::encode([1; 32]),"size":1
        });
        assert_eq!(manifest, serde_json::json!({"version":1,"files":[file]}));
        assert!(names_manifest(&entries, CACHE_MANIFEST_KEY));
        assert!(!names_manifest(&entries, "a"));
    }

    // An input at the manifest key turns the cache off and is read as that input.
    #[test]
    fn manifest_key_disables() {
        let local = entry("a").captured.source_node_id;
        let taken = vec![entry("a"), entry(CACHE_MANIFEST_KEY)];
        let mut keys = CachedKeysOperation::new(taken.clone(), local);
        assert_eq!(keys.first(), Ok(Next::Done(BTreeSet::new())));
        assert!(!names_manifest(&taken, CACHE_MANIFEST_KEY));
    }

    // Nothing that could be cached asks for no row at all.
    #[test]
    fn empty_candidates_skip() {
        let other = iroh::SecretKey::from_bytes(&[9; 32]).public();
        let mut keys = CachedKeysOperation::new(vec![entry("a")], other);
        assert_eq!(keys.first(), Ok(Next::Done(BTreeSet::new())));
    }

    // A row that does not decode leaves the cache undecided.
    #[test]
    fn bad_row_unavailable() {
        let local = entry("a").captured.source_node_id;
        let mut keys = CachedKeysOperation::new(vec![entry("a")], local);
        keys.first().unwrap();
        let row = IoResult::Values(vec![Some(vec![0xff; 3].into())]);
        assert!(matches!(keys.next(row), Err(ViewError::Unavailable(_))));
    }

    fn manifest_operation(entries: Vec<ViewEntry>) -> ReadManifestOperation {
        let local = entries[0].captured.source_node_id;
        let auth = AuthContext {
            user_id: Default::default(),
            realm_id: RealmId::from_bytes([1; 32]),
            path_restrictions: None,
            session: None,
        };
        ReadManifestOperation::new(auth, entries, local)
    }

    fn cached_answer(keys: &[&str]) -> IoResult {
        let keys: BTreeSet<String> = keys.iter().map(|key| key.to_string()).collect();
        IoResult::Child(ChildOutcome::new(Ok::<_, ViewError>(keys)))
    }

    fn read_answer(result: Result<GroupId, SourceReadError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    // Every cached entry is checked, in order, before the list is built.
    #[test]
    fn manifest_checks_entries() {
        let entries = vec![entry("a"), entry("skipped"), entry("c")];
        let mut manifest = manifest_operation(entries.clone());
        assert!(matches!(manifest.first(), Ok(Next::Call(IoCall::Child(_)))));
        let first = manifest.next(cached_answer(&["a", "c"]));
        assert!(matches!(first, Ok(Next::Call(IoCall::Child(_)))));
        let group = Ulid::from_parts(2, 2);
        let second = manifest.next(read_answer(Ok(group)));
        assert!(matches!(second, Ok(Next::Call(IoCall::Child(_)))));
        let cached = BTreeSet::from(["a".to_string(), "c".to_string()]);
        assert_eq!(
            manifest.next(read_answer(Ok(group))),
            Ok(Next::Done(Some(cache_manifest(&entries, &cached))))
        );
    }

    // A run that caches nothing has no list and checks nothing.
    #[test]
    fn empty_manifest_none() {
        let mut manifest = manifest_operation(vec![entry("a")]);
        manifest.first().unwrap();
        assert_eq!(manifest.next(cached_answer(&[])), Ok(Next::Done(None)));
    }

    // A denied entry refuses the whole list.
    #[test]
    fn manifest_denied_fails() {
        let mut manifest = manifest_operation(vec![entry("a")]);
        manifest.first().unwrap();
        manifest.next(cached_answer(&["a"])).unwrap();
        assert_eq!(
            manifest.next(read_answer(Err(SourceReadError::Denied))),
            Err(ViewError::Denied)
        );
    }

    #[tokio::test]
    async fn manifest_needs_reads() {
        // A rule that denies only GetObject keeps an input out of the file list.
        use aruna_core::effects::StorageEffect;
        use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
        use aruna_core::request_policy::{PolicyKind, RequestPolicy};
        use aruna_core::structs::storage::blob::BucketInfo;
        use aruna_core::structs::storage::format::Compression;

        let (_directory, storage) = crate::tests::s3::test_storage();
        let context = crate::testing::test_context(storage);
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
            crate::testing_access::seed_open_access(&context, scope, "none/**", None, vec![policy])
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

        let manifest = |entries| ReadManifestOperation::new(owner.clone(), entries, local);
        let open = drive(manifest(vec![entry("a")]), &context).await;
        assert!(open.unwrap().is_some());
        let blocked = vec![entry("a"), entry("blocked")];
        let head = drive(
            SourceReadOperation::of_object(&owner, local, ("b", "blocked"), HEAD_OPERATION),
            &context,
        );
        assert!(head.await.is_ok());
        let denied = drive(manifest(blocked), &context).await;
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

    fn viewer() -> AuthContext {
        AuthContext {
            user_id: Default::default(),
            realm_id: RealmId([1; 32]),
            path_restrictions: None,
            session: None,
        }
    }

    fn holder_node() -> NodeId {
        entry("a").captured.source_node_id
    }

    fn other_node() -> NodeId {
        iroh::SecretKey::from_bytes(&[2; 32]).public()
    }

    fn source_group() -> GroupId {
        Ulid::from_parts(2, 2)
    }

    fn entry_reply<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn source_read(operation: &'static str) -> IoCall {
        let object = ("b", "a");
        let read = SourceReadOperation::of_object(&viewer(), holder_node(), object, operation);
        IoCall::child(read)
    }

    // A head of a local entry only checks the creator may still read it.
    #[test]
    fn local_head_authorizes() {
        let mut head = ViewEntryOperation::head(viewer(), entry("a"), holder_node());
        assert_eq!(head.first(), Ok(Next::Call(source_read(HEAD_OPERATION))));
        let allowed = entry_reply(Ok::<_, SourceReadError>(source_group()));
        assert_eq!(head.next(allowed), Ok(Next::Done(None)));
    }

    // A local read fetches the captured version, with an end-exclusive range made inclusive.
    #[test]
    fn local_read_fetches() {
        let mut read = ViewEntryOperation::read(viewer(), entry("a"), holder_node(), Some(2..5));
        assert_eq!(read.first(), Ok(Next::Call(source_read(READ_OPERATION))));
        let get = GetObjectOperation::new(GetObjectInput {
            bucket: "b".to_string(),
            key: "a".to_string(),
            version_id: Some(Ulid::from_parts(1, 1)),
            range: Some(ObjectRangeRequest::StartEnd { start: 2, end: 4 }),
            group_id: source_group(),
            user_identity: viewer().user_id,
            realm_id: viewer().realm_id,
            node_id: holder_node(),
        });
        let allowed = entry_reply(Ok::<_, SourceReadError>(source_group()));
        assert_eq!(read.next(allowed), Ok(Next::Call(IoCall::child(get))));
        let failed = entry_reply(Err::<GetObjectResult, _>(GetObjectError::NoSuchKey));
        let message = GetObjectError::NoSuchKey.to_string();
        assert_eq!(read.next(failed), Err(ViewError::Unavailable(message)));
    }

    // A refused source keeps its meaning for the S3 caller.
    #[test]
    fn refused_source_mapped() {
        let cases = [
            (SourceReadError::Denied, ViewError::Denied),
            (SourceReadError::NotFound, ViewError::NotFound),
        ];
        for (error, expected) in cases {
            let mut read = ViewEntryOperation::read(viewer(), entry("a"), holder_node(), None);
            read.first().unwrap();
            let refused = entry_reply(Err::<GroupId, _>(error));
            assert_eq!(read.next(refused), Err(expected));
        }
    }

    // An entry on another node is read from its holder at the exact hash and version.
    #[test]
    fn remote_entry_managed() {
        let mut head = ViewEntryOperation::head(viewer(), entry("a"), other_node());
        let request = head.managed_request([1; 32]);
        assert!(request.metadata_only);
        let read = ManagedReadOperation::new(holder_node(), request);
        assert_eq!(head.first(), Ok(Next::Call(IoCall::child(read))));
        let denied = BaoReadError::Refused(BaoReadRefusal::ReadDenied);
        let refused = entry_reply(Err::<BaoReadOutput, _>(denied));
        assert_eq!(head.next(refused), Err(ViewError::Denied));
    }

    // A reference on another node without a grant cannot be read.
    #[test]
    fn ungranted_reference_denied() {
        let mut reference = entry("a");
        reference.captured.content = InputContent::Reference {
            etag: None,
            range_bound: false,
        };
        let mut read = ViewEntryOperation::read(viewer(), reference, other_node(), None);
        assert_eq!(read.first(), Err(ViewError::Denied));
    }
}
