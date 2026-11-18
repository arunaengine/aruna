//! Publishes database releases: lists current files at each range's coordinator, authorizes them
//! and writes the tagged crate as one version. Also answers other members.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, MetadataRequestEffect, NetEffect};
use aruna_core::events::{Event, MetadataAnswer, NetEvent};
use aruna_core::keyspaces::{BLOB_VERSIONS_KEYSPACE, BUCKET_RELEASE_KEYSPACE};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::rows::PrefixPages;
use aruna_core::stream::BackendStream;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::identifier::Identifier;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::policy::document::group_admin_path;
use aruna_core::structs::storage::blob::{BucketInfo, VersionKey, bucket_permission_path};
use aruna_core::structs::storage::bucket_metadata::{
    RELEASE_PREFIX_TAG, RELEASE_TAG, ReleasePointer, curator_path, release_prefix,
};
use aruna_core::structs::storage::data::identity::content_id;
use aruna_core::structs::storage::distribution::fence::LocalFence;
use aruna_core::structs::storage::distribution::{DistributionView, KeyRange};
use aruna_core::structs::storage::origin::{OriginVersion, SourcePin};
use aruna_core::structs::storage::release::{
    CrateBytes, CrateChunk, CrateRead, CrateReadError, HolderAnswer, HolderAsk, RangeFiles,
    RangeMembers, ReleaseEffect, ReleaseEvent, ReleaseFile,
};
use aruna_core::structs::storage::routing::{NodeRouting, RoutingSnapshot};
use aruna_core::types::{Key, Value as Row};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use ulid::Ulid;

use super::access::LoadAccessOperation;
use super::{
    Access, BucketScope, MembersConfig, MembersOutput, ReleaseError, ReleaseMembersOperation,
};
use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::distribution::guard::GuardError;
use crate::distribution::routing::local_fence;
use crate::distribution::status::{StatusError, read_view};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::groups::routing::{BucketRules, RoutingInputsError, SnapshotOperation};
use crate::metadata::protocol::{
    MetadataReadError, MetadataTransportMessage, decode_message, encode_message,
};
use crate::placement::policy::{GateContext, GateContextError, GateOperation};
use crate::replication::live_queue::{CompletePutInput, CompletePutOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::list::{
    ListBucketError, ListBucketInput, ListBucketOperation, ListBucketResult, ListContinuationToken,
};
use crate::s3::object::origin::{OriginError, OriginView, ReadOriginOperation};
use crate::s3::object::put::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation, PutObjectResult,
};

#[derive(Debug, Error, PartialEq)]
pub enum PublishError {
    #[error("the caller may not publish this release: {0}")]
    Forbidden(&'static str),
    #[error("{0}")]
    Unavailable(String),
    #[error("{0}")]
    Conflict(String),
}

impl From<ReleaseError> for PublishError {
    fn from(error: ReleaseError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

impl From<IoError> for PublishError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

fn unavailable(error: impl std::fmt::Display) -> PublishError {
    PublishError::Unavailable(error.to_string())
}

type PublishTurn<T> = Result<Next<T>, PublishError>;

/// What a publisher declares about a release besides its files.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReleaseSpec {
    pub label: String,
    pub prefix: String,
    pub upstream: Vec<Identifier>,
    /// Runs that prepared the files, by run id.
    pub preparation: Vec<Ulid>,
}

/// The release crate: deterministic for the same spec and files, so a repeated publish of an
/// unchanged release yields the same bytes.
pub fn release_crate(spec: &ReleaseSpec, files: &[ReleaseFile]) -> Vec<u8> {
    let mut graph = vec![json!({
        "@id": "ro-crate-metadata.json",
        "@type": "CreativeWork",
        "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
        "about": {"@id": "./"}
    })];
    let identifiers: Vec<Value> = (0..spec.upstream.len())
        .map(|index| json!({"@id": format!("#upstream-{index}")}))
        .collect();
    let based_on: Vec<Value> = (spec.upstream.iter())
        .map(|identifier| json!({"@id": identifier.url}))
        .collect();
    let runs: Vec<Value> = (spec.preparation.iter())
        .map(|run_id| json!({"@id": format!("#run-{run_id}")}))
        .collect();
    let parts: Vec<Value> = (files.iter())
        .map(|file| json!({"@id": file.version.to_id()}))
        .collect();
    graph.push(json!({
        "@id": "./",
        "@type": "Dataset",
        "name": format!("Release {}", spec.label),
        "version": spec.label,
        "description": format!("Files under `{}` pinned at publication", spec.prefix),
        "identifier": identifiers,
        "isBasedOn": based_on,
        "mentions": runs,
        "hasPart": parts
    }));
    for (index, identifier) in spec.upstream.iter().enumerate() {
        graph.push(json!({
            "@id": format!("#upstream-{index}"),
            "@type": "PropertyValue",
            "name": identifier.name,
            "value": identifier.value,
            "url": identifier.url
        }));
    }
    for run_id in &spec.preparation {
        graph.push(json!({
            "@id": format!("#run-{run_id}"),
            "@type": "CreateAction",
            "name": format!("Preparation run {run_id}"),
            "identifier": run_id.to_string()
        }));
    }
    let mut pinned = Vec::new();
    for file in files {
        let mut entity = json!({
            "@id": file.version.to_id(),
            "@type": "File",
            "name": file.key,
            "contentSize": file.size.to_string(),
            "contentUrl": file.version.to_id()
        });
        if let Some(content) = &file.content {
            entity["identifier"] = json!(content);
        }
        if let Some((pin, etag)) = &file.pin {
            if let Some(url) = &pin.url {
                entity["isBasedOn"] = json!({"@id": url});
            }
            let id = file.version.to_id();
            let pins = [
                ("source-path", Some(&pin.path)),
                ("source-etag", etag.as_ref()),
                ("source-version", pin.source_version.as_ref()),
            ];
            let mut properties = Vec::new();
            for (term, value) in pins {
                let Some(value) = value else { continue };
                let property = format!("{id}#{term}");
                properties.push(json!({"@id": property}));
                pinned.push(json!({
                    "@id": property,
                    "@type": "PropertyValue",
                    "propertyID": format!("https://w3id.org/aruna/terms/{term}"),
                    "value": value
                }));
            }
            if !properties.is_empty() {
                entity["additionalProperty"] = json!(properties);
            }
        }
        graph.push(entity);
    }
    graph.extend(pinned);
    let document = json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": graph
    });
    serde_json::to_vec_pretty(&document).unwrap_or_default()
}

const UNREADABLE: &str = "a file under the prefix is not readable";
const CHANGED: &str = "a pinned file changed while the release was written; publish again";

/// Keys listed for one page of release files, so a holder's reply stays far below the transport
/// ceiling.
const FILE_PAGE: usize = 256;
/// Rows read per storage page while looking for a bucket's releases.
const ROW_PAGE: usize = 256;
/// How long one question to a range's coordinator may take in all.
const HOLDER_DEADLINE: Duration = Duration::from_secs(60);

/// A bucket's identity, kept by the operations that outlive their caller's borrow.
#[derive(Clone, Debug, PartialEq)]
struct ReleaseScope {
    realm_id: RealmId,
    node_id: NodeId,
    bucket: String,
    info: BucketInfo,
}

impl ReleaseScope {
    fn of(scope: BucketScope<'_>) -> Self {
        Self {
            realm_id: scope.realm_id,
            node_id: scope.node_id,
            bucket: scope.bucket.to_string(),
            info: scope.info.clone(),
        }
    }

    fn view(&self) -> BucketScope<'_> {
        BucketScope {
            realm_id: self.realm_id,
            node_id: self.node_id,
            bucket: &self.bucket,
            info: &self.info,
        }
    }
}

/// One page of release files and the key the next page starts after.
pub type FilesPage = (Vec<ReleaseFile>, Option<String>);

/// One page of current versions under a prefix in a range (or the whole bucket) after a key, one
/// key left out. Each file comes from one exact version, and the caller must be able to read
/// every file, or nothing is published.
#[derive(Debug, PartialEq)]
pub struct ReleaseFilesOperation {
    scope: ReleaseScope,
    access: Access,
    prefix: String,
    skip: String,
    range: Option<KeyRange>,
    after: Option<String>,
    continuation: Option<ListContinuationToken>,
    keys: VecDeque<String>,
    files: Vec<ReleaseFile>,
    more: Option<ListContinuationToken>,
    last: Option<String>,
    current: Option<String>,
}

impl ReleaseFilesOperation {
    pub fn new(
        scope: BucketScope<'_>,
        access: &Access,
        (prefix, skip): (&str, &str),
        (range, after): (Option<&KeyRange>, Option<String>),
    ) -> Self {
        Self {
            scope: ReleaseScope::of(scope),
            access: access.clone(),
            prefix: prefix.to_string(),
            skip: skip.to_string(),
            range: range.cloned(),
            after,
            continuation: None,
            keys: VecDeque::new(),
            files: Vec::new(),
            more: None,
            last: None,
            current: None,
        }
    }

    /// Only the range's keys are listed, so no key outside it decides the page or its end.
    fn list(&mut self) -> PublishTurn<FilesPage> {
        let list = ListBucketOperation::new(ListBucketInput {
            bucket: self.scope.bucket.clone(),
            group_id: self.scope.info.group_id,
            continuation_token: self.continuation.take(),
            max_keys: Some(FILE_PAGE),
            prefix: Some(self.prefix.clone()),
            delimiter: None,
            start_after: self.after.take(),
        });
        let list = match &self.range {
            Some(range) => list.with_bounds(range.lower.clone(), range.upper.clone()),
            None => list,
        };
        Ok(Next::Call(IoCall::child(list)))
    }

    fn next_key(&mut self) -> PublishTurn<FilesPage> {
        while let Some(key) = self.keys.pop_front() {
            let outside = (self.range.as_ref()).is_some_and(|range| !range.contains(&key));
            if key == self.skip || outside {
                continue;
            }
            if !self.access.0.allows(Some(&key), &Permission::READ) {
                return Err(PublishError::Forbidden(UNREADABLE));
            }
            let scope = &self.scope;
            let origin = ReadOriginOperation::new(scope.realm_id, &scope.bucket, &key, None);
            self.current = Some(key);
            return Ok(Next::Call(IoCall::child(origin)));
        }
        // A page with no key goes on to the next; otherwise its last key is the position.
        let files = match (self.more.take(), self.last.take()) {
            (Some(token), None) => {
                self.continuation = Some(token);
                return self.list();
            }
            (Some(_), last) => (std::mem::take(&mut self.files), last),
            (None, _) => (std::mem::take(&mut self.files), None),
        };
        Ok(Next::Done(files))
    }

    /// Every field comes from the one version the origin read pinned.
    fn add_file(&mut self, key: String, origin: OriginView) {
        let Some(version) = self.scope.view().exact(&key, origin.version_id) else {
            return;
        };
        let pin = origin.source.map(|source| {
            let pin = SourcePin {
                url: source.url,
                path: source.path,
                source_version: source.version,
            };
            (pin, source.etag)
        });
        self.files.push(ReleaseFile {
            key,
            version,
            size: origin.size.unwrap_or_default(),
            content: origin.content_hash.map(content_id),
            pin,
        });
    }
}

impl Operation for ReleaseFilesOperation {
    type Output = FilesPage;
    type Error = PublishError;

    fn first(&mut self) -> PublishTurn<FilesPage> {
        self.list()
    }

    fn next(&mut self, result: IoResult) -> PublishTurn<FilesPage> {
        if let Some(key) = self.current.take() {
            let origin = result.child::<OriginView, OriginError>()?;
            self.add_file(key, origin.map_err(unavailable)?);
            return self.next_key();
        }
        let listed = result.child::<ListBucketResult, ListBucketError>()?;
        let listed = listed.map_err(unavailable)?;
        self.last = (listed.objects.last()).map(|object| object.head.key.clone());
        self.more = listed.continuation_token;
        self.keys = (listed.objects.into_iter())
            .map(|object| object.head.key)
            .collect();
        self.next_key()
    }
}

/// Every current file under a prefix in a range, page by page.
#[derive(Debug, PartialEq)]
struct AllFilesOperation {
    scope: ReleaseScope,
    access: Access,
    prefix: String,
    skip: String,
    range: Option<KeyRange>,
    files: Vec<ReleaseFile>,
}

impl AllFilesOperation {
    fn page(&self, after: Option<String>) -> IoCall {
        let asked = (self.prefix.as_str(), self.skip.as_str());
        let at = (self.range.as_ref(), after);
        let page = ReleaseFilesOperation::new(self.scope.view(), &self.access, asked, at);
        IoCall::child(page)
    }
}

impl Operation for AllFilesOperation {
    type Output = Vec<ReleaseFile>;
    type Error = PublishError;

    fn first(&mut self) -> PublishTurn<Vec<ReleaseFile>> {
        Ok(Next::Call(self.page(None)))
    }

    fn next(&mut self, result: IoResult) -> PublishTurn<Vec<ReleaseFile>> {
        let (page, next) = result.child::<FilesPage, PublishError>()??;
        self.files.extend(page);
        match next {
            Some(next) => Ok(Next::Call(self.page(Some(next)))),
            None => Ok(Next::Done(std::mem::take(&mut self.files))),
        }
    }
}

/// One release to publish. The bearer never shows in a `Debug` print.
#[derive(Clone, PartialEq)]
pub struct PublishInput {
    pub auth: AuthContext,
    /// Asks the coordinators of other ranges; required when the prefix spans them.
    pub bearer: Option<String>,
    pub key: String,
    pub spec: ReleaseSpec,
}

impl std::fmt::Debug for PublishInput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PublishInput")
            .field("auth", &self.auth)
            .field("bearer", &self.bearer.as_ref().map(|_| "<redacted>"))
            .field("key", &self.key)
            .field("spec", &self.spec)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Published {
    pub version_id: Ulid,
    pub files: usize,
    pub replayed: bool,
}

/// How one key range of a release is listed.
#[derive(Debug, PartialEq)]
enum RangePlan {
    Here(KeyRange),
    At(KeyRange, Option<NodeId>),
}

#[derive(Debug, PartialEq)]
enum CoordinatedStep {
    View,
    Fence(Box<DistributionView>),
    Local,
    Remote(KeyRange, NodeId),
}

/// The current files under the prefix, each range listed by its own coordinator; a range no
/// coordinator lists now refuses the publish, since a copy may be behind.
#[derive(Debug, PartialEq)]
struct CoordinatedFilesOperation {
    scope: ReleaseScope,
    access: Access,
    input: PublishInput,
    plans: VecDeque<RangePlan>,
    files: Vec<ReleaseFile>,
    step: CoordinatedStep,
}

fn range_refused(range: &KeyRange) -> PublishError {
    PublishError::Unavailable(format!(
        "range {} could not be listed by its coordinator",
        range.range_id
    ))
}

/// A release question to another member under the caller's token.
fn holder_call(
    node_id: NodeId,
    auth_token: AuthToken,
    bucket: String,
    ask: HolderAsk,
) -> Result<IoCall, PublishError> {
    let frame = encode_message(&MetadataTransportMessage::ReleaseHolder {
        auth_token: Some(auth_token),
        bucket,
        ask,
    })
    .map_err(PublishError::Unavailable)?;
    let request = MetadataRequestEffect {
        peer: node_id,
        frame,
        timeout: HOLDER_DEADLINE,
    };
    Ok(Effect::Net(NetEffect::MetadataRequest(Box::new(request))).into())
}

/// A member's answer; a transport failure reads like an unavailable member.
fn holder_reply(result: IoResult) -> Result<Result<HolderAnswer, MetadataReadError>, IoError> {
    let Event::Net(NetEvent::MetadataAnswer(answer)) = result.event()? else {
        return Ok(Err(MetadataReadError::Unavailable));
    };
    let MetadataAnswer { result, .. } = *answer;
    let reply = result.ok().and_then(|bytes| decode_message(&bytes).ok());
    Ok(match reply {
        Some(MetadataTransportMessage::ReleaseHolderResults { result }) => result,
        _ => Err(MetadataReadError::Unavailable),
    })
}

impl CoordinatedFilesOperation {
    fn new(scope: ReleaseScope, access: Access, input: PublishInput) -> Self {
        Self {
            scope,
            access,
            input,
            plans: VecDeque::new(),
            files: Vec::new(),
            step: CoordinatedStep::View,
        }
    }

    fn list_files(&mut self, range: Option<KeyRange>) -> PublishTurn<Vec<ReleaseFile>> {
        self.step = CoordinatedStep::Local;
        Ok(Next::Call(IoCall::child(AllFilesOperation {
            scope: self.scope.clone(),
            access: self.access.clone(),
            prefix: self.input.spec.prefix.clone(),
            skip: self.input.key.clone(),
            range,
            files: Vec::new(),
        })))
    }

    fn ask(
        &mut self,
        range: KeyRange,
        node_id: NodeId,
        after: Option<String>,
    ) -> PublishTurn<Vec<ReleaseFile>> {
        let Some(bearer) = self.input.bearer.clone() else {
            return Err(range_refused(&range));
        };
        let token = AuthToken::bearer(bearer).map_err(unavailable)?;
        let ask = HolderAsk::Files {
            range_id: range.range_id,
            prefix: self.input.spec.prefix.clone(),
            skip: self.input.key.clone(),
            after,
        };
        let call = holder_call(node_id, token, self.scope.bucket.clone(), ask)?;
        self.step = CoordinatedStep::Remote(range, node_id);
        Ok(Next::Call(call))
    }

    fn next_range(&mut self) -> PublishTurn<Vec<ReleaseFile>> {
        match self.plans.pop_front() {
            Some(RangePlan::Here(range)) => self.list_files(Some(range)),
            Some(RangePlan::At(range, Some(node_id))) => self.ask(range, node_id, None),
            Some(RangePlan::At(range, None)) => Err(range_refused(&range)),
            None => {
                let mut files = std::mem::take(&mut self.files);
                files.sort_by(|left, right| left.key.cmp(&right.key));
                Ok(Next::Done(files))
            }
        }
    }
}

impl Operation for CoordinatedFilesOperation {
    type Output = Vec<ReleaseFile>;
    type Error = PublishError;

    fn first(&mut self) -> PublishTurn<Vec<ReleaseFile>> {
        let Some(binding) = &self.scope.info.distribution else {
            return self.list_files(None);
        };
        Ok(Next::Call(IoCall::child(read_view(binding.set_id))))
    }

    fn next(&mut self, result: IoResult) -> PublishTurn<Vec<ReleaseFile>> {
        let binding = self.scope.info.distribution.clone();
        match std::mem::replace(&mut self.step, CoordinatedStep::Local) {
            CoordinatedStep::View => {
                let binding = binding.ok_or(IoError::Unfinished)?;
                let view = (result.child::<Option<DistributionView>, StatusError>()?)
                    .map_err(unavailable)?
                    .ok_or_else(|| unavailable("this node holds no record of the set"))?;
                self.step = CoordinatedStep::Fence(Box::new(view));
                Ok(Next::Call(IoCall::child(local_fence(binding.set_id))))
            }
            CoordinatedStep::Fence(view) => {
                let binding = binding.ok_or(IoError::Unfinished)?;
                let fence = result.child::<LocalFence, GuardError>()?;
                let fence = fence.map_err(unavailable)?;
                let snapshot = view.snapshot();
                self.plans = (snapshot.listed_ranges(&self.input.spec.prefix).into_iter())
                    .map(
                        |range| match snapshot.lists_locally(binding.member_id, range, &fence) {
                            true => RangePlan::Here(range.clone()),
                            false => RangePlan::At(
                                range.clone(),
                                snapshot.list_holder(range).map(|member| member.node_id),
                            ),
                        },
                    )
                    .collect();
                self.next_range()
            }
            CoordinatedStep::Local => {
                let files = result.child::<Vec<ReleaseFile>, PublishError>()??;
                if self.scope.info.distribution.is_none() {
                    return Ok(Next::Done(files));
                }
                self.files.extend(files);
                self.next_range()
            }
            CoordinatedStep::Remote(range, node_id) => match holder_reply(result)? {
                Ok(HolderAnswer::Files(listed)) if !listed.refused => {
                    self.files.extend(listed.files);
                    match listed.next {
                        Some(next) => self.ask(range, node_id, Some(next)),
                        None => self.next_range(),
                    }
                }
                // The coordinator refused a file; the generic refusal names none.
                Err(MetadataReadError::Forbidden) => Err(PublishError::Forbidden(UNREADABLE)),
                _ => Err(range_refused(&range)),
            },
        }
    }
}

/// Every release row of a bucket this node holds, in key order.
#[derive(Debug, PartialEq)]
struct ReleaseRowsOperation {
    prefix: Vec<u8>,
    pages: PrefixPages,
    rows: Vec<(Ulid, ReleasePointer)>,
}

impl ReleaseRowsOperation {
    fn new(bucket: &str) -> Self {
        let prefix = release_prefix(bucket);
        Self {
            pages: PrefixPages::new(BUCKET_RELEASE_KEYSPACE, prefix.clone(), ROW_PAGE),
            prefix,
            rows: Vec::new(),
        }
    }
}

impl Operation for ReleaseRowsOperation {
    type Output = Vec<(Ulid, ReleasePointer)>;
    type Error = PublishError;

    fn first(&mut self) -> PublishTurn<Vec<(Ulid, ReleasePointer)>> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> PublishTurn<Vec<(Ulid, ReleasePointer)>> {
        for (key, value) in self.pages.take(result)? {
            let id = key.get(self.prefix.len()..).map(<[u8; 16]>::try_from);
            let Some(Ok(id)) = id else {
                continue;
            };
            let pointer = ReleasePointer::from_bytes(value.as_ref()).map_err(unavailable)?;
            self.rows.push((Ulid::from_bytes(id), pointer));
        }
        match self.pages.done() {
            true => Ok(Next::Done(std::mem::take(&mut self.rows))),
            false => Ok(Next::Call(self.pages.call())),
        }
    }
}

/// A version row a write must still read as given: keyspace, key and row.
type Guard = (String, Key, Option<Row>);

#[derive(Debug, PartialEq)]
enum PutStep {
    Config,
    Routing(Box<RealmConfigDocument>),
    Gate(Box<RealmConfigDocument>, Box<RoutingSnapshot>),
    Put,
    Complete(Ulid),
}

/// Writes the release crate as one version tagged with its label and prefix; `None` when the
/// key's head is no longer the given one, so nothing was written. Each guard must still read as
/// given.
#[derive(Debug, PartialEq)]
struct PutCrateOperation {
    scope: ReleaseScope,
    input: PublishInput,
    routing: NodeRouting,
    bytes: Vec<u8>,
    head: Option<Ulid>,
    guards: Vec<Guard>,
    now_ms: u64,
    step: PutStep,
}

impl PutCrateOperation {
    fn put(
        &mut self,
        realm: &RealmConfigDocument,
        routing: RoutingSnapshot,
        gate: Option<GateContext>,
    ) -> PublishTurn<Option<Ulid>> {
        let scope = &self.scope;
        let input = &self.input;
        let group_id = scope.info.group_id;
        let bytes = std::mem::take(&mut self.bytes);
        let size = bytes.len() as u64;
        let body = futures_util::stream::iter([Ok::<Bytes, std::io::Error>(Bytes::from(bytes))]);
        let tags = HashMap::from([
            (RELEASE_TAG.to_string(), input.spec.label.clone()),
            (RELEASE_PREFIX_TAG.to_string(), input.spec.prefix.clone()),
            (
                aruna_core::structs::storage::blob::CONTENT_TYPE_KEY.to_string(),
                "application/ld+json".to_string(),
            ),
        ]);
        let mut operation = PutObjectOperation::new(PutObjectConfig {
            user_id: input.auth.user_id,
            group_id,
            realm_id: scope.realm_id,
            node_id: scope.node_id,
            request: PutObjectInput {
                bucket: scope.bucket.clone(),
                key: input.key.clone(),
                content_length: Some(size),
                body: Some(BackendStream::new(body)),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: realm.quota.effective_group_ceiling(&group_id),
            routing,
        })
        .with_metadata(tags)
        .with_expected_head(self.head)
        .with_restrictions(input.auth.path_restrictions.clone());
        if let Some(gate) = gate {
            operation = operation.with_gate(gate);
        }
        let guards = std::mem::take(&mut self.guards);
        if !guards.is_empty() {
            operation = operation.with_fence(guards, Vec::new());
        }
        self.step = PutStep::Put;
        Ok(Next::Call(IoCall::child(operation)))
    }
}

impl Operation for PutCrateOperation {
    type Output = Option<Ulid>;
    type Error = PublishError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> PublishTurn<Option<Ulid>> {
        let config = GetConfigOperation::new(self.scope.realm_id);
        Ok(Next::Call(IoCall::child(config)))
    }

    fn next(&mut self, result: IoResult) -> PublishTurn<Option<Ulid>> {
        match std::mem::replace(&mut self.step, PutStep::Put) {
            PutStep::Config => {
                let realm = result.child::<RealmConfigDocument, GetConfigError>()?;
                let realm = realm.map_err(unavailable)?;
                let info = &self.scope.info;
                let rules = BucketRules::of(info);
                let snapshot = SnapshotOperation::new(&self.routing, info.group_id, rules);
                self.step = PutStep::Routing(Box::new(realm));
                Ok(Next::Call(IoCall::child(snapshot)))
            }
            PutStep::Routing(realm) => {
                let routing = result.child::<RoutingSnapshot, RoutingInputsError>()?;
                let routing = routing.map_err(unavailable)?;
                let gate = GateOperation::new(self.scope.realm_id, self.now_ms);
                self.step = PutStep::Gate(realm, Box::new(routing));
                Ok(Next::Call(IoCall::child(gate)))
            }
            PutStep::Gate(realm, routing) => {
                let gate = result.child::<Option<GateContext>, GateContextError>()?;
                self.put(&realm, *routing, gate.map_err(unavailable)?)
            }
            PutStep::Put => {
                let written = match result.child::<PutObjectResult, PutObjectError>()? {
                    Ok(written) => written,
                    Err(PutObjectError::HeadChanged) => return Ok(Next::Done(None)),
                    Err(PutObjectError::FenceTaken) => {
                        return Err(PublishError::Conflict(CHANGED.to_string()));
                    }
                    Err(error) => return Err(unavailable(error)),
                };
                let scope = &self.scope;
                let complete = CompletePutOperation::new(CompletePutInput {
                    realm_id: scope.realm_id,
                    node_id: scope.node_id,
                    auth: self.input.auth.clone(),
                    group_id: scope.info.group_id,
                    bucket: scope.bucket.clone(),
                    key: self.input.key.clone(),
                    version_id: written.version_id,
                    size_bytes: written.location.blob_size,
                });
                self.step = PutStep::Complete(written.version_id);
                Ok(Next::Call(IoCall::child(complete)))
            }
            PutStep::Complete(version_id) => Ok(Next::Done(Some(version_id))),
        }
    }
}

#[derive(Debug, PartialEq)]
enum PublishStep {
    BucketWrite,
    Admin,
    Curator,
    Access,
    Files,
    Guards,
    Head,
    Rows(Ulid),
    Put,
}

/// Publishes a release on the node that writes its key: the caller needs bucket WRITE and admin
/// or curator rights, may read every pinned file, and gets the existing version back when the
/// identical release is already the key's head.
#[derive(Debug, PartialEq)]
pub struct PublishReleaseOperation {
    scope: ReleaseScope,
    input: PublishInput,
    routing: NodeRouting,
    files: usize,
    bytes: Vec<u8>,
    hash: [u8; 32],
    guard_keys: Vec<Key>,
    guards: Vec<Guard>,
    attempts: usize,
    step: PublishStep,
}

type ReleaseTurn = PublishTurn<Published>;

impl PublishReleaseOperation {
    pub fn new(scope: BucketScope<'_>, input: PublishInput, routing: NodeRouting) -> Self {
        Self {
            scope: ReleaseScope::of(scope),
            input,
            routing,
            files: 0,
            bytes: Vec::new(),
            hash: [0; 32],
            guard_keys: Vec::new(),
            guards: Vec::new(),
            attempts: 0,
            step: PublishStep::BucketWrite,
        }
    }

    fn allowed_call(&mut self, path: &str, step: PublishStep) -> ReleaseTurn {
        let extras = PolicyRequestExtras::rest();
        let realm_id = self.scope.realm_id;
        let check =
            AuthorizeOperation::new(realm_id, &self.input.auth, path, &Permission::WRITE, extras);
        self.step = step;
        Ok(Next::Call(IoCall::child(check)))
    }

    fn load_access(&mut self) -> ReleaseTurn {
        let load = LoadAccessOperation::new(self.scope.view(), &self.input.auth);
        self.step = PublishStep::Access;
        Ok(Next::Call(IoCall::child(load)))
    }

    /// The version rows of an ordinary bucket's pinned files as they are now. The crate's write
    /// keeps them, so a file deleted meanwhile fails the publish instead of breaking the release.
    fn read_guards(&mut self, files: Vec<ReleaseFile>) -> ReleaseTurn {
        self.files = files.len();
        self.bytes = release_crate(&self.input.spec, &files);
        self.hash = *blake3::hash(&self.bytes).as_bytes();
        let mut reads = Vec::new();
        if self.scope.info.distribution.is_none() {
            for file in &files {
                let OriginVersion::Node(arn) = &file.version else {
                    continue;
                };
                let key = VersionKey::new(&self.scope.bucket, file.key.as_str(), arn.version)
                    .to_bytes()
                    .map_err(unavailable)?;
                reads.push((BLOB_VERSIONS_KEYSPACE.to_string(), Key::from(key)));
            }
        }
        if reads.is_empty() {
            return self.read_head();
        }
        self.step = PublishStep::Guards;
        self.guard_keys = reads.iter().map(|(_, key)| key.clone()).collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// A concurrent publish moves the head; the next pass finds its release or conflicts.
    fn read_head(&mut self) -> ReleaseTurn {
        if self.attempts == 2 {
            return Err(PublishError::Conflict(
                "the release key changed while the release was written; publish again".to_string(),
            ));
        }
        self.attempts += 1;
        let scope = &self.scope;
        let head = ReadOriginOperation::new(scope.realm_id, &scope.bucket, &self.input.key, None);
        self.step = PublishStep::Head;
        Ok(Next::Call(IoCall::child(head)))
    }

    fn write(&mut self, head: Option<Ulid>) -> ReleaseTurn {
        self.step = PublishStep::Put;
        Ok(Next::Call(IoCall::child(PutCrateOperation {
            scope: self.scope.clone(),
            input: self.input.clone(),
            routing: self.routing.clone(),
            bytes: self.bytes.clone(),
            head,
            guards: self.guards.clone(),
            now_ms: 0,
            step: PutStep::Config,
        })))
    }

    fn published(&self, version_id: Ulid, replayed: bool) -> ReleaseTurn {
        Ok(Next::Done(Published {
            version_id,
            files: self.files,
            replayed,
        }))
    }
}

fn write_allowed(result: IoResult) -> Result<bool, PublishError> {
    match result.child::<(), AuthorizeError>()? {
        Ok(()) => Ok(true),
        Err(AuthorizeError::Storage(error)) => Err(unavailable(error)),
        Err(_) => Ok(false),
    }
}

impl Operation for PublishReleaseOperation {
    type Output = Published;
    type Error = PublishError;

    fn first(&mut self) -> ReleaseTurn {
        let scope = &self.scope;
        let group_id = scope.info.group_id;
        let path = bucket_permission_path(scope.realm_id, group_id, scope.node_id, &scope.bucket);
        self.allowed_call(&path, PublishStep::BucketWrite)
    }

    fn next(&mut self, result: IoResult) -> ReleaseTurn {
        let realm_id = self.scope.realm_id;
        match std::mem::replace(&mut self.step, PublishStep::Put) {
            PublishStep::BucketWrite => {
                if !write_allowed(result)? {
                    return Err(PublishError::Forbidden("no WRITE on the bucket"));
                }
                let admin = group_admin_path(realm_id, self.scope.info.group_id);
                self.allowed_call(&admin, PublishStep::Admin)
            }
            PublishStep::Admin => match write_allowed(result)? {
                true => self.load_access(),
                false => self.allowed_call(&curator_path(realm_id), PublishStep::Curator),
            },
            PublishStep::Curator => match write_allowed(result)? {
                true => self.load_access(),
                false => Err(PublishError::Forbidden("neither group admin nor curator")),
            },
            PublishStep::Access => {
                let access = result.child::<Access, ReleaseError>()??;
                let files =
                    CoordinatedFilesOperation::new(self.scope.clone(), access, self.input.clone());
                self.step = PublishStep::Files;
                Ok(Next::Call(IoCall::child(files)))
            }
            PublishStep::Files => {
                self.read_guards(result.child::<Vec<ReleaseFile>, PublishError>()??)
            }
            PublishStep::Guards => {
                let values = result.values()?;
                if values.iter().any(Option::is_none) {
                    return Err(PublishError::Conflict(CHANGED.to_string()));
                }
                let keys = std::mem::take(&mut self.guard_keys);
                self.guards = (keys.into_iter().zip(values))
                    .map(|(key, value)| (BLOB_VERSIONS_KEYSPACE.to_string(), key, value))
                    .collect();
                self.read_head()
            }
            PublishStep::Head => match result.child::<OriginView, OriginError>()? {
                Ok(current) if current.content_hash == Some(self.hash) => {
                    let rows = ReleaseRowsOperation::new(&self.scope.bucket);
                    self.step = PublishStep::Rows(current.version_id);
                    Ok(Next::Call(IoCall::child(rows)))
                }
                Ok(current) => self.write(Some(current.version_id)),
                Err(OriginError::VersionMissing) => self.write(None),
                Err(error) => Err(unavailable(error)),
            },
            PublishStep::Rows(version_id) => {
                let rows = result.child::<Vec<(Ulid, ReleasePointer)>, PublishError>()??;
                let spec = &self.input.spec;
                let same = rows.iter().any(|(row, pointer)| {
                    *row == version_id
                        && pointer.label == spec.label
                        && pointer.prefix == spec.prefix
                });
                match same {
                    true => self.published(version_id, true),
                    false => self.write(Some(version_id)),
                }
            }
            PublishStep::Put => match result.child::<Option<Ulid>, PublishError>()?? {
                Some(version_id) => self.published(version_id, false),
                None => self.read_head(),
            },
        }
    }
}

/// Reads one release crate version on this node with the caller's identity.
#[derive(Debug, PartialEq)]
pub struct ReadCrateOperation {
    read: Option<CrateRead>,
}

impl ReadCrateOperation {
    pub fn new(read: CrateRead) -> Self {
        Self { read: Some(read) }
    }
}

impl Operation for ReadCrateOperation {
    type Output = Result<CrateBytes, CrateReadError>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
        let read = self.read.take().ok_or(IoError::Unfinished)?;
        let effect = ReleaseEffect::ReadCrate(Box::new(read));
        Ok(Next::Call(
            Effect::Net(NetEffect::Release(Box::new(effect))).into(),
        ))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, IoError> {
        match result.event()? {
            Event::Net(NetEvent::Release(ReleaseEvent::CrateRead(read))) => Ok(Next::Done(read)),
            other => Err(IoError::Unexpected {
                expected: "ReleaseEvent::CrateRead",
                got: other.label(),
            }),
        }
    }
}

#[derive(Debug, PartialEq)]
enum HolderStep {
    Bucket,
    Access,
    Members,
    View(Box<AskedFiles>),
    Fence(Box<AskedFiles>, Box<DistributionView>),
    Files,
    Chunk,
}

/// The files of one range another member asks for.
#[derive(Debug, PartialEq)]
struct AskedFiles {
    range_id: Ulid,
    prefix: String,
    skip: String,
    after: Option<String>,
}

/// A holder's answer for its ranges, for the caller it validated from the request's bearer
/// itself. It answers only for ranges it coordinates now.
#[derive(Debug, PartialEq)]
pub struct HolderAnswerOperation {
    realm_id: RealmId,
    node_id: NodeId,
    auth: AuthContext,
    bucket: String,
    ask: Option<HolderAsk>,
    scope: Option<ReleaseScope>,
    access: Option<Access>,
    step: HolderStep,
}

type HolderTurn = PublishTurn<HolderAnswer>;

fn no_files() -> HolderTurn {
    Ok(Next::Done(HolderAnswer::Files(RangeFiles {
        files: Vec::new(),
        refused: true,
        next: None,
    })))
}

impl HolderAnswerOperation {
    pub fn new(
        (realm_id, node_id): (RealmId, NodeId),
        auth: &AuthContext,
        (bucket, ask): (&str, HolderAsk),
    ) -> Self {
        Self {
            realm_id,
            node_id,
            auth: auth.clone(),
            bucket: bucket.to_string(),
            ask: Some(ask),
            scope: None,
            access: None,
            step: HolderStep::Bucket,
        }
    }

    fn scope(&self) -> Result<&ReleaseScope, IoError> {
        self.scope.as_ref().ok_or(IoError::Unfinished)
    }

    fn access(&self) -> Result<&Access, IoError> {
        self.access.as_ref().ok_or(IoError::Unfinished)
    }

    fn answer(&mut self) -> HolderTurn {
        let scope = self.scope()?.clone();
        let access = self.access()?.clone();
        match self.ask.take().ok_or(IoError::Unfinished)? {
            HolderAsk::Members { range_ids, queries } => {
                let members = ReleaseMembersOperation::new(MembersConfig {
                    realm_id: self.realm_id,
                    node_id: self.node_id,
                    bucket: self.bucket.clone(),
                    info: scope.info,
                    auth: self.auth.clone(),
                    access,
                    bearer: None,
                    queries,
                    ranges: Some(range_ids),
                    retention: false,
                });
                self.step = HolderStep::Members;
                Ok(Next::Call(IoCall::child(members)))
            }
            HolderAsk::Files {
                range_id,
                prefix,
                skip,
                after,
            } => {
                let Some(binding) = &scope.info.distribution else {
                    return no_files();
                };
                self.step = HolderStep::View(Box::new(AskedFiles {
                    range_id,
                    prefix,
                    skip,
                    after,
                }));
                Ok(Next::Call(IoCall::child(read_view(binding.set_id))))
            }
            HolderAsk::Crate {
                key,
                version_id,
                offset,
            } => {
                if !access.0.allows(Some(&key), &Permission::READ) {
                    return Err(PublishError::Forbidden("the release crate is not readable"));
                }
                let read = CrateRead {
                    bucket: self.bucket.clone(),
                    key,
                    version_id,
                    group_id: scope.info.group_id,
                    auth: self.auth.clone(),
                    realm_id: self.realm_id,
                    node_id: self.node_id,
                };
                let read = Box::new(read);
                let effect = ReleaseEffect::ReadChunk { read, offset };
                self.step = HolderStep::Chunk;
                Ok(Next::Call(
                    Effect::Net(NetEffect::Release(Box::new(effect))).into(),
                ))
            }
        }
    }

    fn list_range(&mut self, asked: AskedFiles, range: &KeyRange) -> HolderTurn {
        let scope = self.scope()?;
        let names = (asked.prefix.as_str(), asked.skip.as_str());
        let page = (Some(range), asked.after);
        let list = ReleaseFilesOperation::new(scope.view(), self.access()?, names, page);
        self.step = HolderStep::Files;
        Ok(Next::Call(IoCall::child(list)))
    }
}

impl Operation for HolderAnswerOperation {
    type Output = HolderAnswer;
    type Error = PublishError;

    fn first(&mut self) -> HolderTurn {
        let bucket = GetBucketOperation::new(self.bucket.clone());
        Ok(Next::Call(IoCall::child(bucket)))
    }

    fn next(&mut self, result: IoResult) -> HolderTurn {
        match std::mem::replace(&mut self.step, HolderStep::Files) {
            HolderStep::Bucket => {
                let info = result.child::<BucketInfo, GetBucketError>()?;
                let scope = ReleaseScope {
                    realm_id: self.realm_id,
                    node_id: self.node_id,
                    bucket: self.bucket.clone(),
                    info: info.map_err(unavailable)?,
                };
                let load = LoadAccessOperation::new(scope.view(), &self.auth);
                self.scope = Some(scope);
                self.step = HolderStep::Access;
                Ok(Next::Call(IoCall::child(load)))
            }
            HolderStep::Access => {
                self.access = Some(result.child::<Access, ReleaseError>()??);
                self.answer()
            }
            HolderStep::Members => {
                let output = result.child::<MembersOutput, ReleaseError>()??;
                Ok(Next::Done(HolderAnswer::Members(RangeMembers {
                    answers: output.answers,
                    releases: output.releases,
                    complete: output.complete,
                    refused: output.refused,
                })))
            }
            HolderStep::View(asked) => {
                let view = (result.child::<Option<DistributionView>, StatusError>()?)
                    .map_err(unavailable)?
                    .ok_or_else(|| unavailable("no record of the set"))?;
                let binding =
                    (self.scope()?.info.distribution.as_ref()).ok_or(IoError::Unfinished)?;
                let fence = local_fence(binding.set_id);
                self.step = HolderStep::Fence(asked, Box::new(view));
                Ok(Next::Call(IoCall::child(fence)))
            }
            HolderStep::Fence(asked, view) => {
                let fence = result.child::<LocalFence, GuardError>()?;
                let fence = fence.map_err(unavailable)?;
                let binding =
                    (self.scope()?.info.distribution.as_ref()).ok_or(IoError::Unfinished)?;
                let member_id = binding.member_id;
                let snapshot = view.snapshot();
                let range = (snapshot.config.ranges.iter())
                    .find(|range| range.range_id == asked.range_id)
                    .filter(|range| snapshot.lists_locally(member_id, range, &fence));
                match range {
                    Some(range) => self.list_range(*asked, range),
                    None => no_files(),
                }
            }
            HolderStep::Files => {
                let (files, next) = result.child::<FilesPage, PublishError>()??;
                Ok(Next::Done(HolderAnswer::Files(RangeFiles {
                    files,
                    refused: false,
                    next,
                })))
            }
            HolderStep::Chunk => {
                let Event::Net(NetEvent::Release(ReleaseEvent::CrateChunk(read))) =
                    result.event()?
                else {
                    return Err(unavailable("no release crate answer"));
                };
                match read {
                    Ok((found, size)) if found.metadata.contains_key(RELEASE_TAG) => {
                        let bytes = found.bytes;
                        let chunk = CrateChunk { bytes, size };
                        Ok(Next::Done(HolderAnswer::Crate(Some(chunk))))
                    }
                    Ok(_) | Err(CrateReadError::Missing) => {
                        Ok(Next::Done(HolderAnswer::Crate(None)))
                    }
                    Err(CrateReadError::Failed(reason)) => Err(PublishError::Unavailable(reason)),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{pinned_ids, upstream_of};
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::identifier::IdentifierTarget;
    use aruna_core::structs::storage::global_id::GlobalId;
    use aruna_core::structs::storage::origin::OriginVersion;

    #[test]
    fn crate_pins_files() {
        // The same release gives the same bytes, and names every file by its exact version.
        let realm_id = RealmId::from_bytes([1; 32]);
        let version = |seed: u8| {
            OriginVersion::Global(GlobalId::Version {
                realm_id,
                version_id: Ulid::from_bytes([seed; 16]),
            })
        };
        let spec = ReleaseSpec {
            label: "6.0".to_string(),
            prefix: "db/".to_string(),
            upstream: vec![Identifier {
                name: "DOI".to_string(),
                value: "10.5281/zenodo.1".to_string(),
                url: "https://doi.org/10.5281/zenodo.1".to_string(),
                target: IdentifierTarget::Key,
            }],
            preparation: vec![Ulid::from_bytes([9; 16])],
        };
        let files = vec![
            ReleaseFile {
                key: "db/a.fasta".to_string(),
                version: version(2),
                size: 4,
                content: Some(content_id([7; 32])),
                pin: None,
            },
            ReleaseFile {
                key: "db/b.tsv".to_string(),
                version: version(3),
                size: 9,
                content: None,
                pin: Some((
                    SourcePin {
                        url: Some("https://zenodo.org/records/1/files/b.tsv".to_string()),
                        path: "b.tsv".to_string(),
                        source_version: Some("3".to_string()),
                    },
                    Some("\"abc\"".to_string()),
                )),
            },
        ];
        let bytes = release_crate(&spec, &files);
        assert_eq!(bytes, release_crate(&spec, &files));
        let crate_json: Value = serde_json::from_slice(&bytes).unwrap();
        let graph = |crate_json: &Value| -> Vec<Value> {
            crate_json["@graph"].as_array().cloned().unwrap_or_default()
        };
        let pinned = pinned_ids(&crate_json);
        assert!(pinned.contains(&version(2).to_id()) && pinned.contains(&version(3).to_id()));
        assert_eq!(upstream_of(&crate_json), spec.upstream);
        let reference = graph(&crate_json)
            .into_iter()
            .find(|entity| entity["@id"] == version(3).to_id().as_str())
            .unwrap();
        assert_eq!(
            reference["isBasedOn"]["@id"],
            "https://zenodo.org/records/1/files/b.tsv"
        );
        let etag = format!("{}#source-etag", version(3).to_id());
        let property = graph(&crate_json)
            .into_iter()
            .find(|entity| entity["@id"] == etag.as_str())
            .unwrap();
        assert_eq!(property["value"], "\"abc\"");
        // The stored pin keeps its path and source version.
        for (term, value) in [("source-path", "b.tsv"), ("source-version", "3")] {
            let id = format!("{}#{term}", version(3).to_id());
            let entity = graph(&crate_json)
                .into_iter()
                .find(|entity| entity["@id"] == id.as_str());
            assert_eq!(entity.unwrap()["value"], value);
        }
    }

    fn step_info(realm_id: RealmId) -> BucketInfo {
        BucketInfo {
            group_id: Ulid::from_bytes([2; 16]),
            created_at: std::time::UNIX_EPOCH,
            created_by: aruna_core::UserId::nil(realm_id),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: aruna_core::structs::storage::format::Compression::Off,
            distribution: None,
        }
    }

    fn step_input(realm_id: RealmId) -> PublishInput {
        PublishInput {
            auth: AuthContext::anonymous(realm_id),
            bearer: Some("do-not-print".to_string()),
            key: "db/release.json".to_string(),
            spec: ReleaseSpec::default(),
        }
    }

    #[test]
    fn bearer_stays_hidden() {
        let input = step_input(RealmId::from_bytes([1; 32]));
        assert!(!format!("{input:?}").contains("do-not-print"));
    }

    #[test]
    fn missing_write_refused() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let info = step_info(realm_id);
        let scope = BucketScope {
            realm_id,
            node_id: iroh::SecretKey::from_bytes(&[3; 32]).public(),
            bucket: "db",
            info: &info,
        };
        let routing = NodeRouting::default();
        let mut publish = PublishReleaseOperation::new(scope, step_input(realm_id), routing);
        publish.first().unwrap();
        let denied: Result<(), AuthorizeError> = Err(AuthorizeError::PermissionDenied);
        let answer = publish.next(IoResult::Child(ChildOutcome::new(denied)));
        assert_eq!(
            answer,
            Err(PublishError::Forbidden("no WRITE on the bucket"))
        );
    }

    #[test]
    fn failed_transport_unavailable() {
        let peer = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let answer = MetadataAnswer {
            peer,
            elapsed: Duration::ZERO,
            result: Ok(vec![0xff]),
        };
        let event = Event::Net(NetEvent::MetadataAnswer(Box::new(answer)));
        assert_eq!(
            holder_reply(IoResult::Event(Box::new(event))),
            Ok(Err(MetadataReadError::Unavailable))
        );
    }

    #[test]
    fn missing_chunk_answers() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let node = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let auth = AuthContext::anonymous(realm_id);
        let ask = HolderAsk::Crate {
            key: "db/release.json".to_string(),
            version_id: Ulid::from_bytes([4; 16]),
            offset: 0,
        };
        let mut holder = HolderAnswerOperation::new((realm_id, node), &auth, ("db", ask));
        holder.step = HolderStep::Chunk;
        let read = ReleaseEvent::CrateChunk(Err(CrateReadError::Missing));
        let event = Event::Net(NetEvent::Release(read));
        assert_eq!(
            holder.next(IoResult::Event(Box::new(event))),
            Ok(Next::Done(HolderAnswer::Crate(None)))
        );
    }
}
