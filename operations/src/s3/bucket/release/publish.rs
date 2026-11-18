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
            auth: input.auth.clone(),
            group_id: scope.info.group_id,
            bucket: scope.bucket.to_string(),
            key: input.key.clone(),
            version_id: result.version_id,
            size_bytes: result.location.blob_size,
        }),
        context,
    )
    .await
    .ok();
    Ok(Some(result.version_id))
}

/// A holder's answer for its ranges, with the caller context it validated from the request's
/// bearer itself. It answers only for ranges it coordinates now.
pub async fn holder_answer(
    context: &DriverContext,
    (realm_id, node_id): (RealmId, NodeId),
    auth: &AuthContext,
    (bucket, ask): (&str, HolderAsk),
) -> Result<HolderAnswer, PublishError> {
    let unavailable = |error: &dyn std::fmt::Display| PublishError::Unavailable(error.to_string());
    let info = drive(GetBucketOperation::new(bucket.to_string()), context)
        .await
        .map_err(|error| unavailable(&error))?;
    let scope = BucketScope {
        realm_id,
        node_id,
        bucket,
        info: &info,
    };
    let access = load_access(context, scope, auth).await?;
    match ask {
        HolderAsk::Members { range_ids, queries } => {
            let config = MembersConfig {
                realm_id,
                node_id,
                bucket: bucket.to_string(),
                info: info.clone(),
                auth: auth.clone(),
                access,
                bearer: None,
                queries,
                ranges: Some(range_ids),
                retention: false,
            };
            let output = drive(ReleaseMembersOperation::new(config), context).await?;
            Ok(HolderAnswer::Members(RangeMembers {
                answers: output.answers,
                releases: output.releases,
                complete: output.complete,
                refused: output.refused,
            }))
        }
        HolderAsk::Files {
            range_id,
            prefix,
            skip,
            after,
        } => {
            let refused = RangeFiles {
                files: Vec::new(),
                refused: true,
                next: None,
            };
            let Some(binding) = &info.distribution else {
                return Ok(HolderAnswer::Files(refused));
            };
            let view = drive(read_view(binding.set_id), context)
                .await
                .map_err(|error| unavailable(&error))?
                .ok_or_else(|| PublishError::Unavailable("no record of the set".into()))?;
            let fence = drive(local_fence(binding.set_id), context)
                .await
                .map_err(|error| unavailable(&error))?;
            let snapshot = view.snapshot();
            let range = (snapshot.config.ranges.iter()).find(|range| range.range_id == range_id);
            let Some(range) =
                range.filter(|range| snapshot.lists_locally(binding.member_id, range, &fence))
            else {
                return Ok(HolderAnswer::Files(refused));
            };
            let asked = (prefix.as_str(), skip.as_str());
            let page = (Some(range), after);
            let (files, next) = release_files(context, scope, &access, asked, page).await?;
            Ok(HolderAnswer::Files(RangeFiles {
                files,
                refused: false,
                next,
            }))
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
                bucket: bucket.to_string(),
                key,
                version_id,
                group_id: info.group_id,
                auth: auth.clone(),
                realm_id,
                node_id,
            };
            let chunk = ObjectRangeRequest::StartEnd {
                start: offset,
                end: offset.saturating_add(CRATE_CHUNK - 1),
            };
            match read_part(context, read, Some(chunk)).await {
                Ok((found, size)) if found.metadata.contains_key(RELEASE_TAG) => {
                    let bytes = found.bytes;
                    Ok(HolderAnswer::Crate(Some(CrateChunk { bytes, size })))
                }
                Ok(_) | Err(CrateReadError::Missing) => Ok(HolderAnswer::Crate(None)),
                Err(CrateReadError::Failed(reason)) => Err(PublishError::Unavailable(reason)),
            }
        }
    }
}

/// Runs one release effect: a crate read with the caller's identity, or a holder request.
pub async fn dispatch_release(context: &DriverContext, effect: ReleaseEffect) -> ReleaseEvent {
    match effect {
        ReleaseEffect::ReadCrate(read) => ReleaseEvent::CrateRead(read_crate(context, *read).await),
        ReleaseEffect::Holder(request) => ReleaseEvent::Holder(ask_holder(context, request).await),
    }
}

/// Reads one release crate version on this node with the caller's identity.
pub async fn read_crate(
    context: &DriverContext,
    read: CrateRead,
) -> Result<CrateBytes, CrateReadError> {
    Ok(read_part(context, read, None).await?.0)
}

/// Reads an object, through a holder when this member keeps the record but not the bytes.
/// Boxed, since a routed read drives operations that may read release crates again.
fn routed_read(
    context: &DriverContext,
    input: GetObjectInput,
    restrictions: Option<Vec<PathRestriction>>,
) -> Pin<Box<dyn Future<Output = Result<GetObjectResult, GetObjectError>> + Send + '_>> {
    Box::pin(drive(
        GetRoutedOperation::new(input, restrictions, None),
        context,
    ))
}

/// Reads `range` of a release crate version, or all of it, and the version's whole size.
async fn read_part(
    context: &DriverContext,
    read: CrateRead,
    range: Option<ObjectRangeRequest>,
) -> Result<(CrateBytes, u64), CrateReadError> {
    let input = GetObjectInput {
        bucket: read.bucket,
        key: read.key,
        version_id: Some(read.version_id),
        range,
        group_id: read.group_id,
        user_identity: read.auth.user_id,
        realm_id: read.realm_id,
        node_id: read.node_id,
    };
    let restrictions = read.auth.path_restrictions.clone();
    let result = match routed_read(context, input, restrictions).await {
        Ok(result) => result,
        Err(
            GetObjectError::NoSuchKey
            | GetObjectError::NoSuchVersion
            | GetObjectError::DeleteMarker,
        ) => return Err(CrateReadError::Missing),
        Err(error) => return Err(CrateReadError::Failed(error.to_string())),
    };
    let size = result.info.size;
    let mut bytes = Vec::new();
    let mut blob = result.blob;
    while let Some(chunk) = blob.next().await {
        let chunk = chunk.map_err(|error| CrateReadError::Failed(error.to_string()))?;
        bytes.extend_from_slice(&chunk);
    }
    let found = CrateBytes {
        metadata: result.metadata,
        bytes,
    };
    Ok((found, size))
}

/// Asks a holder with the caller's bearer, which goes only to a realm infrastructure node.
async fn ask_holder(
    context: &DriverContext,
    request: HolderRequest,
) -> Result<HolderAnswer, String> {
    let handle = context
        .metadata_handle
        .as_ref()
        .ok_or("no metadata handle")?;
    let realm_id = (context.net_handle.as_ref())
        .map(|net| *net.realm_id())
        .ok_or("no realm")?;
    let config = drive(GetConfigOperation::new(realm_id), context)
        .await
        .map_err(|error| error.to_string())?;
    if !is_sync_eligible(&config, request.node_id) {
        return Err(format!("{} is no infrastructure node", request.node_id));
    }
    let token = AuthToken::bearer(request.bearer).map_err(|error| format!("{error:?}"))?;
    handle
        .request_release_holder(request.node_id, Some(token), request.bucket, request.ask)
        .await
        .map_err(|error| format!("{error:?}"))
}

#[cfg(test)]
mod tests {
    use super::super::{pinned_ids, upstream_of};
    use super::*;
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
}
