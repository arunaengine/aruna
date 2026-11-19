//! The steps of a run's host calls: the per call check, reads of datasets, the checks before a
//! metadata write or a connection request, and native work through the run's call history.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::entity_patch::craqle_patch_request;
use aruna_core::injection::OutgoingRequest;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::CallIntent;
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::structs::storage::data::identity::DataIdentity;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_plugins::broker::{Document, ErrorKind, FilePage, HostError, Listing};
use aruna_plugins::connection::Method;
use aruna_plugins::permission::Permission;
use craqle::GraphId;
use oxrdf::Term;
use serde_json::Value;
use ulid::Ulid;

use super::check::{Authorize, CallError, CheckCall, RunScope, matching};
use super::creates::write_outcome;
use super::dataset::{
    content_urls, dataset_change, dataset_crate, dataset_of, file_entities, parse_document,
    same_revision,
};
use super::grants::{install_scope, parse_grants, patterns_for, restricted_account};
use super::host_calls::{CallEnd, CallFailure, RecordCall, begun_reply};
use super::install::{InstallError, config_answer, config_call, pick_version, read_manifest};
use super::intents::{Begun, begin, written};
use super::job_work::StartWork;
use super::objects::LocalObject;
use super::outgoing::{outgoing, request_context};
use super::rate::take_call;
use super::rows::StoreError;
use super::submit::WorkRequest;
use super::unavailable;
use super::work::RequestContext;
use crate::forward::transport::MetadataWriteError;
use crate::jobs::records::pages::ServingNode;
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export_types::{
    ExportMetadataRequest, ExportMetadataResult, RoCrateExportView,
};
use crate::metadata::api::list::ListVisibleOperation;
use crate::metadata::api::list_types::{ListVisibleRequest, ListVisibleResult, MetadataListOrder};
use crate::metadata::create_document::CreateDocumentPayload;
use crate::metadata::forward::authorized_create::CreateAuthorizedError;
use crate::metadata::forward::pinned::{PinnedCreate, PinnedMintOperation};
use crate::metadata::forward::routed::export::RoutedExportOperation;
use crate::metadata::forward::routed::update::{DocumentUpdate, RouteUpdateOperation};
use crate::metadata::update_document::UpdateDocumentMutation;
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};

/// Entries of one metadata listing page.
const LIST_PAGE: usize = 100;
/// File entities one page asks for; a page over the byte bound comes back smaller.
const FILE_PAGE: usize = 100;
/// Bytes of a create key at most.
const MAX_KEY: usize = 256;

fn invalid(message: &str) -> HostError {
    HostError::new(ErrorKind::Invalid, message)
}

/// A read error as the guest sees it; an unreachable or not yet visible document is retryable.
fn api_error(error: MetadataApiError) -> CallError {
    CallError(match error {
        MetadataApiError::NotFound => HostError::new(ErrorKind::NotFound, "no such document"),
        MetadataApiError::TooLarge => {
            HostError::new(ErrorKind::Limit, "one entity is larger than a reply may be")
        }
        MetadataApiError::Unauthorized | MetadataApiError::Forbidden => {
            HostError::new(ErrorKind::Denied, "the account may not read this")
        }
        MetadataApiError::BadRequest
        | MetadataApiError::InvalidCursor(_)
        | MetadataApiError::InvalidQuery(_) => {
            HostError::new(ErrorKind::Invalid, error.to_string())
        }
        other => unavailable(other.to_string()),
    })
}

/// Steps 0 to 2 and 5: the run may call now, within the install's call rate.
#[derive(Debug, PartialEq)]
pub struct AdmitCall {
    scope: RunScope,
    target: Option<(Permission, String)>,
}

impl AdmitCall {
    pub fn new(scope: &RunScope, target: Option<(Permission, &str)>) -> Self {
        Self {
            scope: scope.clone(),
            target: target.map(|(permission, path)| (permission, path.to_string())),
        }
    }
}

fn admitted_install(result: IoResult) -> Result<PluginInstall, CallError> {
    let install = result.child::<PluginInstall, CallError>()??;
    match take_call(install.install_id, Instant::now()) {
        true => Ok(install),
        false => Err(CallError(HostError::new(
            ErrorKind::Limit,
            "the install makes calls too fast",
        ))),
    }
}

impl Operation for AdmitCall {
    type Output = PluginInstall;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<PluginInstall>, CallError> {
        let target = (self.target.as_ref()).map(|(permission, path)| (*permission, path.as_str()));
        let check = CheckCall::new(&self.scope, target);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PluginInstall>, CallError> {
        Ok(Next::Done(admitted_install(result)?))
    }
}

/// An admitted call with the account restricted to the grants it uses.
#[derive(Clone, Debug, PartialEq)]
pub struct Granted {
    pub install: PluginInstall,
    pub auth: AuthContext,
}

/// Steps 0 to 4 for a target, or steps 0 to 2 for a listing, and the restricted account.
#[derive(Debug, PartialEq)]
pub struct GrantedCall {
    scope: RunScope,
    permission: Permission,
    target: Option<String>,
}

impl GrantedCall {
    /// The call on `target`, under the grants that cover it.
    pub fn target(scope: &RunScope, permission: Permission, target: &str) -> Self {
        Self {
            scope: scope.clone(),
            permission,
            target: Some(target.to_string()),
        }
    }

    /// A listing, under every grant of `permission`.
    pub fn listing(scope: &RunScope, permission: Permission) -> Self {
        Self {
            scope: scope.clone(),
            permission,
            target: None,
        }
    }
}

impl Operation for GrantedCall {
    type Output = Granted;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<Granted>, CallError> {
        let target = (self.target.as_deref()).map(|target| (self.permission, target));
        let check = CheckCall::new(&self.scope, target);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Granted>, CallError> {
        let install = admitted_install(result)?;
        let patterns = match &self.target {
            Some(target) => matching(&install, self.permission, target)?,
            None => patterns_for(&parse_grants(&install.grants), self.permission),
        };
        let realm_id = self.scope.realm_id;
        let auth = restricted_account((&install, realm_id), &patterns, self.permission);
        Ok(Next::Done(Granted { install, auth }))
    }
}

fn granted(result: IoResult) -> Result<Granted, CallError> {
    result.child::<Granted, CallError>()?
}

fn granted_call(call: GrantedCall) -> IoCall {
    IoCall::child(call)
}

/// Where a run reads documents from.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadPlace {
    pub scope: RunScope,
    pub network_node: Option<NodeId>,
    pub max_bytes: usize,
}

impl ReadPlace {
    fn export(
        &self,
        (document_id, view): (Ulid, RoCrateExportView),
        auth: AuthContext,
        (limit, after): (Option<usize>, Option<String>),
    ) -> IoCall {
        let request = ExportMetadataRequest {
            document_id,
            auth: Some(auth.clone()),
            view,
            limit,
            offset: None,
            after,
        };
        let token = Some(AuthToken::Internal(auth));
        let realm_id = self.scope.realm_id;
        // The holder counts the cursor and the room around each entity in this bound.
        let bound = self.max_bytes as u64;
        let export = RoutedExportOperation::new(self.network_node, realm_id, request, token, bound);
        IoCall::child(export)
    }
}

fn exported(result: IoResult) -> Result<ExportMetadataResult, CallError> {
    (result.child::<ExportMetadataResult, MetadataApiError>()?).map_err(api_error)
}

/// The registry record and JSON-LD of a whole or summary export.
fn document_of(
    export: ExportMetadataResult,
) -> Result<(MetadataRegistryRecord, String), CallError> {
    match export {
        ExportMetadataResult::Full { record, jsonld }
        | ExportMetadataResult::Summary { record, jsonld } => Ok((record, jsonld)),
        _ => Err(CallError(unavailable("the document has no such export"))),
    }
}

#[derive(Debug, PartialEq)]
enum ReadPhase {
    Listing,
    Export,
    Check(Box<MetadataRegistryRecord>, String),
}

/// The dataset entity of a document from the given export view, checked under the read grants.
#[derive(Debug, PartialEq)]
pub struct ReadDataset {
    place: ReadPlace,
    resource: String,
    view: RoCrateExportView,
    phase: ReadPhase,
}

impl ReadDataset {
    pub fn new(place: ReadPlace, resource: &str, view: RoCrateExportView) -> Self {
        Self {
            place,
            resource: resource.to_string(),
            view,
            phase: ReadPhase::Listing,
        }
    }
}

impl Operation for ReadDataset {
    type Output = Document;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<Document>, CallError> {
        parse_document(&self.resource)?;
        let listing = GrantedCall::listing(&self.place.scope, Permission::MetadataRead);
        Ok(Next::Call(granted_call(listing)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Document>, CallError> {
        match std::mem::replace(&mut self.phase, ReadPhase::Export) {
            ReadPhase::Listing => {
                let auth = granted(result)?.auth;
                let document = parse_document(&self.resource)?;
                let read = (document, self.view);
                Ok(Next::Call(self.place.export(read, auth, (None, None))))
            }
            ReadPhase::Export => {
                let (record, jsonld) = document_of(exported(result)?)?;
                let path = &record.permission_path;
                let scope = &self.place.scope;
                let check = GrantedCall::target(scope, Permission::MetadataRead, path);
                self.phase = ReadPhase::Check(Box::new(record), jsonld);
                Ok(Next::Call(granted_call(check)))
            }
            ReadPhase::Check(record, jsonld) => {
                granted(result)?;
                Ok(Next::Done(Document {
                    resource: self.resource.clone(),
                    revision: record.last_event_id.to_string(),
                    json: dataset_of(&jsonld)?,
                }))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum FilesPhase {
    Listing,
    Export,
    Check(PageDraft),
}

/// A page of file entities and its cursor, kept while the path is checked.
#[derive(Debug, PartialEq)]
struct PageDraft {
    jsonld: String,
    cursor: Option<String>,
}

/// One page of a document's file entities.
#[derive(Debug, PartialEq)]
pub struct ReadFiles {
    place: ReadPlace,
    resource: String,
    cursor: Option<String>,
    phase: FilesPhase,
}

impl ReadFiles {
    pub fn new(place: ReadPlace, resource: &str, cursor: Option<String>) -> Self {
        Self {
            place,
            resource: resource.to_string(),
            cursor,
            phase: FilesPhase::Listing,
        }
    }
}

impl Operation for ReadFiles {
    type Output = FilePage;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<FilePage>, CallError> {
        parse_document(&self.resource)?;
        let listing = GrantedCall::listing(&self.place.scope, Permission::MetadataRead);
        Ok(Next::Call(granted_call(listing)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<FilePage>, CallError> {
        match std::mem::replace(&mut self.phase, FilesPhase::Export) {
            FilesPhase::Listing => {
                let auth = granted(result)?.auth;
                let read = (parse_document(&self.resource)?, RoCrateExportView::Page);
                let page = (Some(FILE_PAGE), self.cursor.take());
                Ok(Next::Call(self.place.export(read, auth, page)))
            }
            FilesPhase::Export => {
                let ExportMetadataResult::Page { record, page } = exported(result)? else {
                    return Err(CallError(unavailable("the document has no page export")));
                };
                let path = &record.permission_path;
                let scope = &self.place.scope;
                let check = GrantedCall::target(scope, Permission::MetadataRead, path);
                self.phase = FilesPhase::Check(PageDraft {
                    jsonld: page.jsonld,
                    cursor: page.next_cursor,
                });
                Ok(Next::Call(granted_call(check)))
            }
            FilesPhase::Check(page) => {
                granted(result)?;
                Ok(Next::Done(FilePage {
                    files: file_entities(&page.jsonld)?,
                    cursor: page.cursor,
                }))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum ListPhase {
    Listing,
    Documents(Box<PluginInstall>),
    Checking(Box<PluginInstall>),
}

/// One page of the documents the account may read now: the grants must cover each and it must
/// pass the same authorization as an individual call. Only an unavailable check fails.
#[derive(Debug, PartialEq)]
pub struct ListDatasets {
    scope: RunScope,
    cursor: Option<String>,
    offset: usize,
    documents: VecDeque<(String, String)>,
    current: Option<String>,
    resources: Vec<String>,
    next_cursor: Option<String>,
    phase: ListPhase,
}

impl ListDatasets {
    pub fn new(scope: &RunScope, cursor: Option<String>) -> Self {
        Self {
            scope: scope.clone(),
            cursor,
            offset: 0,
            documents: VecDeque::new(),
            current: None,
            resources: Vec::new(),
            next_cursor: None,
            phase: ListPhase::Listing,
        }
    }

    fn advance(&mut self, install: Box<PluginInstall>) -> Next<Listing> {
        while let Some((path, id)) = self.documents.pop_front() {
            let permission = Permission::MetadataRead;
            let Ok(patterns) = matching(&install, permission, &path) else {
                continue;
            };
            let check = Authorize::call(&install, permission, &path, &patterns);
            self.current = Some(id);
            self.phase = ListPhase::Checking(install);
            return Next::Call(IoCall::child(check));
        }
        Next::Done(Listing {
            resources: std::mem::take(&mut self.resources),
            cursor: self.next_cursor.take(),
        })
    }
}

impl Operation for ListDatasets {
    type Output = Listing;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<Listing>, CallError> {
        let listing = GrantedCall::listing(&self.scope, Permission::MetadataRead);
        Ok(Next::Call(granted_call(listing)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Listing>, CallError> {
        match std::mem::replace(&mut self.phase, ListPhase::Listing) {
            ListPhase::Listing => {
                let Granted { install, auth } = granted(result)?;
                let offset = self.cursor.as_deref().map(str::parse::<usize>).transpose();
                self.offset = offset
                    .map_err(|_| CallError(invalid("bad cursor")))?
                    .unwrap_or(0);
                let request = ListVisibleRequest {
                    group_id: Some(install.group_id),
                    path_prefix: None,
                    include_summary: false,
                    limit: Some(LIST_PAGE),
                    offset: Some(self.offset),
                    order: MetadataListOrder::Created,
                    auth: Some(auth),
                };
                self.phase = ListPhase::Documents(Box::new(install));
                let list = ListVisibleOperation::new(self.scope.realm_id, request);
                Ok(Next::Call(IoCall::child(list)))
            }
            ListPhase::Documents(install) => {
                let listed = result.child::<ListVisibleResult, MetadataApiError>()?;
                let listed = listed.map_err(api_error)?;
                let count = listed.documents.len();
                let next = (count >= LIST_PAGE).then(|| (self.offset + count).to_string());
                self.next_cursor = next;
                self.documents = (listed.documents.iter())
                    .map(|document| {
                        let record = &document.record;
                        let id = record.document_id.to_string();
                        (record.permission_path.clone(), id)
                    })
                    .collect();
                Ok(self.advance(install))
            }
            ListPhase::Checking(install) => {
                let id = self.current.take().ok_or(IoError::Unfinished)?;
                match result.child::<(), CallError>()? {
                    Ok(()) => self.resources.push(id),
                    Err(CallError(error)) if error.kind == ErrorKind::Unavailable => {
                        return Err(CallError(error));
                    }
                    Err(_) => {}
                }
                Ok(self.advance(install))
            }
        }
    }
}

/// The objects an entity names, and the content hash they must have.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct NamedObjects {
    objects: Vec<(String, Option<Ulid>)>,
    hash: Option<[u8; 32]>,
}

/// Reads what an entity names. Export renders each stored value as is, without `./`, or as
/// literal text, under any term the context gives, so every value of every predicate counts.
pub(crate) fn named_objects(
    scope: &RunScope,
    group: aruna_core::types::GroupId,
    (graph, file): (&str, &Value),
) -> Result<NamedObjects, HostError> {
    let realm_id = scope.realm_id;
    let converted = craqle_patch_request(&GraphId::new(graph), &file.to_string());
    let converted =
        converted.map_err(|error| HostError::new(ErrorKind::Invalid, error.to_string()))?;
    let entity = converted.entity;
    let objects = (entity.additional_triples.iter()).filter_map(|(_, object)| match object {
        Term::NamedNode(node) => Some(node.as_str().to_string()),
        Term::Literal(literal) => Some(literal.value().to_string()),
        _ => None,
    });
    let own = [&entity.entity_id, &entity.entity_type, &entity.name].map(String::clone);
    let stored: Vec<String> = own.into_iter().chain(objects).collect();
    let bare = stored.iter().filter_map(|value| value.strip_prefix("./"));
    let bare: Vec<String> = bare.map(str::to_string).collect();
    let values: Vec<String> = stored.into_iter().chain(bare).collect();
    let refused = |what: &str| {
        let message = format!("{what} names no object a plugin may check; use its path");
        HostError::new(ErrorKind::Denied, message)
    };
    // A content hash or global id names no object alone; a concrete one must come with it.
    let (mut named, mut hash, mut unplaced) = (Vec::new(), None, None);
    for value in values.iter().map(String::as_str) {
        let identity = DataIdentity::read(value, &[]);
        if value.starts_with(&format!("/{realm_id}/g/")) {
            named.push((value.to_string(), None));
        } else if let Some(exact) = identity.exact {
            if exact.realm_id != realm_id {
                return Err(refused(value));
            }
            let (node, bucket, key) = (exact.node_id, &exact.bucket, &exact.key);
            let path = object_permission_path(realm_id, group, node, bucket, key);
            named.push((path, Some(exact.version)));
        } else if let Some(at) = identity.location {
            let local = scope.local;
            let path = object_permission_path(realm_id, group, local, &at.bucket, &at.key);
            named.push((path, None));
        } else if identity.is_aruna() {
            // Readers keep one hash of a file, so two different ones are never admitted.
            if let Some(found) = identity.hash {
                if hash.is_some_and(|known| known != found) {
                    return Err(invalid("the file names two different content hashes"));
                }
                hash = Some(found);
            }
            unplaced.get_or_insert(value);
        }
    }
    if let (Some(value), true) = (unplaced, named.is_empty()) {
        return Err(refused(value));
    }
    Ok(NamedObjects {
        objects: named,
        hash,
    })
}

#[derive(Debug, PartialEq)]
enum ObjectPhase {
    Granted(String, Option<Ulid>),
    Located(Option<Ulid>),
    Content,
}

/// Checks each Aruna object an entity names through the plugin's own `objects.read` call check
/// and that it lies in a group bucket here. A named hash must be the object's verified digest.
#[derive(Debug, PartialEq)]
pub(crate) struct CheckObjects {
    scope: RunScope,
    install: PluginInstall,
    objects: VecDeque<(String, Option<Ulid>)>,
    hash: Option<[u8; 32]>,
    phase: Option<ObjectPhase>,
}

impl CheckObjects {
    pub(crate) fn new(scope: &RunScope, install: &PluginInstall, named: NamedObjects) -> Self {
        Self {
            scope: scope.clone(),
            install: install.clone(),
            objects: named.objects.into(),
            hash: named.hash,
            phase: None,
        }
    }

    fn advance(&mut self) -> Next<()> {
        let Some((path, version)) = self.objects.pop_front() else {
            return Next::Done(());
        };
        let read = Permission::ObjectsRead;
        let check = GrantedCall::target(&self.scope, read, &path);
        self.phase = Some(ObjectPhase::Granted(path, version));
        Next::Call(granted_call(check))
    }

    /// Admits the hash only when the object's verified BLAKE3 digest is recorded and equal; a
    /// pending upload without one cannot vouch for any hash.
    fn same_content(&self, result: IoResult) -> Result<(), CallError> {
        let head = result.child::<HeadObjectResult, HeadObjectError>()?;
        let gone = |_| CallError(HostError::new(ErrorKind::NotFound, "the object is gone"));
        let head = head.map_err(gone)?;
        let stored = (head.location).and_then(|at| at.hashes.get(HASH_BLAKE3).cloned());
        match stored {
            None => Err(CallError(invalid(
                "the object has no verified digest for the file's hash id yet",
            ))),
            Some(stored) if self.hash.is_some_and(|hash| stored != hash) => Err(CallError(
                invalid("the object's content does not match the file's hash id"),
            )),
            Some(_) => Ok(()),
        }
    }
}

impl Operation for CheckObjects {
    type Output = ();
    type Error = CallError;

    fn first(&mut self) -> Result<Next<()>, CallError> {
        Ok(self.advance())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, CallError> {
        match self.phase.take().ok_or(IoError::Unfinished)? {
            ObjectPhase::Granted(path, version) => {
                granted(result)?;
                let place = (self.scope.realm_id, self.scope.local);
                let locate = LocalObject::new(place, &self.install, &path);
                self.phase = Some(ObjectPhase::Located(version));
                Ok(Next::Call(IoCall::child(locate)))
            }
            ObjectPhase::Located(version_id) => {
                let (bucket, key) = result.child::<(String, String), CallError>()??;
                if self.hash.is_none() {
                    return Ok(self.advance());
                }
                let head = HeadObjectOperation::new(HeadObjectInput {
                    bucket,
                    key,
                    version_id,
                });
                self.phase = Some(ObjectPhase::Content);
                Ok(Next::Call(IoCall::child(head)))
            }
            ObjectPhase::Content => {
                self.same_content(result)?;
                Ok(self.advance())
            }
        }
    }
}

/// What a metadata write changes.
#[derive(Debug, PartialEq)]
pub enum ChangeInput {
    /// A JSON merge patch of the dataset entity.
    Patch(String),
    /// A file entity, checked when one is written; a removal names none.
    File(Option<Value>),
}

/// A checked metadata write, ready to be routed.
#[derive(Debug, PartialEq)]
pub struct ReadyChange {
    pub record: Box<MetadataRegistryRecord>,
    pub granted: Granted,
    pub document_id: Ulid,
    pub expected: Ulid,
    /// The entity a patch writes.
    pub entity: Option<String>,
}

/// How the checks before a mutating call ended.
#[derive(Debug, PartialEq)]
pub enum CallStep<T> {
    /// The same call settled before; its stored reply answers it.
    Replied(String),
    Proceed(T),
}

#[derive(Debug, PartialEq)]
enum ChangePhase {
    Listing,
    Export,
    Check,
    Objects,
    Written,
    Begin,
}

/// Checks a revision-checked metadata write under the write grants. The target is resolved
/// first, and a new call starts only from the copy it expects; an earlier attempt's intent
/// decides otherwise. An invalid change is refused before its intent exists.
#[derive(Debug, PartialEq)]
pub struct PrepareChange {
    place: ReadPlace,
    document_id: Ulid,
    expected: Ulid,
    intent: CallIntent,
    input: ChangeInput,
    exported: Option<(Box<MetadataRegistryRecord>, String)>,
    granted: Option<Granted>,
    entity: Option<String>,
    phase: ChangePhase,
}

impl PrepareChange {
    pub fn new(
        place: ReadPlace,
        (resource, expected): (&str, &str),
        intent: CallIntent,
        input: ChangeInput,
    ) -> Result<Self, HostError> {
        Ok(Self {
            place,
            document_id: parse_document(resource)?,
            expected: Ulid::from_string(expected).map_err(|_| invalid("bad revision"))?,
            intent,
            input,
            exported: None,
            granted: None,
            entity: None,
            phase: ChangePhase::Listing,
        })
    }

    fn run_id(&self) -> Ulid {
        self.place.scope.run.run_id
    }

    fn written_call(&mut self) -> Next<CallStep<ReadyChange>> {
        self.phase = ChangePhase::Written;
        let check = written(self.run_id(), &self.intent);
        Next::Call(IoCall::child(check))
    }

    fn checked(&mut self) -> Result<Next<CallStep<ReadyChange>>, CallFailure> {
        let ChangeInput::File(Some(file)) = &self.input else {
            return Ok(self.written_call());
        };
        content_urls(file)?;
        let exported = self.exported.as_ref().ok_or(IoError::Unfinished)?;
        let granted = self.granted.as_ref().ok_or(IoError::Unfinished)?;
        let scope = &self.place.scope;
        let graph = exported.0.graph_iri.as_str();
        let named = named_objects(scope, granted.install.group_id, (graph, file))?;
        let check = CheckObjects::new(scope, &granted.install, named);
        self.phase = ChangePhase::Objects;
        Ok(Next::Call(IoCall::child(check)))
    }

    fn ready(&mut self) -> Result<ReadyChange, IoError> {
        let (record, _) = self.exported.take().ok_or(IoError::Unfinished)?;
        Ok(ReadyChange {
            record,
            granted: self.granted.take().ok_or(IoError::Unfinished)?,
            document_id: self.document_id,
            expected: self.expected,
            entity: self.entity.take(),
        })
    }
}

impl Operation for PrepareChange {
    type Output = CallStep<ReadyChange>;
    type Error = CallFailure;

    fn first(&mut self) -> Result<Next<Self::Output>, CallFailure> {
        let listing = GrantedCall::listing(&self.place.scope, Permission::MetadataWrite);
        Ok(Next::Call(granted_call(listing)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CallFailure> {
        match self.phase {
            ChangePhase::Listing => {
                let auth = granted(result)?.auth;
                // The summary has no file links, so a large dataset is not exported whole.
                let read = (self.document_id, RoCrateExportView::Summary);
                self.phase = ChangePhase::Export;
                Ok(Next::Call(self.place.export(read, auth, (None, None))))
            }
            ChangePhase::Export => {
                let (record, jsonld) = document_of(exported(result)?)?;
                let scope = &self.place.scope;
                let write = Permission::MetadataWrite;
                let check = GrantedCall::target(scope, write, &record.permission_path);
                self.exported = Some((Box::new(record), jsonld));
                self.phase = ChangePhase::Check;
                Ok(Next::Call(granted_call(check)))
            }
            ChangePhase::Check => {
                self.granted = Some(granted(result)?);
                self.checked()
            }
            ChangePhase::Objects => {
                result.child::<(), CallError>()??;
                Ok(self.written_call())
            }
            ChangePhase::Written => {
                let earlier = result.child::<bool, StoreError>()??;
                let (record, jsonld) = self.exported.as_ref().ok_or(IoError::Unfinished)?;
                if !earlier {
                    same_revision(record.last_event_id, self.expected)?;
                }
                if let ChangeInput::Patch(patch) = &self.input {
                    self.entity = Some(dataset_change(jsonld, patch)?);
                }
                self.phase = ChangePhase::Begin;
                let begin = begin(self.run_id(), self.intent.clone());
                Ok(Next::Call(IoCall::child(begin)))
            }
            ChangePhase::Begin => {
                let begun = result.child::<Begun, StoreError>()??;
                Ok(Next::Done(match begun_reply(begun)? {
                    Some(reply) => CallStep::Replied(reply),
                    None => CallStep::Proceed(self.ready()?),
                }))
            }
        }
    }
}

/// Routes a checked metadata write and records its outcome in the call's intent.
#[derive(Debug, PartialEq)]
pub struct ApplyChange {
    route: Option<RouteUpdateOperation>,
    run_id: Ulid,
    intent: CallIntent,
    recording: bool,
}

impl ApplyChange {
    pub fn new(
        (scope, network_node): (&RunScope, Option<NodeId>),
        ready: ReadyChange,
        mutation: UpdateDocumentMutation,
        intent: CallIntent,
    ) -> Self {
        let Granted { install, auth } = ready.granted;
        let run_id = scope.run.run_id;
        let actor = Actor {
            node_id: scope.local,
            user_id: install.account,
            realm_id: scope.realm_id,
        };
        let update = DocumentUpdate {
            public: None,
            mutation,
            expected_revision: Some(ready.expected),
            commit_message: Some(change_message(&install, run_id)),
        };
        let route = RouteUpdateOperation::new(
            (actor, network_node),
            (Some(*ready.record), ready.document_id),
            update,
            Some(AuthToken::Internal(auth)),
        );
        Self {
            route: Some(route),
            run_id,
            intent,
            recording: false,
        }
    }
}

impl Operation for ApplyChange {
    type Output = String;
    type Error = CallFailure;

    fn first(&mut self) -> Result<Next<String>, CallFailure> {
        let route = self.route.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(route)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<String>, CallFailure> {
        if self.recording {
            return Ok(Next::Done(result.child::<String, CallFailure>()??));
        }
        let routed = result.child::<MetadataRegistryRecord, MetadataWriteError>()?;
        let outcome =
            (routed.map(|record| record.last_event_id.to_string())).map_err(write_outcome);
        self.recording = true;
        let record = RecordCall::new(self.run_id, self.intent.clone(), outcome);
        Ok(Next::Call(IoCall::child(record)))
    }
}

/// A new document's call, checked and with its id and node pinned; `minted` says why not.
#[derive(Debug, PartialEq)]
pub struct ReadyCreate {
    pub actor: Actor,
    pub granted: Granted,
    pub minted: Result<Box<PinnedCreate>, String>,
}

#[derive(Debug, PartialEq)]
enum CreatePhase {
    Admit,
    Check,
    Objects,
    Begin,
    Mint,
}

/// Checks a create under the create grants and pins its document. An invalid dataset is refused
/// before its intent exists, so it leaves no uncertainty.
#[derive(Debug, PartialEq)]
pub struct PrepareCreate {
    scope: RunScope,
    path: String,
    dataset: String,
    intent: CallIntent,
    jsonld: Option<String>,
    granted: Option<Granted>,
    phase: CreatePhase,
}

impl PrepareCreate {
    pub fn new(
        scope: &RunScope,
        (path, key): (&str, &str),
        dataset: &str,
        intent: CallIntent,
    ) -> Result<Self, HostError> {
        let path = MetadataRegistryRecord::normalize_document_path(path);
        if path.is_empty() || path.contains('@') {
            return Err(invalid("a dataset path is not empty and has no @"));
        }
        if key.is_empty() || key.len() > MAX_KEY {
            return Err(invalid("a create key has 1 to 256 bytes"));
        }
        Ok(Self {
            scope: scope.clone(),
            path,
            dataset: dataset.to_string(),
            intent,
            jsonld: None,
            granted: None,
            phase: CreatePhase::Admit,
        })
    }

    fn begin_call(&mut self) -> Next<CallStep<ReadyCreate>> {
        self.phase = CreatePhase::Begin;
        let begin = begin(self.scope.run.run_id, self.intent.clone());
        Next::Call(IoCall::child(begin))
    }

    fn actor(&self, install: &PluginInstall) -> Actor {
        Actor {
            node_id: self.scope.local,
            user_id: install.account,
            realm_id: self.scope.realm_id,
        }
    }

    fn mint(&mut self) -> Result<Next<CallStep<ReadyCreate>>, CallFailure> {
        let install = &self.granted.as_ref().ok_or(IoError::Unfinished)?.install;
        let jsonld = self.jsonld.take().ok_or(IoError::Unfinished)?;
        let message = change_message(install, self.scope.run.run_id);
        let payload = (CreateDocumentPayload::RoCrate { jsonld }, Some(message));
        let place = (install.group_id, self.path.as_str());
        let mint = PinnedMintOperation::new(self.actor(install), place, payload);
        self.phase = CreatePhase::Mint;
        Ok(Next::Call(IoCall::child(mint)))
    }
}

/// The commit message of a run's metadata change.
pub(crate) fn change_message(install: &PluginInstall, run_id: Ulid) -> String {
    let (plugin, id) = (&install.plugin, install.install_id);
    format!("plugin {plugin} install {id} run {run_id}")
}

impl Operation for PrepareCreate {
    type Output = CallStep<ReadyCreate>;
    type Error = CallFailure;

    fn first(&mut self) -> Result<Next<Self::Output>, CallFailure> {
        let admit = AdmitCall::new(&self.scope, None);
        Ok(Next::Call(IoCall::child(admit)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CallFailure> {
        match self.phase {
            CreatePhase::Admit => {
                let current = result.child::<PluginInstall, CallError>()??;
                let (realm_id, group) = (self.scope.realm_id, current.group_id);
                let target = format!("/{realm_id}/g/{group}/meta/{}", self.path);
                let create = Permission::MetadataCreate;
                self.phase = CreatePhase::Check;
                let check = GrantedCall::target(&self.scope, create, &target);
                Ok(Next::Call(granted_call(check)))
            }
            CreatePhase::Check => {
                let access = granted(result)?;
                let jsonld = dataset_crate(&self.dataset)?;
                // The entity is checked as stored; the new graph's name is not known yet.
                let stored: Value = serde_json::from_str(&jsonld).unwrap_or(Value::Null);
                let graph = MetadataRegistryRecord::graph_iri_for(Ulid::nil());
                let group = access.install.group_id;
                let entity = (graph.as_str(), &stored["@graph"][1]);
                let named = named_objects(&self.scope, group, entity)?;
                let check = CheckObjects::new(&self.scope, &access.install, named);
                self.jsonld = Some(jsonld);
                self.granted = Some(access);
                self.phase = CreatePhase::Objects;
                Ok(Next::Call(IoCall::child(check)))
            }
            CreatePhase::Objects => {
                result.child::<(), CallError>()??;
                Ok(self.begin_call())
            }
            CreatePhase::Begin => {
                let begun = result.child::<Begun, StoreError>()??;
                match begun_reply(begun)? {
                    Some(reply) => Ok(Next::Done(CallStep::Replied(reply))),
                    None => self.mint(),
                }
            }
            CreatePhase::Mint => {
                let minted = result.child::<PinnedCreate, CreateAuthorizedError>()?;
                let granted = self.granted.take().ok_or(IoError::Unfinished)?;
                Ok(Next::Done(CallStep::Proceed(ReadyCreate {
                    actor: self.actor(&granted.install),
                    granted,
                    minted: minted.map(Box::new).map_err(|error| error.to_string()),
                })))
            }
        }
    }
}

/// The admitted install with the realm config a secret or connection is decided on.
#[derive(Debug, PartialEq)]
pub struct ConfiguredCall {
    pub install: PluginInstall,
    pub config: Arc<RealmConfigDocument>,
}

fn config_failure(error: InstallError) -> CallError {
    CallError(unavailable(error.to_string()))
}

#[derive(Debug, PartialEq)]
enum ConfiguredPhase {
    Admit,
    Check,
    Config(Box<PluginInstall>),
}

/// The per call check of the path `path_of` names for the admitted install, then the config.
#[derive(Debug, PartialEq)]
pub struct CheckConfigured {
    scope: RunScope,
    permission: Permission,
    name: String,
    phase: ConfiguredPhase,
}

impl CheckConfigured {
    /// Sending through the connection `name`.
    pub fn connection(scope: &RunScope, name: &str) -> Self {
        Self::of(scope, Permission::ConnectionsSend, name)
    }

    /// Reading the value of the secret slot `name`.
    pub fn secret(scope: &RunScope, name: &str) -> Self {
        Self::of(scope, Permission::SecretsValue, name)
    }

    fn of(scope: &RunScope, permission: Permission, name: &str) -> Self {
        Self {
            scope: scope.clone(),
            permission,
            name: name.to_string(),
            phase: ConfiguredPhase::Admit,
        }
    }

    fn path(&self, install: &PluginInstall) -> String {
        let scope = install_scope(install);
        match self.permission {
            Permission::SecretsValue => scope.secret(&self.name),
            _ => scope.connection(&self.name),
        }
    }
}

impl Operation for CheckConfigured {
    type Output = ConfiguredCall;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<ConfiguredCall>, CallError> {
        let admit = AdmitCall::new(&self.scope, None);
        Ok(Next::Call(IoCall::child(admit)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ConfiguredCall>, CallError> {
        match std::mem::replace(&mut self.phase, ConfiguredPhase::Check) {
            ConfiguredPhase::Admit => {
                let current = result.child::<PluginInstall, CallError>()??;
                let path = self.path(&current);
                let check = GrantedCall::target(&self.scope, self.permission, &path);
                Ok(Next::Call(granted_call(check)))
            }
            ConfiguredPhase::Check => {
                let install = granted(result)?.install;
                self.phase = ConfiguredPhase::Config(Box::new(install));
                Ok(Next::Call(config_call(self.scope.realm_id)))
            }
            ConfiguredPhase::Config(install) => {
                let config = config_answer(result).map_err(config_failure)?;
                Ok(Next::Done(ConfiguredCall {
                    install: *install,
                    config: Arc::new(config),
                }))
            }
        }
    }
}

/// The address and guest parts of a connection request.
#[derive(Clone, Debug, PartialEq)]
pub struct ConnectionAsk {
    pub connection: String,
    pub method: Method,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
}

/// A checked connection request before its secret is released.
#[derive(Debug, PartialEq)]
pub struct ConnectionPlan {
    pub call: ConfiguredCall,
    pub context: RequestContext,
    pub outgoing: OutgoingRequest,
    pub slot: Option<String>,
}

/// The per call check, the address below the connection and the guest's own headers. The guest
/// built its request from the settings saved at admission, so the address follows them, while
/// the grant and release checks use the current install.
#[derive(Debug, PartialEq)]
pub struct PrepareConnection {
    check: CheckConfigured,
    settings: String,
    ask: ConnectionAsk,
}

impl PrepareConnection {
    pub fn new(scope: &RunScope, ask: ConnectionAsk) -> Self {
        Self {
            check: CheckConfigured::connection(scope, &ask.connection),
            settings: scope.run.settings.clone(),
            ask,
        }
    }

    fn plan(&self, call: ConfiguredCall) -> Result<ConnectionPlan, HostError> {
        let manifest = current_manifest(&call).map_err(|error| error.0)?;
        let ask = &self.ask;
        let called = (ask.connection.as_str(), ask.method);
        let parts = (ask.path.as_str(), &ask.query[..], &ask.headers[..]);
        let context = request_context(&manifest, called, &self.settings, parts)?;
        let (outgoing, slot) = outgoing(&manifest, &context)?;
        Ok(ConnectionPlan {
            call,
            context,
            outgoing,
            slot,
        })
    }
}

fn current_manifest(call: &ConfiguredCall) -> Result<aruna_plugins::manifest::Manifest, CallError> {
    let install = &call.install;
    let plugin = pick_version(&call.config, &install.plugin, Some(&install.digest));
    let manifest = plugin.and_then(|plugin| read_manifest(&plugin));
    manifest.map_err(config_failure)
}

impl Operation for PrepareConnection {
    type Output = ConnectionPlan;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<ConnectionPlan>, CallError> {
        match self.check.first()? {
            Next::Call(call) => Ok(Next::Call(call)),
            _ => Err(IoError::Unfinished.into()),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ConnectionPlan>, CallError> {
        match self.check.next(result)? {
            Next::Call(call) => Ok(Next::Call(call)),
            Next::Done(call) => Ok(Next::Done(self.plan(call)?)),
            _ => Err(IoError::Unfinished.into()),
        }
    }
}

/// A kept reply's request as it would go now: its secret still has to release.
#[derive(Debug, PartialEq)]
pub struct ReplyPlan {
    pub call: ConfiguredCall,
    pub outgoing: OutgoingRequest,
    pub slot: Option<String>,
}

/// Whether a kept reply of `context` may still be considered: the connection is still granted
/// and declared. `None` when it is not.
#[derive(Debug, PartialEq)]
pub struct CheckReply {
    check: CheckConfigured,
    context: RequestContext,
}

impl CheckReply {
    pub fn new(scope: &RunScope, context: &RequestContext) -> Self {
        Self {
            check: CheckConfigured::connection(scope, &context.connection),
            context: context.clone(),
        }
    }

    fn plan(&self, call: ConfiguredCall) -> Option<ReplyPlan> {
        let manifest = current_manifest(&call).ok()?;
        let (outgoing, slot) = outgoing(&manifest, &self.context).ok()?;
        Some(ReplyPlan {
            call,
            outgoing,
            slot,
        })
    }
}

impl Operation for CheckReply {
    type Output = Option<ReplyPlan>;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<Option<ReplyPlan>>, CallError> {
        match self.check.first()? {
            Next::Call(call) => Ok(Next::Call(call)),
            _ => Err(IoError::Unfinished.into()),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<ReplyPlan>>, CallError> {
        match self.check.next(result) {
            Ok(Next::Call(call)) => Ok(Next::Call(call)),
            Ok(Next::Done(call)) => Ok(Next::Done(self.plan(call))),
            Ok(_) => Err(IoError::Unfinished.into()),
            Err(_) => Ok(Next::Done(None)),
        }
    }
}

#[derive(Debug, PartialEq)]
enum NativePhase {
    Begin,
    Start,
    Record,
}

/// Starts native work through the run's call history: other arguments at a known ordinal are
/// refused before any work is reserved, a settled call answers its job id again, and an earlier
/// attempt is safe to repeat, as the work row names the job by run and ordinal.
#[derive(Debug, PartialEq)]
pub struct NativeCall {
    scope: RunScope,
    intent: CallIntent,
    request: Option<WorkRequest>,
    minted: (u64, Ulid),
    serving: ServingNode,
    phase: NativePhase,
}

impl NativeCall {
    pub fn new(
        scope: &RunScope,
        intent: CallIntent,
        request: WorkRequest,
        (minted, serving): ((u64, Ulid), ServingNode),
    ) -> Self {
        Self {
            scope: scope.clone(),
            intent,
            request: Some(request),
            minted,
            serving,
            phase: NativePhase::Begin,
        }
    }

    fn start(&mut self) -> Result<Next<String>, CallFailure> {
        let request = self.request.take().ok_or(IoError::Unfinished)?;
        let start = StartWork::new(&self.scope, request, self.minted, self.serving);
        self.phase = NativePhase::Start;
        Ok(Next::Call(IoCall::child(start)))
    }
}

/// A conflict means the run called differently than an earlier attempt.
fn marked(failure: CallFailure) -> CallFailure {
    let changed = failure.changed || failure.error.kind == ErrorKind::Conflict;
    CallFailure { changed, ..failure }
}

impl Operation for NativeCall {
    type Output = String;
    type Error = CallFailure;

    fn first(&mut self) -> Result<Next<String>, CallFailure> {
        let begin = begin(self.scope.run.run_id, self.intent.clone());
        Ok(Next::Call(IoCall::child(begin)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<String>, CallFailure> {
        match self.phase {
            NativePhase::Begin => {
                let begun = result.child::<Begun, StoreError>()??;
                match begun_reply(begun) {
                    Ok(Some(reply)) => Ok(Next::Done(reply)),
                    Ok(None) => self.start(),
                    Err(failure) if failure.error.kind == ErrorKind::UnknownOutcome => self.start(),
                    Err(failure) => Err(marked(failure)),
                }
            }
            NativePhase::Start => {
                // A refused start leaves no job, or one its work row finds again, so the call
                // may be made again; its intent goes.
                let started = result.child::<Ulid, CallError>()?;
                let outcome = match started {
                    Ok(work_id) => Ok(work_id.to_string()),
                    Err(CallError(error)) => Err(CallEnd::None(error)),
                };
                let run_id = self.scope.run.run_id;
                let record = RecordCall::new(run_id, self.intent.clone(), outcome);
                self.phase = NativePhase::Record;
                Ok(Next::Call(IoCall::child(record)))
            }
            NativePhase::Record => {
                let recorded = result.child::<String, CallFailure>()?;
                Ok(Next::Done(recorded.map_err(marked)?))
            }
        }
    }
}

#[cfg(test)]
#[path = "host_steps_tests.rs"]
mod tests;
