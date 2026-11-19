//! Answers one node's part of a reference preflight: the readable documents that name each
//! target on this node, its hidden references, local locations and index freshness.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{DEAD_LETTER_KEYSPACE, MATERIALIZATION_JOB_KEYSPACE};
use aruna_core::metadata::IriIndexRecord;
use aruna_core::operation::Operation;
use aruna_core::rows::all_values;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::HashIndex;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{GroupId, Key};
use ulid::Ulid;

use super::list::PendingRecordsOperation;
use super::preflight::{
    MAX_TARGET_VERSIONS, PREFLIGHT_BUDGET, add_location_iris, index_state, visible_prefix,
};
use super::preflight_types::{
    MetadataNodeFreshness, MetadataPreflightLocation, MetadataVisibleReference,
    ReferenceNodeExecution, ReferenceNodeRequest, ReferenceNodeTarget,
};
use super::read_access::{child_results, children};
use super::readable::RecordReadableOperation;
use super::title::DocumentTitleOperation;
use super::{MAX_PAGINATION_DEPTH, MetadataApiError, REGISTRY_CANDIDATE_LIMIT};
use crate::blob::permission_paths::{ResolvePathsError, ResolvePathsOperation};
use crate::metadata::call_results::page_rows;
use crate::metadata::data_links::LinkBudget;
use crate::metadata::iri_index::{
    BacklinkError, BacklinkPage, BacklinksOperation, IriFreshness, iri_freshness, status_rows,
};
use crate::metadata::live_records::{LiveRecordOperation, LiveRecordsError};

type Backlinks = (BacklinkPage, LinkBudget);
type PendingRecords = HashMap<GroupId, Vec<MetadataRegistryRecord>>;

/// The page of one target IRI the preflight reads and what it found on it so far.
#[derive(Debug, PartialEq)]
struct IriWalk {
    iri: String,
    content_w3ids: BTreeSet<String>,
    after: Option<Ulid>,
    found: usize,
}

#[derive(Debug, PartialEq)]
enum LocalStage {
    Start,
    Aliases,
    Backlinks(IriWalk),
    Records(IriWalk, BacklinkPage),
    Readable(IriWalk, BacklinkPage, Vec<Ulid>),
    Freshness(Vec<MetadataRegistryRecord>),
    Pending(IriFreshness),
    Queued(IriFreshness, bool),
    Dead(IriFreshness, bool),
    Titles(Box<Titling>),
}

/// The page this node answers once its documents' titles are read.
#[derive(Debug, PartialEq)]
struct Titling {
    selected: Vec<(String, Ulid)>,
    saturated: bool,
    freshness: (IriFreshness, bool, bool),
    documents: Vec<Ulid>,
}

/// One node's preflight. Each object yields its first `limit + 1` visible documents or ends,
/// which covers every reference of the page prefix; a lookup over the budget fails instead of
/// scanning.
#[derive(Debug, PartialEq)]
pub struct LocalPreflightOperation {
    realm_id: RealmId,
    local_node: NodeId,
    auth: Option<AuthContext>,
    request: ReferenceNodeRequest,
    s3_endpoint: Option<String>,
    iris: VecDeque<(String, BTreeSet<String>)>,
    target_locations: BTreeMap<String, (bool, bool)>,
    aliases_seen: bool,
    budget: LinkBudget,
    records: HashMap<Ulid, Option<MetadataRegistryRecord>>,
    readable: HashMap<Ulid, bool>,
    hidden: BTreeSet<String>,
    visible: BTreeSet<(String, Ulid)>,
    stage: LocalStage,
}

/// The preflight of `request` on this node for a caller with `auth`; `s3_endpoint` is this
/// node's S3 endpoint, whose path-style IRIs also name its objects.
pub fn references_preflight_local(
    (realm_id, local_node): (RealmId, NodeId),
    auth: Option<AuthContext>,
    request: ReferenceNodeRequest,
    s3_endpoint: Option<String>,
) -> LocalPreflightOperation {
    LocalPreflightOperation {
        realm_id,
        local_node,
        auth,
        request,
        s3_endpoint,
        iris: VecDeque::new(),
        target_locations: BTreeMap::new(),
        aliases_seen: false,
        budget: PREFLIGHT_BUDGET,
        records: HashMap::new(),
        readable: HashMap::new(),
        hidden: BTreeSet::new(),
        visible: BTreeSet::new(),
        stage: LocalStage::Start,
    }
}

type LocalTurn = Result<Next<ReferenceNodeExecution>, MetadataApiError>;

impl LocalPreflightOperation {
    /// Adds this node's copies of each target and collects the IRIs that name the targets.
    fn aliased(&mut self, aliases: Vec<Vec<HashIndex>>) -> LocalTurn {
        let mut iri_targets = BTreeMap::<String, BTreeSet<String>>::new();
        let mut aliases = aliases.into_iter();
        for target in self.request.targets.iter_mut() {
            // A location target has no content identity to find other copies by.
            let mut local = match target.location() {
                Some(_) => Vec::new(),
                None => aliases.next().unwrap_or_default(),
            };
            local.retain(|alias| {
                alias.realm_id == self.realm_id && alias.node_id == self.local_node
            });
            self.aliases_seen |= !local.is_empty();
            let (mut found, mut remaining) = (false, false);
            for alias in local {
                found = true;
                let location = MetadataPreflightLocation {
                    node_id: alias.node_id,
                    bucket: alias.bucket.clone(),
                    key: alias.key.clone(),
                    version_id: alias.version_id,
                };
                let removed = target.remove_resolvable_locations
                    || target.removed_locations.contains(&location);
                remaining |= !removed;
                let endpoint = self.s3_endpoint.as_deref();
                add_location_iris(
                    &mut target.queried_iris,
                    endpoint,
                    &alias.bucket,
                    &alias.key,
                );
            }
            target.queried_iris.sort();
            target.queried_iris.dedup();
            for iri in &target.queried_iris {
                let entry = iri_targets.entry(iri.clone()).or_default();
                entry.insert(target.content_w3id.clone());
            }
            let content_w3id = target.content_w3id.clone();
            self.target_locations
                .insert(content_w3id, (found, remaining));
        }
        self.iris = iri_targets.into_iter().collect();
        self.next_iri()
    }

    fn next_iri(&mut self) -> LocalTurn {
        let Some((iri, content_w3ids)) = self.iris.pop_front() else {
            let returned: Vec<_> = self.records.values().flatten().cloned().collect();
            let rows = status_rows(&returned);
            self.stage = LocalStage::Freshness(returned);
            return Ok(Next::Call(IoCall::Read(rows)));
        };
        let walk = IriWalk {
            iri,
            content_w3ids,
            after: None,
            found: 0,
        };
        self.backlinks(walk)
    }

    fn backlinks(&mut self, walk: IriWalk) -> LocalTurn {
        let limit = self.request.limit.saturating_add(1);
        let read = BacklinksOperation::new((&walk.iri, None), (walk.after, limit), self.budget);
        self.stage = LocalStage::Backlinks(walk);
        Ok(Next::Call(IoCall::child(read)))
    }

    /// Loads the records of the page's documents this preflight has not seen yet.
    fn page_records(&mut self, walk: IriWalk, page: BacklinkPage) -> LocalTurn {
        let unknown: Vec<_> = (page.documents.keys())
            .filter(|document_id| !self.records.contains_key(document_id))
            .map(|document_id| LiveRecordOperation::new(*document_id))
            .collect();
        if unknown.is_empty() {
            return self.page_readable(walk, page);
        }
        self.stage = LocalStage::Records(walk, page);
        Ok(Next::Call(children(unknown)))
    }

    /// The page's documents whose record carries one of their postings.
    fn live_documents(&self, page: &BacklinkPage) -> Vec<Ulid> {
        let current = |document_id: &Ulid, postings: &[IriIndexRecord]| {
            let Some(Some(record)) = self.records.get(document_id) else {
                return false;
            };
            postings
                .iter()
                .any(|posting| posting.document_cursor == record.last_event_id)
        };
        (page.documents.iter())
            .filter(|(document_id, postings)| current(document_id, postings))
            .map(|(document_id, _)| *document_id)
            .collect()
    }

    /// Checks the readability of the page's live documents not checked yet.
    fn page_readable(&mut self, walk: IriWalk, page: BacklinkPage) -> LocalTurn {
        let unchecked: Vec<_> = (self.live_documents(&page).into_iter())
            .filter(|document_id| !self.readable.contains_key(document_id))
            .collect();
        if unchecked.is_empty() {
            return self.classified(walk, page);
        }
        let checks = (unchecked.iter())
            .filter_map(|document_id| self.records.get(document_id).cloned().flatten())
            .map(|record| RecordReadableOperation::new(self.realm_id, self.auth.clone(), record))
            .collect();
        self.stage = LocalStage::Readable(walk, page, unchecked);
        Ok(Next::Call(children(checks)))
    }

    fn classified(&mut self, mut walk: IriWalk, page: BacklinkPage) -> LocalTurn {
        for document_id in self.live_documents(&page) {
            if !self.readable.get(&document_id).copied().unwrap_or(false) {
                self.hidden.extend(walk.content_w3ids.iter().cloned());
                continue;
            }
            walk.found = walk.found.saturating_add(1);
            for content_w3id in &walk.content_w3ids {
                self.visible.insert((content_w3id.clone(), document_id));
            }
        }
        match page.next {
            Some(next) if walk.found <= self.request.limit => {
                walk.after = Some(next);
                self.backlinks(walk)
            }
            _ => self.next_iri(),
        }
    }

    fn titled(
        &mut self,
        titling: Titling,
        titles: HashMap<Ulid, String>,
    ) -> ReferenceNodeExecution {
        let Titling {
            selected,
            saturated,
            freshness: (freshness, pending, failed),
            ..
        } = titling;
        let mut visible_references = Vec::with_capacity(selected.len());
        for (content_w3id, document_id) in selected {
            let Some(Some(record)) = self.records.get(&document_id) else {
                continue;
            };
            let title =
                (titles.get(&document_id).cloned()).unwrap_or_else(|| record.document_path.clone());
            visible_references.push(MetadataVisibleReference {
                content_w3id,
                document_id: document_id.to_string(),
                title,
            });
        }
        let targets = std::mem::take(&mut self.request.targets)
            .into_iter()
            .map(|target| {
                let (resolvable_location_found, location_after_operation) =
                    (self.target_locations.remove(&target.content_w3id)).unwrap_or((false, false));
                ReferenceNodeTarget {
                    hidden_references_exist: self.hidden.contains(&target.content_w3id),
                    content_w3id: target.content_w3id,
                    resolvable_location_found,
                    location_after_operation,
                }
            })
            .collect();
        ReferenceNodeExecution {
            visible_references,
            targets,
            freshness: MetadataNodeFreshness {
                node_id: self.local_node,
                index_state: index_state(freshness.state, pending, failed),
                oldest_status_updated: freshness.oldest_status_updated,
            },
            path_style_available: self.s3_endpoint.is_some() || !self.aliases_seen,
            saturated,
        }
    }
}

fn unavailable<E>(_: E) -> MetadataApiError {
    MetadataApiError::ServiceUnavailable
}

/// A page call of at most one row of `space`, to tell whether it holds any.
fn any_row(space: &str) -> IoCall {
    IoCall::Page {
        space: space.to_string(),
        prefix: Key::from(Vec::new()),
        after: None,
        limit: 1,
    }
}

impl Operation for LocalPreflightOperation {
    type Output = ReferenceNodeExecution;
    type Error = MetadataApiError;

    fn first(&mut self) -> LocalTurn {
        let request = &self.request;
        if request.targets.len() > MAX_TARGET_VERSIONS
            || request.limit == 0
            || request.limit > MAX_PAGINATION_DEPTH
        {
            return Err(MetadataApiError::BadRequest);
        }
        let auth = self.auth.as_ref().ok_or(MetadataApiError::Unauthorized)?;
        if auth.realm_id != self.realm_id {
            return Err(MetadataApiError::Forbidden);
        }
        let lookups: Vec<_> = (request.targets.iter())
            .filter(|target| target.location().is_none())
            .map(|target| ResolvePathsOperation::new(target.content_hash))
            .collect();
        if lookups.is_empty() {
            return self.aliased(Vec::new());
        }
        self.stage = LocalStage::Aliases;
        Ok(Next::Call(children(lookups)))
    }

    fn next(&mut self, result: IoResult) -> LocalTurn {
        match std::mem::replace(&mut self.stage, LocalStage::Start) {
            LocalStage::Start => Err(MetadataApiError::Internal(
                "preflight not started".to_string(),
            )),
            LocalStage::Aliases => {
                let found = child_results::<Vec<HashIndex>, ResolvePathsError>(result)?;
                let aliases = (found.into_iter())
                    .map(|found| {
                        found
                            .and_then(Result::ok)
                            .ok_or(MetadataApiError::ServiceUnavailable)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                self.aliased(aliases)
            }
            LocalStage::Backlinks(walk) => {
                let (page, left) =
                    (result.child::<Backlinks, BacklinkError>()?).map_err(unavailable)?;
                self.budget = left;
                self.page_records(walk, page)
            }
            LocalStage::Records(walk, page) => {
                let unknown: Vec<_> = (page.documents.keys())
                    .filter(|document_id| !self.records.contains_key(document_id))
                    .copied()
                    .collect();
                let loaded =
                    child_results::<Option<MetadataRegistryRecord>, LiveRecordsError>(result)?;
                for (document_id, record) in unknown.into_iter().zip(loaded) {
                    let record = record.ok_or(MetadataApiError::ServiceUnavailable)??;
                    self.records.insert(document_id, record);
                }
                self.page_readable(walk, page)
            }
            LocalStage::Readable(walk, page, unchecked) => {
                let checked = child_results::<(), MetadataApiError>(result)?;
                for (document_id, allowed) in unchecked.into_iter().zip(checked) {
                    let allowed = matches!(allowed, Some(Ok(())));
                    self.readable.insert(document_id, allowed);
                }
                self.classified(walk, page)
            }
            LocalStage::Freshness(returned) => {
                let values = all_values(result)?;
                let freshness = iri_freshness(&returned, values).map_err(unavailable)?;
                self.stage = LocalStage::Pending(freshness);
                let pending = PendingRecordsOperation::new(None, REGISTRY_CANDIDATE_LIMIT);
                Ok(Next::Call(IoCall::child(pending)))
            }
            LocalStage::Pending(freshness) => {
                let pending = result.child::<PendingRecords, MetadataApiError>()??;
                let realm = self.realm_id;
                let pending =
                    (pending.into_values().flatten()).any(|record| record.realm_id == realm);
                self.stage = LocalStage::Queued(freshness, pending);
                Ok(Next::Call(any_row(MATERIALIZATION_JOB_KEYSPACE)))
            }
            LocalStage::Queued(freshness, pending) => {
                let (queued, _) = page_rows(result).map_err(unavailable)?;
                self.stage = LocalStage::Dead(freshness, pending || !queued.is_empty());
                Ok(Next::Call(any_row(DEAD_LETTER_KEYSPACE)))
            }
            LocalStage::Dead(freshness, pending) => {
                let (dead, _) = page_rows(result).map_err(unavailable)?;
                // A queued or failed materialization has no postings yet, so its references
                // are missing.
                let freshness = (freshness, pending, !dead.is_empty());
                let visible = std::mem::take(&mut self.visible);
                let (selected, saturated) = visible_prefix(visible, self.request.limit);
                let documents: BTreeSet<_> = selected.iter().map(|(_, id)| *id).collect();
                let documents: Vec<_> = (documents.into_iter())
                    .filter(|document_id| matches!(self.records.get(document_id), Some(Some(_))))
                    .collect();
                let lookups: Vec<_> = (documents.iter())
                    .filter_map(|document_id| self.records.get(document_id).cloned().flatten())
                    .map(|record| DocumentTitleOperation::new(&record))
                    .collect();
                let titling = Titling {
                    selected,
                    saturated,
                    freshness,
                    documents,
                };
                if lookups.is_empty() {
                    return Ok(Next::Done(self.titled(titling, HashMap::new())));
                }
                self.stage = LocalStage::Titles(Box::new(titling));
                Ok(Next::Call(children(lookups)))
            }
            LocalStage::Titles(titling) => {
                let found = child_results::<Option<String>, MetadataApiError>(result)?;
                let titles = (titling.documents.iter().copied().zip(found))
                    .filter_map(|(document_id, title)| Some((document_id, title?.ok()??)))
                    .collect();
                Ok(Next::Done(self.titled(*titling, titles)))
            }
        }
    }
}

#[cfg(test)]
#[path = "preflight_local_tests.rs"]
mod call_tests;
