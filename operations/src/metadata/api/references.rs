//! Reads one local page of the documents that name an IRI as an object.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::metadata::IriIndexRecord;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

use super::read_types::{
    MetadataReferenceEntry, MetadataReferencesExecution, MetadataReferencesRequest,
};
use super::readable::RecordReadableOperation;
use super::title::DocumentTitleOperation;
use super::{MetadataApiError, REFERENCES_LIMIT, REFERENCES_MAX_LIMIT};
use crate::metadata::data_links::LinkBudget;
use crate::metadata::iri_index::{BacklinkError, BacklinkPage, BacklinksOperation};
use crate::metadata::live_records::{LiveRecordOperation, LiveRecordsError};

/// Postings one references page may read, so a page never scans the index.
const REFERENCE_BUDGET: LinkBudget = LinkBudget {
    seeks: 1100,
    rows: 4096,
};
/// Documents checked at the same time.
const DOCUMENT_LIMIT: usize = 8;

fn entry(record: &MetadataRegistryRecord, title: Option<String>) -> MetadataReferenceEntry {
    MetadataReferenceEntry {
        document_id: record.document_id.to_string(),
        group_id: record.group_id.to_string(),
        document_path: record.document_path.clone(),
        graph_iri: record.graph_iri.clone(),
        predicate: None,
        subject_iris: Vec::new(),
        title,
    }
}

#[derive(Debug, PartialEq)]
enum EntryStage {
    Record,
    Readable(MetadataRegistryRecord),
    Title(MetadataRegistryRecord),
}

/// The references one document makes: its current postings, when it is live and the caller
/// may read it; the postings of an older document event are dropped. Without postings it is
/// the predicate-less summary of the graph the IRI names.
#[derive(Debug, PartialEq)]
pub(crate) struct ReferenceEntriesOperation {
    realm_id: RealmId,
    auth: Option<AuthContext>,
    document_id: Option<Ulid>,
    /// The IRI the document's graph must have, for the graph summary.
    graph_iri: Option<String>,
    postings: Vec<IriIndexRecord>,
    stage: EntryStage,
}

impl ReferenceEntriesOperation {
    fn document(
        (realm_id, auth): (RealmId, Option<AuthContext>),
        document_id: Ulid,
        postings: Vec<IriIndexRecord>,
    ) -> Self {
        Self {
            realm_id,
            auth,
            document_id: Some(document_id),
            graph_iri: None,
            postings,
            stage: EntryStage::Record,
        }
    }

    /// The summary of the document whose graph is `iri`, by one point read of the document it
    /// names; never a walk over the registry.
    fn graph((realm_id, auth): (RealmId, Option<AuthContext>), iri: &str) -> Self {
        Self {
            realm_id,
            auth,
            document_id: MetadataRegistryRecord::graph_document(iri),
            graph_iri: Some(iri.to_string()),
            postings: Vec::new(),
            stage: EntryStage::Record,
        }
    }

    fn entries(
        &mut self,
        record: &MetadataRegistryRecord,
        title: Option<String>,
    ) -> Vec<MetadataReferenceEntry> {
        if self.graph_iri.is_some() {
            return vec![entry(record, title)];
        }
        std::mem::take(&mut self.postings)
            .into_iter()
            .map(|posting| MetadataReferenceEntry {
                predicate: Some(posting.predicate_iri),
                subject_iris: posting.subject_iris,
                ..entry(record, title.clone())
            })
            .collect()
    }
}

impl Operation for ReferenceEntriesOperation {
    type Output = Vec<MetadataReferenceEntry>;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<Vec<MetadataReferenceEntry>>, MetadataApiError> {
        let Some(document_id) = self.document_id else {
            return Ok(Next::Done(Vec::new()));
        };
        Ok(Next::Call(IoCall::child(LiveRecordOperation::new(
            document_id,
        ))))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<Vec<MetadataReferenceEntry>>, MetadataApiError> {
        match std::mem::replace(&mut self.stage, EntryStage::Record) {
            EntryStage::Record => {
                let record =
                    result.child::<Option<MetadataRegistryRecord>, LiveRecordsError>()??;
                let Some(record) = record else {
                    return Ok(Next::Done(Vec::new()));
                };
                let matches = match &self.graph_iri {
                    Some(iri) => record.graph_iri == *iri,
                    None => {
                        let cursor = record.last_event_id;
                        self.postings
                            .retain(|posting| posting.document_cursor == cursor);
                        !self.postings.is_empty()
                    }
                };
                if !matches {
                    return Ok(Next::Done(Vec::new()));
                }
                let readable =
                    RecordReadableOperation::new(self.realm_id, self.auth.clone(), record.clone());
                self.stage = EntryStage::Readable(record);
                Ok(Next::Call(IoCall::child(readable)))
            }
            EntryStage::Readable(record) => {
                if result.child::<(), MetadataApiError>()?.is_err() {
                    return Ok(Next::Done(Vec::new()));
                }
                let title = DocumentTitleOperation::new(&record);
                self.stage = EntryStage::Title(record);
                Ok(Next::Call(IoCall::child(title)))
            }
            EntryStage::Title(record) => {
                let title = result.child::<Option<String>, MetadataApiError>()??;
                Ok(Next::Done(self.entries(&record, title)))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum ReferencesStage {
    Backlinks,
    Documents(Option<Ulid>),
    Graph(Option<Ulid>),
}

/// Reads one local page of the documents naming the request's IRI as an object, by document id
/// after `after`; each must be live at the posting's cursor and readable. An empty first page
/// for a known graph IRI, or `resolve`, returns one predicate-less summary.
#[derive(Debug, PartialEq)]
pub struct ReferencesOperation {
    realm_id: RealmId,
    request: MetadataReferencesRequest,
    limit: usize,
    references: Vec<MetadataReferenceEntry>,
    stage: ReferencesStage,
}

impl ReferencesOperation {
    pub fn new(realm_id: RealmId, request: MetadataReferencesRequest) -> Self {
        let limit = request
            .limit
            .unwrap_or(REFERENCES_LIMIT)
            .clamp(1, REFERENCES_MAX_LIMIT);
        Self {
            realm_id,
            request,
            limit,
            references: Vec::new(),
            stage: ReferencesStage::Backlinks,
        }
    }

    fn caller(&self) -> (RealmId, Option<AuthContext>) {
        (self.realm_id, self.request.auth.clone())
    }

    fn graph(
        &mut self,
        next_after: Option<Ulid>,
    ) -> Result<Next<MetadataReferencesExecution>, MetadataApiError> {
        self.stage = ReferencesStage::Graph(next_after);
        let graph = ReferenceEntriesOperation::graph(self.caller(), &self.request.iri);
        Ok(Next::Call(IoCall::child(graph)))
    }

    fn documents(
        &mut self,
        page: BacklinkPage,
    ) -> Result<Next<MetadataReferencesExecution>, MetadataApiError> {
        let caller = self.caller();
        let members: Vec<_> = (page.documents.into_iter())
            .map(|(document_id, postings)| {
                let check =
                    ReferenceEntriesOperation::document(caller.clone(), document_id, postings);
                Effect::SubOperation(boxed_suboperation(check, |result| {
                    Event::Child(ChildOutcome::new(result))
                }))
            })
            .collect();
        if members.is_empty() {
            return self.listed(page.next);
        }
        self.stage = ReferencesStage::Documents(page.next);
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members,
            limit: DOCUMENT_LIMIT,
            form: GroupForm::All { deadline: None },
        })))
    }

    fn listed(
        &mut self,
        next_after: Option<Ulid>,
    ) -> Result<Next<MetadataReferencesExecution>, MetadataApiError> {
        if self.references.is_empty() && self.request.after.is_none() && next_after.is_none() {
            return self.graph(next_after);
        }
        Ok(Next::Done(self.done(next_after)))
    }

    fn done(&mut self, next_after: Option<Ulid>) -> MetadataReferencesExecution {
        MetadataReferencesExecution {
            references: std::mem::take(&mut self.references),
            next_after,
        }
    }
}

impl Operation for ReferencesOperation {
    type Output = MetadataReferencesExecution;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<MetadataReferencesExecution>, MetadataApiError> {
        let named = |iri: &str| oxrdf::NamedNode::new(iri).is_ok();
        if self.request.iri.trim().is_empty() || !named(&self.request.iri) {
            return Err(MetadataApiError::BadRequest);
        }
        if self
            .request
            .predicate
            .as_deref()
            .is_some_and(|iri| !named(iri))
        {
            return Err(MetadataApiError::BadRequest);
        }
        if self.request.resolve {
            return self.graph(None);
        }
        let request = &self.request;
        let read = BacklinksOperation::new(
            (&request.iri, request.predicate.as_deref()),
            (request.after, self.limit),
            REFERENCE_BUDGET,
        );
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<MetadataReferencesExecution>, MetadataApiError> {
        match std::mem::replace(&mut self.stage, ReferencesStage::Backlinks) {
            ReferencesStage::Backlinks => {
                let read = result.child::<(BacklinkPage, LinkBudget), BacklinkError>()?;
                let (page, _) = read.map_err(|_| MetadataApiError::ServiceUnavailable)?;
                self.documents(page)
            }
            ReferencesStage::Documents(next_after) => {
                let IoResult::Group(GroupEvent::Finished { results }) = result else {
                    return Err(MetadataApiError::ServiceUnavailable);
                };
                for member in results {
                    let GroupResult::Done(event) = member else {
                        return Err(MetadataApiError::ServiceUnavailable);
                    };
                    let Event::Child(outcome) = *event else {
                        return Err(MetadataApiError::ServiceUnavailable);
                    };
                    let entries = outcome.take::<Vec<MetadataReferenceEntry>, MetadataApiError>();
                    self.references
                        .extend(entries.ok_or(MetadataApiError::ServiceUnavailable)??);
                }
                self.listed(next_after)
            }
            ReferencesStage::Graph(next_after) => {
                let entries = result.child::<Vec<MetadataReferenceEntry>, MetadataApiError>()??;
                self.references.extend(entries);
                Ok(Next::Done(self.done(next_after)))
            }
        }
    }
}

#[cfg(test)]
#[path = "references_tests.rs"]
mod call_tests;
