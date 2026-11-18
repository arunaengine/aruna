//! Finds the readable documents this node indexes under one external identifier.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::keyspaces::SECONDARY_ID_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::rows::PrefixPages;
use aruna_core::structs::PersistentIdMapping;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::secondary_id::{SecondaryIdKind, index_document, secondary_id_prefix};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

use crate::metadata::api::MetadataApiError;
use crate::metadata::api::readable::RecordReadableOperation;
use crate::metadata::id_mapping::PersistentIdError;
use crate::metadata::persistent_id::MappingReadOperation;
use crate::metadata::record_reads::DocumentRecordOperation;
use crate::metadata::repository::StorageReadError;
use crate::metadata::secondary_ids::IdentifierMatch;

/// Index rows one page of the reverse index holds.
const INDEX_PAGE: usize = 256;
/// Documents checked at the same time.
const MATCH_LIMIT: usize = 8;

/// One external identifier, optionally bound to the endpoint that issued it.
#[derive(Clone, Debug, PartialEq)]
pub struct IdentifierQuery {
    pub kind: SecondaryIdKind,
    pub value: String,
    pub endpoint: Option<String>,
}

impl IdentifierQuery {
    /// The earliest origin under which `mapping` holds this identifier.
    fn origin(
        &self,
        mapping: &PersistentIdMapping,
    ) -> Option<aruna_core::structs::secondary_id::IdentifierOrigin> {
        (mapping.secondary_identifiers.iter())
            .filter(|held| {
                held.kind == self.kind
                    && held.value == self.value
                    && (self.endpoint.as_deref())
                        .is_none_or(|endpoint| held.endpoint.as_deref() == Some(endpoint))
            })
            .map(|held| held.origin)
            .min()
    }
}

#[derive(Debug, PartialEq)]
enum MatchStage {
    Record,
    Readable,
    Mapping,
}

/// The match of one indexed document: none when it is gone, unreadable to the caller, or its
/// mapping is retired or no longer holds the identifier.
#[derive(Debug, PartialEq)]
pub(crate) struct DocumentMatchOperation {
    realm_id: RealmId,
    auth: Option<AuthContext>,
    query: IdentifierQuery,
    document_id: Ulid,
    stage: MatchStage,
}

impl Operation for DocumentMatchOperation {
    type Output = Option<IdentifierMatch>;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<Option<IdentifierMatch>>, MetadataApiError> {
        Ok(Next::Call(IoCall::child(DocumentRecordOperation::new(
            self.document_id,
        ))))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<Option<IdentifierMatch>>, MetadataApiError> {
        match self.stage {
            MatchStage::Record => {
                let record = result.child::<Option<MetadataRegistryRecord>, StorageReadError>()?;
                let Some(record) = record.map_err(|_| MetadataApiError::ServiceUnavailable)? else {
                    return Ok(Next::Done(None));
                };
                self.stage = MatchStage::Readable;
                let readable =
                    RecordReadableOperation::new(self.realm_id, self.auth.clone(), record);
                Ok(Next::Call(IoCall::child(readable)))
            }
            // Unreadable and deleted documents are skipped, so a miss never reveals them.
            MatchStage::Readable => match result.child::<(), MetadataApiError>()? {
                Ok(()) => {
                    self.stage = MatchStage::Mapping;
                    let mapping = MappingReadOperation::new(self.document_id);
                    Ok(Next::Call(IoCall::child(mapping)))
                }
                Err(_) => Ok(Next::Done(None)),
            },
            MatchStage::Mapping => {
                let mapping = result.child::<Option<PersistentIdMapping>, PersistentIdError>()?;
                let mapping =
                    mapping.map_err(|error| MetadataApiError::Internal(error.to_string()))?;
                let Some(mapping) = mapping.filter(|mapping| !mapping.is_retired()) else {
                    return Ok(Next::Done(None));
                };
                let Some(origin) = self.query.origin(&mapping) else {
                    return Ok(Next::Done(None));
                };
                Ok(Next::Done(Some(IdentifierMatch {
                    document_id: self.document_id,
                    pid: mapping.is_active().then_some(mapping.pid),
                    origin,
                })))
            }
        }
    }
}

/// Every readable document in this node's reverse index that holds the identifier, in
/// document order. Unreadable and deleted documents are skipped, so a miss never reveals that
/// they exist.
#[derive(Debug, PartialEq)]
pub struct IdentifierLookupOperation {
    realm_id: RealmId,
    auth: Option<AuthContext>,
    query: IdentifierQuery,
    pages: PrefixPages,
    documents: BTreeSet<Ulid>,
    matching: bool,
}

impl IdentifierLookupOperation {
    pub fn new(realm_id: RealmId, auth: Option<AuthContext>, query: IdentifierQuery) -> Self {
        let prefix = secondary_id_prefix(query.kind, &query.value, query.endpoint.as_deref());
        Self {
            realm_id,
            auth,
            pages: PrefixPages::new(SECONDARY_ID_KEYSPACE, prefix, INDEX_PAGE),
            query,
            documents: BTreeSet::new(),
            matching: false,
        }
    }

    fn matches(&mut self) -> Next<Vec<IdentifierMatch>> {
        if self.documents.is_empty() {
            return Next::Done(Vec::new());
        }
        let members = (self.documents.iter())
            .map(|document_id| {
                let check = DocumentMatchOperation {
                    realm_id: self.realm_id,
                    auth: self.auth.clone(),
                    query: self.query.clone(),
                    document_id: *document_id,
                    stage: MatchStage::Record,
                };
                Effect::SubOperation(boxed_suboperation(check, |result| {
                    Event::Child(ChildOutcome::new(result))
                }))
            })
            .collect();
        self.matching = true;
        Next::Call(IoCall::Group(EffectGroup {
            members,
            limit: MATCH_LIMIT,
            form: GroupForm::All { deadline: None },
        }))
    }
}

impl Operation for IdentifierLookupOperation {
    type Output = Vec<IdentifierMatch>;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<Vec<IdentifierMatch>>, MetadataApiError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<IdentifierMatch>>, MetadataApiError> {
        if !self.matching {
            let rows = (self.pages.take(result))
                .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
            self.documents
                .extend(rows.iter().filter_map(|(key, _)| index_document(key)));
            if !self.pages.done() {
                return Ok(Next::Call(self.pages.call()));
            }
            return Ok(self.matches());
        }
        let IoResult::Group(GroupEvent::Finished { results }) = result else {
            return Err(MetadataApiError::ServiceUnavailable);
        };
        let mut matches = Vec::new();
        for member in results {
            let GroupResult::Done(event) = member else {
                return Err(MetadataApiError::ServiceUnavailable);
            };
            let Event::Child(outcome) = *event else {
                return Err(MetadataApiError::ServiceUnavailable);
            };
            let found = outcome.take::<Option<IdentifierMatch>, MetadataApiError>();
            matches.extend(found.ok_or(MetadataApiError::ServiceUnavailable)??);
        }
        Ok(Next::Done(matches))
    }
}

#[cfg(test)]
#[path = "identifier_lookup_tests.rs"]
mod call_tests;
