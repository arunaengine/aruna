//! Checks at consent whether an export into another realm could read its selected files.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::ExportRoCrateSpec;
use aruna_core::structs::storage::encryption::BucketEncryption;
use serde_json::Value as JsonValue;

use super::archive::recognize_entities;
use super::resolve::CandidateSource;
use super::sources::{AliasState, ResolveEntriesOperation, Resolved};
use super::{ExportEntity, ExportFailure, SourceScope};
use crate::replication::plaintext::{HolderCheckOperation, PlaintextError};

/// Why the consent check could not finish.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ConsentCheckError(pub String);

impl From<IoError> for ConsentCheckError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

impl From<ExportFailure> for ConsentCheckError {
    fn from(error: ExportFailure) -> Self {
        Self(format!("{error:?}"))
    }
}

#[derive(Debug, PartialEq)]
enum ConsentStep {
    Resolve,
    KeyRow,
    Holder,
}

/// One local copy of the entity being checked.
#[derive(Debug, PartialEq)]
struct LocalCopy {
    bucket: String,
    keyed: bool,
}

/// Finds the bucket that refuses a selected file of a crate at consent: the job's resolution
/// finds copies of it on this node, and the holder rule refuses each of them.
#[derive(Debug, PartialEq)]
pub struct UnheldBucketOperation {
    scope: SourceScope,
    jsonld: String,
    entities: VecDeque<ExportEntity>,
    copies: VecDeque<LocalCopy>,
    unheld: Option<String>,
    step: ConsentStep,
}

type ConsentTurn = Result<Next<Option<String>>, ConsentCheckError>;

impl UnheldBucketOperation {
    pub fn new(spec: ExportRoCrateSpec, node_id: NodeId, jsonld: String) -> Self {
        Self {
            scope: SourceScope {
                spec: Arc::new(spec),
                owner_node: node_id,
            },
            jsonld,
            entities: VecDeque::new(),
            copies: VecDeque::new(),
            unheld: None,
            step: ConsentStep::Resolve,
        }
    }

    fn selected(&self) -> Result<Vec<ExportEntity>, ConsentCheckError> {
        let text = |error: &dyn std::fmt::Display| ConsentCheckError(error.to_string());
        let canonical = craqle::canonicalize_jsonld(&self.jsonld).map_err(|it| text(&it))?;
        let document: JsonValue = serde_json::from_str(&self.jsonld).map_err(|it| text(&it))?;
        let spec = &self.scope.spec;
        let realm_id = spec.auth_context.realm_id;
        let mut entities = recognize_entities(&document, &canonical.nquads, realm_id)?;
        let files = (spec.selection.as_ref()).map(|selection| &selection.files[..]);
        entities.retain(|entity| files.unwrap_or_default().contains(&entity.entity_id));
        Ok(entities)
    }

    /// Starts the next entity, and ends when one has only copies the caller cannot read.
    fn next_entity(&mut self) -> ConsentTurn {
        if self.unheld.is_some() {
            return Ok(Next::Done(self.unheld.take()));
        }
        let Some(entity) = self.entities.pop_front() else {
            return Ok(Next::Done(None));
        };
        self.copies = (entity.candidates.iter())
            .filter_map(|candidate| match &candidate.source {
                CandidateSource::Local {
                    location, bucket, ..
                } => Some(LocalCopy {
                    bucket: bucket.clone(),
                    keyed: location.format.bucket_key().is_some(),
                }),
                _ => None,
            })
            .collect();
        self.next_copy()
    }

    fn next_copy(&mut self) -> ConsentTurn {
        let Some(copy) = self.copies.front() else {
            return self.next_entity();
        };
        if self.scope.spec.selection.is_none() {
            return self.allowed();
        }
        if copy.keyed {
            return self.check_holder();
        }
        let read = StorageEffect::Read {
            key_space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            key: copy.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        };
        self.step = ConsentStep::KeyRow;
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(read)))))
    }

    /// A copy the caller can read settles its entity.
    fn allowed(&mut self) -> ConsentTurn {
        self.copies.clear();
        self.unheld = None;
        self.next_entity()
    }

    fn refused(&mut self) -> ConsentTurn {
        if let Some(copy) = self.copies.pop_front() {
            self.unheld.get_or_insert(copy.bucket);
        }
        self.next_copy()
    }

    fn check_holder(&mut self) -> ConsentTurn {
        let Some(copy) = self.copies.front() else {
            return self.next_entity();
        };
        let auth = &self.scope.spec.auth_context;
        let holder = HolderCheckOperation::new(auth.realm_id, &copy.bucket, auth.user_id);
        self.step = ConsentStep::Holder;
        Ok(Next::Call(IoCall::child(holder)))
    }
}

impl Operation for UnheldBucketOperation {
    type Output = Option<String>;
    type Error = ConsentCheckError;

    fn first(&mut self) -> ConsentTurn {
        let entities = self.selected()?;
        let resolve =
            ResolveEntriesOperation::new(self.scope.clone(), entities, AliasState::default());
        Ok(Next::Call(IoCall::child(resolve)))
    }

    fn next(&mut self, result: IoResult) -> ConsentTurn {
        match self.step {
            ConsentStep::Resolve => {
                let resolved = result.child::<Resolved, ExportFailure>()??;
                self.entities = resolved.entities.into();
                self.next_entity()
            }
            // The active bucket key decides for a plain copy of an encrypting bucket.
            ConsentStep::KeyRow => {
                let row = match result.event()? {
                    Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
                    Event::Storage(StorageEvent::Error { error }) => {
                        return Err(ExportFailure::Retryable(error.to_string()).into());
                    }
                    event => {
                        return Err(ExportFailure::Retryable(format!(
                            "unexpected storage read event: {}",
                            event.label()
                        ))
                        .into());
                    }
                };
                let settings = BucketEncryption::from_row(row.as_deref())
                    .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
                match settings.active_key() {
                    Some(_) => self.check_holder(),
                    None => self.allowed(),
                }
            }
            ConsentStep::Holder => {
                let holder = (result.child::<bool, PlaintextError>()?)
                    .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
                match holder {
                    true => self.allowed(),
                    false => self.refused(),
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "consent_tests.rs"]
mod tests;
