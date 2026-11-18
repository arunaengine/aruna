//! Tells a replayed forwarded document write from a new one this node has not accepted yet.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::metadata::create_document::CreateDocumentConfig;
use crate::metadata::create_document::accepted_create_matches;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::protocol::MetadataTransportMessage;
use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{
    CREATE_ACCEPTANCE_KEYSPACE, DOCUMENT_INDEX_KEYSPACE, PENDING_PROJECTION_KEYSPACE,
};
use aruna_core::metadata::MetadataEventRecord;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::placement::holds_placement;
use aruna_core::rows::{decode, page, read_row, value, values};
use aruna_core::storage_entries::{create_acceptance_key, metadata_document_key};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::Key;
use ulid::Ulid;

pub(crate) fn routed_record_matches(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    document_id: Ulid,
    placement: &PlacementRef,
    record: &MetadataRegistryRecord,
) -> bool {
    record.realm_id == realm_id
        && record.document_id == document_id
        && record.placement == *placement
        && record.graph_iri == MetadataRegistryRecord::graph_iri_for(document_id)
        && record.permission_path
            == MetadataRegistryRecord::permission_path_for(
                &realm_id,
                record.group_id,
                &record.document_path,
                document_id,
            )
        && resolve_metadata_id(config, realm_id, Some(record.group_id), record.document_id)
            .is_ok_and(|resolved| resolved == *placement)
}

pub(crate) fn create_record_matches(
    config: &CreateDocumentConfig,
    document_id: Ulid,
    placement: &PlacementRef,
    record: &MetadataRegistryRecord,
) -> bool {
    let normalized_path = MetadataRegistryRecord::normalize_document_path(&config.document_path);
    record.realm_id == config.actor.realm_id
        && record.group_id == config.group_id
        && record.document_id == document_id
        && record.document_path == normalized_path
        && record.graph_iri == MetadataRegistryRecord::graph_iri_for(document_id)
        && record.permission_path
            == MetadataRegistryRecord::permission_path_for(
                &config.actor.realm_id,
                config.group_id,
                &normalized_path,
                document_id,
            )
        && record.placement == *placement
        && record.public == config.public
}

fn pending_scan(document_id: Ulid) -> IoCall {
    IoCall::Page {
        space: PENDING_PROJECTION_KEYSPACE.to_string(),
        prefix: Key::from(document_id.to_bytes().to_vec()),
        after: None,
        limit: 1,
    }
}

/// The registry record of a document and its create acceptance, from one snapshot.
pub(crate) type CreateRows = (MetadataRegistryRecord, Option<MetadataEventRecord>);

/// Reads what tells a replayed forwarded create from a new one.
#[derive(Debug, PartialEq)]
pub(crate) struct CreateRowsOperation {
    document_id: Ulid,
}

impl CreateRowsOperation {
    pub(crate) fn new(document_id: Ulid) -> Self {
        Self { document_id }
    }
}

impl Operation for CreateRowsOperation {
    type Output = Option<CreateRows>;
    type Error = IoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }

    fn first(&mut self) -> Result<Next<Option<CreateRows>>, IoError> {
        Ok(Next::Call(IoCall::Read(vec![
            (
                DOCUMENT_INDEX_KEYSPACE.to_string(),
                metadata_document_key(self.document_id),
            ),
            (
                CREATE_ACCEPTANCE_KEYSPACE.to_string(),
                create_acceptance_key(self.document_id),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<CreateRows>>, IoError> {
        let [record, accepted] = values::<2>(result)?;
        let Some(record) = record else {
            return Ok(Next::Done(None));
        };
        let accepted = accepted.map(|bytes| decode(&bytes)).transpose()?;
        Ok(Next::Done(Some((decode(&record)?, accepted))))
    }
}

/// The answer to a forwarded create whose document already exists: the stored record when it is
/// this same create, and an error when another document holds the id.
pub(crate) fn replay_answer(
    config: &CreateDocumentConfig,
    (record, accepted): CreateRows,
) -> Result<MetadataTransportMessage, String> {
    let accepted = accepted
        .ok_or_else(|| "existing metadata document has no create acceptance".to_string())?;
    if !accepted_create_matches(config, &accepted)
        || record.realm_id != config.actor.realm_id
        || record.group_id != config.group_id
        || record.document_path
            != MetadataRegistryRecord::normalize_document_path(&config.document_path)
    {
        return Err("forwarded metadata create collides with an existing document".to_string());
    }
    Ok(MetadataTransportMessage::ForwardedRecord {
        record: Box::new(record),
    })
}

/// Whether this node has evidence that the document was created here and is now
/// gone, rather than a create whose projection has not landed: the create
/// acceptance survives the delete, while a queued projection means "not yet".
#[derive(Debug, PartialEq)]
pub(crate) struct DeletedHereOperation {
    document_id: Ulid,
    scanned: bool,
}

impl DeletedHereOperation {
    pub(crate) fn new(document_id: Ulid) -> Self {
        Self {
            document_id,
            scanned: false,
        }
    }
}

impl Operation for DeletedHereOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        Ok(Next::Call(pending_scan(self.document_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        if self.scanned {
            let accepted = value(result)?;
            let accepted = (accepted.as_deref().map(decode::<MetadataEventRecord>)).transpose()?;
            return Ok(Next::Done(accepted.is_some()));
        }
        if !page(result)?.0.is_empty() {
            return Ok(Next::Done(false));
        }
        self.scanned = true;
        Ok(Next::Call(read_row(
            CREATE_ACCEPTANCE_KEYSPACE,
            create_acceptance_key(self.document_id),
        )))
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum HeldRecordError {
    NotFound,
    Unavailable(String),
}

impl From<IoError> for HeldRecordError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(format!("metadata registry read failed: {error}"))
    }
}

/// Loads a document only when this node holds its current structured placement.
#[derive(Debug, PartialEq)]
pub(crate) struct HeldRecordOperation {
    config: RealmConfigDocument,
    local_node_id: NodeId,
    document_id: Ulid,
    placement: Option<PlacementRef>,
    scanning: bool,
}

impl HeldRecordOperation {
    pub(crate) fn new(
        config: RealmConfigDocument,
        local_node_id: NodeId,
        document_id: Ulid,
    ) -> Self {
        Self {
            config,
            local_node_id,
            document_id,
            placement: None,
            scanning: false,
        }
    }

    fn checked(
        &self,
        record: MetadataRegistryRecord,
    ) -> Result<MetadataRegistryRecord, HeldRecordError> {
        let placement = self.placement.ok_or(HeldRecordError::NotFound)?;
        let realm_id = self.config.realm_id;
        if !routed_record_matches(
            &self.config,
            realm_id,
            self.document_id,
            &placement,
            &record,
        ) {
            return Err(HeldRecordError::Unavailable(
                "metadata registry record does not match its structured placement".to_string(),
            ));
        }
        if !record.holder_node_ids.contains(&self.local_node_id) {
            return Err(HeldRecordError::Unavailable(
                "node is not a frozen holder for this metadata document".to_string(),
            ));
        }
        Ok(record)
    }
}

impl Operation for HeldRecordOperation {
    type Output = MetadataRegistryRecord;
    type Error = HeldRecordError;

    fn first(&mut self) -> Result<Next<MetadataRegistryRecord>, HeldRecordError> {
        let document_id = self.document_id;
        let placement = resolve_metadata_id(&self.config, self.config.realm_id, None, document_id)
            .map_err(|error| HeldRecordError::Unavailable(error.to_string()))?;
        if !holds_placement(&self.config, &placement, self.local_node_id) {
            return Err(HeldRecordError::Unavailable(format!(
                "node does not hold bucket {}/{} of metadata document `{document_id}`",
                placement.strategy_id, placement.shard
            )));
        }
        self.placement = Some(placement);
        Ok(Next::Call(read_row(
            DOCUMENT_INDEX_KEYSPACE,
            metadata_document_key(document_id),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<MetadataRegistryRecord>, HeldRecordError> {
        let document_id = self.document_id;
        if self.scanning {
            // A pending committed projection makes an empty registry read inconclusive.
            let (rows, _) = page(result).map_err(|_| {
                HeldRecordError::Unavailable(
                    "pending metadata projection scan is unavailable".to_string(),
                )
            })?;
            return Err(match rows.is_empty() {
                true => HeldRecordError::NotFound,
                false => HeldRecordError::Unavailable(format!(
                    "metadata document `{document_id}` has a queued registry projection"
                )),
            });
        }
        match value(result)? {
            Some(bytes) => Ok(Next::Done(self.checked(decode(&bytes)?)?)),
            None => {
                self.scanning = true;
                Ok(Next::Call(pending_scan(document_id)))
            }
        }
    }
}

#[cfg(test)]
#[path = "replay_tests.rs"]
mod tests;
