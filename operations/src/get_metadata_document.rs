use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::metadata::{
    MetadataDocumentView, MetadataEffect, MetadataError, MetadataEvent,
    MetadataMaterializationState,
};
use aruna_core::operation::Operation;
use aruna_core::structs::MetadataRegistryRecord;
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::metadata::repository::{
    StorageReadError, parse_graph_lifecycle_read, parse_materialization_status_read,
    parse_registry_read, read_graph_lifecycle_effect, read_materialization_status_effect,
    read_registry_by_document_effect, read_registry_effect,
};

#[derive(Debug, PartialEq)]
pub struct GetMetadataDocumentOperation {
    group_id: GroupId,
    document_id: MetaResourceId,
    record: Option<MetadataRegistryRecord>,
    state: GetMetadataDocumentState,
    output: Option<Result<MetadataDocumentView, GetMetadataDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum GetMetadataDocumentState {
    Init,
    ReadRecord,
    ReadGraphLifecycle,
    ReadMaterializationStatus,
    ExportRoCrate,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetMetadataDocumentError {
    #[error(transparent)]
    StorageError(#[from] aruna_core::errors::StorageError),
    #[error(transparent)]
    ConversionError(#[from] aruna_core::errors::ConversionError),
    #[error(transparent)]
    MetadataError(#[from] MetadataError),
    #[error("document not found")]
    DocumentNotFound,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl GetMetadataDocumentOperation {
    pub fn new(group_id: GroupId, document_id: MetaResourceId) -> Self {
        Self {
            group_id,
            document_id,
            record: None,
            state: GetMetadataDocumentState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: GetMetadataDocumentError) -> Effects {
        self.state = GetMetadataDocumentState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(GetMetadataDocumentError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

pub async fn load_metadata_record_by_document(
    context: &DriverContext,
    document_id: MetaResourceId,
) -> Result<Option<MetadataRegistryRecord>, StorageReadError> {
    let event = context
        .storage_handle
        .send_effect(read_registry_by_document_effect(document_id, None))
        .await;
    parse_registry_read(event)
}

pub async fn is_metadata_record_materialized_for_graph_read(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
) -> Result<bool, StorageReadError> {
    let event = context
        .storage_handle
        .send_effect(read_materialization_status_effect(record.document_id, None))
        .await;
    let Some(status) = parse_materialization_status_read(event)? else {
        return Ok(true);
    };
    Ok(status.event_id != record.last_event_id
        || matches!(status.state, MetadataMaterializationState::Materialized))
}

impl Operation for GetMetadataDocumentOperation {
    type Output = MetadataDocumentView;
    type Error = GetMetadataDocumentError;

    fn start(&mut self) -> Effects {
        self.state = GetMetadataDocumentState::ReadRecord;
        smallvec![read_registry_effect(self.group_id, self.document_id, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetMetadataDocumentState::ReadRecord => match parse_registry_read(event) {
                Ok(Some(record)) => {
                    let graph_iri = record.graph_iri.clone();
                    self.record = Some(record);
                    self.state = GetMetadataDocumentState::ReadGraphLifecycle;
                    smallvec![read_graph_lifecycle_effect(&graph_iri, None)]
                }
                Ok(None) => self.fail(GetMetadataDocumentError::DocumentNotFound),
                Err(StorageReadError::Storage(error)) => self.fail(error.into()),
                Err(StorageReadError::Conversion(error)) => self.fail(error.into()),
            },
            GetMetadataDocumentState::ReadGraphLifecycle => match parse_graph_lifecycle_read(event)
            {
                Ok(Some(record)) if record.is_deleted() => {
                    self.fail(GetMetadataDocumentError::DocumentNotFound)
                }
                Ok(_) => {
                    let Some(record) = self.record.as_ref() else {
                        return self.fail(GetMetadataDocumentError::DocumentNotFound);
                    };
                    self.state = GetMetadataDocumentState::ReadMaterializationStatus;
                    smallvec![read_materialization_status_effect(record.document_id, None)]
                }
                Err(StorageReadError::Storage(error)) => self.fail(error.into()),
                Err(StorageReadError::Conversion(error)) => self.fail(error.into()),
            },
            GetMetadataDocumentState::ReadMaterializationStatus => {
                match parse_materialization_status_read(event) {
                    Ok(status) => {
                        let Some(record) = self.record.as_ref() else {
                            return self.fail(GetMetadataDocumentError::DocumentNotFound);
                        };
                        if status.is_some_and(|status| {
                            status.event_id == record.last_event_id
                                && !matches!(
                                    status.state,
                                    MetadataMaterializationState::Materialized
                                )
                        }) {
                            return self.fail(MetadataError::GraphNotFound.into());
                        }
                        let graph_iri = record.graph_iri.clone();
                        self.state = GetMetadataDocumentState::ExportRoCrate;
                        smallvec![aruna_core::effects::Effect::Metadata(
                            MetadataEffect::ExportRoCrate { graph_iri },
                        )]
                    }
                    Err(StorageReadError::Storage(error)) => self.fail(error.into()),
                    Err(StorageReadError::Conversion(error)) => self.fail(error.into()),
                }
            }
            GetMetadataDocumentState::ExportRoCrate => match event {
                Event::Metadata(MetadataEvent::RoCrateExportResult { jsonld, .. }) => {
                    let Some(record) = self.record.take() else {
                        return self.fail(GetMetadataDocumentError::DocumentNotFound);
                    };
                    self.state = GetMetadataDocumentState::Finish;
                    self.output = Some(Ok(MetadataDocumentView { record, jsonld }));
                    smallvec![]
                }
                Event::Metadata(MetadataEvent::Error { error, .. }) => self.fail(error.into()),
                other => self.unexpected_event("metadata export result", format!("{other:?}")),
            },
            GetMetadataDocumentState::Finish
            | GetMetadataDocumentState::Error
            | GetMetadataDocumentState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            GetMetadataDocumentState::Finish | GetMetadataDocumentState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.expect("metadata get operation must set output")
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
