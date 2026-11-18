//! Reads the title a document is listed under: its graph root's name, else its path.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::operation::Operation;
use aruna_core::rows::event;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;

use super::MetadataApiError;

/// The title of one document. A node without a graph store gives none; a graph that cannot
/// be described falls back to the document path, as an unnamed root does.
#[derive(Debug, PartialEq)]
pub(crate) struct DocumentTitleOperation {
    graph_iri: String,
    document_path: String,
}

impl DocumentTitleOperation {
    pub(crate) fn new(record: &MetadataRegistryRecord) -> Self {
        Self {
            graph_iri: record.graph_iri.clone(),
            document_path: record.document_path.clone(),
        }
    }

    fn path(&self) -> Option<String> {
        (!self.document_path.is_empty()).then(|| self.document_path.clone())
    }
}

impl Operation for DocumentTitleOperation {
    type Output = Option<String>;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<Option<String>>, MetadataApiError> {
        let effect = MetadataEffect::DescribeTitle {
            graph_iri: self.graph_iri.clone(),
            document_path: self.document_path.clone(),
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            effect,
        )))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<String>>, MetadataApiError> {
        Ok(Next::Done(match event(result)? {
            Event::Metadata(MetadataEvent::DocumentTitle { title }) => title,
            Event::Metadata(MetadataEvent::Error {
                error: MetadataError::HandleMissing,
                ..
            }) => None,
            _ => self.path(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    fn title() -> DocumentTitleOperation {
        DocumentTitleOperation {
            graph_iri: MetadataRegistryRecord::graph_iri_for(Ulid::from_parts(3, 3)),
            document_path: "datasets/notes".to_string(),
        }
    }

    fn answer(event: MetadataEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Metadata(event)))
    }

    #[test]
    fn named_root_titles() {
        let mut title = title();
        assert!(matches!(title.first(), Ok(Next::Call(IoCall::Effect(_)))));
        let named = answer(MetadataEvent::DocumentTitle {
            title: Some("Field notes".to_string()),
        });
        assert_eq!(
            title.next(named),
            Ok(Next::Done(Some("Field notes".to_string())))
        );
    }

    #[test]
    fn missing_store_untitled() {
        let missing = answer(MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::HandleMissing,
        });
        assert_eq!(title().next(missing), Ok(Next::Done(None)));
        let refused = answer(MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::GraphNotFound,
        });
        let path = Some("datasets/notes".to_string());
        assert_eq!(title().next(refused), Ok(Next::Done(path)));
    }
}
