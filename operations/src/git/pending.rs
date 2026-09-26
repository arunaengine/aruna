//! Applies the metadata of accepted pushes to main, retrying until the graph takes it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::snapshot::execute;
use super::{GitError, records};
use crate::driver::DriverContext;
use crate::metadata::update_document::{
    UpdateDocumentConfig, UpdateDocumentError, UpdateDocumentMutation, UpdateDocumentOperation,
    update_metadata_document,
};
use aruna_blob::git::GitStore;
use aruna_core::git::{GitEffect, GitEvent, PENDING, PendingMerge, ZERO_OID};
use aruna_core::metadata::{MetadataEffect, MetadataEvent};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use tracing::warn;
use ulid::Ulid;

/// The local row of a pending merge, written with the push record that brings it.
pub fn entry(
    document_id: Ulid,
    merge: &PendingMerge,
) -> Result<(String, byteview::ByteView, byteview::ByteView), GitError> {
    let mut key = document_id.to_bytes().to_vec();
    key.extend_from_slice(merge.new.as_bytes());
    let value = postcard::to_allocvec(merge).map_err(|_| GitError::Invalid)?;
    Ok((PENDING.into(), key.into(), value.into()))
}

/// Applies every pending merge of the document. A merge that cannot apply yet stays for the
/// next call; one the metadata refuses is dropped and logged, since retrying cannot help.
pub async fn apply(
    context: &DriverContext,
    store: &GitStore,
    document: &MetadataRegistryRecord,
) -> Result<(), GitError> {
    let prefix = document.document_id.to_bytes().to_vec();
    for (key, merge) in records::prefixed::<PendingMerge>(context, PENDING, prefix).await? {
        // Boxed: the metadata update is a deep future and Git refresh already nests deeply.
        match Box::pin(apply_one(context, store, document, &merge)).await {
            Ok(()) => records::remove(context, PENDING, key).await?,
            Err(GitError::Refused(reason)) => {
                warn!(document_id = %document.document_id, commit = %merge.new, %reason,
                    "Pushed metadata could not be merged into the document");
                records::remove(context, PENDING, key).await?;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Merges the values `merge` changed onto the current graph. The update is planned against
/// the exact graph version it was merged with, so a concurrent or late edit is never removed.
async fn apply_one(
    context: &DriverContext,
    store: &GitStore,
    document: &MetadataRegistryRecord,
    merge: &PendingMerge,
) -> Result<(), GitError> {
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(GitError::Unavailable)?;
    let node_id = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(GitError::Unavailable)?;
    for _ in 0..8 {
        let exported = metadata
            .send_metadata_effect(MetadataEffect::ExportVersioned {
                graph_iri: document.graph_iri.clone(),
            })
            .await;
        let MetadataEvent::VersionedExport {
            jsonld, version, ..
        } = (match exported {
            aruna_core::events::Event::Metadata(event) => event,
            _ => return Err(GitError::Unavailable),
        })
        else {
            return Err(GitError::Unavailable);
        };
        let effect = GitEffect::MergeMetadata {
            document_id: document.document_id,
            old: (merge.old != ZERO_OID).then(|| merge.old.clone()),
            new: merge.new.clone(),
            graph: jsonld,
        };
        let jsonld = match execute(store, effect, merge.user_id).await? {
            GitEvent::MetadataMerged(Ok(Some(jsonld))) => jsonld,
            GitEvent::MetadataMerged(Ok(None)) => return Ok(()),
            GitEvent::MetadataMerged(Err(error)) => return Err(GitError::Refused(error)),
            _ => return Err(GitError::Unavailable),
        };
        let operation = UpdateDocumentOperation::new(UpdateDocumentConfig {
            actor: Actor {
                node_id,
                user_id: merge.user_id,
                realm_id: document.realm_id,
            },
            group_id: document.group_id,
            document_id: document.document_id,
            public: document.public,
            mutation: UpdateDocumentMutation::ReplaceRoCrate { jsonld },
            expected_revision: None,
        })
        .with_expected_graph(version);
        match Box::pin(update_metadata_document(operation, context)).await {
            Ok(_) => return Ok(()),
            Err(UpdateDocumentError::GraphChanged) => {}
            Err(UpdateDocumentError::MetadataError(error)) => {
                return Err(GitError::Refused(error.to_string()));
            }
            Err(error) => {
                warn!(document_id = %document.document_id, %error, "Pushed metadata waits");
                return Err(GitError::Unavailable);
            }
        }
    }
    Err(GitError::Stale)
}
