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
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use std::sync::{LazyLock, Mutex};
use tracing::warn;
use ulid::Ulid;

/// Orders pending merges as they are accepted; later ones build on earlier ones.
static SEQUENCE: LazyLock<Mutex<ulid::Generator>> = LazyLock::new(Default::default);

/// The local row of a pending merge, written with the push record that brings it. Keys
/// sort by acceptance, so merges apply in the order their pushes moved `main`.
pub fn entry(
    document_id: Ulid,
    merge: &PendingMerge,
) -> Result<(String, byteview::ByteView, byteview::ByteView), GitError> {
    let sequence = SEQUENCE
        .lock()
        .map_err(|_| GitError::Unavailable)?
        .generate()
        .map_err(|_| GitError::Unavailable)?;
    let mut key = document_id.to_bytes().to_vec();
    key.extend_from_slice(&sequence.to_bytes());
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
            // Later merges build on this one, so they wait too.
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
            // Only a verdict on the merged crate itself is final; anything else is retried.
            Err(UpdateDocumentError::MetadataError(
                error @ (MetadataError::InvalidInput(_)
                | MetadataError::Validation(_)
                | MetadataError::ProfileValidation(_)),
            )) => return Err(GitError::Refused(error.to_string())),
            Err(error) => {
                warn!(document_id = %document.document_id, %error, "Pushed metadata waits");
                return Err(GitError::Unavailable);
            }
        }
    }
    Err(GitError::Stale)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;

    #[test]
    fn rows_keep_order() {
        let merge = |new: char| PendingMerge {
            user_id: aruna_core::UserId::new(Ulid::from(1), RealmId([1; 32])),
            old: ZERO_OID.into(),
            new: new.to_string().repeat(40),
        };
        let document = Ulid::from(9);
        // Commit ids sort the other way; the rows must still follow acceptance.
        let keys: Vec<_> = ['f', 'a', 'c']
            .into_iter()
            .map(|new| entry(document, &merge(new)).expect("row").1.to_vec())
            .collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }
}
