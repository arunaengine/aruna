//! Brings a holder's repository cache up to date from the document's Git records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::state::{Ancestry, GitState, reduce};
use super::{GitError, objects, publish, records};
use crate::driver::DriverContext;
use aruna_blob::git::GitStore;
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::git::{DocumentLocks, GitChange, GitEffect, GitEvent, GitRecord};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use std::sync::LazyLock;
use tokio::sync::OwnedMutexGuard;
use ulid::Ulid;

static LOCKS: LazyLock<DocumentLocks> = LazyLock::new(DocumentLocks::default);

/// Serializes projection, snapshots and pushes of one document on this node.
pub async fn lock(document_id: Ulid) -> OwnedMutexGuard<()> {
    LOCKS.lock(document_id).await
}

pub struct Projection {
    pub state: GitState,
    pub records: Vec<GitRecord>,
    pub holders: Vec<NodeId>,
}

/// Remote holders serve a pack to the user who stored it.
pub fn author(user_id: UserId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id: user_id.realm_id,
        path_restrictions: None,
        session: None,
    }
}

async fn execute(store: &GitStore, effect: GitEffect, actor: UserId) -> Result<GitEvent, GitError> {
    store
        .execute(effect, actor)
        .await
        .map_err(|_| GitError::Unavailable)
}

/// Imports missing packs and moves local refs to the records' reduced state. The caller
/// holds [`lock`] for the document. A deleted cache is rebuilt the same way.
pub async fn project(
    context: &DriverContext,
    store: &GitStore,
    document: &MetadataRegistryRecord,
) -> Result<Projection, GitError> {
    let holders = publish::holders(context, document).await?;
    let id = document.document_id;
    let actor = UserId::nil(document.realm_id);
    execute(store, GitEffect::Initialize(id), actor).await?;
    let records = records::scan(context, id).await?;
    let (mut state, mut needs) = reduce(&records, &Ancestry::new());
    let GitEvent::Imported(known) = execute(store, GitEffect::Imported(id), actor).await? else {
        return Err(GitError::Unavailable);
    };
    for pack in state
        .packs
        .iter()
        .filter(|pack| !known.contains(&pack.sha256))
    {
        let owner = records
            .iter()
            .find(|record| match &record.change {
                GitChange::Objects {
                    pack: Some(own), ..
                } => **own == *pack,
                GitChange::Checkpoint(checkpoint) => checkpoint.pack == *pack,
                _ => false,
            })
            .map_or(actor, |record| record.user_id);
        let bytes = objects::fetch(context, &author(owner), document, pack, &holders).await?;
        let digest = pack.sha256.clone();
        let effect = GitEffect::Import {
            document_id: id,
            digest,
            pack: bytes,
        };
        execute(store, effect, actor).await?;
    }
    let mut ancestry = Ancestry::new();
    // Each round answers every pair the previous one needed, so few rounds suffice.
    for _ in 0..64 {
        if needs.is_empty() {
            let GitEvent::Refs(current) = execute(store, GitEffect::Refs(id), actor).await? else {
                return Err(GitError::Unavailable);
            };
            let effect = GitEffect::SetRefs {
                document_id: id,
                expected: current,
                target: state.refs.clone(),
            };
            execute(store, effect, actor).await?;
            return Ok(Projection {
                state,
                records,
                holders,
            });
        }
        let effect = GitEffect::Ancestry {
            document_id: id,
            pairs: needs.clone(),
        };
        let GitEvent::Ancestry(answers) = execute(store, effect, actor).await? else {
            return Err(GitError::Unavailable);
        };
        ancestry.extend(needs.into_iter().zip(answers));
        (state, needs) = reduce(&records, &ancestry);
    }
    Err(GitError::Unavailable)
}
