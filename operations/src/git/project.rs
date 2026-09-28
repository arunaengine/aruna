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
use lru::LruCache;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{LazyLock, Mutex};
use tokio::sync::OwnedMutexGuard;
use ulid::Ulid;

static LOCKS: LazyLock<DocumentLocks> = LazyLock::new(DocumentLocks::default);

/// Serializes projection, snapshots and pushes of one document on this node.
pub async fn lock(document_id: Ulid) -> OwnedMutexGuard<()> {
    LOCKS.lock(document_id).await
}

/// The refs each document's cache last served, which a push must build on.
static SERVED: LazyLock<
    std::sync::Mutex<std::collections::HashMap<Ulid, BTreeMap<String, String>>>,
> = LazyLock::new(Default::default);

/// The refs this node last served for the document, if it projected it.
pub fn served(document_id: Ulid) -> Option<BTreeMap<String, String>> {
    SERVED.lock().ok()?.get(&document_id).cloned()
}

#[derive(Clone)]
pub struct Projection {
    pub state: GitState,
    pub records: Vec<GitRecord>,
    pub holders: Vec<NodeId>,
}

/// Documents whose last refresh and ancestry answers this node keeps; older ones drop first.
const REMEMBERED: NonZeroUsize = NonZeroUsize::new(256).unwrap();
/// More kept ancestry answers than this for one document are dropped instead.
const MAX_ANSWERS: usize = 4096;

/// The node, document's last event, update time and whether other holders may generate yet.
type Inputs = (Option<NodeId>, Ulid, u64, bool);

type Recent = Mutex<LruCache<Ulid, (Inputs, Projection)>>;
static RECENT: LazyLock<Recent> = LazyLock::new(|| Mutex::new(LruCache::new(REMEMBERED)));

/// Answers that a commit is an ancestor never change, so later projections reuse them.
static ANSWERS: LazyLock<Mutex<LruCache<Ulid, Ancestry>>> =
    LazyLock::new(|| Mutex::new(LruCache::new(REMEMBERED)));

fn inputs(node: Option<NodeId>, document: &MetadataRegistryRecord, overdue: bool) -> Inputs {
    (
        node,
        document.last_event_id,
        document.updated_at_ms,
        overdue,
    )
}

/// The projection of the last refresh, if the document, its holders and all its records,
/// replicated ones included, are unchanged since then.
pub async fn recent(
    context: &DriverContext,
    store: &GitStore,
    document: &MetadataRegistryRecord,
    overdue: bool,
) -> Result<Option<Projection>, GitError> {
    let node = context.net_handle.as_ref().map(|net| net.node_id());
    let Some(projection) = remembered(node, document, overdue) else {
        return Ok(None);
    };
    if !store.configured(document.document_id).await {
        return Ok(None);
    }
    let holders = publish::holders(context, document).await?;
    let records = records::scan(context, document.document_id).await?;
    Ok((holders == projection.holders && records == projection.records).then_some(projection))
}

fn remembered(
    node: Option<NodeId>,
    document: &MetadataRegistryRecord,
    overdue: bool,
) -> Option<Projection> {
    let mut recent = RECENT.lock().ok()?;
    let (known, projection) = recent.get(&document.document_id)?;
    (*known == inputs(node, document, overdue)).then(|| projection.clone())
}

pub fn remember(
    node: Option<NodeId>,
    document: &MetadataRegistryRecord,
    overdue: bool,
    projection: &Projection,
) {
    if let Ok(mut recent) = RECENT.lock() {
        let entry = (inputs(node, document, overdue), projection.clone());
        recent.put(document.document_id, entry);
    }
}

/// Drops the remembered refresh, so the next one projects again.
pub fn forget(document_id: Ulid) {
    if let Ok(mut recent) = RECENT.lock() {
        recent.pop(&document_id);
    }
}

fn answered(document_id: Ulid) -> Ancestry {
    ANSWERS
        .lock()
        .ok()
        .and_then(|mut known| known.get(&document_id).cloned())
        .unwrap_or_default()
}

fn learn(document_id: Ulid, ancestry: &Ancestry) {
    let answers: Ancestry = ancestry
        .iter()
        .filter(|(_, answer)| **answer)
        .map(|(pair, answer)| (pair.clone(), *answer))
        .collect();
    if let Ok(mut known) = ANSWERS.lock() {
        if answers.len() > MAX_ANSWERS {
            known.pop(&document_id);
        } else {
            known.put(document_id, answers);
        }
    }
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
                GitChange::Checkpoint(checkpoint) => checkpoint.packs.contains(pack),
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
    let mut ancestry = answered(id);
    if !ancestry.is_empty() {
        (state, needs) = reduce(&records, &ancestry);
    }
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
            learn(id, &ancestry);
            if let Ok(mut served) = SERVED.lock() {
                served.insert(id, state.refs.clone());
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;

    fn document() -> MetadataRegistryRecord {
        let realm_id = RealmId::from_bytes([7; 32]);
        let document_id = Ulid::generate();
        MetadataRegistryRecord {
            realm_id,
            group_id: Ulid::from_parts(7, 1),
            document_id,
            document_path: "datasets/cached".into(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: String::new(),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::from(1),
            last_event_id: Ulid::from(1),
        }
    }

    #[test]
    fn inputs_decide_reuse() {
        let node = Some(iroh::SecretKey::from_bytes(&[1; 32]).public());
        let document = document();
        let mut projection = Projection {
            state: GitState::default(),
            records: Vec::new(),
            holders: Vec::new(),
        };
        projection
            .state
            .refs
            .insert("refs/heads/main".into(), "a".repeat(40));
        remember(node, &document, false, &projection);
        let reused = remembered(node, &document, false).expect("unchanged inputs reuse");
        assert_eq!(reused.state, projection.state);
        let other = Some(iroh::SecretKey::from_bytes(&[2; 32]).public());
        assert!(remembered(other, &document, false).is_none());
        // Other holders may generate once the revision is overdue, so that refreshes again.
        assert!(remembered(node, &document, true).is_none());
        let edited = MetadataRegistryRecord {
            last_event_id: Ulid::from(2),
            updated_at_ms: 2,
            ..document.clone()
        };
        assert!(remembered(node, &edited, false).is_none());
        forget(document.document_id);
        assert!(remembered(node, &document, false).is_none());
    }

    #[test]
    fn keeps_true_answers() {
        let id = Ulid::generate();
        let pair = |first: char, second: char| (first.to_string(), second.to_string());
        let ancestry = Ancestry::from([(pair('a', 'b'), true), (pair('b', 'c'), false)]);
        learn(id, &ancestry);
        // A missing commit answers false until its pack arrives, so false is asked again.
        assert_eq!(answered(id), Ancestry::from([(pair('a', 'b'), true)]));
        let many = (0..=MAX_ANSWERS)
            .map(|index| ((index.to_string(), "x".into()), true))
            .collect();
        learn(id, &many);
        assert!(answered(id).is_empty());
    }
}
