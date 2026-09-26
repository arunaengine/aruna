//! Accepts a validated push: stores its pack and publishes its ref updates before refs move.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::state::{Ancestry, GitState, reduce};
use super::{GitError, objects, publish, records};
use crate::driver::DriverContext;
use aruna_core::git::{
    GitChange, GitRecord, LfsObject, PendingMerge, RefUpdate, StoredObject, ZERO_OID, valid_path,
    valid_ref,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// What the receive hook reports about one push, next to the pack of its new objects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushRequest {
    pub refs: Vec<RefUpdate>,
    /// SHA-256 ids of LFS objects the pushed commits point to.
    pub lfs: Vec<String>,
    /// Files the push changes, checked against other users' LFS locks.
    pub paths: Vec<String>,
}

const MAX_ITEMS: usize = 100_000;

/// A length-prefixed JSON request followed by the raw pack.
pub fn encode(request: &PushRequest, pack: &[u8]) -> Result<Vec<u8>, GitError> {
    let json = serde_json::to_vec(request).map_err(|_| GitError::Invalid)?;
    let length = u32::try_from(json.len()).map_err(|_| GitError::Invalid)?;
    Ok([&length.to_be_bytes()[..], &json, pack].concat())
}

pub fn decode(body: Bytes) -> Result<(PushRequest, Bytes), GitError> {
    let length = body
        .get(..4)
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .map(u32::from_be_bytes)
        .and_then(|length| usize::try_from(length).ok())
        .ok_or(GitError::Invalid)?;
    let end = length.checked_add(4).ok_or(GitError::Invalid)?;
    let request = serde_json::from_slice(body.get(4..end).ok_or(GitError::Invalid)?)
        .map_err(|_| GitError::Invalid)?;
    Ok((request, body.slice(end..)))
}

fn objects_in(pack: &[u8]) -> u32 {
    pack.get(8..12)
        .and_then(|count| <[u8; 4]>::try_from(count).ok())
        .map_or(0, u32::from_be_bytes)
}

/// Runs inside the receive hook of a push that already holds the document lock, so it
/// must not project. Locked paths of other users and unknown LFS objects refuse the push.
pub async fn accept(
    context: &DriverContext,
    auth: &AuthContext,
    (id, key): (Ulid, &str),
    request: PushRequest,
    pack: Bytes,
) -> Result<GitRecord, GitError> {
    let (document, repository) = super::repository(context, auth, id, Permission::WRITE).await?;
    if !super::push_key_valid(id, key) {
        return Err(GitError::NotHook);
    }
    if request.refs.is_empty()
        || request.lfs.len() + request.paths.len() > MAX_ITEMS
        || !request
            .refs
            .iter()
            .all(|update| valid_ref(&update.name, false))
        || !request.paths.iter().all(|path| valid_path(path))
    {
        return Err(GitError::Invalid);
    }
    let (mut state, _) = reduce(&records::scan(context, id).await?, &Ancestry::new());
    // The refs this node served the pushing client; a replay without ancestry answers can
    // disagree with them about fast-forwarded branches.
    if let Some(served) = super::project::served(id) {
        state.refs = served;
    }
    for update in &request.refs {
        if update.name == "refs/heads/main" && update.new == ZERO_OID {
            return Err(GitError::Refused(
                "the main branch cannot be deleted".into(),
            ));
        }
        let current = state
            .refs
            .get(&update.name)
            .map_or(ZERO_OID, String::as_str);
        if current != update.old {
            return Err(GitError::Stale);
        }
    }
    unlocked(&state, auth, &request.paths)?;
    let mut lfs = Vec::with_capacity(request.lfs.len());
    for oid in &request.lfs {
        let valid = LfsObject {
            oid: oid.clone(),
            size: 0,
        }
        .valid();
        // A known object keeps its original location, so access is decided for the original.
        let location = match state.lfs.get(oid) {
            Some(original) => Some(original.clone()),
            None => objects::copy(context, &document, oid).await?,
        };
        lfs.push(location.filter(|_| valid).ok_or(GitError::Invalid)?);
    }
    // Pushed metadata follows the recorded push; the hook already checked that it merges.
    let merge = request
        .refs
        .iter()
        .find(|update| update.name == "refs/heads/main" && update.new != ZERO_OID)
        .filter(|_| repository.arc)
        .map(|update| PendingMerge {
            user_id: auth.user_id,
            old: update.old.clone(),
            new: update.new.clone(),
        });
    let written = (pack, lfs);
    record(
        context,
        auth,
        &document,
        request.refs,
        written,
        (Vec::new(), merge),
    )
    .await
}

/// Refuses changes to files another user locked.
pub(super) fn unlocked(
    state: &GitState,
    auth: &AuthContext,
    paths: &[String],
) -> Result<(), GitError> {
    match paths.iter().find(|path| {
        state
            .locks
            .get(*path)
            .is_some_and(|lock| lock.user_id != auth.user_id)
    }) {
        Some(path) => Err(GitError::Locked(path.clone())),
        None => Ok(()),
    }
}

/// Stores a non-empty pack and publishes the ref updates as one replicated record. `made`
/// lists commits this node created itself; a pending merge is written in the same step.
pub(super) async fn record(
    context: &DriverContext,
    auth: &AuthContext,
    document: &MetadataRegistryRecord,
    refs: Vec<RefUpdate>,
    (pack, lfs): (Bytes, Vec<StoredObject>),
    (made, merge): (Vec<String>, Option<PendingMerge>),
) -> Result<GitRecord, GitError> {
    let pack = if objects_in(&pack) == 0 {
        None
    } else {
        Some(objects::store_pack(context, auth, document, pack).await?)
    };
    let change = GitChange::Objects {
        pack: pack.map(Box::new),
        refs,
        lfs,
        revision: None,
        digest: None,
        made,
    };
    let extra = match &merge {
        Some(merge) => vec![super::pending::entry(document.document_id, merge)?],
        None => Vec::new(),
    };
    publish::publish_with(context, document, auth.user_id, change, extra).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_roundtrip() {
        let request = PushRequest {
            refs: vec![RefUpdate {
                name: "refs/heads/main".into(),
                old: "0".repeat(40),
                new: "a".repeat(40),
            }],
            lfs: vec!["b".repeat(64)],
            paths: vec!["isa.investigation.xlsx".into()],
        };
        let body = encode(&request, b"PACK\0\0\0\x02\0\0\0\x03rest").expect("encodes");
        let (decoded, pack) = decode(Bytes::from(body)).expect("decodes");
        assert_eq!(decoded, request);
        assert_eq!(objects_in(&pack), 3);
        assert!(decode(Bytes::from_static(&[0, 0, 1, 0, b'{'])).is_err());
    }
}
