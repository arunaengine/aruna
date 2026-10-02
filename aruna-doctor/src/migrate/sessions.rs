//! Deletes S3 sessions the current format cannot read, with their expiry and owner index rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::explorer::ExplorerError;
use aruna_core::structs::identity::s3_session::S3Session;
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use std::collections::BTreeSet;

/// Rows to remove or rewrite so that only readable sessions and their index entries remain.
pub(super) struct SessionCleanup {
    pub(super) scanned: usize,
    pub(super) sessions: Vec<Vec<u8>>,
    pub(super) expiries: Vec<Vec<u8>>,
    pub(super) owner_removes: Vec<Vec<u8>>,
    pub(super) owner_writes: Vec<(Vec<u8>, Vec<u8>)>,
}

pub(super) fn stale_sessions(
    db: &OptimisticTxDatabase,
    sessions: &OptimisticTxKeyspace,
    expiries: &OptimisticTxKeyspace,
    owners: &OptimisticTxKeyspace,
) -> Result<SessionCleanup, ExplorerError> {
    let read = db.read_tx();
    let mut cleanup = SessionCleanup {
        scanned: 0,
        sessions: Vec::new(),
        expiries: Vec::new(),
        owner_removes: Vec::new(),
        owner_writes: Vec::new(),
    };
    let mut live = BTreeSet::new();
    for entry in read.iter(sessions) {
        let (key, value) = entry.into_inner()?;
        cleanup.scanned += 1;
        match S3Session::from_bytes(&value) {
            Ok(session) if session.access_key.as_bytes() == key.as_ref() => {
                live.insert(session.access_key);
            }
            _ => cleanup.sessions.push(key.to_vec()),
        }
    }
    for entry in read.iter(expiries) {
        let (key, _) = entry.into_inner()?;
        let access_key = key.get(8..).and_then(|name| std::str::from_utf8(name).ok());
        if !access_key.is_some_and(|access_key| live.contains(access_key)) {
            cleanup.expiries.push(key.to_vec());
        }
    }
    for entry in read.iter(owners) {
        let (key, value) = entry.into_inner()?;
        let Ok(index) = postcard::from_bytes::<BTreeSet<String>>(&value) else {
            cleanup.owner_removes.push(key.to_vec());
            continue;
        };
        let kept: BTreeSet<String> = index.intersection(&live).cloned().collect();
        if kept.is_empty() {
            cleanup.owner_removes.push(key.to_vec());
        } else if kept.len() != index.len() {
            let value = postcard::to_allocvec(&kept)
                .map_err(|error| ExplorerError::Decode(error.to_string()))?;
            cleanup.owner_writes.push((key.to_vec(), value));
        }
    }
    Ok(cleanup)
}

#[cfg(test)]
mod tests {
    use super::super::migrate_output;
    use super::super::tests::{read, write};
    use aruna_core::UserId;
    use aruna_core::credential_encryption::EncryptedS3Secret;
    use aruna_core::keyspaces::{
        S3_SESSION_KEYSPACE, SESSION_EXPIRY_KEYSPACE, SESSION_OWNER_KEYSPACE,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::identity::s3_session::S3Session;
    use std::collections::BTreeSet;
    use std::time::{Duration, SystemTime};
    use ulid::Ulid;

    fn access_key(id: u128) -> String {
        S3Session::build_access_key(&Ulid::from(id).to_string()).expect("access key")
    }

    fn expiry_key(access_key: &str) -> Vec<u8> {
        [&100u64.to_be_bytes()[..], access_key.as_bytes()].concat()
    }

    #[test]
    fn deletes_old_sessions() {
        let (live, old) = (access_key(1), access_key(2));
        let session = S3Session {
            access_key: live.clone(),
            user_identity: UserId::local(Ulid::from(3), RealmId::from_bytes([1; 32])),
            group_id: Ulid::from(4),
            secret: EncryptedS3Secret::empty(),
            token_hash: S3Session::hash_token("token"),
            expiry: SystemTime::UNIX_EPOCH + Duration::from_secs(100),
            path_restrictions: None,
            issued_by: [5; 32],
            last_used_at: None,
            previous: None,
        };
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().join("db");
        let database = path.to_str().expect("path");
        // A row from before the format change no longer decodes as a session.
        write(
            &path,
            S3_SESSION_KEYSPACE,
            vec![
                (live.as_bytes(), session.to_bytes().expect("encodes")),
                (old.as_bytes(), vec![1, 2, 3]),
            ],
        );
        let (live_expiry, old_expiry) = (expiry_key(&live), expiry_key(&old));
        write(
            &path,
            SESSION_EXPIRY_KEYSPACE,
            vec![(&live_expiry, Vec::new()), (&old_expiry, Vec::new())],
        );
        let shared = BTreeSet::from([live.clone(), old.clone()]);
        let stale = BTreeSet::from([old.clone()]);
        write(
            &path,
            SESSION_OWNER_KEYSPACE,
            vec![
                (b"shared", postcard::to_allocvec(&shared).expect("encodes")),
                (b"stale", postcard::to_allocvec(&stale).expect("encodes")),
            ],
        );

        let output = migrate_output(database).expect("migration runs");

        assert_eq!((output.sessions_scanned, output.sessions_deleted), (2, 1));
        let sessions = read(&path, S3_SESSION_KEYSPACE);
        assert_eq!(sessions.keys().collect::<Vec<_>>(), [live.as_bytes()]);
        let expiries = read(&path, SESSION_EXPIRY_KEYSPACE);
        assert_eq!(expiries.keys().collect::<Vec<_>>(), [&live_expiry]);
        let owners = read(&path, SESSION_OWNER_KEYSPACE);
        assert_eq!(owners.len(), 1);
        let kept: BTreeSet<String> =
            postcard::from_bytes(&owners[b"shared".as_slice()]).expect("index decodes");
        assert_eq!(kept, BTreeSet::from([live]));
        let again = migrate_output(database).expect("migration repeats");
        assert_eq!(again.sessions_deleted, 0);
    }
}
