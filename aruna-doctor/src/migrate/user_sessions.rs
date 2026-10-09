//! Re-encodes login sessions from before a session named the linked login it came through.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::structs::identity::auth::SessionKind;
use aruna_core::structs::identity::user::session::UserSession;
use serde::Deserialize;

#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
pub(super) struct LegacySession {
    sid: String,
    user_id: UserId,
    kind: SessionKind,
    label: Option<String>,
    created_at: u64,
    expires_at: u64,
    token_hash: String,
    revoked: bool,
}

impl From<LegacySession> for UserSession {
    fn from(old: LegacySession) -> Self {
        UserSession {
            sid: old.sid,
            user_id: old.user_id,
            kind: old.kind,
            label: old.label,
            created_at: old.created_at,
            expires_at: old.expires_at,
            token_hash: old.token_hash,
            revoked: old.revoked,
            via: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::migrate_output;
    use super::super::tests::{read, write};
    use super::*;
    use aruna_core::keyspaces::USER_SESSION_KEYSPACE;

    #[test]
    fn adds_no_linked_login() {
        let old = LegacySession {
            sid: "01JCNCTR0123456789ABCDEFGH".to_string(),
            user_id: UserId::default(),
            kind: SessionKind::Portal,
            label: None,
            created_at: 1,
            expires_at: 2,
            token_hash: "aa".repeat(32),
            revoked: false,
        };
        let bytes = postcard::to_allocvec(&old).expect("old session encodes");
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().join("db");
        let database = path.to_str().expect("path");
        write(&path, USER_SESSION_KEYSPACE, vec![(b"session", bytes)]);
        let output = migrate_output(database).expect("migration runs");
        assert_eq!(output.user_sessions_rewritten, 1);
        let rows = read(&path, USER_SESSION_KEYSPACE);
        let session = UserSession::from_bytes(&rows[b"session".as_slice()]).expect("decodes");
        assert_eq!((session.via, session.created_at), (None, 1));
        let again = migrate_output(database).expect("migration repeats");
        assert_eq!(again.user_sessions_rewritten, 0);
    }
}
