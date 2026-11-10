//! Rewrites rows whose record gained trailing optional fields by appending the missing `None`s.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{Rewrites, row_note};
use crate::explorer::ExplorerError;
use aruna_core::keyspaces::ADMIN_AUDIT_KEYSPACE;
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use serde::de::DeserializeOwned;

/// A keyspace and how many optional fields its record gained at its end since its first shape.
pub(super) type Added = (&'static str, usize);

/// Admin audit rows before `secret`.
pub(super) const AUDIT: Added = (ADMIN_AUDIT_KEYSPACE, 1);

/// Rows of `T` that lack trailing optional fields gain them as `None`; current rows stay as
/// they are. Rows that match no shape are added to `unknown` and left unchanged.
pub(super) fn missing_options<T: DeserializeOwned>(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    (name, added): Added,
    unknown: &mut Vec<String>,
) -> Result<Rewrites, ExplorerError> {
    let mut scanned = 0;
    let mut rows = Vec::new();
    for entry in db.read_tx().iter(keyspace) {
        let (key, value) = entry.into_inner()?;
        scanned += 1;
        if postcard::from_bytes::<T>(&value).is_ok() {
            continue;
        }
        match filled::<T>(&value, added) {
            Some(bytes) => rows.push((key.to_vec(), bytes)),
            None => unknown.push(row_note(name, &key, "matches no known shape")),
        }
    }
    Ok(Rewrites { scanned, rows })
}

/// Postcard writes `None` as one zero byte. The shortest suffix goes first, so a row
/// missing only the newest field keeps the values it has.
fn filled<T: DeserializeOwned>(value: &[u8], added: usize) -> Option<Vec<u8>> {
    (1..=added)
        .map(|missing| [value, &vec![0; missing]].concat())
        .find(|bytes| postcard::from_bytes::<T>(bytes).is_ok())
}

#[cfg(test)]
mod tests {
    use super::super::migrate_output;
    use super::super::tests::{read, write};
    use super::*;
    use aruna_core::UserId;
    use aruna_core::secrets::{SecretName, SecretScope};
    use aruna_core::structs::identity::admin_audit::{
        AdminAuditAction, AdminAuditRecord, SecretAudit,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::types::GroupId;
    use serde::{Deserialize, Serialize};
    use ulid::Ulid;

    /// An admin audit row as stored before `secret`.
    #[derive(Serialize)]
    struct OldAudit {
        audit_id: Ulid,
        action: AdminAuditAction,
        actor: UserId,
        target: UserId,
        group_id: Option<GroupId>,
        session_id: Option<String>,
    }

    #[test]
    fn audit_gains_secret() {
        let actor = UserId::local(Ulid::from_parts(1, 1), RealmId::from_bytes([1; 32]));
        let old = OldAudit {
            audit_id: Ulid::from_parts(2, 2),
            action: AdminAuditAction::RestrictedTokenIssued,
            actor,
            target: actor,
            group_id: Some(Ulid::from_parts(3, 3)),
            session_id: Some("session".to_string()),
        };
        let old = postcard::to_allocvec(&old).expect("old row encodes");
        let secret = SecretAudit {
            scope: SecretScope::User(actor),
            name: SecretName::try_from("catalog-key".to_string()).expect("name"),
            incarnation: Some(Ulid::from_parts(4, 4)),
            write_id: None,
        };
        let record = AdminAuditRecord::for_secret(AdminAuditAction::SecretRead, actor, secret);
        let current = record.to_bytes().expect("current row encodes");
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().join("db");
        let database = path.to_str().expect("path");
        let rows = vec![
            (b"old".as_slice(), old),
            (b"current".as_slice(), current.clone()),
            (b"broken".as_slice(), vec![0xff; 3]),
        ];
        write(&path, ADMIN_AUDIT_KEYSPACE, rows);
        let output = migrate_output(database).expect("migration runs");
        assert_eq!(
            (output.audit_rows_scanned, output.audit_rows_rewritten),
            (3, 1)
        );
        assert_eq!(output.unknown_rows.len(), 1);
        assert!(output.unknown_rows[0].starts_with("admin_audit/"));
        let stored = read(&path, ADMIN_AUDIT_KEYSPACE);
        let migrated = AdminAuditRecord::from_bytes(&stored[b"old".as_slice()]).expect("decodes");
        assert_eq!(migrated.session_id.as_deref(), Some("session"));
        assert_eq!(migrated.secret, None);
        assert_eq!(stored[b"current".as_slice()], current);
        assert_eq!(stored[b"broken".as_slice()], vec![0xff; 3]);
        let again = migrate_output(database).expect("migration repeats");
        assert_eq!(again.audit_rows_rewritten, 0);
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Grown {
        id: u8,
        older: Option<u8>,
        newest: Option<u8>,
    }

    #[test]
    fn keeps_older_values() {
        let current = postcard::to_allocvec(&Grown {
            id: 1,
            older: Some(7),
            newest: None,
        })
        .expect("row encodes");
        let without_newest = &current[..current.len() - 1];
        let rewritten = filled::<Grown>(without_newest, 2).expect("shape matches");
        assert_eq!(rewritten, current);
        let first = postcard::to_allocvec(&1u8).expect("id encodes");
        let rewritten = filled::<Grown>(&first, 2).expect("shape matches");
        let grown = postcard::from_bytes::<Grown>(&rewritten).expect("decodes");
        assert_eq!((grown.older, grown.newest), (None, None));
    }
}
