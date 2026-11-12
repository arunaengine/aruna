//! Rewrites realm configs and queued install writes whose plugin installs predate `activated_by`
//! and `widened` (alpha.184) or `secrets` (alpha.188); a relayed queued write stops the migration.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::UserId;
use aruna_core::admin_documents::AdminDocumentOperation;
use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord};
use aruna_core::plugin_installs::{InstallState, PluginInstall, Prerequisite, Widening};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::{GroupId, RoleId};
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use ulid::Ulid;

use super::session_refs::{self, Layout};
use super::{Rewrites, decode_error, row_note};
use crate::explorer::ExplorerError;

/// Installs inside queued publishes: alpha.188 stored 20 fields, alpha.184 stored 18.
const INSTALL_LAYOUTS: [Layout; 2] = [&[("PluginInstall", 20)], &[("PluginInstall", 18)]];

/// An install as alpha.184 stored it.
#[derive(Deserialize)]
struct StoredInstall {
    install_id: Ulid,
    group_id: GroupId,
    plugin: String,
    version: String,
    digest: String,
    state: InstallState,
    paused: bool,
    settings: String,
    grants: BTreeMap<String, Vec<String>>,
    account: UserId,
    role_id: RoleId,
    receiver: aruna_core::NodeId,
    granted_by: UserId,
    request: String,
    revision: u64,
    active_since_ms: Option<u64>,
    failed: Option<String>,
    prerequisites: Vec<Prerequisite>,
}

/// An install as alpha.188 stored it: the alpha.184 fields, then the coverage fields.
#[derive(Deserialize)]
struct CoveredInstall(
    Ulid,
    GroupId,
    String,
    String,
    String,
    InstallState,
    bool,
    String,
    BTreeMap<String, Vec<String>>,
    UserId,
    RoleId,
    aruna_core::NodeId,
    UserId,
    String,
    u64,
    Option<u64>,
    Option<String>,
    Vec<Prerequisite>,
    Option<aruna_core::NodeId>,
    Option<Widening>,
);

impl From<CoveredInstall> for PluginInstall {
    fn from(old: CoveredInstall) -> Self {
        let stored = StoredInstall {
            install_id: old.0,
            group_id: old.1,
            plugin: old.2,
            version: old.3,
            digest: old.4,
            state: old.5,
            paused: old.6,
            settings: old.7,
            grants: old.8,
            account: old.9,
            role_id: old.10,
            receiver: old.11,
            granted_by: old.12,
            request: old.13,
            revision: old.14,
            active_since_ms: old.15,
            failed: old.16,
            prerequisites: old.17,
        };
        Self {
            activated_by: old.18,
            widened: old.19,
            ..stored.into()
        }
    }
}

impl From<StoredInstall> for PluginInstall {
    fn from(old: StoredInstall) -> Self {
        Self {
            install_id: old.install_id,
            group_id: old.group_id,
            plugin: old.plugin,
            version: old.version,
            digest: old.digest,
            state: old.state,
            paused: old.paused,
            settings: old.settings,
            grants: old.grants,
            account: old.account,
            role_id: old.role_id,
            receiver: old.receiver,
            granted_by: old.granted_by,
            request: old.request,
            revision: old.revision,
            active_since_ms: old.active_since_ms,
            failed: old.failed,
            prerequisites: old.prerequisites,
            activated_by: None,
            widened: None,
            secrets: BTreeMap::new(),
        }
    }
}

/// The config re-encoded with its stored installs in the current shape, or `None` when no split
/// of the row reads as the fields before the install list and a list of stored installs.
pub(super) fn upgraded(value: &[u8]) -> Option<Vec<u8>> {
    upgraded_from::<CoveredInstall>(value).or_else(|| upgraded_from::<StoredInstall>(value))
}

fn upgraded_from<T: DeserializeOwned + Into<PluginInstall>>(value: &[u8]) -> Option<Vec<u8>> {
    // An empty list is one zero byte, so a config with no installs already decodes.
    for split in (0..value.len()).rev() {
        let Ok((stored, rest)) = postcard::take_from_bytes::<Vec<T>>(&value[split..]) else {
            continue;
        };
        if !rest.is_empty() || stored.is_empty() {
            continue;
        }
        let head = [&value[..split], &[0u8][..]].concat();
        let Ok(mut config) = RealmConfigDocument::from_bytes(&head) else {
            continue;
        };
        config.installs = stored.into_iter().map(Into::into).collect();
        return postcard::to_allocvec(&config).ok();
    }
    None
}

/// A queued publish read with an install of an earlier release.
pub(super) fn queued_install(value: &[u8]) -> Option<DocumentOutboxRecord> {
    (INSTALL_LAYOUTS.iter()).find_map(|layout| session_refs::decode(value, layout).ok())
}

/// Queued install writes this node originated, in the current shape. This node signs them when
/// it publishes; a relayed one keeps the origin signature over its old bytes and stops the run.
pub(super) fn outbox_rows(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    name: &str,
) -> Result<Rewrites, ExplorerError> {
    let mut scanned = 0;
    let mut rows = Vec::new();
    for entry in db.read_tx().iter(keyspace) {
        let (key, value) = entry.into_inner()?;
        scanned += 1;
        if postcard::from_bytes::<DocumentOutboxRecord>(&value).is_ok() {
            continue;
        }
        let Some(record) = queued_install(&value) else {
            continue;
        };
        let DocumentOutboxEvent::AdminOperation {
            event,
            origin_signature,
        } = &record.event
        else {
            continue;
        };
        if !matches!(event.op, AdminDocumentOperation::ConfigInstallSet { .. }) {
            continue;
        }
        if origin_signature.is_some() {
            return Err(ExplorerError::Refused(row_note(
                name,
                &key,
                "a relayed plugin install write from an earlier release waits for publishing, \
                 and its origin signature covers the old encoding. Start the node with the \
                 earlier release until its sync outbox has published the row, stop it, then run \
                 the migration again",
            )));
        }
        let bytes =
            postcard::to_allocvec(&record).map_err(|error| decode_error(name, &key, error))?;
        rows.push((key.to_vec(), bytes));
    }
    Ok(Rewrites { scanned, rows })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;
    use serde::Serialize;

    /// The alpha.184 shape, to write rows as that release did.
    #[derive(Serialize)]
    struct OldInstall {
        install_id: Ulid,
        group_id: GroupId,
        plugin: String,
        version: String,
        digest: String,
        state: InstallState,
        paused: bool,
        settings: String,
        grants: BTreeMap<String, Vec<String>>,
        account: UserId,
        role_id: RoleId,
        receiver: aruna_core::NodeId,
        granted_by: UserId,
        request: String,
        revision: u64,
        active_since_ms: Option<u64>,
        failed: Option<String>,
        prerequisites: Vec<Prerequisite>,
    }

    fn old_install(seed: u8, realm_id: RealmId) -> OldInstall {
        let id = Ulid::from_parts(u64::from(seed), 1);
        OldInstall {
            install_id: id,
            group_id: Ulid::from_parts(9, 9),
            plugin: "announcer".into(),
            version: "0.1.0".into(),
            digest: format!("sha256:{}", "ab".repeat(32)),
            state: InstallState::Active,
            paused: false,
            settings: "{}".into(),
            grants: BTreeMap::from([("metadata.read".into(), vec!["/r/g/x/meta/**".into()])]),
            account: UserId::local(id, realm_id),
            role_id: id,
            receiver: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            granted_by: UserId::local(Ulid::from_parts(3, 3), realm_id),
            request: format!("sha256:{}", "cd".repeat(32)),
            revision: 2,
            active_since_ms: Some(1_000),
            failed: None,
            prerequisites: Vec::new(),
        }
    }

    #[test]
    fn keeps_stored_installs() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let empty = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        let current = postcard::to_allocvec(&empty).unwrap();
        // The old row: the config without its empty install list, then two old installs.
        let olds = vec![old_install(1, realm_id), old_install(2, realm_id)];
        let old = [
            &current[..current.len() - 1],
            &postcard::to_allocvec(&olds).unwrap(),
        ]
        .concat();
        assert!(RealmConfigDocument::from_bytes(&old).is_err());
        let migrated = RealmConfigDocument::from_bytes(&upgraded(&old).unwrap()).unwrap();
        assert_eq!(migrated.installs.len(), 2);
        let first = &migrated.installs[0];
        assert_eq!((first.install_id, first.revision), (olds[0].install_id, 2));
        assert_eq!(first.receiver, olds[0].receiver);
        assert_eq!(
            (first.active_since_ms, first.activated_by),
            (Some(1_000), None)
        );
        assert_eq!(first.widened, None);
        assert_eq!(migrated.realm_id, realm_id);
        assert_eq!(upgraded(&current), None);
    }

    #[test]
    fn keeps_covered_installs() {
        // An alpha.188 install: the alpha.184 fields, then `activated_by` and `widened`.
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let empty = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        let current = postcard::to_allocvec(&empty).unwrap();
        let node = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
        let widened = Widening {
            at_ms: 9,
            by: node,
            extends: 1,
        };
        let olds = vec![(old_install(1, realm_id), Some(node), Some(widened))];
        let old = [
            &current[..current.len() - 1],
            &postcard::to_allocvec(&olds).unwrap(),
        ]
        .concat();
        assert!(RealmConfigDocument::from_bytes(&old).is_err());
        let migrated = RealmConfigDocument::from_bytes(&upgraded(&old).unwrap()).unwrap();
        let first = &migrated.installs[0];
        assert_eq!(
            (first.install_id, first.revision),
            (olds[0].0.install_id, 2)
        );
        assert_eq!(
            (first.activated_by, first.widened),
            (Some(node), Some(widened))
        );
        assert!(first.secrets.is_empty());
        assert_eq!(first.prerequisites, olds[0].0.prerequisites);
    }

    /// A queued install write and its bytes as alpha.188 stored them, without `secrets`.
    fn queued(origin_signature: Option<iroh::Signature>) -> (DocumentOutboxRecord, Vec<u8>) {
        use aruna_core::admin_documents::{
            AdminDocumentClock, AdminDocumentEvent, AdminDocumentTarget,
        };
        use aruna_core::document::DocumentTarget;
        use aruna_core::structs::identity::auth::Actor;
        use aruna_core::structs::placement::record::PlacementRef;
        use aruna_operations::sync::document_outbox::new_outbox_record;
        let realm_id = RealmId::from_bytes([3; 32]);
        let origin = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let stored = postcard::to_allocvec(&old_install(4, realm_id)).unwrap();
        let install: PluginInstall = postcard::from_bytes::<StoredInstall>(&stored)
            .unwrap()
            .into();
        let event = AdminDocumentEvent {
            event_id: Ulid::from(1),
            target: AdminDocumentTarget::RealmConfig { realm_id },
            origin_node_id: origin,
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor: Actor {
                node_id: origin,
                user_id: UserId::nil(realm_id),
                realm_id,
            },
            op: AdminDocumentOperation::ConfigInstallSet {
                install_id: install.install_id,
                install: Some(Box::new(install.clone())),
            },
        };
        let event = DocumentOutboxEvent::AdminOperation {
            event: Box::new(event),
            origin_signature,
        };
        let target = DocumentTarget::RealmConfig { realm_id };
        let record = new_outbox_record(origin, target, Vec::new(), event, PlacementRef::NIL, false);
        let current = postcard::to_allocvec(&record).unwrap();
        let encoded = postcard::to_allocvec(&install).unwrap();
        // The empty `secrets` map is the install's last byte.
        let old = super::super::tests::strip(&current, &encoded, 1);
        (record, old)
    }

    #[test]
    fn rewrites_queued_installs() {
        use super::super::migrate_output;
        use super::super::tests::{read, write};
        use aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE;
        let (record, old) = queued(None);
        assert!(postcard::from_bytes::<DocumentOutboxRecord>(&old).is_err());
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("db");
        let database = path.to_str().unwrap();
        write(&path, SYNC_OUTBOX_KEYSPACE, vec![(b"install", old)]);
        let output = migrate_output(database).unwrap();
        assert_eq!(output.outbox_installs_rewritten, 1);
        let rows = read(&path, SYNC_OUTBOX_KEYSPACE);
        let migrated: DocumentOutboxRecord =
            postcard::from_bytes(&rows[b"install".as_slice()]).unwrap();
        assert_eq!(migrated, record);
        assert_eq!(
            migrate_output(database).unwrap().outbox_installs_rewritten,
            0
        );
        // A relayed write keeps its origin's signature over the old bytes, so it stops the run.
        let signature = iroh::SecretKey::from_bytes(&[1; 32]).sign(b"install");
        let (_, relayed) = queued(Some(signature));
        write(
            &path,
            SYNC_OUTBOX_KEYSPACE,
            vec![(b"relayed", relayed.clone())],
        );
        let refused = migrate_output(database);
        assert!(matches!(refused, Err(ExplorerError::Refused(_))));
        let rows = read(&path, SYNC_OUTBOX_KEYSPACE);
        assert_eq!(rows[b"relayed".as_slice()], relayed);
    }
}
