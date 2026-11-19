//! Builds the job ids, report rows, crate documents, admin conflicts, realm records and the
//! environment guard that tests of several crates share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde_json::{Value, json};
use ulid::Ulid;

use crate::admin_documents::AdminDocumentDot;
use crate::reducer::{AdminConflict, AdminConflictValue};
use crate::structs::execution::job::{ImportReportDetail, ImportReportRow, JobId, ReasonCode};
use crate::structs::identity::realm::{OidcProviderConfig, RealmId};
use crate::structs::placement::record::{FIRST_GRANTABLE_HANDLE, PlacementRef};
use crate::structs::storage::metadata_registry::MetadataRegistryRecord;
use crate::structured_id::{BucketId, PlacementHandle};

/// A job id at `timestamp_ms` in the first grantable handle and bucket 0.
pub fn job_at(timestamp_ms: u64, nonce: u64) -> JobId {
    JobId::from_parts(
        timestamp_ms,
        PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
        BucketId::new(0).unwrap(),
        nonce,
    )
    .unwrap()
}

/// An import report row that says `entry_key` was imported under its own name.
pub fn report_row(entry_key: &str) -> ImportReportRow {
    ImportReportRow {
        entry_key: entry_key.to_string(),
        code: ReasonCode::Imported,
        message: None,
        detail: ImportReportDetail {
            archive_path: entry_key.to_string(),
            target_key: Some(entry_key.to_string()),
            version_id: None,
            blake3: None,
            size: None,
            arn: None,
            w3id: None,
            validation: None,
        },
    }
}

/// A plain RO-Crate 1.2 metadata document with `parts` and the `extra` graph entities.
pub fn crate_document(parts: Value, extra: Vec<Value>) -> Value {
    let mut graph = vec![
        json!({"@id": "ro-crate-metadata.json", "@type": "CreativeWork",
            "about": {"@id": "./"}, "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"}}),
        json!({"@id": "./", "@type": "Dataset", "name": "Plain", "description": "Plain crate",
            "datePublished": "2026-09-28", "hasPart": parts,
            "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"}}),
    ];
    graph.extend(extra);
    json!({"@context": "https://w3id.org/ro/crate/1.2/context", "@graph": graph})
}

/// An admin document dot of the node with key seed `seed`.
pub fn admin_dot(seed: u8) -> AdminDocumentDot {
    AdminDocumentDot {
        event_id: Ulid::from_bytes([seed; 16]),
        origin_node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
        origin_seq: u64::from(seed),
    }
}

/// A conflict at `path` between the values `value-<first_seed>` and `value-<second_seed>`.
pub fn admin_conflict(path: &str, first_seed: u8, second_seed: u8) -> AdminConflict {
    AdminConflict {
        path: path.to_string(),
        values: vec![
            AdminConflictValue {
                value: Some(format!("value-{first_seed}")),
                dot: admin_dot(first_seed),
            },
            AdminConflictValue {
                value: Some(format!("value-{second_seed}")),
                dot: admin_dot(second_seed),
            },
        ],
    }
}

/// An OIDC provider `id` whose issuer ends in `issuer_suffix`.
pub fn oidc_provider(id: &str, issuer_suffix: &str) -> OidcProviderConfig {
    OidcProviderConfig {
        id: id.to_string(),
        issuer: format!("https://issuer.example/{issuer_suffix}"),
        audience: "aruna".to_string(),
        discovery_url: format!(
            "https://issuer.example/{issuer_suffix}/.well-known/openid-configuration"
        ),
    }
}

/// A public registry record of a new document at `document_path` in a new group of realm 7.
pub fn registry_record(document_path: &str) -> MetadataRegistryRecord {
    let document_id = Ulid::generate();
    MetadataRegistryRecord {
        realm_id: RealmId([7u8; 32]),
        group_id: Ulid::generate(),
        document_id,
        document_path: document_path.to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: format!("/metadata/{document_path}"),
        placement: PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 0,
        updated_at_ms: 0,
        establishing_event_id: Ulid::nil(),
        last_event_id: Ulid::nil(),
    }
}

/// Restores every swapped environment variable on drop, also when the test panics first.
pub struct EnvGuard {
    previous: Vec<(String, Option<String>)>,
}

impl EnvGuard {
    /// Sets each variable with a value and removes each without one.
    pub fn swap(entries: &[(&str, Option<String>)]) -> Self {
        let previous = entries
            .iter()
            .map(|(key, value)| {
                let previous = std::env::var(key).ok();
                match value {
                    Some(value) => unsafe { std::env::set_var(key, value) },
                    None => unsafe { std::env::remove_var(key) },
                }
                ((*key).to_string(), previous)
            })
            .collect();
        Self { previous }
    }

    /// Sets each variable to its value.
    pub fn set(vars: &[(&str, String)]) -> Self {
        let entries: Vec<_> = vars
            .iter()
            .map(|(key, value)| (*key, Some(value.clone())))
            .collect();
        Self::swap(&entries)
    }

    /// Removes each variable.
    pub fn remove(keys: &[&str]) -> Self {
        let entries: Vec<_> = keys.iter().map(|key| (*key, None)).collect();
        Self::swap(&entries)
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, value) in self.previous.drain(..) {
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }
}
