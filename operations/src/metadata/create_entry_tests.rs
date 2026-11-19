//! Drives the create entry through its mint, Profile verdict and create children without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::metadata::{
    ProfileValidationFinding, ProfileValidationSeverity, ProfileValidationState,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

use crate::metadata::create_document::CreateDocumentConfig;

fn actor() -> Actor {
    let realm_id = RealmId::from_bytes([3; 32]);
    Actor {
        node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
        user_id: aruna_core::UserId::local(Ulid::from_parts(1, 1), realm_id),
        realm_id,
    }
}

fn config(document_id: Ulid, payload: CreateDocumentPayload) -> CreateDocumentConfig {
    CreateDocumentConfig {
        actor: actor(),
        group_id: Ulid::from_parts(2, 2),
        document_id,
        document_path: "datasets/entry".to_string(),
        public: false,
        payload,
    }
}

fn scaffold() -> CreateDocumentPayload {
    CreateDocumentPayload::Scaffold {
        name: "Entry".to_string(),
        description: "Created through the entry".to_string(),
        date_published: "2026-01-01".to_string(),
        license: None,
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn created(document_id: Ulid) -> CreateDocumentResult {
    CreateDocumentResult {
        record: MetadataRegistryRecord {
            realm_id: actor().realm_id,
            group_id: Ulid::from_parts(2, 2),
            document_id,
            document_path: "datasets/entry".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: String::new(),
            placement: aruna_core::structs::placement::record::PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::from_parts(5, 5),
            last_event_id: Ulid::from_parts(5, 5),
        },
        event_id: Ulid::from_parts(5, 5),
    }
}

#[test]
fn nil_id_mints() {
    let template = CreateDocumentOperation::new(config(Ulid::nil(), scaffold()));
    let mut entry = CreateEntryOperation::new(template, None);
    assert!(matches!(entry.first(), Ok(Next::Call(IoCall::Child(_)))));
    let refused = child(Err::<MetaResourceId, _>(CreateDocumentError::HoldsNoBucket));
    assert_eq!(entry.next(refused), Err(CreateDocumentError::HoldsNoBucket));
}

#[test]
fn scaffold_create_wakes() {
    // A supplied id persists and a scaffold carries no Profile, so the create runs at once.
    let document_id = Ulid::from_parts(9, 9);
    let template = CreateDocumentOperation::new(config(document_id, scaffold()));
    let mut entry = CreateEntryOperation::new(template, None);
    assert!(matches!(entry.first(), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(entry.stage, EntryStage::Create);
    let done = child(Ok::<_, CreateDocumentError>(created(document_id)));
    assert_eq!(
        entry.next(done),
        Ok(Next::Then {
            output: created(document_id),
            calls: vec![
                IoCall::Effect(Box::new(schedule_projection(Duration::ZERO))),
                IoCall::Effect(Box::new(schedule_materialization())),
            ],
        })
    );
}

#[test]
fn profile_verdict_refuses() {
    let document_id = Ulid::from_parts(9, 9);
    let payload = CreateDocumentPayload::RoCrate {
        jsonld: "{}".to_string(),
    };
    let template = CreateDocumentOperation::new(config(document_id, payload));
    let mut entry = CreateEntryOperation::new(template, None);
    assert!(matches!(entry.first(), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(entry.stage, EntryStage::Validate);
    let mut status = not_profiled_status(document_id);
    status.state = ProfileValidationState::Invalid;
    status.findings = vec![ProfileValidationFinding {
        code: "constraint_violation".to_string(),
        severity: ProfileValidationSeverity::Violation,
        focus_node: None,
        path: None,
        rule: "sh:minCount".to_string(),
        message: "missing".to_string(),
        profile_revision: None,
        completeness: aruna_core::metadata::ProfileValidationCompleteness::Complete,
    }];
    let preview = MetadataProfilePreview {
        status,
        structural_violations: Vec::new(),
    };
    let refused = entry.next(child(Ok::<_, MetadataError>(preview)));
    assert!(matches!(
        refused,
        Err(CreateDocumentError::MetadataError(
            MetadataError::ProfileValidation(_)
        ))
    ));
}

#[test]
fn job_mint_blind() {
    let mut mint = DocumentMintOperation::job(actor(), Ulid::from_parts(2, 2), "jobs/out".into());
    assert!(mint.forward_blind);
    assert!(matches!(mint.first(), Ok(Next::Call(IoCall::Child(_)))));
    let missing = child(Err::<RealmConfigDocument, _>(
        GetConfigError::DocumentNotFound,
    ));
    assert!(matches!(
        mint.next(missing),
        Err(CreateDocumentError::PlacementBindingUnavailable(_))
    ));
}
