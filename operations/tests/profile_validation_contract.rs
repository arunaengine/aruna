#![recursion_limit = "256"]

use std::sync::{Arc, LazyLock};

use aruna_core::StructuredId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, METADATA_EVENT_LOG_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataEffect, MetadataError, MetadataEvent, MetadataProfileValidationSeverity,
    MetadataProfileValidationState, PROCESS_RUN_CRATE_PROFILE_IRI,
};
use aruna_core::storage_entries::metadata_event_log_prefix;
use aruna_core::structs::{
    Actor, Group, GroupAuthorizationDocument, RealmAuthorizationDocument, RealmConfigDocument,
    RealmId, RealmNodeKind,
};
use aruna_core::types::UserId;
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
    CreateMetadataDocumentPayload, mint_local_document,
};
use aruna_operations::driver::DriverContext;
use aruna_operations::metadata::MetadataReadError;
use aruna_operations::metadata::forward::{
    MetadataWriteError, admits_profile_peer, create_metadata_document_routed, export_profile_local,
};
use aruna_operations::metadata::profile_validation::{
    current_validation_status, load_validation_status, preview_submission, profile_public_iri,
    revalidate_current,
};
use aruna_operations::metadata::{
    MetadataHandle, MetadataHandleOptions,
    materialization_queue::process_metadata_materialization_batch,
    projector::drain_pending_metadata_projection_queue,
};
use aruna_operations::update_metadata_document::{
    UpdateMetadataDocumentConfig, UpdateMetadataDocumentError, UpdateMetadataDocumentMutation,
    UpdateMetadataDocumentOperation, update_metadata_document,
};
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use serde_json::json;
use tempfile::TempDir;
use ulid::Ulid;

static PROFILE_TEST_LOCK: LazyLock<Arc<tokio::sync::Mutex<()>>> =
    LazyLock::new(|| Arc::new(tokio::sync::Mutex::new(())));

struct TestContext {
    _test_lock: tokio::sync::OwnedMutexGuard<()>,
    _storage_dir: TempDir,
    _metadata_dir: TempDir,
    actor: Actor,
    config: RealmConfigDocument,
    context: Arc<DriverContext>,
}

#[tokio::test]
async fn tagged_create_rejects_atomically_and_can_be_retried()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let (profile_id, profile_revision) = register_profile(&test, group_id, minimum_shape()).await?;
    let document_id = mint(&test, group_id, "datasets/tagged-create")?;
    let tag = profile_public_iri(profile_id);

    let error = create_crate(
        &test,
        group_id,
        document_id,
        "datasets/tagged-create",
        crate_json(document_id, Some(&tag), false, true),
    )
    .await
    .expect_err("missing identifier must be rejected");
    let CreateMetadataDocumentError::MetadataError(MetadataError::ProfileValidation(findings)) =
        error
    else {
        panic!("expected a structured Profile rejection");
    };
    assert!(findings.iter().any(|finding| {
        finding.rule == "http://www.w3.org/ns/shacl#minCount"
            && finding.profile_revision == Some(profile_revision.to_string())
    }));
    assert_eq!(event_count(&test, document_id).await?, 0);

    let created = create_crate(
        &test,
        group_id,
        document_id,
        "datasets/tagged-create",
        crate_json(document_id, Some(&tag), true, true),
    )
    .await?;
    assert_eq!(event_count(&test, document_id).await?, 1);
    let status = load_validation_status(test.context.as_ref(), document_id, None)
        .await?
        .expect("create commits status atomically");
    assert_eq!(status.state, MetadataProfileValidationState::Valid);
    assert_eq!(status.dataset_revision, created.event_id);
    assert_eq!(status.profile_id, Some(profile_id));
    assert_eq!(status.profile_revision, Some(profile_revision.to_string()));

    let removable_id = mint(&test, group_id, "datasets/removable-tag")?;
    create_crate(
        &test,
        group_id,
        removable_id,
        "datasets/removable-tag",
        crate_json(removable_id, Some(&tag), false, true),
    )
    .await
    .expect_err("tagged invalid create must fail");
    create_crate(
        &test,
        group_id,
        removable_id,
        "datasets/removable-tag",
        crate_json(removable_id, None, false, true),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn external_tag_and_unavailable_validator_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let unavailable = build_context(true).await?;
    let group_id = Ulid::generate();
    let (profile_id, _) = register_profile(&unavailable, group_id, minimum_shape()).await?;
    let document_id = mint(&unavailable, group_id, "datasets/unavailable")?;
    let tag = profile_public_iri(profile_id);
    let error = create_crate(
        &unavailable,
        group_id,
        document_id,
        "datasets/unavailable",
        crate_json(document_id, Some(&tag), true, true),
    )
    .await
    .expect_err("tagged write must fail when the evaluator is unavailable");
    assert_profile_code(error, "validator_unavailable");
    assert_eq!(event_count(&unavailable, document_id).await?, 0);
    create_crate(
        &unavailable,
        group_id,
        document_id,
        "datasets/unavailable",
        crate_json(document_id, None, true, true),
    )
    .await?;

    let external_id = mint(&unavailable, group_id, "datasets/external-profile")?;
    let external = create_crate(
        &unavailable,
        group_id,
        external_id,
        "datasets/external-profile",
        crate_json(
            external_id,
            Some("https://example.org/profiles/external"),
            true,
            true,
        ),
    )
    .await
    .expect_err("unregistered external Profile must fail closed");
    assert_profile_code(external, "profile_not_registered");
    assert_eq!(event_count(&unavailable, external_id).await?, 0);
    Ok(())
}

#[tokio::test]
async fn unsupported_registered_constraint_rejects_without_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    // SHACL-SPARQL is outside craqle's Core subset and must fail closed.
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let (profile_id, revision) = register_profile(
        &test,
        group_id,
        r#"
          @prefix sh: <http://www.w3.org/ns/shacl#> .
          @prefix schema: <http://schema.org/> .
          <urn:test:unsupported> a sh:NodeShape ;
            sh:targetClass schema:Dataset ;
            sh:sparql [ sh:select "SELECT $this WHERE { $this a ?type }" ] .
        "#,
    )
    .await?;
    let document_id = mint(&test, group_id, "datasets/unsupported")?;
    let error = create_crate(
        &test,
        group_id,
        document_id,
        "datasets/unsupported",
        crate_json(
            document_id,
            Some(&profile_public_iri(profile_id)),
            true,
            true,
        ),
    )
    .await
    .expect_err("unsupported registered constraint must fail closed");
    let CreateMetadataDocumentError::MetadataError(MetadataError::ProfileValidation(findings)) =
        error
    else {
        panic!("expected structured Profile validation findings");
    };
    assert!(
        findings.iter().any(|finding| {
            finding.code == "unsupported_constraint"
                && finding.rule == "http://www.w3.org/ns/shacl#SPARQLConstraintComponent"
                && finding.profile_revision == Some(revision.to_string())
        }),
        "{findings:#?}"
    );
    assert_eq!(event_count(&test, document_id).await?, 0);
    Ok(())
}

#[tokio::test]
async fn enforces_core_constraints() -> Result<(), Box<dyn std::error::Error>> {
    // sh:minLength and sh:or were unsupported before craqle owned evaluation.
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let (profile_id, revision) = register_profile(
        &test,
        group_id,
        r#"
          @prefix sh: <http://www.w3.org/ns/shacl#> .
          @prefix schema: <http://schema.org/> .
          <urn:test:core> a sh:NodeShape ;
            sh:targetClass schema:Dataset ;
            sh:property [ sh:path schema:identifier ; sh:minLength 64 ] ;
            sh:or (
              [ sh:path schema:creator ; sh:minCount 1 ]
              [ sh:path schema:publisher ; sh:minCount 1 ]
            ) .
        "#,
    )
    .await?;
    let document_id = mint(&test, group_id, "datasets/core")?;
    let error = create_crate(
        &test,
        group_id,
        document_id,
        "datasets/core",
        crate_json(
            document_id,
            Some(&profile_public_iri(profile_id)),
            true,
            true,
        ),
    )
    .await
    .expect_err("a short identifier and a missing alternative must be rejected");
    let CreateMetadataDocumentError::MetadataError(MetadataError::ProfileValidation(findings)) =
        error
    else {
        panic!("expected structured Profile validation findings");
    };
    assert!(
        findings
            .iter()
            .all(|finding| finding.code == "constraint_violation"
                && finding.profile_revision == Some(revision.to_string())),
        "{findings:#?}"
    );
    for rule in [
        "http://www.w3.org/ns/shacl#minLength",
        "http://www.w3.org/ns/shacl#or",
    ] {
        assert!(
            findings.iter().any(|finding| finding.rule == rule),
            "{rule} missing in {findings:#?}"
        );
    }
    assert_eq!(event_count(&test, document_id).await?, 0);
    Ok(())
}

#[tokio::test]
async fn warning_severity_accepts() -> Result<(), Box<dyn std::error::Error>> {
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let (profile_id, _) = register_profile(
        &test,
        group_id,
        r#"
          @prefix sh: <http://www.w3.org/ns/shacl#> .
          @prefix schema: <http://schema.org/> .
          <urn:test:advisory> a sh:NodeShape ;
            sh:targetClass schema:Dataset ;
            sh:property [ sh:path schema:citation ; sh:minCount 1 ; sh:severity sh:Warning ] .
        "#,
    )
    .await?;
    let document_id = mint(&test, group_id, "datasets/advisory")?;
    create_crate(
        &test,
        group_id,
        document_id,
        "datasets/advisory",
        crate_json(
            document_id,
            Some(&profile_public_iri(profile_id)),
            true,
            true,
        ),
    )
    .await?;
    let status = load_validation_status(test.context.as_ref(), document_id, None)
        .await?
        .expect("status is durable");
    assert_eq!(status.state, MetadataProfileValidationState::Valid);
    assert_eq!(status.findings.len(), 1, "{:#?}", status.findings);
    assert_eq!(
        status.findings[0].severity,
        MetadataProfileValidationSeverity::Warning
    );
    Ok(())
}

#[tokio::test]
async fn spans_crate_versions() -> Result<(), Box<dyn std::error::Error>> {
    // A policy is compiled per detected RO-Crate version, not per profile.
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let (profile_id, _) = register_profile(&test, group_id, minimum_shape()).await?;
    let tag = profile_public_iri(profile_id);
    for version in ["1.2", "1.3"] {
        let path = format!("datasets/version-{version}");
        let document_id = mint(&test, group_id, &path)?;
        let invalid = create_crate(
            &test,
            group_id,
            document_id,
            &path,
            versioned_crate(document_id, &tag, version, false),
        )
        .await
        .expect_err("the missing identifier must be rejected for every version");
        assert_profile_code(invalid, "constraint_violation");
        create_crate(
            &test,
            group_id,
            document_id,
            &path,
            versioned_crate(document_id, &tag, version, true),
        )
        .await?;
        let status = load_validation_status(test.context.as_ref(), document_id, None)
            .await?
            .expect("status is durable");
        assert_eq!(status.state, MetadataProfileValidationState::Valid);
    }
    Ok(())
}

#[tokio::test]
async fn preview_stores_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let (profile_id, revision) = register_profile(&test, group_id, minimum_shape()).await?;
    let draft_id = mint(&test, group_id, "datasets/draft")?;
    let tag = profile_public_iri(profile_id);

    let rejected = preview_submission(
        test.context.as_ref(),
        Some(group_id),
        &crate_json(draft_id, Some(&tag), false, true),
    )
    .await?;
    assert!(!rejected.accepted());
    assert_eq!(
        rejected.status.state,
        MetadataProfileValidationState::Invalid
    );
    assert!(rejected.structural_violations.is_empty());
    assert!(
        rejected.status.findings.iter().any(|finding| {
            finding.rule == "http://www.w3.org/ns/shacl#minCount"
                && finding.focus_node.as_deref() == Some("./")
                && finding.profile_revision == Some(revision.to_string())
        }),
        "{:#?}",
        rejected.status.findings
    );

    let accepted = preview_submission(
        test.context.as_ref(),
        Some(group_id),
        &crate_json(draft_id, Some(&tag), true, true),
    )
    .await?;
    assert!(accepted.accepted());
    assert_eq!(accepted.status.state, MetadataProfileValidationState::Valid);

    assert!(!graph_exists(&test, draft_id).await?);
    assert_eq!(event_count(&test, draft_id).await?, 0);
    Ok(())
}

#[tokio::test]
async fn builtin_profile_enforced() -> Result<(), Box<dyn std::error::Error>> {
    // The Process Run Crate profile validates without any realm document.
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let document_id = mint(&test, group_id, "datasets/run")?;

    let rejected = preview_submission(
        test.context.as_ref(),
        Some(group_id),
        &run_crate_json(document_id, false),
    )
    .await?;
    assert!(!rejected.accepted());
    assert_eq!(
        rejected.status.state,
        MetadataProfileValidationState::Invalid
    );
    assert_eq!(rejected.status.profile_id, None);
    assert_eq!(
        rejected.status.profile_iri.as_deref(),
        Some(PROCESS_RUN_CRATE_PROFILE_IRI)
    );
    assert_eq!(rejected.status.profile_revision.as_deref(), Some("builtin"));
    assert!(
        rejected.status.findings.iter().any(|finding| {
            finding.path.as_deref() == Some("http://schema.org/instrument")
                && finding.severity == MetadataProfileValidationSeverity::Violation
        }),
        "{:#?}",
        rejected.status.findings
    );

    let refused = create_crate(
        &test,
        group_id,
        document_id,
        "datasets/run",
        run_crate_json(document_id, false),
    )
    .await
    .expect_err("a run crate without an instrument must be refused");
    assert_profile_code(refused, "constraint_violation");
    assert_eq!(event_count(&test, document_id).await?, 0);

    let accepted = preview_submission(
        test.context.as_ref(),
        Some(group_id),
        &run_crate_json(document_id, true),
    )
    .await?;
    assert!(accepted.accepted(), "{:#?}", accepted.status.findings);
    assert!(
        accepted.status.findings.iter().any(|finding| {
            finding.path.as_deref() == Some("http://schema.org/softwareVersion")
                && finding.severity == MetadataProfileValidationSeverity::Warning
        }),
        "{:#?}",
        accepted.status.findings
    );

    create_crate(
        &test,
        group_id,
        document_id,
        "datasets/run",
        run_crate_json(document_id, true),
    )
    .await?;
    let status = load_validation_status(test.context.as_ref(), document_id, None)
        .await?
        .expect("a built-in verdict is durable");
    assert_eq!(status.state, MetadataProfileValidationState::Valid);
    assert_eq!(status.profile_revision.as_deref(), Some("builtin"));
    Ok(())
}

#[tokio::test]
async fn preview_reports_structural() -> Result<(), Box<dyn std::error::Error>> {
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let draft_id = mint(&test, group_id, "datasets/structural")?;
    let graph_iri = aruna_core::structs::MetadataRegistryRecord::graph_iri_for(draft_id);
    let jsonld = json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                "about": {"@id": graph_iri}
            },
            {"@id": graph_iri, "@type": "Dataset", "name": "No description"}
        ]
    })
    .to_string();
    let preview = preview_submission(test.context.as_ref(), Some(group_id), &jsonld).await?;
    assert!(!preview.accepted());
    assert_eq!(
        preview.status.state,
        MetadataProfileValidationState::NotProfiled
    );
    assert!(
        !preview.structural_violations.is_empty(),
        "an incomplete root data entity must be reported"
    );
    Ok(())
}

#[tokio::test]
async fn create_refuses_structural() -> Result<(), Box<dyn std::error::Error>> {
    // The write must refuse exactly what preview_reports_structural reports.
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let document_id = mint(&test, group_id, "datasets/structural-write")?;
    let graph_iri = aruna_core::structs::MetadataRegistryRecord::graph_iri_for(document_id);
    let jsonld = json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                "about": {"@id": graph_iri}
            },
            {"@id": graph_iri, "@type": "Dataset", "name": "No description"}
        ]
    })
    .to_string();
    let error = create_crate(
        &test,
        group_id,
        document_id,
        "datasets/structural-write",
        jsonld,
    )
    .await
    .expect_err("an incomplete root data entity must be rejected");
    let CreateMetadataDocumentError::MetadataError(MetadataError::Validation(violations)) = error
    else {
        panic!("expected structural violations");
    };
    assert!(!violations.is_empty());
    assert_eq!(event_count(&test, document_id).await?, 0);
    Ok(())
}

#[tokio::test]
async fn binds_untargeted_root() -> Result<(), Box<dyn std::error::Error>> {
    // Portal Profiles leave root shapes untargeted, with or without a crate base.
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    for (suffix, shapes) in [("based", portal_shape(true)), ("bare", portal_shape(false))] {
        let (profile_id, _) = register_profile(&test, group_id, &shapes).await?;
        let tag = profile_public_iri(profile_id);
        let path = format!("datasets/portal-{suffix}");
        let document_id = mint(&test, group_id, &path)?;
        let preview = preview_submission(
            test.context.as_ref(),
            Some(group_id),
            &portal_crate(document_id, &tag, false),
        )
        .await?;
        assert!(!preview.accepted());
        assert!(
            preview.status.findings.iter().any(|finding| {
                finding.rule == "http://www.w3.org/ns/shacl#minCount"
                    && finding.focus_node.as_deref() == Some("./")
            }),
            "{suffix}: {:#?}",
            preview.status.findings
        );
        let complete = preview_submission(
            test.context.as_ref(),
            Some(group_id),
            &portal_crate(document_id, &tag, true),
        )
        .await?;
        assert!(complete.accepted(), "{suffix}: {:#?}", complete.status);
    }
    Ok(())
}

#[tokio::test]
async fn rejects_crate_local() -> Result<(), Box<dyn std::error::Error>> {
    // Craqle stores non-root crate ids in relative form its compiler rejects.
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let (profile_id, _) = register_profile(
        &test,
        group_id,
        r#"
          @prefix sh: <http://www.w3.org/ns/shacl#> .
          @prefix schema: <http://schema.org/> .
          @base <arcp://name,aruna-portal/crate/> .
          <urn:test:local> a sh:NodeShape ;
            sh:property [ sh:path schema:hasPart ; sh:hasValue <#person-1> ] .
        "#,
    )
    .await?;
    let tag = profile_public_iri(profile_id);
    let document_id = mint(&test, group_id, "datasets/crate-local")?;
    let preview = preview_submission(
        test.context.as_ref(),
        Some(group_id),
        &portal_crate(document_id, &tag, true),
    )
    .await?;
    assert!(!preview.accepted());
    assert!(
        preview.status.findings.iter().any(|finding| {
            finding.code == "unsupported_constraint" && finding.rule == "crate_local_reference"
        }),
        "{:#?}",
        preview.status.findings
    );
    Ok(())
}

#[tokio::test]
async fn invalid_replace_preserves_the_last_revision_and_is_retryable()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let (profile_id, _) = register_profile(&test, group_id, minimum_shape()).await?;
    let document_id = mint(&test, group_id, "datasets/replace")?;
    let created = create_crate(
        &test,
        group_id,
        document_id,
        "datasets/replace",
        crate_json(document_id, None, true, true),
    )
    .await?;
    drain_pending_metadata_projection_queue(test.context.as_ref()).await?;
    // A replace plans its batch against the local graph, so it must be applied.
    process_metadata_materialization_batch(test.context.as_ref()).await?;
    let tag = profile_public_iri(profile_id);

    let error = update_metadata_document(
        UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
            actor: test.actor.clone(),
            group_id,
            document_id,
            public: true,
            mutation: UpdateMetadataDocumentMutation::ReplaceRoCrate {
                jsonld: crate_json(document_id, Some(&tag), false, true),
            },
        }),
        test.context.as_ref(),
    )
    .await
    .expect_err("invalid replacement must fail before commit");
    assert!(matches!(
        error,
        UpdateMetadataDocumentError::MetadataError(MetadataError::ProfileValidation(_))
    ));
    assert_eq!(event_count(&test, document_id).await?, 1);
    let status = load_validation_status(test.context.as_ref(), document_id, None)
        .await?
        .expect("original status remains");
    assert_eq!(status.dataset_revision, created.event_id);

    let updated = update_metadata_document(
        UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
            actor: test.actor.clone(),
            group_id,
            document_id,
            public: true,
            mutation: UpdateMetadataDocumentMutation::ReplaceRoCrate {
                jsonld: crate_json(document_id, Some(&tag), true, true),
            },
        }),
        test.context.as_ref(),
    )
    .await?;
    assert_eq!(event_count(&test, document_id).await?, 2);
    let status = load_validation_status(test.context.as_ref(), document_id, None)
        .await?
        .expect("replacement commits status");
    assert_eq!(status.dataset_revision, updated.last_event_id);
    Ok(())
}

#[tokio::test]
async fn rejects_graph_alias() -> Result<(), Box<dyn std::error::Error>> {
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let (profile_id, _) = register_profile(&test, group_id, minimum_shape()).await?;
    let document_id = mint(&test, group_id, "datasets/graph-alias")?;
    let tag = aruna_core::structs::MetadataRegistryRecord::graph_iri_for(profile_id);
    let error = create_crate(
        &test,
        group_id,
        document_id,
        "datasets/graph-alias",
        crate_json(document_id, Some(&tag), true, true),
    )
    .await
    .expect_err("the graph IRI is not a Profile PID");
    assert_profile_code(error, "profile_not_registered");
    Ok(())
}

#[tokio::test]
async fn rejects_private_profile() -> Result<(), Box<dyn std::error::Error>> {
    // The Dataset lives in a second group, so only the foreign-group rule
    // can refuse it.
    let test = build_context(false).await?;
    let owner_group = Ulid::generate();
    let other_group = Ulid::generate();
    let (profile_id, _) = register_profile(&test, owner_group, minimum_shape()).await?;
    seed_group(&test, other_group).await?;
    make_private(&test, owner_group, profile_id).await?;
    let document_id = mint(&test, other_group, "datasets/private-probe")?;

    let error = preview_submission(
        test.context.as_ref(),
        Some(other_group),
        &crate_json(
            document_id,
            Some(&profile_public_iri(profile_id)),
            true,
            true,
        ),
    )
    .await
    .expect_err("a private Profile is not available to preview");
    let MetadataError::ProfileValidation(findings) = error else {
        panic!("expected structured Profile validation findings");
    };
    assert!(
        findings
            .iter()
            .any(|finding| finding.code == "profile_not_registered")
    );
    Ok(())
}

#[tokio::test]
async fn accepts_group_profile() -> Result<(), Box<dyn std::error::Error>> {
    // A group's own Profile validates its Datasets while it is not public.
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let (profile_id, _) = register_profile(&test, group_id, minimum_shape()).await?;
    let revision = make_private(&test, group_id, profile_id).await?;
    let document_id = mint(&test, group_id, "datasets/group-profile")?;

    let created = create_crate(
        &test,
        group_id,
        document_id,
        "datasets/group-profile",
        crate_json(
            document_id,
            Some(&profile_public_iri(profile_id)),
            true,
            true,
        ),
    )
    .await?;
    let status = load_validation_status(test.context.as_ref(), document_id, None)
        .await?
        .expect("an accepted create commits its status");
    assert_eq!(status.state, MetadataProfileValidationState::Valid);
    assert_eq!(status.profile_id, Some(profile_id));
    assert_eq!(status.profile_revision, Some(revision.to_string()));
    assert_eq!(status.dataset_revision, created.event_id);
    Ok(())
}

#[tokio::test]
async fn rejects_foreign_profile() -> Result<(), Box<dyn std::error::Error>> {
    // A private Profile of another group answers exactly as an unknown one.
    let test = build_context(false).await?;
    let owner_group = Ulid::generate();
    let other_group = Ulid::generate();
    let (profile_id, _) = register_profile(&test, owner_group, minimum_shape()).await?;
    seed_group(&test, other_group).await?;
    make_private(&test, owner_group, profile_id).await?;
    let document_id = mint(&test, other_group, "datasets/foreign-profile")?;

    let error = create_crate(
        &test,
        other_group,
        document_id,
        "datasets/foreign-profile",
        crate_json(
            document_id,
            Some(&profile_public_iri(profile_id)),
            true,
            true,
        ),
    )
    .await
    .expect_err("a foreign group's private Profile must not resolve");
    assert_profile_code(error, "profile_not_registered");
    assert_eq!(event_count(&test, document_id).await?, 0);
    Ok(())
}

#[tokio::test]
async fn public_crosses_groups() -> Result<(), Box<dyn std::error::Error>> {
    // Publishing a Profile keeps it usable by every group's Datasets.
    let test = build_context(false).await?;
    let owner_group = Ulid::generate();
    let other_group = Ulid::generate();
    let (profile_id, revision) = register_profile(&test, owner_group, minimum_shape()).await?;
    seed_group(&test, other_group).await?;
    let document_id = mint(&test, other_group, "datasets/public-profile")?;

    create_crate(
        &test,
        other_group,
        document_id,
        "datasets/public-profile",
        crate_json(
            document_id,
            Some(&profile_public_iri(profile_id)),
            true,
            true,
        ),
    )
    .await?;
    let status = load_validation_status(test.context.as_ref(), document_id, None)
        .await?
        .expect("an accepted create commits its status");
    assert_eq!(status.state, MetadataProfileValidationState::Valid);
    assert_eq!(status.profile_revision, Some(revision.to_string()));
    Ok(())
}

#[tokio::test]
async fn preview_needs_group() -> Result<(), Box<dyn std::error::Error>> {
    // Without a group the preview resolves public Profiles only.
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let (profile_id, _) = register_profile(&test, group_id, minimum_shape()).await?;
    make_private(&test, group_id, profile_id).await?;
    let draft_id = mint(&test, group_id, "datasets/preview-group")?;
    let jsonld = crate_json(draft_id, Some(&profile_public_iri(profile_id)), true, true);

    let error = preview_submission(test.context.as_ref(), None, &jsonld)
        .await
        .expect_err("a group Profile does not resolve without its group");
    let MetadataError::ProfileValidation(findings) = error else {
        panic!("expected structured Profile validation findings");
    };
    assert!(
        findings
            .iter()
            .any(|finding| finding.code == "profile_not_registered")
    );

    let preview = preview_submission(test.context.as_ref(), Some(group_id), &jsonld).await?;
    assert!(preview.accepted(), "{:#?}", preview.status);
    Ok(())
}

#[tokio::test]
async fn render_resolves_group() -> Result<(), Box<dyn std::error::Error>> {
    // Materialization has no caller at all, so the merged render reaches the
    // group's own Profile through the validation channel.
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let (profile_id, _) = register_profile(&test, group_id, minimum_shape()).await?;
    make_private(&test, group_id, profile_id).await?;
    let document_id = mint(&test, group_id, "datasets/render-group")?;
    create_crate(
        &test,
        group_id,
        document_id,
        "datasets/render-group",
        crate_json(
            document_id,
            Some(&profile_public_iri(profile_id)),
            true,
            true,
        ),
    )
    .await?;
    drain_pending_metadata_projection_queue(test.context.as_ref()).await?;
    process_metadata_materialization_batch(test.context.as_ref()).await?;

    let status = load_validation_status(test.context.as_ref(), document_id, None)
        .await?
        .expect("materialization writes a status");
    assert_eq!(status.state, MetadataProfileValidationState::Valid);
    assert_eq!(status.profile_id, Some(profile_id));
    Ok(())
}

#[tokio::test]
async fn channel_refuses_datasets() -> Result<(), Box<dyn std::error::Error>> {
    // Serving `profiles/` and nothing else is the whole authorization of a
    // fetch that carries no caller.
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    seed_group(&test, group_id).await?;
    let document_id = mint(&test, group_id, "datasets/not-a-profile")?;
    let created = create_crate(
        &test,
        group_id,
        document_id,
        "datasets/not-a-profile",
        crate_json(document_id, None, true, true),
    )
    .await?;

    let refusal = export_profile_local(
        test.context.as_ref(),
        test.actor.realm_id,
        document_id,
        created.record.last_event_id,
    )
    .await
    .expect_err("a Dataset is never served on the validation channel");
    assert_eq!(refusal, MetadataReadError::NotFound);
    Ok(())
}

#[test]
fn channel_refuses_devices() {
    // A device may vouch for its owner only, so it never joins a channel that
    // asserts no user.
    let realm_id = RealmId([53u8; 32]);
    let server = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
    let device = iroh::SecretKey::from_bytes(&[12u8; 32]).public();
    let stranger = iroh::SecretKey::from_bytes(&[13u8; 32]).public();
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(server, RealmNodeKind::Server);
    config.ensure_node(
        device,
        RealmNodeKind::User {
            owner: UserId::local(Ulid::generate(), realm_id),
        },
    );

    assert!(admits_profile_peer(&config, server, realm_id));
    assert!(!admits_profile_peer(&config, device, realm_id));
    assert!(!admits_profile_peer(&config, stranger, realm_id));
    assert!(!admits_profile_peer(&config, server, RealmId([54u8; 32])));
}

#[tokio::test]
async fn channel_fences_revision() -> Result<(), Box<dyn std::error::Error>> {
    // A holder answers the exact revision the requester read, or nothing.
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let (profile_id, revision) = register_profile(&test, group_id, minimum_shape()).await?;

    export_profile_local(
        test.context.as_ref(),
        test.actor.realm_id,
        profile_id,
        revision,
    )
    .await
    .expect("the requested revision is served");
    let refusal = export_profile_local(
        test.context.as_ref(),
        test.actor.realm_id,
        profile_id,
        Ulid::generate(),
    )
    .await
    .expect_err("a mismatched revision is refused");
    assert_eq!(refusal, MetadataReadError::Unavailable);
    Ok(())
}

#[tokio::test]
async fn cache_skips_refetch() -> Result<(), Box<dyn std::error::Error>> {
    // One fetch serves every validation of the same Profile revision.
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let (profile_id, _) = register_profile(&test, group_id, minimum_shape()).await?;
    let draft_id = mint(&test, group_id, "datasets/cache")?;
    let jsonld = crate_json(draft_id, Some(&profile_public_iri(profile_id)), true, true);
    let metadata = test
        .context
        .metadata_handle
        .as_ref()
        .ok_or("metadata handle is configured")?;

    let before = metadata.profile_loads();
    preview_submission(test.context.as_ref(), Some(group_id), &jsonld).await?;
    let after = metadata.profile_loads();
    preview_submission(test.context.as_ref(), Some(group_id), &jsonld).await?;

    assert_eq!(after, before + 1);
    assert_eq!(metadata.profile_loads(), after);
    Ok(())
}

async fn make_private(
    test: &TestContext,
    group_id: Ulid,
    profile_id: Ulid,
) -> Result<Ulid, Box<dyn std::error::Error>> {
    let updated = update_metadata_document(
        UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
            actor: test.actor.clone(),
            group_id,
            document_id: profile_id,
            public: false,
            mutation: UpdateMetadataDocumentMutation::ReplaceRoCrate {
                jsonld: profile_json(profile_id, minimum_shape()),
            },
        }),
        test.context.as_ref(),
    )
    .await?;
    Ok(updated.last_event_id)
}

#[tokio::test]
async fn profile_revision_change_invalidates_and_revalidation_repins()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let (profile_id, original_revision) =
        register_profile(&test, group_id, minimum_shape()).await?;
    let document_id = mint(&test, group_id, "datasets/revalidate")?;
    let created = create_crate(
        &test,
        group_id,
        document_id,
        "datasets/revalidate",
        crate_json(
            document_id,
            Some(&profile_public_iri(profile_id)),
            true,
            true,
        ),
    )
    .await?;
    drain_pending_metadata_projection_queue(test.context.as_ref()).await?;

    let updated_profile = update_metadata_document(
        UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
            actor: test.actor.clone(),
            group_id,
            document_id: profile_id,
            public: true,
            mutation: UpdateMetadataDocumentMutation::ReplaceRoCrate {
                jsonld: profile_json(profile_id, minimum_shape()),
            },
        }),
        test.context.as_ref(),
    )
    .await?;
    assert_ne!(updated_profile.last_event_id, original_revision);

    let stale = current_validation_status(test.context.as_ref(), &created.record).await?;
    assert_eq!(stale.state, MetadataProfileValidationState::Stale);
    assert_eq!(
        stale.stale_reason.as_deref(),
        Some("profile_revision_changed")
    );
    assert_eq!(stale.profile_revision, Some(original_revision.to_string()));

    let refreshed = revalidate_current(test.context.as_ref(), &created.record).await?;
    assert_eq!(refreshed.state, MetadataProfileValidationState::Valid);
    assert_eq!(
        refreshed.profile_revision,
        Some(updated_profile.last_event_id.to_string())
    );
    assert_eq!(refreshed.dataset_revision, created.event_id);
    Ok(())
}

fn assert_profile_code(error: CreateMetadataDocumentError, code: &str) {
    let CreateMetadataDocumentError::MetadataError(MetadataError::ProfileValidation(findings)) =
        error
    else {
        panic!("expected structured Profile validation findings");
    };
    assert!(findings.iter().any(|finding| finding.code == code));
}

fn portal_shape(with_base: bool) -> String {
    let base = if with_base {
        "@base <arcp://name,aruna-portal/crate/> ."
    } else {
        ""
    };
    format!(
        r#"
          @prefix sh: <http://www.w3.org/ns/shacl#> .
          @prefix schema: <http://schema.org/> .
          {base}
          <urn:test:portal-root> a sh:NodeShape ;
            sh:property [ sh:path schema:identifier ; sh:minCount 1 ] .
        "#
    )
}

fn portal_crate(document_id: Ulid, tag: &str, complete: bool) -> String {
    let graph_iri = aruna_core::structs::MetadataRegistryRecord::graph_iri_for(document_id);
    let mut root = json!({
        "@id": graph_iri,
        "@type": "Dataset",
        "name": "Portal Dataset",
        "description": "Portal profile fixture",
        "datePublished": "2026-08-22",
        "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"},
        "conformsTo": {"@id": tag}
    });
    let mut entries = vec![json!({
        "@id": "ro-crate-metadata.json",
        "@type": "CreativeWork",
        "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
        "about": {"@id": graph_iri}
    })];
    if complete {
        root["identifier"] = json!("portal-identifier");
        root["hasPart"] = json!({"@id": "#person-1"});
        entries.push(root);
        entries.push(json!({"@id": "#person-1", "@type": "Person", "name": "Ada"}));
    } else {
        entries.push(root);
    }
    json!({"@context": "https://w3id.org/ro/crate/1.2/context", "@graph": entries}).to_string()
}

fn versioned_crate(document_id: Ulid, tag: &str, version: &str, valid: bool) -> String {
    let graph_iri = aruna_core::structs::MetadataRegistryRecord::graph_iri_for(document_id);
    let specification = format!("https://w3id.org/ro/crate/{version}");
    let mut root = json!({
        "@id": graph_iri,
        "@type": "Dataset",
        "name": "Versioned Dataset",
        "description": "Profile validation contract fixture",
        "datePublished": "2026-08-22",
        "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"},
        "conformsTo": {"@id": tag}
    });
    if valid {
        root["identifier"] = json!("dataset-identifier");
    }
    json!({
        "@context": format!("{specification}/context"),
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": {"@id": specification},
                "about": {"@id": graph_iri}
            },
            root
        ]
    })
    .to_string()
}

async fn graph_exists(
    test: &TestContext,
    document_id: Ulid,
) -> Result<bool, Box<dyn std::error::Error>> {
    let metadata = test
        .context
        .metadata_handle
        .as_ref()
        .ok_or("metadata handle is configured")?;
    let graph_iri = aruna_core::structs::MetadataRegistryRecord::graph_iri_for(document_id);
    match metadata
        .send_metadata_effect(MetadataEffect::ContainsGraph { graph_iri })
        .await
    {
        Event::Metadata(MetadataEvent::ContainsGraphResult { exists, .. }) => Ok(exists),
        other => Err(format!("unexpected graph existence result: {other:?}").into()),
    }
}

async fn register_profile(
    test: &TestContext,
    group_id: Ulid,
    shapes: &str,
) -> Result<(Ulid, Ulid), Box<dyn std::error::Error>> {
    seed_group(test, group_id).await?;
    let path = format!("profiles/{}", Ulid::generate());
    let profile_id = mint(test, group_id, &path)?;
    let created = create_crate(
        test,
        group_id,
        profile_id,
        &path,
        profile_json(profile_id, shapes),
    )
    .await?;
    drain_pending_metadata_projection_queue(test.context.as_ref()).await?;
    process_metadata_materialization_batch(test.context.as_ref()).await?;
    Ok((profile_id, created.event_id))
}

async fn seed_group(test: &TestContext, group_id: Ulid) -> Result<(), Box<dyn std::error::Error>> {
    let realm = RealmAuthorizationDocument::new_default_realm_doc(test.actor.realm_id);
    let auth = GroupAuthorizationDocument::new_default_group_doc(
        test.actor.user_id,
        test.actor.realm_id,
        group_id,
    );
    let group = Group {
        display_name: "profiles".to_string(),
        group_id,
        realm_id: test.actor.realm_id,
        roles: auth.roles.keys().copied().collect(),
        owner: test.actor.user_id,
    };
    let writes = vec![
        (
            AUTH_KEYSPACE.to_string(),
            test.actor.realm_id.as_bytes().to_vec().into(),
            realm.to_bytes(&test.actor)?.into(),
        ),
        (
            AUTH_KEYSPACE.to_string(),
            group_id.to_bytes().to_vec().into(),
            auth.to_bytes(&test.actor)?.into(),
        ),
        (
            GROUP_KEYSPACE.to_string(),
            group_id.to_bytes().to_vec().into(),
            group.to_bytes(&test.actor)?.into(),
        ),
    ];
    match test
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        other => Err(format!("unexpected authorization seed: {other:?}").into()),
    }
}

fn profile_json(profile_id: Ulid, shapes: &str) -> String {
    let graph_iri = aruna_core::structs::MetadataRegistryRecord::graph_iri_for(profile_id);
    json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                "about": {"@id": graph_iri}
            },
            {
                "@id": graph_iri,
                "@type": ["Dataset", "http://www.w3.org/ns/dx/prof/Profile"],
                "name": "Test Profile",
                "description": "Profile validation contract fixture",
                "datePublished": "2026-08-19",
                "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"},
                "hasPart": {"@id": "#shapes"}
            },
            {
                "@id": "#shapes",
                "@type": "File",
                "name": "Profile shapes",
                "encodingFormat": "text/turtle",
                "text": shapes
            }
        ]
    })
    .to_string()
}

fn minimum_shape() -> &'static str {
    r#"
      @prefix sh: <http://www.w3.org/ns/shacl#> .
      @prefix schema: <http://schema.org/> .
      <urn:test:dataset> a sh:NodeShape ;
        sh:targetClass schema:Dataset ;
        sh:property [ sh:path schema:identifier ; sh:minCount 1 ] .
    "#
}

fn crate_json(document_id: Ulid, tag: Option<&str>, valid: bool, extra: bool) -> String {
    let graph_iri = aruna_core::structs::MetadataRegistryRecord::graph_iri_for(document_id);
    let mut root = json!({
        "@id": graph_iri,
        "@type": "Dataset",
        "name": "Profile-gated Dataset",
        "description": "Profile validation contract fixture",
        "datePublished": "2026-08-19",
        "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"}
    });
    if let Some(tag) = tag {
        root["conformsTo"] = json!({"@id": tag});
    }
    if valid {
        root["identifier"] = json!("dataset-identifier");
    }
    if extra {
        root["keywords"] = json!(["open", "world"]);
    }
    json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                "about": {"@id": graph_iri}
            },
            root
        ]
    })
    .to_string()
}

/// A Process Run Crate the built-in Profile is meant to accept. `complete`
/// adds the instrument the profile requires.
fn run_crate_json(document_id: Ulid, complete: bool) -> String {
    let graph_iri = aruna_core::structs::MetadataRegistryRecord::graph_iri_for(document_id);
    let mut action = json!({
        "@id": "#run-1",
        "@type": "CreateAction",
        "name": "Variant calling",
        "description": "/usr/bin/tool --in in.txt",
        "startTime": "2026-08-19T09:00:00+00:00",
        "endTime": "2026-08-19T09:05:00+00:00",
        "agent": {"@id": "#agent-1"},
        "result": [{"@id": "s3://runs/out.txt"}],
        "actionStatus": {"@id": "http://schema.org/CompletedActionStatus"}
    });
    if complete {
        action["instrument"] = json!({"@id": "#software-1"});
    }
    json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                "about": {"@id": graph_iri}
            },
            {
                "@id": graph_iri,
                "@type": "Dataset",
                "name": "Variant calling",
                "description": "Call variants for sample one",
                "datePublished": "2026-08-19",
                "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"},
                "hasPart": [{"@id": "s3://runs/out.txt"}],
                "mentions": {"@id": "#run-1"},
                "conformsTo": {"@id": PROCESS_RUN_CRATE_PROFILE_IRI}
            },
            action,
            {"@id": "#agent-1", "@type": "Person", "name": "Ada"},
            {
                "@id": "#software-1",
                "@type": "SoftwareApplication",
                "name": "tool",
                "url": "https://example.test/tool"
            },
            {"@id": "s3://runs/out.txt", "@type": "File", "name": "out.txt"}
        ]
    })
    .to_string()
}

async fn create_crate(
    test: &TestContext,
    group_id: Ulid,
    document_id: Ulid,
    path: &str,
    jsonld: String,
) -> Result<
    aruna_operations::create_metadata_document::CreateMetadataDocumentResult,
    CreateMetadataDocumentError,
> {
    match create_metadata_document_routed(
        CreateMetadataDocumentOperation::new_for_generated_document_id(
            CreateMetadataDocumentConfig {
                actor: test.actor.clone(),
                group_id,
                document_id,
                document_path: path.to_string(),
                public: true,
                payload: CreateMetadataDocumentPayload::RoCrate { jsonld },
            },
        ),
        test.context.clone(),
        None,
    )
    .await
    {
        Ok(created) => Ok(created),
        Err(MetadataWriteError::Create(error)) => Err(error),
        Err(error) => Err(CreateMetadataDocumentError::MetadataError(
            MetadataError::Backend(error.to_string()),
        )),
    }
}

fn mint(
    test: &TestContext,
    group_id: Ulid,
    path: &str,
) -> Result<Ulid, CreateMetadataDocumentError> {
    Ok(mint_local_document(&test.config, &test.actor, group_id, path)?.as_ulid())
}

async fn event_count(
    test: &TestContext,
    document_id: Ulid,
) -> Result<usize, Box<dyn std::error::Error>> {
    match test
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: METADATA_EVENT_LOG_KEYSPACE.to_string(),
            prefix: Some(metadata_event_log_prefix(document_id)),
            start: None,
            limit: 10,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values.len()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(format!("unexpected event-log read: {other:?}").into()),
    }
}

async fn build_context(
    validator_disabled: bool,
) -> Result<TestContext, Box<dyn std::error::Error>> {
    let test_lock = PROFILE_TEST_LOCK.clone().lock_owned().await;
    let storage_dir = tempfile::tempdir()?;
    let metadata_dir = tempfile::tempdir()?;
    let storage = FjallStorage::open(storage_dir.path().to_str().ok_or("invalid storage path")?)?;
    let realm_id = RealmId([77u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[77u8; 32]).public();
    let metadata = MetadataHandle::new_with_options(
        metadata_dir.path(),
        node_id,
        storage.clone(),
        None,
        None,
        None,
        MetadataHandleOptions::default().with_profile_validation_disabled(validator_disabled),
    )?;
    let actor = Actor {
        node_id,
        user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
        realm_id,
    };
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.seed_default_placement();
    config.ensure_node(node_id, RealmNodeKind::Server);
    config.seed_job_control(node_id, 0);
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(*realm_id.as_bytes()),
            value: ByteView::from(config.to_bytes(&actor)?),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => return Err(format!("unexpected realm config write: {other:?}").into()),
    }
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: Some(metadata),
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    });
    Ok(TestContext {
        _test_lock: test_lock,
        _storage_dir: storage_dir,
        _metadata_dir: metadata_dir,
        actor,
        config,
        context,
    })
}
