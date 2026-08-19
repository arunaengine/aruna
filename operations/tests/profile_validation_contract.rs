#![recursion_limit = "256"]

use std::sync::Arc;

use aruna_core::StructuredId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{METADATA_EVENT_LOG_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::metadata::{MetadataError, MetadataProfileValidationState};
use aruna_core::storage_entries::metadata_event_log_prefix;
use aruna_core::structs::{
    Actor, DocumentClass, HandleRange, PlacementBinding, PlacementScope, RealmConfigDocument,
    RealmId, RealmNodeKind, band_start,
};
use aruna_core::structured_id::PlacementHandle;
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
    CreateMetadataDocumentPayload, mint_local_document,
};
use aruna_operations::driver::DriverContext;
use aruna_operations::metadata::forward::{MetadataWriteError, create_metadata_document_routed};
use aruna_operations::metadata::profile_validation::{
    current_validation_status, load_validation_status, profile_public_iri, revalidate_current,
};
use aruna_operations::metadata::{
    MetadataHandle, MetadataHandleOptions, projector::drain_pending_metadata_projection_queue,
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

struct TestContext {
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
            && finding.profile_revision == Some(profile_revision)
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
    assert_eq!(status.profile_revision, Some(profile_revision));

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
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let (profile_id, revision) = register_profile(
        &test,
        group_id,
        r#"
          @prefix sh: <http://www.w3.org/ns/shacl#> .
          <urn:test:unsupported> a sh:NodeShape ; sh:minLength 2 .
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
    assert!(findings.iter().any(|finding| {
        finding.code == "unsupported_constraint"
            && finding.rule == "http://www.w3.org/ns/shacl#minLength"
            && finding.profile_revision == Some(revision)
    }));
    assert_eq!(event_count(&test, document_id).await?, 0);
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
async fn public_and_legacy_profile_iris_pin_the_same_revision()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context(false).await?;
    let group_id = Ulid::generate();
    let (profile_id, revision) = register_profile(&test, group_id, minimum_shape()).await?;
    for (suffix, tag) in [
        ("public", profile_public_iri(profile_id)),
        (
            "legacy",
            aruna_core::structs::MetadataRegistryRecord::graph_iri_for(profile_id),
        ),
    ] {
        let path = format!("datasets/{suffix}");
        let document_id = mint(&test, group_id, &path)?;
        create_crate(
            &test,
            group_id,
            document_id,
            &path,
            crate_json(document_id, Some(&tag), true, true),
        )
        .await?;
        let status = load_validation_status(test.context.as_ref(), document_id, None)
            .await?
            .expect("status is durable");
        assert_eq!(status.profile_id, Some(profile_id));
        assert_eq!(status.profile_revision, Some(revision));
    }
    Ok(())
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
    assert_eq!(stale.profile_revision, Some(original_revision));

    let refreshed = revalidate_current(test.context.as_ref(), &created.record).await?;
    assert_eq!(refreshed.state, MetadataProfileValidationState::Valid);
    assert_eq!(
        refreshed.profile_revision,
        Some(updated_profile.last_event_id)
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

async fn register_profile(
    test: &TestContext,
    group_id: Ulid,
    shapes: &str,
) -> Result<(Ulid, Ulid), Box<dyn std::error::Error>> {
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
    Ok((profile_id, created.event_id))
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
    let range = HandleRange {
        range_id: Ulid::from_bytes([1; 16]),
        owner: node_id,
        start: band_start(0),
        end: band_start(1),
    };
    config.placement_handle_ranges.push(range);
    config.placement_bindings.push(PlacementBinding {
        handle: PlacementHandle::new(range.start).expect("band start is a valid handle"),
        scope: PlacementScope::Realm(realm_id),
        document_class: DocumentClass::JobControl,
        strategy_id: config
            .default_strategy_id
            .expect("seeded config has a default strategy"),
        allocator_range_id: Some(range.range_id),
        allocated_by: Some(node_id),
        allocated_at_ms: Some(1),
    });
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
        _storage_dir: storage_dir,
        _metadata_dir: metadata_dir,
        actor,
        config,
        context,
    })
}
