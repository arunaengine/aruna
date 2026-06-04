use std::sync::Arc;

use aruna_core::structs::{Actor, RealmId};
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
};
use aruna_operations::delete_metadata_document::DeleteMetadataDocumentOperation;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_metadata_document::{
    GetMetadataDocumentError, GetMetadataDocumentOperation,
};
use aruna_operations::list_metadata_documents::ListMetadataDocumentsOperation;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::update_metadata_document::{
    UpdateMetadataDocumentConfig, UpdateMetadataDocumentMutation, UpdateMetadataDocumentOperation,
};
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use ulid::Ulid;

struct TestContext {
    _storage_dir: TempDir,
    _metadata_dir: TempDir,
    actor: Actor,
    context: Arc<DriverContext>,
}

#[tokio::test]
async fn metadata_crud_roundtrip_uses_craqle_backend() -> Result<(), Box<dyn std::error::Error>> {
    let test = build_context().await?;
    let group_id = Ulid::new();
    let document_id = Ulid::new();

    let created = drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: test.actor.clone(),
            group_id,
            document_id,
            document_path: "datasets/public-dataset".to_string(),
            public: false,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "Initial Dataset".to_string(),
                description: "Created through Craqle".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
        }),
        test.context.as_ref(),
    )
    .await?;

    assert_eq!(created.document_id, document_id);
    assert_eq!(
        created.graph_iri,
        format!("https://w3id.org/aruna/{document_id}")
    );
    assert_eq!(created.document_path, "datasets/public-dataset");
    assert_eq!(created.holder_node_ids, vec![test.actor.node_id]);

    let listed = drive(
        ListMetadataDocumentsOperation::new(group_id),
        test.context.as_ref(),
    )
    .await?;
    assert_eq!(listed, vec![created.clone()]);

    let fetched = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        test.context.as_ref(),
    )
    .await?;
    assert_eq!(fetched.record, created);
    assert!(fetched.jsonld.contains("Initial Dataset"));
    assert!(
        fetched
            .jsonld
            .contains(&format!("https://w3id.org/aruna/{document_id}"))
    );

    let updated_jsonld = format!(
        r#"{{
  "@context": "https://w3id.org/ro/crate/1.2/context",
  "@graph": [
    {{
      "@id": "ro-crate-metadata.json",
      "@type": "CreativeWork",
      "conformsTo": {{"@id": "https://w3id.org/ro/crate/1.2"}},
      "about": {{"@id": "https://w3id.org/aruna/{document_id}"}}
    }},
    {{
      "@id": "https://w3id.org/aruna/{document_id}",
      "@type": "Dataset",
      "name": "Updated Dataset",
      "description": "Updated through Craqle",
      "datePublished": "2026-02-01",
      "license": {{"@id": "https://creativecommons.org/licenses/by/4.0/"}}
    }}
  ]
}}"#
    );

    let updated = drive(
        UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
            actor: test.actor.clone(),
            group_id,
            document_id,
            public: true,
            mutation: UpdateMetadataDocumentMutation::ReplaceRoCrate {
                jsonld: updated_jsonld,
            },
        }),
        test.context.as_ref(),
    )
    .await?;
    assert!(updated.public);

    let fetched_after_update = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        test.context.as_ref(),
    )
    .await?;
    assert!(fetched_after_update.record.public);
    assert!(fetched_after_update.jsonld.contains("Updated Dataset"));
    assert!(
        fetched_after_update
            .jsonld
            .contains("Updated through Craqle")
    );

    drive(
        DeleteMetadataDocumentOperation::new(test.actor.clone(), group_id, document_id),
        test.context.as_ref(),
    )
    .await?;

    let deleted = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        test.context.as_ref(),
    )
    .await;
    assert!(matches!(
        deleted,
        Err(GetMetadataDocumentError::DocumentNotFound)
    ));

    if let Some(net_handle) = &test.context.net_handle {
        net_handle.shutdown().await;
    }

    Ok(())
}

async fn build_context() -> Result<TestContext, Box<dyn std::error::Error>> {
    let storage_dir = tempfile::tempdir()?;
    let metadata_dir = tempfile::tempdir()?;
    let storage_handle =
        FjallStorage::open(storage_dir.path().to_str().ok_or("invalid storage path")?)?;
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone()).await?;
    let node_id = net_handle.node_id();
    let metadata_handle = MetadataHandle::new(
        metadata_dir.path(),
        node_id,
        storage_handle.clone(),
        None,
        None,
    )?;
    let actor = Actor {
        node_id,
        user_id: aruna_core::UserId::local(Ulid::new(), RealmId([5u8; 32])),
        realm_id: RealmId([5u8; 32]),
    };
    let context = Arc::new(DriverContext {
        storage_handle,
        net_handle: Some(net_handle),
        blob_handle: None,
        metadata_handle: Some(metadata_handle),
        task_handle: Some(TaskHandle::new()),
    });
    Ok(TestContext {
        _storage_dir: storage_dir,
        _metadata_dir: metadata_dir,
        actor,
        context,
    })
}
