//! Regression coverage for parallel metadata creates in one realm.

use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::structs::{Actor, RealmId};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
    create_metadata_document,
};
use aruna_operations::driver::DriverContext;
use aruna_operations::metadata::MetadataHandle;
use aruna_storage::FjallStorage;
use ulid::Ulid;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const CONCURRENT_CREATES: usize = 8;

fn scaffold(index: usize) -> CreateMetadataDocumentPayload {
    CreateMetadataDocumentPayload::Scaffold {
        name: format!("Concurrent Dataset {index}"),
        description: "Concurrent create regression".to_string(),
        date_published: "2026-07-24".to_string(),
        license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_creates_succeed() -> Result<(), BoxError> {
    // Parallel creates in one realm must not raise transaction conflicts now
    // that the unchanged realm-config write-back is gone.
    let temp_dir = tempfile::tempdir()?;
    let storage = FjallStorage::open(
        temp_dir
            .path()
            .join("fjall")
            .to_str()
            .ok_or("invalid storage path")?,
    )?;
    let node_id = iroh::SecretKey::generate().public();
    let metadata_handle = MetadataHandle::new(
        temp_dir.path().join("metadata"),
        node_id,
        storage.clone(),
        None,
        None,
        None,
    )?;
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: Some(metadata_handle),
        task_handle: None,
        compute_handle: None,
    });

    let realm_id = RealmId([61u8; 32]);
    let group_id = Ulid::generate();
    let mut handles = Vec::with_capacity(CONCURRENT_CREATES);
    for index in 0..CONCURRENT_CREATES {
        let context = context.clone();
        handles.push(tokio::spawn(async move {
            let document_id = Ulid::generate();
            let operation = CreateMetadataDocumentOperation::new_for_generated_document_id(
                CreateMetadataDocumentConfig {
                    actor: Actor {
                        node_id,
                        user_id: UserId::local(Ulid::generate(), realm_id),
                        realm_id,
                    },
                    group_id,
                    document_id,
                    document_path: format!("datasets/concurrent-{index}"),
                    public: true,
                    payload: scaffold(index),
                },
            );
            create_metadata_document(operation, context).await
        }));
    }

    for handle in handles {
        handle
            .await?
            .map_err(|error| format!("concurrent create failed: {error:?}"))?;
    }
    assert_eq!(storage.snapshot_metrics().conflicts_total, 0);
    Ok(())
}
