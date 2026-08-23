use std::collections::HashSet;

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::GROUP_KEYSPACE;
use aruna_core::structs::{Group, RealmId};
use aruna_core::types::GroupId;
use futures_util::{StreamExt, stream};
use serde_json::Value;

use super::api::MetadataApiError;
use super::repository::StorageReadError;
use crate::driver::DriverContext;
use crate::get_metadata_document::is_metadata_record_materialized_for_graph_read;
use crate::jobs::workflow::run_crate::PROCESS_PROFILE;

const GROUP_COUNT_PAGE_SIZE: usize = 1_000;
const GROUP_PURPOSE_SUMMARY_FANOUT_LIMIT: usize = 8;
const PROFILE_TYPE_IRI: &str = "http://www.w3.org/ns/dx/prof/Profile";
const DCTERMS_CONFORMS_TO_IRI: &str = "http://purl.org/dc/terms/conformsTo";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupDocumentPurposeCounts {
    pub dataset_count: u64,
    pub profile_count: u64,
    pub process_run_count: u64,
}

/// Realm-wide number of live metadata documents, not filtered by what any
/// caller may read.
///
/// The count comes from the cached registry snapshot plus one bounded lifecycle
/// batch, and excludes lifecycle-deleted documents. Returns `None` when the node
/// runs without a metadata subsystem, so an absent count stays distinguishable
/// from zero documents.
///
/// An exact per-caller count would need per-document glob evaluation, because
/// read visibility is glob-granular: a `DENY` can subtract a single document
/// from a group-wide grant. The realm total discloses only document volume,
/// which callers already reach realm auth to see.
pub async fn count_realm_documents(
    context: &DriverContext,
    realm_id: RealmId,
) -> Result<Option<u64>, MetadataApiError> {
    let Some(metadata_handle) = context.metadata_handle.as_ref() else {
        return Ok(None);
    };
    let records = metadata_handle
        .list_cached_registry_records()
        .await
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
    let records =
        super::api::filter_live_records(&context.storage_handle, records.as_ref()).await?;
    Ok(Some(
        records
            .iter()
            .filter(|record| record.realm_id == realm_id)
            .count() as u64,
    ))
}

/// Number of stored groups belonging to one realm.
///
/// The group keyspace is read in bounded pages under one read transaction, so
/// the returned count is a consistent snapshot rather than the 10,000-row
/// default page exposed by `ListGroupOperation`.
pub async fn count_realm_groups(
    context: &DriverContext,
    realm_id: RealmId,
) -> Result<u64, MetadataApiError> {
    let txn_id = match context
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: true })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(MetadataApiError::Internal(error.to_string()));
        }
        other => return Err(MetadataApiError::Internal(format!("{other:?}"))),
    };

    let mut count = 0_u64;
    let mut start_after = None;
    let scan = 'scan: loop {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: GROUP_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.map(IterStart::After),
                limit: GROUP_COUNT_PAGE_SIZE,
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                for (_, value) in values {
                    let group = match Group::from_bytes(value.as_ref()) {
                        Ok(group) => group,
                        Err(error) => {
                            break 'scan Err(MetadataApiError::Internal(error.to_string()));
                        }
                    };
                    if group.realm_id == realm_id {
                        count = match count.checked_add(1) {
                            Some(count) => count,
                            None => {
                                break 'scan Err(MetadataApiError::Internal(
                                    "realm group count overflow".to_string(),
                                ));
                            }
                        };
                    }
                }
                match next_start_after {
                    Some(next) => start_after = Some(next),
                    None => break Ok(count),
                }
            }
            Event::Storage(StorageEvent::Error { error }) => {
                break Err(MetadataApiError::Internal(error.to_string()));
            }
            other => break Err(MetadataApiError::Internal(format!("{other:?}"))),
        }
    };

    let completion = if scan.is_ok() {
        StorageEffect::CommitTransaction { txn_id }
    } else {
        StorageEffect::AbortTransaction { txn_id }
    };
    let completion_event = context.storage_handle.send_storage_effect(completion).await;
    match scan {
        Err(error) => Err(error),
        Ok(count) => match completion_event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(count),
            Event::Storage(StorageEvent::Error { error }) => {
                Err(MetadataApiError::Internal(error.to_string()))
            }
            other => Err(MetadataApiError::Internal(format!("{other:?}"))),
        },
    }
}

/// Exact lifecycle-live metadata-document counts for one group, classified
/// solely from each document's root RO-Crate entity.
///
/// The cached group registry plus the shared lifecycle filter select at most
/// the metadata registry candidate limit. For each live document, the graph
/// store's root-summary export is read with at most eight reads in flight; full
/// crates and storage paths are never read for classification. Returns `None`
/// when this node has no metadata subsystem.
pub async fn count_group_documents_by_purpose(
    context: &DriverContext,
    realm_id: RealmId,
    group_id: GroupId,
) -> Result<Option<GroupDocumentPurposeCounts>, MetadataApiError> {
    let Some(metadata_handle) = context.metadata_handle.clone() else {
        return Ok(None);
    };
    let records = metadata_handle
        .list_cached_registry_records_for_group(group_id)
        .await
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
    let records = super::api::filter_live_records(&context.storage_handle, records.as_ref())
        .await?
        .into_iter()
        .filter(|record| record.realm_id == realm_id && record.group_id == group_id)
        .collect::<Vec<_>>();

    let classifications = stream::iter(records.into_iter().map(|record| {
        let metadata_handle = metadata_handle.clone();
        async move {
            if !is_metadata_record_materialized_for_graph_read(context, &record)
                .await
                .map_err(|error| match error {
                    StorageReadError::Storage(error) => {
                        MetadataApiError::Internal(error.to_string())
                    }
                    StorageReadError::Conversion(error) => {
                        MetadataApiError::Internal(error.to_string())
                    }
                })?
            {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            let summary = metadata_handle
                .export_rocrate_summary_jsonld(record.graph_iri.clone())
                .await
                .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
            classify_root_summary(&summary, &record.graph_iri)
        }
    }))
    .buffered(GROUP_PURPOSE_SUMMARY_FANOUT_LIMIT)
    .collect::<Vec<_>>()
    .await;

    let mut counts = GroupDocumentPurposeCounts::default();
    for classification in classifications {
        match classification? {
            DocumentPurpose::Profile => counts.profile_count += 1,
            DocumentPurpose::ProcessRun => counts.process_run_count += 1,
            DocumentPurpose::Dataset => counts.dataset_count += 1,
        }
    }
    Ok(Some(counts))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DocumentPurpose {
    Profile,
    ProcessRun,
    Dataset,
}

pub(crate) fn classify_root_summary(
    summary: &str,
    graph_iri: &str,
) -> Result<DocumentPurpose, MetadataApiError> {
    let document: Value = serde_json::from_str(summary)
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
    let graph = document
        .get("@graph")
        .and_then(Value::as_array)
        .ok_or_else(|| MetadataApiError::Internal("RO-Crate summary has no @graph".to_string()))?;
    let root = graph
        .iter()
        .find(|entity| entity.get("@id").and_then(Value::as_str) == Some(graph_iri))
        .ok_or_else(|| {
            MetadataApiError::Internal("RO-Crate summary has no root entity".to_string())
        })?;

    if jsonld_value_contains_iri(root.get("@type"), PROFILE_TYPE_IRI) {
        return Ok(DocumentPurpose::Profile);
    }

    let mut conforms_to_keys = HashSet::from([
        "conformsTo".to_string(),
        DCTERMS_CONFORMS_TO_IRI.to_string(),
    ]);
    collect_conforms_to_terms(document.get("@context"), &mut conforms_to_keys);
    if root.as_object().is_some_and(|root| {
        root.iter().any(|(key, value)| {
            conforms_to_keys.contains(key)
                && jsonld_value_contains_iri(Some(value), PROCESS_PROFILE)
        })
    }) {
        return Ok(DocumentPurpose::ProcessRun);
    }

    Ok(DocumentPurpose::Dataset)
}

pub fn summary_is_profile(summary: &str, graph_iri: &str) -> Result<bool, MetadataApiError> {
    classify_root_summary(summary, graph_iri)
        .map(|purpose| matches!(purpose, DocumentPurpose::Profile))
}

/// Classifies the validated create payload before projection. Imported crates
/// may still name their root as `./`, so fall back to the descriptor's `about`
/// target when the final graph IRI is not present yet.
pub(crate) fn rocrate_is_profile(jsonld: &str, graph_iri: &str) -> Result<bool, String> {
    let document: Value = serde_json::from_str(jsonld).map_err(|error| error.to_string())?;
    let graph = document
        .get("@graph")
        .and_then(Value::as_array)
        .ok_or_else(|| "RO-Crate has no @graph".to_string())?;
    let descriptor_root = graph
        .iter()
        .find(|entity| entity.get("@id").and_then(Value::as_str) == Some("ro-crate-metadata.json"))
        .and_then(|descriptor| {
            [
                "about",
                "http://schema.org/about",
                "https://schema.org/about",
            ]
            .into_iter()
            .find_map(|key| descriptor.get(key))
        })
        .and_then(|about| about.get("@id"))
        .and_then(Value::as_str);
    let root = graph
        .iter()
        .find(|entity| entity.get("@id").and_then(Value::as_str) == Some(graph_iri))
        .or_else(|| {
            descriptor_root.and_then(|root_id| {
                graph
                    .iter()
                    .find(|entity| entity.get("@id").and_then(Value::as_str) == Some(root_id))
            })
        })
        .ok_or_else(|| "RO-Crate has no root entity".to_string())?;
    Ok(jsonld_value_contains_iri(
        root.get("@type"),
        PROFILE_TYPE_IRI,
    ))
}

fn collect_conforms_to_terms(value: Option<&Value>, terms: &mut HashSet<String>) {
    match value {
        Some(Value::Array(values)) => {
            for value in values {
                collect_conforms_to_terms(Some(value), terms);
            }
        }
        Some(Value::Object(entries)) => {
            for (term, definition) in entries {
                let iri = match definition {
                    Value::String(iri) => Some(iri.as_str()),
                    Value::Object(definition) => definition.get("@id").and_then(Value::as_str),
                    _ => None,
                };
                if iri == Some(DCTERMS_CONFORMS_TO_IRI) {
                    terms.insert(term.clone());
                }
            }
        }
        _ => {}
    }
}

fn jsonld_value_contains_iri(value: Option<&Value>, expected: &str) -> bool {
    match value {
        Some(Value::String(value)) => value == expected,
        Some(Value::Array(values)) => values
            .iter()
            .any(|value| jsonld_value_contains_iri(Some(value), expected)),
        Some(Value::Object(value)) => {
            value
                .get("@id")
                .or_else(|| value.get("id"))
                .and_then(Value::as_str)
                == Some(expected)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::metadata::{
        MetadataApplyRoCrateRequest, MetadataEffect, MetadataEvent, MetadataGraphLifecycleRecord,
        MetadataGraphPolicy, MetadataRequestDurability,
    };
    use aruna_core::storage_entries::{
        metadata_graph_lifecycle_write_entry, metadata_registry_write_entries,
    };
    use aruna_core::structs::{Actor, Group, MetadataRegistryRecord, PlacementRef};
    use aruna_core::types::GroupId;
    use aruna_storage::{FjallStorage, StorageHandle};
    use byteview::ByteView;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    use super::*;
    use crate::metadata::MetadataHandle;

    const REALM_SEED: [u8; 32] = [7u8; 32];

    struct Fixture {
        _storage_dir: TempDir,
        _metadata_dir: TempDir,
        storage: StorageHandle,
        context: Arc<DriverContext>,
        realm_id: RealmId,
    }

    fn setup_fixture() -> Fixture {
        let storage_dir = tempdir().expect("temp dir");
        let metadata_dir = tempdir().expect("metadata dir");
        let storage = FjallStorage::open(storage_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
        let metadata_handle = MetadataHandle::new(
            metadata_dir.path(),
            node_id,
            storage.clone(),
            None,
            None,
            None,
        )
        .expect("metadata handle opens");
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata_handle),
            task_handle: None,
            compute_handle: None,
        });
        Fixture {
            _storage_dir: storage_dir,
            _metadata_dir: metadata_dir,
            storage,
            context,
            realm_id: RealmId::from_bytes(REALM_SEED),
        }
    }

    fn registry_record(
        realm_id: RealmId,
        group_id: GroupId,
        public: bool,
    ) -> MetadataRegistryRecord {
        registry_record_at(realm_id, group_id, public, None)
    }

    fn registry_record_at(
        realm_id: RealmId,
        group_id: GroupId,
        public: bool,
        path: Option<&str>,
    ) -> MetadataRegistryRecord {
        let document_id = Ulid::generate();
        let path = path
            .map(str::to_string)
            .unwrap_or_else(|| format!("datasets/{document_id}"));
        MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: path.clone(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                &path,
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::nil(),
            last_event_id: Ulid::nil(),
        }
    }

    async fn write_entries(storage: &StorageHandle, writes: Vec<(String, ByteView, ByteView)>) {
        match storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    async fn write_record(fixture: &Fixture, record: &MetadataRegistryRecord) {
        write_entries(
            &fixture.storage,
            metadata_registry_write_entries(record).expect("registry entries"),
        )
        .await;
    }

    async fn write_rocrate(
        fixture: &Fixture,
        record: &MetadataRegistryRecord,
        root_type: Value,
        conforms_to: Option<&str>,
    ) {
        let mut root = serde_json::json!({
            "@id": record.graph_iri,
            "@type": root_type,
            "name": "Purpose fixture",
            "description": "Classification comes from this root",
            "datePublished": "2026-08-19",
            "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"}
        });
        if let Some(profile) = conforms_to {
            root.as_object_mut().unwrap().insert(
                "purposeProfile".to_string(),
                serde_json::json!({"@id": profile}),
            );
        }
        let jsonld = serde_json::json!({
            "@context": [
                "https://w3id.org/ro/crate/1.2/context",
                {"purposeProfile": {"@id": DCTERMS_CONFORMS_TO_IRI, "@type": "@id"}}
            ],
            "@graph": [
                {
                    "@id": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                    "about": {"@id": record.graph_iri}
                },
                root
            ]
        });
        let handle = fixture.context.metadata_handle.as_ref().unwrap();
        match handle
            .send_metadata_effect(MetadataEffect::ApplyRoCrate {
                request: MetadataApplyRoCrateRequest {
                    graph_iri: record.graph_iri.clone(),
                    jsonld: jsonld.to_string(),
                    policy: MetadataGraphPolicy {
                        public: true,
                        permission_paths: Vec::new(),
                    },
                    durability: MetadataRequestDurability::Durable,
                    deterministic_actor: None,
                },
            })
            .await
        {
            Event::Metadata(MetadataEvent::ApplyRoCrateResult { .. }) => {}
            other => panic!("unexpected metadata event: {other:?}"),
        }
        write_record(fixture, record).await;
    }

    #[tokio::test]
    async fn counts_realm_total() {
        // Private documents and groups the caller holds no role in still count.
        let fixture = setup_fixture();
        let first_group = Ulid::generate();
        let second_group = Ulid::generate();
        for record in [
            registry_record(fixture.realm_id, first_group, false),
            registry_record(fixture.realm_id, first_group, true),
            registry_record(fixture.realm_id, second_group, false),
        ] {
            write_record(&fixture, &record).await;
        }

        assert_eq!(
            count_realm_documents(&fixture.context, fixture.realm_id)
                .await
                .expect("count succeeds"),
            Some(3)
        );
    }

    #[tokio::test]
    async fn skips_foreign_realm() {
        let fixture = setup_fixture();
        let group_id = Ulid::generate();
        write_record(&fixture, &registry_record(fixture.realm_id, group_id, true)).await;
        write_record(
            &fixture,
            &registry_record(RealmId::from_bytes([9u8; 32]), group_id, true),
        )
        .await;

        assert_eq!(
            count_realm_documents(&fixture.context, fixture.realm_id)
                .await
                .expect("count succeeds"),
            Some(1)
        );
    }

    #[tokio::test]
    async fn deleted_not_counted() {
        // A graph lifecycle tombstone hides its document from the count even
        // while the registry row is still present.
        let fixture = setup_fixture();
        let group_id = Ulid::generate();
        let kept = registry_record(fixture.realm_id, group_id, true);
        let deleted = registry_record(fixture.realm_id, group_id, true);
        write_record(&fixture, &kept).await;
        write_record(&fixture, &deleted).await;
        let tombstone = MetadataGraphLifecycleRecord::deleted(
            deleted.graph_iri.clone(),
            fixture.realm_id,
            group_id,
            deleted.document_id,
            2,
        );
        write_entries(
            &fixture.storage,
            vec![metadata_graph_lifecycle_write_entry(&tombstone).expect("lifecycle entry")],
        )
        .await;

        assert_eq!(
            count_realm_documents(&fixture.context, fixture.realm_id)
                .await
                .expect("count succeeds"),
            Some(1)
        );
    }

    #[tokio::test]
    async fn counts_realm_groups_from_a_consistent_paged_scan() {
        let fixture = setup_fixture();
        let owner = aruna_core::UserId::nil(fixture.realm_id);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
            user_id: owner,
            realm_id: fixture.realm_id,
        };
        let local = Group {
            display_name: "local".to_string(),
            group_id: Ulid::generate(),
            realm_id: fixture.realm_id,
            roles: Default::default(),
            owner,
        };
        let foreign_realm = RealmId::from_bytes([9u8; 32]);
        let foreign = Group {
            display_name: "foreign".to_string(),
            group_id: Ulid::generate(),
            realm_id: foreign_realm,
            roles: Default::default(),
            owner: aruna_core::UserId::nil(foreign_realm),
        };
        write_entries(
            &fixture.storage,
            vec![
                (
                    GROUP_KEYSPACE.to_string(),
                    ByteView::from(local.group_id.to_bytes().to_vec()),
                    ByteView::from(local.to_bytes(&actor).unwrap()),
                ),
                (
                    GROUP_KEYSPACE.to_string(),
                    ByteView::from(foreign.group_id.to_bytes().to_vec()),
                    ByteView::from(foreign.to_bytes(&actor).unwrap()),
                ),
            ],
        )
        .await;

        assert_eq!(
            count_realm_groups(&fixture.context, fixture.realm_id)
                .await
                .expect("group count succeeds"),
            1
        );
    }

    #[tokio::test]
    async fn group_purpose_counts_classify_root_fixtures() {
        let fixture = setup_fixture();
        let group_id = Ulid::generate();
        // Deliberately misleading paths prove that classification does not use
        // the storage path. Profile also conforms to Process Run and must win.
        let profile = registry_record_at(
            fixture.realm_id,
            group_id,
            true,
            Some("runs/looks-like-process"),
        );
        let process_run = registry_record_at(
            fixture.realm_id,
            group_id,
            true,
            Some("profiles/looks-like-profile"),
        );
        let dataset =
            registry_record_at(fixture.realm_id, group_id, true, Some("runs/plain-dataset"));
        write_rocrate(
            &fixture,
            &profile,
            serde_json::json!(["Dataset", PROFILE_TYPE_IRI]),
            Some(PROCESS_PROFILE),
        )
        .await;
        write_rocrate(
            &fixture,
            &process_run,
            serde_json::json!("Dataset"),
            Some(PROCESS_PROFILE),
        )
        .await;
        write_rocrate(&fixture, &dataset, serde_json::json!("Dataset"), None).await;

        assert_eq!(
            count_group_documents_by_purpose(&fixture.context, fixture.realm_id, group_id)
                .await
                .expect("purpose count succeeds"),
            Some(GroupDocumentPurposeCounts {
                dataset_count: 1,
                profile_count: 1,
                process_run_count: 1,
            })
        );
    }

    #[tokio::test]
    async fn unconfigured_reports_none() {
        // An absent count must stay distinguishable from zero documents.
        let storage_dir = tempdir().expect("temp dir");
        let context = DriverContext {
            storage_handle: FjallStorage::open(storage_dir.path().to_str().expect("utf-8 path"))
                .expect("storage opens"),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        assert_eq!(
            count_realm_documents(&context, RealmId::from_bytes(REALM_SEED))
                .await
                .expect("count succeeds"),
            None
        );
        assert_eq!(
            count_group_documents_by_purpose(
                &context,
                RealmId::from_bytes(REALM_SEED),
                Ulid::generate(),
            )
            .await
            .expect("unconfigured purpose count succeeds"),
            None
        );
    }
}
