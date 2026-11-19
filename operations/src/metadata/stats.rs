//! Counts a group's documents by their purpose.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::entity_patch::DCTERMS_CONFORMS_IRI;
use std::collections::HashSet;

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::metadata::{
    MetadataEffect, MetadataError, MetadataEvent, REGISTRY_CANDIDATE_LIMIT,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::rows::event;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::GroupId;
use serde_json::Value;

use super::api::MetadataApiError;
use super::api::list::{GroupRecordsOperation, RecordSummaryOperation};
use crate::metadata::profile::rules::{
    PROFILE_TYPE_IRI, collect_conforms_terms, jsonld_contains_iri,
};
use aruna_core::metadata::CRATE_PROFILE_IRI;

const SUMMARY_FANOUT_LIMIT: usize = 8;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupDocumentCounts {
    pub dataset_count: u64,
    pub profile_count: u64,
    pub process_run_count: u64,
}

#[derive(Debug, PartialEq)]
enum PurposeStage {
    Probe,
    Records,
    Summaries(Vec<String>),
}

/// Exact lifecycle-live metadata-document counts for one group, classified solely from each
/// document's root RO-Crate entity via bounded root-summary reads; full paths never load.
/// `None` without a metadata subsystem.
#[derive(Debug, PartialEq)]
pub struct GroupPurposeOperation {
    realm_id: RealmId,
    group_id: GroupId,
    stage: PurposeStage,
}

impl GroupPurposeOperation {
    pub fn new(realm_id: RealmId, group_id: GroupId) -> Self {
        Self {
            realm_id,
            group_id,
            stage: PurposeStage::Probe,
        }
    }

    fn summaries(
        &mut self,
        records: Vec<MetadataRegistryRecord>,
    ) -> Next<Option<GroupDocumentCounts>> {
        let records: Vec<_> = (records.into_iter())
            .filter(|record| record.realm_id == self.realm_id && record.group_id == self.group_id)
            .collect();
        if records.is_empty() {
            return Next::Done(Some(GroupDocumentCounts::default()));
        }
        let graphs = records
            .iter()
            .map(|record| record.graph_iri.clone())
            .collect();
        let members = (records.into_iter())
            .map(|record| {
                Effect::SubOperation(boxed_suboperation(
                    RecordSummaryOperation::new(record),
                    |result| Event::Child(ChildOutcome::new(result)),
                ))
            })
            .collect();
        self.stage = PurposeStage::Summaries(graphs);
        Next::Call(IoCall::Group(EffectGroup {
            members,
            limit: SUMMARY_FANOUT_LIMIT,
            form: GroupForm::All { deadline: None },
        }))
    }

    fn counted(
        graphs: Vec<String>,
        result: IoResult,
    ) -> Result<GroupDocumentCounts, MetadataApiError> {
        let IoResult::Group(GroupEvent::Finished { results }) = result else {
            return Err(MetadataApiError::ServiceUnavailable);
        };
        let mut counts = GroupDocumentCounts::default();
        for (graph_iri, member) in graphs.iter().zip(results) {
            let GroupResult::Done(event) = member else {
                return Err(MetadataApiError::ServiceUnavailable);
            };
            let Event::Child(outcome) = *event else {
                return Err(MetadataApiError::ServiceUnavailable);
            };
            let summary = outcome.take::<String, MetadataApiError>();
            let summary = summary.ok_or(MetadataApiError::ServiceUnavailable)??;
            match classify_root_summary(&summary, graph_iri)? {
                DocumentPurpose::Profile => counts.profile_count += 1,
                DocumentPurpose::ProcessRun => counts.process_run_count += 1,
                DocumentPurpose::Dataset => counts.dataset_count += 1,
            }
        }
        Ok(counts)
    }
}

impl Operation for GroupPurposeOperation {
    type Output = Option<GroupDocumentCounts>;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<Option<GroupDocumentCounts>>, MetadataApiError> {
        // The cache probe tells a node without a metadata subsystem apart.
        let probe = MetadataEffect::CachedGroup {
            group_id: self.group_id,
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            probe,
        )))))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<Option<GroupDocumentCounts>>, MetadataApiError> {
        match std::mem::replace(&mut self.stage, PurposeStage::Records) {
            PurposeStage::Probe => {
                if let Event::Metadata(MetadataEvent::Error {
                    error: MetadataError::HandleMissing,
                    ..
                }) = event(result)?
                {
                    return Ok(Next::Done(None));
                }
                self.stage = PurposeStage::Records;
                let records = GroupRecordsOperation::new(self.group_id, REGISTRY_CANDIDATE_LIMIT);
                Ok(Next::Call(IoCall::child(records)))
            }
            PurposeStage::Records => {
                let records = result.child::<Vec<MetadataRegistryRecord>, MetadataApiError>()??;
                Ok(self.summaries(records))
            }
            PurposeStage::Summaries(graphs) => Ok(Next::Done(Some(Self::counted(graphs, result)?))),
        }
    }
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

    if jsonld_contains_iri(root.get("@type"), PROFILE_TYPE_IRI) {
        return Ok(DocumentPurpose::Profile);
    }

    let mut conforms_to_keys =
        HashSet::from(["conformsTo".to_string(), DCTERMS_CONFORMS_IRI.to_string()]);
    collect_conforms_terms(document.get("@context"), &mut conforms_to_keys);
    if root.as_object().is_some_and(|root| {
        root.iter().any(|(key, value)| {
            conforms_to_keys.contains(key) && jsonld_contains_iri(Some(value), CRATE_PROFILE_IRI)
        })
    }) {
        return Ok(DocumentPurpose::ProcessRun);
    }

    Ok(DocumentPurpose::Dataset)
}

#[cfg(test)]
mod tests {
    use crate::testing::write_entries;
    use std::sync::Arc;

    use aruna_core::events::Event;
    use aruna_core::keyspaces::GROUP_KEYSPACE;
    use aruna_core::metadata::{
        ApplyRoCrateRequest, GraphLifecycleRecord, MetadataEffect, MetadataEvent,
        MetadataGraphPolicy, MetadataRequestDurability,
    };
    use aruna_core::storage_entries::{graph_lifecycle_entry, registry_write_entries};
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::group::Group;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
    use aruna_core::types::GroupId;
    use aruna_storage::{FjallStorage, StorageHandle};
    use byteview::ByteView;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::metadata::MetadataHandle;
    use crate::metadata::realm_counts::{RealmDocumentsOperation, RealmGroupsOperation};

    const REALM_SEED: [u8; 32] = [7u8; 32];

    async fn count_group_purpose(
        context: &DriverContext,
        realm_id: RealmId,
        group_id: GroupId,
    ) -> Result<Option<GroupDocumentCounts>, MetadataApiError> {
        drive(GroupPurposeOperation::new(realm_id, group_id), context).await
    }

    #[test]
    fn missing_subsystem_empty() {
        let mut count = GroupPurposeOperation::new(RealmId::from_bytes(REALM_SEED), Ulid::nil());
        assert!(matches!(count.first(), Ok(Next::Call(IoCall::Effect(_)))));
        let missing = IoResult::Event(Box::new(Event::Metadata(MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::HandleMissing,
        })));
        assert_eq!(count.next(missing), Ok(Next::Done(None)));
    }

    #[test]
    fn empty_group_zero() {
        let mut count = GroupPurposeOperation::new(RealmId::from_bytes(REALM_SEED), Ulid::nil());
        count.first().unwrap();
        let cached = IoResult::Event(Box::new(Event::Metadata(MetadataEvent::CachedGroup {
            records: None,
        })));
        assert!(matches!(
            count.next(cached),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let none = IoResult::Child(ChildOutcome::new(Ok::<
            Vec<MetadataRegistryRecord>,
            MetadataApiError,
        >(Vec::new())));
        assert_eq!(
            count.next(none),
            Ok(Next::Done(Some(GroupDocumentCounts::default())))
        );
    }

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
        let graph_handle = aruna_graph::GraphHandle::open(
            metadata_dir.path(),
            node_id,
            None,
            None,
            aruna_graph::GraphOptions::default(),
        )
        .expect("metadata handle opens");
        let metadata_handle = MetadataHandle::new(
            metadata_dir.path(),
            storage.clone(),
            None,
            Default::default(),
        );
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata_handle),
            graph_handle: Some(graph_handle),
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
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

    async fn write_record(fixture: &Fixture, record: &MetadataRegistryRecord) {
        write_entries(
            &fixture.storage,
            registry_write_entries(record).expect("registry entries"),
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
                {"purposeProfile": {"@id": DCTERMS_CONFORMS_IRI, "@type": "@id"}}
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
        match crate::metadata::send_metadata_effect(
            &fixture.context,
            MetadataEffect::ApplyRoCrate {
                request: ApplyRoCrateRequest {
                    graph_iri: record.graph_iri.clone(),
                    jsonld: jsonld.to_string(),
                    policy: MetadataGraphPolicy {
                        public: true,
                        permission_paths: Vec::new(),
                    },
                    durability: MetadataRequestDurability::Durable,
                    deterministic_actor: None,
                },
            },
        )
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
            drive(
                RealmDocumentsOperation::new(fixture.realm_id),
                &fixture.context
            )
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
            drive(
                RealmDocumentsOperation::new(fixture.realm_id),
                &fixture.context
            )
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
        let tombstone = GraphLifecycleRecord::deleted(
            deleted.graph_iri.clone(),
            fixture.realm_id,
            group_id,
            deleted.document_id,
            2,
        );
        write_entries(
            &fixture.storage,
            vec![graph_lifecycle_entry(&tombstone).expect("lifecycle entry")],
        )
        .await;

        assert_eq!(
            drive(
                RealmDocumentsOperation::new(fixture.realm_id),
                &fixture.context
            )
            .await
            .expect("count succeeds"),
            Some(1)
        );
    }

    #[tokio::test]
    async fn counts_realm_groups() {
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
            drive(
                RealmGroupsOperation::new(fixture.realm_id),
                &fixture.context
            )
            .await
            .expect("group count succeeds"),
            1
        );
    }

    #[tokio::test]
    async fn group_purpose_counts() {
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
            Some(CRATE_PROFILE_IRI),
        )
        .await;
        write_rocrate(
            &fixture,
            &process_run,
            serde_json::json!("Dataset"),
            Some(CRATE_PROFILE_IRI),
        )
        .await;
        write_rocrate(&fixture, &dataset, serde_json::json!("Dataset"), None).await;

        assert_eq!(
            count_group_purpose(&fixture.context, fixture.realm_id, group_id)
                .await
                .expect("purpose count succeeds"),
            Some(GroupDocumentCounts {
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        assert_eq!(
            drive(
                RealmDocumentsOperation::new(RealmId::from_bytes(REALM_SEED)),
                &context
            )
            .await
            .expect("count succeeds"),
            None
        );
        assert_eq!(
            count_group_purpose(&context, RealmId::from_bytes(REALM_SEED), Ulid::generate(),)
                .await
                .expect("unconfigured purpose count succeeds"),
            None
        );
    }
}
