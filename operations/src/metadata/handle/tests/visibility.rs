use super::super::*;
use super::*;
pub(super) fn registry_record(document_path: &str) -> MetadataRegistryRecord {
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

pub(super) fn group_record(group_id: GroupId, document_path: &str) -> MetadataRegistryRecord {
    let mut record = registry_record(document_path);
    record.group_id = group_id;
    record.public = false;
    record.permission_path = MetadataRegistryRecord::permission_path_for(
        &record.realm_id,
        group_id,
        document_path,
        record.document_id,
    );
    record
}

pub(super) fn read_rules(
    patterns: &[(&str, Permission)],
) -> crate::auth::permission_rules::PermissionRules {
    crate::auth::permission_rules::PermissionRules::from_roles(
        vec![crate::auth::permission_rules::CollectedRole {
            role: aruna_core::structs::Role {
                role_id: Ulid::generate(),
                name: "test".to_string(),
                permissions: patterns
                    .iter()
                    .map(|(pattern, permission)| ((*pattern).to_string(), permission.clone()))
                    .collect(),
                assigned_users: HashSet::new(),
            },
            direct: true,
            public: false,
        }],
        None,
    )
    .expect("patterns compile")
}

pub(super) fn filled_cache(records: Vec<MetadataRegistryRecord>) -> MetadataVisibilityCache {
    let cache = MetadataVisibilityCache::new();
    cache.store_registry_records(Arc::new(records));
    cache
}

#[test]
fn upsert_replaces_existing_record_and_appends_new_ones() {
    let mut existing = registry_record("datasets/a");
    let cache = filled_cache(vec![existing.clone()]);

    existing.public = false;
    existing.updated_at_ms = 42;
    let added = registry_record("datasets/b");
    cache.upsert_registry_records(&[existing.clone(), added.clone()]);

    let records = cache.registry_records().expect("cache entry");
    assert_eq!(records.len(), 2);
    let updated = records
        .iter()
        .find(|record| record.document_id == existing.document_id)
        .expect("updated record");
    assert!(!updated.public);
    assert_eq!(updated.updated_at_ms, 42);
    assert!(
        records
            .iter()
            .any(|record| record.document_id == added.document_id)
    );
}
#[test]
fn upsert_discards_cache() {
    let records = (0..METADATA_REGISTRY_CANDIDATE_LIMIT)
        .map(|index| registry_record(&format!("datasets/{index}")))
        .collect();
    let cache = filled_cache(records);

    cache.upsert_registry_records(&[registry_record("datasets/overflow")]);

    assert!(cache.registry_records().is_none());
}
#[test]
fn upsert_without_filled_cache_is_noop_until_refill() {
    let cache = MetadataVisibilityCache::new();
    cache.upsert_registry_records(&[registry_record("datasets/a")]);
    assert!(cache.registry_records().is_none());
}
#[test]
fn stale_cache_callback() {
    let record = registry_record("datasets/race");
    let cache = filled_cache(vec![record.clone()]);
    let generation = cache.current_generation();

    cache.remove_registry_record(record.document_id);
    cache.upsert_at(std::slice::from_ref(&record), Some(generation));

    assert!(
        cache
            .registry_records()
            .is_some_and(|records| records.is_empty())
    );
}
#[test]
fn remove_by_document_and_graph_drop_records() {
    let by_document = registry_record("datasets/a");
    let by_graph = registry_record("datasets/b");
    let kept = registry_record("datasets/c");
    let cache = filled_cache(vec![by_document.clone(), by_graph.clone(), kept.clone()]);

    cache.remove_registry_record(by_document.document_id);
    cache.remove_registry_records_by_graph(&by_graph.graph_iri);

    let records = cache.registry_records().expect("cache entry");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].document_id, kept.document_id);
}
