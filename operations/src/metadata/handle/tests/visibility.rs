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
#[test]
fn group_snapshots_are_scoped_and_invalidate_per_group() {
    let group_a = Ulid::generate();
    let group_b = Ulid::generate();
    let mut record_a = registry_record("datasets/a");
    record_a.group_id = group_a;
    let mut record_b = registry_record("datasets/b");
    record_b.group_id = group_b;
    let cache = filled_cache(vec![record_a.clone(), record_b.clone()]);

    let (listed_a, fresh) = cache
        .registry_records_for_group_any(group_a)
        .expect("group A snapshot exists");
    assert!(fresh);
    assert_eq!(listed_a.as_ref(), &vec![record_a.clone()]);

    let mut added_b = registry_record("datasets/b2");
    added_b.group_id = group_b;
    cache.upsert_registry_records(std::slice::from_ref(&added_b));
    let (listed_a_again, _) = cache
        .registry_records_for_group_any(group_a)
        .expect("group A snapshot still exists");
    assert!(Arc::ptr_eq(&listed_a, &listed_a_again));

    let mut added_a = registry_record("datasets/a2");
    added_a.group_id = group_a;
    cache.upsert_registry_records(std::slice::from_ref(&added_a));
    let (listed_a_after, _) = cache
        .registry_records_for_group_any(group_a)
        .expect("group A snapshot refreshes");
    assert_eq!(listed_a_after.len(), 2);
    assert!(!Arc::ptr_eq(&listed_a, &listed_a_after));
}
#[test]
fn upsert_does_not_extend_expiry_or_resurrect_expired_entries() {
    let cache = filled_cache(vec![registry_record("datasets/a")]);
    {
        let mut registry = cache.registry.lock().unwrap();
        registry.as_mut().expect("cache entry").expires_at =
            Instant::now() - Duration::from_secs(1);
    }

    cache.upsert_registry_records(&[registry_record("datasets/b")]);

    assert!(cache.registry_records().is_none());
}
#[test]
fn lifecycle_entry_removal_forces_storage_reread() {
    let cache = MetadataVisibilityCache::new();
    cache.store_lifecycle_deleted("urn:graph:a".to_string(), false);
    assert_eq!(cache.lifecycle_deleted("urn:graph:a"), Some(false));

    cache.remove_lifecycle_entry("urn:graph:a");
    assert_eq!(cache.lifecycle_deleted("urn:graph:a"), None);
}
#[test]
fn store_prunes_expired() {
    let cache = MetadataVisibilityCache::new();
    cache.store_lifecycle_deleted("urn:graph:old".to_string(), false);
    cache.expire_now();

    cache.store_lifecycle_deleted("urn:graph:new".to_string(), false);

    assert_eq!(cache.lifecycle_deleted_any("urn:graph:old"), None);
    assert_eq!(cache.lifecycle_deleted("urn:graph:new"), Some(false));
}
#[test]
fn refresh_keeps_tombstone() {
    let cache = MetadataVisibilityCache::new();
    cache.refresh_lifecycle_deleted(
        (0..METADATA_REGISTRY_CANDIDATE_LIMIT)
            .map(|index| (format!("urn:graph:old:{index}"), false))
            .collect::<Vec<_>>(),
    );

    cache.refresh_lifecycle_deleted(vec![("urn:graph:deleted".to_string(), true)]);

    assert_eq!(cache.lifecycle_deleted("urn:graph:deleted"), Some(true));
    assert_eq!(
        cache.lifecycle_deleted.lock().unwrap().len(),
        METADATA_REGISTRY_CANDIDATE_LIMIT
    );
}
#[test]
fn eviction_keeps_tombstone() {
    let deleted_record = registry_record("datasets/deleted");
    let cache = MetadataVisibilityCache::new();
    cache.store_lifecycle_deleted(deleted_record.graph_iri.clone(), true);
    cache.refresh_lifecycle_deleted(
        (0..METADATA_REGISTRY_CANDIDATE_LIMIT)
            .map(|index| (format!("urn:graph:current:{index}"), false))
            .collect::<Vec<_>>(),
    );
    assert!(
        cache
            .lifecycle_deleted_any(&deleted_record.graph_iri)
            .is_none()
    );

    let scope = GraphVisibilityScope {
        records: Arc::new(vec![deleted_record.clone()]),
        permissions: GroupPermissionRules::default(),
        lifecycle_visibility: LifecycleVisibility::Cache(HashSet::from([deleted_record
            .graph_iri
            .clone()])),
    };
    assert!(!scope.graph_visible(&cache, &deleted_record.graph_iri));
}
#[test]
fn expired_registry_entry_is_served_stale_not_dropped() {
    let record = registry_record("datasets/a");
    let cache = filled_cache(vec![record.clone()]);
    cache.expire_now();

    assert!(cache.registry_records().is_none());
    let (records, fresh) = cache.registry_records_any().expect("stale entry kept");
    assert!(!fresh);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].document_id, record.document_id);

    cache.store_registry_records(Arc::new(vec![record.clone()]));
    let (_, fresh) = cache.registry_records_any().expect("fresh entry");
    assert!(fresh);
    assert!(cache.registry_records().is_some());
}
#[test]
fn background_visibility_fill_does_not_overwrite_newer_upsert() {
    let mut stale_record = registry_record("datasets/a");
    let cache = filled_cache(vec![stale_record.clone()]);
    let fill_generation = cache.current_generation();

    let mut updated_record = stale_record.clone();
    updated_record.public = false;
    updated_record.updated_at_ms = 42;
    cache.upsert_registry_records(std::slice::from_ref(&updated_record));

    stale_record.updated_at_ms = 1;
    assert!(!cache.store_visibility_fill(
        Arc::new(vec![stale_record]),
        Vec::new(),
        fill_generation,
    ));
    let records = cache.registry_records().expect("cache entry");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].document_id, updated_record.document_id);
    assert!(!records[0].public);
    assert_eq!(records[0].updated_at_ms, 42);
}
#[test]
fn background_visibility_fill_does_not_resurrect_removed_document() {
    let removed = registry_record("datasets/removed");
    let kept = registry_record("datasets/kept");
    let cache = filled_cache(vec![removed.clone(), kept.clone()]);
    let fill_generation = cache.current_generation();

    cache.remove_registry_record(removed.document_id);

    assert!(!cache.store_visibility_fill(
        Arc::new(vec![removed.clone(), kept.clone()]),
        Vec::new(),
        fill_generation,
    ));
    let records = cache.registry_records().expect("cache entry");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].document_id, kept.document_id);
    assert!(
        !records
            .iter()
            .any(|record| record.document_id == removed.document_id)
    );
}
#[test]
fn background_visibility_fill_does_not_clear_newer_lifecycle_tombstone() {
    let record = registry_record("datasets/deleted");
    let cache = filled_cache(vec![record.clone()]);
    let fill_generation = cache.current_generation();

    cache.store_lifecycle_deleted(record.graph_iri.clone(), true);

    assert!(!cache.store_visibility_fill(
        Arc::new(vec![record.clone()]),
        vec![(record.graph_iri.clone(), false)],
        fill_generation,
    ));
    assert_eq!(cache.lifecycle_deleted(&record.graph_iri), Some(true));
}
#[test]
fn rejected_cold_group_fill_filters_fresh_records_for_requested_group() {
    let group_a = Ulid::generate();
    let group_b = Ulid::generate();
    let mut record_a = registry_record("datasets/a");
    record_a.group_id = group_a;
    let mut record_b = registry_record("datasets/b");
    record_b.group_id = group_b;
    let cache = MetadataVisibilityCache::new();
    let fill_generation = cache.current_generation();
    let fresh_records = Arc::new(vec![record_a.clone(), record_b]);

    cache.advance_generation();
    assert!(!cache.store_visibility_fill(fresh_records.clone(), Vec::new(), fill_generation));
    assert!(cache.registry_records_for_group_any(group_a).is_none());

    let listed = registry_records_for_group(&fresh_records, group_a);
    assert_eq!(listed.as_ref(), &vec![record_a]);
}
#[test]
fn expired_lifecycle_entry_is_served_stale_not_dropped() {
    let cache = MetadataVisibilityCache::new();
    cache.store_lifecycle_deleted("urn:graph:a".to_string(), true);
    cache.expire_now();

    assert_eq!(cache.lifecycle_deleted("urn:graph:a"), None);
    assert_eq!(
        cache.lifecycle_deleted_any("urn:graph:a"),
        Some((true, false))
    );
}
#[test]
fn registry_record_lookup_parses_iri_and_falls_back_to_scan() {
    let mut records: Vec<_> = (0..4)
        .map(|index| registry_record(&format!("datasets/{index}")))
        .collect();
    let mut custom = registry_record("datasets/custom");
    custom.graph_iri = "https://example.org/custom-graph".to_string();
    records.push(custom.clone());
    records.sort_unstable_by_key(|record| record.document_id);

    for record in &records {
        let found = registry_record_for_graph(&records, &record.graph_iri).expect("record found");
        assert_eq!(found.document_id, record.document_id);
    }
    assert!(
        registry_record_for_graph(
            &records,
            &MetadataRegistryRecord::graph_iri_for(Ulid::generate())
        )
        .is_none()
    );
    assert!(registry_record_for_graph(&records, "https://example.org/missing").is_none());
}
#[test]
fn visibility_scope_enforces_public_group_and_lifecycle_rules() {
    let realm = RealmId([7u8; 32]);
    let mut public_record = registry_record("datasets/public");
    public_record.public = true;
    let mut private_record = registry_record("datasets/private");
    private_record.public = false;
    let mut deleted_record = registry_record("datasets/deleted");
    deleted_record.public = true;
    let mut records = vec![
        public_record.clone(),
        private_record.clone(),
        deleted_record.clone(),
    ];
    records.sort_unstable_by_key(|record| record.document_id);

    let cache = MetadataVisibilityCache::new();
    cache.store_lifecycle_deleted(deleted_record.graph_iri.clone(), true);

    let anonymous = GraphVisibilityScope {
        records: Arc::new(records.clone()),
        permissions: GroupPermissionRules::default(),
        lifecycle_visibility: LifecycleVisibility::Cache(HashSet::new()),
    };
    assert!(anonymous.graph_visible(&cache, &public_record.graph_iri));
    assert!(!anonymous.graph_visible(&cache, &private_record.graph_iri));
    assert!(!anonymous.graph_visible(&cache, &deleted_record.graph_iri));
    assert!(!anonymous.graph_visible(
        &cache,
        &MetadataRegistryRecord::graph_iri_for(Ulid::generate())
    ));

    let readable = HashMap::from([(
        private_record.group_id,
        read_rules(&[(private_record.permission_path.as_str(), Permission::READ)]),
    )]);
    let member = GraphVisibilityScope {
        records: Arc::new(records.clone()),
        permissions: GroupPermissionRules::from_groups(Some(realm), readable.clone()),
        lifecycle_visibility: LifecycleVisibility::Cache(HashSet::new()),
    };
    assert!(member.graph_visible(&cache, &public_record.graph_iri));
    assert!(member.graph_visible(&cache, &private_record.graph_iri));
    assert!(!member.graph_visible(&cache, &deleted_record.graph_iri));

    let wrong_realm = GraphVisibilityScope {
        records: Arc::new(records),
        permissions: GroupPermissionRules::from_groups(Some(RealmId([8u8; 32])), readable),
        lifecycle_visibility: LifecycleVisibility::Cache(HashSet::new()),
    };
    assert!(wrong_realm.graph_visible(&cache, &public_record.graph_iri));
    assert!(!wrong_realm.graph_visible(&cache, &private_record.graph_iri));
}
