use super::super::*;
use super::visibility::{group_record, read_rules, registry_record};
use super::*;
#[test]
fn deny_hides_document() {
    // A per-document DENY under a group-wide READ hides only that document.
    let realm = RealmId([7u8; 32]);
    let group_id = Ulid::generate();
    let secret = group_record(group_id, "datasets/secret");
    let open = group_record(group_id, "datasets/open");
    let mut records = vec![secret.clone(), open.clone()];
    records.sort_unstable_by_key(|record| record.document_id);

    let rules = HashMap::from([(
        group_id,
        read_rules(&[
            (
                format!("/{realm}/g/{group_id}/meta/**").as_str(),
                Permission::READ,
            ),
            (secret.permission_path.as_str(), Permission::DENY),
        ]),
    )]);
    let scope = GraphVisibilityScope {
        records: Arc::new(records),
        permissions: GroupPermissionRules::from_groups(Some(realm), rules),
        lifecycle_visibility: LifecycleVisibility::Cache(HashSet::new()),
    };

    let cache = MetadataVisibilityCache::new();
    assert!(!scope.graph_visible(&cache, &secret.graph_iri));
    assert!(scope.graph_visible(&cache, &open.graph_iri));
}
#[test]
fn narrow_grant_visible() {
    // A grant on one document shows it without opening the whole group.
    let realm = RealmId([7u8; 32]);
    let group_id = Ulid::generate();
    let granted = group_record(group_id, "datasets/shared");
    let hidden = group_record(group_id, "datasets/hidden");
    let mut records = vec![granted.clone(), hidden.clone()];
    records.sort_unstable_by_key(|record| record.document_id);

    let rules = HashMap::from([(
        group_id,
        read_rules(&[(granted.permission_path.as_str(), Permission::READ)]),
    )]);
    let scope = GraphVisibilityScope {
        records: Arc::new(records),
        permissions: GroupPermissionRules::from_groups(Some(realm), rules),
        lifecycle_visibility: LifecycleVisibility::Cache(HashSet::new()),
    };

    let cache = MetadataVisibilityCache::new();
    assert!(scope.graph_visible(&cache, &granted.graph_iri));
    assert!(!scope.graph_visible(&cache, &hidden.graph_iri));
}
fn open_policy() -> GraphPolicy {
    GraphPolicy {
        public: true,
        permission_paths: Vec::new(),
    }
}
fn admits(authorizer: &ScopeAuthorizer<'_>, graph_iri: &str) -> bool {
    authorizer
        .authorize(&GraphId::new(graph_iri), &open_policy(), CraqleAction::Read)
        .is_ok()
}
#[test]
fn authorizer_admits_public() {
    let public = registry_record("datasets/public");
    let scope = scope_for(vec![public.clone()], GroupPermissionRules::default());
    let cache = MetadataVisibilityCache::new();
    let authorizer = ScopeAuthorizer {
        scope: &scope,
        visibility_cache: &cache,
    };

    assert!(admits(&authorizer, &public.graph_iri));
    assert!(
        authorizer
            .authorize(
                &GraphId::new(&public.graph_iri),
                &open_policy(),
                CraqleAction::Write,
            )
            .is_err()
    );
}
#[test]
fn authorizer_refuses_denied() {
    // A per-document DENY under a group-wide grant hides only that document.
    let realm = RealmId([7u8; 32]);
    let group_id = Ulid::generate();
    let secret = group_record(group_id, "datasets/secret");
    let open = group_record(group_id, "datasets/open");
    let rules = HashMap::from([(
        group_id,
        read_rules(&[
            (
                format!("/{realm}/g/{group_id}/meta/**").as_str(),
                Permission::READ,
            ),
            (secret.permission_path.as_str(), Permission::DENY),
        ]),
    )]);
    let scope = scope_for(
        vec![secret.clone(), open.clone()],
        GroupPermissionRules::from_groups(Some(realm), rules),
    );
    let cache = MetadataVisibilityCache::new();
    let authorizer = ScopeAuthorizer {
        scope: &scope,
        visibility_cache: &cache,
    };

    assert!(!admits(&authorizer, &secret.graph_iri));
    assert!(admits(&authorizer, &open.graph_iri));
}
#[test]
fn authorizer_admits_narrow() {
    // A grant on one document shows it without opening the whole group.
    let realm = RealmId([7u8; 32]);
    let group_id = Ulid::generate();
    let granted = group_record(group_id, "datasets/shared");
    let hidden = group_record(group_id, "datasets/hidden");
    let rules = HashMap::from([(
        group_id,
        read_rules(&[(granted.permission_path.as_str(), Permission::READ)]),
    )]);
    let scope = scope_for(
        vec![granted.clone(), hidden.clone()],
        GroupPermissionRules::from_groups(Some(realm), rules),
    );
    let cache = MetadataVisibilityCache::new();
    let authorizer = ScopeAuthorizer {
        scope: &scope,
        visibility_cache: &cache,
    };

    assert!(admits(&authorizer, &granted.graph_iri));
    assert!(!admits(&authorizer, &hidden.graph_iri));
}
#[test]
fn authorizer_refuses_deleted() {
    // Craqle still holds the graph, so only our tombstone can hide it.
    let deleted = registry_record("datasets/deleted");
    let scope = scope_for(vec![deleted.clone()], GroupPermissionRules::default());
    let cache = MetadataVisibilityCache::new();
    cache.store_lifecycle_deleted(deleted.graph_iri.clone(), true);
    let authorizer = ScopeAuthorizer {
        scope: &scope,
        visibility_cache: &cache,
    };

    assert!(!admits(&authorizer, &deleted.graph_iri));
}
#[test]
fn authorizer_refuses_unlisted() {
    // Craqle may hold graphs the registry does not list; they stay hidden.
    let known = registry_record("datasets/known");
    let scope = scope_for(vec![known], GroupPermissionRules::default());
    let cache = MetadataVisibilityCache::new();
    let authorizer = ScopeAuthorizer {
        scope: &scope,
        visibility_cache: &cache,
    };

    assert!(!admits(
        &authorizer,
        &MetadataRegistryRecord::graph_iri_for(Ulid::generate())
    ));
}
