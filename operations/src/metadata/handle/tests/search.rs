use super::visibility::{group_record, read_rules, registry_record};
use super::*;
use aruna_core::structs::Permission;
use craqle::{Action as CraqleAction, Authorizer as CraqleAuthorizer};
use craqle::GraphPolicy;
use crate::auth::permission_rules::GroupPermissionRules;
use crate::metadata::handle::search::clamp_remote_limit;
use crate::metadata::search_cursor::METADATA_SEARCH_MAX_PAGINATION_DEPTH;
use oxrdf::Literal;
use tracing::Span;
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
// Permissive on purpose: craqle's stored policy must not sway the decision.
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
#[tokio::test]
async fn enrichment_keeps_order() {
    // The gate forces the describes to complete in exactly reverse order;
    // the properties must still line up with the targets that asked for them.
    let count = METADATA_ENRICH_TASKS;
    let gate = Arc::new((Mutex::new(count - 1), std::sync::Condvar::new()));
    let describe_gate = gate.clone();
    let describe: HitDescribe = Arc::new(move |_graph_iri, subject_iri: &str| {
        let index = subject_iri
            .rsplit('/')
            .next()
            .and_then(|tail| tail.parse::<usize>().ok())
            .expect("indexed subject");
        let (turn, ready) = &*describe_gate;
        let mut turn = turn.lock().expect("gate lock");
        // Bounded so a lost wakeup fails the assertion instead of hanging.
        while *turn != index {
            let (guard, wait) = ready
                .wait_timeout(turn, Duration::from_secs(30))
                .expect("gate lock");
            turn = guard;
            if wait.timed_out() {
                break;
            }
        }
        *turn = index.wrapping_sub(1);
        ready.notify_all();
        vec![(
            "urn:test:index".to_string(),
            Term::Literal(Literal::new_simple_literal(index.to_string())),
        )]
    });

    let targets = (0..count)
        .map(|index| ("urn:graph".to_string(), format!("urn:subject/{index}")))
        .collect::<Vec<_>>();
    let permits = Arc::new(tokio::sync::Semaphore::new(count));
    let properties = describe_hits_parallel(&permits, targets, describe, &Span::none()).await;

    let expected = (0..count)
        .map(|index| {
            vec![(
                "urn:test:index".to_string(),
                Term::Literal(Literal::new_simple_literal(index.to_string())),
            )]
        })
        .collect::<Vec<_>>();
    assert_eq!(properties, expected);
}
fn scope_for(
    mut records: Vec<MetadataRegistryRecord>,
    permissions: GroupPermissionRules,
) -> GraphVisibilityScope {
    records.sort_unstable_by_key(|record| record.document_id);
    GraphVisibilityScope {
        records: Arc::new(records),
        permissions,
        lifecycle_visibility: LifecycleVisibility::Cache(HashSet::new()),
    }
}
#[test]
fn selection_excludes_deleted() {
    let live = registry_record("datasets/live");
    let deleted = registry_record("datasets/deleted");
    let cache = MetadataVisibilityCache::new();
    cache.store_lifecycle_deleted(deleted.graph_iri.clone(), true);
    let scope = scope_for(
        vec![live.clone(), deleted.clone()],
        GroupPermissionRules::default(),
    );

    let selection = select_visible_records(&scope, &cache);

    assert_eq!(selection.deleted, 1);
    assert_eq!(
        selection
            .visible
            .iter()
            .map(|record| record.graph_iri.clone())
            .collect::<Vec<_>>(),
        vec![live.graph_iri]
    );
}
#[test]
fn selection_excludes_denied() {
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

    let selection = select_visible_records(&scope, &MetadataVisibilityCache::new());

    assert_eq!(selection.denied, 1);
    assert_eq!(selection.private, 2);
    assert_eq!(selection.visible.len(), 1);
    assert_eq!(selection.visible[0].graph_iri, open.graph_iri);
}
#[test]
fn narrow_grant_selected() {
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

    let selection = select_visible_records(&scope, &MetadataVisibilityCache::new());

    assert_eq!(selection.visible.len(), 1);
    assert_eq!(selection.visible[0].graph_iri, granted.graph_iri);
    assert_eq!(selection.denied, 1);
}
#[test]
fn anonymous_sees_public() {
    // Anonymous callers hold no rules, so only public records survive.
    let public = registry_record("datasets/public");
    let private = group_record(Ulid::generate(), "datasets/private");
    let scope = scope_for(
        vec![public.clone(), private.clone()],
        GroupPermissionRules::default(),
    );

    let selection = select_visible_records(&scope, &MetadataVisibilityCache::new());

    assert_eq!(selection.public, 1);
    assert_eq!(selection.denied, 1);
    assert_eq!(selection.visible.len(), 1);
    assert_eq!(selection.visible[0].graph_iri, public.graph_iri);
}
#[test]
fn filters_restrict_candidates() {
    let group_id = Ulid::generate();
    let wanted = group_record(group_id, "datasets/wanted");
    let same_group = group_record(group_id, "datasets/sibling");
    let other_group = group_record(Ulid::generate(), "datasets/other");
    let records = Arc::new(vec![
        wanted.clone(),
        same_group.clone(),
        other_group.clone(),
    ]);

    let by_graph = filter_candidate_records(
        records.clone(),
        Some(&HashSet::from([wanted.graph_iri.clone()])),
        None,
    );
    assert_eq!(by_graph.len(), 1);
    assert_eq!(by_graph[0].graph_iri, wanted.graph_iri);

    let by_group = filter_candidate_records(records.clone(), None, Some(group_id));
    assert_eq!(by_group.len(), 2);
    assert!(by_group.iter().all(|record| record.group_id == group_id));

    let combined = filter_candidate_records(
        records.clone(),
        Some(&HashSet::from([
            wanted.graph_iri.clone(),
            other_group.graph_iri.clone(),
        ])),
        Some(group_id),
    );
    assert_eq!(combined.len(), 1);
    assert_eq!(combined[0].graph_iri, wanted.graph_iri);

    assert_eq!(filter_candidate_records(records, None, None).len(), 3);
}
#[test]
fn rejected_lifecycle_refresh() {
    let deleted_record = registry_record("datasets/deleted-race");
    let cache = MetadataVisibilityCache::new();
    cache.store_lifecycle_deleted(deleted_record.graph_iri.clone(), false);
    let scope = GraphVisibilityScope {
        records: Arc::new(vec![deleted_record.clone()]),
        permissions: GroupPermissionRules::default(),
        lifecycle_visibility: LifecycleVisibility::FreshDeletedGraphs(HashSet::from([
            deleted_record.graph_iri.clone(),
        ])),
    };

    assert!(!scope.graph_visible(&cache, &deleted_record.graph_iri));
}
#[test]
fn lifecycle_refresh_restamps() {
    let cache = MetadataVisibilityCache::new();
    cache.store_lifecycle_deleted("urn:graph:kept".to_string(), true);
    cache.store_lifecycle_deleted("urn:graph:gone".to_string(), false);
    cache.expire_now();
    cache.store_lifecycle_deleted("urn:graph:fresh".to_string(), false);

    cache.refresh_lifecycle_deleted(vec![("urn:graph:kept".to_string(), false)]);

    assert_eq!(cache.lifecycle_deleted("urn:graph:kept"), Some(false));
    assert_eq!(cache.lifecycle_deleted_any("urn:graph:gone"), None);
    assert_eq!(cache.lifecycle_deleted("urn:graph:fresh"), Some(false));
}

#[test]
fn search_limit_clamps() {
    assert_eq!(clamp_remote_limit(0), 1);
    assert_eq!(clamp_remote_limit(25), 25);
    assert_eq!(
        clamp_remote_limit(METADATA_SEARCH_MAX_PAGINATION_DEPTH + 1),
        METADATA_SEARCH_MAX_PAGINATION_DEPTH
    );
}
