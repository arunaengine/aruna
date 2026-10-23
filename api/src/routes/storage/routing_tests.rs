//! Tests routing rule routes for round trips, admin checks and rejected rule targets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::openapi::ApiDoc;
use crate::tests::storage_routing::setup_state;

fn class_rule(class: &str) -> RoutingRuleRequest {
    RoutingRuleRequest {
        key_prefix: "archive/".to_string(),
        exact: false,
        target: RoutingTargetRequest {
            backend_id: None,
            class: Some(class.to_string()),
        },
    }
}

#[tokio::test]
async fn bucket_rules_roundtrip() {
    let test = setup_state().await;

    let Json(stored) = put_bucket_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.bucket.clone()),
        Json(BucketRoutingRequest {
            rules: vec![class_rule("cold")],
        }),
    )
    .await
    .unwrap();

    assert_eq!(stored.rules, vec![class_rule("cold")]);
    // The node offers no cold backend, so the rule is stored with a warning.
    assert_eq!(stored.warnings.len(), 1);

    let Json(fetched) = get_bucket_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.bucket.clone()),
    )
    .await
    .unwrap();

    assert_eq!(fetched.rules, stored.rules);
}

#[tokio::test]
async fn rejects_operator_backend() {
    let test = setup_state().await;

    let result = put_bucket_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.bucket.clone()),
        Json(BucketRoutingRequest {
            rules: vec![RoutingRuleRequest {
                key_prefix: String::new(),
                exact: false,
                target: RoutingTargetRequest {
                    backend_id: Some("cold".to_string()),
                    class: None,
                },
            }],
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::BadRequest)));
}

#[tokio::test]
async fn rejects_non_admin() {
    let test = setup_state().await;

    let result = put_bucket_routing(
        State(test.state.clone()),
        Extension(Some(test.other_auth.clone())),
        Path(test.bucket.clone()),
        Json(BucketRoutingRequest { rules: Vec::new() }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn group_default_roundtrip() {
    let test = setup_state().await;

    let Json(empty) = get_group_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(empty.default_target, None);

    let Json(stored) = put_group_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
        Json(GroupRoutingRequest {
            bucket: None,
            bucket_template: None,
            default_target: Some(RoutingTargetRequest {
                backend_id: None,
                class: Some("cold".to_string()),
            }),
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        stored
            .default_target
            .and_then(|target| target.class)
            .as_deref(),
        Some("cold")
    );

    let Json(cleared) = put_group_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
        Json(GroupRoutingRequest {
            bucket: None,
            bucket_template: None,
            default_target: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(cleared.default_target, None);
}

#[tokio::test]
async fn rejects_invalid_class() {
    let test = setup_state().await;

    let result = put_group_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
        Json(GroupRoutingRequest {
            bucket: None,
            bucket_template: None,
            default_target: Some(RoutingTargetRequest {
                backend_id: None,
                class: Some("NOT VALID".to_string()),
            }),
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::BadRequestReason(_))));
}

#[tokio::test]
async fn bucket_needs_storage() {
    // A default bucket only means something on one of the group's storages.
    let test = setup_state().await;

    let result = put_group_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
        Json(GroupRoutingRequest {
            bucket: Some("lab-default".to_string()),
            bucket_template: None,
            default_target: Some(RoutingTargetRequest {
                backend_id: None,
                class: Some("cold".to_string()),
            }),
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::BadRequestMessage(_))));

    // A bucket template too.
    let result = put_group_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
        Json(GroupRoutingRequest {
            bucket: None,
            bucket_template: Some("aruna-{bucket}".to_string()),
            default_target: Some(RoutingTargetRequest {
                backend_id: None,
                class: Some("cold".to_string()),
            }),
        }),
    )
    .await;
    assert!(matches!(result, Err(ServerError::BadRequestMessage(_))));
}

#[tokio::test]
async fn rejects_foreign_backend() {
    // Nothing registered this id for the group, so the rule must not store.
    let test = setup_state().await;
    let foreign = Ulid::generate();

    let result = put_bucket_routing(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.bucket.clone()),
        Json(BucketRoutingRequest {
            rules: vec![RoutingRuleRequest {
                key_prefix: String::new(),
                exact: false,
                target: RoutingTargetRequest {
                    backend_id: Some(foreign.to_string()),
                    class: None,
                },
            }],
        }),
    )
    .await;

    let Err(ServerError::BadRequestReason(reason)) = result else {
        panic!("expected a 400 naming the backend, got {result:?}")
    };
    assert!(reason.contains(&foreign.to_string()), "{reason}");
}

#[test]
fn openapi_lists_routes() {
    let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();

    assert!(openapi["paths"]["/data/buckets/{bucket}/storage/routing"]["put"].is_object());
    assert!(openapi["paths"]["/data/groups/{group_id}/storage/routing"]["put"].is_object());
    assert!(
        openapi["components"]["schemas"]["BucketRoutingResponse"]["properties"]
            .get("warnings")
            .is_some()
    );
}

#[tokio::test]
async fn lists_readable_buckets() {
    let test = setup_state().await;
    let list = |auth: AuthContext| {
        list_group_buckets(
            State(test.state.clone()),
            Extension(Some(auth)),
            Path(test.group_id.to_string()),
            axum::extract::Query(BucketPageQuery {
                limit: Some(1),
                after: None,
            }),
        )
    };
    let Json(page) = list(test.auth.clone()).await.unwrap();
    let names: Vec<_> = page
        .buckets
        .iter()
        .map(|found| found.bucket.as_str())
        .collect();
    assert_eq!(names, [test.bucket.as_str()]);
    // A token for the admin path alone sees no bucket it may not read.
    let mut admin_only = test.auth.clone();
    admin_only.path_restrictions =
        Some(vec![aruna_core::structs::identity::auth::PathRestriction {
            pattern: format!(
                "/{}/g/{}/admin/**",
                test.state.get_realm_id(),
                test.group_id
            ),
            permission: Permission::WRITE,
        }]);
    let Json(page) = list(admin_only).await.unwrap();
    // The cursor never names a bucket the caller may not see.
    assert!(page.buckets.is_empty() && page.next.is_none());
}

#[tokio::test]
async fn lists_bucket_jobs() {
    use aruna_core::keyspaces::{
        COMPRESSION_MIGRATION_KEYSPACE, RENAME_MOVE_KEYSPACE, RENAME_TOTAL_KEYSPACE,
        S3_BUCKET_KEYSPACE,
    };
    use aruna_core::structs::storage::backend_import::{RenameRequest, RenameTotal, TokenGrant};
    use aruna_core::structs::storage::blob::BucketInfo;
    use aruna_core::structs::storage::format::{Compression, CompressionMigration};
    let test = setup_state().await;
    let context = test.state.get_ctx();
    let key = |bucket: &str| bucket.as_bytes().to_vec().into();
    let quiet = BucketInfo {
        group_id: test.group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: test.auth.user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
    };
    let quiet = quiet.to_bytes().unwrap().into();
    crate::tests::routes::write_doc(&context, S3_BUCKET_KEYSPACE, key("quiet"), quiet).await;
    // A paused rename: the move record and the rename request of the same move.
    let mut record = CompressionMigration::new(Compression::Off, 7);
    (
        record.moving,
        record.migrated,
        record.skipped,
        record.failed,
    ) = (true, 4, 1, 2);
    let request = RenameRequest {
        started_at_ms: 7,
        auth: test.auth.clone(),
        node_id: test.state.get_node_id(),
        grant: TokenGrant {
            token_hash: "token".to_string(),
            issuer: test.state.get_realm_id(),
            issued_at_secs: 1,
            expires_at_secs: u64::MAX,
            via: None,
        },
        paused: true,
        aborted_at_ms: None,
    };
    let rows = [
        (COMPRESSION_MIGRATION_KEYSPACE, record.to_bytes().unwrap()),
        (RENAME_MOVE_KEYSPACE, request.to_bytes().unwrap()),
    ];
    for (space, value) in rows {
        crate::tests::routes::write_doc(&context, space, key(&test.bucket), value.into()).await;
    }
    let list = || async {
        let query = BucketPageQuery {
            limit: None,
            after: None,
        };
        let listed = list_group_buckets(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.group_id.to_string()),
            axum::extract::Query(query),
        );
        serde_json::to_value(listed.await.unwrap().0).unwrap()
    };
    let page = list().await;
    assert_eq!(page["buckets"][0]["bucket"], "quiet");
    assert!(page["buckets"][0].get("job").is_none());
    assert_eq!(page["buckets"][1]["bucket"], test.bucket.as_str());
    assert_eq!(
        page["buckets"][1]["job"],
        serde_json::json!({"kind": "rename", "state": "paused", "done": 5, "failed": 2})
    );
    // A total stored for an older move stays unknown; the move's own shows.
    for (started_at_ms, shown) in [(6, None), (7, Some(9))] {
        let total = RenameTotal {
            started_at_ms,
            total: 9,
        };
        let row = total.to_bytes().unwrap().into();
        crate::tests::routes::write_doc(&context, RENAME_TOTAL_KEYSPACE, key(&test.bucket), row)
            .await;
        let page = list().await;
        let total = page["buckets"][1]["job"].get("total");
        assert_eq!(total.and_then(serde_json::Value::as_u64), shown);
    }
}

#[tokio::test]
async fn lists_unrouted_import() {
    // A running import shows also for a rule whose write storage is gone from routing.
    use aruna_core::keyspaces::{BACKEND_IMPORT_KEYSPACE, S3_BUCKET_KEYSPACE};
    use aruna_core::structs::execution::job::{JobId, JobPayload, JobRecord};
    use aruna_core::structs::storage::backend_import::{BackendImport, BackendImportSpec};
    use aruna_core::structs::storage::blob::BucketInfo;
    use aruna_core::structs::storage::format::Compression;
    let test = setup_state().await;
    let context = test.state.get_ctx();
    let info = BucketInfo {
        group_id: test.group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: test.auth.user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
    };
    let row = info.to_bytes().unwrap().into();
    let bucket = b"linked".to_vec().into();
    crate::tests::routes::write_doc(&context, S3_BUCKET_KEYSPACE, bucket, row).await;
    let job_id = JobId::from_bytes([7; 16]);
    let rule = BackendImport {
        import_id: ulid::Ulid::from_parts(2, 2),
        group_id: test.group_id,
        backend_id: ulid::Ulid::from_parts(3, 3),
        backend_prefix: "raw/".to_string(),
        bucket: "linked".to_string(),
        key_prefix: String::new(),
        schedule: None,
        created_by: test.auth.user_id,
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
        last_job: Some(job_id),
        last_started_ms: Some(1),
        write_backend_id: None,
        write_prefix: None,
        write_pattern: aruna_core::structs::storage::backend_import::DEFAULT_PATTERN.to_string(),
    };
    let row = rule.to_bytes().unwrap().into();
    crate::tests::routes::write_doc(&context, BACKEND_IMPORT_KEYSPACE, rule.key(), row).await;
    let spec = BackendImportSpec {
        auth_context: test.auth.clone(),
        node_id: test.state.get_node_id(),
        group_id: test.group_id,
        import_id: rule.import_id,
    };
    let payload = JobPayload::BackendImport(spec);
    let (user, node) = (test.auth.user_id, test.state.get_node_id());
    let record = JobRecord::new(job_id, payload, user, node, 1, 1, None);
    let storage = &context.storage_handle;
    aruna_operations::jobs::store::insert_job(storage, &record)
        .await
        .unwrap();
    let query = BucketPageQuery {
        limit: None,
        after: None,
    };
    let listed = list_group_buckets(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
        axum::extract::Query(query),
    );
    let page = serde_json::to_value(listed.await.unwrap().0).unwrap();
    let linked = (page["buckets"].as_array().unwrap().iter())
        .find(|bucket| bucket["bucket"] == "linked")
        .unwrap();
    assert_eq!(linked["job"]["kind"], "import");
}
