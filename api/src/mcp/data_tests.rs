//! Tests key and cursor validation, error mapping, and time bucketing in the data tools.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

fn body(result: CallToolResult) -> serde_json::Value {
    assert_eq!(result.is_error, Some(true));
    result
        .structured_content
        .expect("a tool error carries the structured body")
}

#[test]
fn key_rejects_traversal() {
    let text = body(validate_key("../secret").unwrap_err());
    assert_eq!(text["code"], "Bad request");
    assert!(
        text["error"]
            .as_str()
            .unwrap_or_default()
            .contains("relative key")
    );
    assert!(validate_key("reads/sample.fastq.gz").is_ok());
}

#[test]
fn bounded_bytes_range() {
    assert_eq!(bounded_bytes(None).unwrap(), MAX_TEXT_BYTES);
    assert_eq!(bounded_bytes(Some(1024)).unwrap(), 1024);
    assert!(bounded_bytes(Some(0)).is_err());
    assert!(bounded_bytes(Some(MAX_TEXT_BYTES + 1)).is_err());
}

#[test]
fn object_error_forbidden() {
    let forbidden = body(object_error(crate::error::ServerError::Forbidden, "write"));
    assert!(
        forbidden["error"]
            .as_str()
            .unwrap_or_default()
            .contains("write permission")
    );
    assert_eq!(
        body(object_error(crate::error::ServerError::NotFound, "read"))["code"],
        "Not found"
    );
}

#[test]
fn bucket_error_maps() {
    assert!(
        body(map_bucket_error(GetBucketError::NotFound))["error"]
            .as_str()
            .unwrap_or_default()
            .contains("list_buckets")
    );
    assert_eq!(
        body(map_bucket_error(GetBucketError::Incomplete))["code"],
        "Internal error"
    );
}

#[test]
fn get_error_categories() {
    assert_eq!(
        body(map_get_error(GetObjectError::NoSuchKey))["code"],
        "Not found"
    );
    assert_eq!(
        body(map_get_error(GetObjectError::InvalidRange))["code"],
        "Bad request"
    );
    assert_eq!(
        body(map_get_error(GetObjectError::GovernedUnavailable))["code"],
        "Forbidden"
    );
    assert_eq!(
        body(map_get_error(GetObjectError::ReferenceSourceChanged))["code"],
        "Conflict"
    );
}

#[test]
fn put_error_categories() {
    assert_eq!(
        body(server_error(put_server_error(PutObjectError::MissingBody)))["code"],
        "Bad request"
    );
    assert_eq!(
        body(server_error(put_server_error(
            PutObjectError::QuotaExceeded {
                limit: 10,
                usage: 20
            }
        )))["code"],
        "Conflict"
    );
    assert_eq!(
        body(server_error(put_server_error(
            PutObjectError::PutObjectFailed
        )))["code"],
        "Internal error"
    );
}

fn sample(at: &str, bytes: u64) -> ObjectSample {
    ObjectSample {
        at: chrono::DateTime::parse_from_rfc3339(at)
            .expect("fixture timestamp")
            .with_timezone(&chrono::Utc),
        bytes,
    }
}

#[test]
fn weeks_start_monday() {
    // A Sunday belongs to the week that began on the preceding Monday.
    let samples = [
        sample("2026-01-04T23:59:59Z", 10),
        sample("2026-01-05T00:00:00Z", 20),
        sample("2026-01-11T12:00:00Z", 30),
    ];
    let folded = fold_buckets(&samples, BucketUnit::Week, 10);
    assert_eq!(
        folded.buckets,
        vec![
            TimeBucketOutput {
                start: "2025-12-29T00:00:00+00:00".to_string(),
                count: 1,
                bytes: 10,
            },
            TimeBucketOutput {
                start: "2026-01-05T00:00:00+00:00".to_string(),
                count: 2,
                bytes: 50,
            },
        ]
    );
    assert_eq!(folded.total_count, 3);
    assert_eq!(folded.total_bytes, 60);
    assert!(!folded.truncated);
}

#[test]
fn months_and_days() {
    let samples = [
        sample("2026-01-31T23:00:00Z", 1),
        sample("2026-02-01T00:00:00Z", 2),
    ];
    let months = fold_buckets(&samples, BucketUnit::Month, 10);
    assert_eq!(months.buckets.len(), 2);
    assert_eq!(months.buckets[0].start, "2026-01-01T00:00:00+00:00");
    assert_eq!(months.buckets[1].start, "2026-02-01T00:00:00+00:00");
    let days = fold_buckets(&samples, BucketUnit::Day, 10);
    assert_eq!(days.buckets[0].start, "2026-01-31T00:00:00+00:00");
}

#[test]
fn folds_empty_window() {
    let folded = fold_buckets(&[], BucketUnit::Day, 1);
    assert!(folded.buckets.is_empty());
    assert_eq!(folded.total_count, 0);
    assert!(!folded.truncated);
}

#[test]
fn caps_bucket_count() {
    // Beyond the cap the series is cut, but the totals still cover it.
    let samples = [
        sample("2026-01-01T00:00:00Z", 1),
        sample("2026-01-02T00:00:00Z", 2),
        sample("2026-01-03T00:00:00Z", 4),
    ];
    let folded = fold_buckets(&samples, BucketUnit::Day, 2);
    assert_eq!(folded.buckets.len(), 2);
    assert!(folded.truncated);
    assert_eq!(folded.total_count, 3);
    assert_eq!(folded.total_bytes, 7);
}

#[test]
fn bound_rejects_garbage() {
    assert!(parse_bound("since", Some("yesterday")).is_err());
    assert!(parse_bound("since", None).unwrap().is_none());
    assert!(
        parse_bound("until", Some("2026-01-01T00:00:00Z"))
            .unwrap()
            .is_some()
    );
}

#[test]
fn filenames_drop_prefix() {
    assert_eq!(filename_of("results/run-1/chart.png"), "chart.png");
    assert_eq!(filename_of("chart.png"), "chart.png");
    assert_eq!(filename_of("results/"), "results");
}

#[test]
fn head_error_categories() {
    assert_eq!(
        body(map_head_error(HeadObjectError::NoSuchKey))["code"],
        "Not found"
    );
    assert!(
        body(map_head_error(HeadObjectError::NoSuchVersion))["error"]
            .as_str()
            .unwrap_or_default()
            .contains("version_id")
    );
    assert_eq!(
        body(map_head_error(HeadObjectError::HeadObjectFailed))["code"],
        "Internal error"
    );
}

#[test]
fn search_kind_names() {
    assert_eq!(SearchKind::Documents.as_str(), "documents");
    assert_eq!(SearchKind::Buckets.as_str(), "buckets");
    assert_eq!(SearchKind::Groups.as_str(), "groups");
    assert_eq!(SearchKind::Users.as_str(), "users");
}

// A write refusal keeps its meaning for the caller: a bad body is the caller's to fix, a
// quota a conflict, a rebuild or a moved range retryable, everything else this node's fault.
#[test]
fn put_errors_mapped() {
    use crate::error::ServerError;
    use aruna_operations::distribution::guard::GuardError;
    for error in [
        PutObjectError::MissingBody,
        PutObjectError::IncompleteBody,
        PutObjectError::Unmapped,
    ] {
        assert!(matches!(
            put_server_error(error),
            ServerError::BadRequestReason(_)
        ));
    }
    let quota = PutObjectError::QuotaExceeded { limit: 1, usage: 2 };
    assert!(matches!(put_server_error(quota), ServerError::Conflict(_)));
    let rebuilding = put_server_error(PutObjectError::UsageRebuilding);
    assert!(matches!(
        rebuilding,
        ServerError::ServiceUnavailableReason(_)
    ));
    let set_id = ulid::Ulid::from_bytes([3; 16]);
    let moved = put_server_error(PutObjectError::Distribution(GuardError::RecordMissing(
        set_id,
    )));
    assert!(!matches!(moved, ServerError::InternalError(_)));
    let taken = put_server_error(PutObjectError::ObjectExists);
    assert!(matches!(
        taken,
        ServerError::Refused(http::StatusCode::CONFLICT, "name_taken", _)
    ));
    for error in [
        PutObjectError::InvalidOperationState,
        PutObjectError::NoTransactionFound,
        PutObjectError::MissingOutput,
        PutObjectError::MissingHash("blake3".to_string()),
        PutObjectError::InvalidPreassignedVersion,
        PutObjectError::PutObjectFailed,
        PutObjectError::HeadChanged,
        PutObjectError::FenceTaken,
    ] {
        assert!(matches!(
            put_server_error(error),
            ServerError::InternalError(_)
        ));
    }
}

// A version held only by other members is this node's failure to read, never a missing key.
#[test]
fn held_read_internal() {
    let held = aruna_operations::distribution::pull::HeldElsewhere {
        set_id: ulid::Ulid::from_bytes([4; 16]),
        version_id: ulid::Ulid::from_bytes([5; 16]),
        identity: [6; 32],
        blake3: None,
        size: 1,
        metadata: HashMap::new(),
        version_created_at: None,
        source_policies: Vec::new(),
    };
    let error = body(map_get_error(GetObjectError::HeldElsewhere(Box::new(held))));
    assert_eq!(error["code"], "Internal error");
}

// An ordinary bucket's listing shows neither a link to a target the caller cannot read nor
// a key the caller's bearer denies, and no cursor names either of them.
#[tokio::test]
async fn lists_readable_objects() {
    use aruna_core::structs::identity::auth::PathRestriction;
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use rmcp::handler::server::wrapper::Parameters;
    let node = crate::tests::distributed::node_state().await;
    let bucket = crate::tests::distributed::BUCKET;
    let info = crate::routes::storage::deletion::bucket_info(&node.state, bucket)
        .await
        .unwrap();
    for key in ["a.md", "secret.md", "z.md"] {
        let input = WriteObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            text: "x".to_string(),
            content_type: None,
        };
        store_text(&node.state, &node.owner, &info, input, Default::default())
            .await
            .unwrap();
    }
    crate::tests::distributed::hidden_link(&node, "link.md").await;
    let state = &node.state;
    let root = bucket_permission_path(
        state.get_realm_id(),
        node.group_id,
        state.get_node_id(),
        bucket,
    );
    let rule = |pattern: String, permission| PathRestriction {
        pattern,
        permission,
    };
    let auth = AuthContext {
        path_restrictions: Some(vec![
            rule(root.clone(), Permission::READ),
            rule(format!("{root}/**"), Permission::READ),
            rule(format!("{root}/secret.md"), Permission::DENY),
        ]),
        ..node.owner.clone()
    };
    let server = McpServer::new(node.state.clone());
    let (mut cursor, mut keys) = (None, Vec::new());
    for _ in 0..6 {
        let (mut parts, ()) = http::Request::new(()).into_parts();
        parts.extensions.insert(Some(auth.clone()));
        parts
            .extensions
            .insert(None::<crate::auth::ValidatedBearer>);
        let input = ListObjectsInput {
            bucket: bucket.to_string(),
            prefix: None,
            cursor: cursor.take(),
            limit: Some(2),
        };
        let listed = server
            .list_objects(Extension(parts), Parameters(input))
            .await
            .unwrap()
            .0;
        keys.extend(listed.objects.iter().map(|object| object.key.clone()));
        cursor = listed.next_cursor;
        let Some(cursor) = &cursor else { break };
        let bytes = URL_SAFE_NO_PAD.decode(cursor).unwrap_or_default();
        for hidden in [&b"secret"[..], b"link"] {
            assert!(!bytes.windows(hidden.len()).any(|window| window == hidden));
        }
    }
    assert_eq!(keys, vec!["a.md", "z.md"]);
}

mod distributed {
    //! The data tools on a member of a distributed bucket.
    use super::*;
    use crate::tests::members::{BUCKET, Members, members};
    use aruna_core::structs::identity::auth::NodeCapabilities;
    use rmcp::handler::server::wrapper::Parameters;

    async fn server(members: &Members, on: usize) -> McpServer {
        let node = &members.nodes[on];
        let capabilities = NodeCapabilities::user_node(members.realm_id).unwrap();
        let state = crate::tests::routes::test_state(
            node.context.clone(),
            members.realm_id,
            node.node_id(),
            capabilities,
        )
        .await;
        McpServer::new(Arc::new(state))
    }

    fn parts(members: &Members) -> http::request::Parts {
        let auth = AuthContext {
            user_id: members.user.user_identity,
            realm_id: members.realm_id,
            path_restrictions: None,
            session: None,
        };
        let (mut parts, ()) = http::Request::new(()).into_parts();
        parts.extensions.insert(Some(auth));
        parts
            .extensions
            .insert(None::<crate::auth::ValidatedBearer>);
        parts
    }

    fn write(key: &str) -> WriteObjectInput {
        WriteObjectInput {
            bucket: BUCKET.to_string(),
            key: key.to_string(),
            text: "noted".to_string(),
            content_type: None,
        }
    }

    /// Writes a link at `key` on the first member to a target in a group the user cannot read.
    async fn hidden_link(members: &Members, key: &str) {
        use aruna_core::structs::storage::link::LinkTarget;
        use aruna_core::structs::storage::replication::VersionedObjectArn;
        use aruna_operations::staging::reference::{ReferenceWrite, write_reference_version};
        let node = &members.nodes[0];
        let target = LinkTarget {
            arn: VersionedObjectArn {
                realm_id: members.realm_id,
                node_id: node.node_id(),
                bucket: "other".to_string(),
                key: "x".to_string(),
                version: ulid::Ulid::from_parts(5, 5),
            },
            group_id: ulid::Ulid::from_parts(9, 9),
        };
        let write = ReferenceWrite {
            job_id: None,
            preassigned_version_id: None,
            group_id: members.group_id,
            user_id: members.user.user_identity,
            realm_id: members.realm_id,
            node_id: node.node_id(),
            bucket: BUCKET.to_string(),
            key: key.to_string(),
            expected_bucket: None,
            version_source: target.binding(),
            metadata: aruna_core::structs::execution::source_access::SourceMetadata {
                content_length: 1,
                content_type: None,
                etag: Some(target.arn.version.to_string()),
                last_modified: None,
                source_version: None,
                digests: Default::default(),
            },
            inherited_policies: Vec::new(),
            connector_guard: None,
            restrictions: None,
            mirror_guards: Vec::new(),
        };
        write_reference_version(&node.context, write).await.unwrap();
    }

    // A text write to a key this member coordinates is stored here as a new version, and the
    // listing of the range it keeps shows it.
    #[tokio::test]
    async fn writes_and_lists() {
        let members = members(3, &["h", "p"], 0).await;
        let server = server(&members, 0).await;
        let written = server
            .write_object(Extension(parts(&members)), Parameters(write("a/note")))
            .await
            .unwrap()
            .0;
        assert_eq!((written.key.as_str(), written.size), ("a/note", 5));
        // A link to a target the caller cannot read stays out of the range's listing.
        hidden_link(&members, "a/secret").await;
        let input = ListObjectsInput {
            bucket: BUCKET.to_string(),
            prefix: Some("a/".to_string()),
            cursor: None,
            limit: None,
        };
        let listed = server
            .list_objects(Extension(parts(&members)), Parameters(input))
            .await
            .unwrap()
            .0;
        let keys: Vec<&str> = listed
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect();
        assert_eq!(keys, ["a/note"]);
        assert_eq!(listed.objects[0].size, Some(5));
    }

    // A text write to a key another member coordinates is relayed under the caller's own
    // bearer token, which that member checks; without a token, or while the member cannot be
    // reached, the write fails and nothing is stored here.
    #[tokio::test]
    async fn relay_needs_coordinator() {
        let members = members(3, &["h", "p"], 0).await;
        let server = server(&members, 0).await;
        let mut bearer = parts(&members);
        let token = crate::auth::ValidatedBearer::new_for_test("caller-token");
        bearer.extensions.insert(Some(token));
        for (parts, code) in [
            (parts(&members), "Not authorized"),
            (bearer, "Service unavailable"),
        ] {
            let written = server
                .write_object(Extension(parts), Parameters(write("k/note")))
                .await;
            let Err(refused) = written else {
                panic!("the write cannot reach its coordinator");
            };
            assert_eq!(super::body(refused)["code"], code);
        }
        let node = &members.nodes[0];
        let input = HeadObjectInput {
            bucket: BUCKET.to_string(),
            key: "k/note".to_string(),
            version_id: None,
        };
        let head = drive(HeadObjectOperation::new(input), &node.context).await;
        assert!(head.is_err(), "nothing is stored on the relaying member");
    }
}

fn caller(auth: &AuthContext) -> http::request::Parts {
    let (mut parts, ()) = http::Request::new(()).into_parts();
    parts.extensions.insert(Some(auth.clone()));
    parts
        .extensions
        .insert(None::<crate::auth::ValidatedBearer>);
    parts
}

// The bucket list shows what the bucket search shows: a public bucket of another group joins
// the caller's own, while a closed bucket or one a deny policy hides is left out, not refused.
#[tokio::test]
async fn lists_readable_buckets() {
    use crate::tests::distributed::{bucket_info, node_state, write};
    use crate::tests::routes::{open_paths, seed_group_docs};
    use aruna_core::keyspaces::{REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE};
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
    use rmcp::handler::server::wrapper::Parameters;
    let node = node_state().await;
    let (state, realm_id) = (&node.state, node.state.get_realm_id());
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: node.owner.user_id,
        realm_id,
    };
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(state.get_node_id(), RealmNodeKind::Server);
    config.request_policies = vec![RequestPolicy {
        policy_id: ulid::Ulid::from_parts(3, 3),
        name: "hide-bucket".to_string(),
        kind: PolicyKind::Deny,
        when: None,
        expression: "path.endsWith('/hidden')".to_string(),
        enabled: true,
    }];
    let config = config.to_bytes(&actor).unwrap();
    write(
        state,
        REALM_CONFIG_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        config,
    )
    .await;
    let other = ulid::Ulid::from_bytes([21; 16]);
    let stranger = Actor {
        user_id: node.stranger.user_id,
        ..actor
    };
    let context = state.get_ctx();
    seed_group_docs(
        &context,
        realm_id,
        &stranger,
        other,
        "other",
        stranger.user_id,
    )
    .await;
    for (bucket, group_id) in [
        ("hidden", node.group_id),
        ("open-data", other),
        ("closed-data", other),
    ] {
        let info = bucket_info(group_id, None).to_bytes().unwrap();
        write(state, S3_BUCKET_KEYSPACE, bucket.as_bytes().to_vec(), info).await;
    }
    let open = bucket_permission_path(realm_id, other, state.get_node_id(), "open-data");
    open_paths(&context, &stranger, other, &[open]).await;
    let server = McpServer::new(state.clone());
    for (auth, expected) in [
        (&node.owner, ["open-data", "routed"]),
        (&node.stranger, ["closed-data", "open-data"]),
    ] {
        let input = Parameters(ListBucketsInput::default());
        let listed = server.list_buckets(Extension(caller(auth)), input).await;
        let listed = listed.unwrap().0;
        let names: Vec<&str> = listed
            .buckets
            .iter()
            .map(|hit| hit.bucket.as_str())
            .collect();
        assert_eq!(names, expected);
        assert_eq!(listed.buckets[0].node_id, state.get_node_id().to_string());
    }
}
