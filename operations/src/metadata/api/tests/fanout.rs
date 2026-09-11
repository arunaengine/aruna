use super::list::{auth_for, user_role, write_policy_docs};
use super::*;

// Fan-out follows the live holders of the stored bucket; the event-time
// holder stamp on the record is ignored, and no config means local only.
#[test]
fn query_fans_out_to_holders() {
    let local_node_id = iroh::SecretKey::from_bytes(&[21u8; 32]).public();
    let remote_node_id = iroh::SecretKey::from_bytes(&[22u8; 32]).public();
    let stale_node_id = iroh::SecretKey::from_bytes(&[23u8; 32]).public();
    let realm_id = RealmId([3u8; 32]);
    let document_id = Ulid::generate();
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
    config.seed_default_placement();
    config.ensure_node(local_node_id, aruna_core::structs::RealmNodeKind::Server);
    config.ensure_node(remote_node_id, aruna_core::structs::RealmNodeKind::Server);
    let strategy = config
        .strategy(&config.default_strategy_id.expect("default strategy"))
        .expect("default strategy resolves");
    let placement = crate::placement::choose_origin_bucket(
        &config,
        strategy,
        local_node_id,
        &document_id.to_bytes(),
    )
    .expect("origin holds a bucket");

    let record = MetadataRegistryRecord {
        realm_id,
        group_id: Ulid::generate(),
        document_id,
        document_path: "datasets/query-targets".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: "/metadata/query-targets".to_string(),
        placement,
        holder_node_ids: vec![stale_node_id],
        created_at_ms: 0,
        updated_at_ms: 0,
        establishing_event_id: Ulid::nil(),
        last_event_id: Ulid::nil(),
    };

    let nodes = document_replica_query_nodes(Some(&config), &record, local_node_id);
    assert_eq!(nodes.len(), 2);
    assert!(nodes.contains(&local_node_id) && nodes.contains(&remote_node_id));
    assert!(!nodes.contains(&stale_node_id));

    assert_eq!(
        document_replica_query_nodes(None, &record, local_node_id),
        vec![local_node_id]
    );
}

#[test]
fn fanout_filters_nodes() {
    let server = iroh::SecretKey::from_bytes(&[24u8; 32]).public();
    let user = iroh::SecretKey::from_bytes(&[25u8; 32]).public();
    let unknown = iroh::SecretKey::from_bytes(&[26u8; 32]).public();
    let realm_id = RealmId([4u8; 32]);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
    config.ensure_node(server, RealmNodeKind::Server);
    config.ensure_node(
        user,
        RealmNodeKind::User {
            owner: UserId::nil(realm_id),
        },
    );

    let nodes = authorized_realm_nodes(&config, HashSet::from([server, user, unknown]))
        .expect("valid node ids");

    assert_eq!(nodes, HashSet::from([server]));
}

#[test]
fn deduplicate_fanout_nodes_preserves_first_seen_order() {
    let first = iroh::SecretKey::from_bytes(&[31u8; 32]).public();
    let second = iroh::SecretKey::from_bytes(&[32u8; 32]).public();
    let third = iroh::SecretKey::from_bytes(&[33u8; 32]).public();

    assert_eq!(
        deduplicate_fanout_nodes(vec![first, second, first, third, second]),
        vec![first, second, third]
    );
}

#[test]
fn rejects_cursor_tampering() {
    let secret = iroh::SecretKey::from_bytes(&[34u8; 32]);
    let signer = secret.public();
    let receiver = iroh::SecretKey::from_bytes(&[36u8; 32]).public();
    let fingerprint = [35u8; 32];
    let mut cursor = ObjectSearchCursor::new_signed(
        fingerprint,
        SystemTime::UNIX_EPOCH,
        &[ObjectSearchPartitionState {
            node_id: signer,
            start_after: None,
            exhausted: false,
            observed_at: Some(SystemTime::UNIX_EPOCH),
        }],
        &[],
        false,
        0,
        signer,
        |bytes| secret.sign(bytes),
    )
    .expect("object search cursor signs");

    assert!(
        ObjectSearchCursor::decode(&cursor.encode().unwrap(), fingerprint, &[receiver, signer])
            .is_ok()
    );
    assert!(matches!(
        ObjectSearchCursor::decode(&cursor.encode().unwrap(), fingerprint, &[receiver]),
        Err(MetadataApiError::InvalidCursor(_))
    ));
    cursor.payload.omitted_partitions = 1;
    assert!(matches!(
        ObjectSearchCursor::decode(&cursor.encode().unwrap(), fingerprint, &[signer]),
        Err(MetadataApiError::InvalidCursor(_))
    ));
}

#[tokio::test]
async fn write_policy_denies() {
    // The policy must be evaluated with the permission the caller asked for,
    // so a write-deny policy cannot be bypassed by a fixed read request.
    let test = metadata_test();
    let group_id = Ulid::generate();
    let user = UserId::local(Ulid::generate(), TEST_REALM_ID);
    let path = format!("/{TEST_REALM_ID}/g/{group_id}/data/object");
    write_policy_docs(
        &test,
        group_id,
        user_role(
            user,
            HashMap::from([(
                format!("/{TEST_REALM_ID}/g/{group_id}/**"),
                Permission::WRITE,
            )]),
        ),
        vec![aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::generate(),
            name: "no-writes".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: "permission == 'write'".to_string(),
            enabled: true,
        }],
    )
    .await;

    let denied = ensure_permission(
        &test.context,
        TEST_REALM_ID,
        auth_for(user),
        group_id,
        path.clone(),
        Permission::WRITE,
        None,
    )
    .await;
    // The same role allows the read, so the denial comes from the policy.
    let allowed = ensure_permission(
        &test.context,
        TEST_REALM_ID,
        auth_for(user),
        group_id,
        path,
        Permission::READ,
        None,
    )
    .await;

    assert!(matches!(denied, Err(MetadataApiError::Forbidden)));
    assert!(allowed.is_ok());
}

#[test]
fn bearer_limits() {
    assert!(matches!(
        forwarded_bearer(Some(&"x".repeat(4096))),
        Ok(Some(MetadataAuthToken::Bearer(_)))
    ));
    assert!(matches!(
        forwarded_bearer(Some(&"x".repeat(4097))),
        Err(MetadataApiError::BadRequest)
    ));
    assert!(fanout_bearer(Some(&"x".repeat(4097))).is_none());
    assert!(matches!(forwarded_bearer(None), Ok(None)));
}
