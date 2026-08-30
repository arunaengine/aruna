#![recursion_limit = "512"]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

use aruna_api::cors::CorsConfig;
use aruna_api::server::{DEFAULT_MAX_HTTP_BODY_SIZE, Server, ServerConfig};
use aruna_api::server_state::ServerState;
use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE, USER_KEYSPACE,
};
use aruna_core::request_policy::{PolicyKind, RequestPolicy};
use aruna_core::structs::{
    Actor, Backend, BackendConfig, BucketInfo, Group, GroupAuthorizationDocument, NodeCapabilities,
    RealmId, User,
};
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::claim_initial_realm_admin::{
    ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
};
use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::metadata::MetadataHandle;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use axum::http::{HeaderValue, header};
use byteview::ByteView;
use ed25519_dalek::SigningKey;
use rmcp::model::{CallToolRequestParams, ClientInfo, ProtocolVersion};
use rmcp::transport::{
    StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
};
use rmcp::{ClientLifecycleMode, ClientServiceExt};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

struct Fixture {
    _root: TempDir,
    state: Arc<ServerState>,
    actor: Actor,
    _group_id: Ulid,
    token: String,
    net: NetHandle,
}

async fn write_value(state: &ServerState, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    let event = state
        .get_ctx()
        .storage_handle
        .send_effect(aruna_core::effects::Effect::Storage(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(value),
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn setup_fixture() -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(root.path().to_str().unwrap()).unwrap();
    let net = NetHandle::new(NetConfig::default(), storage.clone())
        .await
        .unwrap();
    let node_id = net.node_id();
    let blob = BlobHandler::new(
        BackendConfig {
            backend_type: Backend::FileSystem,
            root: root.path().join("blobs").to_string_lossy().to_string(),
            service_config: HashMap::new(),
            bucket_prefix: Some("mcp_".to_string()),
            max_bucket_size: None,
            multipart_bucket: Some("multipart".to_string()),
            timeouts: Default::default(),
        },
        storage.clone(),
        net.clone(),
    )
    .await
    .unwrap();
    let metadata = MetadataHandle::new(
        root.path().join("metadata"),
        node_id,
        storage.clone(),
        None,
        None,
        None,
    )
    .unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: Some(blob),
        metadata_handle: Some(metadata),
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    });
    let realm_key = SigningKey::from_bytes(&[31u8; 32]);
    let realm_id = RealmId::from_bytes(realm_key.verifying_key().to_bytes());
    let user_id = UserId::local(Ulid::from_bytes([32u8; 16]), realm_id);
    let actor = Actor {
        node_id,
        user_id,
        realm_id,
    };
    drive(
        CreateRealmOperation::new(CreateRealmConfig {
            actor: Actor {
                user_id: UserId::nil(realm_id),
                ..actor.clone()
            },
            realm_description: "MCP test realm".to_string(),
            oidc_providers: Vec::new(),
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        }),
        &context,
    )
    .await
    .unwrap();
    drive(
        ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
            actor: actor.clone(),
        }),
        &context,
    )
    .await
    .unwrap();
    let capabilities = NodeCapabilities::management_node(realm_key).unwrap();
    let state = Arc::new(
        ServerState::new(
            context.clone(),
            realm_id,
            node_id,
            capabilities.clone(),
            false,
            None,
            JobsRuntime::new(),
        )
        .await,
    );
    let group_id = Ulid::from_bytes([33u8; 16]);
    let group_auth = GroupAuthorizationDocument::new_default_group_doc(user_id, realm_id, group_id);
    let group = Group {
        display_name: "MCP group".to_string(),
        group_id,
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner: user_id,
    };
    let user = User {
        user_id,
        name: "MCP User".to_string(),
        subject_ids: Vec::new(),
        alias_user_ids: HashSet::new(),
        attributes: HashMap::new(),
    };
    for (key_space, key, value) in [
        (
            USER_KEYSPACE,
            user_id.to_storage_key(),
            user.to_bytes(&actor).unwrap(),
        ),
        (
            GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group.to_bytes(&actor).unwrap(),
        ),
        (
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group_auth.to_bytes(&actor).unwrap(),
        ),
        (
            S3_BUCKET_KEYSPACE,
            b"mcp-data".to_vec(),
            BucketInfo {
                group_id,
                created_at: SystemTime::UNIX_EPOCH,
                created_by: user_id,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
            }
            .to_bytes()
            .unwrap(),
        ),
    ] {
        write_value(&state, key_space, key, value).await;
    }
    let token = drive(
        CreateTokenOperation::new(CreateTokenConfig {
            time: chrono::Utc::now().timestamp().max(0) as u64,
            expiry: None,
            user_id,
            realm_id,
            node_capabilities: capabilities,
            session: None,
        })
        .unwrap(),
        &context,
    )
    .await
    .unwrap();
    Fixture {
        _root: root,
        state,
        actor,
        _group_id: group_id,
        token,
        net,
    }
}

fn arguments(value: Value) -> serde_json::Map<String, Value> {
    value.as_object().unwrap().clone()
}

#[tokio::test]
async fn mcp_transport_contract() {
    let fixture = setup_fixture().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let server_task = tokio::spawn(
        Server::new(
            fixture.state.clone(),
            ServerConfig {
                http_addr: address,
                max_http_body_size: DEFAULT_MAX_HTTP_BODY_SIZE,
                cors: CorsConfig::default(),
            },
        )
        .run_with_listener(listener, shutdown.clone()),
    );
    let url = format!("http://{address}/mcp");
    let anonymous = reqwest::Client::new()
        .post(&url)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2026-07-28")
        .header("Mcp-Method", "server/discover")
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "server/discover",
            "params": {
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                    "io.modelcontextprotocol/clientInfo": {"name": "test", "version": "1"},
                    "io.modelcontextprotocol/clientCapabilities": {}
                }
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(
        anonymous.headers().get(header::WWW_AUTHENTICATE),
        Some(&HeaderValue::from_static("Bearer"))
    );

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(url).auth_header(fixture.token.clone()),
    );
    let client = ClientInfo::default()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .unwrap();
    let tools = client.list_tools(None).await.unwrap();
    assert!(tools.tools.iter().any(|tool| tool.name == "whoami"));
    let whoami = client
        .call_tool(CallToolRequestParams::new("whoami"))
        .await
        .unwrap();
    assert_ne!(whoami.is_error, Some(true));

    let profiles = client
        .call_tool(CallToolRequestParams::new("list_profiles"))
        .await
        .unwrap();
    assert_eq!(
        profiles.structured_content.as_ref().unwrap()["documents"],
        json!([])
    );
    let validation = client
        .call_tool(
            CallToolRequestParams::new("validate_dataset").with_arguments(arguments(json!({
                "rocrate": {
                    "@context": "https://w3id.org/ro/crate/1.3/context",
                    "@graph": [
                        {
                            "@id": "ro-crate-metadata.json",
                            "@type": "CreativeWork",
                            "conformsTo": { "@id": "https://w3id.org/ro/crate/1.3" },
                            "about": { "@id": "./" }
                        },
                        {
                            "@id": "./",
                            "@type": "Dataset",
                            "name": "Incomplete dataset"
                        }
                    ]
                }
            }))),
        )
        .await
        .unwrap();
    let validation = validation.structured_content.as_ref().unwrap();
    assert_eq!(validation["accepted"], false);
    assert!(
        validation["structural_violations"]
            .as_array()
            .is_some_and(|items| !items.is_empty())
    );

    let written = client
        .call_tool(
            CallToolRequestParams::new("write_object").with_arguments(arguments(json!({
                "bucket": "mcp-data",
                "key": "roundtrip.txt",
                "text": "hello mcp",
                "content_type": "text/plain"
            }))),
        )
        .await
        .unwrap();
    assert_ne!(written.is_error, Some(true));
    let read = client
        .call_tool(
            CallToolRequestParams::new("read_object").with_arguments(arguments(json!({
                "bucket": "mcp-data",
                "key": "roundtrip.txt"
            }))),
        )
        .await
        .unwrap();
    assert_eq!(
        read.structured_content.as_ref().unwrap()["text"],
        "hello mcp"
    );

    let mut realm = drive(
        GetRealmConfigOperation::new(fixture.state.get_realm_id()),
        &fixture.state.get_ctx(),
    )
    .await
    .unwrap();
    realm.request_policies = vec![RequestPolicy {
        policy_id: Ulid::generate(),
        name: "deny-mcp".to_string(),
        kind: PolicyKind::Deny,
        when: None,
        expression: "request.operation.startsWith(\"mcp:\")".to_string(),
        enabled: true,
    }];
    write_value(
        &fixture.state,
        REALM_CONFIG_KEYSPACE,
        fixture.state.get_realm_id().as_bytes().to_vec(),
        realm.to_bytes(&fixture.actor).unwrap(),
    )
    .await;
    let denied = client
        .call_tool(CallToolRequestParams::new("whoami"))
        .await
        .unwrap();
    assert_eq!(denied.is_error, Some(true));

    client.cancel().await.unwrap();
    shutdown.cancel();
    server_task.await.unwrap().unwrap();
    fixture.net.shutdown().await;
}
