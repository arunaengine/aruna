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
use rmcp::model::{
    CallToolRequestParams, ClientInfo, GetPromptRequestParams, ProtocolVersion,
    ReadResourceRequestParams, ResourceContents,
};
use rmcp::service::RunningService;
use rmcp::transport::{
    StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
};
use rmcp::{ClientLifecycleMode, ClientServiceExt, RoleClient};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

struct Fixture {
    _root: TempDir,
    state: Arc<ServerState>,
    actor: Actor,
    group_id: Ulid,
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
        group_id,
        token,
        net,
    }
}

fn arguments(value: Value) -> serde_json::Map<String, Value> {
    value.as_object().unwrap().clone()
}

type ServerTask = tokio::task::JoinHandle<Result<(), aruna_api::error::ServerSetupError>>;

async fn start_server(state: Arc<ServerState>) -> (String, CancellationToken, ServerTask) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(
        Server::new(
            state,
            ServerConfig {
                http_addr: address,
                max_http_body_size: DEFAULT_MAX_HTTP_BODY_SIZE,
                cors: CorsConfig::default(),
            },
        )
        .run_with_listener(listener, shutdown.clone()),
    );
    (format!("http://{address}/mcp"), shutdown, task)
}

async fn connect(url: &str, token: &str) -> RunningService<RoleClient, ClientInfo> {
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(url.to_string())
            .auth_header(token.to_string()),
    );
    ClientInfo::default()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .unwrap()
}

async fn call(
    client: &RunningService<RoleClient, ClientInfo>,
    tool: &str,
    args: Value,
) -> rmcp::model::CallToolResult {
    let request = match args {
        Value::Null => CallToolRequestParams::new(tool.to_string()),
        value => CallToolRequestParams::new(tool.to_string()).with_arguments(arguments(value)),
    };
    client.call_tool(request).await.unwrap()
}

fn is_error(result: &rmcp::model::CallToolResult) -> bool {
    result.is_error == Some(true)
}

fn code(result: &rmcp::model::CallToolResult) -> String {
    result
        .structured_content
        .as_ref()
        .and_then(|body| body.get("code"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
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

#[tokio::test]
async fn context_tools_directory() {
    let fixture = setup_fixture().await;
    let (url, shutdown, task) = start_server(fixture.state.clone()).await;
    let client = connect(&url, &fixture.token).await;
    let group = fixture.group_id.to_string();

    let groups = call(&client, "list_groups", Value::Null).await;
    assert!(!is_error(&groups));
    let listed = groups.structured_content.as_ref().unwrap();
    assert_eq!(listed["groups"][0]["group_id"], json!(group));

    let counts = call(&client, "count_datasets", Value::Null).await;
    assert!(!is_error(&counts));
    assert_eq!(
        counts.structured_content.as_ref().unwrap()["total"]["dataset_count"],
        json!(0)
    );

    assert!(!is_error(
        &call(&client, "get_realm_info", Value::Null).await
    ));
    assert!(!is_error(
        &call(&client, "get_node_info", Value::Null).await
    ));
    assert!(!is_error(
        &call(&client, "get_group", json!({ "group_id": group })).await
    ));
    assert!(!is_error(
        &call(&client, "list_group_members", json!({ "group_id": group })).await
    ));
    assert!(!is_error(
        &call(&client, "get_group_usage", json!({ "group_id": group })).await
    ));

    let bad = call(&client, "get_group", json!({ "group_id": "not-a-ulid" })).await;
    assert!(is_error(&bad));
    assert_eq!(code(&bad), "Bad request");
    let missing = call(
        &client,
        "get_group",
        json!({ "group_id": ulid::Ulid::generate().to_string() }),
    )
    .await;
    assert!(is_error(&missing));
    assert_eq!(code(&missing), "Not found");

    client.cancel().await.unwrap();
    shutdown.cancel();
    task.await.unwrap().unwrap();
    fixture.net.shutdown().await;
}

#[tokio::test]
async fn data_guard_keys() {
    let fixture = setup_fixture().await;
    let (url, shutdown, task) = start_server(fixture.state.clone()).await;
    let client = connect(&url, &fixture.token).await;

    let buckets = call(&client, "list_buckets", Value::Null).await;
    assert!(!is_error(&buckets));
    let names = buckets.structured_content.as_ref().unwrap()["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|entry| entry["bucket"].as_str())
        .any(|name| name == "mcp-data");
    assert!(names, "the owned bucket is listed");

    let listed = call(&client, "list_objects", json!({ "bucket": "mcp-data" })).await;
    assert!(!is_error(&listed));

    let missing = call(&client, "list_objects", json!({ "bucket": "absent" })).await;
    assert!(is_error(&missing));
    assert_eq!(code(&missing), "Not found");

    let no_key = call(
        &client,
        "read_object",
        json!({ "bucket": "mcp-data", "key": "nope.txt" }),
    )
    .await;
    assert!(is_error(&no_key));
    assert_eq!(code(&no_key), "Not found");

    let traversal = call(
        &client,
        "read_object",
        json!({ "bucket": "mcp-data", "key": "../escape" }),
    )
    .await;
    assert!(is_error(&traversal));
    assert_eq!(code(&traversal), "Bad request");

    let bad_window = call(
        &client,
        "read_object",
        json!({ "bucket": "mcp-data", "key": "notes.txt", "max_bytes": 0 }),
    )
    .await;
    assert!(is_error(&bad_window));
    assert_eq!(code(&bad_window), "Bad request");

    client.cancel().await.unwrap();
    shutdown.cancel();
    task.await.unwrap().unwrap();
    fixture.net.shutdown().await;
}

#[tokio::test]
async fn metadata_explains_refusals() {
    let fixture = setup_fixture().await;
    let (url, shutdown, task) = start_server(fixture.state.clone()).await;
    let client = connect(&url, &fixture.token).await;
    let group = fixture.group_id.to_string();
    let unknown = ulid::Ulid::generate().to_string();

    let bad_id = call(&client, "get_dataset", json!({ "id": "nope" })).await;
    assert!(is_error(&bad_id));
    assert_eq!(code(&bad_id), "Bad request");

    let absent = call(&client, "get_dataset", json!({ "id": unknown })).await;
    assert!(is_error(&absent));
    assert_eq!(code(&absent), "Not found");

    let not_profile = call(&client, "get_profile", json!({ "id": unknown })).await;
    assert!(is_error(&not_profile));
    assert_eq!(code(&not_profile), "Not found");

    let bad_iri = call(&client, "find_references", json!({ "iri": "not-an-iri" })).await;
    assert!(is_error(&bad_iri));
    assert_eq!(code(&bad_iri), "Bad request");

    let empty_query = call(&client, "search_datasets", json!({ "q": "" })).await;
    assert!(is_error(&empty_query));
    assert_eq!(code(&empty_query), "Bad request");

    let bad_group = call(
        &client,
        "create_dataset",
        json!({ "group_id": "nope", "path": "datasets/x", "rocrate": { "@graph": [] } }),
    )
    .await;
    assert!(is_error(&bad_group));
    assert_eq!(code(&bad_group), "Bad request");

    let empty_path = call(
        &client,
        "create_dataset",
        json!({ "group_id": group, "path": "///", "rocrate": { "@graph": [] } }),
    )
    .await;
    assert!(is_error(&empty_path));
    assert_eq!(code(&empty_path), "Bad request");

    client.cancel().await.unwrap();
    shutdown.cancel();
    task.await.unwrap().unwrap();
    fixture.net.shutdown().await;
}

#[tokio::test]
async fn compute_validates_input() {
    let fixture = setup_fixture().await;
    let (url, shutdown, task) = start_server(fixture.state.clone()).await;
    let client = connect(&url, &fixture.token).await;
    let group = fixture.group_id.to_string();

    let runtimes = call(&client, "list_runtimes", Value::Null).await;
    assert!(!is_error(&runtimes));
    assert!(
        runtimes.structured_content.as_ref().unwrap()["runtimes"]
            .as_array()
            .is_some_and(|entries| !entries.is_empty())
    );

    let jobs = call(&client, "list_jobs", json!({})).await;
    assert!(!is_error(&jobs));

    let bad_job = call(&client, "get_job", json!({ "id": "nope" })).await;
    assert!(is_error(&bad_job));
    assert_eq!(code(&bad_job), "Bad request");

    let bad_state = call(&client, "list_jobs", json!({ "state": "flying" })).await;
    assert!(is_error(&bad_state));
    assert_eq!(code(&bad_state), "Bad request");

    let bad_cancel = call(&client, "cancel_job", json!({ "id": "nope" })).await;
    assert!(is_error(&bad_cancel));
    assert_eq!(code(&bad_cancel), "Bad request");

    let bad_runtime = call(
        &client,
        "run_script",
        json!({
            "group_id": group,
            "bucket": "mcp-data",
            "runtime": "ruby",
            "script": "print('x')"
        }),
    )
    .await;
    assert!(is_error(&bad_runtime));
    assert_eq!(code(&bad_runtime), "Bad request");

    client.cancel().await.unwrap();
    shutdown.cancel();
    task.await.unwrap().unwrap();
    fixture.net.shutdown().await;
}

#[tokio::test]
async fn prompt_names_bucket() {
    let fixture = setup_fixture().await;
    let (url, shutdown, task) = start_server(fixture.state.clone()).await;
    let client = connect(&url, &fixture.token).await;

    let prompts = client.list_prompts(None).await.unwrap();
    let prompt = prompts
        .prompts
        .iter()
        .find(|prompt| prompt.name == "create-dataset")
        .unwrap();
    let names: Vec<&str> = prompt
        .arguments
        .as_ref()
        .unwrap()
        .iter()
        .map(|argument| argument.name.as_str())
        .collect();
    assert!(names.contains(&"bucket"));
    assert!(names.contains(&"prefix"));
    for argument in prompt.arguments.as_ref().unwrap() {
        if argument.name == "bucket" || argument.name == "prefix" {
            assert_ne!(argument.required, Some(true));
        }
    }

    let result = client
        .get_prompt(
            GetPromptRequestParams::new("create-dataset").with_arguments(arguments(json!({
                "group_id": fixture.group_id.to_string(),
                "bucket": "mcp-data",
                "prefix": "reads/2026/"
            }))),
        )
        .await
        .unwrap();
    let text = result.messages[0].content.as_text().unwrap().text.clone();
    assert!(text.contains("mcp-data"));
    assert!(text.contains("reads/2026/"));
    assert!(text.contains("aruna://docs/dataset-authoring"));
    assert!(text.contains("license"));
    assert!(text.contains("creator"));
    assert!(text.contains("Never invent"));
    assert!(text.contains("s3://bucket/key"));

    client.cancel().await.unwrap();
    shutdown.cancel();
    task.await.unwrap().unwrap();
    fixture.net.shutdown().await;
}

#[tokio::test]
async fn prompt_derives_group() {
    // A bucket alone names the owner; neither argument is refused.
    let fixture = setup_fixture().await;
    let (url, shutdown, task) = start_server(fixture.state.clone()).await;
    let client = connect(&url, &fixture.token).await;

    let result = client
        .get_prompt(
            GetPromptRequestParams::new("create-dataset")
                .with_arguments(arguments(json!({ "bucket": "mcp-data" }))),
        )
        .await
        .unwrap();
    let text = result.messages[0].content.as_text().unwrap().text.clone();
    assert!(text.contains("the group that owns bucket mcp-data"));

    let error = client
        .get_prompt(GetPromptRequestParams::new("create-dataset"))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("group_id"));

    client.cancel().await.unwrap();
    shutdown.cancel();
    task.await.unwrap().unwrap();
    fixture.net.shutdown().await;
}

#[tokio::test]
async fn authoring_docs_resource() {
    let fixture = setup_fixture().await;
    let (url, shutdown, task) = start_server(fixture.state.clone()).await;
    let client = connect(&url, &fixture.token).await;
    let uri = "aruna://docs/dataset-authoring";

    let resources = client.list_resources(None).await.unwrap();
    assert!(
        resources
            .resources
            .iter()
            .any(|resource| resource.uri == uri)
    );

    let read = client
        .read_resource(ReadResourceRequestParams::new(uri))
        .await
        .unwrap();
    let ResourceContents::TextResourceContents { text, .. } = &read.contents[0] else {
        panic!("dataset authoring docs must be text");
    };
    assert!(text.contains("## Inventory the bucket"));
    assert!(text.contains("next_cursor"));
    assert!(text.contains("https://w3id.org/aruna/profile/"));

    let missing = client
        .read_resource(ReadResourceRequestParams::new("aruna://docs/nothing"))
        .await;
    assert!(missing.is_err());

    client.cancel().await.unwrap();
    shutdown.cancel();
    task.await.unwrap().unwrap();
    fixture.net.shutdown().await;
}
