use aruna_rust_api::v3::aruna::api::v3::group_service_client::GroupServiceClient;
use aruna_rust_api::v3::aruna::api::v3::realm_service_client::RealmServiceClient;
use aruna_rust_api::v3::aruna::api::v3::resource_service_client::ResourceServiceClient;
use aruna_rust_api::v3::aruna::api::v3::user_service_client::UserServiceClient;
use aruna_server::{start_server, Config};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::AtomicU16;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;
use tokio::fs;
use tokio::sync::Notify;
use tokio::time::sleep;
use tonic::codegen::InterceptedService;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::transport::Channel;
use tracing_subscriber::EnvFilter;
use ulid::Ulid;

// USERS
//
// ADMIN:
// {
//  "user": {
//      "id":"01JER6JBQ39EDVR2M7A6Z9512B",
//      "first_name":"aruna",
//      "last_name":"admin",
//      "email":"admin@test.com",
//      "identifiers":"",
//      "global_admin":false,
//      "deleted":false
//   }
// }
// {
//   "token": {
//     "id": 0,
//     "user_id": "01JER6JBQ39EDVR2M7A6Z9512B",
//     "name": "ADMIN_TOKEN",
//     "expires_at": "2050-12-10T11:52:17.683Z",
//     "token_type": "Aruna",
//     "scope": "Personal",
//     "constraints": null,
//     "default_realm": null,
//     "default_group": null,
//     "component_id": null
//   },
//   "secret": "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSkVSNkpCUTM5RURWUjJNN0E2Wjk1MTJCIiwiYXVkIjoiYXJ1bmEiLCJleHAiOjI1NTQyODU5MzcsImluZm8iOlswLDBdfQ.64t0FKm26aAQL4yRoccjfq6NH1lmtz7UT3eV6zztYmiTVDIrto8SXYNvpQQj15fOhDLeftsNYovsfllJRwATCw"
// }
#[allow(unused)]
pub const ADMIN_OIDC: &str = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJocS1BcGJfWS15RzJ1YktjSDFmTGN4UmltZ3YzSlBSelRQUENKbEtpOW9zIn0.eyJleHAiOjE3ODUyMzk0MjQsImlhdCI6MTY5ODgzOTQyNCwiYXV0aF90aW1lIjoxNjk4ODM5NDI0LCJqdGkiOiI5ZjJlMjdhYi04MDIzLTQ1MTctYTE3Yi1jNDY2OGRlZTk2MzAiLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjE5OTgvcmVhbG1zL3Rlc3QiLCJhdWQiOiJ0ZXN0LWxvbmciLCJzdWIiOiIxNGYwZTdiZi0wOTQ3LTRhYTEtYThjZC0zMzdkZGVmZjQ1NzMiLCJ0eXAiOiJJRCIsImF6cCI6InRlc3QtbG9uZyIsIm5vbmNlIjoiREFrX3BTZjYxVEpPYnpRWDhwN0JQUSIsInNlc3Npb25fc3RhdGUiOiJiYTkxYmZkMi0wNmY2LTRjYTMtOTFlYS0wYmQ1ZmQxNzZkZjIiLCJhdF9oYXNoIjoiX3pkYXhxMHlucDRvajk1UmhiRG5VdyIsImFjciI6IjEiLCJzaWQiOiJiYTkxYmZkMi0wNmY2LTRjYTMtOTFlYS0wYmQ1ZmQxNzZkZjIiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwicHJlZmVycmVkX3VzZXJuYW1lIjoiYXJ1bmFhZG1pbiIsImdpdmVuX25hbWUiOiIiLCJmYW1pbHlfbmFtZSI6IiIsImVtYWlsIjoiYWRtaW5AdGVzdC5jb20ifQ.sV0qo32b4tl7Y984_hW8Pc8a8trkmNg_6MKb7l3aacEH6eC1633JsI8D6qMPw22y4Lf5sb3XOCY_LZQpIKWs7TmkaSlv-9I2Ioi9kZRHpoNd75PnYJDFi6NrK7byJ5IeE167UskEqVTNfCkhkWFUzjogDRaHL-oscb-aTG35tqR-9DcVWUb5wuyKYbJQyRVetiQIKdo-ExNgqad1ScVPdhX9ktRJRZvWSeP7AHV2NpoM3x0WojAWXNIkhWoNksUJclaR25PcTlQmAh43QvICxpaiCCKTOcNSf-wBLGzTvxvFijYjYPgfyXCThFzOJkBC-qhrpVRXQh_nVcLmXJPxCQ";
pub const ADMIN_TOKEN: &str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSkVSNkpCUTM5RURWUjJNN0E2Wjk1MTJCIiwiYXVkIjoiYXJ1bmEiLCJleHAiOjI1NTQyODU5MzcsImluZm8iOlswLDBdfQ.64t0FKm26aAQL4yRoccjfq6NH1lmtz7UT3eV6zztYmiTVDIrto8SXYNvpQQj15fOhDLeftsNYovsfllJRwATCw";

// REGULAR
// {
//   "user": {
//      "id":"01JER6Q2MEX5SS7GQCSSDFJJVG",
//      "first_name":"regular",
//      "last_name":"user",
//      "email":"regular@test.com",
//      "identifiers":"",
//      "global_admin":false,
//      "deleted":false
//  }
// }
// {
//   "token": {
//     "id": 0,
//     "user_id": "01JER6Q2MEX5SS7GQCSSDFJJVG",
//     "name": "REGULAR_TOKEN",
//     "expires_at": "2050-12-10T11:54:54.614Z",
//     "token_type": "Aruna",
//     "scope": "Personal",
//     "constraints": null,
//     "default_realm": null,
//     "default_group": null,
//     "component_id": null
//   },
//   "secret": "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSkVSNlEyTUVYNVNTN0dRQ1NTREZKSlZHIiwiYXVkIjoiYXJ1bmEiLCJleHAiOjI1NTQyODYwOTQsImluZm8iOlswLDBdfQ.Q4FXyWqUdJ37NGMpe1x4pjMPao2NAE1CIauNEKb7N6VUQyjiYLGe32OEmp-DeikGyjhns7bBfNg-w60v-v1DCw"
// }
#[allow(unused)]
pub const REGULAR_OIDC: &str = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJocS1BcGJfWS15RzJ1YktjSDFmTGN4UmltZ3YzSlBSelRQUENKbEtpOW9zIn0.eyJleHAiOjE3ODUyMzk1OTksImlhdCI6MTY5ODgzOTU5OSwiYXV0aF90aW1lIjoxNjk4ODM5NTk5LCJqdGkiOiJmZjkyMzEwNC1hZGNkLTRjOTEtYjdjNi03MWM1ODMxNjlhYzciLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjE5OTgvcmVhbG1zL3Rlc3QiLCJhdWQiOiJ0ZXN0LWxvbmciLCJzdWIiOiI4ZGJlZTAwOS1hM2U4LTQ2NjQtODg1Ni0xNDE3M2Q5YWJkNWIiLCJ0eXAiOiJJRCIsImF6cCI6InRlc3QtbG9uZyIsIm5vbmNlIjoiQ21NRWxIM3JQSVF2dENBTFVSQWlPZyIsInNlc3Npb25fc3RhdGUiOiIyY2FmNGE0Ni1mZDYxLTQ2MWEtODIwZS1jMTM0YmY4ZjU0ZTYiLCJhdF9oYXNoIjoiMXRRYjhETWRaNjJVaW9MTl9tRkQxZyIsImFjciI6IjEiLCJzaWQiOiIyY2FmNGE0Ni1mZDYxLTQ2MWEtODIwZS1jMTM0YmY4ZjU0ZTYiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwicHJlZmVycmVkX3VzZXJuYW1lIjoicmVndWxhciIsImdpdmVuX25hbWUiOiIiLCJmYW1pbHlfbmFtZSI6IiIsImVtYWlsIjoicmVndWxhckB0ZXN0LmNvbSJ9.dc759HTpLgcMT8exZPWpgO9k3O5eQy0KKkqVRj6LQZAIq9CcK-rEHs6P6QiT3vWq8CKLQkBcYPTY4zniKQ78spip9b1OrNdvQ5K9aHuCsZHvaH72tOXQGCsMXKwV_WX6EkRn75A1y4nqJ0H3GCcrNzJTLeh32dcUcxHZtHxcBp3SKpTeq6e-hXYP1XSK73KfSsDj5-zYcaVHWR-av7Q7YcxBul4P2bfOPQRDZNIqkHa7cZGD6nMpLb5WFB-mGHqEB3V4dmvF4Wu9CJScyiVkleG-aSRLXzGDQMtk8iRbCM-xQpr-JvwvKvQXeas5B6ifiMO8GRq8DOPf5m9rCAwEVw";
#[allow(unused)]
pub const REGULAR_TOKEN: &str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSkVSNlEyTUVYNVNTN0dRQ1NTREZKSlZHIiwiYXVkIjoiYXJ1bmEiLCJleHAiOjI1NTQyODYwOTQsImluZm8iOlswLDBdfQ.Q4FXyWqUdJ37NGMpe1x4pjMPao2NAE1CIauNEKb7N6VUQyjiYLGe32OEmp-DeikGyjhns7bBfNg-w60v-v1DCw";
#[allow(unused)]
pub const REGULAR_ULID: &str = "01JER6Q2MEX5SS7GQCSSDFJJVG";

// REGULAR2
// {
//   "user": {
//      "id":"01JER6X92DX6RSCETWZ3KERV4B",
//      "first_name":"regular2",
//      "last_name":"user",
//      "email":"regular2@test.com",
//      "identifiers":"",
//      "global_admin":false,
//      "deleted":false
//   }
// }
// {
//   "token": {
//     "id": 0,
//     "user_id": "01JER6X92DX6RSCETWZ3KERV4B",
//     "name": "REGULAR2_TOKEN",
//     "expires_at": "2050-12-10T11:58:10.258Z",
//     "token_type": "Aruna",
//     "scope": "Personal",
//     "constraints": null,
//     "default_realm": null,
//     "default_group": null,
//     "component_id": null
//   },
//   "secret": "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSkVSNlg5MkRYNlJTQ0VUV1ozS0VSVjRCIiwiYXVkIjoiYXJ1bmEiLCJleHAiOjI1NTQyODYyOTAsImluZm8iOlswLDBdfQ.YXy4VtbsmLoWuI1UIvlAuZ2rrrJ2_X9iHfmYl-aD3-rXDFffaUxWYFSstlFTR0DHAhmsMUfWRQ76S6D61-WSCQ"
// }
#[allow(unused)]
pub const REGULAR2_TOKEN: &str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSkVSNlg5MkRYNlJTQ0VUV1ozS0VSVjRCIiwiYXVkIjoiYXJ1bmEiLCJleHAiOjI1NTQyODYyOTAsImluZm8iOlswLDBdfQ.YXy4VtbsmLoWuI1UIvlAuZ2rrrJ2_X9iHfmYl-aD3-rXDFffaUxWYFSstlFTR0DHAhmsMUfWRQ76S6D61-WSCQ";

// WEBTEST
// {
//   "user": {
//      "id":"01JER70VYP61KHR9QJDAX2N4T0",
//      "first_name":"webtest",
//      "last_name":"user",
//      "email":"webtest@test.com",
//      "identifiers":"",
//      "global_admin":false,
//      "deleted":false
//   }
// }
// {
//   "token": {
//     "id": 0,
//     "user_id": "01JER70VYP61KHR9QJDAX2N4T0",
//     "name": "WEBTEST_TOKEN",
//     "expires_at": "2050-12-10T12:00:03.554Z",
//     "token_type": "Aruna",
//     "scope": "Personal",
//     "constraints": null,
//     "default_realm": null,
//     "default_group": null,
//     "component_id": null
//   },
//   "secret": "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSkVSNzBWWVA2MUtIUjlRSkRBWDJONFQwIiwiYXVkIjoiYXJ1bmEiLCJleHAiOjI1NTQyODY0MDMsImluZm8iOlswLDBdfQ.hSW0cf-qj-Jw3B-aXxgxsRfNosOiF_R1EnkdEYJg9A8jLE7Zw_7QwnbpaJL5A4rnLJjhMS2h0FCx_X_kEByXCA"
// }
#[allow(unused)]
pub const WEBTEST_TOKEN: &str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSkVSNzBWWVA2MUtIUjlRSkRBWDJONFQwIiwiYXVkIjoiYXJ1bmEiLCJleHAiOjI1NTQyODY0MDMsImluZm8iOlswLDBdfQ.hSW0cf-qj-Jw3B-aXxgxsRfNosOiF_R1EnkdEYJg9A8jLE7Zw_7QwnbpaJL5A4rnLJjhMS2h0FCx_X_kEByXCA";

pub static SUBSCRIBERS: AtomicU16 = AtomicU16::new(0);
static INIT_TRACING: Once = Once::new();
const MAX_RETRIES: u8 = 50;

// Create a client interceptor which always adds the specified api token to the request header
#[derive(Clone)]
pub struct ClientInterceptor {
    api_token: String,
}
// Implement a request interceptor which always adds
//  the authorization header with a specific API token to all requests
impl tonic::service::Interceptor for ClientInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let mut mut_req: tonic::Request<()> = request;
        let metadata = mut_req.metadata_mut();
        metadata.append(
            AsciiMetadataKey::from_bytes("Authorization".as_bytes()).unwrap(),
            AsciiMetadataValue::try_from(format!("Bearer {}", self.api_token.as_str())).unwrap(),
        );

        return Ok(mut_req);
    }
}

#[allow(unused)]
pub struct Clients {
    pub realm_client: RealmServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    pub group_client: GroupServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    pub user_client: UserServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    pub resource_client: ResourceServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    pub rest_endpoint: String,
}
pub async fn init_test(offset: u16) -> Clients {
    INIT_TRACING.call_once(init_tracing);

    // Start server and get port
    let (grpc_port, rest_port, notify) = init_testing_server(offset).await;
    notify.notified().await;
    // Create connection to the Aruna instance via gRPC
    let api_token = ADMIN_TOKEN;
    let endpoint = Channel::from_shared(format!("http://0.0.0.0:{grpc_port}")).unwrap();

    let mut retries = MAX_RETRIES;
    let channel = loop {
        retries -= 1;
        if retries == 0 {
            panic!()
        }
        sleep(Duration::from_millis(10)).await;
        match endpoint.connect().await {
            Ok(channel) => break channel,
            Err(e) => {
                dbg!(e);
                sleep(Duration::from_millis(100)).await;
            }
        }
    };
    //let channel = endpoint.connect().await.unwrap();
    let interceptor = ClientInterceptor {
        api_token: api_token.to_string(),
    };

    // Create the individual client services
    let realm_client = RealmServiceClient::with_interceptor(channel.clone(), interceptor.clone());
    let group_client = GroupServiceClient::with_interceptor(channel.clone(), interceptor.clone());
    let user_client = UserServiceClient::with_interceptor(channel.clone(), interceptor.clone());
    let resource_client =
        ResourceServiceClient::with_interceptor(channel.clone(), interceptor.clone());
    Clients {
        realm_client,
        group_client,
        user_client,
        resource_client,
        rest_endpoint: format!("http://localhost:{}", rest_port),
    }
}

async fn init_testing_server(offset: u16) -> (u16, u16, Arc<Notify>) {
    // Create notifier
    let notify = Arc::new(Notify::new());
    let notify_clone = notify.clone();

    // Copy & create db
    let node_id = Ulid::new();
    let test_path = format!("/dev/shm/{node_id}");
    fs::create_dir_all(format!("{test_path}/events"))
        .await
        .unwrap();
    fs::create_dir_all(format!("{test_path}/store"))
        .await
        .unwrap();
    fs::copy(
        "./tests/test_db/events/data.mdb",
        &format!("{test_path}/events/data.mdb"),
    )
    .await
    .unwrap();

    fs::copy(
        "./tests/test_db/store/data.mdb",
        &format!("{test_path}/store/data.mdb"),
    )
    .await
    .unwrap();

    // Create server config with unused ports
    let subscriber_count = SUBSCRIBERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + offset;
    let node_serial = subscriber_count;
    let grpc_port = 50050 + subscriber_count;
    let consensus_port = 60050 + subscriber_count;
    let socket_addr = format!("0.0.0.0:{consensus_port}");
    let rest_port = 8080 + subscriber_count;

    // Spawn server
    tokio::spawn(async move {
        start_server(
            Config {
                node_id,
                grpc_port,
                rest_port,
                node_serial,
                database_path: test_path,
                key_config: (
                    1,
                    "MC4CAQAwBQYDK2VwBCIEICHl/V9wxvENDJKePwusDhnC7xgaHYV6iHLb0ENJZndj".to_string(),
                    "MCowBQYDK2VwAyEA2YfYTgb8Y0LTFr+2Rm2Fkdu38eJTfnsMDH2iZHErBH0=".to_string(),
                ),
                socket_addr: SocketAddr::from_str(&socket_addr).unwrap(),
                init_node: None,
                issuer_config: None,
                opentelemetry_name: "aruna-test".to_string(),
                opentelemetry_endpoint: "http://localhost:4317".to_string()
            },
            Some(notify_clone),
        )
        .await
    });

    // Return grpc port
    (grpc_port, rest_port, notify)
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("h2=off".parse().unwrap())
        .add_directive("tower=off".parse().unwrap())
        .add_directive("hyper=off".parse().unwrap())
        .add_directive("tonic=off".parse().unwrap());
    //.add_directive("synevi_core=trace".parse().unwrap());

    tracing_subscriber::fmt().with_env_filter(filter).init();
}
