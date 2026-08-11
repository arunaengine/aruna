// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
mod shared;

use reqwest::StatusCode;
use shared::{
    TestResult, create_bearer_token, create_group_via_http, create_s3_credentials_via_http,
    s3_client, spawn_full_seed_node, spawn_seed_node, wait_for_group_via_http,
};

async fn scrape(ops_url: &str) -> TestResult<String> {
    let response = reqwest::get(format!("{ops_url}/metrics")).await?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(response.text().await?)
}

fn gauge_value(body: &str, series: &str) -> Option<f64> {
    body.lines()
        .find(|line| line.starts_with(series))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse::<f64>().ok())
}

#[tokio::test]
async fn readyz_reflects_startup_gate() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let result = async {
        let client = reqwest::Client::new();

        let health = client
            .get(format!("{}/healthz", seed.ops_url))
            .send()
            .await?;
        assert_eq!(health.status(), StatusCode::OK);
        assert_eq!(health.text().await?, "ok");

        let before = client
            .get(format!("{}/readyz", seed.ops_url))
            .send()
            .await?;
        assert_eq!(before.status(), StatusCode::SERVICE_UNAVAILABLE);
        let before_body: serde_json::Value = before.json().await?;
        assert_eq!(before_body["ready"], serde_json::json!(false));
        assert!(
            before_body["checks"]["startup"]
                .as_str()
                .unwrap_or_default()
                .starts_with("failed"),
            "unexpected startup check: {before_body}"
        );

        seed.readiness.set_ready();

        let after = client
            .get(format!("{}/readyz", seed.ops_url))
            .send()
            .await?;
        assert_eq!(after.status(), StatusCode::OK, "node should be ready");
        let after_body: serde_json::Value = after.json().await?;
        assert_eq!(after_body["ready"], serde_json::json!(true));

        // Liveness is unconditional.
        let health = client
            .get(format!("{}/healthz", seed.ops_url))
            .send()
            .await?;
        assert_eq!(health.status(), StatusCode::OK);
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn metrics_expose_rest_storage_and_queue_series() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let result = async {
        let client = reqwest::Client::new();
        // Drive one REST request so the counter has a rest sample.
        client
            .get(format!("{}/api/v1/info", seed.base_url))
            .send()
            .await?;

        let body = scrape(&seed.ops_url).await?;
        assert_eq!(
            gauge_value(
                &body,
                "aruna_http_requests_total{interface=\"rest\",method=\"GET\",code=\"200\"}"
            ),
            Some(1.0),
            "expected exactly one rest GET 200 request: {body}"
        );
        assert!(
            body.contains("aruna_storage_requests_total"),
            "missing storage counter: {body}"
        );
        assert!(
            body.contains("# TYPE aruna_storage_requests counter"),
            "storage requests not typed as a counter: {body}"
        );
        assert!(
            gauge_value(&body, "aruna_queue_depth{queue=\"document_sync_outbox\"}")
                .is_some_and(|depth| depth >= 0.0),
            "outbox depth gauge missing or negative: {body}"
        );
        assert!(
            gauge_value(
                &body,
                "aruna_queue_probe_up{queue=\"document_sync_outbox\"}"
            )
            .is_some(),
            "outbox probe health gauge missing: {body}"
        );
        assert!(
            gauge_value(&body, "aruna_node_started").is_some(),
            "node_started gauge missing: {body}"
        );
        assert!(
            body.contains("aruna_build_info{version=\""),
            "missing build info: {body}"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn metrics_absent_public() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let result = async {
        // The Prometheus scrape must never be reachable on the public REST port.
        let response = reqwest::get(format!("{}/metrics", seed.base_url)).await?;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response.text().await?;
        assert!(
            !body.contains("aruna_build_info") && !body.contains("aruna_http_requests_total"),
            "public port leaked metrics: {body}"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn metrics_expose_s3_operation_label() -> TestResult<()> {
    let seed = spawn_full_seed_node().await?;
    let result = async {
        let bearer_token = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group =
            create_group_via_http(&seed.base_url, &bearer_token, "obs-metrics-group").await?;
        wait_for_group_via_http(&seed.base_url, &bearer_token, &group.group_id).await?;
        let credentials =
            create_s3_credentials_via_http(&seed.base_url, &bearer_token, &group.group_id).await?;

        let endpoint = seed
            .s3
            .as_ref()
            .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
        let s3 = s3_client(endpoint, &credentials);
        s3.create_bucket()
            .bucket("obs-metrics-bucket")
            .send()
            .await?;

        let body = scrape(&seed.ops_url).await?;
        assert!(
            body.contains("interface=\"s3\""),
            "missing s3 interface sample: {body}"
        );
        assert!(
            body.contains("op=\"CreateBucket\""),
            "missing resolved CreateBucket op label: {body}"
        );
        assert!(
            gauge_value(
                &body,
                "aruna_http_request_duration_seconds_count{interface=\"s3\",op=\"CreateBucket\"}",
            )
            .is_some_and(|count| count >= 1.0),
            "CreateBucket duration count missing: {body}"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    seed.shutdown().await;
    result
}

/// Compiled-process harness. The in-process seed nodes above assemble the server
/// by hand, so only launching the real binary can observe `main`'s startup
/// order, its serving gate, and its restart outbox delta.
mod process {
    use aruna_core::effects::{IterStart, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::DOCUMENT_SYNC_OUTBOX_KEYSPACE;
    use aruna_storage::{FjallStorage, StorageHandle};
    use std::net::TcpListener;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::time::Duration;
    use tokio::time::Instant;

    /// Deadlock guard only: a breach means the process never reached the gate.
    /// Never a speed assertion.
    pub const HANG_GUARD: Duration = Duration::from_secs(240);

    pub struct NodeEnv {
        dir: tempfile::TempDir,
        http_port: u16,
        p2p_port: u16,
        ops_port: u16,
        s3_port: u16,
    }

    fn free_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        listener.local_addr().expect("ephemeral addr").port()
    }

    impl NodeEnv {
        pub fn new() -> Self {
            let dir = tempfile::tempdir().expect("temp dir");
            let env = Self {
                http_port: free_port(),
                p2p_port: free_port(),
                ops_port: free_port(),
                s3_port: free_port(),
                dir,
            };
            env.write_env_file();
            env
        }

        pub fn root(&self) -> &Path {
            self.dir.path()
        }

        pub fn storage_path(&self) -> PathBuf {
            self.root().join("storage")
        }

        pub fn ops_url(&self) -> String {
            format!("http://127.0.0.1:{}", self.ops_port)
        }

        pub fn rest_url(&self) -> String {
            format!("http://127.0.0.1:{}", self.http_port)
        }

        pub fn log_path(&self) -> PathBuf {
            self.root().join("node.log")
        }

        fn write_env_file(&self) {
            let storage = self.storage_path();
            let body = format!(
                "STORAGE_PATH={storage}\n\
                 BLOB_ROOT={storage}/blobstore\n\
                 SOCKET_ADDRESS=127.0.0.1:{http}\n\
                 P2P_SOCKET_ADDRESS=127.0.0.1:{p2p}\n\
                 OPS_SOCKET_ADDRESS=127.0.0.1:{ops}\n\
                 S3_HOST=127.0.0.1:{s3}\n\
                 S3_ADDRESS=127.0.0.1:{s3}\n\
                 API_PUBLIC_URL=http://127.0.0.1:{http}\n\
                 S3_PUBLIC_URL=http://127.0.0.1:{s3}\n\
                 REALM_DESCRIPTION=observability harness realm\n\
                 ARUNA_COMPUTE_EXECUTOR=none\n\
                 ARUNA_SHUTDOWN_GRACE_SECS=16\n\
                 RUST_LOG=info\n",
                storage = storage.display(),
                http = self.http_port,
                p2p = self.p2p_port,
                ops = self.ops_port,
                s3 = self.s3_port,
            );
            std::fs::write(self.root().join(".env"), body).expect("write .env");
        }

        pub fn launch(&self) -> NodeProcess {
            let log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.log_path())
                .expect("open node log");
            let errlog = log.try_clone().expect("clone node log");
            let child = Command::new(env!("CARGO_BIN_EXE_aruna"))
                .current_dir(self.root())
                .stdin(Stdio::null())
                .stdout(Stdio::from(log))
                .stderr(Stdio::from(errlog))
                .spawn()
                .expect("spawn aruna process");
            NodeProcess {
                child,
                ops_url: self.ops_url(),
                rest_url: self.rest_url(),
                log_path: self.log_path(),
            }
        }

        /// Opens the stopped node's store; it is single-writer, so the process
        /// must have exited.
        pub async fn open_storage(&self) -> StorageHandle {
            let path = self.storage_path();
            let path = path.to_str().expect("utf8 storage path");
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                match FjallStorage::open(path) {
                    Ok(storage) => return storage,
                    Err(error) if Instant::now() < deadline => {
                        tracing::debug!(%error, "waiting for the node store lock");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    Err(error) => panic!("could not open the stopped node store: {error}"),
                }
            }
        }

        /// Counts document-sync outbox records in the stopped node's store.
        pub async fn outbox_len(&self) -> usize {
            let storage = self.open_storage().await;
            let counted = count_outbox(&storage).await;
            drop(storage);
            counted
        }
    }

    pub async fn count_outbox(storage: &StorageHandle) -> usize {
        let mut start: Option<aruna_core::types::Key> = None;
        let mut total = 0usize;
        loop {
            let event = storage
                .send_storage_effect(StorageEffect::Iter {
                    key_space: DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
                    prefix: None,
                    start: start.take().map(IterStart::After),
                    limit: 1024,
                    txn_id: None,
                })
                .await;
            let Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) = event
            else {
                panic!("unexpected outbox iter event: {event:?}");
            };
            total += values.len();
            match next_start_after {
                Some(next) => start = Some(next),
                None => break,
            }
        }
        total
    }

    pub struct NodeProcess {
        child: Child,
        pub ops_url: String,
        pub rest_url: String,
        log_path: PathBuf,
    }

    impl NodeProcess {
        pub fn pid(&self) -> u32 {
            self.child.id()
        }

        pub fn is_running(&mut self) -> bool {
            matches!(self.child.try_wait(), Ok(None))
        }

        pub fn logs(&self) -> String {
            std::fs::read_to_string(&self.log_path).unwrap_or_default()
        }

        /// Waits until the ops listener answers `path` with `expect`.
        pub async fn wait_status(&mut self, path: &str, expect: StatusCode) -> String {
            let client = probe_client();
            let deadline = Instant::now() + HANG_GUARD;
            let mut last = String::from("no response yet");
            while Instant::now() < deadline {
                if !self.is_running() {
                    panic!(
                        "process exited before {path} returned {expect}\n{}",
                        self.logs()
                    );
                }
                match client.get(format!("{}{path}", self.ops_url)).send().await {
                    Ok(response) => {
                        let status = response.status();
                        let body = response.text().await.unwrap_or_default();
                        if status == expect {
                            return body;
                        }
                        last = format!("status {status}: {body}");
                    }
                    Err(error) => last = error.to_string(),
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            panic!(
                "{path} never returned {expect} (last {last})\n{}",
                self.logs()
            );
        }

        /// Waits until `needle` appears in the process log.
        pub async fn wait_log(&mut self, needle: &str) {
            let deadline = Instant::now() + HANG_GUARD;
            while Instant::now() < deadline {
                if self.logs().contains(needle) {
                    return;
                }
                if !self.is_running() {
                    panic!("process exited before logging {needle}\n{}", self.logs());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            panic!("never logged {needle}\n{}", self.logs());
        }

        pub fn signal(&self, name: &str) {
            let status = Command::new("kill")
                .arg(format!("-{name}"))
                .arg(self.pid().to_string())
                .status()
                .expect("send signal");
            assert!(status.success(), "kill -{name} failed");
        }

        /// Sends SIGTERM and waits for exit inside the hang guard.
        pub async fn terminate(mut self) -> std::process::ExitStatus {
            self.signal("TERM");
            let deadline = Instant::now() + HANG_GUARD;
            loop {
                match self.child.try_wait().expect("wait for exit") {
                    Some(status) => return status,
                    None if Instant::now() >= deadline => {
                        let logs = self.logs();
                        let _ = self.child.kill();
                        panic!("process did not exit after SIGTERM\n{logs}");
                    }
                    None => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
        }
    }

    impl Drop for NodeProcess {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    pub fn probe_client() -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(8))
            .build()
            .expect("probe client")
    }

    use reqwest::StatusCode;
}

/// Adds synthetic, never-dialled peers to the stopped node's realm config, so
/// the node holds shards whose co-holders are absent.
async fn inject_offline_peers(env: &process::NodeEnv, count: u8) -> TestResult<()> {
    use aruna_core::effects::{IterStart, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
    use aruna_core::structs::{RealmConfigDocument, RealmNodeKind};

    let storage = env.open_storage().await;
    let event = storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            prefix: None,
            start: None::<IterStart>,
            limit: 8,
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
        panic!("unexpected realm config iter event: {event:?}");
    };
    let (key, bytes) = values
        .into_iter()
        .find(|(_, value)| RealmConfigDocument::from_bytes(value).is_ok())
        .expect("node should have a realm config after its first boot");
    let mut config = RealmConfigDocument::from_bytes(&bytes).expect("decode realm config");
    for seed in 0..count {
        let peer = iroh::SecretKey::from_bytes(&[0xA0 + seed; 32]).public();
        config.ensure_node(peer, RealmNodeKind::Server);
    }
    let value = postcard::to_allocvec(&config).expect("encode realm config");
    let event = storage
        .send_storage_effect(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key,
            value: value.into(),
            txn_id: None,
        })
        .await;
    assert!(
        matches!(event, Event::Storage(StorageEvent::WriteResult { .. })),
        "unexpected realm config write event: {event:?}"
    );
    storage.sync_all().await?;
    drop(storage);
    Ok(())
}

/// The delta between two restarts against unchanged state is what startup itself
/// adds to the document-sync outbox; it must be exactly zero.
#[tokio::test]
async fn restart_adds_no_outbox() -> TestResult<()> {
    let env = process::NodeEnv::new();

    let mut first = env.launch();
    first.wait_status("/readyz", StatusCode::OK).await;
    first.terminate().await;

    inject_offline_peers(&env, 2).await?;

    let mut second = env.launch();
    second.wait_status("/healthz", StatusCode::OK).await;
    second.wait_log("startup.recovery.degraded").await;
    second.terminate().await;
    let before = env.outbox_len().await;

    let mut third = env.launch();
    third.wait_status("/healthz", StatusCode::OK).await;
    third.wait_log("startup.recovery.degraded").await;
    third.terminate().await;
    let after = env.outbox_len().await;

    assert_eq!(
        after, before,
        "an unchanged restart must add no document sync outbox work"
    );
    Ok(())
}

/// With unreachable peers the node must still bind its listeners, keep
/// `/healthz` 200, and answer `/readyz` 200 while recovery reports degraded.
#[tokio::test]
async fn gate_survives_outage() -> TestResult<()> {
    let env = process::NodeEnv::new();

    let mut first = env.launch();
    first.wait_status("/readyz", StatusCode::OK).await;
    first.terminate().await;

    inject_offline_peers(&env, 2).await?;

    let mut node = env.launch();
    // Liveness answers as soon as the ops listener is up.
    assert_eq!(node.wait_status("/healthz", StatusCode::OK).await, "ok");
    let body = node.wait_status("/readyz", StatusCode::OK).await;
    let ready: serde_json::Value = serde_json::from_str(&body)?;
    assert_eq!(ready["ready"], serde_json::json!(true));
    assert_eq!(ready["checks"]["startup"], serde_json::json!("ok"));

    // REST is bound without the peers ever becoming reachable.
    let client = process::probe_client();
    let info = client
        .get(format!("{}/api/v1/info", node.rest_url))
        .send()
        .await?;
    assert_eq!(info.status(), StatusCode::OK);

    node.wait_log("startup.recovery.degraded").await;
    assert!(node.is_running(), "a peer outage must not end the process");
    assert_eq!(node.wait_status("/healthz", StatusCode::OK).await, "ok");

    let body = node.wait_status("/readyz", StatusCode::OK).await;
    let ready: serde_json::Value = serde_json::from_str(&body)?;
    assert_eq!(
        ready["recovery"]["state"],
        serde_json::json!("degraded"),
        "{body}"
    );
    assert_eq!(
        ready["recovery"]["last_error_class"],
        serde_json::json!("peer_unavailable"),
        "{body}"
    );
    // Fixed fields only: no peer, topic, or document identifiers.
    let recovery = ready["recovery"].as_object().expect("recovery object");
    let mut fields: Vec<&str> = recovery.keys().map(String::as_str).collect();
    fields.sort_unstable();
    assert_eq!(
        fields,
        [
            "last_error_class",
            "last_progress_timestamp",
            "state",
            "topics_remaining"
        ]
    );

    let scrape = client
        .get(format!("{}/metrics", node.ops_url))
        .send()
        .await?
        .text()
        .await?;
    assert!(
        scrape.contains("aruna_recovery_state{state=\"degraded\"} 1"),
        "{scrape}"
    );

    node.terminate().await;
    Ok(())
}

/// SIGTERM during active recovery must drain in order and exit cleanly.
#[tokio::test]
async fn sigterm_drains_recovery() -> TestResult<()> {
    use std::os::unix::process::ExitStatusExt;

    let env = process::NodeEnv::new();

    let mut first = env.launch();
    first.wait_status("/readyz", StatusCode::OK).await;
    first.terminate().await;

    inject_offline_peers(&env, 2).await?;

    let mut node = env.launch();
    node.wait_status("/readyz", StatusCode::OK).await;
    // Recovery is blocked on unreachable peers and the drain timer is armed.
    node.wait_log("startup.recovery.degraded").await;

    let status = node.terminate().await;
    assert_eq!(
        status.code(),
        Some(0),
        "graceful shutdown must exit 0, not the forced-exit code"
    );
    assert_eq!(
        status.signal(),
        None,
        "the process must not be killed by a signal"
    );
    Ok(())
}
