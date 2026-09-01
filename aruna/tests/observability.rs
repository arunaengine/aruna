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
            .get(format!("{}/api/v1/system/info", seed.base_url))
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
    use std::io::{Read, Seek, SeekFrom};
    use std::net::{TcpListener, UdpSocket};
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::time::Duration;
    use tokio::time::Instant;

    /// Deadlock guard only: a breach means the process never reached the gate.
    /// Never a speed assertion.
    pub const HANG_GUARD: Duration = Duration::from_secs(240);

    pub struct NodeEnv {
        dir: tempfile::TempDir,
    }

    #[derive(Default)]
    struct LaunchPaths {
        core: Option<PathBuf>,
        recovery: Option<PathBuf>,
        outbox: Option<PathBuf>,
        ledger: Option<PathBuf>,
    }

    impl LaunchPaths {
        fn env_lines(&self) -> String {
            let mut lines = String::new();
            for (key, path) in [
                ("ARUNA_TEST_CORE_PUBLICATION_BARRIER", self.core.as_deref()),
                ("ARUNA_TEST_RECOVERY_BARRIER", self.recovery.as_deref()),
                ("ARUNA_TEST_OUTBOX_BARRIER", self.outbox.as_deref()),
                ("ARUNA_TEST_OUTBOX_LEDGER", self.ledger.as_deref()),
            ] {
                if let Some(path) = path {
                    lines.push_str(&format!("{key}={}\n", path.display()));
                }
            }
            lines
        }
    }

    struct Ports {
        http: TcpListener,
        p2p: UdpSocket,
        ops: TcpListener,
        s3: TcpListener,
    }

    fn bind_port() -> TcpListener {
        TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port")
    }

    fn bind_udp() -> UdpSocket {
        UdpSocket::bind("127.0.0.1:0").expect("bind ephemeral port")
    }

    impl Ports {
        fn new() -> Self {
            Self {
                http: bind_port(),
                p2p: bind_udp(),
                ops: bind_port(),
                s3: bind_port(),
            }
        }

        fn values(&self) -> (u16, u16, u16, u16) {
            (
                self.http.local_addr().expect("http addr").port(),
                self.p2p.local_addr().expect("p2p addr").port(),
                self.ops.local_addr().expect("ops addr").port(),
                self.s3.local_addr().expect("s3 addr").port(),
            )
        }
    }

    impl NodeEnv {
        pub fn new() -> Self {
            Self {
                dir: tempfile::tempdir().expect("temp dir"),
            }
        }

        pub fn root(&self) -> &Path {
            self.dir.path()
        }

        pub fn storage_path(&self) -> PathBuf {
            self.root().join("storage")
        }

        pub fn log_path(&self) -> PathBuf {
            self.root().join("node.log")
        }

        pub fn ledger_path(&self) -> PathBuf {
            self.root().join("outbox.ledger")
        }

        fn write_env_file(&self, http: u16, p2p: u16, ops: u16, s3: u16, paths: &LaunchPaths) {
            let storage = self.storage_path();
            let test_env = paths.env_lines();
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
                 REALM_DESCRIPTION=\"observability harness realm\"\n\
                 ARUNA_COMPUTE_EXECUTOR=none\n\
                 ARUNA_SHUTDOWN_GRACE_SECS=16\n\
                 {test_env}\
                 RUST_LOG=info\n",
                storage = storage.display(),
                http = http,
                p2p = p2p,
                ops = ops,
                s3 = s3,
            );
            std::fs::write(self.root().join(".env"), body).expect("write .env");
        }

        pub fn launch(&self) -> NodeProcess {
            self.launch_mode(LaunchPaths::default())
        }

        pub fn launch_core(&self) -> NodeProcess {
            let paths = LaunchPaths {
                core: Some(self.root().join("core-publication.barrier")),
                ..LaunchPaths::default()
            };
            self.clear_paths(&paths);
            self.launch_mode(paths)
        }

        pub fn launch_ledger(&self) -> NodeProcess {
            let ledger = self.ledger_path();
            let paths = LaunchPaths {
                ledger: Some(ledger.clone()),
                ..LaunchPaths::default()
            };
            self.clear_paths(&paths);
            std::fs::File::create(ledger).expect("create outbox ledger");
            self.launch_mode(paths)
        }

        pub fn launch_recovery(&self) -> NodeProcess {
            let paths = LaunchPaths {
                recovery: Some(self.root().join("recovery.barrier")),
                outbox: Some(self.root().join("outbox.barrier")),
                ..LaunchPaths::default()
            };
            self.clear_paths(&paths);
            self.launch_mode(paths)
        }

        fn clear_paths(&self, paths: &LaunchPaths) {
            for path in [
                paths.core.as_deref(),
                paths.recovery.as_deref(),
                paths.outbox.as_deref(),
                paths.ledger.as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                let _ = std::fs::remove_file(path);
            }
        }

        fn launch_mode(&self, paths: LaunchPaths) -> NodeProcess {
            let ports = Ports::new();
            let (http_port, p2p_port, ops_port, s3_port) = ports.values();
            let log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.log_path())
                .expect("open node log");
            let errlog = log.try_clone().expect("clone node log");
            // Every launch appends to one log, so a launch only ever reads past
            // what its predecessors already wrote.
            let log_start = std::fs::metadata(self.log_path())
                .map(|meta| meta.len() as usize)
                .unwrap_or(0);
            self.write_env_file(http_port, p2p_port, ops_port, s3_port, &paths);
            let mut command = Command::new(env!("CARGO_BIN_EXE_aruna"));
            command
                .current_dir(self.root())
                .stdin(Stdio::null())
                .stdout(Stdio::from(log))
                .stderr(Stdio::from(errlog));
            drop(ports);
            let child = command.spawn().expect("spawn aruna process");
            NodeProcess {
                child,
                ops_url: format!("http://127.0.0.1:{ops_port}"),
                rest_url: format!("http://127.0.0.1:{http_port}"),
                log_path: self.log_path(),
                log_start,
                paths,
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

        /// Exact document-sync outbox rows of the stopped node.
        pub async fn outbox_rows(
            &self,
        ) -> Vec<(Vec<u8>, aruna_core::document::DocumentSyncOutboxRecord)> {
            let storage = self.open_storage().await;
            let rows = read_outbox(&storage).await;
            // close() waits for the lock release a plain drop only schedules;
            // the next launch would die on a still-locked store.
            storage.close().await;
            rows
        }
    }

    pub async fn read_outbox(
        storage: &StorageHandle,
    ) -> Vec<(Vec<u8>, aruna_core::document::DocumentSyncOutboxRecord)> {
        let mut start: Option<aruna_core::types::Key> = None;
        let mut rows = Vec::new();
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
            rows.extend(values.into_iter().map(|(key, value)| {
                let record = postcard::from_bytes(&value).expect("outbox record decodes");
                (key.to_vec(), record)
            }));
            match next_start_after {
                Some(next) => start = Some(next),
                None => break,
            }
        }
        rows
    }

    pub struct NodeProcess {
        child: Child,
        pub ops_url: String,
        pub rest_url: String,
        log_path: PathBuf,
        log_start: usize,
        paths: LaunchPaths,
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

        /// Only what this launch logged.
        pub fn own_logs(&self) -> String {
            let logs = self.logs();
            logs.get(self.log_start..).unwrap_or_default().to_string()
        }

        fn own_log_chunk(&self, offset: &mut u64) -> String {
            let Ok(mut log) = std::fs::File::open(&self.log_path) else {
                return String::new();
            };
            if log.seek(SeekFrom::Start(*offset)).is_err() {
                return String::new();
            }
            let mut bytes = Vec::new();
            if log.read_to_end(&mut bytes).is_err() {
                return String::new();
            }
            *offset = (*offset).saturating_add(bytes.len() as u64);
            String::from_utf8_lossy(&bytes).into_owned()
        }

        /// Waits until the ops listener answers `path` with `expect`.
        pub async fn wait_status(&mut self, path: &str, expect: StatusCode) -> String {
            self.wait_body(path, expect, "").await
        }

        /// Waits for readiness to report draining while liveness remains up.
        pub async fn wait_draining(&mut self) -> String {
            let body = self
                .wait_body(
                    "/readyz",
                    StatusCode::SERVICE_UNAVAILABLE,
                    "node is draining",
                )
                .await;
            let health = probe_client()
                .get(format!("{}/healthz", self.ops_url))
                .send()
                .await
                .expect("liveness probe during shutdown");
            assert_eq!(
                health.status(),
                StatusCode::OK,
                "ops listener must remain up"
            );
            body
        }

        /// Same, but also waiting for `needle` in the body. Recovery cycles
        /// between degraded and running, so its report has to be awaited.
        pub async fn wait_body(&mut self, path: &str, expect: StatusCode, needle: &str) -> String {
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
                        if status == expect && body.contains(needle) {
                            return body;
                        }
                        last = format!("status {status}: {body}");
                    }
                    Err(error) => last = error.to_string(),
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            panic!(
                "{path} never returned {expect} with {needle:?} (last {last})\n{}",
                self.logs()
            );
        }

        /// Waits until `needle` appears in this launch's own log output.
        pub async fn wait_log(&mut self, needle: &str) {
            let deadline = Instant::now() + HANG_GUARD;
            let mut offset = self.log_start as u64;
            let mut pending = String::new();
            while Instant::now() < deadline {
                pending.push_str(&self.own_log_chunk(&mut offset));
                if pending.contains(needle) {
                    return;
                }
                if let Some(index) = pending.rfind('\n') {
                    pending = pending.split_off(index + 1);
                }
                if !self.is_running() {
                    panic!("process exited before logging {needle}\n{}", self.logs());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            panic!("never logged {needle}\n{}", self.logs());
        }

        /// Waits for the tracked core publication barrier to reach `expected`.
        pub async fn wait_core(&mut self, expected: &str) {
            let path = self
                .paths
                .core
                .as_ref()
                .expect("core barrier path is present")
                .clone();
            self.wait_path(path, expected).await;
        }

        /// Waits for the tracked recovery child barrier to reach `expected`.
        pub async fn wait_recovery(&mut self, expected: &str) {
            let path = self
                .paths
                .recovery
                .as_ref()
                .expect("recovery barrier path is present")
                .clone();
            self.wait_path(path, expected).await;
        }

        /// Waits for the tracked outbox drain barrier to reach `expected`.
        pub async fn wait_outbox(&mut self, expected: &str) {
            let path = self
                .paths
                .outbox
                .as_ref()
                .expect("outbox barrier path is present")
                .clone();
            self.wait_path(path, expected).await;
        }

        async fn wait_path(&mut self, path: PathBuf, expected: &str) {
            let deadline = Instant::now() + HANG_GUARD;
            let reached = |path: &PathBuf| {
                std::fs::read_to_string(path)
                    .map(|value| value.trim() == expected)
                    .unwrap_or(false)
            };
            while Instant::now() < deadline {
                if reached(&path) {
                    return;
                }
                if !self.is_running() {
                    // The exit check races the write, so the file decides: an
                    // exited process cannot have written the barrier late.
                    assert!(
                        reached(&path),
                        "process exited before test barrier reached {expected}\n{}",
                        self.logs()
                    );
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            panic!("test barrier never reached {expected}\n{}", self.logs());
        }

        /// Waits for a complete drain invocation before sampling the store.
        pub async fn wait_drain_quiet(&mut self) {
            let deadline = Instant::now() + HANG_GUARD;
            let mut offset = self.log_start as u64;
            let mut pending = String::new();
            let mut summary_seen = false;
            while Instant::now() < deadline {
                if !self.is_running() {
                    panic!("process exited before a quiescent drain\n{}", self.logs());
                }
                pending.push_str(&self.own_log_chunk(&mut offset));
                for line in pending.lines() {
                    summary_seen |= line.contains("pipeline.drain.summary")
                        && line.contains("rotation_complete=true")
                        && line.contains("has_unvisited=false");
                    if summary_seen && line.contains("pipeline.drain.rotation") {
                        return;
                    }
                }
                if let Some(index) = pending.rfind('\n') {
                    pending = pending.split_off(index + 1);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            panic!("never reached a quiescent drain\n{}", self.logs());
        }

        pub fn signal(&self, name: &str) {
            let status = Command::new("kill")
                .arg(format!("-{name}"))
                .arg(self.pid().to_string())
                .status()
                .expect("send signal");
            assert!(status.success(), "kill -{name} failed");
        }

        pub fn freeze(&self) {
            self.signal("STOP");
        }

        /// Sends SIGTERM and waits for exit inside the hang guard.
        pub async fn terminate(mut self) -> std::process::ExitStatus {
            self.signal("TERM");
            self.wait_exit().await
        }

        /// Resumes a frozen process after delivering SIGTERM.
        pub async fn resume_term(mut self) -> std::process::ExitStatus {
            self.signal("TERM");
            self.signal("CONT");
            self.wait_exit().await
        }

        /// Freezes a quiescent process before delivering SIGTERM.
        pub async fn stop_terminate(self) -> std::process::ExitStatus {
            self.freeze();
            self.resume_term().await
        }

        /// Waits for this launch to exit inside the deadlock guard.
        pub async fn wait_exit(&mut self) -> std::process::ExitStatus {
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

/// Adds synthetic, never-dialled peers and bounds their placement workload.
async fn inject_offline_peers(env: &process::NodeEnv, count: u8) -> TestResult<()> {
    use aruna_core::effects::{IterStart, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
    use aruna_core::structs::{
        PlacementStrategy, RealmConfigDocument, RealmDiscoveryConfig, RealmNodeKind,
    };

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
    config.discovery = RealmDiscoveryConfig::Static {
        endpoints: Vec::new(),
    };
    // Keep outage recovery bounded to the fewest synthetic topics.
    for strategy in &mut config.strategies {
        strategy.shard_count = if count == 1 { 1 } else { 2 };
    }
    // Extra strategy coverage remains in the two-peer outage fixtures.
    if count > 1 {
        config.strategies.push(PlacementStrategy {
            strategy_id: ulid::Ulid::from_bytes([0x5A; 16]),
            name: "offline-harness".to_string(),
            replica_count: None,
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 2,
        });
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
    storage.close().await;
    Ok(())
}

async fn load_state(
    storage: &aruna_storage::StorageHandle,
) -> TestResult<aruna::config::PersistedNodeState> {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::NODE_STATE_KEYSPACE;

    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: b"node_state".to_vec().into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => Ok(postcard::from_bytes::<aruna::config::PersistedNodeState>(
            &value,
        )?),
        other => Err(format!("unexpected node state event: {other:?}").into()),
    }
}

async fn load_realm(
    storage: &aruna_storage::StorageHandle,
) -> TestResult<aruna_core::structs::RealmConfigDocument> {
    use aruna_core::effects::{IterStart, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
    use aruna_core::structs::RealmConfigDocument;

    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            prefix: None,
            start: None::<IterStart>,
            limit: 8,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values
            .into_iter()
            .find_map(|(_, value)| RealmConfigDocument::from_bytes(&value).ok())
            .ok_or("realm config missing")?),
        other => Err(format!("unexpected realm config event: {other:?}").into()),
    }
}

async fn clear_space(storage: &aruna_storage::StorageHandle, key_space: &str) -> TestResult<()> {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};

    let values = match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: None,
            start: None,
            limit: 4096,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values,
        other => return Err(format!("unexpected {key_space} event: {other:?}").into()),
    };
    if values.is_empty() {
        return Ok(());
    }
    let deletes = values
        .into_iter()
        .map(|(key, _)| (key_space.to_string(), key))
        .collect();
    let event = storage
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await;
    assert!(
        matches!(
            event,
            Event::Storage(StorageEvent::BatchDeleteResult { .. })
        ),
        "unexpected {key_space} delete event: {event:?}"
    );
    Ok(())
}

/// Leaves one valid document-sync row for the first post-start drain.
async fn inject_outbox(env: &process::NodeEnv) -> TestResult<Vec<u8>> {
    use aruna_core::document::{
        DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent, DocumentSyncRevision,
        DocumentSyncTarget,
    };
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{DOCUMENT_SYNC_OUTBOX_KEYSPACE, TASK_TIMER_KEYSPACE};
    use aruna_core::structs::PlacementRef;
    use aruna_operations::document_sync_outbox::{new_outbox_record, outbox_write_entry};

    let storage = env.open_storage().await;
    clear_space(&storage, DOCUMENT_SYNC_OUTBOX_KEYSPACE).await?;
    clear_space(&storage, TASK_TIMER_KEYSPACE).await?;
    let state = load_state(&storage).await?;
    let config = load_realm(&storage).await?;
    let node_id = iroh::SecretKey::from_bytes(&state.net_secret_key).public();
    let placement = PlacementRef::NIL;
    let record = new_outbox_record(
        node_id,
        DocumentSyncTarget::NodeInfo {
            realm_id: config.realm_id,
            node_id,
        },
        Vec::new(),
        DocumentSyncOutboxEvent::Upsert {
            bytes: Vec::new(),
            change: DocumentSyncChange {
                base: None,
                current: DocumentSyncRevision {
                    generation: 1,
                    event_id: ulid::Ulid::from_parts(7, 1),
                    actor: node_id,
                    updated_at_ms: 1,
                },
                kind: DocumentSyncChangeKind::Upsert,
                placement,
            },
        },
        placement,
        true,
    );
    let (key_space, key, value) = outbox_write_entry(&record)?;
    let event = storage
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key: key.clone(),
            value,
            txn_id: None,
        })
        .await;
    assert!(
        matches!(event, Event::Storage(StorageEvent::WriteResult { .. })),
        "unexpected outbox write event: {event:?}"
    );
    storage.sync_all().await?;
    storage.close().await;
    Ok(key.to_vec())
}

/// The delta between two restarts against unchanged state is what startup itself
/// adds to the document-sync outbox; it must be exactly zero.
#[tokio::test]
async fn restart_preserves_outbox() -> TestResult<()> {
    let env = process::NodeEnv::new();

    let mut first = env.launch();
    first.wait_status("/readyz", StatusCode::OK).await;
    // The initial boot publishes full usage; terminating before that converged
    // leaves a durable retry that the measured start would legitimately replay.
    first.wait_log("startup.recovery.complete").await;
    first.terminate().await;

    inject_offline_peers(&env, 2).await?;

    let mut second = env.launch();
    second.wait_status("/healthz", StatusCode::OK).await;
    second.wait_log("startup.recovery.degraded").await;
    second.wait_drain_quiet().await;
    assert!(
        second.stop_terminate().await.success(),
        "baseline shutdown must complete durably"
    );
    let before = env.outbox_rows().await;

    let mut third = env.launch_ledger();
    third.wait_status("/healthz", StatusCode::OK).await;
    third.wait_log("startup.recovery.degraded").await;
    third.wait_drain_quiet().await;
    third.freeze();
    let ledger = std::fs::read_to_string(env.ledger_path()).expect("read outbox ledger");
    assert!(
        ledger.is_empty(),
        "unchanged restart attempted outbox enqueues:\n{ledger}"
    );
    assert!(
        third.resume_term().await.success(),
        "restart shutdown must complete durably"
    );
    let after = env.outbox_rows().await;

    assert_eq!(
        after, before,
        "an unchanged restart must add no document sync outbox work"
    );
    Ok(())
}

/// SIGTERM must join the tracked core publisher before storage sealing.
#[tokio::test]
async fn sigterm_joins_core() -> TestResult<()> {
    use std::os::unix::process::ExitStatusExt;

    let env = process::NodeEnv::new();
    let mut first = env.launch();
    first.wait_status("/readyz", StatusCode::OK).await;
    first.terminate().await;

    let mut node = env.launch_core();
    node.wait_status("/readyz", StatusCode::OK).await;
    node.wait_core("active").await;
    node.signal("TERM");
    node.wait_core("joined").await;

    let status = node.wait_exit().await;
    assert_eq!(status.code(), Some(0), "core shutdown must exit cleanly");
    assert_eq!(status.signal(), None, "core shutdown must not be signaled");

    let logs = node.own_logs();
    let joined = logs
        .find("event=\"test.core_publication.joined\"")
        .expect("core publication join must be observable");
    let background = logs
        .find("phase=\"background\"")
        .expect("background shutdown phase missing");
    let complete = logs
        .find("Shutdown complete")
        .expect("shutdown completion missing");
    assert!(joined < background && background < complete, "{logs}");
    assert!(!logs.contains("storage.write.after_seal"), "{logs}");
    assert!(!logs.contains("storage.write.after_fence"), "{logs}");
    assert!(logs.contains("rejected_writes=0"), "{logs}");
    Ok(())
}

async fn prepare_shutdown(env: &process::NodeEnv) -> TestResult<process::NodeProcess> {
    let mut first = env.launch();
    first.wait_status("/readyz", StatusCode::OK).await;
    first.terminate().await;

    inject_offline_peers(env, 1).await?;
    let outbox_key = inject_outbox(env).await?;
    assert!(
        env.outbox_rows()
            .await
            .iter()
            .any(|(key, _)| key == &outbox_key),
        "the shutdown case must start with durable outbox work"
    );

    let mut node = env.launch_recovery();
    node.wait_status("/readyz", StatusCode::OK).await;
    node.wait_recovery("active").await;
    node.wait_outbox("active").await;
    Ok(node)
}

async fn wait_children(node: &mut process::NodeProcess) {
    node.wait_recovery("joined").await;
    node.wait_outbox("joined").await;
}

fn assert_shutdown(logs: &str) {
    let recovery = logs
        .find("event=\"test.recovery.joined\"")
        .expect("recovery join must be observable");
    let outbox = logs
        .find("event=\"test.outbox.joined\"")
        .expect("outbox join must be observable");
    let draining = logs
        .find("Shutdown: readiness gate closed, draining")
        .expect("shutdown must close readiness first");
    let tasks = logs
        .find("Shutdown: task scheduler drained")
        .unwrap_or_else(|| panic!("task children did not finish\n{logs}"));
    let background = logs
        .find("phase=\"background\"")
        .unwrap_or_else(|| panic!("background phase missing\n{logs}"));
    // Completion is logged only after storage seal and final sync.
    let sealed = logs
        .find("Shutdown complete")
        .expect("shutdown must finish after storage sync");
    assert!(
        draining < outbox
            && draining < recovery
            && outbox < tasks
            && recovery < background
            && outbox < sealed
            && recovery < sealed
            && background < sealed,
        "{logs}"
    );
    assert!(
        !logs.contains("Background children failed to drain before shutdown continued"),
        "{logs}"
    );
    assert!(!logs.contains("storage.write.after_seal"), "{logs}");
    assert!(!logs.contains("storage.write.after_fence"), "{logs}");
    assert!(logs.contains("rejected_writes=0"), "{logs}");
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
        .get(format!("{}/api/v1/system/info", node.rest_url))
        .send()
        .await?;
    assert_eq!(info.status(), StatusCode::OK);
    let realm = client
        .get(format!("{}/api/v1/system/realm", node.rest_url))
        .send()
        .await?;
    assert_eq!(realm.status(), StatusCode::OK);
    let realm: serde_json::Value = realm.json().await?;
    assert_eq!(realm["description"], "observability harness realm");
    assert!(realm["realm_id"].as_str().is_some(), "missing realm id");

    node.wait_log("startup.recovery.degraded").await;
    assert!(node.is_running(), "a peer outage must not end the process");
    assert_eq!(node.wait_status("/healthz", StatusCode::OK).await, "ok");

    let body = node
        .wait_body("/readyz", StatusCode::OK, "\"state\":\"degraded\"")
        .await;
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

    let scrape = node
        .wait_body(
            "/metrics",
            StatusCode::OK,
            "aruna_recovery_state{state=\"degraded\"} 1",
        )
        .await;
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
    let mut node = prepare_shutdown(&env).await?;

    node.signal("TERM");
    // Readiness closes before the children join, so sample it first; waiting for
    // the joins would leave only the last instant before exit to observe it in.
    let body = node.wait_draining().await;
    wait_children(&mut node).await;
    let ready: serde_json::Value = serde_json::from_str(&body)?;
    assert_eq!(ready["ready"], serde_json::json!(false));
    assert_eq!(
        ready["checks"]["startup"],
        serde_json::json!("failed: node is draining")
    );

    let status = node.wait_exit().await;
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
    assert_shutdown(&node.own_logs());
    Ok(())
}

/// A second SIGTERM must force the process out of an active drain.
#[tokio::test]
async fn second_signal_exits() -> TestResult<()> {
    use std::os::unix::process::ExitStatusExt;

    // The pinned recovery and outbox children keep the drain active until the
    // second signal lands; without them shutdown can win the /readyz probe.
    let env = process::NodeEnv::new();
    let mut node = prepare_shutdown(&env).await?;

    node.signal("TERM");
    node.wait_draining().await;
    node.signal("TERM");

    let status = node.wait_exit().await;
    assert_eq!(
        status.signal(),
        None,
        "forced exit should use the documented code"
    );
    assert_eq!(status.code(), Some(143), "second SIGTERM must force exit");
    Ok(())
}
