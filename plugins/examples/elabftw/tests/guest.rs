//! Runs the plugin built for WASM in the plugin runtime, against the fake eLabFTW and Aruna
//! behind a broker. Set ELABFTW_MODULE to the built module; the CI job does.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod support;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aruna_plugins::broker::{
    Broker, CallContext, Created, Document, Entry, ErrorKind, Field, FileChange, FilePage,
    FullResponse, HostError, Intent, Listing, MetadataCreate, MetadataPatch, Method as Verb,
    ObjectInfo, ObjectPage, Response, SendRequest, StatePut,
};
use aruna_plugins::limits::{Limits, RuntimeConfig};
use aruna_plugins::package::{load_manifest, package};
use aruna_plugins::permission::Permission;
use aruna_plugins::{CancellationToken, Compiled, Invocation, Runtime, Trigger, WatchEvent};
use elabftw_sync::host::{Fault, Host, Kind, Method, Request};
use serde_json::{Value, json};
use support::{BUCKET, START, World};

/// Generous cap so a broken run fails the test instead of hanging it.
const HANG_CAP: Duration = Duration::from_secs(300);

fn refusal(fault: Fault) -> HostError {
    let kind = match fault.kind {
        Kind::NotGranted => ErrorKind::NotGranted,
        Kind::Denied => ErrorKind::Denied,
        Kind::Invalid => ErrorKind::Invalid,
        Kind::Limit => ErrorKind::Limit,
        Kind::Conflict => ErrorKind::Conflict,
        Kind::Unknown => ErrorKind::UnknownOutcome,
        Kind::Unavailable => ErrorKind::Unavailable,
        Kind::NotFound => ErrorKind::NotFound,
    };
    HostError::new(kind, fault.message)
}

fn unused() -> HostError {
    HostError::new(ErrorKind::Unavailable, "not used by the plugin")
}

fn parsed(text: &str) -> Value {
    serde_json::from_str(text).unwrap()
}

/// Serves the guest's host calls from the shared fake.
struct Hosted(Arc<World>);

#[async_trait::async_trait]
impl Broker for Hosted {
    async fn granted(&self, _: &CallContext, _: Permission, _: &str) -> bool {
        true
    }

    async fn metadata_get(&self, _: &CallContext, _: &str) -> Result<Document, HostError> {
        Err(unused())
    }

    async fn metadata_list(
        &self,
        _: &CallContext,
        _: Option<String>,
    ) -> Result<Listing, HostError> {
        Err(unused())
    }

    async fn metadata_summary(
        &self,
        _: &CallContext,
        resource: &str,
    ) -> Result<Document, HostError> {
        let doc = self.0.summary(resource).map_err(refusal)?;
        Ok(Document {
            resource: resource.into(),
            revision: doc.revision,
            json: doc.json.to_string(),
        })
    }

    async fn metadata_files(
        &self,
        _: &CallContext,
        resource: &str,
        cursor: Option<String>,
    ) -> Result<FilePage, HostError> {
        let page = self.0.files(resource, cursor.as_deref()).map_err(refusal)?;
        let files = page.files.iter().map(Value::to_string).collect();
        Ok(FilePage {
            files,
            cursor: page.cursor,
        })
    }

    async fn metadata_patch(
        &self,
        _: &CallContext,
        patch: MetadataPatch,
    ) -> Result<String, HostError> {
        (self
            .0
            .patch(&patch.resource, &parsed(&patch.patch), &patch.expected))
        .map_err(refusal)
    }

    async fn metadata_put_file(
        &self,
        _: &CallContext,
        change: FileChange,
    ) -> Result<String, HostError> {
        (self
            .0
            .put_file(&change.resource, &parsed(&change.file), &change.expected))
        .map_err(refusal)
    }

    async fn metadata_remove_file(
        &self,
        _: &CallContext,
        change: FileChange,
    ) -> Result<String, HostError> {
        (self
            .0
            .remove_file(&change.resource, &change.file, &change.expected))
        .map_err(refusal)
    }

    async fn metadata_create(
        &self,
        _: &CallContext,
        create: MetadataCreate,
    ) -> Result<Created, HostError> {
        let created = self
            .0
            .create(&create.path, &parsed(&create.dataset), &create.key)
            .map_err(refusal)?;
        Ok(Created {
            resource: created.resource,
            revision: created.revision,
        })
    }

    async fn objects_info(&self, _: &CallContext, path: &str) -> Result<ObjectInfo, HostError> {
        let size = self.0.object_size(path).map_err(refusal)?;
        Ok(ObjectInfo {
            path: path.into(),
            size,
            version: "v1".into(),
        })
    }

    async fn objects_list(
        &self,
        _: &CallContext,
        prefix: &str,
        _: Option<String>,
    ) -> Result<ObjectPage, HostError> {
        self.0.objects(prefix).map_err(refusal)?;
        Ok(ObjectPage {
            objects: Vec::new(),
            cursor: None,
        })
    }

    async fn connection_send(
        &self,
        _: &CallContext,
        _: SendRequest,
    ) -> Result<Response, HostError> {
        Err(unused())
    }

    async fn connection_send_full(
        &self,
        _: &CallContext,
        request: SendRequest,
    ) -> Result<FullResponse, HostError> {
        assert_eq!(request.connection, "elab");
        let method = match request.method {
            Verb::Get => Method::Get,
            Verb::Post => Method::Post,
            Verb::Patch => Method::Patch,
            other => panic!("the connection does not allow {other:?}"),
        };
        let json = request
            .headers
            .iter()
            .any(|field| field.name == "content-type" && field.value == "application/json");
        assert_eq!(json, !request.body.is_empty(), "a body is sent as JSON");
        let outgoing = Request {
            method,
            path: request.path,
            query: request
                .query
                .into_iter()
                .map(|field| (field.name, field.value))
                .collect(),
            body: (!request.body.is_empty())
                .then(|| serde_json::from_slice(&request.body).unwrap()),
            key: request.intent.and_then(|intent| intent.key),
        };
        let reply = self.0.elab(&outgoing).map_err(refusal)?;
        let headers = reply
            .headers
            .into_iter()
            .map(|(name, value)| Field { name, value })
            .collect();
        Ok(FullResponse {
            status: reply.status,
            headers,
            body: reply.body,
        })
    }

    async fn state_get(&self, _: &CallContext, key: &str) -> Result<Option<Entry>, HostError> {
        let entry = Host::get(&*self.0, key).map_err(refusal)?;
        Ok(entry.map(|(version, value)| Entry { version, value }))
    }

    async fn state_put(&self, _: &CallContext, put: StatePut) -> Result<u64, HostError> {
        (self.0.put(&put.key, &put.value, put.expected)).map_err(refusal)
    }

    async fn abandoned(&self, _: &CallContext, _: &Intent) {}

    async fn log(&self, _: &CallContext, _: &str, message: &str) {
        self.0.log(message);
    }
}

/// The packaged plugin, or `None` when no module was built for this run.
fn module() -> Option<Vec<u8>> {
    let Some(path) = std::env::var_os("ELABFTW_MODULE") else {
        eprintln!("skipping: set ELABFTW_MODULE to the module built for wasm32-unknown-unknown");
        return None;
    };
    let project = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut manifest = load_manifest(project).unwrap();
    Some(package(&mut manifest, &std::fs::read(path).unwrap()).unwrap())
}

/// The compiled guest and the fake it runs against, at a moving time.
struct Guest {
    runtime: Runtime,
    compiled: Arc<Compiled>,
    world: Arc<World>,
    now: AtomicU64,
    runs: AtomicU64,
}

impl Guest {
    async fn new() -> Option<Self> {
        let runtime = Runtime::new(RuntimeConfig::default()).unwrap();
        let compiled = runtime.prepare(module()?).await.unwrap();
        let (now, runs) = (AtomicU64::new(START), AtomicU64::new(0));
        Some(Self {
            runtime,
            compiled,
            world: Arc::default(),
            now,
            runs,
        })
    }

    /// A run id as the node makes them: a ULID, whose time the guest reads.
    fn run_id(&self) -> String {
        const DIGITS: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
        let (now, run) = (
            self.now.load(Ordering::SeqCst),
            self.runs.fetch_add(1, Ordering::SeqCst),
        );
        let time = (0..10)
            .rev()
            .map(|shift| DIGITS[((now >> (shift * 5)) & 31) as usize] as char);
        format!("{}{run:016}", time.collect::<String>())
    }

    async fn invoke(&self, handler: &str, trigger: Trigger) -> Result<Option<Value>, String> {
        self.invoke_in(handler, trigger, BUCKET).await
    }

    /// A run with the uploads in `bucket`.
    async fn invoke_in(
        &self,
        handler: &str,
        trigger: Trigger,
        bucket: &str,
    ) -> Result<Option<Value>, String> {
        let settings = serde_json::to_string(&json!({
            "elab_url": "https://elab.example.org", "sync_user": support::SYNC_USER, "writers": "3",
            "bucket": bucket, "prefix": "elabftw", "tag": "aruna", "aruna_url": "https://portal.example.org",
        }))
        .unwrap();
        let invocation = Invocation {
            run: self.run_id(),
            install: "install-1".into(),
            group: "lab".into(),
            handler: handler.into(),
            settings,
            trigger,
            permissions: BTreeSet::from([
                Permission::MetadataRead,
                Permission::MetadataWrite,
                Permission::MetadataCreate,
                Permission::ObjectsRead,
            ]),
            connections: BTreeSet::from(["elab".to_string()]),
            limits: Limits::default(),
            cancel: CancellationToken::new(),
        };
        let broker = Arc::new(Hosted(self.world.clone()));
        let run = self.runtime.invoke(&self.compiled, invocation, broker);
        let outcome = tokio::time::timeout(HANG_CAP, run)
            .await
            .expect("the run ended within the hang cap");
        let output = outcome.map_err(|error| format!("{error:?}"))?;
        Ok(output.map(|text| serde_json::from_str(&text).unwrap()))
    }

    async fn poll(&self) -> Value {
        let now = self.now.fetch_add(5 * 60 * 1000, Ordering::SeqCst) + 5 * 60 * 1000;
        self.invoke("poll", Trigger::Timer(now))
            .await
            .unwrap()
            .unwrap()
    }

    async fn event(&self, dataset: &str) {
        let found = self.world.dataset(dataset);
        let change = WatchEvent {
            kind: "dataset_updated".into(),
            path: format!("/r/g/lab/meta/{}@{dataset}", found.path),
            resource: dataset.into(),
            revision: found.revision,
        };
        self.invoke("changed", Trigger::Event(change))
            .await
            .unwrap();
    }

    async fn manual(&self, input: Value) -> Value {
        let output = self
            .invoke("manual", Trigger::Manual(input.to_string()))
            .await;
        output.unwrap().unwrap()
    }

    fn marks(&self, experiment: u64) -> Value {
        self.world
            .state(&format!("link/{experiment}"))
            .unwrap_or_default()["marks"]
            .clone()
    }
}

#[tokio::test]
async fn guest_syncs_both() {
    let Some(guest) = Guest::new().await else {
        return;
    };
    let id = guest
        .world
        .experiment("Run 7", "<p>a</p>", &["aruna", "pcr"]);
    guest.world.upload(id, 1, "gel.png", 42);
    assert_eq!(guest.poll().await["synced"], 1);
    let (dataset, stored) = guest.world.only_dataset();
    assert_eq!(
        (
            stored.entity["name"].clone(),
            stored.entity["keywords"].clone()
        ),
        (json!("Run 7"), json!(["pcr"]))
    );
    assert!(stored.files.contains_key("elabftw/uploads/1"));
    assert_eq!(guest.world.extra(id)["Aruna dataset id"]["value"], dataset);

    // Aruna to eLabFTW: a title, a body eLabFTW filters, and a file shown as a link.
    guest.world.aruna_edit(
        &dataset,
        json!({ "name": "Run 8", "text": "<p>b<br>c</p>" }),
    );
    guest
        .world
        .aruna_file(&dataset, json!({ "@id": "raw.csv", "@type": "File" }));
    guest.event(&dataset).await;
    let experiment = guest.world.get(id);
    assert_eq!(
        (experiment["title"].clone(), experiment["body"].clone()),
        (json!("Run 8"), json!("<p>b<br />c</p>"))
    );
    assert_eq!(
        guest.world.dataset(&dataset).entity["text"],
        "<p>b<br />c</p>"
    );
    let link = format!("https://portal.example.org/datasets/{dataset}/files/raw.csv");
    assert_eq!(guest.world.extra(id)["Aruna files"]["value"], json!([link]));

    // eLabFTW to Aruna; the plugin's own writes are skipped on the way back.
    guest.world.edit(id, |experiment| {
        experiment["body"] = json!("<p>from eLabFTW</p>")
    });
    guest.poll().await;
    assert_eq!(
        guest.world.dataset(&dataset).entity["description"],
        "from eLabFTW"
    );
    let revision = guest.world.dataset(&dataset).revision;
    guest.poll().await;
    guest.event(&dataset).await;
    assert_eq!(guest.world.dataset(&dataset).revision, revision);
    assert_eq!(guest.marks(id), Value::Null);

    // A dataset made in Aruna below meta/elabftw becomes an experiment.
    let made = guest
        .world
        .aruna_dataset("elabftw/new", json!({ "name": "Made in Aruna" }));
    guest.event(&made).await;
    let titles: Vec<Value> = guest
        .world
        .lock()
        .experiments
        .values()
        .map(|experiment| experiment["title"].clone())
        .collect();
    assert!(titles.contains(&json!("Made in Aruna")), "{titles:?}");
}

#[tokio::test]
async fn guest_settles_conflicts() {
    let Some(guest) = Guest::new().await else {
        return;
    };
    let id = guest.world.experiment("Run 7", "", &["aruna"]);
    guest.poll().await;
    let (dataset, _) = guest.world.only_dataset();
    guest
        .world
        .edit(id, |experiment| experiment["title"] = json!("E"));
    guest.world.aruna_edit(&dataset, json!({ "name": "A" }));
    guest.poll().await;
    let report = guest.manual(json!({ "report": true })).await;
    assert_eq!(
        (
            report["totals"]["conflict"].clone(),
            report["marks"][0]["experiment"].clone()
        ),
        (json!(1), json!(id))
    );
    guest
        .manual(json!({ "resolve": id, "keep": "elabftw" }))
        .await;
    assert_eq!(guest.world.dataset(&dataset).entity["name"], "E");
    assert_eq!(guest.marks(id), Value::Null);

    // A timer and a manual run at once still make one link and one dataset; the run that
    // finds the link held fails as retryable.
    let other = guest.world.experiment("Parallel", "", &["aruna"]);
    let input = Trigger::Manual(json!({ "experiment": other }).to_string());
    let (timer, manual) = tokio::join!(guest.poll(), guest.invoke("manual", input));
    assert!(timer.is_object());
    if let Err(held) = manual {
        assert!(
            held.contains("Retryable") && held.contains("another run holds the link"),
            "{held}"
        );
    }
    guest.poll().await;
    assert_eq!(guest.world.lock().datasets.len(), 2);
}

#[tokio::test]
async fn guest_recovers_creates() {
    let Some(guest) = Guest::new().await else {
        return;
    };
    // A lost create reply, then an edit: the keyed create gives the same dataset.
    let id = guest.world.experiment("Run 7", "", &["aruna"]);
    guest.world.lock().create_unknown = true;
    guest.manual(json!({ "experiment": id })).await;
    guest
        .world
        .edit(id, |experiment| experiment["title"] = json!("Run 7b"));
    guest.now.fetch_add(20 * 60 * 1000, Ordering::SeqCst);
    guest.poll().await;
    let (_, stored) = guest.world.only_dataset();
    assert_eq!(stored.entity["name"], "Run 7b");

    // An experiment `POST` with an unknown outcome waits for a person; then a new key creates.
    let dataset = guest
        .world
        .aruna_dataset("elabftw/study", json!({ "name": "Study" }));
    guest.world.lock().post_outcome = Some((Kind::Unknown, false));
    guest.event(&dataset).await;
    guest.poll().await;
    assert_eq!(
        guest.world.lock().posts,
        1,
        "a negative lookup sends no second POST"
    );
    guest
        .manual(json!({ "resolve_create": dataset, "experiment": "absent" }))
        .await;
    assert_eq!(guest.world.lock().posts, 2);

    // One that did land is found through the dataset id field and adopted.
    let landed = guest
        .world
        .aruna_dataset("elabftw/landed", json!({ "name": "Landed" }));
    guest.world.lock().post_outcome = Some((Kind::Unknown, true));
    guest.event(&landed).await;
    guest.poll().await;
    assert_eq!(guest.world.lock().posts, 3);
    assert!(guest.world.state(&format!("dataset/{landed}")).unwrap()["experiment"].is_u64());
}

#[tokio::test]
async fn guest_marks_problems() {
    let Some(guest) = Guest::new().await else {
        return;
    };
    let big = guest.world.experiment("Big", "", &["aruna"]);
    let id = guest.world.experiment("Run 7", "", &["aruna"]);
    guest.world.upload(id, 1, "a.bin", 10);
    guest.world.lock().huge.insert(big);
    guest.poll().await;
    assert!(
        guest.marks(big)["too_large"].is_array(),
        "a reply over the call bound is caught"
    );

    // A person's license on a synced file survives an upload change and a refused revision.
    let dataset = guest.world.state(&format!("link/{id}")).unwrap()["dataset"]
        .as_str()
        .unwrap()
        .to_string();
    let mut file = guest.world.dataset(&dataset).files["elabftw/uploads/1"].clone();
    file["license"] = json!("CC0");
    guest.world.aruna_file(&dataset, file);
    guest.world.upload(id, 1, "b.bin", 11);
    guest.world.lock().race_file = Some(json!({ "keywords": ["k"] }));
    guest.poll().await;
    let file = guest.world.dataset(&dataset).files["elabftw/uploads/1"].clone();
    assert_eq!(
        (file["license"].clone(), file["name"].clone()),
        (json!("CC0"), json!("b.bin"))
    );

    // Exclusive edit mode refuses the write; the link is retried later.
    guest.world.lock().edit_mode = true;
    guest.world.aruna_edit(&dataset, json!({ "name": "Run 8" }));
    guest.event(&dataset).await;
    assert!(
        guest.marks(id)["failed"]
            .to_string()
            .contains("exclusive edit mode")
    );
    guest.world.lock().edit_mode = false;
    guest.now.fetch_add(20 * 60 * 1000, Ordering::SeqCst);
    guest.poll().await;
    assert_eq!(guest.world.get(id)["title"], "Run 8");

    // A write eLabFTW drops shows up as drift in the full reconciliation.
    guest.world.lock().drop_writes = true;
    guest.world.aruna_edit(&dataset, json!({ "name": "Run 9" }));
    guest.event(&dataset).await;
    guest.world.lock().drop_writes = false;
    guest.manual(json!({ "reconcile": true })).await;
    let report = guest.manual(json!({ "report": true })).await;
    assert_eq!(report["totals"]["out_of_sync"], 1);

    // A user field of another type blocks the link fields.
    let clash = guest.world.experiment("Clash", "", &["aruna"]);
    guest.world.edit(clash, |experiment| {
        let field = json!({ "extra_fields": { "Aruna files": { "type": "text", "value": "" } } });
        experiment["metadata"] = json!(field.to_string());
    });
    guest.manual(json!({ "experiment": clash })).await;
    assert_eq!(guest.marks(clash)["blocked"][0], "Aruna files");
}

#[tokio::test]
async fn guest_checks_bucket() {
    let Some(guest) = Guest::new().await else {
        return;
    };
    let accepted = guest.invoke("poll", Trigger::Timer(START)).await;
    assert!(
        accepted.is_ok(),
        "an empty bucket on the receiver is fine: {accepted:?}"
    );
    let elsewhere = "/r/g/lab/data/elsewhere/shared";
    let refused = guest
        .invoke_in("poll", Trigger::Timer(START), elsewhere)
        .await
        .unwrap_err();
    assert!(refused.contains("receiver node"), "{refused}");
    let wrong = guest.invoke("changed", Trigger::Manual("{}".into())).await;
    assert!(wrong.is_err(), "the event handler takes events only");
}
