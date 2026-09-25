//! A stateful Invenio fixture with records, drafts, versions and a required bearer token.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use std::collections::BTreeMap;

#[derive(Clone)]
pub(super) struct Rec {
    pub(super) parent: String,
    index: u64,
    pub(super) published: bool,
    pub(super) revision: u64,
    reserved: bool,
    pub(super) review: Option<String>,
    pub(super) metadata: Value,
    custom_fields: Value,
    pub(super) files: BTreeMap<String, (Option<Vec<u8>>, bool)>,
}

#[derive(Default)]
pub(super) struct Remote {
    origin: String,
    pub(super) token: String,
    /// Community submissions go here; the fixture answers for this one community only.
    pub(super) community: Option<String>,
    /// Refuses file content like a repository that rejects the upload.
    pub(super) reject_uploads: bool,
    /// Answers the next DOI reservation with 429, like a busy repository.
    pub(super) busy_reserve: bool,
    next: u64,
    pub(super) records: BTreeMap<String, Rec>,
    pub(super) calls: Vec<(Method, String)>,
}

impl Remote {
    fn json(&self, id: &str) -> Value {
        let rec = &self.records[id];
        let pids = if rec.published || rec.reserved {
            json!({"doi": {"identifier": format!("10.1234/{id}"), "provider": "datacite"}})
        } else {
            json!({})
        };
        let review = rec
            .review
            .as_ref()
            .map_or(Value::Null, |status| json!({"status": status}));
        json!({"id": id, "parent": {"id": rec.parent, "review": review,
                "pids": {"doi": {"identifier": format!("10.1234/{}", rec.parent)}}},
            "revision_id": rec.revision,
            "is_published": rec.published, "metadata": rec.metadata,
            "custom_fields": rec.custom_fields, "files": {"enabled": true},
            "versions": {"index": rec.index, "is_latest": self.lineage(&rec.parent).first().is_some_and(|latest| latest == id)},
            "pids": pids,
            "links": {"self_html": format!("{}/records/{id}", self.origin)}})
    }

    fn files(&self, id: &str) -> Value {
        let entries = self.records[id]
            .files
            .iter()
            .map(|(key, (bytes, committed))| match bytes {
                Some(bytes) => file(key, bytes, *committed),
                None => json!({"key": key, "status": "pending"}),
            })
            .collect::<Vec<_>>();
        json!({"entries": entries, "links": {}})
    }

    /// Published versions of one lineage, newest first.
    fn lineage(&self, parent: &str) -> Vec<String> {
        let mut versions = self
            .records
            .iter()
            .filter(|(_, rec)| rec.parent == parent && rec.published)
            .map(|(id, rec)| (rec.index, id.clone()))
            .collect::<Vec<_>>();
        versions.sort();
        versions.into_iter().rev().map(|(_, id)| id).collect()
    }

    fn page(&self, ids: Vec<String>) -> Value {
        let hits = ids
            .iter()
            .take(1)
            .map(|id| self.json(id))
            .collect::<Vec<_>>();
        json!({"hits": {"total": ids.len(), "hits": hits}, "links": {}})
    }

    pub(super) fn insert(&mut self, parent: Option<String>, rec_from: Option<&Rec>) -> String {
        self.next += 1;
        let id = self.next.to_string();
        let parent = parent.unwrap_or_else(|| format!("p{id}"));
        let index = self.lineage(&parent).len() as u64 + 1;
        let (metadata, custom_fields) = rec_from
            .map(|rec| (rec.metadata.clone(), rec.custom_fields.clone()))
            .unwrap_or((json!({}), json!({})));
        self.records.insert(
            id.clone(),
            Rec {
                parent,
                index,
                published: false,
                revision: 1,
                reserved: false,
                review: None,
                metadata,
                custom_fields,
                files: BTreeMap::new(),
            },
        );
        id
    }
}

async fn remote_request(State(state): State<Arc<Mutex<Remote>>>, request: Request) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let query = request.uri().query().unwrap_or("").to_string();
    let bearer = request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let if_match = request
        .headers()
        .get("if-match")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let body = to_bytes(request.into_body(), 1024 * 1024).await.unwrap();
    let mut state = state.lock().unwrap();
    state.calls.push((method.clone(), path.clone()));
    if bearer != Some(format!("Bearer {}", state.token)) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let parts = path
        .trim_start_matches('/')
        .split('/')
        .map(|part| {
            percent_encoding::percent_decode_str(part)
                .decode_utf8()
                .unwrap()
                .into_owned()
        })
        .collect::<Vec<_>>();
    let parts = parts.iter().map(String::as_str).collect::<Vec<_>>();
    let draft = |state: &Remote, id: &str| state.records.get(id).is_some_and(|rec| !rec.published);
    let value = match (method, parts.as_slice()) {
        (Method::GET, ["api", "records"]) => {
            let parent = query
                .split('&')
                .find_map(|pair| pair.strip_prefix("q=parent.id%3A"))
                .unwrap_or_default()
                .to_string();
            state.page(state.lineage(&parent))
        }
        (Method::POST, ["api", "records"]) => {
            let body: Value = serde_json::from_slice(&body).unwrap();
            let id = state.insert(None, None);
            let rec = state.records.get_mut(&id).unwrap();
            rec.metadata = body["metadata"].clone();
            rec.custom_fields = body["custom_fields"].clone();
            state.json(&id)
        }
        (Method::GET, ["api", "records", id, "draft"]) if draft(&state, id) => state.json(id),
        (Method::PUT, ["api", "records", id, "draft"]) if draft(&state, id) => {
            let body: Value = serde_json::from_slice(&body).unwrap();
            let rec = state.records.get_mut(*id).unwrap();
            if if_match != Some(rec.revision) {
                return StatusCode::PRECONDITION_FAILED.into_response();
            }
            rec.metadata = body["metadata"].clone();
            rec.custom_fields = body["custom_fields"].clone();
            // Like Zenodo, a body without pids drops the reserved DOI.
            rec.reserved &= body["pids"]["doi"]["identifier"].is_string();
            rec.revision += 2;
            state.json(id)
        }
        (Method::POST, ["api", "records", id, "draft", "pids", "doi"]) if draft(&state, id) => {
            if std::mem::take(&mut state.busy_reserve) {
                return StatusCode::TOO_MANY_REQUESTS.into_response();
            }
            let rec = state.records.get_mut(*id).unwrap();
            if rec.reserved {
                return StatusCode::BAD_REQUEST.into_response();
            }
            rec.reserved = true;
            rec.revision += 2;
            state.json(id)
        }
        (Method::PUT, ["api", "records", id, "draft", "review"]) if draft(&state, id) => {
            let body: Value = serde_json::from_slice(&body).unwrap();
            if Some(body["receiver"]["community"].as_str().unwrap()) != state.community.as_deref() {
                return StatusCode::BAD_REQUEST.into_response();
            }
            state.records.get_mut(*id).unwrap().review = Some("created".into());
            json!({"status": "created"})
        }
        (Method::POST, ["api", "records", id, "draft", "actions", "submit-review"])
            if draft(&state, id) =>
        {
            let rec = state.records.get_mut(*id).unwrap();
            assert_eq!(rec.review.as_deref(), Some("created"));
            rec.review = Some("submitted".into());
            return (
                StatusCode::ACCEPTED,
                axum::Json(json!({"status": "submitted"})),
            )
                .into_response();
        }
        (Method::GET, ["api", "communities", slug]) if state.community.is_some() => {
            json!({"id": state.community, "slug": slug})
        }
        (Method::GET, ["api", "records", id, "versions"]) => {
            let Some(parent) = state.records.get(*id).map(|rec| rec.parent.clone()) else {
                return StatusCode::NOT_FOUND.into_response();
            };
            state.page(state.lineage(&parent))
        }
        (Method::GET, ["api", "records", id])
            if state.records.get(*id).is_some_and(|r| r.published) =>
        {
            state.json(id)
        }
        (Method::POST, ["api", "records", id, "versions"]) => {
            let source = state.records.get(*id).filter(|rec| rec.published).cloned();
            let Some(source) = source else {
                return StatusCode::NOT_FOUND.into_response();
            };
            let open = state
                .records
                .iter()
                .find(|(_, rec)| rec.parent == source.parent && !rec.published)
                .map(|(id, _)| id.clone());
            let id =
                open.unwrap_or_else(|| state.insert(Some(source.parent.clone()), Some(&source)));
            state.json(&id)
        }
        (Method::GET, ["api", "records", id, "draft", "files"]) if draft(&state, id) => {
            state.files(id)
        }
        (Method::GET, ["api", "records", id, "files"]) if !draft(&state, id) => state.files(id),
        (Method::POST, ["api", "records", id, "draft", "files"]) if draft(&state, id) => {
            let body: Value = serde_json::from_slice(&body).unwrap();
            let key = body[0]["key"].as_str().unwrap().to_string();
            let rec = state.records.get_mut(*id).unwrap();
            assert!(rec.files.insert(key.clone(), (None, false)).is_none());
            // Like Invenio, file changes move the draft revision.
            rec.revision += 1;
            json!({"entries": [{"key": key, "status": "pending"}]})
        }
        (Method::PUT, ["api", "records", id, "draft", "files", key, "content"]) => {
            if state.reject_uploads {
                return StatusCode::BAD_REQUEST.into_response();
            }
            let rec = state.records.get_mut(*id).unwrap();
            rec.files.get_mut(*key).unwrap().0 = Some(body.to_vec());
            json!({})
        }
        (Method::POST, ["api", "records", id, "draft", "files", key, "commit"]) => {
            let rec = state.records.get_mut(*id).unwrap();
            let entry = rec.files.get_mut(*key).unwrap();
            entry.1 = true;
            file(key, entry.0.as_ref().unwrap(), true)
        }
        (Method::DELETE, ["api", "records", id, "draft", "files", key]) if draft(&state, id) => {
            state.records.get_mut(*id).unwrap().files.remove(*key);
            return StatusCode::NO_CONTENT.into_response();
        }
        (Method::POST, ["api", "records", id, "draft", "actions", "publish"])
            if draft(&state, id) =>
        {
            let rec = state.records.get_mut(*id).unwrap();
            assert!(
                rec.review.is_none(),
                "a draft in review is published by its community"
            );
            rec.published = true;
            state.json(id)
        }
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    axum::Json(value).into_response()
}

pub(super) struct RemoteServer {
    pub(super) endpoint: String,
    pub(super) state: Arc<Mutex<Remote>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for RemoteServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(super) async fn remote(token: &str) -> RemoteServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let state = Arc::new(Mutex::new(Remote {
        origin: origin.clone(),
        token: token.into(),
        ..Remote::default()
    }));
    let app = Router::new()
        .fallback(remote_request)
        .with_state(state.clone());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    RemoteServer {
        endpoint: format!("{origin}/api/"),
        state,
        task,
    }
}
