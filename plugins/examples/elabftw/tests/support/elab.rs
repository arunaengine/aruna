//! The fake eLabFTW API v2: listings, experiments, uploads, tags, extra fields, and the
//! HTML filter and failure switches the tests use.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use elabftw_sync::host::{Kind, Method, Reply, Request, Result};
use serde_json::{Map, Value, json};

use super::{BUCKET, Inner, PERSON, SYNC_USER, World, fault};

fn reply(status: u16, body: Value) -> Result<Reply> {
    Ok(Reply {
        status,
        headers: Vec::new(),
        body: body.to_string().into_bytes(),
    })
}

/// An eLabFTW refusal: `ImproperActionException` answers 400 with its message.
pub(super) fn refuse(message: &str) -> Result<Reply> {
    reply(
        400,
        json!({ "code": 400, "message": message, "description": "" }),
    )
}

/// eLabFTW's HTML filter, as far as the tests need it: scripts go, `<br>` becomes `<br />`.
pub fn purify(html: &str) -> String {
    let mut out = html.replace("<br>", "<br />");
    while let (Some(start), Some(end)) = (out.find("<script"), out.find("</script>")) {
        out.replace_range(start..end + "</script>".len(), "");
    }
    out
}

impl Inner {
    fn tick(&mut self) -> String {
        self.clock += 1;
        format!(
            "2026-11-13 10:{:02}:{:02}",
            self.clock / 60,
            self.clock % 60
        )
    }

    fn tag_id(&mut self, tag: &str) -> u64 {
        let next = self.tag_ids.len() as u64 + 1;
        *self.tag_ids.entry(tag.to_string()).or_insert(next)
    }

    fn tags_of(experiment: &Value) -> Vec<String> {
        let tags = experiment["tags"].as_str().unwrap_or_default();
        tags.split('|')
            .filter(|tag| !tag.is_empty())
            .map(String::from)
            .collect()
    }

    fn set_tags(&mut self, id: u64, tags: &[String]) {
        let ids: Vec<String> = tags
            .iter()
            .map(|tag| self.tag_id(tag).to_string())
            .collect();
        let experiment = self.experiments.get_mut(&id).unwrap();
        experiment["tags"] = json!(tags.join("|"));
        experiment["tags_id"] = json!(ids.join(","));
    }

    fn extra(experiment: &Value) -> Map<String, Value> {
        let metadata: Value = serde_json::from_str(experiment["metadata"].as_str().unwrap_or("{}"))
            .unwrap_or_default();
        metadata["extra_fields"]
            .as_object()
            .cloned()
            .unwrap_or_default()
    }

    fn set_extra(experiment: &mut Value, extra: Map<String, Value>) {
        experiment["metadata"] = json!(json!({ "extra_fields": extra }).to_string());
    }

    /// A change by `user`: eLabFTW stores the filtered body and moves `modified_at`.
    fn touch(&mut self, id: u64, user: u64) {
        let now = self.tick();
        let experiment = self.experiments.get_mut(&id).unwrap();
        experiment["modified_at"] = json!(now);
        experiment["lastchangeby"] = json!(user);
    }

    fn list(&self, query: &BTreeMap<String, String>) -> Value {
        let tag = query.get("tags[]");
        let mut entries: Vec<&Value> = (self.experiments.values())
            .filter(|experiment| tag.is_none_or(|tag| Self::tags_of(experiment).contains(tag)))
            .collect();
        if let Some(extended) = query.get("extended") {
            entries.retain(|experiment| {
                Self::extra(experiment).iter().any(|(name, field)| {
                    let value = field["value"].as_str().unwrap_or_default();
                    *extended == format!("extrafield:\"{name}\":\"{value}\"")
                })
            });
        }
        let pinned = |experiment: &Value| experiment["is_pinned"] == 1;
        match query.get("order").map(String::as_str) {
            Some("lastchange") => entries.sort_by(|a, b| {
                (pinned(b), b["modified_at"].as_str()).cmp(&(pinned(a), a["modified_at"].as_str()))
            }),
            _ => entries.sort_by_key(|experiment| experiment["id"].as_u64()),
        }
        let offset = query
            .get("offset")
            .and_then(|offset| offset.parse().ok())
            .unwrap_or(0);
        let limit = query
            .get("limit")
            .and_then(|limit| limit.parse().ok())
            .unwrap_or(15);
        let page: Vec<Value> = entries
            .into_iter()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect();
        json!(page)
    }

    pub(super) fn elab(&mut self, request: &Request) -> Result<Reply> {
        let query: BTreeMap<String, String> = request.query.iter().cloned().collect();
        let body = request.body.clone().unwrap_or(Value::Null);
        let parts: Vec<&str> = request
            .path
            .split('/')
            .filter(|part| !part.is_empty())
            .collect();
        let id = parts.first().and_then(|id| id.parse::<u64>().ok());
        match (request.method, parts.as_slice()) {
            (Method::Get, []) => reply(200, self.list(&query)),
            (Method::Get, [_]) => {
                let id = id.unwrap();
                if self.huge.contains(&id) {
                    return Err(fault(
                        Kind::Limit,
                        "the answer with status 200 is larger than the call allows",
                    ));
                }
                match self.experiments.get(&id) {
                    Some(experiment) => reply(200, experiment.clone()),
                    None => reply(
                        404,
                        json!({ "code": 404, "description": "Nothing to show with this id" }),
                    ),
                }
            }
            (Method::Get, [_, "uploads"]) => reply(
                200,
                json!(self.uploads.get(&id.unwrap()).cloned().unwrap_or_default()),
            ),
            (Method::Post, []) => self.post(&body),
            (_, _) if self.drop_writes => reply(200, json!({})),
            (Method::Patch, _) if self.edit_mode => reply(
                403,
                json!({ "code": 403, "description": "This entry is in exclusive edit mode" }),
            ),
            (Method::Patch, [_]) => self.patch(id.unwrap(), &body),
            (Method::Post, [_, "tags"]) => {
                let id = id.unwrap();
                let mut tags = Self::tags_of(&self.experiments[&id]);
                tags.push(body["tag"].as_str().unwrap().to_string());
                self.set_tags(id, &tags);
                reply(201, json!({}))
            }
            (Method::Patch, [_, "tags", tag]) => {
                let id = id.unwrap();
                let name = self
                    .tag_ids
                    .iter()
                    .find(|(_, tag_id)| tag_id.to_string() == *tag)
                    .map(|(name, _)| name.clone());
                let tags: Vec<String> = Self::tags_of(&self.experiments[&id])
                    .into_iter()
                    .filter(|tag| Some(tag) != name.as_ref())
                    .collect();
                self.set_tags(id, &tags);
                reply(200, json!({}))
            }
            _ => reply(400, json!({ "code": 400, "description": "unknown route" })),
        }
    }

    fn post(&mut self, body: &Value) -> Result<Reply> {
        self.posts += 1;
        let outcome = self.post_outcome.take();
        if let Some((kind, false)) = outcome {
            return Err(fault(kind, "the request was not completed"));
        }
        let id = self.experiments.keys().last().copied().unwrap_or(100) + 1;
        let html = body["content_type"] != 2;
        let text = body["body"].as_str().unwrap_or_default();
        let stored = if html { purify(text) } else { text.to_string() };
        let experiment = json!({
            "id": id, "title": body["title"], "body": stored, "body_html": stored,
            "content_type": if html { 1 } else { 2 }, "date": "2026-11-13", "status_title": "Running",
            "fullname": "Sync User", "userid": SYNC_USER, "metadata": body["metadata"],
            "canwrite": body["canwrite"], "state": 1, "is_pinned": 0,
        });
        self.experiments.insert(id, experiment);
        let tags: Vec<String> = body["tags"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(String::from)
            .collect();
        self.set_tags(id, &tags);
        self.touch(id, SYNC_USER);
        if let Some((kind, true)) = outcome {
            return Err(fault(kind, "the answer was lost after sending"));
        }
        Ok(Reply {
            status: 201,
            headers: vec![(
                "location".into(),
                format!("https://elab.example.org/api/v2/experiments/{id}"),
            )],
            body: Vec::new(),
        })
    }

    fn patch(&mut self, id: u64, body: &Value) -> Result<Reply> {
        let Some(experiment) = self.experiments.get_mut(&id) else {
            return reply(404, json!({ "code": 404 }));
        };
        let mut extra = Self::extra(experiment);
        if body["action"] == "updatemetadatafield" {
            for (name, value) in body
                .as_object()
                .unwrap()
                .iter()
                .filter(|(name, _)| *name != "action")
            {
                let Some(field) = extra.get_mut(name) else {
                    return refuse(&format!("Invalid metadata field {name}."));
                };
                if value.is_array() && field["allow_multi_values"] != true {
                    return refuse(&format!("Metadata field {name} expects a single value."));
                }
                field["value"] = value.clone();
            }
            Self::set_extra(experiment, extra);
        } else {
            // An ordinary update casts every value to a string, as PHP's `(string)` does.
            let text = |value: &Value| match value {
                Value::String(text) => text.clone(),
                Value::Array(_) | Value::Object(_) => "Array".to_string(),
                Value::Bool(true) => "1".to_string(),
                Value::Bool(false) | Value::Null => String::new(),
                other => other.to_string(),
            };
            if let Some(merged) = body.get("metadatamerge") {
                let merged: Value = match serde_json::from_str(&text(merged)) {
                    Ok(merged @ Value::Object(_)) => merged,
                    _ => return refuse("Invalid metadata JSON provided."),
                };
                let incoming = merged["extra_fields"]
                    .as_object()
                    .cloned()
                    .unwrap_or_default();
                for (name, field) in incoming {
                    match extra.get_mut(&name) {
                        // An existing field keeps its schema; only its value changes.
                        Some(old) if field.get("type").is_some_and(|kind| *kind != old["type"]) => {
                            let message = format!("Metadata field {name} has incompatible type.");
                            return refuse(&message);
                        }
                        Some(old) => old["value"] = field["value"].clone(),
                        None => {
                            extra.insert(name, field);
                        }
                    }
                }
                Self::set_extra(experiment, extra);
            }
            if let Some(title) = body.get("title") {
                experiment["title"] = json!(text(title));
            }
            if let Some(raw) = body.get("body") {
                let kind = body.get("content_type").map(text);
                let html = kind.map_or(experiment["content_type"] != 2, |kind| kind != "2");
                let raw = text(raw);
                experiment["content_type"] = json!(if html { 1 } else { 2 });
                experiment["body"] = json!(if html { purify(&raw) } else { raw });
            }
        }
        self.touch(id, SYNC_USER);
        reply(200, json!({}))
    }
}

impl World {
    /// An experiment made by a person, shared with the sync user, with these tags.
    pub fn experiment(&self, title: &str, body: &str, tags: &[&str]) -> u64 {
        let mut inner = self.lock();
        let id = inner.experiments.keys().last().copied().unwrap_or(100) + 1;
        let canwrite =
            json!({ "base": 40, "teams": [], "teamgroups": [], "users": [SYNC_USER] }).to_string();
        let experiment = json!({
            "id": id, "title": title, "body": purify(body), "body_html": purify(body), "content_type": 1,
            "date": "2026-11-12", "status_title": "Running", "fullname": "Ada Person", "userid": PERSON,
            "metadata": null, "canwrite": canwrite, "state": 1, "is_pinned": 0,
        });
        inner.experiments.insert(id, experiment);
        let tags: Vec<String> = tags.iter().map(|tag| tag.to_string()).collect();
        inner.set_tags(id, &tags);
        inner.touch(id, PERSON);
        id
    }

    /// A person's edit of an experiment; `modified_at` moves unless only tags change.
    pub fn edit(&self, id: u64, change: impl FnOnce(&mut Value)) {
        let mut inner = self.lock();
        change(inner.experiments.get_mut(&id).unwrap());
        inner.touch(id, PERSON);
    }

    /// A person's tag change, which leaves `modified_at` as it is, like eLabFTW.
    pub fn retag(&self, id: u64, tags: &[&str]) {
        let tags: Vec<String> = tags.iter().map(|tag| tag.to_string()).collect();
        self.lock().set_tags(id, &tags);
    }

    pub fn upload(&self, id: u64, upload: u64, name: &str, size: u64) {
        let mut inner = self.lock();
        let long_name = format!("ab/{upload}-uuid.bin");
        inner
            .objects
            .insert(format!("{BUCKET}/elabftw/{long_name}"), size);
        let entry = json!({ "id": upload, "real_name": name, "long_name": long_name, "hash": format!("h{upload}"), "filesize": size, "state": 1 });
        let list = inner.uploads.entry(id).or_default();
        list.retain(|old| old["id"] != upload);
        list.push(entry);
        inner.touch(id, PERSON);
    }

    pub fn get(&self, id: u64) -> Value {
        self.lock().experiments[&id].clone()
    }

    pub fn extra(&self, id: u64) -> Map<String, Value> {
        Inner::extra(&self.lock().experiments[&id])
    }
}
