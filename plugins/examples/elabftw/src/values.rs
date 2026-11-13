//! Maps eLabFTW experiments and Aruna datasets to the values the sync compares, and those
//! values back to dataset patches, file entities and the experiment's link fields.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::body::{Body, Format};

/// The `@id` prefix of every file entity that comes from an eLabFTW upload.
pub const UPLOAD_PREFIX: &str = "elabftw/uploads/";
pub const DATASET_FIELD: &str = "Aruna dataset";
pub const ID_FIELD: &str = "Aruna dataset id";
pub const FILES_FIELD: &str = "Aruna files";
/// eLabFTW's `BasePermissions`: Full 50, Organization 40, Team 30, User 20, UserOnly 10.
pub const BASE_TEAM: u64 = 30;
pub const BASE_USER: u64 = 20;

/// The install's settings; `bucket` is the node-scoped bucket path.
#[derive(Clone, Debug, Deserialize)]
pub struct Settings {
    pub elab_url: String,
    pub sync_user: u64,
    #[serde(default)]
    pub writers: String,
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default = "default_tag")]
    pub tag: String,
    pub aruna_url: String,
}

fn default_tag() -> String {
    "aruna".into()
}

impl Settings {
    /// The listing prefix of eLabFTW's uploads: the bucket path and the path prefix.
    pub fn uploads(&self) -> String {
        let bucket = self.bucket.trim_end_matches('/');
        match self.prefix.trim_matches('/') {
            "" => format!("{bucket}/"),
            prefix => format!("{bucket}/{prefix}/"),
        }
    }

    pub fn dataset_url(&self, dataset: &str) -> String {
        format!(
            "{}/datasets/{dataset}",
            self.aruna_url.trim_end_matches('/')
        )
    }

    pub fn file_url(&self, dataset: &str, id: &str) -> String {
        format!("{}/files/{id}", self.dataset_url(dataset))
    }

    pub fn experiment_url(&self, id: u64) -> String {
        let base = self.elab_url.trim_end_matches('/');
        format!("{base}/experiments.php?mode=view&id={id}")
    }

    /// The `canwrite` list of experiments the plugin creates, beside the base `User` (owner and
    /// admins): the configured writers, eLabFTW user ids or `group:<id>` for team groups.
    pub fn canwrite(&self) -> Value {
        let entries = self.writers.split(',').map(str::trim);
        let mut users = Vec::new();
        let mut groups = Vec::new();
        for entry in entries.filter(|entry| !entry.is_empty()) {
            match entry.strip_prefix("group:") {
                Some(group) => groups.extend(group.trim().parse::<u64>()),
                None => users.extend(entry.parse::<u64>()),
            }
        }
        json!({ "teams": [], "teamgroups": groups, "users": users })
    }
}

/// The synced fields. Title, tags and body go both ways, properties and files from eLabFTW to
/// Aruna, and links to Aruna's other files from Aruna to eLabFTW.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Field {
    Title,
    Tags,
    Body,
    Props,
    Files,
    Links,
}

impl Field {
    pub fn name(self) -> &'static str {
        match self {
            Self::Title => "title",
            Self::Tags => "tags",
            Self::Body => "body",
            Self::Props => "props",
            Self::Files => "files",
            Self::Links => "links",
        }
    }

    pub fn named(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|field| field.name() == name)
    }

    pub const ALL: [Field; 6] = [
        Self::Title,
        Self::Tags,
        Self::Body,
        Self::Props,
        Self::Files,
        Self::Links,
    ];
}

/// Status, date and owner of an experiment, kept on the dataset.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Props {
    pub status: String,
    pub date: String,
    pub owner: String,
}

/// The sync-owned facts of one file entity.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileFacts {
    pub name: String,
    pub url: String,
    pub size: String,
    pub hash: String,
}

/// One side's values. Files and links are `None` when this run did not read them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Values {
    pub title: String,
    pub tags: BTreeSet<String>,
    pub body: Body,
    /// Aruna's plain text `description`; eLabFTW has none.
    pub description: Option<String>,
    pub props: Props,
    pub files: Option<BTreeMap<String, FileFacts>>,
    pub links: Option<BTreeSet<String>>,
}

/// One experiment as `GET /experiments/{id}` answers it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Experiment {
    pub id: u64,
    pub title: String,
    pub body: Body,
    pub date: String,
    pub status: String,
    pub owner: String,
    pub owner_id: u64,
    /// Tag names and their eLabFTW ids.
    pub tags: BTreeMap<String, u64>,
    pub modified: String,
    pub changed_by: u64,
    pub extra: Map<String, Value>,
    pub canwrite: Value,
    /// eLabFTW's top-level `canwrite_base`; older answers keep `base` inside `canwrite`.
    pub canwrite_base: Option<u64>,
    /// eLabFTW's `state`: 1 normal, 2 archived, 3 deleted.
    pub state: u64,
}

/// One upload of an experiment; `long_name` is its key below the storage prefix.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Upload {
    pub id: u64,
    pub name: String,
    pub long_name: String,
    pub hash: String,
    pub size: u64,
    /// 1 normal, 2 archived, 3 deleted.
    pub state: u64,
}

/// A number eLabFTW answers either as a JSON number or as a numeric string.
pub fn number(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.trim().parse().ok(),
        _ => None,
    }
}

/// A text value; numbers and booleans as their JSON text, anything else empty.
fn text(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Number(number)) => number.to_string(),
        Some(Value::Bool(flag)) => flag.to_string(),
        _ => String::new(),
    }
}

/// A JSON value eLabFTW may also answer as JSON text, such as `metadata` or `canwrite`.
fn embedded(value: Option<&Value>) -> Value {
    match value {
        Some(Value::String(text)) => serde_json::from_str(text).unwrap_or(Value::Null),
        Some(other) => other.clone(),
        None => Value::Null,
    }
}

/// Strings of a value that is one string or a list of them, as JSON-LD returns a list of one.
fn strings(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::String(text)) if !text.is_empty() => vec![text.clone()],
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .map(String::from)
            .collect(),
        _ => Vec::new(),
    }
}

impl Experiment {
    pub fn parse(json: &Value) -> Option<Self> {
        let id = number(json.get("id")?)?;
        let names = text(json.get("tags"));
        let ids = text(json.get("tags_id"));
        let tags = (names.split('|').zip(ids.split(',')))
            .filter(|(name, _)| !name.is_empty())
            .filter_map(|(name, id)| Some((name.to_string(), id.trim().parse().ok()?)))
            .collect();
        let metadata = embedded(json.get("metadata"));
        let extra = (metadata.get("extra_fields").and_then(Value::as_object)).cloned();
        let format = Format::from_type(json.get("content_type").and_then(number).unwrap_or(1));
        Some(Self {
            id,
            title: text(json.get("title")),
            body: Body::new(text(json.get("body")), format),
            date: text(json.get("date")),
            status: text(json.get("status_title")),
            owner: text(json.get("fullname")),
            owner_id: json.get("userid").and_then(number).unwrap_or_default(),
            tags,
            modified: text(json.get("modified_at")),
            changed_by: json
                .get("lastchangeby")
                .and_then(number)
                .unwrap_or_default(),
            extra: extra.unwrap_or_default(),
            canwrite: embedded(json.get("canwrite")),
            canwrite_base: json.get("canwrite_base").and_then(number),
            state: json.get("state").and_then(number).unwrap_or(1),
        })
    }

    /// Whether the sync user, a team member, may write: owner, a base of team or wider, or listed.
    /// A team group may include it too, so it counts as allowed and a refused write marks the link.
    pub fn writable(&self, sync_user: u64) -> bool {
        let listed = |key: &str| self.canwrite.get(key).and_then(Value::as_array);
        let user = listed("users")
            .is_some_and(|users| (users.iter()).any(|user| number(user) == Some(sync_user)));
        let group = listed("teamgroups").is_some_and(|groups| !groups.is_empty());
        let base = (self.canwrite_base)
            .or_else(|| self.canwrite.get("base").and_then(number))
            .unwrap_or(BASE_USER);
        self.owner_id == sync_user || base >= BASE_TEAM || user || group
    }

    fn field(&self, name: &str) -> Option<&Value> {
        self.extra.get(name)?.get("value")
    }

    /// The dataset id the experiment's link field names, if any.
    pub fn linked(&self) -> Option<String> {
        let id = text(self.field(ID_FIELD));
        (!id.is_empty()).then_some(id)
    }

    /// Whether one of the three link fields is gone, so they must be created again.
    pub fn missing_fields(&self) -> bool {
        [DATASET_FIELD, ID_FIELD, FILES_FIELD]
            .iter()
            .any(|name| !self.extra.contains_key(*name))
    }

    /// A link field name a user field of another type already uses.
    pub fn clashing_field(&self) -> Option<&'static str> {
        let kind = |name: &str| self.extra.get(name).map(|field| text(field.get("type")));
        [
            (DATASET_FIELD, "url"),
            (ID_FIELD, "text"),
            (FILES_FIELD, "url"),
        ]
        .into_iter()
        .find(|(name, wanted)| kind(name).is_some_and(|kind| !kind.is_empty() && kind != *wanted))
        .map(|(name, _)| name)
    }

    /// The experiment's values; the sync tag is membership, not a keyword.
    pub fn values(&self, uploads: Option<&[Upload]>, settings: &Settings) -> Values {
        let tags = self
            .tags
            .keys()
            .filter(|tag| **tag != settings.tag)
            .cloned()
            .collect();
        let files = uploads.map(|uploads| {
            (uploads.iter())
                .filter(|upload| upload.state != 3)
                .map(|upload| {
                    (
                        format!("{UPLOAD_PREFIX}{}", upload.id),
                        upload.facts(settings),
                    )
                })
                .collect()
        });
        Values {
            title: self.title.clone(),
            tags,
            body: self.body.clone(),
            description: None,
            props: Props {
                status: self.status.clone(),
                date: self.date.clone(),
                owner: self.owner.clone(),
            },
            files,
            links: Some(strings(self.field(FILES_FIELD)).into_iter().collect()),
        }
    }
}

impl Upload {
    pub fn parse(json: &Value) -> Option<Self> {
        Some(Self {
            id: number(json.get("id")?)?,
            name: text(json.get("real_name")),
            long_name: text(json.get("long_name")),
            hash: text(json.get("hash")),
            size: json.get("filesize").and_then(number).unwrap_or_default(),
            state: json.get("state").and_then(number).unwrap_or(1),
        })
    }

    /// The object path of the upload below the configured bucket and prefix.
    pub fn path(&self, settings: &Settings) -> String {
        format!("{}{}", settings.uploads(), self.long_name)
    }

    fn facts(&self, settings: &Settings) -> FileFacts {
        FileFacts {
            name: self.name.clone(),
            url: self.path(settings),
            size: self.size.to_string(),
            hash: self.hash.clone(),
        }
    }
}

/// The dataset's values from its summary and, when read, all its file entities.
pub fn dataset_values(
    json: &Value,
    files: Option<&[Value]>,
    dataset: &str,
    settings: &Settings,
) -> Values {
    let format = Format::from_mime(&text(json.get("encodingFormat")));
    let mut ours = BTreeMap::new();
    let mut links = BTreeSet::new();
    for file in files.unwrap_or_default() {
        let id = text(file.get("@id"));
        match id.strip_prefix(UPLOAD_PREFIX) {
            Some(_) => {
                let facts = FileFacts {
                    name: text(file.get("name")),
                    url: text(file.get("contentUrl")),
                    size: text(file.get("contentSize")),
                    hash: text(file.get("sha256")),
                };
                ours.insert(id, facts);
            }
            None => {
                links.insert(settings.file_url(dataset, &id));
            }
        }
    }
    Values {
        title: text(json.get("name")),
        tags: strings(json.get("keywords")).into_iter().collect(),
        body: Body::new(text(json.get("text")), format),
        description: Some(text(json.get("description"))),
        props: Props {
            status: text(json.get("creativeWorkStatus")),
            date: text(json.get("dateCreated")),
            owner: text(json.get("author")),
        },
        files: files.map(|_| ours),
        links: files.map(|_| links),
    }
}

/// The experiment a dataset's `identifier` names, for a link rebuilt from Aruna.
pub fn identified(json: &Value) -> Option<u64> {
    json.get("identifier")?
        .get("elabftw")?
        .get("id")
        .and_then(number)
}

/// The dataset entity a create sends: every mapped field and the experiment's identifier.
pub fn dataset_entity(values: &Values, experiment: u64, settings: &Settings) -> Value {
    let fields = [Field::Title, Field::Tags, Field::Body, Field::Props];
    let mut entity = dataset_patch(values, &fields);
    if let Value::Object(fields) = &mut entity {
        fields.retain(|_, value| !value.is_null());
    }
    entity["@type"] = json!("Dataset");
    entity["identifier"] = identifier(experiment, settings);
    entity
}

pub fn identifier(experiment: u64, settings: &Settings) -> Value {
    let url = settings.experiment_url(experiment);
    json!({ "elabftw": { "id": experiment.to_string(), "url": url } })
}

/// A merge patch of the dataset entity for these fields; an empty value removes the field.
pub fn dataset_patch(values: &Values, fields: &[Field]) -> Value {
    let mut patch = Map::new();
    let mut put = |name: &str, value: Value| {
        let empty = value.as_str() == Some("") || value.as_array().is_some_and(Vec::is_empty);
        patch.insert(name.to_string(), if empty { Value::Null } else { value });
    };
    for field in fields {
        match field {
            Field::Title => put("name", json!(values.title)),
            Field::Tags => put("keywords", json!(values.tags)),
            Field::Body => {
                put("text", json!(values.body.text));
                put("description", json!(values.body.plain()));
                put("encodingFormat", json!(values.body.format.mime()));
            }
            Field::Props => {
                put("creativeWorkStatus", json!(values.props.status));
                put("dateCreated", json!(values.props.date));
                put("author", json!(values.props.owner));
            }
            Field::Files | Field::Links => {}
        }
    }
    Value::Object(patch)
}

/// The file entity to send: the existing one with only the sync-owned facts replaced, so
/// every other property an Aruna user added stays.
pub fn merge_file(existing: Option<&Value>, id: &str, facts: &FileFacts) -> Value {
    let mut entity = match existing {
        Some(Value::Object(fields)) => fields.clone(),
        _ => Map::new(),
    };
    entity.insert("@id".into(), json!(id));
    entity.entry("@type").or_insert(json!("File"));
    entity.insert("name".into(), json!(facts.name));
    entity.insert("contentUrl".into(), json!(facts.url));
    entity.insert("contentSize".into(), json!(facts.size));
    entity.insert("sha256".into(), json!(facts.hash));
    Value::Object(entity)
}

/// The three link fields, in the form `metadata` or `metadatamerge` creates them.
pub fn link_fields(settings: &Settings, dataset: &str, files: &BTreeSet<String>) -> Value {
    json!({ "extra_fields": {
        DATASET_FIELD: {
            "type": "url",
            "value": settings.dataset_url(dataset),
            "description": "Set by elabftw-sync",
        },
        ID_FIELD: { "type": "text", "value": dataset },
        FILES_FIELD: { "type": "url", "allow_multi_values": true, "value": files },
    } })
}

/// The `updatemetadatafield` request that sets the link values; empty values end a link.
pub fn field_update(settings: &Settings, dataset: Option<&str>, files: &BTreeSet<String>) -> Value {
    json!({
        "action": "updatemetadatafield",
        DATASET_FIELD: dataset.map(|dataset| settings.dataset_url(dataset)).unwrap_or_default(),
        ID_FIELD: dataset.unwrap_or_default(),
        FILES_FIELD: files,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> Settings {
        let json = json!({
            "elab_url": "https://elab.example.org/", "sync_user": 7, "writers": "3, group:4, x",
            "bucket": "/r/g/lab/data/n/b/", "prefix": "", "aruna_url": "https://portal.example.org",
        });
        serde_json::from_value(json).unwrap()
    }

    fn experiment(canwrite: Value, metadata: Value) -> Experiment {
        let json = json!({
            "id": "12", "title": "T", "body": "*b*", "content_type": 2, "tags": "aruna|pcr",
            "tags_id": "1, 5", "modified_at": "2026-11-13 10:00:00", "userid": 2,
            "metadata": metadata.to_string(), "canwrite": canwrite.to_string(),
        });
        Experiment::parse(&json).unwrap()
    }

    #[test]
    fn experiment_parsed() {
        let parsed = experiment(
            json!({ "base": 40 }),
            json!({ "extra_fields": { ID_FIELD: { "type": "text", "value": "01J" } } }),
        );
        assert_eq!(parsed.id, 12);
        assert_eq!(parsed.body, Body::new("*b*", Format::Markdown));
        assert_eq!(
            parsed.tags,
            BTreeMap::from([("aruna".into(), 1), ("pcr".into(), 5)])
        );
        assert_eq!(parsed.linked().as_deref(), Some("01J"));
        assert!(parsed.missing_fields());
        let values = parsed.values(None, &settings());
        assert_eq!(values.tags, BTreeSet::from(["pcr".to_string()]));
        assert_eq!(values.files, None);
    }

    #[test]
    fn write_access() {
        let refused = experiment(json!({ "base": 20, "users": [3] }), json!({}));
        assert!(!refused.writable(7));
        assert!(experiment(json!({ "base": 10, "users": ["7"] }), json!({})).writable(7));
        assert!(experiment(json!({ "base": 30 }), json!({})).writable(7));
        assert!(experiment(json!({ "base": 40 }), json!({})).writable(7));
        assert!(experiment(json!({ "base": 20, "teamgroups": [4] }), json!({})).writable(7));
        assert!(
            !experiment(json!({}), json!({})).writable(7),
            "the default base is User"
        );
        assert_eq!(
            settings().canwrite(),
            json!({ "teams": [], "teamgroups": [4], "users": [3] })
        );
    }

    #[test]
    fn current_answer() {
        // An answer of eLabFTW 6.0.9: the base is the top-level `canwrite_base`.
        let answer: Value =
            serde_json::from_str(include_str!("../tests/fixtures/experiment.json")).unwrap();
        let parsed = Experiment::parse(&answer).unwrap();
        assert_eq!((parsed.id, parsed.canwrite_base), (12, Some(BASE_TEAM)));
        assert!(parsed.writable(7), "a team-writable experiment");
        let mut owner_only = answer.clone();
        owner_only["canwrite_base"] = json!(BASE_USER);
        assert!(!Experiment::parse(&owner_only).unwrap().writable(7));
        assert_eq!(
            parsed.tags,
            BTreeMap::from([("aruna".into(), 3), ("pcr".into(), 7)])
        );
        assert_eq!(
            parsed.linked().as_deref(),
            Some("01JC0000000000000000000001")
        );
        assert_eq!(
            (parsed.status.as_str(), parsed.owner.as_str()),
            ("Running", "Ada Person")
        );
    }

    #[test]
    fn clashing_fields() {
        let clash = json!({ "extra_fields": { FILES_FIELD: { "type": "text", "value": "" } } });
        assert_eq!(
            experiment(json!({}), clash).clashing_field(),
            Some(FILES_FIELD)
        );
        let ours = link_fields(&settings(), "01J", &BTreeSet::new());
        let parsed = experiment(json!({}), ours);
        assert_eq!(
            (parsed.clashing_field(), parsed.missing_fields()),
            (None, false)
        );
        let update = field_update(&settings(), None, &BTreeSet::new());
        assert_eq!(
            update,
            json!({ "action": "updatemetadatafield", DATASET_FIELD: "", ID_FIELD: "", FILES_FIELD: [] })
        );
    }

    #[test]
    fn dataset_side() {
        let settings = settings();
        let entity = json!({ "name": "N", "keywords": "one", "text": "# t", "encodingFormat": "text/markdown" });
        let files = [
            json!({ "@id": "elabftw/uploads/4", "name": "a", "contentUrl": "/p", "contentSize": "3", "sha256": "h" }),
            json!({ "@id": "raw.csv" }),
        ];
        let values = dataset_values(&entity, Some(&files), "01J", &settings);
        assert_eq!(
            values.tags,
            BTreeSet::from(["one".to_string()]),
            "JSON-LD gives a list of one as a string"
        );
        assert_eq!(values.body.format, Format::Markdown);
        assert_eq!(values.files.unwrap()["elabftw/uploads/4"].size, "3");
        let link = "https://portal.example.org/datasets/01J/files/raw.csv".to_string();
        assert_eq!(values.links, Some(BTreeSet::from([link])));
        assert_eq!(settings.uploads(), "/r/g/lab/data/n/b/");
        let patch = dataset_patch(&Values::default(), &[Field::Title, Field::Tags]);
        assert_eq!(
            patch,
            json!({ "name": null, "keywords": null }),
            "empty values remove the field"
        );
    }

    #[test]
    fn file_merge_keeps() {
        let stored = json!({ "@id": "elabftw/uploads/4", "@type": "File", "name": "old", "license": "CC0", "about": { "@id": "#x" } });
        let facts = FileFacts {
            name: "new".into(),
            url: "/p".into(),
            size: "5".into(),
            hash: "h".into(),
        };
        let merged = merge_file(Some(&stored), "elabftw/uploads/4", &facts);
        assert_eq!(merged["license"], "CC0");
        assert_eq!(merged["about"], json!({ "@id": "#x" }));
        assert_eq!(
            (merged["name"].clone(), merged["contentSize"].clone()),
            (json!("new"), json!("5"))
        );
        assert_eq!(
            merge_file(None, "elabftw/uploads/4", &facts)["@type"],
            "File"
        );
    }
}
