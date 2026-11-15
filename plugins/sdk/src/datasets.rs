//! Datasets of the install's group: their fields, saved as a change of only what changed at the
//! revision read, their file entities, and creates named by a path.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

use crate::host;
use crate::{Error, Object, Result};

/// One dataset at the revision it was read. Its fields can be changed in place; `save` sends what
/// changed. Any other property is reached by name.
#[derive(Debug, Clone, PartialEq)]
pub struct Dataset {
    pub id: String,
    pub revision: String,
    pub name: String,
    pub description: String,
    pub keywords: Vec<String>,
    pub license: Option<String>,
    /// The dataset's `identifier`, by name, such as `doi`.
    pub identifiers: BTreeMap<String, Value>,
    properties: Map<String, Value>,
    /// The entity as it was read or last saved.
    read: Map<String, Value>,
}

/// The dataset `id`, without its file links.
pub fn get(id: &str) -> Result<Dataset> {
    let document = host::with(|host| host.dataset_get(id, false))?;
    Dataset::parse(id, document.revision, &document.json)
}

/// Every dataset of the group the install may read, each loaded, one page of ids at a time.
pub fn list() -> impl Iterator<Item = Result<Dataset>> {
    let pages = Pages::new(|cursor| {
        let listing = host::with(|host| host.dataset_list(cursor.as_deref()))?;
        Ok((listing.resources, listing.cursor))
    });
    pages.map(|id| get(&id?))
}

/// Creates the dataset at `path` below the group's metadata root from its fields; the path names
/// it, so creating it again answers the same dataset.
pub fn create(path: &str, fields: Value) -> Result<Dataset> {
    let Value::Object(mut fields) = fields else {
        return Err(Error::InvalidCall(
            "a dataset's fields are a JSON object".into(),
        ));
    };
    if let Some(identifiers) = fields.remove("identifiers") {
        fields.insert("identifier".into(), identifiers);
    }
    let entity = Value::Object(fields);
    let created = host::with(|host| host.dataset_create(path, &entity.to_string()))?;
    Dataset::parse(&created.resource, created.revision, &entity.to_string())
}

impl Dataset {
    fn parse(id: &str, revision: String, json: &str) -> Result<Self> {
        let Value::Object(entity) = serde_json::from_str(json)? else {
            return Err(Error::Fail(format!("dataset {id} is not a JSON object")));
        };
        let mut dataset = Self {
            id: id.to_string(),
            revision,
            name: String::new(),
            description: String::new(),
            keywords: Vec::new(),
            license: None,
            identifiers: BTreeMap::new(),
            properties: Map::new(),
            read: Map::new(),
        };
        for (name, value) in entity {
            dataset.put(&name, value);
        }
        // Compared in the form a save writes, so a field read as text is unchanged until changed.
        dataset.read = dataset.entity();
        Ok(dataset)
    }

    /// A property by its name in the entity, such as `publisher`.
    pub fn get(&self, name: &str) -> Option<Value> {
        self.entity().remove(name)
    }

    pub fn set(&mut self, name: &str, value: impl Into<Value>) {
        self.put(name, value.into());
    }

    pub fn remove(&mut self, name: &str) {
        self.put(name, Value::Null);
    }

    fn put(&mut self, name: &str, value: Value) {
        let text = |value: &Value| value.as_str().unwrap_or_default().to_string();
        match name {
            "name" => self.name = text(&value),
            "description" => self.description = text(&value),
            "keywords" => self.keywords = keywords(&value),
            "license" => {
                self.license = (value.get("@id").unwrap_or(&value).as_str()).map(str::to_string)
            }
            "identifier" | "identifiers" => {
                self.identifiers =
                    (value.as_object().cloned().unwrap_or_default().into_iter()).collect();
            }
            _ if value.is_null() => drop(self.properties.remove(name)),
            _ => drop(self.properties.insert(name.to_string(), value)),
        }
    }

    /// The entity as the fields now stand.
    fn entity(&self) -> Map<String, Value> {
        let mut entity = self.properties.clone();
        let fields = [
            ("name", (!self.name.is_empty()).then(|| json!(self.name))),
            (
                "description",
                (!self.description.is_empty()).then(|| json!(self.description)),
            ),
            (
                "keywords",
                (!self.keywords.is_empty()).then(|| json!(self.keywords)),
            ),
            (
                "license",
                self.license.as_ref().map(|license| json!(license)),
            ),
            (
                "identifier",
                (!self.identifiers.is_empty()).then(|| json!(self.identifiers)),
            ),
        ];
        // An empty field is left out, so a save removes it.
        for (name, value) in fields {
            if let Some(value) = value {
                entity.insert(name.into(), value);
            }
        }
        entity
    }

    /// Sends the fields that changed with the revision read, and answers their names; nothing
    /// changed makes no call. A change made meanwhile is a `Conflict`.
    pub fn save(&mut self) -> Result<Vec<String>> {
        let now = self.entity();
        let mut patch = merge_patch(&self.read, &now);
        patch.retain(|name, _| !name.starts_with('@'));
        if patch.is_empty() {
            return Ok(Vec::new());
        }
        let text = Value::Object(patch.clone()).to_string();
        self.revision = host::with(|host| host.dataset_patch(&self.id, &text, &self.revision))?;
        self.read = now;
        let names = patch.into_iter().map(|(name, _)| match name.as_str() {
            "identifier" => "identifiers".to_string(),
            _ => name,
        });
        Ok(names.collect())
    }

    /// Reads the dataset again; changes not saved are lost.
    pub fn reload(&mut self) -> Result<()> {
        *self = get(&self.id)?;
        Ok(())
    }

    /// The dataset entity with its file links.
    pub fn ro_crate(&self) -> Result<Value> {
        let document = host::with(|host| host.dataset_get(&self.id, true))?;
        Ok(serde_json::from_str(&document.json)?)
    }

    /// The dataset's `File` entities, one page at a time.
    pub fn files(&self) -> impl Iterator<Item = Result<Value>> + use<> {
        let id = self.id.clone();
        let pages = Pages::new(move |cursor| {
            let page = host::with(|host| host.dataset_files(&id, cursor.as_deref()))?;
            Ok((page.files, page.cursor))
        });
        pages.map(|file| Ok(serde_json::from_str(&file?)?))
    }

    /// Links `object` as a `File` entity named by `id`, else by its `s3://` address, whose
    /// `contentUrl` is that address.
    pub fn add_file(&mut self, object: &Object, id: Option<&str>) -> Result<()> {
        self.add_file_with(object, id, Map::new())
    }

    /// `add_file` with further properties of the entity.
    pub fn add_file_with(
        &mut self,
        object: &Object,
        id: Option<&str>,
        properties: Map<String, Value>,
    ) -> Result<()> {
        let name = object.path.rsplit('/').next().unwrap_or(&object.path);
        let mut file = json!({
            "@id": id.unwrap_or(&object.url),
            "@type": "File",
            "name": name,
            "contentSize": object.size,
            "contentUrl": object.url,
        });
        if let Value::Object(fields) = &mut file {
            fields.extend(properties);
        }
        self.put_file(file)
    }

    /// Adds or replaces one `File` entity the plugin built itself.
    pub fn put_file(&mut self, file: Value) -> Result<()> {
        let text = file.to_string();
        self.revision = host::with(|host| host.file_put(&self.id, &text, &self.revision))?;
        Ok(())
    }

    pub fn remove_file(&mut self, id: &str) -> Result<()> {
        self.revision = host::with(|host| host.file_remove(&self.id, id, &self.revision))?;
        Ok(())
    }
}

/// The JSON merge patch that turns `old` into `new`: what changed, `null` for each name removed,
/// and the same rule inside objects on both sides, as the host applies a merge patch.
pub(crate) fn merge_patch(
    old: &Map<String, Value>,
    new: &Map<String, Value>,
) -> Map<String, Value> {
    let removed = (old.keys()).filter(|name| !new.contains_key(*name));
    let mut patch: Map<String, Value> = removed.map(|name| (name.clone(), Value::Null)).collect();
    for (name, value) in new {
        match (old.get(name), value) {
            (Some(Value::Object(before)), Value::Object(after)) => {
                let inner = merge_patch(before, after);
                if !inner.is_empty() {
                    patch.insert(name.clone(), Value::Object(inner));
                }
            }
            (Some(before), after) if before == after => {}
            _ => drop(patch.insert(name.clone(), value.clone())),
        }
    }
    patch
}

fn keywords(value: &Value) -> Vec<String> {
    match value {
        Value::Array(words) => words
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect(),
        Value::String(words) => words
            .split(',')
            .map(str::trim)
            .filter(|word| !word.is_empty())
            .map(str::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

type Page<T> = Result<(Vec<T>, Option<String>)>;

/// Items of paged host answers, one page fetched at a time.
pub(crate) struct Pages<T, F> {
    fetch: F,
    page: Vec<T>,
    cursor: Option<String>,
    done: bool,
}

impl<T, F: FnMut(Option<String>) -> Page<T>> Pages<T, F> {
    pub(crate) fn new(fetch: F) -> Self {
        Self {
            fetch,
            page: Vec::new(),
            cursor: None,
            done: false,
        }
    }
}

impl<T, F: FnMut(Option<String>) -> Page<T>> Iterator for Pages<T, F> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.page.is_empty() && !self.done {
            match (self.fetch)(self.cursor.take()) {
                Ok((items, cursor)) => {
                    self.done = cursor.is_none();
                    self.cursor = cursor;
                    self.page = items.into_iter().rev().collect();
                }
                Err(error) => {
                    self.done = true;
                    return Some(Err(error));
                }
            }
        }
        self.page.pop().map(Ok)
    }
}

#[cfg(test)]
#[path = "datasets_tests.rs"]
mod tests;
