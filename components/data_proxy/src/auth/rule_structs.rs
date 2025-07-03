use super::rule_engine::RuleEngine;
use crate::structs::Bundle;
use crate::structs::DbPermissionLevel;
use crate::structs::Object;
use crate::structs::ResourceStates;
use anyhow::anyhow;
use anyhow::Result;
use chrono::TimeDelta;
use chrono::Utc;
use diesel_ulid::DieselUlid;
use http::HeaderMap;
use http::HeaderValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRuleInfo {
    pub user_id: String,
    pub permissions: HashMap<String, String>,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectHierarchyRuleInfo {
    pub object: Option<Object>,
    pub dataset: Option<Object>,
    pub collection: Option<Object>,
    pub project: Object,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestInfo {
    pub bucket: bool, // Is this request for a bucket or an object
    pub method: String,
    pub headers: HashMap<String, StringOrVec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleInfo {
    pub id: String,
    pub expires: i64,
}

/// ------- INPUTS ---------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootRuleInput {
    pub user: UserRuleInfo,
    pub request: RequestInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectRuleInput {
    pub user: Option<UserRuleInfo>,
    pub object_hierarchy: ObjectHierarchyRuleInfo,
    pub request: RequestInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageObjectRuleInput {
    pub user: UserRuleInfo,
    pub object: Object,
    pub parents: Vec<Object>,
    pub request: RequestInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleRuleInput {
    pub user: UserRuleInfo,
    pub objects: Vec<Object>,
    pub request: RequestInfo,
    pub bundle: BundleInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationIncomingRuleInput {
    pub objects: Vec<Object>,
    pub target_proxy_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationOutgoingRuleInput {
    pub objects: Option<Object>,
    pub target_proxy_id: String,
}

#[derive(Debug, Default)]
pub struct RootRuleInputBuilder {
    user_id: String,
    permissions: HashMap<String, String>,
    attributes: HashMap<String, String>,
    method: String,
    headers: HashMap<String, StringOrVec>,
    skip: bool,
}

impl RootRuleInputBuilder {
    pub fn new(engine: &RuleEngine) -> Self {
        Self {
            skip: engine.has_root(),
            ..Self::default()
        }
    }

    pub fn user_id(mut self, user_id: &str) -> Self {
        if self.skip {
            return self;
        }
        self.user_id = user_id.to_string();
        self
    }

    pub fn permissions(mut self, permissions: &HashMap<DieselUlid, DbPermissionLevel>) -> Self {
        if self.skip {
            return self;
        }
        self.permissions = convert_permissions(permissions.clone());
        self
    }

    pub fn attributes(mut self, attributes: &HashMap<String, String>) -> Self {
        if self.skip {
            return self;
        }
        self.attributes.clone_from(attributes);
        self
    }

    pub fn method(mut self, method: &http::Method) -> Self {
        if self.skip {
            return self;
        }
        self.method = method.to_string();
        self
    }

    pub fn headers(mut self, headers: &HeaderMap<HeaderValue>) -> Self {
        if self.skip {
            return self;
        }
        self.headers = convert_headers(headers);
        self
    }

    pub fn build(self) -> Result<RootRuleInput> {
        if !self.skip && self.user_id.is_empty() {
            return Err(anyhow!("user_id is required"));
        }

        if !self.skip && self.method.is_empty() {
            return Err(anyhow!("method is required"));
        }

        Ok(RootRuleInput {
            user: UserRuleInfo {
                user_id: self.user_id,
                permissions: self.permissions,
                attributes: self.attributes,
            },
            request: RequestInfo {
                bucket: false,
                method: self.method,
                headers: self.headers,
            },
        })
    }
}

#[derive(Debug, Default)]
pub struct ObjectRuleInputBuilder {
    user_id: Option<String>,
    permissions: Option<HashMap<String, String>>,
    attributes: Option<HashMap<String, String>>,
    bucket: bool, // Bucket or Object request
    method: String,
    headers: HashMap<String, StringOrVec>,
    object: Option<Object>,
    dataset: Option<Object>,
    collection: Option<Object>,
    project: Option<Object>,
    skip: bool,
}

impl ObjectRuleInputBuilder {
    pub fn new(rule_engine: &RuleEngine) -> Self {
        Self {
            skip: !rule_engine.has_object(),
            ..Self::default()
        }
    }

    #[allow(dead_code)]
    pub fn bucket(mut self, bucket: bool) -> Self {
        self.bucket = bucket;
        self
    }

    #[allow(dead_code)]
    pub fn user_id(mut self, user_id: &str) -> Self {
        if self.skip {
            return self;
        }
        self.user_id = Some(user_id.to_string());
        self
    }

    pub fn permissions(mut self, permissions: &HashMap<DieselUlid, DbPermissionLevel>) -> Self {
        if self.skip {
            return self;
        }
        self.permissions = Some(convert_permissions(permissions.clone()));
        self
    }

    pub fn attributes(mut self, attributes: &HashMap<String, String>) -> Self {
        if self.skip {
            return self;
        }
        self.attributes = Some(attributes.clone());
        self
    }

    pub fn method(mut self, method: &http::Method) -> Self {
        if self.skip {
            return self;
        }
        self.method = method.to_string();
        self
    }

    pub fn headers(mut self, headers: &HeaderMap<HeaderValue>) -> Self {
        if self.skip {
            return self;
        }
        self.headers = convert_headers(headers);
        self
    }

    #[allow(dead_code)]
    pub fn object(mut self, object: &Object) -> Result<Self> {
        if self.skip {
            return Ok(self);
        }
        if self.object.is_some() {
            return Err(anyhow!("object is already set"));
        }
        self.object = Some(object.clone());
        Ok(self)
    }

    #[allow(dead_code)]
    pub fn dataset(mut self, dataset: &Object) -> Result<Self> {
        if self.skip {
            return Ok(self);
        }
        if self.dataset.is_some() {
            return Err(anyhow!("dataset is already set"));
        }
        self.dataset = Some(dataset.clone());
        Ok(self)
    }

    #[allow(dead_code)]
    pub fn collection(mut self, collection: &Object) -> Result<Self> {
        if self.collection.is_some() {
            return Err(anyhow!("collection is already set"));
        }
        self.collection = Some(collection.clone());
        Ok(self)
    }

    #[allow(dead_code)]
    pub fn project(mut self, project: &Object) -> Result<Self> {
        if self.skip {
            return Ok(self);
        }
        if self.project.is_some() {
            return Err(anyhow!("project is already set"));
        }
        self.project = Some(project.clone());
        Ok(self)
    }

    pub fn add_resource_states(mut self, resource_states: &ResourceStates) -> Self {
        if self.skip {
            return self;
        }
        if let Some(project) = resource_states.get_project_or_missing() {
            self.project = Some(project);
        }
        if let Some(collection) = resource_states.get_collection_or_missing() {
            self.collection = Some(collection);
        }
        if let Some(dataset) = resource_states.get_dataset_or_missing() {
            self.dataset = Some(dataset);
        }
        if let Some(object) = resource_states.get_object_or_missing() {
            self.object = Some(object);
        }
        self
    }

    pub fn build(self) -> Result<ObjectRuleInput> {
        if !self.skip && self.method.is_empty() {
            return Err(anyhow!("method is required"));
        }

        if !self.skip && self.project.is_none() {
            return Err(anyhow!("project is required"));
        }

        let user_info =
            if self.user_id.is_none() && self.permissions.is_none() && self.attributes.is_none() {
                None
            } else {
                Some(UserRuleInfo {
                    user_id: self.user_id.ok_or_else(|| anyhow!("user_id is required"))?,
                    permissions: self.permissions.unwrap_or_default(),
                    attributes: self.attributes.unwrap_or_default(),
                })
            };

        Ok(ObjectRuleInput {
            user: user_info,
            object_hierarchy: ObjectHierarchyRuleInfo {
                object: self.object,
                dataset: self.dataset,
                collection: self.collection,
                project: self.project.unwrap_or_default(),
            },
            request: RequestInfo {
                bucket: self.bucket,
                method: self.method,
                headers: self.headers,
            },
        })
    }
}

#[derive(Debug, Default)]
pub struct PackageObjectRuleInputBuilder {
    user_id: String,
    permissions: HashMap<String, String>,
    attributes: HashMap<String, String>,
    method: String,
    headers: HashMap<String, StringOrVec>,
    object: Option<Object>,
    parents: Vec<Object>,
    skip: bool,
}

// TODO: This could be optimized away if no rules are set by passing only references and cloning only when needed

impl PackageObjectRuleInputBuilder {
    pub fn new(rule_engine: &RuleEngine) -> Self {
        Self {
            skip: !rule_engine.has_object_package(),
            ..Self::default()
        }
    }

    #[allow(dead_code)]
    pub fn user_id(mut self, user_id: &str) -> Self {
        if self.skip {
            return self;
        }
        self.user_id = user_id.to_string();
        self
    }

    pub fn permissions(mut self, permissions: &HashMap<DieselUlid, DbPermissionLevel>) -> Self {
        if self.skip {
            return self;
        }
        self.permissions = convert_permissions(permissions.clone());
        self
    }

    pub fn attributes(mut self, attributes: &HashMap<String, String>) -> Self {
        if self.skip {
            return self;
        }
        self.attributes.clone_from(attributes);
        self
    }

    pub fn method(mut self, method: &http::Method) -> Self {
        if self.skip {
            return self;
        }
        self.method = method.to_string();
        self
    }

    pub fn headers(mut self, headers: &HeaderMap<HeaderValue>) -> Self {
        if self.skip {
            return self;
        }
        self.headers = convert_headers(headers);
        self
    }

    pub fn object(mut self, object: Option<Object>) -> Self {
        if self.skip {
            return self;
        }
        self.object = object;
        self
    }

    pub fn parents(mut self, parents: &[Object]) -> Self {
        if self.skip {
            return self;
        }
        self.parents = parents.to_vec();
        self
    }

    pub fn build(self) -> Result<PackageObjectRuleInput> {
        if !self.skip && self.user_id.is_empty() {
            return Err(anyhow!("user_id is required"));
        }

        if !self.skip && self.method.is_empty() {
            return Err(anyhow!("method is required"));
        }

        if !self.skip && self.object.is_none() {
            return Err(anyhow!("object is required"));
        }

        Ok(PackageObjectRuleInput {
            user: UserRuleInfo {
                user_id: self.user_id,
                permissions: self.permissions,
                attributes: self.attributes,
            },
            object: self.object.unwrap_or_default(),
            parents: self.parents,
            request: RequestInfo {
                bucket: false,
                method: self.method,
                headers: self.headers,
            },
        })
    }
}

#[derive(Debug, Default)]
pub struct BundleRuleInputBuilder {
    user_id: String,
    permissions: HashMap<String, String>,
    attributes: HashMap<String, String>,
    method: String,
    headers: HashMap<String, StringOrVec>,
    objects: Vec<Object>,
    bundle: Bundle,
    skip: bool,
}

impl BundleRuleInputBuilder {
    pub fn new(rule_engine: &RuleEngine) -> Self {
        Self {
            skip: !rule_engine.has_bundle(),
            ..Self::default()
        }
    }

    #[allow(dead_code)]
    pub fn user_id(mut self, user_id: &str) -> Self {
        if self.skip {
            return self;
        }
        self.user_id = user_id.to_string();
        self
    }

    pub fn permissions(mut self, permissions: &HashMap<DieselUlid, DbPermissionLevel>) -> Self {
        if self.skip {
            return self;
        }
        self.permissions = convert_permissions(permissions.clone());
        self
    }

    pub fn attributes(mut self, attributes: &HashMap<String, String>) -> Self {
        if self.skip {
            return self;
        }
        self.attributes.clone_from(attributes);
        self
    }

    pub fn method(mut self, method: &http::Method) -> Self {
        if self.skip {
            return self;
        }
        self.method = method.to_string();
        self
    }

    pub fn headers(mut self, headers: &HeaderMap<HeaderValue>) -> Self {
        if self.skip {
            return self;
        }
        self.headers = convert_headers(headers);
        self
    }

    #[allow(dead_code)]
    pub fn objects(mut self, objects: &[Object]) -> Self {
        if self.skip {
            return self;
        }
        self.objects = objects.to_vec();
        self
    }

    pub fn bundle(mut self, bundle_id: &Bundle) -> Self {
        if self.skip {
            return self;
        }
        self.bundle = bundle_id.clone();
        self
    }

    pub fn build(self) -> Result<BundleRuleInput> {
        if !self.skip && self.user_id.is_empty() {
            return Err(anyhow!("user_id is required"));
        }

        if !self.skip && self.method.is_empty() {
            return Err(anyhow!("method is required"));
        }

        if !self.skip && self.objects.is_empty() {
            return Err(anyhow!("objects is required"));
        }

        if !self.skip && self.bundle.is_default() {
            return Err(anyhow!("bundle_id is required"));
        }

        Ok(BundleRuleInput {
            user: UserRuleInfo {
                user_id: self.user_id,
                permissions: self.permissions,
                attributes: self.attributes,
            },
            objects: self.objects,
            request: RequestInfo {
                bucket: false,
                method: self.method,
                headers: self.headers,
            },
            bundle: BundleInfo {
                id: self.bundle.id.to_string(),
                expires: self
                    .bundle
                    .expires_at
                    .unwrap_or_else(|| {
                        Utc::now() + TimeDelta::try_weeks(100 * 52).unwrap_or(TimeDelta::MAX)
                    })
                    .timestamp(),
            },
        })
    }
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct ReplicationIncomingRuleInputBuilder {
    objects: Vec<Object>,
    target_proxy_id: String,
    skip: bool,
}

impl ReplicationIncomingRuleInputBuilder {
    #[allow(dead_code)]
    pub fn new(rule_engine: &RuleEngine) -> Self {
        Self {
            skip: !rule_engine.has_replication_in(),
            ..Self::default()
        }
    }

    #[allow(dead_code)]
    pub fn objects(mut self, objects: &[Object]) -> Self {
        if self.skip {
            return self;
        }
        self.objects = objects.to_vec();
        self
    }

    #[allow(dead_code)]
    pub fn target_proxy_id(mut self, target_proxy_id: &str) -> Self {
        if self.skip {
            return self;
        }
        self.target_proxy_id = target_proxy_id.to_string();
        self
    }

    #[allow(dead_code)]
    pub fn build(self) -> Result<ReplicationIncomingRuleInput> {
        if !self.skip && self.objects.is_empty() {
            return Err(anyhow!("objects is required"));
        }

        if !self.skip && self.target_proxy_id.is_empty() {
            return Err(anyhow!("target_proxy_id is required"));
        }

        Ok(ReplicationIncomingRuleInput {
            objects: self.objects,
            target_proxy_id: self.target_proxy_id,
        })
    }
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct ReplicationOutgoingRuleInputBuilder {
    object: Option<Object>,
    target_proxy_id: String,
    skip: bool,
}

impl ReplicationOutgoingRuleInputBuilder {
    #[allow(dead_code)]
    pub fn new(rule_engine: &RuleEngine) -> Self {
        Self {
            skip: !rule_engine.has_replication_out(),
            ..Self::default()
        }
    }

    #[allow(dead_code)]
    pub fn object(mut self, object: &Object) -> Self {
        if self.skip {
            return self;
        }
        self.object = Some(object.clone());
        self
    }

    #[allow(dead_code)]
    pub fn target_proxy_id(mut self, target_proxy_id: &str) -> Self {
        if self.skip {
            return self;
        }
        self.target_proxy_id = target_proxy_id.to_string();
        self
    }

    #[allow(dead_code)]
    pub fn build(self) -> Result<ReplicationOutgoingRuleInput> {
        if !self.skip && self.object.is_none() {
            return Err(anyhow!("objects is required"));
        }

        if !self.skip && self.target_proxy_id.is_empty() {
            return Err(anyhow!("target_proxy_id is required"));
        }

        Ok(ReplicationOutgoingRuleInput {
            objects: self.object,
            target_proxy_id: self.target_proxy_id,
        })
    }
}

// ------ HELPERS -------

pub fn convert_permissions(
    perm: HashMap<DieselUlid, DbPermissionLevel>,
) -> HashMap<String, String> {
    let mut permissions = HashMap::new();
    for (k, v) in perm {
        permissions.insert(k.to_string(), v.to_string());
    }
    permissions
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StringOrVec {
    Elem(String),
    Vec(Vec<String>),
}

pub fn convert_headers(headers: &HeaderMap<HeaderValue>) -> HashMap<String, StringOrVec> {
    let mut header_map = HashMap::new();
    for (k, v) in headers.iter() {
        let value_string = v.to_str().unwrap_or_default().to_string();
        let entry = header_map.entry(k.to_string());
        match entry {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(StringOrVec::Elem(value_string));
            }
            std::collections::hash_map::Entry::Occupied(mut e) => match e.get_mut() {
                StringOrVec::Elem(existing) => {
                    *e.get_mut() = StringOrVec::Vec(vec![existing.clone(), value_string]);
                }
                StringOrVec::Vec(arr) => {
                    arr.push(value_string);
                }
            },
        }
    }
    header_map
}
