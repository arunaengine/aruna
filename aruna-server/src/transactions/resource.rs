use super::{
    controller::Controller,
    request::{Request, Requester, SerializedResponse},
};
use crate::{
    constants::{
        field_names::{
            AUTHORS_FIELD, DESCRIPTION_FIELD, IDENTIFIERS_FIELD, ID_FIELD, LABELS_FIELD,
            LAST_MODIFIED_FIELD, LICENSE_FIELD, NAME_FIELD, TAG_FIELD, VISIBILITY_FIELD,
        },
        relation_types::{self, DEFAULT, OWNS_PROJECT, PERMISSION_ADMIN, PROJECT_PART_OF_REALM},
    },
    context::{BatchPermission, Context},
    error::ArunaError,
    logerr,
    models::{
        models::{
            Component, DataLocation, MilliIdx, NodeVariant, Resource, ResourceVariant, Subscriber,
            SyncingStatus, VisibilityClass,
        },
        requests::{
            AuthorizeRequest, AuthorizeResponse, CreateProjectRequest, CreateProjectResponse,
            CreateResourceBatchRequest, CreateResourceBatchResponse, CreateResourceRequest,
            CreateResourceResponse, DeleteRequest, DeleteResponse, GetInner, GetResourcesRequest,
            GetResourcesResponse, Parent, RegisterDataRequest, RegisterDataResponse,
            ResourceUpdateRequests, ResourceUpdateResponses, UpdateResourceAuthorsResponse,
            UpdateResourceDescriptionResponse, UpdateResourceIdentifiersResponse,
            UpdateResourceLabelsResponse, UpdateResourceLicenseResponse,
            UpdateResourceNameResponse, UpdateResourceTitleResponse,
            UpdateResourceVisibilityResponse,
        },
    },
    storage::{
        store::Store,
        txns::{Txn, WriteTxn},
    },
    transactions::request::WriteRequest,
};
use ahash::RandomState;
use chrono::{DateTime, Utc};
use petgraph::Direction::{self, Outgoing};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tracing::{debug, error, info};
use ulid::Ulid;

impl Request for CreateProjectRequest {
    type Response = CreateProjectResponse;
    fn get_context(&self) -> Context {
        Context::InRequest
    }

    async fn run_request(
        mut self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        // This checks if a default group or realm are set for the token
        // If not, it will fetch the default group and realm from the token
        // And overwrite the group_id and realm_id in the request
        // Note: Currently both or neither must be set, it is not possible to set only one
        if self.group_id.is_nil() || self.realm_id.is_nil() {
            let requester = requester.clone().ok_or_else(|| ArunaError::Unauthorized)?;

            let token_idx = requester
                .get_token_idx()
                .ok_or_else(|| ArunaError::Unauthorized)?;
            let user_id = requester.get_id().ok_or_else(|| ArunaError::Unauthorized)?;

            let store = controller.get_store();

            (self.group_id, self.realm_id) = tokio::task::spawn_blocking(move || {
                let rtxn = store.read_txn()?;
                let token = store.get_token(&user_id, token_idx, &rtxn)?;
                Ok::<_, ArunaError>((
                    token
                        .default_group
                        .ok_or_else(|| ArunaError::InvalidParameter {
                            name: "default_group".to_string(),
                            error: "expected default group for token".to_string(),
                        })?,
                    token
                        .default_realm
                        .ok_or_else(|| ArunaError::InvalidParameter {
                            name: "default_realm".to_string(),
                            error: "expected default realm for token".to_string(),
                        })?,
                ))
            })
            .await
            .map_err(|e| {
                error!("Failed to join task: {}", e);
                ArunaError::ServerError("".to_string())
            })??;
        }

        let requester = requester.ok_or_else(|| ArunaError::Unauthorized)?;

        // Manuall authorize to allow for default values from token to taken into account
        controller
            .authorize_with_context(
                &requester,
                &self,
                Context::Permission {
                    min_permission: crate::models::models::Permission::Write,
                    source: self.group_id,
                },
            )
            .await?;

        let request_tx = CreateProjectRequestTx {
            req: self,
            project_id: Ulid::new(),
            requester,
            created_at: Utc::now().timestamp_millis(),
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateProjectRequestTx {
    req: CreateProjectRequest,
    requester: Requester,
    project_id: Ulid,
    created_at: i64,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateProjectRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let time = DateTime::from_timestamp_millis(self.created_at).ok_or_else(|| {
            ArunaError::ConversionError {
                from: "i64".to_string(),
                to: "Chrono::DateTime".to_string(),
            }
        })?;

        let mut project = Resource {
            id: self.project_id,
            name: self.req.name.clone(),
            description: self.req.description.clone(),
            title: self.req.title.clone(),
            revision: 0,
            variant: crate::models::models::ResourceVariant::Project,
            labels: self.req.labels.clone(),
            identifiers: self.req.identifiers.clone(),
            content_len: 0,
            count: 0,
            visibility: self.req.visibility.clone(),
            created_at: time,
            last_modified: time,
            authors: self.req.authors.clone(),
            license_id: self.req.license_id.unwrap_or_default(),
            locked: false,
            deleted: false,
            location: vec![], // TODO: Locations and DataProxies
            hashes: vec![],
        };

        let group_id = self.req.group_id;
        let realm_id = self.req.realm_id;

        let endpoint = self.req.data_endpoint.clone();

        let _requester = self.requester.clone();

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            // Create project

            let mut wtxn = store.write_txn()?;

            // Get group idx
            let group_idx = store.get_idx_from_ulid_validate(
                &group_id,
                "group_id",
                &[NodeVariant::Group],
                &wtxn,
            )?;
            // Get the realm
            let realm_idx = store.get_idx_from_ulid_validate(
                &realm_id,
                "realm_id",
                &[NodeVariant::Realm],
                &wtxn,
            )?;

            // Validate that the data endpoint is part of the realm
            let endpoint_idx = if let Some(data_endpoint) = endpoint {
                // Get the endpoint
                let data_endpoint_idx = store.get_idx_from_ulid_validate(
                    &data_endpoint,
                    "data_endpoint",
                    &[NodeVariant::Component],
                    &wtxn,
                )?;

                if !wtxn.get_ro_graph().has_relation(
                    realm_idx,
                    data_endpoint_idx,
                    &[relation_types::REALM_USES_COMPONENT],
                )? {
                    error!("Realm does not use this data endpoint");
                    return Err(ArunaError::InvalidParameter {
                        name: "data_endpoint".to_string(),
                        error: "Realm does not use this data endpoint".to_string(),
                    });
                }
                Some(data_endpoint_idx)
            } else {
                debug!("No data endpoint provided");
                wtxn.get_ro_graph()
                    .get_relations(realm_idx, Some(&[DEFAULT]), Outgoing)?
                    .iter()
                    .find_map(|r| {
                        debug!("Checking relation: {:?}", r);
                        let node_type = wtxn.get_ro_graph().node_weight(r.target);
                        debug!("Node type: {:?}", node_type);
                        if node_type == Some(NodeVariant::Component) {
                            Some(r.target)
                        } else {
                            None
                        }
                    })
            };

            // Check that name is unique
            if !store
                .filtered_universe(
                    Some(&format!("name='{}' AND variant=0", project.name)),
                    &wtxn,
                )?
                .is_empty()
            {
                return Err(ArunaError::ConflictParameter {
                    name: "name".to_string(),
                    error: "Project with this name already exists".to_string(),
                });
            }

            if let Some(endpoint_idx) = endpoint_idx {
                if let Some(component) = store.get_node::<Component>(&wtxn, endpoint_idx) {
                    project.location = vec![DataLocation {
                        endpoint_id: component.id,
                        status: SyncingStatus::Finished,
                    }];
                } else {
                    return Err(ArunaError::NotFound(endpoint_idx.0.to_string()));
                }
            }

            // Create project
            let project_idx = store.create_node(&mut wtxn, &project)?;

            // Add relation group --OWNS_PROJECT--> project
            store.create_relation(
                &mut wtxn,
                group_idx,
                project_idx,
                relation_types::OWNS_PROJECT,
            )?;

            // Add relation realm --PROJECT_PART_OF_REALM--> project
            // TODO: Check if this should be a relation or a field
            store.create_relation(
                &mut wtxn,
                realm_idx,
                project_idx,
                relation_types::PROJECT_PART_OF_REALM,
            )?;

            store.add_subscriber(
                &mut wtxn,
                Subscriber {
                    id: group_id,
                    owner: group_id,
                    target_idx: project_idx.0,
                    cascade: true,
                },
            )?;

            match project.visibility {
                crate::models::models::VisibilityClass::Public
                | crate::models::models::VisibilityClass::PublicMetadata => {
                    store.add_public_resources_universe(&mut wtxn, &[project_idx.0])?;
                }
                crate::models::models::VisibilityClass::Private => {
                    store.add_read_permission_universe(&mut wtxn, group_idx.0, &[project_idx.0])?;
                }
            };

            // Notify the endpoint if user wants to add a data endpoint
            let additional_affected = if let Some(endpoint_idx) = endpoint_idx {
                vec![endpoint_idx]
            } else {
                vec![]
            };

            // Affected nodes: Group, Realm, Project
            wtxn.commit(
                associated_event_id,
                &[realm_idx, group_idx, project_idx],
                &additional_affected,
            )?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&CreateProjectResponse {
                resource: project,
            })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for CreateResourceRequest {
    type Response = CreateResourceResponse;
    fn get_context(&self) -> Context {
        Context::Permission {
            min_permission: crate::models::models::Permission::Write,
            source: self.parent_id,
        }
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        // Check the variant before running consensus
        if self.variant == crate::models::models::ResourceVariant::Project {
            return Err(ArunaError::InvalidParameter {
                name: "variant".to_string(),
                error: "Wrong request, use CreateProjectRequest to create projects".to_string(),
            });
        }

        let request_tx = CreateResourceRequestTx {
            req: self,
            resource_id: Ulid::new(),
            requester: requester
                .ok_or_else(|| ArunaError::Unauthorized)
                .inspect_err(logerr!())?,
            created_at: Utc::now().timestamp_millis(),
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response).inspect_err(logerr!())?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateResourceRequestTx {
    req: CreateResourceRequest,
    requester: Requester,
    resource_id: Ulid,
    created_at: i64,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateResourceRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        info!("Executing CreateResourceRequestTx");

        controller.authorize(&self.requester, &self.req).await?;

        let time = DateTime::from_timestamp_millis(self.created_at).ok_or_else(|| {
            ArunaError::ConversionError {
                from: "i64".to_string(),
                to: "Chrono::DateTime".to_string(),
            }
        })?;

        let mut resource = Resource {
            id: self.resource_id,
            name: self.req.name.clone(),
            description: self.req.description.clone(),
            title: self.req.title.clone(),
            revision: 0,
            variant: self.req.variant.clone(),
            labels: self.req.labels.clone(),
            identifiers: self.req.identifiers.clone(),
            content_len: 0,
            count: 0,
            visibility: self.req.visibility.clone(),
            created_at: time,
            last_modified: time,
            authors: self.req.authors.clone(),
            license_id: self.req.license_id.unwrap_or_default(),
            locked: false,
            deleted: false,
            location: vec![], // TODO: Locations and DataProxies
            hashes: vec![],
        };

        let parent_id = self.req.parent_id;

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            // Create resource

            let mut wtxn = store.write_txn()?;

            let Some(parent_idx) = store.get_idx_from_ulid(&parent_id, &wtxn) else {
                return Err(ArunaError::NotFound(parent_id.to_string()));
            };

            // Make sure that the parent is a folder or project
            let parent_node = store
                .get_node::<Resource>(&wtxn, parent_idx)
                .expect("Idx exist but no node in documents -> corrupted database");

            if !matches!(
                parent_node.variant,
                ResourceVariant::Project | ResourceVariant::Folder
            ) {
                return Err(ArunaError::InvalidParameter {
                    name: "parent_id".to_string(),
                    error: "Wrong parent, must be folder or project".to_string(),
                });
            }

            let mut parent_endpoints = parent_node.location.clone();
            let affected = parent_endpoints
                .iter()
                .filter_map(|l| store.get_idx_from_ulid(&l.endpoint_id, &wtxn))
                .collect::<Vec<_>>();

            if resource.variant == ResourceVariant::Object {
                parent_endpoints.iter_mut().for_each(|l| {
                    l.status = SyncingStatus::Pending;
                });
            }

            resource.location = parent_endpoints;

            // Check for unique name 1. Get all resources with the same name
            // 0 Project, 1 Folder, 2 Object -> < 3
            let universe = store.filtered_universe(
                Some(&format!("name='{}' AND variant < 3", resource.name)),
                &wtxn,
            )?;

            // 2. Check if any of the resources in the universe have the same parent as a parent
            for idx in universe {
                // We need to use this because we have a lock on the store
                if wtxn.get_ro_graph().get_parent(MilliIdx(idx)) == Some(parent_idx) {
                    return Err(ArunaError::ConflictParameter {
                        name: "name".to_string(),
                        error: "Resource with this name already exists in this hierarchy"
                            .to_string(),
                    });
                }
            }

            // Create resource
            let resource_idx = store.create_node(&mut wtxn, &resource)?;

            // Add relation parent --HAS_PART--> resource
            store.create_relation(
                &mut wtxn,
                parent_idx,
                resource_idx,
                relation_types::HAS_PART,
            )?;

            match resource.visibility {
                crate::models::models::VisibilityClass::Public
                | crate::models::models::VisibilityClass::PublicMetadata => {
                    store.add_public_resources_universe(&mut wtxn, &[resource_idx.0])?;
                }
                crate::models::models::VisibilityClass::Private => {
                    let groups = wtxn.get_ro_graph().get_related_user_or_groups(parent_idx)?;
                    for group_idx in groups {
                        store.add_read_permission_universe(
                            &mut wtxn,
                            group_idx.0,
                            &[resource_idx.0],
                        )?;
                    }
                }
            };

            // Affected nodes: Group, Project
            wtxn.commit(associated_event_id, &[parent_idx, resource_idx], &affected)?;
            Ok::<_, ArunaError>(bincode::serialize(&CreateResourceResponse { resource })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for CreateResourceBatchRequest {
    type Response = CreateResourceBatchResponse;
    fn get_context(&self) -> Context {
        Context::PermissionBatch(
            self.resources
                .iter()
                .filter_map(|r| {
                    if let Parent::ID(id) = r.parent {
                        Some(BatchPermission {
                            min_permission: crate::models::models::Permission::Write,
                            source: id,
                        })
                    } else {
                        None
                    }
                })
                .collect(),
        )
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        // Check the variant before running consensus
        let mut resource_ids = Vec::new();
        for resource in &self.resources {
            if resource.variant == crate::models::models::ResourceVariant::Project {
                return Err(ArunaError::InvalidParameter {
                    name: "variant".to_string(),
                    error: "Wrong request, use CreateProjectRequest to create projects".to_string(),
                });
            }
            resource_ids.push(Ulid::new());
        }

        let request_tx = CreateResourceBatchRequestTx {
            req: self,
            resource_ids,
            requester: requester
                .ok_or_else(|| ArunaError::Unauthorized)
                .inspect_err(logerr!())?,
            created_at: Utc::now().timestamp_millis(),
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response).inspect_err(logerr!())?)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateResourceBatchRequestTx {
    req: CreateResourceBatchRequest,
    requester: Requester,
    resource_ids: Vec<Ulid>,
    created_at: i64,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateResourceBatchRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        info!("Executing CreateResourceRequestTx");

        controller.authorize(&self.requester, &self.req).await?;

        //let transaction = self.clone();
        //let (parents_to_check, resources) = self.parse_resources()?;
        let request = self.req.clone();
        let ids = self.resource_ids.clone();
        let time = DateTime::from_timestamp_millis(self.created_at).ok_or_else(|| {
            ArunaError::ConversionError {
                from: "i64".to_string(),
                to: "Chrono::DateTime".to_string(),
            }
        })?;

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            // Create resource

            let mut wtxn = store.write_txn()?;

            let mut to_create: Vec<(Resource, Ulid)> = vec![];
            let mut existing_names = HashMap::<u32, String, RandomState>::default();
            let mut affected_endpoints = vec![];

            for (index, batch_resource) in request.resources.iter().enumerate() {
                let mut resource = Resource {
                    id: ids
                        .get(index)
                        .ok_or_else(|| {
                            error!("Id not found: {}", index);
                            ArunaError::DeserializeError("Id not found".to_string())
                        })?
                        .clone(),
                    name: batch_resource.name.clone(),
                    description: batch_resource.description.clone(),
                    title: batch_resource.title.clone(),
                    revision: 0,
                    variant: batch_resource.variant.clone(),
                    labels: batch_resource.labels.clone(),
                    identifiers: batch_resource.identifiers.clone(),
                    content_len: 0,
                    count: 0,
                    visibility: batch_resource.visibility.clone(),
                    created_at: time,
                    last_modified: time,
                    authors: batch_resource.authors.clone(),
                    license_id: batch_resource.license_id.unwrap_or_default(),
                    locked: false,
                    deleted: false,
                    location: vec![], // TODO: Locations and DataProxies
                    hashes: vec![],
                };
                let parent_id = match batch_resource.parent {
                    Parent::ID(ulid) => {
                        let parent_existing_idx =
                            store.get_idx_from_ulid(&ulid, &wtxn).ok_or_else(|| {
                                error!("Parent not found: {}", ulid);
                                ArunaError::NotFound(ulid.to_string())
                            })?;

                        let parent_node = store
                            .get_node::<Resource>(&wtxn, parent_existing_idx)
                            .expect("Idx exist but no node in documents -> corrupted database");

                        if !matches!(
                            parent_node.variant,
                            ResourceVariant::Project | ResourceVariant::Folder
                        ) {
                            return Err(ArunaError::InvalidParameter {
                                name: "parent_id".to_string(),
                                error: "Wrong parent, must be folder or project".to_string(),
                            });
                        }

                        let mut parent_endpoints = parent_node.location.clone();
                        affected_endpoints.extend(
                            parent_endpoints
                                .iter()
                                .filter_map(|l| store.get_idx_from_ulid(&l.endpoint_id, &wtxn)),
                        );

                        if resource.variant == ResourceVariant::Object {
                            parent_endpoints.iter_mut().for_each(|l| {
                                l.status = SyncingStatus::Pending;
                            });
                        }

                        resource.location = parent_endpoints;

                        // Check for unique name 1. Get all resources with the same name
                        // 0 Project, 1 Folder, 2 Object -> < 3
                        let universe = store.filtered_universe(
                            Some(&format!("name='{}' AND variant < 3", resource.name)),
                            &wtxn,
                        )?;

                        // 2. Check if any of the resources in the universe have the same parent as a parent
                        for idx in universe {
                            // We need to use this because we have a lock on the store
                            if wtxn.get_ro_graph().get_parent(MilliIdx(idx))
                                == Some(parent_existing_idx)
                            {
                                return Err(ArunaError::ConflictParameter {
                                    name: "name".to_string(),
                                    error:
                                        "Resource with this name already exists in this hierarchy"
                                            .to_string(),
                                });
                            }
                        }
                        ulid
                    }
                    Parent::Idx(index) => {
                        let parent: &Resource = &to_create
                            .get(index as usize)
                            .ok_or_else(|| {
                                error!("Parent not found: {}", index);
                                ArunaError::NotFound(index.to_string())
                            })?
                            .0;

                        if !matches!(
                            parent.variant,
                            ResourceVariant::Project | ResourceVariant::Folder
                        ) {
                            return Err(ArunaError::InvalidParameter {
                                name: "parent_id".to_string(),
                                error: "Wrong parent, must be folder or project".to_string(),
                            });
                        }

                        let mut parent_endpoints = parent.location.clone();

                        if resource.variant == ResourceVariant::Object {
                            parent_endpoints.iter_mut().for_each(|l| {
                                l.status = SyncingStatus::Pending;
                            });
                        }

                        resource.location = parent_endpoints;

                        if existing_names
                            .insert(index, resource.name.clone())
                            .is_some()
                        {
                            return Err(ArunaError::ConflictParameter {
                                name: "name".to_string(),
                                error: "Resource with this name already exists in this hierarchy"
                                    .to_string(),
                            });
                        }
                        parent.id
                    }
                };

                to_create.push((resource, parent_id));
            }

            let resource_idx =
                store.create_nodes_batch(&mut wtxn, to_create.iter().map(|e| &e.0).collect())?;

            let mut affected = vec![];
            for (resource, parent_id) in &to_create {
                let Some(parent_idx) = store.get_idx_from_ulid(&parent_id, &wtxn) else {
                    return Err(ArunaError::NotFound(parent_id.to_string()));
                };
                let (_, idx) = resource_idx
                    .iter()
                    .find(|(id, _)| id == &resource.id)
                    .ok_or_else(|| ArunaError::DeserializeError("Idx not found".to_string()))?;

                // Add relation parent --HAS_PART--> resource
                store.create_relation(&mut wtxn, parent_idx, *idx, relation_types::HAS_PART)?;
                affected.push(*idx);

                // Add resource to related universes
                match resource.visibility {
                    crate::models::models::VisibilityClass::Public
                    | crate::models::models::VisibilityClass::PublicMetadata => {
                        store.add_public_resources_universe(&mut wtxn, &[idx.0])?;
                    }
                    crate::models::models::VisibilityClass::Private => {
                        let groups = wtxn.get_ro_graph().get_related_user_or_groups(parent_idx)?;
                        for group_idx in groups {
                            store.add_read_permission_universe(&mut wtxn, group_idx.0, &[idx.0])?;
                        }
                    }
                };
            }

            // Affected nodes: Group, Project
            wtxn.commit(associated_event_id, &affected, &[])?;
            Ok::<_, ArunaError>(bincode::serialize(&CreateResourceBatchResponse {
                resources: to_create.into_iter().map(|(r, _)| r).collect(),
            })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for GetResourcesRequest {
    type Response = GetResourcesResponse;
    fn get_context(&self) -> Context {
        Context::InRequest
        // Context::PermissionBatch(
        //     self.ids
        //         .iter()
        //         .map(|id| BatchPermission {
        //             min_permission: crate::models::models::Permission::Read,
        //             source: *id,
        //         })
        //         .collect(),
        // )
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        // let public = if let Some(requester) = requester {
        //     controller.authorize(&requester, &self).await?;
        //     false
        // } else {
        //     true
        // };

        let check_public = if let Some(requester) = requester {
            let mut check = vec![];
            for id in self.ids.iter() {
                match controller
                    .authorize_with_context(
                        &requester,
                        &self,
                        Context::Permission {
                            min_permission: crate::models::models::Permission::Read,
                            source: *id,
                        },
                    )
                    .await
                {
                    Ok(_) => check.push((*id, false)),
                    Err(err) => check.push((*id, matches!(err, ArunaError::Forbidden(_)))),
                }
            }
            check
        } else {
            self.ids.iter().map(|id| (*id, true)).collect()
        };

        let store = controller.get_store();
        let response = tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;
            let mut resources = Vec::new();

            for (id, must_be_public) in check_public {
                let idx = store
                    .get_idx_from_ulid(&id, &rtxn)
                    .ok_or_else(|| ArunaError::NotFound(id.to_string()))?;

                let resource = store
                    .get_node::<Resource>(&rtxn, idx)
                    .ok_or_else(|| ArunaError::NotFound(id.to_string()))?;

                if must_be_public {
                    if !matches!(
                        resource.visibility,
                        crate::models::models::VisibilityClass::Public
                    ) {
                        return Err(ArunaError::Unauthorized);
                    }
                }
                resources.push(resource);
            }
            Ok::<_, ArunaError>(GetResourcesResponse { resources })
        })
        .await
        .map_err(|e| ArunaError::ServerError(e.to_string()))??;

        Ok(response)
    }
}

impl Request for ResourceUpdateRequests {
    type Response = ResourceUpdateResponses;
    fn get_context(&self) -> Context {
        Context::Permission {
            min_permission: crate::models::models::Permission::Write,
            source: self.get_id(),
        }
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let request_tx = UpdateResourceTx {
            req: self,
            requester: requester.ok_or_else(|| ArunaError::Unauthorized)?,
            updated_at: Utc::now().timestamp_millis(),
        };
        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateResourceTx {
    req: ResourceUpdateRequests,
    requester: Requester,
    updated_at: i64,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for UpdateResourceTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let time = DateTime::from_timestamp_millis(self.updated_at).ok_or_else(|| {
            ArunaError::ConversionError {
                from: "i64".to_string(),
                to: "Chrono::DateTime".to_string(),
            }
        })?;

        let resource_id = self.req.get_id();
        let request = self.req.clone();

        let store = controller.get_store();

        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            // Get resource idx
            let Some(resource_idx) = store.get_idx_from_ulid(&resource_id, &wtxn) else {
                return Err(ArunaError::NotFound(resource_id.to_string()));
            };

            let Some(old_resource): Option<Resource> = store.get_node(&wtxn, resource_idx) else {
                return Err(ArunaError::NotFound(format!(
                    "Resource with id {resource_id} not found"
                )));
            };

            let map = parse_update_fields(
                &store,
                &mut wtxn,
                old_resource,
                resource_id,
                time,
                request.clone(),
            )?;

            store.update_node_field(&mut wtxn, resource_id, map)?;

            // Affected nodes: Group, Realm, Project
            let resource = store.get_node(&wtxn, resource_idx).unwrap();
            let response = match request {
                ResourceUpdateRequests::Name(_) => {
                    ResourceUpdateResponses::Name(UpdateResourceNameResponse { resource })
                }
                ResourceUpdateRequests::Title(_) => {
                    ResourceUpdateResponses::Title(UpdateResourceTitleResponse { resource })
                }
                ResourceUpdateRequests::Description(_) => {
                    ResourceUpdateResponses::Description(UpdateResourceDescriptionResponse {
                        resource,
                    })
                }
                ResourceUpdateRequests::Visibility(_) => {
                    ResourceUpdateResponses::Visibility(UpdateResourceVisibilityResponse {
                        resource,
                    })
                }
                ResourceUpdateRequests::License(_) => {
                    ResourceUpdateResponses::License(UpdateResourceLicenseResponse { resource })
                }
                ResourceUpdateRequests::Labels(_) => {
                    ResourceUpdateResponses::Labels(UpdateResourceLabelsResponse { resource })
                }
                ResourceUpdateRequests::Identifiers(_) => {
                    ResourceUpdateResponses::Identifiers(UpdateResourceIdentifiersResponse {
                        resource,
                    })
                }
                ResourceUpdateRequests::Authors(_) => {
                    ResourceUpdateResponses::Authors(UpdateResourceAuthorsResponse { resource })
                }
            };

            wtxn.commit(associated_event_id, &[resource_idx], &[])?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&response)?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

fn parse_update_fields(
    store: &Store,
    wtxn: &mut WriteTxn,
    old_resource: Resource,
    resource_id: Ulid,
    time: DateTime<Utc>,
    request: ResourceUpdateRequests,
) -> Result<serde_json::Map<String, Value>, ArunaError> {
    let mut map = serde_json::Map::new();
    map.insert(ID_FIELD.to_string(), serde_json::to_value(resource_id)?);
    map.insert(LAST_MODIFIED_FIELD.to_string(), serde_json::to_value(time)?);
    match request {
        ResourceUpdateRequests::Name(request) => {
            if !request.name.is_empty() && request.name != old_resource.name {
                // Check that name is unique
                if !store
                    .filtered_universe(
                        Some(&format!("name='{}' AND variant=0", request.name)),
                        wtxn,
                    )?
                    .is_empty()
                {
                    return Err(ArunaError::ConflictParameter {
                        name: "name".to_string(),
                        error: "Resource with this name already exists".to_string(),
                    });
                }
            }
            map.insert(NAME_FIELD.to_string(), request.name.into());
        }
        ResourceUpdateRequests::Title(request) => {
            map.insert(TAG_FIELD.to_string(), request.title.into());
        }
        ResourceUpdateRequests::Description(request) => {
            map.insert(DESCRIPTION_FIELD.to_string(), request.description.into());
        }
        ResourceUpdateRequests::Visibility(request) => {
            if old_resource.visibility > request.visibility {
                return Err(ArunaError::ConflictParameter {
                    name: "visibility".to_string(),
                    error: "Cannot restrict visibiliyt".to_string(),
                });
            }
            let value = match request.visibility {
                crate::models::models::VisibilityClass::Public => "Public",
                crate::models::models::VisibilityClass::PublicMetadata => "PublicMetadata",
                crate::models::models::VisibilityClass::Private => "Private",
            };
            map.insert(VISIBILITY_FIELD.to_string(), value.into());
        }
        ResourceUpdateRequests::License(request) => {
            map.insert(
                LICENSE_FIELD.to_string(),
                request.license_id.to_string().into(),
            );
        }
        ResourceUpdateRequests::Labels(request) => {
            let mut labels = old_resource.labels;
            if !request.labels_to_remove.is_empty() {
                labels = labels
                    .into_iter()
                    .filter(|kv| !request.labels_to_remove.contains(kv))
                    .collect();
            }
            if !request.labels_to_add.is_empty() {
                labels.extend(request.labels_to_add);
            }
            map.insert(
                LABELS_FIELD.to_string(),
                serde_json::Value::Array(
                    labels
                        .iter()
                        .map(|kv| {
                            serde_json::to_value(kv)
                                .map_err(|e| ArunaError::DeserializeError(e.to_string()))
                        })
                        .collect::<Result<Vec<Value>, ArunaError>>()?,
                ),
            );
        }
        ResourceUpdateRequests::Identifiers(request) => {
            let mut ids = old_resource.identifiers;
            if !request.ids_to_remove.is_empty() {
                ids = ids
                    .into_iter()
                    .filter(|id| !request.ids_to_remove.contains(id))
                    .collect();
            }
            if !request.ids_to_add.is_empty() {
                ids.extend(request.ids_to_add);
            }
            map.insert(
                IDENTIFIERS_FIELD.to_string(),
                serde_json::Value::Array(
                    ids.iter()
                        .map(|id| {
                            serde_json::to_value(id)
                                .map_err(|e| ArunaError::DeserializeError(e.to_string()))
                        })
                        .collect::<Result<Vec<Value>, ArunaError>>()?,
                ),
            );
        }
        ResourceUpdateRequests::Authors(request) => {
            let mut authors = old_resource.authors;
            if !request.authors_to_remove.is_empty() {
                authors = authors
                    .into_iter()
                    .filter(|a| !request.authors_to_remove.contains(a))
                    .collect();
            }
            if !request.authors_to_add.is_empty() {
                authors.extend(request.authors_to_add);
            }
            map.insert(
                AUTHORS_FIELD.to_string(),
                serde_json::Value::Array(
                    authors
                        .iter()
                        .map(|a| {
                            serde_json::to_value(a)
                                .map_err(|e| ArunaError::DeserializeError(e.to_string()))
                        })
                        .collect::<Result<Vec<Value>, ArunaError>>()?,
                ),
            );
        }
    };
    Ok(map)
}

impl Request for RegisterDataRequest {
    type Response = RegisterDataResponse;
    fn get_context(&self) -> Context {
        Context::Permission {
            min_permission: crate::models::models::Permission::Write,
            source: self.object_id,
        }
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let request_tx = RegisterDataRequestTx {
            req: self,
            requester: requester.ok_or_else(|| ArunaError::Unauthorized)?,
            updated_at: Utc::now().timestamp_millis(),
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterDataRequestTx {
    req: RegisterDataRequest,
    requester: Requester,
    updated_at: i64,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for RegisterDataRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let time = DateTime::from_timestamp_millis(self.updated_at).ok_or_else(|| {
            ArunaError::ConversionError {
                from: "i64".to_string(),
                to: "Chrono::DateTime".to_string(),
            }
        })?;

        let object_id = self.req.object_id;
        let request = self.req.clone();

        let store = controller.get_store();

        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            // Get resource idx
            let Some(resource_idx) = store.get_idx_from_ulid(&object_id, &wtxn) else {
                return Err(ArunaError::NotFound(object_id.to_string()));
            };

            let Some(resource): Option<Resource> = store.get_node(&wtxn, resource_idx) else {
                return Err(ArunaError::NotFound(format!(
                    "Resource with id {object_id} not found"
                )));
            };

            if resource.variant != ResourceVariant::Object {
                return Err(ArunaError::InvalidParameter {
                    name: "object_id".to_string(),
                    error: "Resource is not an object".to_string(),
                });
            }

            let mut update = serde_json::Map::new();

            let mut existing_endpoints = resource.location.clone();

            let mut updated = false;
            for location in existing_endpoints.iter_mut() {
                if location.endpoint_id == request.component_id {
                    location.status = SyncingStatus::Finished;
                    updated = true;
                }
            }

            if !updated {
                existing_endpoints.push(DataLocation {
                    endpoint_id: request.component_id,
                    status: SyncingStatus::Finished,
                });
            }

            update.insert(
                "location".to_string(),
                serde_json::to_value(existing_endpoints)?,
            );
            update.insert("hashes".to_string(), serde_json::to_value(request.hashes)?);
            update.insert("updated_at".to_string(), serde_json::to_value(time)?);

            store.update_node_field(&mut wtxn, resource.id, update)?;

            // Affected nodes: Group, Realm, Project
            wtxn.commit(associated_event_id, &[resource_idx], &[])?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&RegisterDataResponse {})?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for AuthorizeRequest {
    type Response = AuthorizeResponse;
    fn get_context(&self) -> Context {
        Context::Permission {
            min_permission: crate::models::models::Permission::Read,
            source: self.id,
        }
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let allowed = if let Some(requester) = requester {
            controller.authorize(&requester, &self).await.is_ok()
        } else {
            let store = controller.get_store();
            let id = self.id;
            tokio::task::spawn_blocking(move || {
                let rtxn = store.read_txn()?;
                let Some(idx) = store.get_idx_from_ulid(&id, &rtxn) else {
                    return Err(ArunaError::NotFound(format!("{id} not found")));
                };
                let Some(node) = store.get_node::<Resource>(&rtxn, idx) else {
                    return Err(ArunaError::NotFound(format!("{id} not found")));
                };
                Ok::<bool, ArunaError>(matches!(node.visibility, VisibilityClass::Public))
            })
            .await
            .map_err(|e| {
                error!("Failed to join task: {}", e);
                ArunaError::ServerError("".to_string())
            })??
        };
        Ok(AuthorizeResponse { allowed })
    }
}

impl Request for DeleteRequest {
    type Response = DeleteResponse;
    fn get_context(&self) -> Context {
        Context::Permission {
            min_permission: crate::models::models::Permission::Admin,
            source: self.id,
        }
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let Some(requester) = requester else {
            return Err(ArunaError::Unauthorized);
        };
        let request_tx = DeleteTx {
            req: self,
            requester,
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response).inspect_err(logerr!())?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteTx {
    req: DeleteRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for DeleteTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let store = controller.get_store();
        let node_id = self.req.id;

        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            // Get resource idx
            let Some(resource_idx) = store.get_idx_from_ulid(&node_id, &wtxn) else {
                return Err(ArunaError::NotFound(node_id.to_string()));
            };

            let Some(raw_node) = store.get_raw_node(&wtxn, resource_idx) else {
                return Err(ArunaError::NotFound(format!(
                    "Resource with id {node_id} not found"
                )));
            };

            let variant: NodeVariant = serde_json::from_slice::<u8>(
                raw_node
                    .get(1)
                    .expect("Missing variant -> corrupted database"),
            )
            .inspect_err(logerr!())?
            .try_into()
            .inspect_err(logerr!())?;
            let to_delete_ids = match variant {
                NodeVariant::ResourceProject => {
                    // Collect all children
                    let mut to_delete: Vec<u32> = wtxn
                        .get_ro_graph()
                        .get_all_children(resource_idx)?
                        .iter()
                        .map(|idx| idx.0)
                        .collect();
                    // Add project
                    to_delete.push(resource_idx.0);
                    to_delete
                }
                NodeVariant::ResourceFolder => {
                    // Collect all children
                    let mut to_delete: Vec<u32> = wtxn
                        .get_ro_graph()
                        .get_all_children(resource_idx)?
                        .iter()
                        .map(|idx| idx.0)
                        .collect();
                    // Add folder
                    to_delete.push(resource_idx.0);
                    to_delete
                }
                NodeVariant::ResourceObject => {
                    // Only object needs to be added
                    vec![resource_idx.0]
                }
                NodeVariant::Realm => {
                    let graph = wtxn.get_ro_graph();
                    let mut to_delete = Vec::new();
                    // Collect projects
                    let projects = graph
                        .get_relations(
                            resource_idx,
                            Some(&[PROJECT_PART_OF_REALM]),
                            Direction::Outgoing,
                        )?
                        .iter()
                        .map(|rel| rel.target)
                        .collect::<Vec<MilliIdx>>();
                    // Collect project children
                    for project_idx in projects {
                        to_delete
                            .extend(graph.get_all_children(project_idx)?.iter().map(|idx| idx.0));
                    }
                    // Add realm
                    to_delete.push(resource_idx.0);
                    to_delete
                }
                NodeVariant::User => {
                    // TODO
                    // - check for Groups that only have User as Admin
                    // - check for Realms that only have User as Admin
                    // - get User & Tokens
                    return Err(ArunaError::Forbidden("Users cannot be deleted".to_string()));
                }
                NodeVariant::ServiceAccount => {
                    vec![resource_idx.0]
                }
                NodeVariant::Group => {
                    let graph = wtxn.get_ro_graph();
                    let mut to_delete = Vec::new();
                    // Collect projects
                    let projects = graph
                        .get_relations(resource_idx, Some(&[OWNS_PROJECT]), Direction::Outgoing)?
                        .iter()
                        .map(|rel| rel.target)
                        .collect::<Vec<MilliIdx>>();
                    // Collect project children
                    for project_idx in projects {
                        to_delete
                            .extend(graph.get_all_children(project_idx)?.iter().map(|idx| idx.0));
                    }
                    // Add group
                    to_delete.push(resource_idx.0);
                    to_delete
                }
                NodeVariant::Component => {
                    // TODO:
                    // - Find out which realms use component
                    // - Set all objects that use this component at this component to unavailable
                    // - component
                    // vec![]
                    return Err(ArunaError::ServerError("Not implemented".to_string()));
                }
                NodeVariant::License => {
                    return Err(ArunaError::ServerError("Not implemented".to_string()));
                }
            };

            store.delete_nodes(&mut wtxn, &to_delete_ids)?;

            // TODO: Delete related events
            wtxn.commit(associated_event_id, &[resource_idx], &[])?;

            Ok::<_, ArunaError>(bincode::serialize(&RegisterDataResponse {})?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}
