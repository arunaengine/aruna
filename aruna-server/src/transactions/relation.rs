use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    constants::relation_types,
    context::Context,
    error::ArunaError,
    models::{
        models::{RelationInfo, Resource},
        requests::{
            CreateRelationRequest, CreateRelationResponse, CreateRelationVariantRequest,
            CreateRelationVariantResponse, Direction, GetRelationInfosRequest,
            GetRelationInfosResponse, GetRelationsRequest, GetRelationsResponse,
        },
    },
    transactions::request::WriteRequest,
};

use super::{
    controller::Controller,
    request::{Request, Requester, SerializedResponse},
};

impl Request for GetRelationsRequest {
    type Response = GetRelationsResponse;
    fn get_context(&self) -> Context {
        Context::InRequest
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        // Disallow impersonation
        if requester
            .as_ref()
            .and_then(|r| r.get_impersonator())
            .is_some()
        {
            return Err(ArunaError::Unauthorized);
        }

        let check_public = if let Some(requester) = requester {
            match controller
                .authorize_with_context(
                    &requester,
                    &self,
                    Context::Permission {
                        min_permission: crate::models::models::Permission::Read,
                        source: self.node,
                    },
                )
                .await {
                    Ok(_) => false,
                    Err(err) => matches!(err, ArunaError::Forbidden(_)),
            }
        } else {
            true
        };

        let store = controller.get_store();
        let response = tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;

            let idx = store
                .get_idx_from_ulid(&self.node, &rtxn)
                .ok_or_else(|| ArunaError::NotFound(self.node.to_string()))?;

            // Check if resource access is public
            // TODO: This will currently not work because we require permission read to the node
            if check_public {
                let resource = store
                    .get_node::<Resource>(&rtxn, idx)
                    .ok_or_else(|| ArunaError::NotFound(self.node.to_string()))?;
                if !matches!(
                    resource.visibility,
                    crate::models::models::VisibilityClass::Public
                ) {
                    return Err(ArunaError::Unauthorized);
                }
            }

            let offset = self.offset.unwrap_or_default();

            let filter: Option<&[u32]> = if self.filter.is_empty() {
                None
            } else {
                Some(&self.filter)
            };

            let relations = match self.direction {
                Direction::Incoming => {
                    store.get_relations(idx, filter, petgraph::Direction::Incoming, &rtxn)?
                }
                Direction::Outgoing => {
                    store.get_relations(idx, filter, petgraph::Direction::Outgoing, &rtxn)?
                }
                Direction::All => {
                    let mut relations =
                        store.get_relations(idx, filter, petgraph::Direction::Incoming, &rtxn)?;
                    relations.extend(store.get_relations(
                        idx,
                        filter,
                        petgraph::Direction::Outgoing,
                        &rtxn,
                    )?);
                    relations
                }
            };
            let total_hits = relations.len() as u32;

            let new_offset = if relations.len() < offset + self.page_size {
                None
            } else {
                Some(offset + self.page_size)
            };

            let relations = match relations.get(offset..offset + self.page_size) {
                Some(relations) => relations.to_vec(),
                None => match relations.get(offset..) {
                    Some(relations) => relations.to_vec(),
                    None => relations,
                },
            };

            Ok::<_, ArunaError>(GetRelationsResponse {
                relations,
                offset: new_offset,
                total_hits,
            })
        })
        .await
        .map_err(|e| ArunaError::ServerError(e.to_string()))??;

        Ok(response)
    }
}

impl Request for GetRelationInfosRequest {
    type Response = GetRelationInfosResponse;
    fn get_context(&self) -> Context {
        Context::Public
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        // Disallow impersonation
        if requester
            .as_ref()
            .and_then(|r| r.get_impersonator())
            .is_some()
        {
            return Err(ArunaError::Unauthorized);
        }
        let store = controller.get_store();
        tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;
            let relation_infos = store.get_relation_infos(&rtxn)?;
            Ok::<_, ArunaError>(GetRelationInfosResponse { relation_infos })
        })
        .await
        .map_err(|e| ArunaError::ServerError(e.to_string()))?
    }
}

impl Request for CreateRelationRequest {
    type Response = CreateRelationResponse;
    fn get_context(&self) -> Context {
        Context::PermissionFork {
            first_source: self.source,
            first_min_permission: crate::models::models::Permission::Write,
            second_min_permission: crate::models::models::Permission::Write,
            second_source: self.source,
        }
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        // Disallow impersonation
        if requester
            .as_ref()
            .and_then(|r| r.get_impersonator())
            .is_some()
        {
            return Err(ArunaError::Unauthorized);
        }
        let request_tx = CreateRelationTx {
            req: self,
            requester: requester.ok_or_else(|| ArunaError::Unauthorized)?,
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateRelationTx {
    req: CreateRelationRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateRelationTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let source_id = self.req.source;
        let target_id = self.req.target;
        let variant = self.req.variant;

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            let Some(source_idx) = store.get_idx_from_ulid(&source_id, wtxn.get_txn()) else {
                return Err(ArunaError::NotFound(format!("{source_id} not found")));
            };
            let Some(target_idx) = store.get_idx_from_ulid(&target_id, wtxn.get_txn()) else {
                return Err(ArunaError::NotFound(format!("{source_id} not found")));
            };
            match variant {
                relation_types::HAS_PART..=relation_types::PROJECT_PART_OF_REALM => {
                    return Err(ArunaError::Forbidden(
                        "Forbidden to set internal relations".to_string(),
                    ));
                }
                _ => {
                    if !store.get_relation_info(&variant, wtxn.get_txn())?.is_some() {
                        return Err(ArunaError::NotFound(format!(
                            "Relation variant_idx {variant} not found"
                        )));
                    }
                }
            }
            store.create_relation(&mut wtxn, source_idx, target_idx, variant)?;

            wtxn.commit(associated_event_id, &[source_idx, target_idx], &[])?;

            Ok::<_, ArunaError>(bincode::serialize(&CreateRelationResponse {})?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for CreateRelationVariantRequest {
    type Response = CreateRelationVariantResponse;
    fn get_context(&self) -> Context {
        Context::UserOnly
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        // Disallow impersonation
        if requester
            .as_ref()
            .and_then(|r| r.get_impersonator())
            .is_some()
        {
            return Err(ArunaError::Unauthorized);
        }
        let request_tx = CreateRelationVariantTx {
            req: self,
            requester: requester.ok_or_else(|| ArunaError::Unauthorized)?,
        };
        let response = controller.transaction(Ulid::new().0, &request_tx).await?;
        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateRelationVariantTx {
    req: CreateRelationVariantRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateRelationVariantTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let store = controller.get_store();
        let forward_type = self.req.forward_type.clone();
        let backward_type = self.req.backward_type.clone();

        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            let idx = store
                .get_relation_infos(&wtxn.get_txn())?
                .iter()
                .map(|i| i.idx)
                .max()
                // This should never happen if the database is loaded
                .expect("Relation infos is empty!")
                + 1;
            let info = RelationInfo {
                idx,
                forward_type,
                backward_type,
                internal: false,
            };
            store.create_relation_variant(&mut wtxn, info)?;

            wtxn.commit(associated_event_id, &[], &[])?;
            Ok::<_, ArunaError>(bincode::serialize(&CreateRelationVariantResponse { idx })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}
