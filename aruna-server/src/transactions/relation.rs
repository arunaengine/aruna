use base64::{prelude::BASE64_STANDARD, Engine};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    constants::relation_types,
    context::Context,
    error::ArunaError,
    models::{
        models::{ContinuationToken, RelationInfo, RelationRange, Resource},
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
                .await
            {
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

            let continuation_idx: Option<ContinuationToken> = match self.continuation_token {
                Some(token) => Some(bincode::deserialize(&BASE64_STANDARD.decode(token)?)?),
                None => None,
            };
            let page_limit = self.page_size;

            let filter: Option<&[u32]> = if self.filter.is_empty() {
                None
            } else {
                Some(&self.filter)
            };

            let (relations, token) = match self.direction {
                Direction::Incoming => {
                    let range = match continuation_idx {
                        Some(ContinuationToken {
                            last_incoming: Some(last_entry),
                            ..
                        }) => RelationRange {
                            last_entry: Some(last_entry),
                            page_size: page_limit as u32,
                        },
                        None => RelationRange {
                            last_entry: None,
                            page_size: page_limit as u32,
                        },
                        _ => {
                            tracing::error!("Invalid ContinuationToken");
                            RelationRange {
                                last_entry: None,
                                page_size: page_limit as u32,
                            }
                        }
                    };
                    let (relations, last_entry) = store.get_relation_range(
                        idx,
                        filter,
                        petgraph::Direction::Incoming,
                        range,
                        &rtxn,
                    )?;
                    let token = ContinuationToken {
                        last_incoming: Some(last_entry),
                        last_outgoing: None,
                    };
                    (relations, token)
                }
                Direction::Outgoing => {
                    let range = match continuation_idx {
                        Some(ContinuationToken {
                            last_outgoing: Some(last_entry),
                            ..
                        }) => RelationRange {
                            last_entry: Some(last_entry),
                            page_size: page_limit as u32,
                        },
                        None => RelationRange {
                            last_entry: None,
                            page_size: page_limit as u32,
                        },
                        _ => {
                            tracing::error!("Invalid ContinuationToken");
                            RelationRange {
                                last_entry: None,
                                page_size: page_limit as u32,
                            }
                        }
                    };
                    let (relations, last_entry) = store.get_relation_range(
                        idx,
                        filter,
                        petgraph::Direction::Outgoing,
                        range,
                        &rtxn,
                    )?;

                    let token = ContinuationToken {
                        last_incoming: None,
                        last_outgoing: Some(last_entry),
                    };
                    (relations, token)
                }
                Direction::All => {
                    let (in_range, out_range) = match continuation_idx {
                        Some(ContinuationToken {
                            last_outgoing: Some(last_out),
                            last_incoming: Some(last_in),
                        }) => (
                            RelationRange {
                                last_entry: Some(last_in),
                                page_size: page_limit as u32,
                            },
                            RelationRange {
                                last_entry: Some(last_out),
                                page_size: page_limit as u32,
                            },
                        ),
                        None => {
                            let range = RelationRange {
                                last_entry: None,
                                page_size: page_limit as u32,
                            };
                            (range.clone(), range)
                        }
                        _ => {
                            tracing::error!("Invalid ContinuationToken");
                            let range = RelationRange {
                                last_entry: None,
                                page_size: page_limit as u32,
                            };
                            (range.clone(), range)
                        }
                    };
                    let (mut relations, incoming_last_entry) = store.get_relation_range(
                        idx,
                        filter,
                        petgraph::Direction::Incoming,
                        in_range,
                        &rtxn,
                    )?;
                    let (outgoing_relations, outgoing_last_entry) = store.get_relation_range(
                        idx,
                        filter,
                        petgraph::Direction::Outgoing,
                        out_range,
                        &rtxn,
                    )?;
                    relations.extend(outgoing_relations);
                    (
                        relations,
                        ContinuationToken {
                            last_incoming: Some(incoming_last_entry),
                            last_outgoing: Some(outgoing_last_entry),
                        },
                    )
                }
            };

            let continuation_token = if relations.len() < page_limit {
                None
            } else {
                let token = BASE64_STANDARD.encode(bincode::serialize(&token)?);
                Some(token)
            };

            Ok::<_, ArunaError>(GetRelationsResponse {
                relations,
                continuation_token,
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

            let Some(source_idx) = store.get_idx_from_ulid(&source_id, &wtxn) else {
                return Err(ArunaError::NotFound(format!("{source_id} not found")));
            };
            let Some(target_idx) = store.get_idx_from_ulid(&target_id, &wtxn) else {
                return Err(ArunaError::NotFound(format!("{source_id} not found")));
            };
            match variant {
                relation_types::HAS_PART..=relation_types::PROJECT_PART_OF_REALM => {
                    return Err(ArunaError::Forbidden(
                        "Forbidden to set internal relations".to_string(),
                    ));
                }
                _ => {
                    if !store.get_relation_info(&variant, &wtxn)?.is_some() {
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
                .get_relation_infos(&wtxn)?
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
