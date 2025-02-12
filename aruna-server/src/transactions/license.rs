use super::{
    controller::Controller,
    request::{Request, Requester, SerializedResponse},
};
use crate::{
    context::Context,
    error::ArunaError,
    models::{
        models::{GenericNode, License},
        requests::{
            CreateLicenseRequest, CreateLicenseResponse, GetLicenseRequest, GetLicenseResponse,
            GetLicensesRequest, GetLicensesResponse,
        },
    },
    transactions::request::WriteRequest,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

impl Request for CreateLicenseRequest {
    type Response = CreateLicenseResponse;
    fn get_context(&self) -> Context {
        Context::UserOnly
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let requester = requester.ok_or_else(|| ArunaError::Unauthorized)?;

        let request_tx = CreateLicenseRequestTx {
            req: self,
            license_id: Ulid::new(),
            requester,
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateLicenseRequestTx {
    req: CreateLicenseRequest,
    requester: Requester,
    license_id: Ulid,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateLicenseRequestTx {

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let license = License {
            id: self.license_id,
            name: self.req.name.clone(),
            description: self.req.description.clone(),
            terms: self.req.license_terms.clone(),
            deleted: false,
        };

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            // Create license
            let license_idx = store.create_node(&mut wtxn, &license)?;
            store.add_public_resources_universe(&mut wtxn, &[license_idx.0])?;

            // Affected nodes: Group, Realm, Project
            wtxn.commit(associated_event_id, &[], &[])?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&CreateLicenseResponse {
                license_id: license.id,
            })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for GetLicenseRequest {
    type Response = GetLicenseResponse;
    fn get_context(&self) -> Context {
        Context::Public
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        _requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let store = controller.get_store();
        let id = self.id;

        tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;
            let node_idx = store.get_idx_from_ulid(&id, &rtxn).ok_or_else(|| {
                ArunaError::NotFound(format!("License with id {} not found", id.to_string()))
            })?;
            let license = store.get_node::<License>(&rtxn, node_idx).ok_or_else(|| {
                ArunaError::NotFound(format!("License with id {} not found", id.to_string()))
            })?;
            rtxn.commit()?;
            Ok(GetLicenseResponse { license })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })?
    }
}

impl Request for GetLicensesRequest {
    type Response = GetLicensesResponse;
    fn get_context(&self) -> Context {
        Context::Public
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        _requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let store = controller.get_store();

        tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;

            let universe = store.get_public_universe(&rtxn)?;
            let (_, result) =
                store.search("".to_string(), 0, 10000, Some("variant=8"), &rtxn, universe)?;
            rtxn.commit()?;
            let result = result
                .into_iter()
                .filter_map(|val| match val {
                    GenericNode::License(license) => Some(license),
                    _ => None,
                })
                .collect::<Vec<_>>();

            Ok(GetLicensesResponse { licenses: result })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })?
    }
}
