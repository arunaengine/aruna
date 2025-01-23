use petgraph::Direction;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    constants::relation_types::{
        self, PERMISSION_ADMIN, PERMISSION_APPEND, PERMISSION_NONE, PERMISSION_READ,
        PERMISSION_WRITE,
    },
    context::Context,
    error::ArunaError,
    logerr,
    models::{
        models::Group,
        requests::{
            AddUserRequest, AddUserResponse, CreateGroupRequest, CreateGroupResponse,
            GetGroupRequest, GetGroupResponse, GetUsersFromGroupRequest, GetUsersFromGroupResponse,
            UserAccessGroupRequest, UserAccessGroupResponse,
        },
    },
    transactions::request::WriteRequest,
};

use super::{
    controller::Controller,
    request::{Request, Requester, SerializedResponse},
};

impl Request for CreateGroupRequest {
    type Response = CreateGroupResponse;
    fn get_context(&self) -> Context {
        Context::UserOnly
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &super::controller::Controller,
    ) -> Result<Self::Response, ArunaError> {
        // Disallow impersonation
        if requester
            .as_ref()
            .and_then(|r| r.get_impersonator())
            .is_some()
        {
            return Err(ArunaError::Unauthorized);
        }
        let request_tx = CreateGroupRequestTx {
            id: Ulid::new(),
            req: self,
            requester: requester
                .ok_or_else(|| ArunaError::Unauthorized)
                .inspect_err(logerr!())?,
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateGroupRequestTx {
    id: Ulid,
    req: CreateGroupRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateGroupRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let group = Group {
            id: self.id,
            name: self.req.name.clone(),
            description: self.req.description.clone(),
            deleted: false,
        };
        let requester_id = self
            .requester
            .get_id()
            .ok_or_else(|| ArunaError::Forbidden("Unregistered".to_string()))?;

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            let Some(user_idx) = store.get_idx_from_ulid(&requester_id, wtxn.get_txn()) else {
                return Err(ArunaError::NotFound(requester_id.to_string()));
            };

            // Create group
            let group_idx = store.create_node(&mut wtxn, &group)?;

            // Add relation user --ADMIN--> group
            store.create_relation(
                &mut wtxn,
                user_idx,
                group_idx,
                relation_types::PERMISSION_ADMIN,
            )?;

            store.add_read_permission_universe(&mut wtxn, group_idx, &[group_idx])?;

            // Affected nodes: User and Group
            wtxn.commit(associated_event_id, &[user_idx, group_idx], &[])?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&CreateGroupResponse { group })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for GetGroupRequest {
    type Response = GetGroupResponse;
    fn get_context(&self) -> Context {
        // Do we need this?
        Context::Permission {
            min_permission: crate::models::models::Permission::Read,
            source: self.id,
        }
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &super::controller::Controller,
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
        let response = tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;

            let idx = store
                .get_idx_from_ulid(&self.id, &rtxn)
                .ok_or_else(|| return ArunaError::NotFound(self.id.to_string()))?;

            let group = store
                .get_node(&rtxn, idx)
                .ok_or_else(|| return ArunaError::NotFound(self.id.to_string()))?;

            rtxn.commit()?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(GetGroupResponse { group })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??;

        Ok(response)
    }
}

impl Request for AddUserRequest {
    type Response = AddUserResponse;
    fn get_context(&self) -> Context {
        Context::Permission {
            min_permission: crate::models::models::Permission::Admin,
            source: self.group_id,
        }
    }
    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &super::controller::Controller,
    ) -> Result<Self::Response, ArunaError> {
        // Disallow impersonation
        if requester
            .as_ref()
            .and_then(|r| r.get_impersonator())
            .is_some()
        {
            return Err(ArunaError::Unauthorized);
        }
        let request_tx = AddUserRequestTx {
            id: Ulid::new(),
            req: self,
            requester: requester
                .ok_or_else(|| ArunaError::Unauthorized)
                .inspect_err(logerr!())?,
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddUserRequestTx {
    id: Ulid,
    req: AddUserRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for AddUserRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let group_id = self.req.group_id;
        let user_id = self.req.user_id;
        let permission = match self.req.permission {
            crate::models::models::Permission::None => PERMISSION_NONE,
            crate::models::models::Permission::Read => PERMISSION_READ,
            crate::models::models::Permission::Append => PERMISSION_APPEND,
            crate::models::models::Permission::Write => PERMISSION_WRITE,
            crate::models::models::Permission::Admin => PERMISSION_ADMIN,
        };

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            // Get indices
            let Some(user_idx) = store.get_idx_from_ulid(&user_id, wtxn.get_txn()) else {
                return Err(ArunaError::NotFound(user_id.to_string()));
            };
            let Some(group_idx) = store.get_idx_from_ulid(&group_id, wtxn.get_txn()) else {
                return Err(ArunaError::NotFound(group_id.to_string()));
            };

            // Add relation user --PERMISSION--> group
            store.create_relation(&mut wtxn, user_idx, group_idx, permission)?;

            // Affected nodes: User and Group
            //store.register_event(&mut wtxn, associated_event_id, &[user_idx, group_idx])?;
            //store.add_event_to_subscribers(&mut wtxn, associated_event_id, &[user_idx])?;

            wtxn.commit(associated_event_id, &[user_idx, group_idx], &[])?;
            Ok::<_, ArunaError>(bincode::serialize(&AddUserResponse {})?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for GetUsersFromGroupRequest {
    type Response = GetUsersFromGroupResponse;
    fn get_context(&self) -> Context {
        Context::Permission {
            min_permission: crate::models::models::Permission::Read,
            source: self.group_id,
        }
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &super::controller::Controller,
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
        let response = tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;

            let idx = store
                .get_idx_from_ulid(&self.group_id, &rtxn)
                .ok_or_else(|| return ArunaError::NotFound(self.group_id.to_string()))?;

            let filter = (PERMISSION_NONE..=PERMISSION_ADMIN).collect::<Vec<u32>>();

            let mut users = Vec::new();
            for source in store
                .get_relations(idx, Some(&filter), petgraph::Direction::Incoming, &rtxn)?
                .into_iter()
                .map(|r| r.from_id)
            {
                let source_idx = store
                    .get_idx_from_ulid(&source, &rtxn)
                    .ok_or_else(|| return ArunaError::NotFound(source.to_string()))?;

                if let Some(user) = store.get_node(&rtxn, source_idx) {
                    users.push(user);
                } else {
                    tracing::error!("Idx not found in database");
                };
            }
            rtxn.commit()?;
            Ok::<_, ArunaError>(GetUsersFromGroupResponse { users })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??;

        Ok(response)
    }
}

impl Request for UserAccessGroupRequest {
    type Response = UserAccessGroupResponse;
    fn get_context(&self) -> Context {
        Context::UserOnly
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &super::controller::Controller,
    ) -> Result<Self::Response, ArunaError> {
        // Disallow impersonation
        if requester
            .as_ref()
            .and_then(|r| r.get_impersonator())
            .is_some()
        {
            return Err(ArunaError::Unauthorized);
        }
        let request_tx = UserAccessGroupTx {
            req: self,
            requester: requester
                .ok_or_else(|| ArunaError::Unauthorized)
                .inspect_err(logerr!())?,
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserAccessGroupTx {
    req: UserAccessGroupRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for UserAccessGroupTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let requester_id = self
            .requester
            .get_id()
            .ok_or_else(|| ArunaError::Forbidden("Unregistered".to_string()))?;

        let store = controller.get_store();
        let group_id = self.req.group_id;
        Ok(tokio::task::spawn_blocking(move || {
            let wtxn = store.write_txn()?;
            let ro_txn = wtxn.get_ro_txn();
            let graph = wtxn.get_ro_graph();

            let Some(group_idx) = store.get_idx_from_ulid(&group_id, ro_txn) else {
                return Err(ArunaError::NotFound(group_id.to_string()));
            };
            let Some(requester_idx) = store.get_idx_from_ulid(&requester_id, ro_txn) else {
                return Err(ArunaError::NotFound(requester_id.to_string()));
            };

            let mut affected = vec![group_idx];
            let filter = (PERMISSION_READ..=PERMISSION_ADMIN).collect::<Vec<u32>>();
            affected.extend(
                store
                    .get_raw_relations(group_idx, Some(&filter), Direction::Incoming, graph)
                    .iter()
                    .map(|rel| rel.source)
                    .collect::<Vec<u32>>(),
            );

            // Notification gets automatically created in commit
            wtxn.commit(associated_event_id, &affected, &[requester_idx])?;
            Ok::<_, ArunaError>(bincode::serialize(&UserAccessGroupResponse {})?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}
