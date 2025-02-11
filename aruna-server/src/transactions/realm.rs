use super::{
    controller::Controller,
    request::{Request, Requester, WriteRequest},
};
use crate::{
    constants::relation_types::{
        self, DEFAULT, GROUP_ADMINISTRATES_REALM, GROUP_PART_OF_REALM, OWNED_BY_USER,
        PERMISSION_READ, REALM_USES_COMPONENT, SHARES_PERMISSION,
    },
    context::Context,
    error::ArunaError,
    logerr,
    models::{
        models::{Component, Group, MilliIdx, NodeVariant, Realm},
        requests::{
            AddComponentToRealmRequest, AddComponentToRealmResponse, AddGroupRequest,
            AddGroupResponse, CreateRealmRequest, CreateRealmResponse, GetGroupsFromRealmRequest,
            GetGroupsFromRealmResponse, GetRealmComponentsRequest, GetRealmComponentsResponse,
            GetRealmRequest, GetRealmResponse, GroupAccessRealmRequest, GroupAccessRealmResponse,
        },
    },
    storage::txns::Txn,
    transactions::request::SerializedResponse,
};
use petgraph::Direction::{self, Outgoing};
use serde::{Deserialize, Serialize};
use tracing::error;
use ulid::Ulid;

impl Request for CreateRealmRequest {
    type Response = CreateRealmResponse;
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
        let request_tx = CreateRealmRequestTx {
            id: Ulid::new(),
            generated_group: Group {
                id: Ulid::new(),
                name: format!("{}-admin-group", self.tag),
                description: format!("Auto-generated admin group for: {}", self.name),
                deleted: false,
            },
            req: self,
            requester: requester.ok_or_else(|| ArunaError::Unauthorized)?,
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateRealmRequestTx {
    id: Ulid,
    generated_group: Group,
    req: CreateRealmRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateRealmRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let realm = Realm {
            id: self.id,
            tag: self.req.tag.clone(),
            name: self.req.name.clone(),
            description: self.req.description.clone(),
            deleted: false,
        };

        let group = self.generated_group.clone();
        let requester_id = self
            .requester
            .get_id()
            .ok_or_else(|| ArunaError::Forbidden("Unregistered".to_string()))?;

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            // Create realm, add user to realm

            let mut wtxn = store.write_txn()?;

            // Exemplary check via the universe filter
            // TODO: escape tag filter
            if !store
                .filtered_universe(
                    Some(&format!("tag='{}' AND variant=6", realm.tag.clone())),
                    &wtxn,
                )?
                .is_empty()
            {
                drop(wtxn);
                return Err(ArunaError::ConflictParameter {
                    name: "tag".to_string(),
                    error: "Realm tag not unique".to_string(),
                });
            };

            let Some(user_idx) = store.get_idx_from_ulid(&requester_id, &wtxn) else {
                return Err(ArunaError::NotFound(requester_id.to_string()));
            };

            // Create realm
            let realm_idx = store.create_node(&mut wtxn, &realm)?;
            // Create group
            let group_idx = store.create_node(&mut wtxn, &group)?;

            // Add relation user --ADMIN--> group
            store.create_relation(
                &mut wtxn,
                user_idx,
                group_idx,
                relation_types::PERMISSION_ADMIN,
            )?;

            // Add relation group --ADMINISTRATES--> realm
            store.create_relation(
                &mut wtxn,
                group_idx,
                realm_idx,
                relation_types::GROUP_ADMINISTRATES_REALM,
            )?;

            store.add_read_permission_universe(
                &mut wtxn,
                group_idx.0,
                &[realm_idx.0, group_idx.0],
            )?;
            store.add_read_permission_universe(
                &mut wtxn,
                realm_idx.0,
                &[realm_idx.0, group_idx.0],
            )?;

            // Affected nodes: User, Realm and Group

            wtxn.commit(associated_event_id, &[user_idx, realm_idx, group_idx], &[])?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&CreateRealmResponse {
                realm,
                admin_group_id: group.id,
            })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for AddGroupRequest {
    type Response = AddGroupResponse;
    fn get_context(&self) -> Context {
        Context::Permission {
            min_permission: crate::models::models::Permission::Admin,
            source: self.realm_id,
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
        let request_tx = AddGroupRequestTx {
            req: self,
            requester: requester.ok_or_else(|| ArunaError::Unauthorized)?,
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddGroupRequestTx {
    req: AddGroupRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for AddGroupRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;
        let group_id = self.req.group_id;
        let realm_id = self.req.realm_id;
        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            // Create realm, add user to realm

            let mut wtxn = store.write_txn()?;

            let Some(group_idx) = store.get_idx_from_ulid(&group_id, &wtxn) else {
                return Err(ArunaError::NotFound(group_id.to_string()));
            };

            let Some(realm_idx) = store.get_idx_from_ulid(&realm_id, &wtxn) else {
                return Err(ArunaError::NotFound(realm_id.to_string()));
            };

            // Add relation group --GROUP_PART_OF_REALM--> realm
            store.create_relation(
                &mut wtxn,
                group_idx,
                realm_idx,
                relation_types::GROUP_PART_OF_REALM,
            )?;

            store.add_read_permission_universe(&mut wtxn, realm_idx.0, &[group_idx.0])?;

            // Affected nodes: Realm and Group
            wtxn.commit(associated_event_id, &[realm_idx, group_idx], &[])?;

            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&AddGroupResponse {})?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for GetRealmRequest {
    type Response = GetRealmResponse;
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
        let store = controller.get_store();
        let response = tokio::task::spawn_blocking(move || {
            // Create realm, add user to realm
            let rtxn = store.read_txn()?;

            let Some(realm_idx) = store.get_idx_from_ulid(&self.id, &rtxn) else {
                return Err(ArunaError::NotFound(self.id.to_string()));
            };

            let realm = store
                .get_node::<Realm>(&rtxn, realm_idx)
                .ok_or_else(|| ArunaError::NotFound(self.id.to_string()))?;

            rtxn.commit()?;

            Ok::<_, ArunaError>(GetRealmResponse { realm })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??;

        Ok(response)
    }
}

impl Request for GetGroupsFromRealmRequest {
    type Response = GetGroupsFromRealmResponse;
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
        let store = controller.get_store();
        let response = tokio::task::spawn_blocking(move || {
            // Create realm, add user to realm
            let rtxn = store.read_txn()?;

            let Some(realm_idx) = store.get_idx_from_ulid(&self.realm_id, &rtxn) else {
                return Err(ArunaError::NotFound(self.realm_id.to_string()));
            };

            let mut groups = Vec::new();
            for source in store
                .get_relations(
                    realm_idx,
                    Some(&[GROUP_PART_OF_REALM]),
                    Direction::Incoming,
                    &rtxn,
                )?
                .into_iter()
                .map(|r| r.from_id)
            {
                let source_idx = store
                    .get_idx_from_ulid(&source, &rtxn)
                    .ok_or_else(|| return ArunaError::NotFound(source.to_string()))?;

                if let Some(user) = store.get_node(&rtxn, source_idx) {
                    groups.push(user);
                } else {
                    tracing::error!("Idx not found in database");
                };
            }
            rtxn.commit()?;

            Ok::<_, ArunaError>(GetGroupsFromRealmResponse { groups })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??;

        Ok(response)
    }
}

impl Request for GetRealmComponentsRequest {
    type Response = GetRealmComponentsResponse;
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
        let realm_id = self.realm_id;
        tokio::task::spawn_blocking(move || {
            let read_txn = store.read_txn()?;
            let Some(realm_idx) = store.get_idx_from_ulid(&realm_id, &read_txn) else {
                return Err(ArunaError::NotFound("Realm not found".to_string()));
            };
            let component_relations = store.get_relations(
                realm_idx,
                Some(&[REALM_USES_COMPONENT]),
                Direction::Outgoing,
                &read_txn,
            )?;

            let mut components = Vec::new();
            for component in component_relations {
                let Some(component_idx) = store.get_idx_from_ulid(&component.to_id, &read_txn)
                else {
                    tracing::error!("Database error");
                    return Err(ArunaError::DatabaseError(
                        "Idx not matched by database".to_string(),
                    ));
                };
                let component = store
                    .get_node::<Component>(&read_txn, component_idx)
                    .expect("Database error: Store/Graph idx mismatch");
                components.push(component);
            }

            read_txn.commit()?;

            Ok::<GetRealmComponentsResponse, ArunaError>(GetRealmComponentsResponse { components })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })?
    }
}

impl Request for AddComponentToRealmRequest {
    type Response = AddComponentToRealmResponse;
    fn get_context(&self) -> Context {
        Context::Permission {
            min_permission: crate::models::models::Permission::Admin,
            source: self.realm_id,
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
        let request_tx = AddComponentToRealmRequestTx {
            req: self,
            requester: requester.ok_or_else(|| ArunaError::Unauthorized)?,
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddComponentToRealmRequestTx {
    req: AddComponentToRealmRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for AddComponentToRealmRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let component_id = self.req.component_id;
        let realm_id = self.req.realm_id;
        let requester_id = self.requester.get_id().ok_or_else(|| {
            error!("Requester not found");
            ArunaError::Unauthorized
        })?;
        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            // Create realm, add user to realm

            let mut wtxn = store.write_txn()?;

            let component_idx = store.get_idx_from_ulid_validate(
                &component_id,
                "component_id",
                &[NodeVariant::Component],
                &wtxn,
            )?;

            let user_idx = store.get_idx_from_ulid_validate(
                &requester_id,
                "user",
                &[NodeVariant::User, NodeVariant::ServiceAccount],
                &wtxn,
            )?;

            let realm_idx = store.get_idx_from_ulid_validate(
                &realm_id,
                "realm_id",
                &[NodeVariant::Realm],
                &wtxn,
            )?;

            let component = store
                .get_node::<Component>(&wtxn, component_idx)
                .ok_or_else(|| ArunaError::NotFound(component_id.to_string()))?;

            if !component.public
                && !wtxn
                    .get_ro_graph()
                    .has_relation(component_idx, user_idx, &[OWNED_BY_USER])?
            {
                error!("User does not own component");
                return Err(ArunaError::Unauthorized);
            };

            // Add relation group --GROUP_PART_OF_REALM--> realm
            store.create_relation(
                &mut wtxn,
                realm_idx,
                component_idx,
                relation_types::REALM_USES_COMPONENT,
            )?;

            if !wtxn
                .get_ro_graph()
                .get_relations(realm_idx, Some(&[DEFAULT]), Outgoing)?
                .iter()
                .any(|r| wtxn.get_ro_graph().node_weight(r.target) == Some(NodeVariant::Component))
            {
                store.create_relation(&mut wtxn, realm_idx, component_idx, DEFAULT)?;
                // TODO: Update all projects + resources to
            }

            store.add_read_permission_universe(&mut wtxn, realm_idx.0, &[component_idx.0])?;

            // Affected nodes: Realm and Group
            wtxn.commit(associated_event_id, &[realm_idx, component_idx], &[])?;

            Ok::<_, ArunaError>(bincode::serialize(&AddComponentToRealmResponse {})?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for GroupAccessRealmRequest {
    type Response = GroupAccessRealmResponse;
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
        let request_tx = GroupAccessRealmTx {
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
pub struct GroupAccessRealmTx {
    req: GroupAccessRealmRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for GroupAccessRealmTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let store = controller.get_store();
        let Some(requester) = self.requester.get_id() else {
            return Err(ArunaError::Unauthorized);
        };
        let realm_id = self.req.realm_id;
        let group_id = self.req.group_id;

        Ok(tokio::task::spawn_blocking(move || {
            let wtxn = store.write_txn()?;
            // let ro_txn = wtxn.get_ro_txn();
            // let graph = wtxn.get_ro_graph();

            let Some(group_idx) = store.get_idx_from_ulid(&group_id, &wtxn) else {
                return Err(ArunaError::NotFound(group_id.to_string()));
            };
            let Some(realm_idx) = store.get_idx_from_ulid(&realm_id, &wtxn) else {
                return Err(ArunaError::NotFound(realm_id.to_string()));
            };
            let Some(requester_idx) = store.get_idx_from_ulid(&requester, &wtxn) else {
                return Err(ArunaError::NotFound(requester.to_string()));
            };

            let mut affected = vec![group_idx, realm_idx];
            let filter = (PERMISSION_READ..=SHARES_PERMISSION).collect::<Vec<u32>>();
            let relations = store
                .get_raw_relations(
                    realm_idx,
                    Some(&[GROUP_ADMINISTRATES_REALM]),
                    Direction::Incoming,
                )?
                .iter()
                .map(|rel| rel.source)
                .collect::<Vec<MilliIdx>>();
            for admin_group in relations {
                let users = &store
                    .get_raw_relations(admin_group, Some(&filter), Direction::Incoming)?
                    .iter()
                    .map(|rel| rel.source)
                    .collect::<Vec<MilliIdx>>();
                affected.extend(users);
            }
            // Notification gets automatically created in commit
            wtxn.commit(associated_event_id, &affected, &[requester_idx])?;
            Ok::<_, ArunaError>(bincode::serialize(&GroupAccessRealmResponse {})?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}
