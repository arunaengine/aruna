use super::{
    auth::TokenHandler,
    controller::Controller,
    request::{Request, Requester, WriteRequest},
};
use crate::{
    constants::relation_types::{
        GROUP_ADMINISTRATES_REALM, GROUP_PART_OF_REALM, PERMISSION_ADMIN, PERMISSION_NONE,
        REALM_USES_COMPONENT,
    },
    context::Context,
    error::ArunaError,
    logerr,
    models::{
        models::{
            Group, NodeVariant, Permission, S3Credential, Subscriber, Token, TokenType, User,
        },
        requests::{
            CreateS3CredentialsRequest, CreateS3CredentialsResponse, CreateTokenRequest,
            CreateTokenResponse, GetGroupsFromUserRequest, GetGroupsFromUserResponse,
            GetRealmsFromUserRequest, GetRealmsFromUserResponse, GetS3CredentialsRequest,
            GetS3CredentialsResponse, GetTokensRequest, GetTokensResponse, GetUserRequest,
            GetUserResponse, RegisterUserRequest, RegisterUserResponse,
        },
    },
    storage::txns::Txn,
    transactions::request::SerializedResponse,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

impl Request for RegisterUserRequest {
    type Response = RegisterUserResponse;
    fn get_context(&self) -> Context {
        Context::Public
    }

    #[tracing::instrument(level = "trace", skip(controller))]
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
        let request_tx = RegisterUserRequestTx {
            id: Ulid::new(),
            req: self,
            requester: requester.ok_or_else(|| {
                tracing::error!("Missing requester");
                ArunaError::Unauthorized
            })?,
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterUserRequestTx {
    id: Ulid,
    req: RegisterUserRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for RegisterUserRequestTx {
    #[tracing::instrument(level = "trace", skip(self, controller))]
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        tracing::trace!("Executing RegisterUserRequestTx");
        controller.authorize(&self.requester, &self.req).await?;

        let user = User {
            id: self.id,
            first_name: self.req.first_name.clone(),
            last_name: self.req.last_name.clone(),
            email: self.req.email.clone(),
            deleted: false,
            // TODO: Multiple identifiers ?
            identifiers: self.req.identifier.clone(),
            global_admin: false,
        };

        let oidc_mapping = match &self.requester {
            Requester::Unregistered {
                oidc_realm,
                oidc_subject,
            } => (oidc_subject.clone(), oidc_realm.clone()),
            _ => {
                return Err(ArunaError::Unauthorized);
            }
        };

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            // Create user
            let user_idx = store.create_node(&mut wtxn, &user)?;
            store.add_oidc_mapping(&mut wtxn, user_idx, oidc_mapping)?;

            store.add_subscriber(
                &mut wtxn,
                Subscriber {
                    id: user.id,
                    owner: user.id,
                    target_idx: user_idx.0,
                    cascade: false,
                },
            )?;

            // Affected nodes: User
            wtxn.commit(associated_event_id, &[user_idx], &[])?;

            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&RegisterUserResponse { user })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for CreateTokenRequest {
    type Response = CreateTokenResponse;
    fn get_context(&self) -> Context {
        Context::Public
    }

    async fn run_request(
        mut self,
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
        if self.expires_at.is_none() {
            self.expires_at = Some(chrono::Utc::now() + chrono::Duration::days(365));
        }

        let request_tx = CreateTokenRequestTx {
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
pub struct CreateTokenRequestTx {
    req: CreateTokenRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateTokenRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;
        let user_id = self
            .requester
            .get_id()
            .ok_or_else(|| ArunaError::Forbidden("Unregistered".to_string()))?;

        let token = Token {
            id: 0, // This id is created in the store
            user_id,
            name: self.req.name.clone(),
            expires_at: self.req.expires_at.expect("Got generated in Request"),
            scope: self.req.scope.clone(),
            constraints: None, // TODO: Constraints
            token_type: TokenType::Aruna,
            component_id: None,
            default_group: self.req.group_id.clone(),
            default_realm: self.req.realm_id.clone(),
        };

        let is_service_account = matches!(self.requester, Requester::ServiceAccount { .. });

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            // Create token
            let user_idx = store
                .get_idx_from_ulid(&user_id, &wtxn)
                .ok_or_else(|| ArunaError::NotFound("User not found".to_string()))?;
            let token = store.add_token(
                &mut wtxn,
                associated_event_id,
                &token.user_id.clone(),
                token,
            )?;
            // Add event to user

            wtxn.commit(associated_event_id, &[user_idx], &[])?;

            let secret = TokenHandler::sign_user_token(
                &store,
                is_service_account,
                token.id,
                &token.user_id,
                None,
                Some(token.expires_at.timestamp() as u64),
            )?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&CreateTokenResponse { token, secret })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for GetUserRequest {
    type Response = GetUserResponse;

    fn get_context<'a>(&'a self) -> Context {
        Context::NotRegistered
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
        let requester_ulid = requester
            .ok_or_else(|| {
                tracing::error!("Missing requester");
                ArunaError::Unauthorized
            })?
            .get_id()
            .ok_or_else(|| {
                tracing::error!("Missing requester id");
                ArunaError::NotFound("User not reqistered".to_string())
            })?;

        let store = controller.get_store();
        tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;

            let idx = store
                .get_idx_from_ulid(&requester_ulid, &rtxn)
                .ok_or_else(|| ArunaError::NotFound("Requester not found".to_string()))?;

            Ok::<GetUserResponse, ArunaError>(GetUserResponse {
                user: store
                    .get_node::<User>(&rtxn, idx)
                    .ok_or_else(|| ArunaError::NotFound("User not found".to_string()))?,
            })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })?
    }
}

impl Request for GetGroupsFromUserRequest {
    type Response = GetGroupsFromUserResponse;

    fn get_context<'a>(&'a self) -> Context {
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
        let requester_id = requester
            .ok_or_else(|| ArunaError::Unauthorized)
            .inspect_err(logerr!())?
            .get_id()
            .ok_or_else(|| ArunaError::NotFound("User not reqistered".to_string()))
            .inspect_err(logerr!())?;

        let store = controller.get_store();
        tokio::task::spawn_blocking(move || {
            // TODO: Optimize!
            let mut groups = Vec::new();
            let filter = (PERMISSION_NONE..=PERMISSION_ADMIN).collect::<Vec<u32>>();

            let rtxn = store.read_txn()?;
            let user_idx = store
                .get_idx_from_ulid(&requester_id, &rtxn)
                .ok_or_else(|| ArunaError::NotFound("Requester not found".to_string()))?;

            let relations = store.get_relations(
                user_idx,
                Some(&filter),
                petgraph::Direction::Outgoing,
                &rtxn,
            )?;
            for relation in &relations {
                let target = relation.to_id;

                let node_idx = store
                    .get_idx_from_ulid(&target, &rtxn)
                    .ok_or_else(|| ArunaError::NotFound("Requester not found".to_string()))?;
                let raw_node = store
                    .get_raw_node(&rtxn, node_idx)
                    .expect("Idx exist but no node in documents -> corrupted database");

                let variant: NodeVariant = serde_json::from_slice::<u8>(
                    raw_node
                        .get(1)
                        .expect("Missing variant -> corrupted database"),
                )
                .inspect_err(logerr!())?
                .try_into()
                .inspect_err(logerr!())?;
                if matches!(variant, NodeVariant::Group) {
                    let group = Group::try_from(raw_node)?;

                    let perm = match relation.relation_type.as_str() {
                        "PermissionNone" => Permission::None,
                        "PermissionAppend" => Permission::Append,
                        "PermissionRead" => Permission::Read,
                        "PermissionWrite" => Permission::Write,
                        "PermissionAdmin" => Permission::Admin,
                        _ => {
                            // Should not happen, because filter is set for all permission
                            // relations
                            continue;
                        }
                    };
                    groups.push((group, perm));
                }
            }
            Ok::<GetGroupsFromUserResponse, ArunaError>(GetGroupsFromUserResponse { groups })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })?
    }
}

impl Request for GetRealmsFromUserRequest {
    type Response = GetRealmsFromUserResponse;

    fn get_context<'a>(&'a self) -> Context {
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

        let requester_id = requester
            .ok_or_else(|| ArunaError::Unauthorized)
            .inspect_err(logerr!())?
            .get_id()
            .ok_or_else(|| ArunaError::NotFound("User not reqistered".to_string()))
            .inspect_err(logerr!())?;

        let store = controller.get_store();
        tokio::task::spawn_blocking(move || {
            let read_txn = store.read_txn()?;
            let realms = store.get_realms_for_user(&read_txn, requester_id)?;
            Ok::<GetRealmsFromUserResponse, ArunaError>(GetRealmsFromUserResponse { realms })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })?
    }
}

impl Request for GetTokensRequest {
    type Response = GetTokensResponse;

    fn get_context<'a>(&'a self) -> Context {
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
        let requester_ulid = requester
            .ok_or_else(|| {
                tracing::error!("Missing requester");
                ArunaError::Unauthorized
            })?
            .get_id()
            .ok_or_else(|| {
                tracing::error!("Missing requester id");
                ArunaError::NotFound("User not reqistered".to_string())
            })?;

        let store = controller.get_store();
        tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;

            let tokens = store.get_tokens(&rtxn, &requester_ulid)?;

            let tokens = tokens
                .into_iter()
                .filter_map(|token| {
                    let token = token?;
                    if token.token_type == TokenType::Aruna {
                        Some(token)
                    } else {
                        None
                    }
                })
                .collect();

            Ok::<GetTokensResponse, ArunaError>(GetTokensResponse { tokens })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })?
    }
}

impl Request for CreateS3CredentialsRequest {
    type Response = CreateS3CredentialsResponse;
    fn get_context(&self) -> Context {
        Context::UserOnly
    }

    async fn run_request(
        mut self,
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
        if self.expires_at.is_none() {
            self.expires_at = Some(chrono::Utc::now() + chrono::Duration::days(365));
        }

        let request_tx = CreateS3CredentialsRequestTx {
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
pub struct CreateS3CredentialsRequestTx {
    req: CreateS3CredentialsRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateS3CredentialsRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;
        let user_id = self
            .requester
            .get_id()
            .ok_or_else(|| ArunaError::Forbidden("Unregistered".to_string()))?;

        let token = Token {
            id: 0, // This id is created in the store
            user_id,
            name: self.req.name.clone(),
            expires_at: self.req.expires_at.expect("Got generated in Request"),
            scope: self.req.scope.clone(),
            constraints: None, // TODO: Constraints
            token_type: TokenType::S3,
            component_id: Some(self.req.component_id.clone()),
            default_group: Some(self.req.group_id.clone()),
            default_realm: Some(self.req.realm_id.clone()),
        };

        let component_id = self.req.component_id.clone();
        let realm_id = self.req.realm_id.clone();
        let group_id = self.req.group_id.clone();

        let _is_service_account = matches!(self.requester, Requester::ServiceAccount { .. });

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            // Create token

            let user_idx = store.get_idx_from_ulid_validate(
                &user_id,
                "user_id",
                &[NodeVariant::User, NodeVariant::ServiceAccount],
                &wtxn,
            )?;

            let component_idx = store.get_idx_from_ulid_validate(
                &component_id,
                "component_id",
                &[NodeVariant::Component],
                &wtxn,
            )?;

            let realm_idx = store.get_idx_from_ulid_validate(
                &realm_id,
                "realm_id",
                &[NodeVariant::Realm],
                &wtxn,
            )?;

            let group_idx = store.get_idx_from_ulid_validate(
                &group_id,
                "group_id",
                &[NodeVariant::Group],
                &wtxn,
            )?;
            if !wtxn.get_ro_graph().has_relation(
                user_idx,
                group_idx,
                &((PERMISSION_NONE..=PERMISSION_ADMIN).collect::<Vec<u32>>()),
            )? {
                return Err(ArunaError::Forbidden("User not in group".to_string()));
            }

            if !wtxn.get_ro_graph().has_relation(
                realm_idx,
                component_idx,
                &[REALM_USES_COMPONENT],
            )? {
                return Err(ArunaError::Forbidden("Component not in realm".to_string()));
            }

            if !wtxn.get_ro_graph().has_relation(
                group_idx,
                realm_idx,
                &[GROUP_PART_OF_REALM, GROUP_ADMINISTRATES_REALM],
            )? {
                return Err(ArunaError::Forbidden("Group not part of realm".to_string()));
            }

            let token = store.add_token(
                &mut wtxn,
                associated_event_id,
                &token.user_id.clone(),
                token,
            )?;
            // Add event to user

            let (access_key, secret_key) =
                TokenHandler::sign_s3_credentials(&store, &token.user_id, token.id, &component_id)?;
            wtxn.commit(associated_event_id, &[user_idx], &[])?;

            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&CreateS3CredentialsResponse {
                token,
                component_id,
                access_key,
                secret_key,
            })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for GetS3CredentialsRequest {
    type Response = GetS3CredentialsResponse;

    fn get_context<'a>(&'a self) -> Context {
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
        let requester_ulid = requester
            .ok_or_else(|| {
                tracing::error!("Missing requester");
                ArunaError::Unauthorized
            })?
            .get_id()
            .ok_or_else(|| {
                tracing::error!("Missing requester id");
                ArunaError::NotFound("User not reqistered".to_string())
            })?;

        let store = controller.get_store();
        tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;

            let tokens = store.get_tokens(&rtxn, &requester_ulid)?;

            let tokens = tokens
                .into_iter()
                .filter_map(|token| {
                    let token = token?;
                    if token.token_type == TokenType::S3 {
                        Some(S3Credential {
                            access_key: format!("{}.{}", &requester_ulid, token.id),
                            token_info: token,
                        })
                    } else {
                        None
                    }
                })
                .collect();

            Ok::<GetS3CredentialsResponse, ArunaError>(GetS3CredentialsResponse { tokens })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })?
    }
}
