use super::{
    structs::{Context, ContextVariant},
    token_handler::{Action, ArunaTokenClaims, OIDCError, ProcessedToken, TokenHandler},
};
use crate::{
    caching::cache::Cache,
    database::{dsls::user_dsl::OIDCMapping, enums::DbPermissionLevel},
};
use anyhow::anyhow;
use anyhow::Result;
use base64::{engine::general_purpose, Engine};
use diesel_ulid::DieselUlid;
use log::error;
use std::sync::Arc;

pub struct PermissionHandler {
    cache: Arc<Cache>,
    pub token_handler: Arc<TokenHandler>,
}

pub struct PermissionCheck {
    pub user_id: DieselUlid,
    pub token: Option<DieselUlid>,
    pub is_proxy: bool,
    pub proxy_id: Option<DieselUlid>,
}

impl PermissionHandler {
    pub fn new(cache: Arc<Cache>, token_handler: Arc<TokenHandler>) -> Self {
        Self {
            cache: cache.clone(),
            token_handler, //Arc::new(TokenHandler::new(cache, realm_info.to_string())),
        }
    }

    pub async fn check_permissions_verbose(
        &self,
        token: &str,
        ctxs: Vec<Context>,
    ) -> Result<PermissionCheck, tonic::Status> {
        // What are the cases?
        // 1. User Aruna token       --> (user_id, token_id)
        // 2. User OIDC token        --> (user_id, None)
        // 3. Endpoint signed token  --> (user_id, ?)
        // 4. Endpoint notifications --> (endpoint_id, None)
        // let (main_id, token, personal, permissions, is_proxy, proxy_intent) = tonic_auth!(
        //     self.token_handler.process_token(token).await,
        //     "Unauthorized"
        // );
        let ref _processed_token @ ProcessedToken {
            main_id,
            token,
            is_personal: personal,
            user_permissions: ref permissions,
            is_proxy,
            ref proxy_intent,
        } = match self.token_handler.process_token(token).await {
            Ok(results) => results,
            Err(err) => {
                error!("Error in auth: {err:?}");
                return match err.downcast_ref::<OIDCError>() {
                    Some(_) => Err(tonic::Status::unauthenticated("Not registered")),
                    None => Err(tonic::Status::unauthenticated("Unauthorized")),
                };
            }
        };

        // dbg!(&processed_token);
        // dbg!(&ctxs);

        // // Define rule
        // let program = Program::compile(
        //     " // If proxy context ...
        //     processed_token.is_proxy ?
        //         (
        //             // Check if has intent
        //             has(processed_token.proxy_intent) ?
        //                 (
        //                 // Check if intent == Action::Impersonate
        //                 processed_token.proxy_intent.endsWith(string(3)) ?
        //                     (
        //                         // Only Registered, SelfUser, GlobalProxy or Resource ctxs are
        //                         // allowed
        //                         ctxs.all(ctx,
        //                             (ctx.variant == 'Registered')
        //                                 || (ctx.variant == 'SelfUser')
        //                                 || (ctx.variant =='GlobalProxy')
        //                                 || has(ctx.variant.Resource)
        //                         ) ?
        //                             'CheckProxyContexts'
        //                         :   false
        //                     )
        //                 :   (
        //                         // Check if intent == Action::FetchInfo
        //                         processed_token.proxy_intent.endsWith(string(2)) ?
        //                             (
        //                                 ctxs.all(ctx,
        //                                     (ctx.variant == 'Registered')
        //                                     || (ctx.variant == 'SelfUser')
        //                                     || (ctx.variant =='GlobalProxy')
        //                                     || (has(ctx.variant.Resource) ?
        //                                             (ctx.variant.Resource[1] == 'READ')
        //                                         :   false )
        //                                     || (has(ctx.variant.User) ?
        //                                             (ctx.variant.User[1] == 'READ')
        //                                         :   false )) ?
        //                                     'CheckProxyContexts'
        //                                 :   false

        //                             )
        //                         :   (
        //                                 (
        //                                 ctx.all(ctx,
        //                                     (ctx.variant == 'NotActivated')
        //                                     || (ctx.variant == 'Registered')
        //                                     || (ctx.variant == 'GlobalProxy'))
        //                                 ) ?
        //                                     true
        //                                 :   (
        //                                     ctxs.exists(ctx, has(ctx.variant.Resource)) ?
        //                                         'CheckResourceProxyContexts'
        //                                     :   ctxs.exists(ctx, (
        //                                             has(ctx.variant.User) ?
        //                                                 (ctx.variant.User[1] == 'READ') : false)) ?
        //                                             'CheckUserProxyContexts'
        //                                         :   false
        //                                     )
        //                             )
        //                     )
        //                 )
        //             :   'Missing Intent'
        //         )
        //     :   'CheckUserContexts'",
        // )
        // .map_err(|e| {
        //     dbg!(&e);
        //     tonic::Status::unauthenticated("Policy error")
        // })?;
        // let mut policy_context = PolicyContext::default();

        // // External data
        // policy_context
        //     .add_variable(
        //         "processed_token",
        //         processed_token.clone(), //to_value(processed_token)
        //                                  //    .map_err(|_| tonic::Status::unauthenticated("Import error"))?,
        //     )
        //     .map_err(|_| tonic::Status::unauthenticated("Import error"))?;
        // policy_context
        //     .add_variable(
        //         "ctxs",
        //         ctxs.clone(),
        //         //to_value(ctxs.clone()).map_err(|e| {
        //         //    dbg!(&e);
        //         //    tonic::Status::unauthenticated("Import error")
        //         //})?,
        //     )
        //     .map_err(|e| {
        //         dbg!(&e);
        //         tonic::Status::unauthenticated("Import error")
        //     })?;

        // // Custom functions
        // let higher_than = |perm1: Arc<String>, perm2: Arc<String>| -> bool {
        //     let perm1 = match perm1.as_str() {
        //         "DENY" => 0,
        //         "NONE" => 1,
        //         "READ" => 2,
        //         "APPEND" => 3,
        //         "WRITE" => 4,
        //         "ADMIN" => 5,
        //         _ => 0,
        //     };
        //     let perm2 = match perm2.as_str() {
        //         "DENY" => 0,
        //         "NONE" => 1,
        //         "READ" => 2,
        //         "APPEND" => 3,
        //         "WRITE" => 4,
        //         "ADMIN" => 5,
        //         _ => 0,
        //     };
        //     perm1 > perm2
        // };
        // policy_context.add_function("higher_than", higher_than);

        // let ends_with =
        //     |This(s): This<Arc<String>>, end: Arc<String>| -> bool { s.ends_with(end.as_str()) };
        // policy_context.add_function("endsWith", ends_with);

        // // Execute query
        // let value = program.execute(&policy_context).map_err(|e| {
        //     dbg!(&e);
        //     tonic::Status::unauthenticated("Policy error")
        // })?;
        // dbg!(&value);

        // Individual permission checking if token is signed from Dataproxy
        if is_proxy {
            return if let Some(intent) = proxy_intent {
                if intent.action == Action::Impersonate {
                    //Case 1: Impersonate
                    //  - Check if provided contexts are proxy/self/resource only
                    for ctx in &ctxs {
                        match ctx.variant {
                            ContextVariant::SelfUser
                            | ContextVariant::Registered
                            | ContextVariant::GlobalProxy
                            | ContextVariant::Resource(_) => {}
                            _ => return Err(tonic::Status::invalid_argument(
                                "Only resource functionality allowed for Dataproxy signed tokens",
                            )),
                        }
                    }
                } else if intent.action == Action::FetchInfo {
                    //Case 2: FetchInfo
                    //  - Only get functions -> DbPermissionLevel::READ in contexts
                    for ctx in &ctxs {
                        match ctx.variant {
                            ContextVariant::SelfUser
                            | ContextVariant::Registered
                            | ContextVariant::GlobalProxy => {}
                            ContextVariant::Resource((_, perm))
                            | ContextVariant::User((_, perm)) => {
                                if perm > DbPermissionLevel::READ {
                                    return Err(tonic::Status::permission_denied(
                                        "Only get functions allowed",
                                    ));
                                }
                            }
                            _ => {
                                return Err(tonic::Status::permission_denied(
                                    "Only get functions allowed",
                                ))
                            }
                        }
                    }
                    //unimplemented!("Permission check for Dataproxy notification fetch not yet implemented")
                }

                if self.cache.check_proxy_ctxs(&intent.target, &ctxs) {
                    //dbg!(&intent.target);
                    //Ok((main_id, token, true, Some(intent.target)))
                    Ok(PermissionCheck {
                        user_id: main_id,
                        token,
                        is_proxy: true,
                        proxy_id: Some(intent.target),
                    })
                } else {
                    Err(tonic::Status::unauthenticated(
                        "Invalid proxy authentication",
                    ))
                }
            } else {
                Err(tonic::Status::internal("Missing intent action"))
            };
        }

        // Check permissions for standard ArunaServer user token
        if self
            .cache
            .check_permissions_with_contexts(&ctxs, permissions, personal, &main_id)
        {
            //Ok((main_id, token, false, None))
            Ok(PermissionCheck {
                user_id: main_id,
                token,
                is_proxy,
                proxy_id: None,
            })
        } else {
            Err(tonic::Status::unauthenticated("Invalid permissions"))
        }
    }

    ///ToDo: Rust Doc
    pub async fn check_permissions(
        &self,
        token: &str,
        ctxs: Vec<Context>,
    ) -> Result<DieselUlid, tonic::Status> {
        let PermissionCheck { user_id, .. } = self.check_permissions_verbose(token, ctxs).await?;
        Ok(user_id)
    }

    pub async fn check_unregistered_oidc(&self, token: &str) -> Result<OIDCMapping> {
        let split = token
            .split('.')
            .nth(1)
            .ok_or_else(|| anyhow!("Invalid token"))?;
        let decoded = general_purpose::STANDARD_NO_PAD.decode(split)?;
        let claims: ArunaTokenClaims = serde_json::from_slice(&decoded)?;

        let issuer = self
            .cache
            .get_issuer(&claims.iss)
            .ok_or_else(|| anyhow!("Unknown issuer"))?;

        let (_, validated_claims) = issuer.check_token(token).await?;

        let mapping = OIDCMapping {
            external_id: validated_claims.sub,
            oidc_name: issuer.issuer_name.to_string(),
        };

        if self.cache.oidc_mapping_exists(&mapping) {
            return Err(anyhow!("User already registered"));
        }
        Ok(mapping)
    }
}
