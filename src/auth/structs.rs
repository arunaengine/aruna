use crate::database::{
    dsls::user_dsl::User,
    enums::{DbPermissionLevel, ObjectMapping},
};
use anyhow::{bail, Result};
use diesel_ulid::DieselUlid;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum ContextVariant {
    Activated,
    ResourceContext((DieselUlid, DbPermissionLevel)),
    User(DieselUlid),
    GlobalAdmin,
}

pub struct Context {
    pub variant: ContextVariant,
    pub allow_service_account: bool,
    pub is_self: bool,
}

impl Context {
    pub fn res_ctx(id: DieselUlid, level: DbPermissionLevel, allow_sa: bool) -> Self {
        Self {
            variant: ContextVariant::ResourceContext((id, level)),
            allow_service_account: allow_sa,
            is_self: false,
        }
    }

    pub fn user_ctx(id: DieselUlid) -> Self {
        Self {
            variant: ContextVariant::User(id),
            allow_service_account: false,
            is_self: false,
        }
    }

    pub fn admin() -> Self {
        Self {
            variant: ContextVariant::GlobalAdmin,
            allow_service_account: false,
            is_self: false,
        }
    }

    pub fn self_ctx() -> Self {
        Self {
            variant: ContextVariant::User(DieselUlid::default()),
            allow_service_account: false,
            is_self: true,
        }
    }
}

impl Default for Context {
    fn default() -> Self {
        Self {
            variant: ContextVariant::Activated,
            allow_service_account: true,
            is_self: false,
        }
    }
}

impl User {
    pub fn get_permissions(
        &self,
        token: Option<DieselUlid>,
    ) -> Result<Vec<(DieselUlid, DbPermissionLevel)>> {
        if let Some(token) = token {
            if let Some(token) = self.attributes.0.tokens.get(&token) {
                // Check if token is mapped to an object
                let object_id = if let Some(mapping) = token.object_id {
                    match mapping {
                        ObjectMapping::PROJECT(id)
                        | ObjectMapping::COLLECTION(id)
                        | ObjectMapping::DATASET(id)
                        | ObjectMapping::OBJECT(id) => id,
                    }
                } else {
                    return Err(anyhow::anyhow!(
                        "Personal token without resource permission"
                    ));
                };

                Ok(vec![(object_id, token.user_rights)])
            } else {
                bail!("Token not found")
            }
        } else {
            Ok(self
                .attributes
                .0
                .permissions
                .iter()
                .map(|(resource_id, object_mapping)| {
                    (
                        *resource_id,
                        match object_mapping {
                            ObjectMapping::PROJECT(perm)
                            | ObjectMapping::COLLECTION(perm)
                            | ObjectMapping::DATASET(perm)
                            | ObjectMapping::OBJECT(perm) => *perm,
                        },
                    )
                })
                .collect())
        }
    }
}
