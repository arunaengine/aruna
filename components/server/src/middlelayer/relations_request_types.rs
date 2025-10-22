use crate::auth::structs::Context;
use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_VERSION,
};
use crate::database::dsls::object_dsl::{ExternalRelation, ObjectWithRelations};
use crate::database::enums::DbPermissionLevel;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::models::v2::{relation, Relation};
use aruna_rust_api::api::storage::services::v2::ModifyRelationsRequest;
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use tokio_postgres::Client;

pub struct ModifyRelations(pub ModifyRelationsRequest);

#[derive(Debug)]
pub struct RelationsToModify {
    pub relations_to_add: RelationsToAdd,
    pub relations_to_remove: RelationsToRemove,
    pub resources_to_check: Vec<Context>,
}

#[derive(Debug)]
pub struct RelationsToAdd {
    pub external: Vec<ExternalRelation>,
    pub internal: Vec<InternalRelation>,
}

#[derive(Debug)]
pub struct RelationsToRemove {
    pub external: Vec<ExternalRelation>,
    pub internal: Vec<InternalRelation>,
}
impl ModifyRelations {
    pub fn get_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.resource_id)?)
    }
    pub async fn get_relations(
        &self,
        resource: ObjectWithRelations,
        transaction_client: &Client,
    ) -> Result<RelationsToModify> {
        let resource_id = resource.object.id;

        let mut ulids_to_check: Vec<Context> = vec![Context::res_ctx(
            resource_id,
            DbPermissionLevel::WRITE,
            true,
        )];

        let (external_add_relations, internal_add_relations, mut added_to_check) =
            ModifyRelations::convert_relations(
                &self.0.add_relations,
                resource_id,
                transaction_client,
                DbPermissionLevel::APPEND,
            )
            .await?;
        let (external_rm_relations, temp_rm_int_relations, mut removed_to_check) =
            ModifyRelations::convert_relations(
                &self.0.remove_relations,
                resource_id,
                transaction_client,
                DbPermissionLevel::WRITE,
            )
            .await?;
        if !temp_rm_int_relations
            .iter()
            .filter(|ir| ir.relation_name == INTERNAL_RELATION_VARIANT_VERSION)
            .collect::<Vec<&InternalRelation>>()
            .is_empty()
        {
            return Err(anyhow!("Cannot remove version relations"));
        }

        let mut existing = Vec::from_iter(resource.outbound.0.into_iter().map(|r| r.1));
        existing.append(&mut Vec::from_iter(
            resource.outbound_belongs_to.0.into_iter().map(|r| r.1),
        ));
        existing.append(&mut Vec::from_iter(
            resource.inbound.0.into_iter().map(|r| r.1),
        ));
        existing.append(&mut Vec::from_iter(
            resource.inbound_belongs_to.0.into_iter().map(|r| r.1),
        ));
        let internal_rm_relations =
            ModifyRelations::check_exists(&existing, &temp_rm_int_relations);
        ulids_to_check.append(&mut added_to_check);
        ulids_to_check.append(&mut removed_to_check);
        Ok(RelationsToModify {
            relations_to_add: RelationsToAdd {
                external: external_add_relations,
                internal: internal_add_relations,
            },
            relations_to_remove: RelationsToRemove {
                external: external_rm_relations,
                internal: internal_rm_relations,
            },
            resources_to_check: ulids_to_check,
        })
    }
    async fn convert_relations(
        api_relations: &Vec<Relation>,
        resource_id: DieselUlid,
        transaction_client: &Client,
        permission_level: DbPermissionLevel,
    ) -> Result<(Vec<ExternalRelation>, Vec<InternalRelation>, Vec<Context>)> {
        let mut external_relations: Vec<ExternalRelation> = Vec::new();
        let mut internal_relations: Vec<InternalRelation> = Vec::new();
        let mut resources_to_check: Vec<Context> = Vec::new();
        for relation in api_relations {
            if let Some(rel) = &relation.relation {
                match rel {
                    relation::Relation::External(external) => {
                        external_relations.push(external.try_into()?)
                    }
                    relation::Relation::Internal(internal) => {
                        resources_to_check.push(Context::res_ctx(
                            DieselUlid::from_str(&internal.resource_id)?,
                            permission_level,
                            true,
                        ));
                        internal_relations
                            // Try into generates a new ULID, so rm via ID does not work
                            .push(
                                InternalRelation::from_api(
                                    internal,
                                    resource_id,
                                    transaction_client,
                                )
                                .await?,
                            );
                    }
                }
            }
        }
        Ok((external_relations, internal_relations, resources_to_check))
    }
    fn check_exists(
        existing: &Vec<InternalRelation>,
        to_remove: &Vec<InternalRelation>,
    ) -> Vec<InternalRelation> {
        let mut new = Vec::new();
        for e in existing {
            for rm in to_remove {
                if e.relation_name == rm.relation_name
                    && e.origin_pid == rm.origin_pid
                    && e.target_pid == rm.target_pid
                {
                    new.push(e.clone());
                }
            }
        }
        new
    }
}
