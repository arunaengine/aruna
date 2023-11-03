use crate::database::{
    dsls::{
        license_dsl::ALL_RIGHTS_RESERVED,
        object_dsl::Object,
        user_dsl::{User, UserAttributes},
        workspaces_dsl::WorkspaceTemplate,
        Empty,
    },
    enums::ObjectMapping,
};
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::storage::services::v2::{
    CreateWorkspaceRequest, CreateWorkspaceTemplateRequest,
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use std::str::FromStr;

pub struct CreateTemplate(pub CreateWorkspaceTemplateRequest);
pub struct CreateWorkspace(pub CreateWorkspaceRequest);

impl CreateTemplate {
    pub fn get_template(&self, owner: DieselUlid) -> Result<WorkspaceTemplate> {
        let workspace = WorkspaceTemplate {
            id: DieselUlid::generate(),
            name: self.0.name.clone(),
            description: self.0.description.clone(),
            owner,
            prefix: self.0.prefix.clone(),
            hook_ids: Json(
                self.0
                    .hook_ids
                    .iter()
                    .map(|id| DieselUlid::from_str(id).map_err(|_| anyhow!("Invalid id")))
                    .collect::<Result<Vec<DieselUlid>>>()?,
            ),
            endpoint_ids: Json(
                self.0
                    .endpoint_id
                    .iter()
                    .map(|id| DieselUlid::from_str(id).map_err(|_| anyhow!("Invalid id")))
                    .collect::<Result<Vec<DieselUlid>>>()?,
            ),
        };
        Ok(workspace)
    }
}

impl CreateWorkspace {
    pub fn get_name(&self) -> String {
        self.0.workspace_template.clone()
    }

    pub fn make_project(template: WorkspaceTemplate, endpoints: Vec<DieselUlid>) -> Object {
        let id = DieselUlid::generate();
        let endpoints = Json(DashMap::from_iter(endpoints.iter().map(|id| (*id, true))));
        Object {
            id,
            revision_number: 0,
            name: [template.prefix, id.to_string()].join("-"),
            description: template.description,
            created_at: None,
            created_by: template.owner,
            content_len: 0,
            count: 0,
            key_values: Json(crate::database::dsls::object_dsl::KeyValues(Vec::new())),
            object_status: crate::database::enums::ObjectStatus::AVAILABLE,
            data_class: crate::database::enums::DataClass::WORKSPACE,
            object_type: crate::database::enums::ObjectType::PROJECT,
            external_relations: Json(crate::database::dsls::object_dsl::ExternalRelations(
                DashMap::default(),
            )),
            hashes: Json(crate::database::dsls::object_dsl::Hashes(Vec::new())),
            dynamic: true,
            endpoints,
            metadata_license: ALL_RIGHTS_RESERVED.to_string(),
            data_license: ALL_RIGHTS_RESERVED.to_string(),
        }
    }

    pub fn create_service_account(endpoints: Vec<DieselUlid>, workspace_id: DieselUlid) -> User {
        let user_id = DieselUlid::generate();
        let endpoints = endpoints.into_iter().map(|id| (id, Empty {}));
        User {
            id: user_id,
            display_name: ["SERVICE_ACCOUNT".to_string(), user_id.to_string()].join("#"),
            external_id: None,
            email: String::new(),
            attributes: Json(UserAttributes {
                global_admin: false,
                service_account: true,
                tokens: DashMap::default(),
                trusted_endpoints: dashmap::DashMap::from_iter(endpoints),
                custom_attributes: vec![],
                permissions: DashMap::from_iter(vec![(
                    workspace_id,
                    ObjectMapping::PROJECT(crate::database::enums::DbPermissionLevel::APPEND),
                )]),
            }),
            active: true,
        }
    }
}