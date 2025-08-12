use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::enums::{ObjectMapping, ReplicationStatus, ReplicationType};
use crate::database::{
    crud::{CrudDb, PrimaryKey},
    enums::{DataClass, ObjectStatus, ObjectType},
};
use crate::utils::database_utils::create_multi_query;
use ahash::RandomState;
use anyhow::Result;
use anyhow::{anyhow, bail};
use chrono::NaiveDateTime;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use lazy_static::lazy_static;
use postgres_from_row::FromRow;
use postgres_types::{FromSql, Json, ToSql};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;
use tokio_postgres::Client;

lazy_static! {
    pub static ref MAX_RETRIES: u64 = dotenvy::var("MAX_RETRIES")
        .map(|var| var.parse::<u64>().unwrap_or(10))
        .unwrap_or(10);
    pub static ref RETRY_TIMEOUT: u64 = dotenvy::var("RETRY_TIMEOUT")
        .map(|var| var.parse::<u64>().unwrap_or(2))
        .unwrap_or(2);
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd)]
#[allow(non_camel_case_types)]
pub enum KeyValueVariant {
    HOOK,
    LABEL,
    STATIC_LABEL,
    HOOK_STATUS,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
    pub variant: KeyValueVariant,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct KeyValues(pub Vec<KeyValue>);

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq, PartialOrd)]
pub enum DefinedVariant {
    URL,
    IDENTIFIER,
    CUSTOM,
}

#[derive(Serialize, Deserialize, Hash, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct ExternalRelation {
    pub identifier: String,
    pub defined_variant: DefinedVariant,
    pub custom_variant: Option<String>,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExternalRelations(pub DashMap<String, ExternalRelation, RandomState>);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Hashes(pub Vec<Hash>);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Hash {
    pub alg: Algorithm,
    pub hash: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Algorithm {
    MD5,
    SHA256,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct EndpointInfo {
    pub replication: ReplicationType,
    pub status: Option<ReplicationStatus>, // Only Some() for ObjectType::Object
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Author {
    pub first_name: String,
    pub last_name: String,
    pub email: Option<String>,
    pub orcid: Option<String>,
    pub user_id: Option<DieselUlid>,
}

#[derive(FromRow, FromSql, Debug, Clone, ToSql)]
pub struct Object {
    pub id: DieselUlid,
    pub revision_number: i32,
    pub name: String,
    pub title: String,
    pub description: String,
    pub created_at: Option<NaiveDateTime>,
    pub created_by: DieselUlid,
    pub authors: Json<Vec<Author>>,
    pub content_len: i64,
    pub count: i64,
    pub key_values: Json<KeyValues>,
    pub object_status: ObjectStatus,
    pub data_class: DataClass,
    pub object_type: ObjectType,
    pub external_relations: Json<ExternalRelations>,
    pub hashes: Json<Hashes>,
    pub dynamic: bool,
    pub endpoints: Json<DashMap<DieselUlid, EndpointInfo, RandomState>>, // <Endpoint_id, EndpointStatus>
    pub metadata_license: String,
    pub data_license: String,
}

#[derive(FromRow, Debug, FromSql, Clone)]
pub struct ObjectWithRelations {
    #[from_row(flatten)]
    pub object: Object,
    pub inbound: Json<DashMap<DieselUlid, InternalRelation, RandomState>>,
    pub inbound_belongs_to: Json<DashMap<DieselUlid, InternalRelation, RandomState>>,
    pub outbound: Json<DashMap<DieselUlid, InternalRelation, RandomState>>,
    pub outbound_belongs_to: Json<DashMap<DieselUlid, InternalRelation, RandomState>>,
}
#[async_trait::async_trait]
impl CrudDb for Object {
    async fn create(&mut self, client: &Client) -> Result<()> {
        let query = "INSERT INTO objects (id, revision_number, title, name, description, created_by, authors, content_len, count, key_values, object_status, data_class, object_type, external_relations, hashes, dynamic, endpoints, metadata_license, data_license ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
        ) RETURNING *;";

        let prepared = client.prepare(query).await?;

        let row = client
            .query_one(
                &prepared,
                &[
                    &self.id,
                    &self.revision_number,
                    &self.title,
                    &self.name,
                    &self.description,
                    &self.created_by,
                    &self.authors,
                    &self.content_len,
                    &self.count,
                    &self.key_values,
                    &self.object_status,
                    &self.data_class,
                    &self.object_type,
                    &self.external_relations,
                    &self.hashes,
                    &self.dynamic,
                    &self.endpoints,
                    &self.metadata_license,
                    &self.data_license,
                ],
            )
            .await?;

        *self = Object::from_row(&row);
        Ok(())
    }

    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM objects WHERE id = $1";
        let prepared = client.prepare(query).await?;
        let mut backoff_counter: u64 = 0;

        while backoff_counter <= *MAX_RETRIES {
            match client.query_opt(&prepared, &[&&id]).await {
                Ok(query) => return Ok(query.map(|e| Object::from_row(&e))),
                Err(err) => {
                    if let Some(sql_error) = err.code() {
                        match sql_error.code() {
                            // => 40001 Error from yugabyte matches to 25P02
                            "25P02" => {
                                backoff_counter += 1;
                                tokio::time::sleep(Duration::from_millis(
                                    RETRY_TIMEOUT.pow(backoff_counter as u32),
                                ))
                                .await;
                            }
                            _code => {
                                return Err(anyhow!(err));
                            }
                        }
                    } else {
                        return Err(anyhow!(err));
                    }
                }
            }
        }
        Err(anyhow!("TODO"))
    }

    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM objects";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows.iter().map(Object::from_row).collect::<Vec<_>>())
    }

    async fn delete(&self, client: &Client) -> Result<()> {
        let query = "UPDATE objects 
            SET object_status = 'DELETED'
            WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&self.id]).await?;
        Ok(())
    }
}

impl Object {
    //ToDo: Docs
    pub async fn add_key_value(id: &DieselUlid, client: &Client, kv: KeyValue) -> Result<()> {
        let query = "UPDATE objects
        SET key_values = key_values || $1::jsonb
        WHERE id = $2;";

        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&Json(kv), id]).await?;
        Ok(())
    }

    //ToDo: Docs
    pub async fn remove_key_value(&self, client: &Client, kv: KeyValue) -> Result<()> {
        let element: i32 = self
            .key_values
            .0
             .0
            .iter()
            .position(|e| *e == kv)
            .ok_or_else(|| anyhow!("Unable to find key_value"))? as i32;

        let query = "UPDATE objects
        SET key_values = key_values - $1::INTEGER
        WHERE id = $2;";

        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&element, &self.id]).await?;
        Ok(())
    }

    //ToDo: Docs
    pub async fn add_external_relations(
        id: &DieselUlid,
        client: &Client,
        rel: Vec<ExternalRelation>,
    ) -> Result<()> {
        let query_one = "UPDATE objects 
            SET external_relations = external_relations || $1::jsonb 
            WHERE id = $2";
        let dash_map: DashMap<String, ExternalRelation, RandomState> =
            DashMap::from_iter(rel.into_iter().map(|r| (r.identifier.clone(), r)));
        let query_two = Json(ExternalRelations(dash_map));
        let prepared = client.prepare(query_one).await?;
        client.execute(&prepared, &[&query_two, &id]).await?;
        Ok(())
    }

    //ToDo: Docs
    pub async fn remove_external_relation(
        id: &DieselUlid,
        client: &Client,
        rel: Vec<ExternalRelation>,
    ) -> Result<()> {
        let keys: Vec<String> = rel.into_iter().map(|e| e.identifier).collect();
        let query = "UPDATE objects 
            SET external_relations = external_relations - $1::text[] 
            WHERE id = $2;";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&keys, id]).await?;
        Ok(())
    }

    //ToDo: Docs
    pub async fn add_endpoint(
        client: &Client,
        object_id: &DieselUlid,
        endpoint_id: &DieselUlid,
        endpoint_info: EndpointInfo,
    ) -> Result<Object> {
        let query = "UPDATE objects 
            SET endpoints = endpoints || $1::jsonb 
            WHERE id = $2
            RETURNING *;";

        let insert: DashMap<DieselUlid, EndpointInfo, RandomState> =
            DashMap::from_iter([(*endpoint_id, endpoint_info)]);
        let prepared = client.prepare(query).await?;
        let row = client
            .query_one(&prepared, &[&Json(insert), object_id])
            .await?;

        Ok(Object::from_row(&row))
    }

    //ToDo: Docs
    pub async fn remove_endpoint(
        client: &Client,
        object_id: &DieselUlid,
        endpoint_id: &DieselUlid,
    ) -> Result<Object> {
        let query = "UPDATE objects 
            SET endpoints = endpoints::jsonb #- $1::text[]
            WHERE id = $2 
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client
            .query_one(&prepared, &[&vec![endpoint_id.to_string()], object_id])
            .await?;

        Ok(Object::from_row(&row))
    }

    //ToDo: Docs
    pub async fn remove_endpoint_from_objects(
        client: &Client,
        endpoint_id: &DieselUlid,
    ) -> Result<Vec<Object>> {
        let query = "UPDATE objects 
            SET endpoints = endpoints::jsonb #- $1::text[]
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let rows = client
            .query(&prepared, &[&vec![endpoint_id.to_string()]])
            .await?;

        let mut users = vec![];
        for row in rows {
            users.push(Object::try_from_row(&row)?)
        }

        Ok(users)
    }

    //ToDo: Docs
    pub async fn finish_object_staging(
        id: &DieselUlid,
        client: &Client,
        hashes: Option<Hashes>,
        content_len: i64,
        object_status: ObjectStatus,
    ) -> Result<()> {
        match hashes {
            Some(h) => {
                let query_some = "UPDATE objects 
            SET hashes = $1, content_len = $2, object_status = $3
            WHERE id = $4;";
                let prepared = client.prepare(query_some).await?;
                client
                    .execute(&prepared, &[&Json(h), &content_len, &object_status, id])
                    .await?
            }
            None => {
                let query_none = "UPDATE objects 
            SET content_len = $1, object_status = $2
            WHERE id = $3;";
                let prepared = client.prepare(query_none).await?;
                client
                    .execute(&prepared, &[&content_len, &object_status, id])
                    .await?
            }
        };
        Ok(())
    }

    pub async fn fetch_recursive_objects(id: &DieselUlid, client: &Client) -> Result<Vec<Object>> {
        let query = "/*+ indexscan(ir) set(yb_bnl_batch_size 1024) */ 
        WITH RECURSIVE paths AS (
            SELECT ir.*
              FROM internal_relations ir WHERE ir.origin_pid = $1 
            UNION 
            SELECT ir2.*
              FROM paths, internal_relations ir2 WHERE ir2.origin_pid = paths.target_pid 
        )
        SELECT objects.* FROM paths
        LEFT JOIN objects ON objects.id = paths.target_pid;";
        let prepared = client.prepare(query).await?;
        let objects = client
            .query(&prepared, &[&id])
            .await?
            .iter()
            .map(Object::from_row)
            .collect::<Vec<Object>>();

        Ok(objects)
    }

    ///ToDo: Rust Doc
    pub async fn fetch_subresources(&self, client: &Client) -> Result<Vec<DieselUlid>> {
        // Return the obvious case before unnecessary query
        if self.object_type == ObjectType::OBJECT {
            return Ok(vec![]);
        }

        let query = "/*+ indexscan(ir) set(yb_bnl_batch_size 1024) */ 
        WITH RECURSIVE paths AS (
            SELECT ir.*
              FROM internal_relations ir WHERE ir.origin_pid = $1 
            UNION 
            SELECT ir2.*
              FROM paths, internal_relations ir2 WHERE ir2.origin_pid = paths.target_pid 
        ) SELECT DISTINCT(paths.target_pid) FROM paths;";

        // Execute query and convert rows to InternalRelations
        let prepared = client.prepare(query).await?;
        let subresource_ids: Vec<DieselUlid> = client
            .query(&prepared, &[&self.id])
            .await?
            .iter()
            .map(|row| {
                let id: DieselUlid = row.get(0);
                id
                //DieselUlid::from_str(id)
            })
            .collect();

        Ok(subresource_ids)
    }

    // ToDo: Rust Doc
    pub async fn fetch_subresources_by_id(
        resource_id: &DieselUlid,
        client: &Client,
    ) -> Result<Vec<DieselUlid>> {
        let query = "/*+ indexscan(ir) set(yb_bnl_batch_size 1024) */ 
        WITH RECURSIVE paths AS (
            SELECT ir.*
              FROM internal_relations ir WHERE ir.origin_pid = $1 
            UNION 
            SELECT ir2.*
              FROM paths, internal_relations ir2 WHERE ir2.origin_pid = paths.target_pid 
        ) SELECT DISTINCT(paths.target_pid) FROM paths;";

        // Execute query and convert rows to InternalRelations
        let prepared = client.prepare(query).await?;
        let subresource_ids: Vec<DieselUlid> = client
            .query(&prepared, &[&resource_id])
            .await?
            .iter()
            .map(|row| {
                let id: DieselUlid = row.get(0);
                id
                //DieselUlid::from_str(id)
            })
            .collect();

        Ok(subresource_ids)
    }

    // ToDo: Rust Doc
    pub async fn fetch_parents_by_id(
        resource_id: &DieselUlid,
        client: &Client,
    ) -> Result<Vec<DieselUlid>> {
        let query = "/*+ indexscan(ir) set(yb_bnl_batch_size 1024) */ 
        WITH RECURSIVE paths AS (
            SELECT ir.*
              FROM internal_relations ir WHERE ir.target_pid = $1 
            UNION 
            SELECT ir2.*
              FROM paths, internal_relations ir2 WHERE ir2.target_pid = paths.origin_pid 
        ) SELECT DISTINCT(paths.origin_pid) FROM paths;";

        // Execute query and convert rows to InternalRelations
        let prepared = client.prepare(query).await?;
        let subresource_ids: Vec<DieselUlid> = client
            .query(&prepared, &[&resource_id])
            .await?
            .iter()
            .map(|row| {
                let id: DieselUlid = row.get(0);
                id
                //DieselUlid::from_str(id)
            })
            .collect();

        Ok(subresource_ids)
    }
    // ToDo: Rust Doc
    pub async fn fetch_object_hierarchies(&self, client: &Client) -> Result<Vec<Hierarchy>> {
        // Return the obvious case before unnecessary query
        if self.object_type == ObjectType::PROJECT {
            return Ok(vec![Hierarchy {
                project_id: self.id.to_string(),
                collection_id: None,
                dataset_id: None,
                object_id: None,
            }]);
        }

        let query = "/*+ indexscan(ir) set(yb_bnl_batch_size 1024) */ 
        WITH RECURSIVE paths AS (
            SELECT ir.*
              FROM internal_relations ir WHERE ir.target_pid = $1 AND ir.relation_name = 'BELONGS_TO'
            UNION 
            SELECT ir2.*
              FROM paths, internal_relations ir2 
                WHERE ir2.target_pid = paths.origin_pid AND ir2.relation_name = 'BELONGS_TO'
        ) SELECT * FROM paths;";

        // Execute query and convert rows to InternalRelations
        let prepared = client.prepare(query).await?;
        let relations = client
            .query(&prepared, &[&self.id])
            .await?
            .iter()
            .map(InternalRelation::from_row)
            .collect::<Vec<_>>();

        // Extract paths from list of internal relations
        extract_paths_from_graph(relations)
    }

    /// Warning:
    ///   This function produces an empty vector for root Objects i.e. ObjectType::PROJECT.
    pub async fn fetch_object_hierarchies_by_id(
        object_id: &DieselUlid,
        client: &Client,
    ) -> Result<Vec<Hierarchy>> {
        let query = "/*+ indexscan(ir) set(yb_bnl_batch_size 1024) */ 
        WITH RECURSIVE paths AS (
            SELECT ir.*
              FROM internal_relations ir WHERE ir.target_pid = $1 AND ir.relation_name = 'BELONGS_TO'
            UNION 
            SELECT ir2.*
              FROM paths, internal_relations ir2 WHERE ir2.target_pid = paths.origin_pid AND ir2.relation_name = 'BELONGS_TO'
        ) SELECT * FROM paths;";

        // Execute query and convert rows to InternalRelations
        let prepared = client.prepare(query).await?;
        let relations = client
            .query(&prepared, &[&object_id])
            .await?
            .iter()
            .map(InternalRelation::from_row)
            .collect::<Vec<_>>();

        // Extract paths from list of internal relations
        extract_paths_from_graph(relations)
    }

    //ToDo: Docs
    pub async fn get_object_with_relations(
        id: &DieselUlid,
        client: &Client,
    ) -> Result<ObjectWithRelations> {
        let query = "SELECT o.*,
        COALESCE(JSON_OBJECT_AGG(ir1.id, ir1.*) FILTER (WHERE ir1.target_pid = o.id AND NOT ir1.relation_name = 'BELONGS_TO'), '{}') inbound,
        COALESCE(JSON_OBJECT_AGG(ir1.origin_pid, ir1.*) FILTER (WHERE ir1.target_pid = o.id AND ir1.relation_name = 'BELONGS_TO'), '{}') inbound_belongs_to,
        COALESCE(JSON_OBJECT_AGG(ir1.id, ir1.*) FILTER (WHERE ir1.origin_pid = o.id AND NOT ir1.relation_name = 'BELONGS_TO'), '{}') outbound,
        COALESCE(JSON_OBJECT_AGG(ir1.target_pid, ir1.*) FILTER (WHERE ir1.origin_pid = o.id AND ir1.relation_name = 'BELONGS_TO'), '{}') outbound_belongs_to
        FROM objects o
        LEFT OUTER JOIN internal_relations ir1 ON o.id IN (ir1.target_pid, ir1.origin_pid)
        WHERE o.id = $1
        GROUP BY o.id;";
        let prepared = client.prepare(query).await?;
        let mut backoff_counter = 0;
        while backoff_counter <= *MAX_RETRIES {
            match client.query_one(&prepared, &[&id]).await {
                Ok(query) => return Ok(ObjectWithRelations::from_row(&query)),
                Err(err) => {
                    if let Some(sql_error) = err.code() {
                        match sql_error.code() {
                            "25P02" => {
                                backoff_counter += 1;
                                tokio::time::sleep(Duration::from_millis(
                                    RETRY_TIMEOUT.pow(backoff_counter as u32),
                                ))
                                .await;
                            }
                            _code => {
                                return Err(anyhow!(err));
                            }
                        }
                    } else {
                        return Err(anyhow!(err));
                    }
                }
            }
        }
        Err(anyhow!("TODO"))
    }

    //ToDo: Docs
    pub async fn get_objects_with_relations(
        ids: &Vec<DieselUlid>,
        client: &Client,
    ) -> Result<Vec<ObjectWithRelations>> {
        // Fast return if no ids are provided
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let query_one = "SELECT o.*,
        COALESCE(JSON_OBJECT_AGG(ir1.id, ir1.*) FILTER (WHERE ir1.target_pid = o.id AND NOT ir1.relation_name = 'BELONGS_TO'), '{}') inbound,
        COALESCE(JSON_OBJECT_AGG(ir1.origin_pid, ir1.*) FILTER (WHERE ir1.target_pid = o.id AND ir1.relation_name = 'BELONGS_TO'), '{}') inbound_belongs_to,
        COALESCE(JSON_OBJECT_AGG(ir1.id, ir1.*) FILTER (WHERE ir1.origin_pid = o.id AND NOT ir1.relation_name = 'BELONGS_TO'), '{}') outbound,
        COALESCE(JSON_OBJECT_AGG(ir1.target_pid, ir1.*) FILTER (WHERE ir1.origin_pid = o.id AND ir1.relation_name = 'BELONGS_TO'), '{}') outbound_belongs_to
        FROM objects o
        LEFT OUTER JOIN internal_relations ir1 ON o.id IN (ir1.target_pid, ir1.origin_pid)
        WHERE o.id IN ";
        let query_three = " GROUP BY o.id;";
        let mut inserts = Vec::<&(dyn ToSql + Sync)>::new();
        for id in ids {
            inserts.push(id);
        }
        let query_two = create_multi_query(&inserts);
        let query = format!("{query_one}{query_two}{query_three}");
        let prepared = client.prepare(&query).await?;

        let mut backoff_counter = 0;
        while backoff_counter <= *MAX_RETRIES {
            match client.query(&prepared, &inserts).await {
                Ok(query) => return Ok(query.iter().map(ObjectWithRelations::from_row).collect()),
                Err(err) => {
                    if let Some(sql_error) = err.code() {
                        match sql_error.code() {
                            "25P02" => {
                                backoff_counter += 1;
                                tokio::time::sleep(Duration::from_millis(
                                    RETRY_TIMEOUT.pow(backoff_counter as u32),
                                ))
                                .await;
                            }
                            _code => {
                                return Err(anyhow!(err));
                            }
                        }
                    } else {
                        return Err(anyhow!(err));
                    }
                }
            }
        }
        Err(anyhow!("TODO"))
    }

    pub async fn update_title(id: &DieselUlid, title: String, client: &Client) -> Result<()> {
        let query = "UPDATE objects
        SET title = $2
        WHERE id = $1;";

        let prepared = client.prepare(query).await?;

        client.query(&prepared, &[id, &title]).await?;
        Ok(())
    }

    pub async fn update_authors(&self, client: &Client) -> Result<()> {
        let query = "UPDATE objects
        SET authors = $2
        WHERE id = $1;";

        let prepared = client.prepare(query).await?;

        client.query(&prepared, &[&self.id, &self.authors]).await?;
        Ok(())
    }
    pub async fn update(&self, client: &Client) -> Result<()> {
        let query = "UPDATE objects 
        SET description = $2, key_values = $3, data_class = $4
        WHERE id = $1;";

        let prepared = client.prepare(query).await?;

        client
            .query(
                &prepared,
                &[
                    &self.id,
                    &self.description,
                    &self.key_values,
                    &self.data_class,
                ],
            )
            .await?;
        Ok(())
    }

    //ToDo: Docs
    pub async fn batch_claim(
        user_id: &DieselUlid,
        objects: &Vec<DieselUlid>,
        client: &Client,
    ) -> Result<()> {
        let query = "UPDATE objects 
            SET data_class = ('PRIVATE'), created_by = $1 
            WHERE id = ANY($2::uuid[]);";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[user_id, objects]).await?;
        Ok(())
    }

    //ToDo: Docs
    pub async fn update_name(id: DieselUlid, name: String, client: &Client) -> Result<Object> {
        let query = "UPDATE objects 
        SET name = $2
        WHERE id = $1
        RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client.query_one(&prepared, &[&id, &name]).await?;

        Ok(Object::from_row(&row))
    }

    //ToDo: Docs
    pub async fn update_description(
        id: DieselUlid,
        description: String,
        client: &Client,
    ) -> Result<Object> {
        let query = "UPDATE objects 
        SET description = $2
        WHERE id = $1
        RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client.query_one(&prepared, &[&id, &description]).await?;

        Ok(Object::from_row(&row))
    }

    //ToDo: Docs
    pub async fn update_licenses(
        id: DieselUlid,
        data_license: String,
        metadata_license: String,
        client: &Client,
    ) -> Result<Object> {
        let query = "UPDATE objects 
        SET metadata_license = $2, data_license = $3
        WHERE id = $1 
        RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client
            .query_one(&prepared, &[&id, &metadata_license, &data_license])
            .await?;

        Ok(Object::from_row(&row))
    }

    //ToDo: Docs
    pub async fn update_dataclass(
        id: DieselUlid,
        dataclass: DataClass,
        client: &Client,
    ) -> Result<Object> {
        let query = "UPDATE objects 
        SET data_class = $2
        WHERE id = $1 
        RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client.query_one(&prepared, &[&id, &dataclass]).await?;

        Ok(Object::from_row(&row))
    }

    //ToDo: Docs
    pub async fn set_deleted(ids: &Vec<DieselUlid>, client: &Client) -> Result<()> {
        let query_one = "UPDATE objects 
            SET object_status = 'DELETED'
            WHERE id IN ";
        let mut inserts = Vec::<&(dyn ToSql + Sync)>::new();
        for id in ids {
            inserts.push(id);
        }
        let query_two = create_multi_query(&inserts);
        let query = format!("{query_one}{query_two};");
        let prepared = client.prepare(&query).await?;
        client.execute(&prepared, &inserts).await?;
        Ok(())
    }

    //ToDo: Docs
    pub fn get_cloned_persistent(&self, new_id: DieselUlid) -> Self {
        let object = self.clone();
        Object {
            id: new_id,
            revision_number: object.revision_number,
            name: object.name,
            title: object.title,
            description: object.description,
            created_at: object.created_at,
            created_by: object.created_by,
            authors: object.authors,
            content_len: object.content_len,
            count: object.count,
            key_values: object.key_values,
            object_status: object.object_status,
            data_class: object.data_class,
            object_type: object.object_type,
            external_relations: object.external_relations,
            hashes: object.hashes,
            dynamic: false,
            endpoints: object.endpoints,
            metadata_license: object.metadata_license,
            data_license: object.data_license,
        }
    }

    //ToDo: Docs
    pub async fn archive(ids: &Vec<DieselUlid>, client: &Client) -> Result<Vec<Object>> {
        let query_one = " WITH o AS 
            (UPDATE objects 
            SET dynamic=false 
            WHERE objects.id IN ";
        //$id
        let query_three =  " RETURNING *)
        SELECT o.*,
            COALESCE(JSON_OBJECT_AGG(ir1.id, ir1.*) FILTER (WHERE ir1.target_pid = o.id AND NOT ir1.relation_name = 'BELONGS_TO'), '{}') inbound,
            COALESCE(JSON_OBJECT_AGG(ir1.origin_pid, ir1.*) FILTER (WHERE ir1.target_pid = o.id AND ir1.relation_name = 'BELONGS_TO'), '{}') inbound_belongs_to,
            COALESCE(JSON_OBJECT_AGG(ir1.id, ir1.*) FILTER (WHERE ir1.origin_pid = o.id AND NOT ir1.relation_name = 'BELONGS_TO'), '{}') outbound,
            COALESCE(JSON_OBJECT_AGG(ir1.target_pid, ir1.*) FILTER (WHERE ir1.origin_pid = o.id AND ir1.relation_name = 'BELONGS_TO'), '{}') outbound_belongs_to
            FROM o
            LEFT OUTER JOIN internal_relations ir1 ON o.id IN (ir1.target_pid, ir1.origin_pid)
            WHERE o.id IN ";
        let query_five = " GROUP BY o.id, o.revision_number, o.name, o.title, o.description, o.created_at,
        o.created_by, o.authors, o.content_len, o.count, o.key_values, o.object_status, o.data_class, o.object_type, 
        o.external_relations, o.hashes, o.dynamic, o.endpoints, o.metadata_license, o.data_license;";
        let mut inserts = Vec::<&(dyn ToSql + Sync)>::new();
        for id in ids {
            inserts.push(id);
        }
        let query_insert = create_multi_query(&inserts);
        let query = format!("{query_one}{query_insert}{query_three}{query_insert}{query_five}");
        let prepared = client.prepare(&query).await?;
        let result: Vec<Object> = client
            .query(&prepared, &inserts)
            .await?
            .iter()
            .map(Object::from_row)
            .collect();

        Ok(result)
    }

    //ToDo: Docs
    pub async fn get_objects(ids: &Vec<DieselUlid>, client: &Client) -> Result<Vec<Object>> {
        // Fast return if no ids are provided
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let query_one = "SELECT * FROM objects WHERE objects.id IN ";
        let mut inserts = Vec::<&(dyn ToSql + Sync)>::new();
        for id in ids {
            inserts.push(id);
        }
        let query_insert = create_multi_query(&inserts);
        let query = format!("{query_one}{query_insert};");
        let prepared = client.prepare(&query).await?;
        Ok(client
            .query(&prepared, &inserts)
            .await?
            .iter()
            .map(Object::from_row)
            .collect())
    }

    //ToDo: Docs
    pub async fn batch_create(objects: &[Object], client: &Client) -> Result<()> {
        // This is ugly but may solve our batch_create problems
        let query = "INSERT INTO objects
        (id, revision_number, name, title, description, created_by, authors, content_len, count, key_values, object_status, data_class, object_type, external_relations, hashes, dynamic, endpoints, metadata_license, data_license)
        VALUES";
        let mut query_list = String::new();
        let mut object_list = Vec::<&(dyn ToSql + Sync)>::new();
        for (idx, object) in objects.iter().enumerate() {
            let mut object_row= format!("(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                                          1+(19*idx), 2+(19*idx), 3+(19*idx), 4+(19*idx), 5+(19*idx), 6+(19*idx), 7+(19*idx),
                                          8+(19*idx), 9+(19*idx), 10+(19*idx), 11+(19*idx), 12+(19*idx), 13+(19*idx),
                                          14+(19*idx), 15+(19*idx), 16+(19*idx), 17+(19*idx), 18+(19*idx), 19+(19*idx)
            );
            if idx == objects.len() - 1 {
                object_row.push(';');
            } else {
                object_row.push(',');
            }
            query_list.push_str(&object_row);
            object_list.append(&mut vec![
                &object.id,
                &object.revision_number,
                &object.name,
                &object.title,
                &object.description,
                &object.created_by,
                &object.authors,
                &object.content_len,
                &object.count,
                &object.key_values,
                &object.object_status,
                &object.data_class,
                &object.object_type,
                &object.external_relations,
                &object.hashes,
                &object.dynamic,
                &object.endpoints,
                &object.metadata_license,
                &object.data_license,
            ]);
        }
        let query = [query, &query_list].join("");
        let prepared = client.prepare(query.as_str()).await?;
        client.execute(&prepared, &object_list).await?;
        /*
        This randomly fails in our tests but is the more performant option suggested by tokio_postgres.
        Potentially needs to be debugged, replacing the 'ugly' solution, to address performance concerns
        in the long run

        let query = "COPY objects \
        (id, revision_number, name, title, description, created_by, authors, content_len, count, key_values, object_status, data_class, object_type, external_relations, hashes, dynamic, endpoints, metadata_license, data_license)
        FROM STDIN BINARY";
        let sink: CopyInSink<_> = client.copy_in(query).await?;
        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::UUID,
                Type::INT4,
                Type::VARCHAR,
                Type::VARCHAR,
                Type::VARCHAR,
                Type::UUID,
                Type::JSONB,
                Type::INT8,
                Type::INT8,
                Type::JSONB,
                ObjectStatus::get_type(),
                DataClass::get_type(),
                ObjectType::get_type(),
                Type::JSONB,
                Type::JSONB,
                Type::BOOL,
                Type::JSONB,
                Type::VARCHAR,
                Type::VARCHAR,
            ],
        );
        pin_mut!(writer);
        for object in objects {
            writer
                .as_mut()
                .write(&[
                    &object.id,
                    &object.revision_number,
                    &object.name,
                    &object.title,
                    &object.description,
                    &object.created_by,
                    &object.authors,
                    &object.content_len,
                    &object.count,
                    &object.key_values,
                    &object.object_status,
                    &object.data_class,
                    &object.object_type,
                    &object.external_relations,
                    &object.hashes,
                    &object.dynamic,
                    &object.endpoints,
                    &object.metadata_license,
                    &object.data_license,
                ])
                .await?;
        }
        writer.finish().await?;
        */
        Ok(())
    }

    //ToDo: Docs
    pub async fn check_existing_projects(
        name: String,
        client: &Client,
    ) -> Result<Option<ObjectWithRelations>> {
        let query = "WITH o AS ( 
            SELECT * FROM objects
            WHERE name = $1 AND object_type = 'PROJECT'
        )
        SELECT o.*,
            COALESCE(JSON_OBJECT_AGG(ir1.id, ir1.*) FILTER (WHERE ir1.target_pid = o.id AND NOT ir1.relation_name = 'BELONGS_TO'), '{}') inbound,
            COALESCE(JSON_OBJECT_AGG(ir1.origin_pid, ir1.*) FILTER (WHERE ir1.target_pid = o.id AND ir1.relation_name = 'BELONGS_TO'), '{}') inbound_belongs_to,
            COALESCE(JSON_OBJECT_AGG(ir1.id, ir1.*) FILTER (WHERE ir1.origin_pid = o.id AND NOT ir1.relation_name = 'BELONGS_TO'), '{}') outbound,
            COALESCE(JSON_OBJECT_AGG(ir1.target_pid, ir1.*) FILTER (WHERE ir1.origin_pid = o.id AND ir1.relation_name = 'BELONGS_TO'), '{}') outbound_belongs_to
            FROM objects o
            LEFT OUTER JOIN internal_relations ir1 ON o.id IN (ir1.target_pid, ir1.origin_pid)
            WHERE o.name = $1 AND o.object_type = 'PROJECT'
            GROUP BY o.id;";
        let prepared = client.prepare(query).await?;
        let result: Option<ObjectWithRelations> = client
            .query_opt(&prepared, &[&name])
            .await?
            .map(|r| ObjectWithRelations::from_row(&r));
        Ok(result)
    }

    pub async fn update_endpoints(
        endpoint_id: DieselUlid,
        ep_status: EndpointInfo,
        object_ids: Vec<DieselUlid>,
        client: &Client,
    ) -> Result<()> {
        let query =
            "UPDATE objects SET endpoints = endpoints || $1::jsonb WHERE id = ANY($2::uuid[]);";
        let endpoint: Json<DashMap<DieselUlid, EndpointInfo, RandomState>> =
            Json(DashMap::from_iter([(endpoint_id, ep_status)]));
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&endpoint, &object_ids]).await?;
        Ok(())
    }
}
impl Eq for Hashes {}
impl PartialEq for Hashes {
    fn eq(&self, other: &Self) -> bool {
        self.0.iter().all(|h| other.0.iter().contains(h))
    }
}

impl PartialEq for Object {
    fn eq(&self, other: &Self) -> bool {
        let self_external_relation: HashSet<_> = self
            .external_relations
            .0
             .0
            .iter()
            .map(|r| r.value().clone())
            .collect();
        let other_external_relation: HashSet<_> = other
            .external_relations
            .0
             .0
            .iter()
            .map(|r| r.value().clone())
            .collect();
        match (&self.created_at, other.created_at) {
            (Some(_), None) | (None, Some(_)) | (None, None) => {
                self.id == other.id
                    && self.revision_number == other.revision_number
                    && self.created_by == other.created_by
                    && self.content_len == other.content_len
                    && self.key_values == other.key_values
                    && self.object_status == other.object_status
                    && self.data_class == other.data_class
                    && self.object_type == other.object_type
                    && self_external_relation
                        .iter()
                        .all(|i| other_external_relation.contains(i))
                    && self.authors.0.iter().all(|i| other.authors.0.contains(i))
                    && self.hashes == other.hashes
                    && self.dynamic == other.dynamic
                    && self.metadata_license == other.metadata_license
                    && self.data_license == other.data_license
                    && self.name == other.name
                    && self.title == other.title
            }
            (Some(_), Some(_)) => {
                self.id == other.id
                    && self.created_at == other.created_at
                    && self.revision_number == other.revision_number
                    && self.created_by == other.created_by
                    && self.content_len == other.content_len
                    && self.key_values == other.key_values
                    && self.object_status == other.object_status
                    && self.data_class == other.data_class
                    && self.object_type == other.object_type
                    && self_external_relation
                        .iter()
                        .all(|i| other_external_relation.contains(i))
                    && self.authors.0.iter().all(|i| other.authors.0.contains(i))
                    && self.hashes == other.hashes
                    && self.dynamic == other.dynamic
                    && self.metadata_license == other.metadata_license
                    && self.data_license == other.data_license
            }
        }
    }
}
impl Eq for Object {}
impl PartialEq for ObjectWithRelations {
    fn eq(&self, other: &Self) -> bool {
        // Faster than comparing vecs
        let self_inbound_set: HashSet<_> =
            self.inbound.0.iter().map(|r| r.value().clone()).collect();
        let other_inbound_set: HashSet<_> =
            other.inbound.0.iter().map(|r| r.value().clone()).collect();
        let self_inbound_belongs_to_set: HashSet<_> = self
            .inbound_belongs_to
            .0
            .iter()
            .map(|r| r.value().clone())
            .collect();
        let other_inbound_belongs_to_set: HashSet<_> = other
            .inbound_belongs_to
            .0
            .iter()
            .map(|r| r.value().clone())
            .collect();
        let self_outbound_set: HashSet<_> =
            self.outbound.0.iter().map(|r| r.value().clone()).collect();
        let other_outbound_set: HashSet<_> =
            other.outbound.0.iter().map(|r| r.value().clone()).collect();
        let self_outbound_belongs_to_set: HashSet<_> = self
            .outbound_belongs_to
            .0
            .iter()
            .map(|r| r.value().clone())
            .collect();
        let other_outbound_belongs_to_set: HashSet<_> = other
            .outbound_belongs_to
            .0
            .iter()
            .map(|r| r.value().clone())
            .collect();
        self.object == other.object
            && self_inbound_set
                .iter()
                .all(|item| other_inbound_set.contains(item))
            && self_outbound_set
                .iter()
                .all(|item| other_outbound_set.contains(item))
            && self_inbound_belongs_to_set
                .iter()
                .all(|item| other_inbound_belongs_to_set.contains(item))
            && self_outbound_belongs_to_set
                .iter()
                .all(|item| other_outbound_belongs_to_set.contains(item))
    }
}
impl Eq for ObjectWithRelations {}

//ToDo: Docs
pub async fn get_all_objects_with_relations(client: &Client) -> Result<Vec<ObjectWithRelations>> {
    let query = "SELECT o.*,
        COALESCE(JSON_OBJECT_AGG(ir1.id, ir1.*) FILTER (WHERE ir1.target_pid = o.id AND NOT ir1.relation_name = 'BELONGS_TO'), '{}') inbound,
        COALESCE(JSON_OBJECT_AGG(ir1.origin_pid, ir1.*) FILTER (WHERE ir1.target_pid = o.id AND ir1.relation_name = 'BELONGS_TO'), '{}') inbound_belongs_to,
        COALESCE(JSON_OBJECT_AGG(ir1.id, ir1.*) FILTER (WHERE ir1.origin_pid = o.id AND NOT ir1.relation_name = 'BELONGS_TO'), '{}') outbound,
        COALESCE(JSON_OBJECT_AGG(ir1.target_pid, ir1.*) FILTER (WHERE ir1.origin_pid = o.id AND ir1.relation_name = 'BELONGS_TO'), '{}') outbound_belongs_to
        FROM objects o
        LEFT OUTER JOIN internal_relations ir1 ON o.id IN (ir1.target_pid, ir1.origin_pid)
        GROUP BY o.id;";
    let prepared = client.prepare(query).await?;
    let row = client.query(&prepared, &[]).await?;

    Ok(row.iter().map(ObjectWithRelations::from_row).collect())
}

impl ObjectWithRelations {
    //ToDo: Docs
    pub fn as_object_mapping<T>(&self, mapping: T) -> ObjectMapping<T> {
        match self.object.object_type {
            ObjectType::PROJECT => ObjectMapping::PROJECT(mapping),
            ObjectType::COLLECTION => ObjectMapping::COLLECTION(mapping),
            ObjectType::DATASET => ObjectMapping::DATASET(mapping),
            ObjectType::OBJECT => ObjectMapping::OBJECT(mapping),
        }
    }

    //ToDo: Docs
    pub fn random_object_to(id: &DieselUlid, to: &DieselUlid) -> Self {
        Self {
            object: Object {
                id: *id,
                created_at: None,
                revision_number: 0,
                created_by: DieselUlid::generate(),
                authors: Json(Vec::new()),
                content_len: 0,
                key_values: Json(KeyValues(vec![])),
                object_status: ObjectStatus::AVAILABLE,
                data_class: DataClass::PUBLIC,
                object_type: ObjectType::OBJECT,
                external_relations: Json(ExternalRelations(DashMap::default())),
                hashes: Json(Hashes(vec![])),
                dynamic: false,
                name: "a_name".to_string(),
                title: "".to_string(),
                description: "a_name".to_string(),
                count: 0,
                endpoints: Json(DashMap::default()),
                metadata_license: "CC-BY-4.0".to_string(),
                data_license: "CC-BY-4.0".to_string(),
            },
            inbound: Json(DashMap::default()),
            inbound_belongs_to: Json(DashMap::default()),
            outbound: Json(DashMap::default()),
            outbound_belongs_to: Json(DashMap::from_iter([(*to, InternalRelation::default())])),
        }
    }

    //ToDo: Docs
    pub fn random_object_v2(
        id: &DieselUlid,
        object_type: ObjectType,
        from: Vec<&DieselUlid>,
        to: Vec<&DieselUlid>,
    ) -> Self {
        Self {
            object: Object {
                id: *id,
                revision_number: 0,
                name: "object_name.whatev".to_string(),
                title: "".to_string(),
                description: "".to_string(),
                created_at: None,
                created_by: DieselUlid::generate(),
                authors: Json(Vec::new()),
                content_len: 0,
                count: 0,
                key_values: Json(KeyValues(vec![])),
                object_status: ObjectStatus::AVAILABLE,
                data_class: DataClass::PUBLIC,
                object_type,
                external_relations: Json(ExternalRelations(DashMap::default())),
                hashes: Json(Hashes(vec![])),
                dynamic: false,
                endpoints: Json(DashMap::default()),
                metadata_license: "CC-BY-4.0".to_string(),
                data_license: "CC-BY-4.0".to_string(),
            },
            inbound: Json(DashMap::default()),
            inbound_belongs_to: Json(DashMap::from_iter(
                from.into_iter()
                    .map(|item| (*item, InternalRelation::default()))
                    .collect::<Vec<_>>(),
            )),
            outbound: Json(DashMap::default()),
            outbound_belongs_to: Json(DashMap::from_iter(
                to.into_iter()
                    .map(|item| (*item, InternalRelation::default()))
                    .collect::<Vec<_>>(),
            )),
        }
    }
}

/* ----- Object path traversal ----- */
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Hierarchy {
    pub project_id: String,
    pub collection_id: Option<String>,
    pub dataset_id: Option<String>,
    pub object_id: Option<String>,
}

///ToDo: Rust Doc
pub fn convert_paths_to_hierarchies(
    collected: Vec<Vec<ObjectMapping<DieselUlid>>>,
) -> Vec<Hierarchy> {
    // Init vec to collect converted hierarchies
    let mut hierarchies = Vec::new();

    // Convert hierarchies
    for path in collected {
        let mut hierarchy = Hierarchy::default();

        for node in path {
            match node {
                ObjectMapping::PROJECT(id) => hierarchy.project_id = id.to_string(),
                ObjectMapping::COLLECTION(id) => hierarchy.collection_id = Some(id.to_string()),
                ObjectMapping::DATASET(id) => hierarchy.dataset_id = Some(id.to_string()),
                ObjectMapping::OBJECT(id) => hierarchy.object_id = Some(id.to_string()),
            }
        }

        hierarchies.push(hierarchy)
    }

    hierarchies
}

pub fn extract_paths_from_graph(edge_list: Vec<InternalRelation>) -> Result<Vec<Hierarchy>> {
    let mut children_map: HashMap<
        DieselUlid,
        HashSet<ObjectMapping<DieselUlid>, RandomState>,
        RandomState,
    > = HashMap::default();
    let mut projects: HashSet<DieselUlid, RandomState> = HashSet::default();

    for edge in edge_list {
        children_map
            .entry(edge.origin_pid)
            .or_default()
            .insert(edge.as_target_object_mapping());

        if edge.origin_type == ObjectType::PROJECT {
            projects.insert(edge.origin_pid);
        }
    }

    let mut queue: VecDeque<(Hierarchy, DieselUlid)> = VecDeque::new();
    let mut results: Vec<Hierarchy> = vec![];

    for project in projects {
        queue.push_back((
            Hierarchy {
                project_id: project.to_string(),
                collection_id: None,
                dataset_id: None,
                object_id: None,
            },
            project,
        ))
    }

    while let Some((hierarchy, last_node)) = queue.pop_front() {
        match children_map.get(&last_node) {
            Some(children) => {
                for child in children {
                    let mut mut_hierarchy = hierarchy.clone();
                    match child {
                        ObjectMapping::PROJECT(_) => {
                            bail!("Project cannot be child of other resources")
                        }
                        ObjectMapping::COLLECTION(collection_id) => {
                            mut_hierarchy.collection_id = Some(collection_id.to_string());
                            queue.push_back((mut_hierarchy, *collection_id))
                        }
                        ObjectMapping::DATASET(dataset_id) => {
                            mut_hierarchy.dataset_id = Some(dataset_id.to_string());
                            queue.push_back((mut_hierarchy, *dataset_id))
                        }
                        ObjectMapping::OBJECT(object_id) => {
                            if children_map.contains_key(object_id) {
                                queue.push_back((mut_hierarchy.clone(), *object_id))
                            }

                            mut_hierarchy.object_id = Some(object_id.to_string());
                            results.push(mut_hierarchy)
                        }
                    }
                }
            }
            None => results.push(hierarchy),
        }
    }

    Ok(results)
}
