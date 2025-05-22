use anyhow::anyhow;
use anyhow::Result;
use diesel_ulid::DieselUlid;
use postgres_types::{FromSql, ToSql};
use serde::Deserialize;
use serde::Serialize;
use std::fmt::{Debug, Display, Formatter};
use tokio_postgres::Client;
use tracing::debug;
use tracing::error;

use crate::structs::LocationBinding;
use crate::structs::ObjectLocation;
use crate::structs::UploadPart;

#[derive(Debug, PartialEq, Eq)]
pub struct GenericBytes<
    X: ToSql + for<'a> FromSql<'a> + Send + Sync,
    T: Serialize + for<'a> Deserialize<'a>,
> {
    pub id: X,
    pub data: tokio_postgres::types::Json<T>,
    pub table: Table,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Table {
    Objects,
    Users,
    PubKeys,
    ObjectLocations,
    Permissions,
    Multiparts,
}

impl Display for Table {
    #[tracing::instrument(level = "trace", skip(self, f))]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Table::Objects => write!(f, "objects"),
            Table::Users => write!(f, "users"),
            Table::PubKeys => write!(f, "pub_keys"),
            Table::ObjectLocations => write!(f, "object_locations"),
            Table::Permissions => write!(f, "permissions"),
            Table::Multiparts => write!(f, "multiparts"),
        }
    }
}

#[async_trait::async_trait]
pub trait WithGenericBytes<
    X: ToSql + for<'a> FromSql<'a> + Send + Sync,
    T: Serialize + for<'a> Deserialize<'a> + Send + Sync + Debug,
>: TryFrom<GenericBytes<X, T>> + TryInto<GenericBytes<X, T>> + Clone where
    <Self as TryFrom<GenericBytes<X, T>>>::Error: Debug + Display,
    <Self as TryInto<GenericBytes<X, T>>>::Error: Debug + Display,
{
    fn get_table() -> Table;
    async fn upsert(&self, client: &Client) -> Result<()> {
        let generic: GenericBytes<X, T> = match self.clone().try_into() {
            Ok(generic) => generic,
            Err(e) => {
                error!(error = ?e, msg = e.to_string());
                return Err(anyhow!("Failed to convert to GenericBytes: {:?}", e));
            }
        };

        let query = format!(
            "INSERT INTO {} (id, data) VALUES ($1, $2::JSONB) ON CONFLICT (id) DO UPDATE SET data = $2;",
            Self::get_table()
        );
        let prepared = client.prepare(&query).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;

        client
            .query(&prepared, &[&generic.id, &generic.data])
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;
        Ok(())
    }

    async fn get_all(client: &Client) -> Result<Vec<Self>>
    where
        Self: WithGenericBytes<X, T>,
    {
        let query = format!("SELECT * FROM {};", Self::get_table());
        let prepared = client.prepare(&query).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;
        let rows = client.query(&prepared, &[]).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;
        Ok(rows
            .iter()
            .map(|row| {
                match Self::try_from(GenericBytes {
                    id: row.get::<&str, X>("id"),
                    data: row.get("data"),
                    table: Self::get_table(),
                }) {
                    Ok(generic) => Ok(generic),
                    Err(e) => {
                        error!(error = ?e, msg = e.to_string());
                        Err(anyhow!("Failed to convert to GenericBytes {:?}", e))
                    }
                }
            })
            .collect::<Result<Vec<Self>>>()?)
    }
    async fn get(id: &X, client: &Client) -> Result<Self>
    where
        Self: WithGenericBytes<X, T>,
    {
        let query = format!("SELECT * FROM {} WHERE id = $1;", Self::get_table());
        let prepared = client.prepare(&query).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;
        let row = client.query_one(&prepared, &[&id]).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;
        match Self::try_from(GenericBytes {
            id: row.get::<usize, X>(0),
            data: row.get(1),
            table: Self::get_table(),
        }) {
            Ok(generic) => Ok(generic),
            Err(e) => {
                error!(error = ?e, msg = e.to_string());
                Err(anyhow!("Failed to convert to GenericBytes, {:?}", e))
            }
        }
    }

    #[allow(dead_code)]
    async fn get_opt(id: &X, client: &Client) -> Result<Option<Self>>
    where
        Self: WithGenericBytes<X, T>,
    {
        let query = format!("SELECT * FROM {} WHERE id = $1;", Self::get_table());
        let prepared = client.prepare(&query).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;
        let row = client.query_opt(&prepared, &[&id]).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;

        match row {
            Some(row) => {
                match Self::try_from(GenericBytes {
                    id: row.get::<usize, X>(0),
                    data: row.get(1),
                    table: Self::get_table(),
                }) {
                    Ok(generic) => Ok(Some(generic)),
                    Err(e) => {
                        error!(error = ?e, msg = e.to_string());
                        Err(anyhow!("Failed to convert to GenericBytes, {:?}", e))
                    }
                }
            }
            None => Ok(None),
        }
    }

    async fn delete(id: &X, client: &Client) -> Result<()> {
        let query = format!("DELETE FROM {} WHERE id = $1;", Self::get_table());
        let prepared = client.prepare(&query).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;
        client.execute(&prepared, &[&id]).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;
        Ok(())
    }

    async fn delete_all(client: &Client) -> Result<()> {
        let query = format!("DELETE FROM {};", Self::get_table());
        let prepared = client.prepare(&query).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;
        client.execute(&prepared, &[]).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;
        Ok(())
    }
}

pub async fn _get_parts_by_upload_id(
    client: &Client,
    upload_id: String,
) -> Result<Vec<UploadPart>> {
    let query = "SELECT * FROM multiparts WHERE data->>'upload_id' = $1;";
    let prepared = client.prepare(query).await?;
    let rows = client.query(&prepared, &[&upload_id]).await?;
    Ok(rows
        .iter()
        .map(|row| {
            let data: tokio_postgres::types::Json<UploadPart> = row.get(1);
            data.0
        })
        .collect())
}

pub async fn delete_parts_by_upload_id(client: &Client, upload_id: String) -> Result<()> {
    let query = "DELETE FROM multiparts WHERE data->>'upload_id' = $1;";
    let prepared = client.prepare(query).await?;
    client.execute(&prepared, &[&upload_id]).await?;
    Ok(())
}

pub async fn delete_parts_by_object_id(client: &Client, object_id: &DieselUlid) -> Result<()> {
    let query = "DELETE FROM multiparts WHERE data->>'object_id' = $1;";
    let prepared = client.prepare(query).await?;
    client.execute(&prepared, &[&object_id.to_string()]).await?;
    Ok(())
}

impl ObjectLocation {
    pub async fn delete_many_by_ids(client: &Client, ids: &[DieselUlid]) -> Result<()> {
        let query = "DELETE FROM object_locations WHERE id = ANY($1::UUID[]);";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&ids]).await?;
        Ok(())
    }
}

impl LocationBinding {
    pub async fn insert_binding(&self, client: &Client) -> Result<()> {
        let query =
            "INSERT INTO location_bindings (object_id, location_id) VALUES ($1::UUID, $2::UUID) ON CONFLICT(object_id) DO UPDATE SET location_id=$2::UUID;"
                .to_string();
        let prepared = client.prepare(&query).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;

        client
            .query(&prepared, &[&self.object_id, &self.location_id])
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;
        Ok(())
    }

    pub async fn _get_all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM location_bindings;".to_string();
        let prepared = client.prepare(&query).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;
        let rows = client.query(&prepared, &[]).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;
        Ok(rows
            .iter()
            .map(|row| Self {
                object_id: row.get::<usize, DieselUlid>(0),
                location_id: row.get::<usize, DieselUlid>(1),
            })
            .collect::<Vec<Self>>())
    }
    pub async fn get_by_object_id(object_id: &DieselUlid, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM location_bindings WHERE object_id = $1;".to_string();
        let prepared = client.prepare(&query).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;
        let row = client
            .query_opt(&prepared, &[&object_id])
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;
        Ok(row.map(|row| Self {
            object_id: row.get::<usize, DieselUlid>(0),
            location_id: row.get::<usize, DieselUlid>(1),
        }))
    }

    pub async fn _get_by_location_id(
        location_id: &DieselUlid,
        client: &Client,
    ) -> Result<Vec<Self>> {
        let query = "SELECT * FROM location_bindings WHERE location_id = $1;".to_string();
        let prepared = client.prepare(&query).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;
        let row = client
            .query(&prepared, &[&location_id])
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;
        Ok(row
            .iter()
            .map(|row| Self {
                object_id: row.get::<usize, DieselUlid>(0),
                location_id: row.get::<usize, DieselUlid>(1),
            })
            .collect::<Vec<Self>>())
    }

    pub async fn delete_by_object_id(object_id: &DieselUlid, client: &Client) -> Result<()> {
        let query = "DELETE FROM location_bindings WHERE object_id = $1;".to_string();
        let prepared = client.prepare(&query).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;
        client
            .execute(&prepared, &[&object_id])
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;
        Ok(())
    }
}
