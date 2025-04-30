use std::borrow::Cow;

use super::store::{Store, tables};
use crate::{
    error::ArunaError,
    models::models::Resource,
    persistence::storage::store::tables::{
        PUBLIC_MAPPINGS_DB_NAME, RESOURCE_DB_NAME, RESOURCE_MAPPINGS_DB_NAME, USER_DB_NAME,
        USER_MAPPINGS_DB_NAME,
    },
};
use fjall::{
    Config, PartitionCreateOptions, ReadTransaction, TransactionalKeyspace,
    TransactionalPartitionHandle, WriteTransaction,
};
use roaring::RoaringBitmap;
use ulid::Ulid;

pub struct FjallStore {
    keyspace: TransactionalKeyspace,
    tables: FjallTables,
}

impl std::fmt::Debug for FjallStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallStore").finish()
    }
}

struct FjallTables {
    resources: TransactionalPartitionHandle,
    resource_mappings: TransactionalPartitionHandle,
    users: TransactionalPartitionHandle,
    user_mappings: TransactionalPartitionHandle,
    public_mappings: TransactionalPartitionHandle,
}
pub struct FjallConfig {
    pub path: String,
    pub res_sdx: tokio::sync::mpsc::Sender<(u32, Resource)>,
    pub idx_sdx: tokio::sync::oneshot::Sender<u32>,
}
pub enum FjallTxn<'a> {
    Read(ReadTransaction),
    Write(WriteTransaction<'a>),
}

impl<'a> Store<'a> for FjallStore {
    type StoreConfig = FjallConfig;

    type Txn = FjallTxn<'a>;
    // type Txn = ();

    fn new(config: Self::StoreConfig) -> Result<Self, crate::error::ArunaError> {
        let path = format!("{}/path", config.path);
        let keyspace = Config::new(path)
            .fsync_ms(Some(500))
            .cache_size(1024 * 1024 * 1024)
            .max_open_files(10)
            .open_transactional()?; // or open_transactional for transactional semantics

        let mut write_txn = keyspace.write_tx();
        let resources =
            keyspace.open_partition(RESOURCE_DB_NAME, PartitionCreateOptions::default())?;
        let resource_mappings = keyspace
            .open_partition(RESOURCE_MAPPINGS_DB_NAME, PartitionCreateOptions::default())?;
        let users = keyspace.open_partition(USER_DB_NAME, PartitionCreateOptions::default())?;
        let user_mappings =
            keyspace.open_partition(USER_MAPPINGS_DB_NAME, PartitionCreateOptions::default())?;
        let public_mappings =
            keyspace.open_partition(PUBLIC_MAPPINGS_DB_NAME, PartitionCreateOptions::default())?;

        for res in write_txn.iter(&resources) {
            let (id, res) = res?;
            let idx = resource_mappings
                .get(id.as_ref())?
                .expect("No valid mapping found"); // TODO: Remove unwraps and replace with
            let doc = automerge::AutoCommit::load(res.as_ref())?;
            let resource: Resource = autosurgeon::hydrate(&doc)?;
            //bincode::serde::decode_from_slice(res.as_ref(), bincode::config::standard())
            //    .map_err(|e| ArunaError::DeserializeError(e.to_string()))?;
            let idx = u32::from_be_bytes(idx.as_ref().try_into().unwrap());
            // new idx because this is a local only sorting
            config
                .res_sdx
                .blocking_send((idx, resource))
                .map_err(|e| ArunaError::DatabaseError(e.to_string()))?;
        }

        // Send idx to persistor
        match resource_mappings.last_key_value()? {
            Some((_id, idx)) => {
                let (idx, _) =
                    bincode::serde::decode_from_slice(idx.as_ref(), bincode::config::standard())
                        .map_err(|e| ArunaError::DeserializeError(e.to_string()))?;
                config
                    .idx_sdx
                    .send(idx)
                    .map_err(|e| ArunaError::DatabaseError(format!("Send error {e}")))?
            }
            None => config
                .idx_sdx
                .send(0u32)
                .map_err(|e| ArunaError::DatabaseError(format!("Send error {e}")))?,
        }

        if write_txn
            .get(&public_mappings, Ulid::default().0.to_be_bytes())?
            .is_none()
        {
            // Init bitmap with public resources
            let mut value = Vec::new();
            RoaringBitmap::new().serialize_into(&mut value)?;
            write_txn.insert(&public_mappings, Ulid::default().to_bytes(), value);
        }

        write_txn.commit()?;

        Ok(Self {
            keyspace,
            tables: FjallTables {
                resources,
                resource_mappings,
                users,
                user_mappings,
                public_mappings,
            },
        })
    }

    fn create_txn(&'a self, write: bool) -> Result<Self::Txn, crate::error::ArunaError> {
        if write {
            Ok(FjallTxn::Write(self.keyspace.write_tx()))
        } else {
            Ok(FjallTxn::Read(self.keyspace.read_tx()))
        }
    }

    fn put(
        &'a self,
        txn: &mut Self::Txn,
        dbname: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), crate::error::ArunaError> {
        let wtxn = match txn {
            FjallTxn::Read(_) => {
                return Err(ArunaError::DatabaseError(
                    "Only write txns permitted".to_string(),
                ));
            }
            FjallTxn::Write(w) => w,
        };
        let tables = &self.tables;
        match dbname {
            tables::RESOURCE_DB_NAME => {
                wtxn.insert(&tables.resources, key, value);
            }
            tables::RESOURCE_MAPPINGS_DB_NAME => {
                wtxn.insert(&tables.resource_mappings, key, value);
            }
            tables::USER_DB_NAME => {
                wtxn.insert(&tables.users, key, value);
            }
            tables::USER_MAPPINGS_DB_NAME => {
                wtxn.insert(&tables.user_mappings, key, value);
            }
            tables::PUBLIC_MAPPINGS_DB_NAME => {
                wtxn.insert(&tables.public_mappings, key, value);
            }
            _ => {
                return Err(ArunaError::DatabaseError("Database not found".to_string()));
            }
        }

        Ok(())
    }

    fn remove(
        &'a self,
        txn: &mut Self::Txn,
        dbname: &str,
        key: &[u8],
    ) -> Result<(), crate::error::ArunaError> {
        let wtxn = match txn {
            FjallTxn::Read(_) => {
                return Err(ArunaError::DatabaseError(
                    "Only write txns permitted".to_string(),
                ));
            }
            FjallTxn::Write(w) => w,
        };
        let tables = &self.tables;
        match dbname {
            tables::RESOURCE_DB_NAME => {
                wtxn.remove(&tables.resources, key);
            }
            tables::RESOURCE_MAPPINGS_DB_NAME => {
                wtxn.remove(&tables.resource_mappings, key);
            }
            tables::USER_DB_NAME => {
                wtxn.remove(&tables.users, key);
            }
            tables::USER_MAPPINGS_DB_NAME => {
                wtxn.remove(&tables.user_mappings, key);
            }
            tables::PUBLIC_MAPPINGS_DB_NAME => {
                wtxn.remove(&tables.public_mappings, key);
            }
            _ => {
                return Err(ArunaError::DatabaseError("Database not found".to_string()));
            }
        }

        Ok(())
    }

    fn get<'b>(
        &'a self,
        txn: &'b Self::Txn,
        dbname: &'b str,
        key: &'b [u8],
    ) -> Result<Option<std::borrow::Cow<'b, [u8]>>, crate::error::ArunaError>
    where
        'a: 'b,
    {
        let tables = &self.tables;
        let result = match txn {
            FjallTxn::Read(r) => {
                match dbname {
                    tables::RESOURCE_DB_NAME => r.get(&tables.resources, key)?,
                    tables::RESOURCE_MAPPINGS_DB_NAME => r.get(&tables.resource_mappings, key)?,
                    tables::USER_DB_NAME => r.get(&tables.users, key)?,
                    tables::USER_MAPPINGS_DB_NAME => r.get(&tables.user_mappings, key)?,
                    tables::PUBLIC_MAPPINGS_DB_NAME => r.get(&tables.public_mappings, key)?,
                    _ => {
                        return Err(ArunaError::DatabaseError("Database not found".to_string()));
                    }
                }
                .map(|v| {
                    // TODO: This sucks, needs improvement
                    let value = v.as_ref().to_vec();
                    Cow::from(value).to_owned()
                })
            }
            FjallTxn::Write(w) => {
                match dbname {
                    tables::RESOURCE_DB_NAME => w.get(&tables.resources, key)?,
                    tables::RESOURCE_MAPPINGS_DB_NAME => w.get(&tables.resource_mappings, key)?,
                    tables::USER_DB_NAME => w.get(&tables.users, key)?,
                    tables::USER_MAPPINGS_DB_NAME => w.get(&tables.user_mappings, key)?,
                    tables::PUBLIC_MAPPINGS_DB_NAME => w.get(&tables.public_mappings, key)?,
                    _ => {
                        return Err(ArunaError::DatabaseError("Database not found".to_string()));
                    }
                }
                .map(|v| {
                    // TODO: This sucks, needs improvement
                    let value = v.as_ref().to_vec();
                    Cow::from(value).to_owned()
                })
            }
        };

        Ok(result)
    }

    fn commit(&self, txn: Self::Txn) -> Result<(), crate::error::ArunaError> {
        match txn {
            FjallTxn::Read(r) => {
                drop(r);
                Ok(())
            }
            FjallTxn::Write(w) => Ok(w.commit()?),
        }
    }

    fn purge(&self) -> Result<(), ArunaError> {
        let mut write_txn = self.keyspace.write_tx();

        for table in [
            &self.tables.public_mappings,
            &self.tables.resources,
            &self.tables.users,
            &self.tables.user_mappings,
            &self.tables.resource_mappings,
        ] {
            let mut indices = Vec::new();
            for item in write_txn.iter(table) {
                let (idx, _) = item?;
                indices.push(idx);
            }
            for idx in indices {
                write_txn.remove(table, idx);
            }
        }

        // Init bitmap with public resources
        let mut value = Vec::new();
        RoaringBitmap::new().serialize_into(&mut value).unwrap();
        write_txn.insert(
            &self.tables.public_mappings,
            Ulid::default().to_bytes(),
            value,
        );
        write_txn.commit().unwrap();
        Ok(())
    }
}
