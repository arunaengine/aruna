use std::{borrow::Cow, fs, sync::Arc};

use super::store::{Store, tables};
use crate::{error::ArunaError, models::models::Resource};
use redb::{Database, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction};
use roaring::RoaringBitmap;
use ulid::Ulid;

const RESOURCES: TableDefinition<&[u8], &[u8]> = TableDefinition::new(tables::RESOURCE_DB_NAME);
const RESOURCE_MAPPINGS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new(tables::RESOURCE_MAPPINGS_DB_NAME);
const USERS: TableDefinition<&[u8], &[u8]> = TableDefinition::new(tables::USER_DB_NAME);
const USER_MAPPINGS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new(tables::USER_MAPPINGS_DB_NAME);
const PUBLIC_MAPPINGS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new(tables::PUBLIC_MAPPINGS_DB_NAME);

pub struct Redb {
    db: Arc<Database>,
}
pub struct RedbConfig {
    pub path: String,
    pub res_sdx: tokio::sync::mpsc::Sender<(u32, Resource)>,
    pub idx_sdx: tokio::sync::oneshot::Sender<u32>,
}
pub enum RedbTxn {
    Write(WriteTransaction),
    Read(ReadTransaction),
}

impl<'a> Store<'a> for Redb {
    type StoreConfig = RedbConfig;

    type Txn = RedbTxn;

    fn new(config: Self::StoreConfig) -> Result<Self, crate::error::ArunaError> {
        let path = format!("{}/store", config.path);
        println!("{path}");
        fs::create_dir_all(&path)?;
        let file = format!("/{path}/database.redb");

        let db = Arc::new(Database::create(file)?);
        let db_clone = db.clone();

        let write_txn = db_clone.begin_write()?;
        {
            // Send resources to search
            let resources = write_txn.open_table(RESOURCES)?;
            let resource_mappings = write_txn.open_table(RESOURCE_MAPPINGS)?;
            for res in resources.iter()? {
                let (id, res) = res?;
                let idx = resource_mappings
                    .get(id.value())?
                    .expect("No valid mapping found"); // TODO: Remove unwraps and replace with

                let doc = automerge::AutoCommit::load(res.value())?;
                let resource: Resource = autosurgeon::hydrate(&doc)?;

                let idx = u32::from_be_bytes(idx.value().try_into().unwrap());
                // new idx because this is a local only sorting
                config
                    .res_sdx
                    .blocking_send((idx, resource))
                    .map_err(|e| ArunaError::DatabaseError(e.to_string()))?;
            }

            // Send idx to persistor
            match resource_mappings.last()? {
                Some((_id, idx)) => {
                    let (idx, _) =
                        bincode::serde::decode_from_slice(idx.value(), bincode::config::standard())
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

            let mut public_mappings = write_txn.open_table(PUBLIC_MAPPINGS)?;
            if public_mappings
                .get(Ulid::default().0.to_be_bytes().as_ref())?
                .is_none()
            {
                // Init bitmap with public resources
                let mut value = Vec::new();
                RoaringBitmap::new().serialize_into(&mut value)?;
                let key = Ulid::default().to_bytes();
                public_mappings.insert(key.as_slice(), value.as_slice())?;
            }
        }
        write_txn.commit()?;

        Ok(Self { db })
    }

    fn create_txn(&'a self, write: bool) -> Result<Self::Txn, crate::error::ArunaError> {
        Ok(if write {
            RedbTxn::Write(self.db.begin_write()?)
        } else {
            RedbTxn::Read(self.db.begin_read()?)
        })
    }

    fn put(
        &'a self,
        txn: &mut Self::Txn,
        dbname: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), crate::error::ArunaError> {
        let RedbTxn::Write(write_txn) = txn else {
            return Err(ArunaError::DatabaseError(
                "Only write txns allowed".to_string(),
            ));
        };

        match dbname {
            tables::RESOURCE_DB_NAME => {
                let mut resources = write_txn.open_table(RESOURCES)?;
                resources.insert(key, value)?;
            }
            tables::RESOURCE_MAPPINGS_DB_NAME => {
                let mut res_mappings = write_txn.open_table(RESOURCE_MAPPINGS)?;
                res_mappings.insert(key, value)?;
            }
            tables::USER_DB_NAME => {
                let mut users = write_txn.open_table(USERS)?;
                users.insert(key, value)?;
            }
            tables::USER_MAPPINGS_DB_NAME => {
                let mut user_mappings = write_txn.open_table(USER_MAPPINGS)?;
                user_mappings.insert(key, value)?;
            }
            tables::PUBLIC_MAPPINGS_DB_NAME => {
                let mut public_mappings = write_txn.open_table(PUBLIC_MAPPINGS)?;
                public_mappings.insert(key, value)?;
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
        // TODO: This is very redundant code
        let result = match txn {
            RedbTxn::Read(r) => {
                match dbname {
                    tables::RESOURCE_DB_NAME => {
                        let res = r.open_table(RESOURCES)?;
                        res.get(key)?
                    }
                    tables::RESOURCE_MAPPINGS_DB_NAME => {
                        let res_mappings = r.open_table(RESOURCE_MAPPINGS)?;
                        res_mappings.get(key)?
                    }
                    tables::USER_DB_NAME => {
                        let users = r.open_table(USERS)?;
                        users.get(key)?
                    }
                    tables::USER_MAPPINGS_DB_NAME => {
                        let user_mappings = r.open_table(USER_MAPPINGS)?;
                        user_mappings.get(key)?
                    }
                    tables::PUBLIC_MAPPINGS_DB_NAME => {
                        let public_mappings = r.open_table(PUBLIC_MAPPINGS)?;
                        public_mappings.get(key)?
                    }
                    _ => {
                        return Err(ArunaError::DatabaseError("Database not found".to_string()));
                    }
                }
                .map(|v| {
                    // TODO: This sucks, needs improvement
                    let value = v.value().to_vec();
                    Cow::from(value).to_owned()
                })
            }
            RedbTxn::Write(w) => match dbname {
                tables::RESOURCE_DB_NAME => {
                    let res = w.open_table(RESOURCES)?;
                    res.get(key)?.map(|v| Cow::from(v.value().to_owned()))
                }
                tables::RESOURCE_MAPPINGS_DB_NAME => {
                    let res_mappings = w.open_table(RESOURCE_MAPPINGS)?;
                    res_mappings
                        .get(key)?
                        .map(|v| Cow::from(v.value().to_owned()))
                }
                tables::USER_DB_NAME => {
                    let users = w.open_table(USERS)?;
                    users.get(key)?.map(|v| Cow::from(v.value().to_owned()))
                }
                tables::USER_MAPPINGS_DB_NAME => {
                    let user_mappings = w.open_table(USER_MAPPINGS)?;
                    user_mappings
                        .get(key)?
                        .map(|v| Cow::from(v.value().to_owned()))
                }
                tables::PUBLIC_MAPPINGS_DB_NAME => {
                    let public_mappings = w.open_table(PUBLIC_MAPPINGS)?;
                    public_mappings
                        .get(key)?
                        .map(|v| Cow::from(v.value().to_owned()))
                }
                _ => {
                    return Err(ArunaError::DatabaseError("Database not found".to_string()));
                }
            },
        };

        Ok(result)
    }

    fn commit(&self, txn: Self::Txn) -> Result<(), crate::error::ArunaError> {
        Ok(match txn {
            RedbTxn::Write(w) => w.commit()?,
            RedbTxn::Read(r) => r.close()?,
        })
    }

    fn purge(&self) -> Result<(), ArunaError> {
        let wtxn = self.db.begin_write()?;
        {
            let mut resources = wtxn.open_table(RESOURCES)?;
            let mut resource_mappings = wtxn.open_table(RESOURCE_MAPPINGS)?;
            let mut users = wtxn.open_table(USERS)?;
            let mut user_mappings = wtxn.open_table(USER_MAPPINGS)?;
            let mut public_mappings = wtxn.open_table(PUBLIC_MAPPINGS)?;
            resources.retain(|_, _| false)?;
            resource_mappings.retain(|_, _| false)?;
            users.retain(|_, _| false)?;
            user_mappings.retain(|_, _| false)?;
            public_mappings.retain(|_, _| false)?;

            // Init bitmap with public resources
            let mut value = Vec::new();
            RoaringBitmap::new().serialize_into(&mut value)?;
            let key = Ulid::default().to_bytes();
            public_mappings.insert(key.as_slice(), value.as_slice())?;
        }
        wtxn.commit()?;

        Ok(())
    }
}
