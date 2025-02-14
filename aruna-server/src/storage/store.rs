use crate::{
    constants::field_names::{DELETED_FIELD, ID_FIELD, VARIANT_FIELD},
    error::ArunaError,
    logerr,
    models::models::{
        Component, EdgeType, GenericNode, Group, IssuerKey, IssuerType, License, MilliIdx, Node,
        NodeVariant, Permission, RawRelation, Realm, Relation, RelationInfo, RelationRange,
        Resource, ServerState, ServiceAccount, Subscriber, Token, User,
    },
    storage::{
        graph::load_graph,
        init,
        milli_helpers::prepopulate_fields,
        utils::{pubkey_from_pem, SigningInfoCodec},
    },
    transactions::{controller::KeyConfig, request::Requester},
};
use ahash::RandomState;
use chrono::NaiveDateTime;
use jsonwebtoken::{DecodingKey, EncodingKey};
use milli::heed::{
    byteorder::BigEndian,
    types::{SerdeBincode, Str, U128},
    Database, DatabaseFlags, EnvFlags, EnvOpenOptions, PutFlags, Unspecified,
};
use milli::{
    documents::{DocumentsBatchBuilder, DocumentsBatchReader},
    execute_search, filtered_universe,
    update::{IndexDocuments, IndexDocumentsConfig, IndexDocumentsMethod, IndexerConfig},
    CboRoaringBitmapCodec, DefaultSearchLogger, Filter, GeoSortStrategy, Index, SearchContext,
    TermsMatchingStrategy, TimeBudget, BEU32, BEU64,
};
use obkv::KvReaderU16;
use petgraph::{visit::EdgeRef, Direction};
use roaring::RoaringBitmap;
use serde_json::Value;
use std::{
    collections::HashMap,
    fs,
    io::Cursor,
    sync::{atomic::AtomicU64, RwLock},
};
use ulid::Ulid;

use super::{
    graph::{Graph, GraphTxn, IndexHelper, Mode},
    obkv_ext::FieldIterator,
    txns::{ReadTxn, Txn, WriteTxn},
};

// LMBD database names
pub mod db_names {
    pub const RELATION_INFO_DB_NAME: &str = "relation_infos";
    pub const NODE_DB_NAME: &str = "nodes";
    pub const RELATION_DB_NAME: &str = "relations"; // -> HashSet with Source/Type/Target
    pub const OIDC_MAPPING_DB_NAME: &str = "oidc_mappings";
    pub const PUBKEY_DB_NAME: &str = "pubkeys";
    pub const TOKENS_DB_NAME: &str = "tokens";
    pub const SERVER_INFO_DB_NAME: &str = "server_infos";
    pub const EVENT_DB_NAME: &str = "events";
    pub const TOKENS: &str = "tokens";
    pub const USER: &str = "users";
    pub const READ_GROUP_PERMS: &str = "read_group_perms";
    pub const SINGLE_ENTRY_DB: &str = "single_entry_database";
    pub const SUBSCRIBERS: &str = "subscribers";
}

pub mod single_entry_names {
    pub const ISSUER_KEYS: &str = "issuer_keys";
    pub const SIGNING_KEYS: &str = "signing_keys";
    pub const PUBLIC_RESOURCES: &str = "public_resources";
    pub const SEARCHABLE_USERS: &str = "searchable_users";
    pub const SUBSCRIBER_CONFIG: &str = "subscriber_config";
}

#[allow(unused)]
pub(super) type DecodingKeyIdentifier = (String, String); // (IssuerName, KeyID)
pub type IssuerInfo = (IssuerType, DecodingKey, Vec<String>); // (IssuerType, DecodingKey, Audiences)

#[allow(unused)]
pub struct Store {
    // Milli index to store objects and allow for search
    milli_index: Index,

    // Contains the following entries
    // ISSUER_KEYS
    // SigningKeys
    // Config?
    // SubscribersConfig
    single_entry_database: Database<Unspecified, Unspecified>,

    // Store it in an increasing list of relations
    relations: Database<BEU64, SerdeBincode<RawRelation>>,
    // Increasing relation index
    relation_idx: AtomicU64,
    // Relations info
    relation_infos: Database<BEU32, SerdeBincode<RelationInfo>>,
    // events db
    events: Database<BEU32, U128<BigEndian>>,
    // TODO:
    // Database for event_subscriber / status
    // Roaring bitmap for subscriber resources + last acknowledged event
    subscribers: Database<U128<BigEndian>, SerdeBincode<Vec<u128>>>,

    // Database for read permissions of groups, users and realms
    read_permissions: Database<BEU32, CboRoaringBitmapCodec>,

    // Database for tokens with user_idx as key and a list of tokens as value
    tokens: Database<BEU32, SerdeBincode<Vec<Option<Token>>>>,
    // Database for (oidc_user_id, oidc_provider) to UserNodeIdx mappings
    oidc_mappings: Database<SerdeBincode<(String, String)>, BEU32>,

    // -------------------
    // Volatile data
    // Component status
    status: RwLock<HashMap<Ulid, ServerState, RandomState>>,
    //issuer_decoding_keys: HashMap<DecodingKeyIdentifier, IssuerInfo, RandomState>,
    //signing_info: (u32, EncodingKey, DecodingKey),
    // This has to be a RwLock because the graph is mutable
    graph: RwLock<Graph>,
    // graph: RwLock<Graph<NodeVariant, EdgeType>>,
    // idx_mappings: RwLock<IdxMappings>,
}

impl Store {
    #[tracing::instrument(level = "trace", skip(key_config))]
    pub fn new(path: String, key_config: KeyConfig) -> Result<Self, ArunaError> {
        use db_names::*;
        let path = format!("{path}/store");
        fs::create_dir_all(&path).inspect_err(logerr!())?;
        // SAFETY: This opens a memory mapped file that may introduce UB
        //         if handled incorrectly
        //         see: https://docs.rs/heed/latest/heed/struct.EnvOpenOptions.html#safety-1

        let mut env_options = EnvOpenOptions::new();
        unsafe { env_options.flags(EnvFlags::MAP_ASYNC | EnvFlags::WRITE_MAP) };
        env_options.map_size(1024 * 1024 * 1024 * 1024); // 1 TB

        let milli_index = Index::new(env_options, path, true).inspect_err(logerr!())?;

        let mut write_txn = milli_index.write_txn().inspect_err(logerr!())?;

        prepopulate_fields(&milli_index, &mut write_txn).inspect_err(logerr!())?;

        let env = &milli_index.env;
        let relations = env
            .create_database(&mut write_txn, Some(RELATION_DB_NAME))
            .inspect_err(logerr!())?;
        let relation_infos = env
            .create_database(&mut write_txn, Some(RELATION_INFO_DB_NAME))
            .inspect_err(logerr!())?;
        let tokens = env
            .create_database(&mut write_txn, Some(TOKENS_DB_NAME))
            .inspect_err(logerr!())?;
        let read_permissions = env
            .create_database(&mut write_txn, Some(READ_GROUP_PERMS))
            .inspect_err(logerr!())?;
        let single_entry_database = env
            .create_database(&mut write_txn, Some(SINGLE_ENTRY_DB))
            .inspect_err(logerr!())?;
        let oidc_mappings = env
            .create_database(&mut write_txn, Some(OIDC_MAPPING_DB_NAME))
            .inspect_err(logerr!())?;

        // Special events database allowing for duplicates
        let events = env
            .database_options()
            .types::<BEU32, U128<BigEndian>>()
            .flags(DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED | DatabaseFlags::INTEGER_KEY)
            .name(EVENT_DB_NAME)
            .create(&mut write_txn)
            .inspect_err(logerr!())?;

        // Database for event subscribers
        let subscribers = env
            .create_database(&mut write_txn, Some(SUBSCRIBERS))
            .inspect_err(logerr!())?;

        // INIT relations
        init::init_relations(&mut write_txn, &relation_infos)?;
        // INIT encoding_keys
        init::init_encoding_keys(&mut write_txn, &key_config, &single_entry_database)?;
        // INIT issuer_keys
        init::init_issuers(&mut write_txn, &key_config, &single_entry_database)?;

        write_txn.commit().inspect_err(logerr!())?;

        let relation_idx = AtomicU64::new(0);

        let rtxn = milli_index.read_txn().inspect_err(logerr!())?;

        let graph = load_graph(&rtxn, &relation_idx, &relations, &milli_index.documents)
            .inspect_err(logerr!())?;

        rtxn.commit()?;

        Ok(Self {
            milli_index,
            relations,
            relation_idx,
            relation_infos,
            events,
            subscribers,
            tokens,
            read_permissions,
            status: RwLock::new(HashMap::default()),
            single_entry_database,
            oidc_mappings,
            //issuer_decoding_keys,
            //signing_info: key_config,
            graph: RwLock::new(graph),
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn read_txn(&self) -> Result<ReadTxn, ArunaError> {
        tracing::trace!(msg="Get read_txn from milli");
        let txn = self.milli_index.read_txn().inspect_err(logerr!())?;
        tracing::trace!(msg="Got read_txn from milli");
        Ok(ReadTxn {
            txn,
            graph: &self.graph,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn write_txn(&self) -> Result<WriteTxn, ArunaError> {
        Ok(WriteTxn {
            graph: &self.graph,
            txn: Some(self.milli_index.write_txn().inspect_err(logerr!())?),
            events: &self.events,
            subscribers: &self.subscribers,
            single_entry_database: &self.single_entry_database,
            nodes: Vec::new(),
            added_edges: Vec::new(),
            removed_edges: Vec::new(),
            committed: false,
        })
    }

    #[tracing::instrument(level = "trace", skip(self, id, txn))]
    pub fn get_idx_from_ulid<'a>(&self, id: &Ulid, txn: &impl Txn<'a>) -> Option<MilliIdx> {
        self.milli_index
            .external_documents_ids
            .get(txn.get_ro_txn(), &id.to_string())
            .inspect_err(logerr!())
            .ok()
            .flatten()
            .map(|idx| MilliIdx(idx))
    }

    #[tracing::instrument(level = "trace", skip(self, id, txn))]
    pub fn get_idx_from_ulid_validate<'a>(
        &self,
        id: &Ulid,
        field_name: &str,
        expected_variants: &[NodeVariant],
        txn: &impl Txn<'a>,
    ) -> Result<MilliIdx, ArunaError> {
        let milli_idx = self
            .milli_index
            .external_documents_ids
            .get(txn.get_ro_txn(), &id.to_string())
            .inspect_err(logerr!())
            .ok()
            .flatten()
            .ok_or_else(|| {
                ArunaError::NotFound(format!("Resource not found: {}, field: {}", id, field_name))
            })?;
        let graph_lock = self.graph.read().expect("Poisoned lock");
        let graph_idx = *graph_lock
            .idx_mappings
            .milli_graph
            .get(milli_idx as usize)
            .ok_or_else(|| ArunaError::GraphError("Idx not found".to_string()))?;

        let node_weight = graph_lock
            .graph
            .node_weight(graph_idx.into())
            .ok_or_else(|| {
                ArunaError::NotFound(format!("Resource not found: {}, field: {}", id, field_name))
            })?;

        if !expected_variants.contains(node_weight) {
            return Err(ArunaError::InvalidParameter {
                name: field_name.to_string(),
                error: format!(
                    "Resource {} not of expected types: {:?}",
                    id, expected_variants
                ),
            });
        }

        Ok(MilliIdx(milli_idx))
    }

    #[tracing::instrument(level = "trace", skip(self, id, txn))]
    pub fn get_ulid_from_idx<'a>(&self, id: &MilliIdx, txn: &impl Txn<'a>) -> Option<Ulid> {
        let response = self
            .milli_index
            .documents
            .get(txn.get_ro_txn(), &id.0)
            .inspect_err(logerr!())
            .ok()
            .flatten()?;
        serde_json::from_slice::<Ulid>(response.get(0u16)?).ok()
    }

    #[tracing::instrument(level = "trace", skip(self, wtxn))]
    pub fn create_relation(
        &self,
        wtxn: &mut WriteTxn,
        source: MilliIdx,
        target: MilliIdx,
        edge_type: EdgeType,
    ) -> Result<(), ArunaError> {
        let relation = RawRelation {
            source,
            target,
            edge_type,
        };

        self.relations
            .put_with_flags(
                wtxn.get_txn(),
                PutFlags::APPEND,
                &self
                    .relation_idx
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                &relation,
            )
            .inspect_err(logerr!())?;

        wtxn.add_edge(source.into(), target.into(), edge_type)
            .inspect_err(logerr!())?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, txn))]
    pub fn get_node<'b, T: Node>(&self, txn: &impl Txn<'b>, node_idx: MilliIdx) -> Option<T>
    where
        for<'a> &'a T: TryInto<serde_json::Map<String, Value>, Error = ArunaError>,
    {
        let response = self
            .milli_index
            .documents
            .get(txn.get_ro_txn(), &node_idx.0)
            .inspect_err(logerr!())
            .ok()
            .flatten()?;

        T::try_from(&response).ok()
    }

    #[tracing::instrument(level = "trace", skip(self, txn))]
    pub fn get_raw_node<'b: 'a, 'a>(
        &self,
        txn: &'b impl Txn<'b>,
        node_idx: MilliIdx,
    ) -> Option<&'a KvReaderU16> {
        self.milli_index
            .documents
            .get(txn.get_ro_txn(), &node_idx.0)
            .inspect_err(logerr!())
            .ok()
            .flatten()
    }

    #[tracing::instrument(level = "trace", skip(self, node, wtxn))]
    pub fn create_node<'a, T: Node>(
        &'a self,
        wtxn: &mut WriteTxn<'a>,
        node: &T,
    ) -> Result<MilliIdx, ArunaError>
    where
        for<'b> &'b T: TryInto<serde_json::Map<String, Value>, Error = ArunaError>,
    {
        let indexer_config = IndexerConfig::default();

        let builder = IndexDocuments::new(
            wtxn.get_txn(),
            &self.milli_index,
            &indexer_config,
            IndexDocumentsConfig::default(),
            |_| (),
            || false,
        )?;

        // Request the ulid from the node
        let id = node.get_id();
        // Request the variant from the node
        let variant = node.get_variant();

        // Create a document batch
        let mut documents_batch = DocumentsBatchBuilder::new(Vec::new());
        // Add the json object to the batch
        documents_batch.append_json_object(&node.try_into()?)?;
        // Create a reader for the batch
        let reader = DocumentsBatchReader::from_reader(Cursor::new(documents_batch.into_inner()?))
            .map_err(|_| {
                tracing::error!(?id, "Unable to index document");
                ArunaError::DatabaseError("Unable to index document".to_string())
            })?;
        // Add the batch to the reader
        let (builder, error) = builder.add_documents(reader)?;
        error.map_err(|e| {
            tracing::error!(?id, ?e, "Error adding document");
            ArunaError::DatabaseError("Error adding document".to_string())
        })?;

        // Execute the indexing
        builder.execute()?;

        // Get the idx of the node
        let milli_idx = self
            .get_idx_from_ulid(&id, wtxn)
            .ok_or_else(|| ArunaError::DatabaseError("Missing idx".to_string()))?;

        // Add the node to the graph
        let _graph_index = wtxn.add_node(milli_idx, variant);

        // Ensure that the index in graph and milli stays in sync
        // assert_eq!(index.index() as u32, milli_idx);

        Ok(milli_idx)
    }

    #[tracing::instrument(level = "trace", skip(self, nodes, wtxn))]
    pub fn create_nodes_batch<'a, T: Node>(
        &'a self,
        wtxn: &mut WriteTxn<'a>,
        nodes: Vec<&T>,
    ) -> Result<Vec<(Ulid, MilliIdx)>, ArunaError>
    where
        for<'b> &'b T: TryInto<serde_json::Map<String, Value>, Error = ArunaError>,
    {
        let indexer_config = IndexerConfig::default();
        let builder = IndexDocuments::new(
            wtxn.get_txn(),
            &self.milli_index,
            &indexer_config,
            IndexDocumentsConfig::default(),
            |_| (),
            || false,
        )?;

        // Create a document batch
        let mut documents_batch = DocumentsBatchBuilder::new(Vec::new());

        let mut ids = Vec::new();
        for node in nodes {
            // Request the ulid from the node
            let id = node.get_id();
            // Request the variant from the node
            let variant = node.get_variant();

            ids.push((id, variant));

            // Add the json object to the batch
            documents_batch.append_json_object(&node.try_into()?)?;
        }
        // Create a reader for the batch
        let reader = DocumentsBatchReader::from_reader(Cursor::new(documents_batch.into_inner()?))
            .map_err(|_| {
                tracing::error!(?ids, "Unable to index document");
                ArunaError::DatabaseError("Unable to index document".to_string())
            })?;
        // Add the batch to the reader
        let (builder, error) = builder.add_documents(reader)?;
        error.map_err(|e| {
            tracing::error!(?ids, ?e, "Error adding document");
            ArunaError::DatabaseError("Error adding document".to_string())
        })?;

        // Execute the indexing
        builder.execute()?;

        let mut result = Vec::new();
        for (id, variant) in ids {
            // Get the idx of the node
            let milli_idx = self
                .get_idx_from_ulid(&id, wtxn)
                .ok_or_else(|| ArunaError::DatabaseError("Missing idx".to_string()))?;

            // Add the node to the graph
            let _index = wtxn.add_node(milli_idx, variant);

            // Ensure that the index in graph and milli stays in sync
            // assert_eq!(index.index() as u32, idx);

            result.push((id, milli_idx));
        }

        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self, wtxn))]
    pub fn register_event(
        &self,
        wtxn: &mut WriteTxn<'_>,
        event_id: u128,
        affected: &[u32],
    ) -> Result<(), ArunaError> {
        for idx in affected {
            self.events
                .put(wtxn.get_txn(), idx, &event_id)
                .inspect_err(logerr!())?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_encoding_key(&self) -> Result<(u32, EncodingKey, [u8; 32]), ArunaError> {
        let rtxn = self.read_txn()?;

        let signing_info = self
            .single_entry_database
            .remap_types::<Str, SigningInfoCodec>()
            .get(&rtxn.get_ro_txn(), single_entry_names::SIGNING_KEYS)
            .inspect_err(logerr!())
            .expect("Signing info not found")
            .expect("Signing info not found");
        rtxn.commit()?;
        Ok((signing_info.0, signing_info.1, signing_info.2))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_issuer_info(
        &self,
        issuer_name: String,
        key_id: String,
    ) -> Option<(IssuerType, String, DecodingKey, [u8; 32], Vec<String>)> {
        let read_txn = self.read_txn().ok()?;

        let issuers = self
            .single_entry_database
            .remap_types::<Str, SerdeBincode<Vec<IssuerKey>>>()
            .get(&read_txn.get_ro_txn(), single_entry_names::ISSUER_KEYS)
            .inspect_err(logerr!())
            .ok()??;

        read_txn.commit().ok()?;
        issuers
            .into_iter()
            .find(|issuer| issuer.key_id == key_id && issuer.issuer_name == issuer_name)
            .map(|issuer| {
                (
                    issuer.issuer_type,
                    issuer.issuer_name,
                    issuer.decoding_key,
                    issuer.x25519_pubkey,
                    issuer.audiences,
                )
            })
    }
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_raw_relations(
        &self,
        idx: MilliIdx,
        filter: Option<&[EdgeType]>,
        direction: Direction,
    ) -> Result<Vec<RawRelation>, ArunaError> {
        GraphTxn {
            state: self.graph.read().expect("Poisoned lock"),
            mode: Mode::ReadTxn,
        }
        .get_relations(idx, filter, direction)
    }

    #[tracing::instrument(level = "trace", skip(self, txn))]
    pub fn get_relations<'a>(
        &self,
        idx: MilliIdx,
        filter: Option<&[EdgeType]>,
        direction: Direction,
        txn: &impl Txn<'a>,
    ) -> Result<Vec<Relation>, ArunaError> {
        let graph_txn = txn.get_ro_graph();

        let relations = graph_txn.get_relations(idx, filter, direction)?;

        let mut result = Vec::new();
        for raw_relation in relations {
            let relation = match direction {
                Direction::Outgoing => Relation {
                    from_id: self
                        .get_ulid_from_idx(&raw_relation.source, txn)
                        .ok_or_else(|| ArunaError::NotFound("Index not found".to_string()))?,
                    to_id: self
                        .get_ulid_from_idx(&raw_relation.target, txn)
                        .ok_or_else(|| ArunaError::NotFound("Index not found".to_string()))?,
                    relation_type: self
                        .relation_infos
                        .get(&txn.get_ro_txn(), &raw_relation.edge_type)?
                        .ok_or_else(|| ArunaError::NotFound("Edge type not found".to_string()))?
                        .forward_type,
                },
                Direction::Incoming => Relation {
                    from_id: self
                        .get_ulid_from_idx(&raw_relation.source, txn)
                        .ok_or_else(|| ArunaError::NotFound("Index not found".to_string()))?,
                    to_id: self
                        .get_ulid_from_idx(&raw_relation.target, txn)
                        .ok_or_else(|| ArunaError::NotFound("Index not found".to_string()))?,
                    relation_type: self
                        .relation_infos
                        .get(&txn.get_ro_txn(), &raw_relation.edge_type)?
                        .ok_or_else(|| ArunaError::NotFound("Edge type not found".to_string()))?
                        .backward_type,
                },
            };
            result.push(relation);
        }

        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self, txn))]
    pub fn get_relation_range<'a>(
        &self,
        idx: MilliIdx,
        filter: Option<&[EdgeType]>,
        direction: Direction,
        range: RelationRange,
        txn: &impl Txn<'a>,
    ) -> Result<(Vec<Relation>, u32), ArunaError> {
        let graph_txn = txn.get_ro_graph();

        let (relations, last_entry) =
            graph_txn.get_relation_range(idx, filter, direction, range)?;

        let mut result = Vec::new();
        for raw_relation in relations {
            let relation = match direction {
                Direction::Outgoing => Relation {
                    from_id: self
                        .get_ulid_from_idx(&raw_relation.source, txn)
                        .ok_or_else(|| ArunaError::NotFound("Index not found".to_string()))?,
                    to_id: self
                        .get_ulid_from_idx(&raw_relation.target, txn)
                        .ok_or_else(|| ArunaError::NotFound("Index not found".to_string()))?,
                    relation_type: self
                        .relation_infos
                        .get(&txn.get_ro_txn(), &raw_relation.edge_type)?
                        .ok_or_else(|| ArunaError::NotFound("Edge type not found".to_string()))?
                        .forward_type,
                },
                Direction::Incoming => Relation {
                    from_id: self
                        .get_ulid_from_idx(&raw_relation.source, txn)
                        .ok_or_else(|| ArunaError::NotFound("Index not found".to_string()))?,
                    to_id: self
                        .get_ulid_from_idx(&raw_relation.target, txn)
                        .ok_or_else(|| ArunaError::NotFound("Index not found".to_string()))?,
                    relation_type: self
                        .relation_infos
                        .get(&txn.get_ro_txn(), &raw_relation.edge_type)?
                        .ok_or_else(|| ArunaError::NotFound("Edge type not found".to_string()))?
                        .backward_type,
                },
            };
            result.push(relation);
        }

        Ok((result, last_entry))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_permissions(
        &self,
        resource: &Ulid,
        constraint: Option<&Ulid>,
        user: &Ulid,
    ) -> Result<Permission, ArunaError> {
        let rtxn = self.read_txn()?;

        let resource_idx = self.get_idx_from_ulid(resource, &rtxn).ok_or_else(|| {
            tracing::error!(?resource, "From not found");
            ArunaError::Unauthorized
        })?;
        let constraint_idx = constraint
            .map(|c| self.get_idx_from_ulid(c, &rtxn))
            .flatten();
        let user_idx = self.get_idx_from_ulid(user, &rtxn).ok_or_else(|| {
            tracing::error!("To not found");
            ArunaError::Unauthorized
        })?;

        let graph = rtxn.get_ro_graph();
        let result = graph.get_permissions(resource_idx, user_idx, constraint_idx)?;

        drop(graph);

        rtxn.commit()?;

        Ok(result)
    }

    /// Returns the token
    #[tracing::instrument(level = "trace", skip(self, txn))]
    pub fn get_token<'a>(
        &self,
        requester_id: &Ulid,
        token_idx: u16,
        txn: &impl Txn<'a>,
    ) -> Result<Token, ArunaError> {
        // Get the internal idx of the token
        let requester_internal_idx = self.get_idx_from_ulid_validate(
            &requester_id,
            "requester",
            &[NodeVariant::User, NodeVariant::ServiceAccount],
            txn,
        )?;

        let Some(tokens) = self
            .tokens
            .get(&txn.get_ro_txn(), &requester_internal_idx.0)
            .inspect_err(logerr!())?
        else {
            tracing::error!("No tokens found");
            return Err(ArunaError::Unauthorized);
        };

        let Some(Some(token)) = tokens.get(token_idx as usize) else {
            tracing::error!("Token not found");
            return Err(ArunaError::Unauthorized);
        };

        Ok(token.clone())
    }

    // Returns the group_id of the service account
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_group_from_sa(&self, service_account: &Ulid) -> Result<Ulid, ArunaError> {
        use crate::constants::relation_types::*;
        let read_txn = self.read_txn()?;

        let sa_idx = self
            .get_idx_from_ulid(service_account, &read_txn)
            .ok_or_else(|| {
                tracing::error!("User not found");
                ArunaError::Unauthorized
            })?;

        let graph = self.graph.read().expect("Poisoned lock");
        let sa_graph_idx = *graph
            .idx_mappings
            .milli_graph
            .get(sa_idx.0 as usize)
            .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?;

        for edge in graph
            .graph
            .edges_directed(sa_graph_idx.into(), petgraph::Direction::Outgoing)
        {
            match edge.weight() {
                PERMISSION_NONE..=PERMISSION_ADMIN => {
                    let group_graph_idx = edge.target().as_u32();
                    let group_milli_idx = MilliIdx(
                        *graph
                            .idx_mappings
                            .graph_milli
                            .get(group_graph_idx as usize)
                            .ok_or_else(|| ArunaError::GraphError("Index not found".to_string()))?,
                    );
                    let group = self
                        .get_ulid_from_idx(&group_milli_idx, &read_txn)
                        .ok_or_else(|| {
                            tracing::error!("Group not found");
                            ArunaError::Unauthorized
                        })?;

                    read_txn.commit()?;
                    return Ok(group);
                }
                _ => {}
            }
        }

        read_txn.commit()?;
        tracing::error!("Group not found");
        Err(ArunaError::Unauthorized)
    }

    #[tracing::instrument(level = "trace", skip(self, wtxn))]
    pub fn add_token(
        &self,
        wtxn: &mut WriteTxn,
        event_id: u128,
        user_id: &Ulid,
        mut token: Token,
    ) -> Result<Token, ArunaError> {
        let user_idx = self
            .get_idx_from_ulid(user_id, wtxn)
            .ok_or_else(|| ArunaError::NotFound(user_id.to_string()))?;

        let txn = wtxn.get_txn();
        let mut tokens = self
            .tokens
            .get(txn, &user_idx.0)
            .inspect_err(logerr!())?
            .unwrap_or_default();

        token.id = tokens.len() as u16;

        tokens.push(Some(token));
        self.tokens
            .put(txn, &user_idx.0, &tokens)
            .inspect_err(logerr!())?;

        // Add token creation event to user
        self.events
            .put(txn, &user_idx.0, &event_id)
            .inspect_err(logerr!())?;

        Ok(tokens.pop().flatten().expect("Added token before"))
    }

    #[tracing::instrument(level = "trace", skip(self, txn))]
    pub fn get_tokens<'a>(
        &self,
        txn: &impl Txn<'a>,
        user_id: &Ulid,
    ) -> Result<Vec<Option<Token>>, ArunaError> {
        let user_idx = self
            .get_idx_from_ulid(user_id, txn)
            .ok_or_else(|| ArunaError::NotFound(user_id.to_string()))?;

        Ok(self
            .tokens
            .get(txn.get_ro_txn(), &user_idx.0)
            .inspect_err(logerr!())?
            .unwrap_or_default())
    }

    #[tracing::instrument(level = "trace", skip(self, txn))]
    pub fn search<'a>(
        &self,
        query: String,
        from: usize,
        limit: usize,
        filter: Option<&str>,
        txn: &impl Txn<'a>,
        filter_universe: RoaringBitmap,
    ) -> Result<(usize, Vec<GenericNode>), ArunaError> {
        let rtxn = txn.get_ro_txn();
        let mut universe = self.filtered_universe(filter, txn)?;
        universe &= filter_universe;

        let mut ctx = SearchContext::new(&self.milli_index, &rtxn).inspect_err(logerr!())?;
        let result = execute_search(
            &mut ctx,                                         // Search context
            (!query.trim().is_empty()).then(|| query.trim()), // Query
            TermsMatchingStrategy::Last,                      // Terms matching strategy
            milli::score_details::ScoringStrategy::Skip,      // Scoring strategy
            false,                                            // exhaustive number of hits ?
            universe,                                         // Universe
            &None,                                            // Sort criteria
            &None,                                            // Distinct criteria
            GeoSortStrategy::default(),                       // Geo sort strategy
            from,                                             // From (for pagination)
            limit,                                            // Limit (for pagination)
            None,                                             // Words limit
            &mut DefaultSearchLogger,                         // Search logger
            &mut DefaultSearchLogger,                         // Search logger
            TimeBudget::max(),                                // Time budget
            None,                                             // Ranking score threshold
            None,                                             // Locales (Languages)
        )
        .inspect_err(logerr!())?;

        Ok((
            result.candidates.len() as usize,
            self.milli_index
                .documents(&rtxn, result.documents_ids)
                .inspect_err(logerr!())?
                .into_iter()
                .map(|(_idx, obkv)| {
                    // TODO: More efficient conversion for found nodes
                    let variant: serde_json::Number =
                        serde_json::from_slice(obkv.get(1).expect("Obkv variant key not found"))
                            .inspect_err(logerr!())?;
                    let variant: NodeVariant = variant.try_into().inspect_err(logerr!())?;

                    Ok(match variant {
                        NodeVariant::ResourceProject
                        | NodeVariant::ResourceFolder
                        | NodeVariant::ResourceObject => {
                            GenericNode::Resource(Resource::try_from(obkv).inspect_err(logerr!())?)
                        }
                        NodeVariant::User => {
                            GenericNode::User(User::try_from(obkv).inspect_err(logerr!())?)
                        }
                        NodeVariant::ServiceAccount => GenericNode::ServiceAccount(
                            ServiceAccount::try_from(obkv).inspect_err(logerr!())?,
                        ),
                        NodeVariant::Group => {
                            GenericNode::Group(Group::try_from(obkv).inspect_err(logerr!())?)
                        }
                        NodeVariant::Realm => {
                            GenericNode::Realm(Realm::try_from(obkv).inspect_err(logerr!())?)
                        }
                        NodeVariant::Component => GenericNode::Component(
                            Component::try_from(obkv).inspect_err(logerr!())?,
                        ),
                        NodeVariant::License => {
                            GenericNode::License(License::try_from(obkv).inspect_err(logerr!())?)
                        }
                    })
                })
                .collect::<Result<Vec<_>, ArunaError>>()?,
        ))
    }

    // This is a lower level function that only returns the filtered universe
    // We can use this to check for generic "exists" queries
    // For full search results use the search function
    #[tracing::instrument(level = "trace", skip(self, txn))]
    pub fn filtered_universe<'a>(
        &self,
        filter: Option<&str>,
        txn: &impl Txn<'a>,
    ) -> Result<RoaringBitmap, ArunaError> {
        let filter = if let Some(filter) = filter {
            Filter::from_str(filter).inspect_err(logerr!())?
        } else {
            None
        };

        Ok(
            filtered_universe(&self.milli_index, txn.get_ro_txn(), &filter)
                .inspect_err(logerr!())?,
        )
    }

    #[tracing::instrument(level = "trace", skip(self, wtxn))]
    pub fn add_public_resources_universe(
        &self,
        wtxn: &mut WriteTxn,
        universe: &[u32],
    ) -> Result<(), ArunaError> {
        let mut universe = RoaringBitmap::from_iter(universe.iter().copied());

        let existing = self
            .single_entry_database
            .remap_types::<Str, CboRoaringBitmapCodec>()
            .get(&wtxn.get_txn(), single_entry_names::PUBLIC_RESOURCES)
            .inspect_err(logerr!())?
            .unwrap_or_default();

        // Union of the newly added universe and the existing universe
        universe |= existing;

        self.single_entry_database
            .remap_types::<Str, CboRoaringBitmapCodec>()
            .put(
                wtxn.get_txn(),
                single_entry_names::PUBLIC_RESOURCES,
                &universe,
            )
            .inspect_err(logerr!())?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, wtxn))]
    pub fn add_user_universe(&self, wtxn: &mut WriteTxn, user: u32) -> Result<(), ArunaError> {
        let mut universe = RoaringBitmap::new();
        universe.insert(user);

        let existing = self
            .single_entry_database
            .remap_types::<Str, CboRoaringBitmapCodec>()
            .get(&wtxn.get_txn(), single_entry_names::SEARCHABLE_USERS)
            .inspect_err(logerr!())?
            .unwrap_or_default();

        // Union of the newly added user and the existing universe
        universe |= existing;

        self.single_entry_database
            .remap_types::<Str, CboRoaringBitmapCodec>()
            .put(
                wtxn.get_txn(),
                single_entry_names::SEARCHABLE_USERS,
                &universe,
            )
            .inspect_err(logerr!())?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, wtxn))]
    pub fn add_read_permission_universe(
        &self,
        wtxn: &mut WriteTxn,
        target: u32, // Group, User or Realm
        universe: &[u32],
    ) -> Result<(), ArunaError> {
        let mut universe = RoaringBitmap::from_iter(universe.iter().copied());

        let existing = self
            .read_permissions
            .get(&wtxn.get_txn(), &target)
            .inspect_err(logerr!())?
            .unwrap_or_default();

        // Union of the newly added user and the existing universe
        universe |= existing;

        self.read_permissions
            .put(wtxn.get_txn(), &target, &universe)
            .inspect_err(logerr!())?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, txn))]
    pub fn get_read_permission_universe<'a>(
        &self,
        txn: &impl Txn<'a>,
        read_resources: &[u32],
    ) -> Result<RoaringBitmap, ArunaError> {
        let mut universe = RoaringBitmap::new();
        for read_resource in read_resources {
            universe |= self
                .read_permissions
                .get(txn.get_ro_txn(), read_resource)
                .inspect_err(logerr!())?
                .unwrap_or_default();
        }
        Ok(universe)
    }

    #[tracing::instrument(level = "trace", skip(self, txn))]
    pub fn get_public_universe<'a>(&self, txn: &impl Txn<'a>) -> Result<RoaringBitmap, ArunaError> {
        Ok(self
            .single_entry_database
            .remap_types::<Str, CboRoaringBitmapCodec>()
            .get(&txn.get_ro_txn(), single_entry_names::PUBLIC_RESOURCES)
            .inspect_err(logerr!())?
            .unwrap_or_default())
    }

    #[tracing::instrument(level = "trace", skip(self, txn))]
    pub fn get_user_universe<'a>(&self, txn: &impl Txn<'a>) -> Result<RoaringBitmap, ArunaError> {
        Ok(self
            .single_entry_database
            .remap_types::<Str, CboRoaringBitmapCodec>()
            .get(txn.get_ro_txn(), single_entry_names::SEARCHABLE_USERS)
            .inspect_err(logerr!())?
            .unwrap_or_default())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_realm_and_groups(&self, user_idx: MilliIdx) -> Result<Vec<MilliIdx>, ArunaError> {
        GraphTxn {
            state: self.graph.read().expect("RWLock poison error"),
            mode: Mode::ReadTxn,
        }
        .get_realm_and_groups(user_idx)
    }

    #[tracing::instrument(level = "trace", skip(self, txn))]
    pub fn get_relation_info<'a>(
        &self,
        relation_idx: &u32,
        txn: &impl Txn<'a>,
    ) -> Result<Option<RelationInfo>, ArunaError> {
        let relation_info = self
            .relation_infos
            .get(txn.get_ro_txn(), relation_idx)
            .inspect_err(logerr!())?;
        Ok(relation_info)
    }

    #[tracing::instrument(level = "trace", skip(self, txn))]
    pub fn get_relation_infos<'a>(
        &self,
        txn: &impl Txn<'a>,
    ) -> Result<Vec<RelationInfo>, ArunaError> {
        let relation_info = self
            .relation_infos
            .iter(txn.get_ro_txn())
            .inspect_err(logerr!())?
            .filter_map(|a| Some(a.ok()?.1))
            .collect();
        Ok(relation_info)
    }

    #[tracing::instrument(level = "trace", skip(self, wtxn))]
    pub fn add_component_key(
        &self,
        wtxn: &mut WriteTxn,
        component_idx: MilliIdx,
        pubkey: String,
    ) -> Result<(), ArunaError> {
        let issuer_single_entry_db = self
            .single_entry_database
            .remap_types::<Str, SerdeBincode<Vec<IssuerKey>>>();

        let mut entries = issuer_single_entry_db
            .get(&wtxn.get_ro_txn(), single_entry_names::ISSUER_KEYS)
            .inspect_err(logerr!())?;

        let component = self
            .get_node::<Component>(wtxn, component_idx)
            .ok_or_else(|| ArunaError::NotFound(format!("{}", component_idx.0)))?;

        let (decoding_key, x25519_pubkey) = pubkey_from_pem(&pubkey).inspect_err(logerr!())?;

        let key = IssuerKey {
            key_id: component.id.to_string(),
            issuer_name: component.id.to_string(),
            issuer_endpoint: None,
            issuer_type: IssuerType::DATAPROXY,
            decoding_key,
            x25519_pubkey, // OIDC does not have a x25519 key
            audiences: vec!["aruna".to_string()],
        };

        match entries {
            Some(ref current_keys) if current_keys.contains(&key) => {}
            Some(ref mut current_keys) if !current_keys.contains(&key) => {
                current_keys.push(key);
                issuer_single_entry_db
                    .put(
                        &mut wtxn.get_txn(),
                        single_entry_names::ISSUER_KEYS,
                        &current_keys,
                    )
                    .inspect_err(logerr!())?;
            }
            _ => {
                // TODO: This should not happen at this stage right?
                issuer_single_entry_db
                    .put(
                        &mut wtxn.get_txn(),
                        single_entry_names::ISSUER_KEYS,
                        &vec![key],
                    )
                    .inspect_err(logerr!())?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, wtxn, keys))]
    pub fn add_issuer(
        &self,
        wtxn: &mut WriteTxn,
        issuer_name: String,
        issuer_endpoint: String,
        audiences: Vec<String>,
        keys: (Vec<(String, DecodingKey)>, NaiveDateTime),
    ) -> Result<(), ArunaError> {
        let mut wtxn = wtxn.get_txn();
        let issuer_single_entry_db = self
            .single_entry_database
            .remap_types::<Str, SerdeBincode<Vec<IssuerKey>>>();

        let mut entries = issuer_single_entry_db
            .get(&wtxn, single_entry_names::ISSUER_KEYS)
            .inspect_err(logerr!())?;

        for key in keys.0.into_iter().map(|(key_id, decoding_key)| IssuerKey {
            key_id,
            issuer_name: issuer_name.clone(),
            issuer_endpoint: Some(issuer_endpoint.clone()),
            issuer_type: IssuerType::OIDC,
            decoding_key,
            x25519_pubkey: [0; 32], // OIDC does not have a x25519 key
            audiences: audiences.clone(),
        }) {
            match entries {
                Some(ref current_keys) if current_keys.contains(&key) => {
                    continue;
                }
                Some(ref mut current_keys) if !current_keys.contains(&key) => {
                    current_keys.push(key);
                    issuer_single_entry_db
                        .put(&mut wtxn, single_entry_names::ISSUER_KEYS, &current_keys)
                        .inspect_err(logerr!())?;
                }
                _ => {
                    // TODO: This should not happen at this stage right?
                    issuer_single_entry_db
                        .put(&mut wtxn, single_entry_names::ISSUER_KEYS, &vec![key])
                        .inspect_err(logerr!())?;
                }
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, wtxn))]
    pub fn add_oidc_mapping(
        &self,
        wtxn: &mut WriteTxn,
        user_idx: MilliIdx,
        oidc_mapping: (String, String), // (oidc_id, issuer_name)
    ) -> Result<(), ArunaError> {
        let mut wtxn = wtxn.get_txn();
        self.oidc_mappings
            .put(&mut wtxn, &oidc_mapping, &user_idx.0)
            .inspect_err(logerr!())?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, oidc_mapping))]
    pub fn get_user_by_oidc(
        &self,
        oidc_mapping: (String, String), // (oidc_id, issuer_name)
    ) -> Result<Requester, ArunaError> {
        let read_txn = self.read_txn()?;

        let user_idx = if let Some(user_idx) = self
            .oidc_mappings
            .get(&read_txn.get_ro_txn(), &oidc_mapping)
            .inspect_err(logerr!())?
        {
            MilliIdx(user_idx)
        } else {
            return Ok(Requester::Unregistered {
                oidc_subject: oidc_mapping.0,
                oidc_realm: oidc_mapping.1,
            });
        };

        let user: User = self
            .get_node(&read_txn, user_idx)
            .ok_or_else(|| ArunaError::NotFound(format!("{}", user_idx.0)))?;

        read_txn.commit()?;

        Ok(Requester::User {
            user_id: user.id,
            auth_method: crate::transactions::request::AuthMethod::Oidc {
                oidc_realm: oidc_mapping.1,
                oidc_subject: oidc_mapping.0,
            },
            impersonated_by: None,
        })
    }

    #[tracing::instrument(level = "trace", skip(self, txn))]
    pub fn get_realms_for_user<'a>(
        &self,
        txn: &impl Txn<'a>,
        user: Ulid,
    ) -> Result<Vec<Realm>, ArunaError> {
        let graph = txn.get_ro_graph();
        let user_idx = self
            .get_idx_from_ulid(&user, txn)
            .ok_or_else(|| ArunaError::NotFound("User not found".to_string()))?;
        let realm_idxs = graph.get_realms(user_idx)?;
        let mut realms = Vec::new();
        for realm in realm_idxs {
            realms.push(self.get_node(txn, realm).expect("Database error"));
        }
        Ok(realms)
    }

    #[tracing::instrument(level = "trace", skip(self, txn))]
    pub fn get_subscribers<'a>(&self, txn: &impl Txn<'a>) -> Result<Vec<Subscriber>, ArunaError> {
        let db = self
            .single_entry_database
            .remap_types::<Str, SerdeBincode<Vec<Subscriber>>>();

        let subscribers = db
            .get(txn.get_ro_txn(), single_entry_names::SUBSCRIBER_CONFIG)
            .inspect_err(logerr!())?;

        Ok(subscribers.unwrap_or_default())
    }

    #[tracing::instrument(level = "trace", skip(self, wtxn))]
    pub fn add_subscriber(
        &self,
        wtxn: &mut WriteTxn,
        subscriber: Subscriber,
    ) -> Result<(), ArunaError> {
        let mut wtxn = wtxn.get_txn();
        let db = self
            .single_entry_database
            .remap_types::<Str, SerdeBincode<Vec<Subscriber>>>();

        let mut subscribers = db
            .get(&wtxn, single_entry_names::SUBSCRIBER_CONFIG)
            .inspect_err(logerr!())?
            .unwrap_or_default();

        subscribers.push(subscriber);

        db.put(
            &mut wtxn,
            single_entry_names::SUBSCRIBER_CONFIG,
            &subscribers,
        )
        .inspect_err(logerr!())?;

        Ok(())
    }

    // Adds the event to all subscribers that are interested in the target_ids
    // #[tracing::instrument(level = "trace", skip(self, wtxn))]
    // pub fn add_event_to_subscribers(
    //     &self,
    //     wtxn: &mut WriteTxn,
    //     event_id: u128,
    //     target_idxs: &[u32], // The target indexes that the event is related to
    // ) -> Result<(), ArunaError> {
    //     let mut wtxn = wtxn.get_txn();

    //     let subscribers_db = self
    //         .single_entry_database
    //         .remap_types::<Str, SerdeBincode<Vec<Subscriber>>>();
    //     let all_subscribers = subscribers_db
    //         .get(&wtxn, single_entry_names::SUBSCRIBER_CONFIG)
    //         .inspect_err(logerr!())?
    //         .unwrap_or_default();

    //     all_subscribers
    //         .into_iter()
    //         .filter(|s| target_idxs.contains(&s.target_idx))
    //         .try_for_each(|s| {
    //             let mut subscribers = self
    //                 .subscribers
    //                 .get(&wtxn, &s.id.0)
    //                 .inspect_err(logerr!())?
    //                 .unwrap_or_default();
    //             subscribers.push(event_id);
    //             self.subscribers
    //                 .put(&mut wtxn, &s.id.0, &subscribers)
    //                 .inspect_err(logerr!())?;
    //             Ok::<_, ArunaError>(())
    //         })?;

    //     Ok(())
    // }

    // This can also be used to acknowledge events
    #[tracing::instrument(level = "trace", skip(self, wtxn))]
    pub fn get_events_subscriber(
        &self,
        wtxn: &mut WriteTxn,
        subscriber_id: u128,
        acknowledge_to: Option<u128>,
    ) -> Result<Vec<u128>, ArunaError> {
        let mut wtxn = wtxn.get_txn();

        let Some(mut events) = self
            .subscribers
            .get(&wtxn, &subscriber_id)
            .inspect_err(logerr!())?
        else {
            return Ok(Vec::new());
        };

        if let Some(drain_till) = acknowledge_to {
            if let Some(event) = events.iter().position(|e| *e == drain_till) {
                events.drain(0..=event);
                self.subscribers
                    .put(&mut wtxn, &subscriber_id, &events)
                    .inspect_err(logerr!())?;
            };
        }

        Ok(events)
    }

    #[tracing::instrument(level = "trace", skip(self, wtxn))]
    pub fn create_relation_variant(
        &self,
        wtxn: &mut WriteTxn,
        info: RelationInfo,
    ) -> Result<(), ArunaError> {
        self.relation_infos
            .put(wtxn.get_txn(), &info.idx, &info)
            .inspect_err(logerr!())?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, wtxn))]
    pub fn update_node_field<'a>(
        &'a self,
        wtxn: &mut WriteTxn<'a>,
        node_id: Ulid,
        mut json_object: serde_json::Map<String, serde_json::Value>,
    ) -> Result<(), ArunaError> {
        let indexer_config = IndexerConfig::default();
        let mut documents_config = IndexDocumentsConfig::default();
        documents_config.update_method = IndexDocumentsMethod::UpdateDocuments;
        let builder = IndexDocuments::new(
            wtxn.get_txn(),
            &self.milli_index,
            &indexer_config,
            documents_config,
            |_| (),
            || false,
        )?;

        json_object.insert(
            "id".to_string(),
            serde_json::Value::String(node_id.to_string()),
        );

        // Create a document batch
        let mut documents_batch = DocumentsBatchBuilder::new(Vec::new());
        // Add the json object to the batch
        documents_batch.append_json_object(&json_object)?;
        // Create a reader for the batch
        let reader = DocumentsBatchReader::from_reader(Cursor::new(documents_batch.into_inner()?))
            .map_err(|_| {
                tracing::error!(?node_id, "Unable to index document");
                ArunaError::DatabaseError("Unable to index document".to_string())
            })?;
        // Add the batch to the reader
        let (builder, error) = builder.add_documents(reader)?;
        error.map_err(|e| {
            tracing::error!(?node_id, ?e, "Error adding document");
            ArunaError::DatabaseError("Error adding document".to_string())
        })?;

        // Execute the indexing
        builder.execute()?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, wtxn))]
    pub fn delete_nodes<'a>(
        &'a self,
        wtxn: &mut WriteTxn<'a>,
        idxs: &[u32],
    ) -> Result<(), ArunaError> {
        // Collect ids
        let mut ids = Vec::new();
        for idx in idxs {
            let raw_node = self
                .get_raw_node(wtxn, MilliIdx(*idx))
                .ok_or_else(|| ArunaError::ServerError("Idx did not match any id".to_string()))?;

            let mut obkv = FieldIterator::new(&raw_node);
            let id: Ulid = obkv.get_required_field(0)?;
            let variant: u8 = obkv.get_required_field(1)?;
            ids.push((id, variant));
        }
        // Indexer setup
        let indexer_config = IndexerConfig::default();
        let mut documents_config = IndexDocumentsConfig::default();

        // Replace instead of update
        documents_config.update_method = IndexDocumentsMethod::ReplaceDocuments;
        let builder = IndexDocuments::new(
            wtxn.get_txn(),
            &self.milli_index,
            &indexer_config,
            documents_config,
            |_| (),
            || false,
        )?;

        // Create a document batch
        let mut documents_batch = DocumentsBatchBuilder::new(Vec::new());

        // Replace every document with id and deleted
        for (id, variant) in ids {
            let mut json_object = serde_json::Map::new();
            json_object.insert(
                ID_FIELD.to_string(),
                serde_json::Value::String(id.to_string()),
            );
            json_object.insert(
                VARIANT_FIELD.to_string(),
                serde_json::Value::Number(variant.into()),
            );
            json_object.insert(DELETED_FIELD.to_string(), serde_json::Value::Bool(true));
            documents_batch.append_json_object(&json_object)?;
        }

        // Create a reader for the batch
        let reader = DocumentsBatchReader::from_reader(Cursor::new(documents_batch.into_inner()?))
            .map_err(|_| {
                tracing::error!("Unable to delete documents");
                ArunaError::DatabaseError("Unable to delete documents".to_string())
            })?;

        // Add the batch to the reader
        let (builder, error) = builder.add_documents(reader)?;
        error.map_err(|e| {
            tracing::error!(?e, "Error deleting documents");
            ArunaError::DatabaseError("Error deleting documents".to_string())
        })?;

        // Execute the indexing
        builder.execute()?;

        Ok(())
    }
}
