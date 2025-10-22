use std::sync::Arc;

use crate::caching::cache::Cache;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::dsls::pub_key_dsl::PubKey;
use crate::database::dsls::rule_dsl::Rule;
use crate::database::dsls::rule_dsl::RuleBinding;
use crate::database::dsls::user_dsl::User;
use crate::database::enums::ObjectStatus;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::generic_resource;
use aruna_rust_api::api::storage::models::v2::GenericResource;
use aruna_rust_api::api::storage::models::v2::Pubkey as APIPubkey;
use aruna_rust_api::api::storage::models::v2::User as APIUser;
use aruna_rust_api::api::storage::services::v2::full_sync_endpoint_response;
use aruna_rust_api::api::storage::services::v2::FullSyncEndpointResponse;
use dashmap::mapref::multiple::RefMulti;
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use jsonwebtoken::DecodingKey;

#[derive(Clone)]
pub enum PubKeyEnum {
    DataProxy((String, DecodingKey, DieselUlid)), // DataProxy((Raw Key String, DecodingKey, Endpoint ID))
    Server((String, DecodingKey)), // Server((Key String, DecodingKey)) + ArunaServer ID ?
}

// This is a helper struct for handling GenericResources
#[derive(Clone)]
pub struct ObjectWrapper {
    pub object_with_relations: ObjectWithRelations,
    pub rules: Arc<Vec<RuleBinding>>,
}

#[derive(Clone)]
pub struct CachedRule {
    pub rule: Rule,
    pub compiled: cel_parser::Expression,
}

impl PubKeyEnum {
    pub fn get_key_string(&self) -> String {
        match self {
            PubKeyEnum::DataProxy((k, _, _)) => k.to_string(),
            PubKeyEnum::Server((k, _)) => k.to_string(),
        }
    }

    pub fn get_name(&self) -> String {
        match self {
            PubKeyEnum::DataProxy((_, _, n)) => n.to_string(),
            PubKeyEnum::Server((_, _)) => "".to_string(),
        }
    }
}

impl TryFrom<PubKey> for PubKeyEnum {
    type Error = anyhow::Error;
    fn try_from(pk: PubKey) -> Result<Self> {
        let public_pem = format!(
            "-----BEGIN PUBLIC KEY-----{}-----END PUBLIC KEY-----",
            &pk.pubkey
        );
        let decoding_key = DecodingKey::from_ed_pem(public_pem.as_bytes())?;

        Ok(match pk.proxy {
            Some(proxy) => PubKeyEnum::DataProxy((pk.pubkey.to_string(), decoding_key, proxy)),
            None => PubKeyEnum::Server((pk.pubkey.to_string(), decoding_key)),
        })
    }
}

impl ObjectWithRelations {
    /// Fetches all ids of children which are associated to the object
    /// through an outbound BELONGS_TO relation.
    ///
    /// Returns:
    ///
    /// * `Vec<DieselUlid>`:
    ///   List of all object ids which are associated as a child through a
    ///   BELONGS_TO relation to the specific object.
    pub fn get_children(&self) -> Vec<DieselUlid> {
        self.outbound_belongs_to
            .0
            .iter()
            .map(|x| *x.key())
            .collect::<Vec<_>>()
    }

    /// Fetches all ids of children which are associated to the object
    /// through an outbound BELONGS_TO or DELETED relation.
    ///
    /// Returns:
    ///
    /// * `Vec<DieselUlid>`:
    ///   List of all object ids which are associated as a child through a
    ///   BELONGS_TO or DELETED relation to the specific object.
    pub fn get_permission_children(&self) -> Vec<DieselUlid> {
        // Get all BELONGS_TO children
        let mut object_children = self.get_children();

        // Extend with all DELETED children
        object_children.extend(
            self.outbound
                .0
                .iter()
                .filter_map(|c| {
                    if c.value().relation_name == "DELETED" {
                        Some(c.value().target_pid)
                    } else {
                        None
                    }
                })
                .collect_vec(),
        );

        object_children
    }

    /// Fetches all ids of parents which are associated to the object
    /// through an inbound BELONGS_TO relation.
    ///
    /// Returns:
    ///
    /// * `Vec<DieselUlid>`:
    ///   List of all object ids which are associated as a parent through a
    ///   BELONGS_TO relation to the specific object.
    pub fn get_parents(&self) -> Vec<DieselUlid> {
        self.inbound_belongs_to
            .0
            .iter()
            .map(|x| *x.key())
            .collect::<Vec<_>>()
    }
}

pub struct ProxyCacheIterator<'a> {
    resource_iter:
        Box<dyn Iterator<Item = RefMulti<'a, DieselUlid, ObjectWithRelations>> + 'a + Send + Sync>,
    user_iter: Box<dyn Iterator<Item = RefMulti<'a, DieselUlid, User>> + 'a + Send + Sync>,
    pub_key_iter: Box<dyn Iterator<Item = RefMulti<'a, i16, PubKeyEnum>> + 'a + Send + Sync>,
    endpoint_id: DieselUlid,
    cache: Arc<Cache>,
}

impl<'a> ProxyCacheIterator<'a> {
    pub fn new(
        resource_iter: Box<
            dyn Iterator<Item = RefMulti<'a, DieselUlid, ObjectWithRelations>> + 'a + Send + Sync,
        >,
        user_iter: Box<dyn Iterator<Item = RefMulti<'a, DieselUlid, User>> + 'a + Send + Sync>,
        pub_key_iter: Box<dyn Iterator<Item = RefMulti<'a, i16, PubKeyEnum>> + 'a + Send + Sync>,
        endpoint_id: DieselUlid,
        cache: Arc<Cache>,
    ) -> ProxyCacheIterator<'a> {
        ProxyCacheIterator {
            resource_iter,
            user_iter,
            pub_key_iter,
            endpoint_id,
            cache,
        }
    }
}

impl<'a> Iterator for ProxyCacheIterator<'a> {
    type Item = GrpcProxyInfos;
    fn next(&mut self) -> Option<Self::Item> {
        for res in self.resource_iter.by_ref() {
            let bindings = self.cache.get_rule_bindings(res.key()).unwrap_or_default();
            let res = res.value().clone();
            if res.object.object_status != ObjectStatus::DELETED
                && res.object.endpoints.0.contains_key(&self.endpoint_id)
            {
                return Some(GrpcProxyInfos::Resource(
                    ObjectWrapper {
                        object_with_relations: res,
                        rules: bindings,
                    }
                    .into(),
                ));
            }
        }
        for res in self.user_iter.by_ref() {
            let res = res.value();
            if res
                .attributes
                .0
                .trusted_endpoints
                .contains_key(&self.endpoint_id)
            {
                return Some(GrpcProxyInfos::User(res.clone().into()));
            }
        }
        if let Some(pk) = self.pub_key_iter.next() {
            return Some(GrpcProxyInfos::PubKey(APIPubkey {
                id: *pk.key() as i32,
                key: pk.value().get_key_string(),
                location: pk.value().get_name(),
            }));
        }
        None
    }
}

#[derive(Clone)]
pub enum GrpcProxyInfos {
    Resource(generic_resource::Resource),
    User(APIUser),
    PubKey(APIPubkey),
}

impl From<GrpcProxyInfos> for FullSyncEndpointResponse {
    fn from(value: GrpcProxyInfos) -> Self {
        match value {
            GrpcProxyInfos::Resource(r) => FullSyncEndpointResponse {
                target: Some(full_sync_endpoint_response::Target::GenericResource(
                    GenericResource { resource: Some(r) },
                )),
            },
            GrpcProxyInfos::User(u) => FullSyncEndpointResponse {
                target: Some(full_sync_endpoint_response::Target::User(u)),
            },
            GrpcProxyInfos::PubKey(p) => FullSyncEndpointResponse {
                target: Some(full_sync_endpoint_response::Target::Pubkey(p)),
            },
        }
    }
}
