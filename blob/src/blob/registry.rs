use crate::error::BlobLibError;
use crate::opendal::init_operator;
use aruna_core::errors::BlobError;
use aruna_core::structs::{
    Backend, BackendCatalog, BackendConfig, BackendRef, BlobTimeoutConfig, NodeBackendsConfig,
    NodeRouting, NodeRoutingRule, ResolvedBackend, Status,
};
use opendal::Operator;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// One operator-registered backend plus its own health state.
#[derive(Clone, Debug)]
pub struct NodeBackend {
    pub config: BackendConfig,
    pub class: Option<String>,
    pub status: Arc<RwLock<Status>>,
}

impl NodeBackend {
    pub fn new(config: BackendConfig, class: Option<String>) -> Self {
        Self {
            config,
            class,
            status: Arc::new(RwLock::new(Status::Unavailable)),
        }
    }
}

/// The node's registered backends. Immutable for the life of the process, so a
/// stored `BackendRef` always resolves to the same operator or to a loud error.
#[derive(Clone, Debug)]
pub struct BackendRegistry {
    node: Arc<BTreeMap<String, NodeBackend>>,
    default_name: String,
    rules: Arc<[NodeRoutingRule]>,
    serve_group_backends: bool,
}

impl BackendRegistry {
    /// The zero-config shape: one unnamed-class backend under the default name.
    pub fn single(config: BackendConfig) -> Self {
        let mut node = BTreeMap::new();
        node.insert(
            BackendRef::DEFAULT_NODE_NAME.to_string(),
            NodeBackend::new(config, None),
        );
        Self {
            node: Arc::new(node),
            default_name: BackendRef::DEFAULT_NODE_NAME.to_string(),
            rules: Arc::from([]),
            serve_group_backends: true,
        }
    }

    /// Builds the registry from the node's parsed backends file.
    pub fn from_config(config: &NodeBackendsConfig) -> Result<Self, BlobLibError> {
        let node = config
            .backends
            .iter()
            .map(|entry| {
                (
                    entry.name.clone(),
                    NodeBackend::new(entry.config.clone(), entry.class.clone()),
                )
            })
            .collect();
        let registry = Self::new(node, config.default_name.clone())?;
        Ok(Self {
            rules: Arc::from(config.rules.clone()),
            serve_group_backends: config.serve_group_backends,
            ..registry
        })
    }

    pub fn new(
        node: BTreeMap<String, NodeBackend>,
        default_name: String,
    ) -> Result<Self, BlobLibError> {
        if !node.contains_key(&default_name) {
            return Err(BlobLibError::IoError(std::io::Error::other(format!(
                "default backend `{default_name}` is not registered"
            ))));
        }
        Ok(Self {
            node: Arc::new(node),
            default_name,
            rules: Arc::from([]),
            serve_group_backends: true,
        })
    }

    pub fn default_name(&self) -> &str {
        &self.default_name
    }

    pub fn default_ref(&self) -> BackendRef {
        BackendRef::Node(self.default_name.clone())
    }

    /// Hidden and job blobs always land here, so their stamp is self-describing
    /// without ever consulting a routing rule.
    pub fn default_resolved(&self) -> ResolvedBackend {
        let class = self
            .node
            .get(&self.default_name)
            .and_then(|backend| backend.class.clone());
        ResolvedBackend::new(self.default_ref(), class)
    }

    pub fn default_config(&self) -> &BackendConfig {
        &self
            .node
            .get(&self.default_name)
            .expect("registry always holds its default backend")
            .config
    }

    pub fn timeouts(&self) -> BlobTimeoutConfig {
        self.default_config().timeouts
    }

    pub fn entries(&self) -> impl Iterator<Item = (&String, &NodeBackend)> {
        self.node.iter()
    }

    pub fn backend(&self, backend: &BackendRef) -> Result<&NodeBackend, BlobError> {
        match backend {
            BackendRef::Node(name) => self.node.get(name),
            BackendRef::Group(_) => None,
        }
        .ok_or_else(|| BlobError::UnknownBackend(backend.to_string()))
    }

    pub fn config_for(&self, backend: &BackendRef) -> Result<&BackendConfig, BlobError> {
        Ok(&self.backend(backend)?.config)
    }

    /// Builds the operator a stored record resolves to. Replaces rebuilding it
    /// from one global config, which stranded objects whenever that changed.
    pub fn operator_for(
        &self,
        backend: &BackendRef,
        root: &str,
        bucket: &str,
    ) -> Result<Operator, BlobError> {
        let entry = self.backend(backend)?;
        let mut config = entry.config.service_config.clone();
        config.insert("root".to_string(), root.to_string());
        if entry.config.backend_type == Backend::S3 {
            config.insert("bucket".to_string(), bucket.to_string());
        }
        init_operator(entry.config.backend_type.clone(), config)
    }

    pub fn bucket_operator(
        &self,
        backend: &BackendRef,
        bucket: &str,
    ) -> Result<Operator, BlobError> {
        let root = self.config_for(backend)?.root.clone();
        self.operator_for(backend, &root, bucket)
    }

    /// Credential-free view handed to operations so they can turn a routing
    /// class into a concrete backend without touching the adapter.
    pub fn catalog(&self) -> BackendCatalog {
        let catalog = self.node.iter().fold(
            BackendCatalog::new(self.default_name.clone()),
            |catalog, (name, backend)| catalog.with_backend(name.clone(), backend.class.clone()),
        );
        if self.serve_group_backends {
            catalog
        } else {
            catalog.without_group_egress()
        }
    }

    /// The node-wide routing inputs. Operations resolve against this snapshot
    /// synchronously; the adapter itself never chooses a backend.
    pub fn routing(&self) -> NodeRouting {
        NodeRouting {
            rules: self.rules.to_vec(),
            catalog: self.catalog(),
        }
    }
}
