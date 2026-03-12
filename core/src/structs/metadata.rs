use std::collections::HashSet;
use std::str::FromStr;

use autosurgeon::{Hydrate, Reconcile, hydrate, reconcile};
use oxrdf::Triple;
use rocraters::ro_crate::context::RoCrateContext;
use rocraters::ro_crate::rdf::{
    ContextResolverBuilder, ConversionOptions, RdfFormat, RdfGraph, rdf_graph_to_rocrate,
    rocrate_to_rdf_with_options,
};
use rocraters::ro_crate::rocrate::RoCrate;
use serde::{Deserialize, Serialize};

use crate::errors::ConversionError;
use crate::structs::Actor;
use crate::types::{GroupId, autosurgeon_ulid};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hydrate, Reconcile)]
pub struct MetadataDocument {
    #[autosurgeon(with = "autosurgeon_ulid")]
    pub document_id: GroupId,
    #[autosurgeon(with = "autosurgeon_ulid")]
    pub group_id: GroupId,
    pub base_iri: String,
    pub context_json: String,
    #[autosurgeon(with = "autosurgeon_triple_set")]
    pub triples: HashSet<String>,
}

impl MetadataDocument {
    pub fn new(group_id: GroupId, document_id: GroupId, base_iri: String) -> Self {
        let context =
            RoCrateContext::ReferenceContext("https://w3id.org/ro/crate/1.2/context".to_string());
        Self {
            document_id,
            group_id,
            base_iri,
            context_json: serde_json::to_string(&context)
                .expect("default RO-Crate context must serialize"),
            triples: HashSet::new(),
        }
    }

    pub fn to_bytes(&self, actor: &Actor) -> Result<Vec<u8>, ConversionError> {
        self.reconcile_bytes(None, actor)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let doc = automerge::AutoCommit::load(bytes)?;
        Ok(hydrate(&doc)?)
    }

    pub fn reconcile_bytes(
        &self,
        current: Option<&[u8]>,
        actor: &Actor,
    ) -> Result<Vec<u8>, ConversionError> {
        let actor = postcard::to_allocvec(actor)?;
        let mut doc = match current {
            Some(bytes) if !bytes.is_empty() => automerge::AutoCommit::load(bytes)?,
            _ => automerge::AutoCommit::new(),
        };
        doc.set_actor((&actor).into());
        reconcile(&mut doc, self)?;
        Ok(doc.save())
    }

    pub fn from_rocrate(
        group_id: GroupId,
        document_id: GroupId,
        crate_: &RoCrate,
        base_iri: String,
    ) -> Result<Self, ConversionError> {
        let options = if base_iri.is_empty() {
            ConversionOptions::AllowRelative
        } else {
            ConversionOptions::with_base(base_iri.clone())
        };
        let graph = rocrate_to_rdf_with_options(crate_, ContextResolverBuilder::default(), options)
            .map_err(|err| ConversionError::RoCrateError(err.to_string()))?;
        let mut metadata = Self::from_rdf_graph(group_id, document_id, graph)?;
        if !base_iri.is_empty() {
            metadata.base_iri = base_iri;
        }
        metadata.context_json = serde_json::to_string(&crate_.context)?;
        Ok(metadata)
    }

    pub fn to_rocrate(&self) -> Result<RoCrate, ConversionError> {
        let graph = self.to_rdf_graph()?;
        rdf_graph_to_rocrate(graph).map_err(|err| ConversionError::RoCrateError(err.to_string()))
    }

    pub fn to_rdf_string(&self, format: RdfFormat) -> Result<String, ConversionError> {
        self.to_rdf_graph()?
            .to_string(format)
            .map_err(|err| ConversionError::RoCrateError(err.to_string()))
    }

    pub fn from_rdf_graph(
        group_id: GroupId,
        document_id: GroupId,
        graph: RdfGraph,
    ) -> Result<Self, ConversionError> {
        let context_json = serde_json::to_string(&graph.context.original)?;
        let base_iri = graph.context.base.clone().unwrap_or_default();
        let mut triples = HashSet::with_capacity(graph.triples.len());
        for triple in graph.triples {
            triples.insert(serialize_triple(&triple)?);
        }

        Ok(Self {
            document_id,
            group_id,
            base_iri,
            context_json,
            triples,
        })
    }

    pub fn to_rdf_graph(&self) -> Result<RdfGraph, ConversionError> {
        let context = self.context()?;
        let mut resolved = ContextResolverBuilder::default()
            .resolve(&context)
            .map_err(|err| ConversionError::RoCrateError(err.to_string()))?;
        if resolved.base.is_none() && !self.base_iri.is_empty() {
            resolved.base = Some(self.base_iri.clone());
        }

        let mut graph = RdfGraph::new(resolved);
        for triple in &self.triples {
            graph.insert(parse_triple(triple)?);
        }

        Ok(graph)
    }

    pub fn context(&self) -> Result<RoCrateContext, ConversionError> {
        Ok(serde_json::from_str(&self.context_json)?)
    }
}

fn serialize_triple(triple: &Triple) -> Result<String, ConversionError> {
    Ok(triple.to_string())
}

fn parse_triple(value: &str) -> Result<Triple, ConversionError> {
    Triple::from_str(value).map_err(|err| ConversionError::RoCrateError(err.to_string()))
}

pub mod autosurgeon_triple_set {
    use std::collections::{HashMap, HashSet};

    use autosurgeon::reconcile::MapReconciler;
    use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconciler};

    pub fn hydrate<'a, D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'a>,
    ) -> Result<HashSet<String>, HydrateError> {
        let inner: HashMap<String, String> = HashMap::hydrate(doc, obj, prop)?;
        Ok(inner.into_keys().collect())
    }

    pub fn reconcile<R: Reconciler>(
        triples: &HashSet<String>,
        mut reconciler: R,
    ) -> Result<(), R::Error> {
        let mut map = reconciler.map()?;
        map.retain(|triple, _| triples.contains(triple))?;
        for triple in triples {
            map.put(triple, String::new())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::Actor;

    fn sample_document() -> MetadataDocument {
        MetadataDocument {
            document_id: GroupId::new(),
            group_id: GroupId::new(),
            base_iri: String::new(),
            context_json: serde_json::to_string(&RoCrateContext::ReferenceContext(
                "https://w3id.org/ro/crate/1.2/context".to_string(),
            ))
            .unwrap(),
            triples: HashSet::from([
                "<http://example.org/root> <http://schema.org/name> \"example\"".to_string(),
                "<http://example.org/root> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Dataset>".to_string(),
            ]),
        }
    }

    #[test]
    fn metadata_document_roundtrip_bytes() {
        let document = sample_document();

        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            user_id: GroupId::new(),
            realm_id: crate::structs::RealmId([1u8; 32]),
        };

        let bytes = document.to_bytes(&actor).expect("to bytes");
        let restored = MetadataDocument::from_bytes(&bytes).expect("from bytes");
        assert_eq!(document, restored);
    }

    #[test]
    fn metadata_document_roundtrip_rdf_graph() {
        let document = sample_document();
        let graph = document.to_rdf_graph().expect("to graph");
        let restored =
            MetadataDocument::from_rdf_graph(document.group_id, document.document_id, graph)
                .expect("from graph");
        assert_eq!(document.triples, restored.triples);
    }

    #[test]
    fn metadata_document_reconcile_removes_deleted_triples() {
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
            user_id: GroupId::new(),
            realm_id: crate::structs::RealmId([2u8; 32]),
        };

        let original = sample_document();
        let original_bytes = original.to_bytes(&actor).expect("original bytes");

        let mut updated = original.clone();
        updated
            .triples
            .remove("<http://example.org/root> <http://schema.org/name> \"example\"");

        let updated_bytes = updated
            .reconcile_bytes(Some(&original_bytes), &actor)
            .expect("updated bytes");
        let restored = MetadataDocument::from_bytes(&updated_bytes).expect("restored metadata");

        assert_eq!(restored.triples, updated.triples);
    }
}
