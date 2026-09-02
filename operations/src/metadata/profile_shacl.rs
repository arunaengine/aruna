//! Server-side SHACL evaluation for registered metadata Profiles.
//!
//! Shapes and candidate documents live in a dedicated craqle store that is
//! never committed to: documents are only prepared, so production graph
//! listing, replication, and search never observe a validation run.

use std::collections::{BTreeSet, VecDeque};
use std::path::Path;
use std::sync::{Mutex, PoisonError};

use craqle::{
    AllowAllAuthorizer, CompiledRoCratePolicy, CraqleError, CraqleErrorKind,
    CraqleFjallPersistMode, CraqleNode, CraqleOptions, CrateViolation, EncodedTerm, GraphId,
    MaterializedQuadChange, PrepareRoCrateOptions, PreparedRoCrateDocument, RoCratePolicyOptions,
    RoCrateVersion, SearchStorage, ShaclCompileOptions, ShaclError, ShaclValidationResult,
};
use oxrdf::{NamedNode, NamedOrBlankNode, Term};
use oxttl::TurtleParser;
use tracing::warn;

/// Every candidate is prepared against one scratch graph that never exists, so
/// the encoded crate root is the same constant for every document and shapes
/// can bind it statically.
pub(crate) const VALIDATION_GRAPH_IRI: &str = "https://craqle.invalid/validation/document";
/// Base craqle resolves relative RO-Crate ids against while parsing JSON-LD.
const CRAQLE_BASE_IRI: &str = "https://craqle.invalid/";
/// Legacy base the portal writes into generated Profile shapes.
const PORTAL_BASE_IRI: &str = "arcp://name,aruna-portal/crate/";

const SH: &str = "http://www.w3.org/ns/shacl#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDFS_CLASS: &str = "http://www.w3.org/2000/01/rdf-schema#Class";
const OWL_CLASS: &str = "http://www.w3.org/2002/07/owl#Class";
const OWL_IMPORTS: &str = "http://www.w3.org/2002/07/owl#imports";
const SHAPES_RULE: &str = "shacl_shapes";
const CRATE_LOCAL_RULE: &str = "crate_local_reference";
const TARGET_PREDICATES: &[&str] = &[
    "targetClass",
    "targetNode",
    "targetSubjectsOf",
    "targetObjectsOf",
];

/// Shapes graphs kept warm. The oldest is dropped with its compiled policies.
const MAX_CACHED_PROFILES: usize = 64;

type ShapeTriple = (EncodedTerm, EncodedTerm, EncodedTerm);

/// One Profile revision and the Turtle shapes it publishes.
#[derive(Clone, Debug)]
pub(crate) struct ProfileShapes {
    /// Graph the shapes are installed under, unique per Profile revision.
    pub graph_iri: String,
    pub sources: Vec<String>,
}

/// Structural RO-Crate findings and SHACL results for one candidate document.
#[derive(Debug, Default)]
pub(crate) struct ProfileShaclReport {
    pub results: Vec<ShaclValidationResult>,
    pub structural: Vec<CrateViolation>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProfileShaclError {
    /// A permanent property of the registered Profile: fail closed.
    #[error("{message}")]
    Unsupported { rule: String, message: String },
    #[error("{message}")]
    Limit { message: String },
    #[error("{message}")]
    Unavailable { message: String },
    #[error("{message}")]
    InvalidInput { message: String },
}

struct CachedProfile {
    graph: GraphId,
    policies: Vec<(RoCrateVersion, CompiledRoCratePolicy)>,
}

pub(crate) struct ProfileShaclEngine {
    node: CraqleNode,
    cached: Mutex<VecDeque<CachedProfile>>,
}

impl ProfileShaclEngine {
    /// Opens the validation store. Its contents are derived from registered
    /// Profiles, so a previous directory is discarded instead of recovered.
    pub(crate) fn open(path: &Path) -> Result<Self, CraqleError> {
        if path.exists() {
            std::fs::remove_dir_all(path)?;
        }
        let options = CraqleOptions::new()
            .with_search_storage(SearchStorage::Memory)
            .with_graph_store_persist_mode(CraqleFjallPersistMode::Buffer);
        Ok(Self {
            node: CraqleNode::open_with_options(path, options)?,
            cached: Mutex::new(VecDeque::new()),
        })
    }

    /// Structural RO-Crate findings only, for a document with no Profile tag.
    pub(crate) fn structural(
        &self,
        jsonld: &str,
    ) -> Result<Vec<CrateViolation>, ProfileShaclError> {
        Ok(self.prepare(jsonld)?.structural_findings().to_vec())
    }

    pub(crate) fn evaluate(
        &self,
        profile: &ProfileShapes,
        jsonld: &str,
    ) -> Result<ProfileShaclReport, ProfileShaclError> {
        let mut cached = self.cached.lock().unwrap_or_else(PoisonError::into_inner);
        let index = self.install_shapes(&mut cached, profile)?;
        let prepared = self.prepare(jsonld)?;
        let policy = self.compiled_policy(&mut cached, index, prepared.detected_version)?;
        let report = self
            .node
            .evaluate_rocrate_policy(
                &AllowAllAuthorizer,
                &prepared,
                &policy,
                &RoCratePolicyOptions::default(),
            )
            .map_err(|error| classify(&error))?;
        Ok(ProfileShaclReport {
            results: report.shacl.results,
            structural: report.rocrate_violations,
        })
    }

    fn prepare(&self, jsonld: &str) -> Result<PreparedRoCrateDocument, ProfileShaclError> {
        self.node
            .prepare_rocrate_document(
                &AllowAllAuthorizer,
                &GraphId::new(VALIDATION_GRAPH_IRI),
                jsonld,
                &PrepareRoCrateOptions::default(),
            )
            .map_err(|error| ProfileShaclError::InvalidInput {
                message: error.to_string(),
            })
    }

    fn install_shapes(
        &self,
        cached: &mut VecDeque<CachedProfile>,
        profile: &ProfileShapes,
    ) -> Result<usize, ProfileShaclError> {
        let graph = GraphId::new(&profile.graph_iri);
        if let Some(index) = cached.iter().position(|entry| entry.graph == graph) {
            return Ok(index);
        }
        while cached.len() >= MAX_CACHED_PROFILES {
            let Some(evicted) = cached.pop_front() else {
                break;
            };
            if let Err(error) = self.node.delete_graph(&AllowAllAuthorizer, &evicted.graph) {
                warn!(error = %error, graph = %evicted.graph, "Evicting a cached Profile shapes graph failed");
            }
        }
        let present = self
            .node
            .contains_graph(&graph)
            .map_err(|error| classify(&error))?;
        if !present {
            let changes = shapes_changes(&graph, &profile.sources)?;
            self.node
                .apply_changes(&AllowAllAuthorizer, &graph, changes)
                .map_err(|error| classify(&error))?;
        }
        cached.push_back(CachedProfile {
            graph,
            policies: Vec::new(),
        });
        Ok(cached.len() - 1)
    }

    fn compiled_policy(
        &self,
        cached: &mut VecDeque<CachedProfile>,
        index: usize,
        version: RoCrateVersion,
    ) -> Result<CompiledRoCratePolicy, ProfileShaclError> {
        let entry = cached
            .get_mut(index)
            .ok_or_else(|| ProfileShaclError::Unavailable {
                message: "the cached Profile shapes graph disappeared".to_string(),
            })?;
        if let Some((_, policy)) = entry
            .policies
            .iter()
            .find(|(cached_version, _)| *cached_version == version)
        {
            return Ok(policy.clone());
        }
        let policy = self
            .node
            .compile_rocrate_policy(
                &AllowAllAuthorizer,
                &entry.graph,
                &ShaclCompileOptions {
                    rocrate_version: version,
                    allow_local_imports: false,
                },
            )
            .map_err(|error| classify(&error))?;
        entry.policies.push((version, policy.clone()));
        Ok(policy)
    }
}

fn shapes_changes(
    graph: &GraphId,
    sources: &[String],
) -> Result<Vec<MaterializedQuadChange>, ProfileShaclError> {
    let mut triples = Vec::new();
    for (source, turtle) in sources.iter().enumerate() {
        let parser = TurtleParser::new()
            .with_base_iri(CRAQLE_BASE_IRI)
            .map_err(|error| ProfileShaclError::Unavailable {
                message: error.to_string(),
            })?;
        for triple in parser.for_slice(turtle.as_bytes()) {
            let triple = triple.map_err(|error| ProfileShaclError::Unsupported {
                rule: SHAPES_RULE.to_string(),
                message: format!("the registered Profile's SHACL could not be parsed: {error}"),
            })?;
            triples.push((
                subject_term(&triple.subject, source)?,
                EncodedTerm::from_named_node(&triple.predicate),
                object_term(&triple.object, source)?,
            ));
        }
    }
    let target = shapes_term(&format!("{SH}targetNode"));
    let root = shapes_term(VALIDATION_GRAPH_IRI);
    for shape in root_shapes(&triples) {
        triples.push((shape, target.clone(), root.clone()));
    }
    Ok(triples
        .into_iter()
        .map(
            |(subject, predicate, object)| MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject,
                predicate,
                object,
            },
        )
        .collect())
}

/// Node shapes that name no target are bound to the crate root, so a Profile
/// can constrain the root entity without knowing its minted IRI.
fn root_shapes(triples: &[ShapeTriple]) -> Vec<EncodedTerm> {
    let node_shape = shapes_term(&format!("{SH}NodeShape")).0;
    let mut candidates = BTreeSet::new();
    let mut excluded = BTreeSet::new();
    for (subject, predicate, object) in triples {
        let Some(predicate) = iri_value(&predicate.0) else {
            continue;
        };
        if predicate == RDF_TYPE {
            if object.0 == node_shape {
                candidates.insert(subject.0.clone());
            }
            if matches!(iri_value(&object.0), Some(RDFS_CLASS | OWL_CLASS)) {
                excluded.insert(subject.0.clone());
            }
            continue;
        }
        if predicate == RDF_FIRST {
            excluded.insert(object.0.clone());
            continue;
        }
        let Some(local) = predicate.strip_prefix(SH) else {
            continue;
        };
        // A shape reached through another shape is never root-targeted.
        excluded.insert(object.0.clone());
        match local {
            "property" => {
                candidates.insert(subject.0.clone());
            }
            "path" => {
                excluded.insert(subject.0.clone());
            }
            local if TARGET_PREDICATES.contains(&local) => {
                excluded.insert(subject.0.clone());
            }
            _ => {}
        }
    }
    candidates
        .difference(&excluded)
        .map(|shape| EncodedTerm(shape.clone()))
        .collect()
}

fn iri_value(term: &str) -> Option<&str> {
    term.strip_prefix('<')
        .and_then(|rest| rest.strip_suffix('>'))
}

/// Rewrites a shape's reference to the crate base into the encoded crate root.
///
/// Craqle stores every other crate-local id in relative form, which its shapes
/// compiler rejects, so such a reference fails closed instead of silently
/// matching nothing.
fn shapes_iri(node: &NamedNode) -> Result<EncodedTerm, ProfileShaclError> {
    let iri = node.as_str();
    let relative = iri
        .strip_prefix(CRAQLE_BASE_IRI)
        .or_else(|| iri.strip_prefix(PORTAL_BASE_IRI));
    let mapped = match relative {
        None => iri,
        Some("") => VALIDATION_GRAPH_IRI,
        Some(_) => {
            return Err(ProfileShaclError::Unsupported {
                rule: CRATE_LOCAL_RULE.to_string(),
                message: format!(
                    "the registered Profile references crate-local id `{iri}`; only the crate root can be referenced"
                ),
            });
        }
    };
    Ok(EncodedTerm::from_named_node(&NamedNode::new_unchecked(
        mapped,
    )))
}

/// Infallible form for the constants this module builds itself.
fn shapes_term(iri: &str) -> EncodedTerm {
    EncodedTerm::from_named_node(&NamedNode::new_unchecked(iri))
}

/// Blank node labels are scoped to their source document: two Turtle artifacts
/// of one Profile may reuse the same label for unrelated shapes.
fn scoped_blank(label: &str, source: usize) -> EncodedTerm {
    EncodedTerm(format!("_:s{source}x{label}"))
}

fn subject_term(
    subject: &NamedOrBlankNode,
    source: usize,
) -> Result<EncodedTerm, ProfileShaclError> {
    match subject {
        NamedOrBlankNode::NamedNode(node) => shapes_iri(node),
        NamedOrBlankNode::BlankNode(node) => Ok(scoped_blank(node.as_str(), source)),
    }
}

fn object_term(object: &Term, source: usize) -> Result<EncodedTerm, ProfileShaclError> {
    match object {
        Term::NamedNode(node) => shapes_iri(node),
        Term::BlankNode(node) => Ok(scoped_blank(node.as_str(), source)),
        Term::Literal(literal) => Ok(EncodedTerm::from_literal(literal)),
        Term::Triple(_) => Err(ProfileShaclError::Unsupported {
            rule: "rdf_star_term".to_string(),
            message: "RDF-star terms are unsupported in registered Profile shapes".to_string(),
        }),
    }
}

/// Splits craqle failures into permanent Profile defects, exceeded validation
/// budgets, and retryable backend conditions.
fn classify(error: &CraqleError) -> ProfileShaclError {
    if let CraqleError::Shacl(shacl) = error
        && let Some(rule) = permanent_rule(shacl)
    {
        return ProfileShaclError::Unsupported {
            rule,
            message: error.to_string(),
        };
    }
    match error.kind() {
        CraqleErrorKind::Unsupported => ProfileShaclError::Unsupported {
            rule: SHAPES_RULE.to_string(),
            message: error.to_string(),
        },
        CraqleErrorKind::ValidationLimit => ProfileShaclError::Limit {
            message: error.to_string(),
        },
        _ => ProfileShaclError::Unavailable {
            message: error.to_string(),
        },
    }
}

/// The rule naming a shapes defect no retry can repair, or `None` when the
/// failure is retryable.
fn permanent_rule(error: &ShaclError) -> Option<String> {
    match error {
        ShaclError::UnsupportedComponent { component, .. } => Some(component.clone()),
        ShaclError::ImportsDisabled { .. }
        | ShaclError::ImportNotLocal { .. }
        | ShaclError::ImportCycle { .. } => Some(OWL_IMPORTS.to_string()),
        ShaclError::InvalidPattern { .. } => Some(format!("{SH}pattern")),
        ShaclError::UnsupportedRecursiveShape { .. }
        | ShaclError::UnsupportedRdfStarTerm { .. }
        | ShaclError::CyclicShapeEvaluation { .. }
        | ShaclError::IllFormedShapes { .. } => Some(SHAPES_RULE.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn triples(turtle: &str) -> Vec<ShapeTriple> {
        let parser = TurtleParser::new().with_base_iri(CRAQLE_BASE_IRI).unwrap();
        parser
            .for_slice(turtle.as_bytes())
            .map(|triple| {
                let triple = triple.unwrap();
                (
                    subject_term(&triple.subject, 0).unwrap(),
                    EncodedTerm::from_named_node(&triple.predicate),
                    object_term(&triple.object, 0).unwrap(),
                )
            })
            .collect()
    }

    #[test]
    fn scopes_blank_labels() {
        assert_eq!(scoped_blank("b0", 0).0, "_:s0xb0");
        assert_ne!(scoped_blank("b0", 0).0, scoped_blank("b0", 1).0);
    }

    #[test]
    fn maps_crate_base() {
        let map = |iri: &str| shapes_iri(&NamedNode::new_unchecked(iri));
        assert_eq!(
            map(CRAQLE_BASE_IRI).unwrap().0,
            format!("<{VALIDATION_GRAPH_IRI}>")
        );
        assert_eq!(
            map(PORTAL_BASE_IRI).unwrap().0,
            format!("<{VALIDATION_GRAPH_IRI}>")
        );
        assert_eq!(
            map("http://schema.org/name").unwrap().0,
            "<http://schema.org/name>"
        );
        for local in [
            format!("{PORTAL_BASE_IRI}#person-1"),
            format!("{CRAQLE_BASE_IRI}data/file.csv"),
        ] {
            assert!(matches!(
                map(&local),
                Err(ProfileShaclError::Unsupported { ref rule, .. }) if rule == CRATE_LOCAL_RULE
            ));
        }
    }

    #[test]
    fn binds_untargeted_shapes() {
        let shapes = triples(
            r#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix schema: <http://schema.org/> .
            <urn:root> a sh:NodeShape ;
              sh:property [ sh:path schema:name ; sh:minCount 1 ] .
            <urn:targeted> a sh:NodeShape ; sh:targetClass schema:Person .
            <urn:nested> a sh:NodeShape ; sh:property [ sh:path schema:age ] .
            <urn:outer> a sh:NodeShape ; sh:targetNode <urn:x> ; sh:node <urn:nested> .
        "#,
        );
        let bound = root_shapes(&shapes)
            .into_iter()
            .map(|shape| shape.0)
            .collect::<Vec<_>>();
        assert_eq!(bound, vec!["<urn:root>".to_string()]);
    }

    #[test]
    fn skips_list_members() {
        let shapes = triples(
            r#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix schema: <http://schema.org/> .
            <urn:root> a sh:NodeShape ;
              sh:or ( [ a sh:NodeShape ; sh:property [ sh:path schema:name ] ]
                      [ a sh:NodeShape ; sh:property [ sh:path schema:title ] ] ) .
        "#,
        );
        assert_eq!(root_shapes(&shapes).len(), 1);
    }

    #[test]
    fn classifies_shapes_defects() {
        let unsupported = CraqleError::Shacl(ShaclError::UnsupportedComponent {
            shape: "urn:shape".to_string(),
            component: "http://www.w3.org/ns/shacl#SPARQLConstraintComponent".to_string(),
        });
        assert!(matches!(
            classify(&unsupported),
            ProfileShaclError::Unsupported { rule, .. }
                if rule == "http://www.w3.org/ns/shacl#SPARQLConstraintComponent"
        ));
        let limit = CraqleError::Shacl(ShaclError::ResultLimitExceeded { limit: 10 });
        assert!(matches!(classify(&limit), ProfileShaclError::Limit { .. }));
        let cancelled = CraqleError::Shacl(ShaclError::ValidationCancelled);
        assert!(matches!(
            classify(&cancelled),
            ProfileShaclError::Unavailable { .. }
        ));
    }
}
