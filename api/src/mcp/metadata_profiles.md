# Metadata Profiles

A Profile is an ordinary Aruna metadata document that additionally declares the
type `http://www.w3.org/ns/dx/prof/Profile` on its root entity and carries its
SHACL shapes as Turtle. A Dataset that tags a Profile it may use (a public one, or a group-only Profile of its own group) is validated
against that Profile's exact revision before the write is accepted.

## Registering a Profile

A Profile RO-Crate publishes each shapes artifact as an entity with
`encodingFormat` `text/turtle` and the Turtle source in `text`:

```json
{
  "@id": "#shapes",
  "@type": "File",
  "name": "Profile shapes",
  "encodingFormat": "text/turtle",
  "text": "@prefix sh: <http://www.w3.org/ns/shacl#> . ..."
}
```

Every Turtle artifact of one Profile is compiled into a single shapes graph.

## Referring to a Profile

Two IRI forms address the same registered Profile and pin the same revision:

| Form   | Template                              |
| ------ | ------------------------------------- |
| Public | `https://w3id.org/aruna/profile/{id}` |
| Legacy | `https://w3id.org/aruna/{id}`         |

A Dataset opts in by naming one of them in the root entity's `conformsTo`. An
RO-Crate specification IRI such as `https://w3id.org/ro/crate/1.3` and the
RO-Crate community profiles under `https://w3id.org/ro/wfrun/` and
`https://w3id.org/workflowhub/workflow-ro-crate/` are never Profile tags.
Exactly one Profile tag is supported; more than one is rejected.

## Validation on writes

`POST /metadata` and `PUT /metadata/{document_id}/rocrate` validate a tagged
submission before any event is written. A violation rejects the write and
leaves the previous revision untouched. The resulting status is committed in
the same transaction as the accepted revision, so it is always bound to one
Dataset revision and one Profile revision.

Findings carry `severity`: only `violation` rejects a write. `warning` and
`info` findings are stored with a valid status.

## Status, revalidation, and preview

| Route                                                  | Purpose                                                     |
| ------------------------------------------------------ | ----------------------------------------------------------- |
| `GET /metadata/profile/validation/capabilities`         | Evaluator identity and the exact supported constraint set.   |
| `GET /metadata/{document_id}/profile/validation`        | The revision-bound status stored with the current revision.  |
| `POST /metadata/{document_id}/profile/validation/revalidate` | Re-runs validation against the Profile's current revision. |
| `POST /metadata/profile/validation/preview`             | The verdict a write would enforce for an unsaved draft.      |

A status becomes `stale` when either the Dataset revision or the registered
Profile revision changes; revalidation repins it.

Preview stores nothing. Its request body is `{"rocrate": <JSON-LD object>}` and
its response reports `accepted`, `state`, the Profile identity, `findings`, and
`structural_violations`. `accepted` is true only when the structural RO-Crate
rules and the Profile constraints would both let a write through.

## Supported SHACL

Shapes are compiled and executed by craqle's native SHACL Core Subset v1 engine.

* Targets: `sh:targetClass`, `sh:targetNode`, `sh:targetSubjectsOf`,
  `sh:targetObjectsOf`, and implicit class targets.
* Paths: predicate, `sh:inversePath`, sequence, `sh:alternativePath`,
  `sh:zeroOrOnePath`, `sh:zeroOrMorePath`, `sh:oneOrMorePath`.
* Constraints: `sh:class`, `sh:datatype`, `sh:nodeKind`, `sh:minCount`,
  `sh:maxCount`, `sh:minExclusive`, `sh:minInclusive`, `sh:maxExclusive`,
  `sh:maxInclusive`, `sh:minLength`, `sh:maxLength`, `sh:pattern`, `sh:flags`,
  `sh:uniqueLang`, `sh:languageIn`, `sh:equals`, `sh:disjoint`, `sh:lessThan`,
  `sh:lessThanOrEquals`, `sh:or`, `sh:and`, `sh:not`, `sh:xone`, `sh:node`,
  `sh:hasValue`, `sh:in`, `sh:qualifiedValueShape`, `sh:qualifiedMinCount`,
  `sh:qualifiedMaxCount`, `sh:qualifiedValueShapesDisjoint`, `sh:closed`,
  `sh:ignoredProperties`, `sh:severity`, `sh:deactivated`.
* Annotations: `sh:message`, `sh:name`, `sh:description`, `sh:order`,
  `sh:group`.

`sh:class` is exact `rdf:type` membership. No RDFS or OWL inference is applied,
so an instance of a subclass does not satisfy a superclass constraint.

### Crate-local references

Shapes may address the crate root relatively: a reference to the crate base
itself, for example `<>` under `@base <arcp://name,aruna-portal/crate/>`, is
bound to the root entity of the validated document.

A node shape that names no `sh:target*` at all is also bound to the crate root,
so a Profile can constrain the root entity without knowing its minted IRI.

Other crate-local ids, such as `<#person-1>` or `<./data/file.csv>`, cannot be
compiled and fail closed with the `crate_local_reference` rule.

Findings report terms in crate-local form: the root entity is `./`, other
crate-local entities keep their `#fragment` or `./path` id, and everything else
is reported as its plain IRI.

## Fail-closed rules

Anything outside the supported set fails closed with an
`unsupported_constraint` finding that names the construct: SHACL-SPARQL,
SHACL-JS, SHACL-AF, custom components and targets, recursive shapes, RDF-star
terms, remote or disabled `owl:imports`, an unparseable Turtle artifact, and
ill-formed shapes. Such a finding rejects the write and marks the status
`incomplete`, so a partially evaluated Profile never reads as valid.

## Limits and error semantics

| Code                     | HTTP | Retry | Meaning                                              |
| ------------------------ | ---- | ----- | ---------------------------------------------------- |
| `constraint_violation`   | 400  | no    | The document does not satisfy a Profile constraint.  |
| `unsupported_constraint` | 400  | no    | The Profile uses a construct outside the subset.     |
| `validation_limit`       | 400  | no    | A validation budget was exhausted.                   |
| `profile_not_registered` | 400  | no    | The tagged IRI is not a registered Profile, or names a group-only Profile of another group.          |
| `profile_unavailable`    | 503  | yes   | The Profile or its revision is temporarily missing.  |
| `validator_unavailable`  | 503  | yes   | The evaluator is disabled or temporarily unusable.   |

Evaluation is bounded by a result limit, a path-edge budget, and a path-depth
budget. Exceeding one returns a permanent `validation_limit` finding with
`incomplete` completeness rather than a partial verdict, so an over-large
Profile or document never yields a silently truncated pass.

A 503 carries `Retry-After`. Import and job pipelines treat 503 codes as
retryable and every other code as a permanent failure.
