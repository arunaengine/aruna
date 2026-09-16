# Repository style and structure

**The location explains the domain. The name explains the action or concept. The code exposes sequence and ownership. Comments explain reasons that are not apparent from the code.**

This root `STYLE.md` is the canonical policy. `CONTRIBUTING.md`, the source guide, and agent instructions should reference it instead of maintaining separate copies.

Apply this policy to repository-owned production code, tests, helpers, scripts, source assets, and development tooling. Preserve behavior and external contracts during every naming or layout change. Generated code, third-party material, required interface names, and mandatory notices require narrow, explicit exceptions.

## 1. Group shared prefixes

**Three or more related sibling source files that require the same meaningful domain prefix must move into the corresponding submodule.** Move the repeated context into the module name and remove it from child filenames. This rule is mandatory, not a suggestion.

| Before | After |
| --- | --- |
| compute_config.rs | compute/config.rs |
| compute_backend.rs | compute/backend.rs |
| compute_tests.rs | compute/tests.rs |
| Parent declares separate files | Parent declares compute; compute/mod.rs declares children |

For Rust code, use a normal module declaration file such as `compute/mod.rs`; it does not count toward the threshold. Include an existing owning file such as `compute.rs` in the move where appropriate, without retaining a wrapper solely to preserve the old layout.

### Precedence over the folder minimum

**The three-file shared-prefix rule takes precedence over the general five-file folder minimum.** A three- or four-file `compute/` family is therefore valid and required. Do not add artificial files to reach five, and do not hoist the family again because it has fewer than five children.

Count real, maintained files in one ownership scope, including related test files. Match a shared domain, not an accidental sequence of letters or a generic action such as `get_`. Do not combine unrelated code across crates or production and integration-test boundaries just to reach the count.

One or two prefixed files do not trigger this rule. A file and its single test companion remain siblings. Remove the prefix only where the remaining name stays accurate, and do not rename files into synonyms to evade grouping.

## 2. Limit names to three terms

**Every repository-owned identifier and source filename stem must contain at most three terms.** This covers functions, methods, tests, structs, enums, variants, traits, type aliases, fields, variables, parameters, constants, statics, macros, and modules.

### Count consistently

Underscores and language-appropriate word separators divide terms. CamelCase and PascalCase word boundaries also divide terms. An established acronym counts as one term. Ignore a filename extension; do not use digit suffixes, joined words, unusual casing, or removed separators to evade the limit.

| Name | Terms | Decision |
| --- | --- | --- |
| read_bucket | 2 | Allowed |
| HttpBodyLimit | 3 | Allowed |
| API_TOKEN_ID | 3 | Allowed |
| max_http_body_size | 4 | Rename |
| ScheduleTimerIfIdle | 4 | Rename |
| bootstrap_tests | 2 | Allowed |

Count each identifier independently. In `metadata::CreateRequest`, the module has one term and the type has two. Use the language's normal casing rather than inventing a repository-specific casing convention.

### Let the module supply context

```rust
metadata::CreateRequest
bucket::GetInfo
session::State
```

Prefer these qualified names to types that repeat the entire domain in every identifier. Keep qualification at use sites where a short imported name would be ambiguous. Do not expose many unrelated `State`, `Input`, or `Error` types through one wildcard facade.

Use complete, familiar words. Avoid invented abbreviations, vague names such as `stuff`, and meaningless numeric suffixes. A name should reveal its actual job: parsing, validation, loading, installation, startup, cancellation, and completion are different actions.

### Name the test contract

```rust
rejects_early_finalize
optional_failure_continues
reclaim_uses_enqueue
```

Put scenario detail in fixed inputs and assertion messages. Split a test only when it checks independent contracts. Do not remove assertions to shorten its name, use generic names such as `works`, or repeat the full enclosing module path in the function name.

Do not create an otherwise unjustified module only to hide an overlong name. A module must own a real responsibility. Required external names and persisted or public strings are handled under Section 11, not silently renamed.

## 3. Give folders a real purpose

**Outside the shared-prefix rule and explicit roots, an optional source folder needs at least five real, related, directly contained files, excluding an optional `mod.rs`.** Five files make a folder eligible, not automatically useful.

| Folder contents | Required treatment |
| --- | --- |
| One file | Hoist it beside its owner. |
| Two to four related files | Hoist or join a genuine larger family, unless Section 1 requires grouping. |
| Three or more shared-prefix files | Keep the corresponding domain submodule, even below five files. |
| Five or more related files | Group when the directory makes ownership clearer. |
| Unrelated files or placeholders | Regroup; they do not justify a folder. |

Planned files, empty placeholders, redundant facades, and arbitrary splits do not count. Subdirectories are not direct files. Count maintained source assets as well as code; a one-file production-asset wrapper is still a one-file wrapper.

Required package/tooling roots, genuine integration-test roots, and the designated `tests/fixtures` asset root are explicit exceptions. Preserve test-target and package boundaries when moving code.

Group real domains before flattening. Do not create an enormous mixed crate root, and do not group unrelated files simply to reach a threshold. Avoid wrapper chains and catch-all homes such as `misc`, `helpers`, or `utils` when a more precise owner exists.

## 4. Keep test files beside their owner

A module with one separate test file should use sibling files, not a singleton test subfolder. Keep substantial tests separate from the implementation when that helps reading.

```text
src/
  bootstrap.rs
  bootstrap_tests.rs
```

The production file can retain its private logical test child while selecting the sibling file explicitly:

```rust
#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod tests;
```

A physical folder is not required for every logical module. Keep small inline tests where useful; do not paste hundreds of test lines into production merely to remove a folder.

A dedicated test folder must satisfy the same folder rules, including the three-file shared-prefix exception. Preserve private test access and the intended unit/integration boundary. Do not widen public APIs just to move tests.

## 5. Keep fixtures shallow and purposeful

**Place fixture assets directly in `tests/fixtures`.** Do not insert generic layers named `data`, `resources`, or `assets`. Use descriptive filenames and keep licenses and provenance beside the corresponding assets.

```text
tests/fixtures/
  apache_table.html
  autoindex_nginx.html
  sample_crate.json
  sample_archive.eln
```

A further folder must represent a genuine family and satisfy the applicable grouping rule. Three related fixture assets with a meaningful shared domain prefix must form that named family; this does not justify restoring a generic `data/` wrapper. Preserve externally fixed asset names where required.

Verify that fixture moves preserve bytes and update every runtime path, compile-time include, test, and documentation reference. Renaming or moving an asset is not permission to regenerate it.

Keep production assets with their production owner. Rust test helpers are code rather than fixture data: place them beside their consumers or in a clear shared test-support owner. Do not maintain forwarding-only fixture namespaces.

## 6. Arrange code for reading

A reader should encounter public purpose and control flow before implementation detail. For an executable, put `main` first after imports and module declarations. Keep one obvious process entry point and name the application function for its role, such as `run_node`.

For an operation or state machine, put inputs, state, and the `start` / `step` / `finalize` / `abort` overview before detailed phase helpers. Retain one visible transition map. For a service, make construction, startup, running behavior, and shutdown easy to locate.

```text
Purpose and public types
Entry points or transition overview
Cohesive implementation sections
Local helpers
Tests or adjacent test-module declarations
```

This is a reading principle, not a rigid template. Do not force the same ordering on subjects that need a different explanation.

Extract a function or module when it names a real responsibility, makes a contract explicit, or reduces what a reader must understand at once. Do not split by line count, add pass-through wrappers, or hide unchanged complexity behind a new filename.

Keep important execution order apparent. Startup, authorization, commit, publication, cleanup, and stream-lifetime decisions must not disappear into vague helpers named `setup`, `process`, or `handle` without useful context.

## 7. Explain reasons, not obvious steps

**An authored implementation comment may contain at most three physical source lines per logical comment.** One line is enough when it explains the point. Apply this to ordinary comments, module comments, and source documentation comments.

Do not evade the limit with adjacent fragments, repeated headings, or extremely wide lines. Put longer rationale and examples in existing documentation and leave a short source reference where useful.

### Remove narration

Delete comments that simply repeat the next statement, restate a type name, or describe an obvious assignment. Improve the name or code first when an explanation exists only because the implementation is unnecessarily confusing.

```rust
// Return the result.
return result;
```

The comment above adds no information. In a time helper, repeating "Get current time" is similarly unhelpful; a non-obvious fallback or time-source requirement may deserve explanation.

### Add the missing reason

Add a short comment when a competent reader could reasonably misunderstand a consequential decision. Explain the constraint or reason at the decision point, not merely what the line does.

| Subject | Explain when not apparent |
| --- | --- |
| Commit uncertainty | Why possibly committed data must survive cleanup. |
| Time and retries | Why this phase supplies the time, and which IDs must remain stable. |
| Shutdown and streams | Why cancellation is not the same as completed resource release. |
| Authorization | Why actor identity and forwarding-peer identity are distinct. |
| Ordering and constants | Why the sequence matters; the units and reason for a limit. |
| Compatibility | Why an apparently redundant representation must remain. |

```rust
// The commit may already be durable if its acknowledgement is lost.
// Preserve the blob until reconciliation establishes the outcome.
```

Keep explanations accurate when behavior changes. A comment promising completion must not describe a function that only signals cancellation. Do not invent a rationale that the implementation and its tests do not support.

### Preserve required information

Do not delete safety obligations, public API descriptions, license text, or provenance to pass a length check. Retain a short accurate source contract and move longer editable material deliberately without losing required published descriptions. Mandatory notices and generated or third-party text need explicit treatment rather than destructive pruning.

## 8. Make dependencies and ownership explicit

A module must own its implementation, not simply forward into the module from which it was supposedly separated. Remove circular responsibility and duplicate implementation homes.

| Responsibility | Required boundary |
| --- | --- |
| Settings | Parse explicit inputs. Keep unrelated storage/network initialization out of parsing. |
| Identity | Own persisted identity types, encodings, and store behavior. |
| Enrollment | Own enrollment decisions and its transport where appropriate. |
| Application actions | Return domain outcomes independently of REST or MCP response formats. |
| Transport adapters | Map requests and outcomes to their own protocol contracts. |
| Background work | Expose admission, cancellation, and completion ownership. |

Use explicit production imports. Do not make child modules depend on an import collection supplied by their parent through `use super::*`. Intentional private unit-test imports may remain.

Re-exports must be deliberate. A small public facade may be useful; migration-only aliases must disappear once callers use canonical paths. Do not maintain competing old and new module maps.

A constructor that starts tasks must make that fact and the completion owner apparent. Distinguish resource construction, installation, startup, cancellation, and waiting. Never discard ownership merely because a cancellation token is available.

## 9. Model meaningful states and outcomes

Use named records and enums instead of large tuples or nested optional tuples with unrelated meanings. Keep the data for an attempt or phase together when that makes ownership and valid states clearer.

```rust
enum SubmitOutcome {
    Stopped,
    Submitted(Submission),
    Reconcile(Attempt),
}
```

Use one meaningful error boundary where possible. Keep `Option` for genuine absence and nested item results where independent item outcomes require them. Do not flatten protocol distinctions simply to make a type shorter.

Incomplete work must not appear as successful absence, and early finalization must not invent default success. Keep typed failure causes until after retry or permanence decisions. Human-readable wording must not control internal behavior when a typed cause is available.

Resource state must be equally honest: stopped admissions, requested cancellation, joined tasks, completed persistence, and successful deletion are different guarantees. Names, outcomes, and comments must describe the actual one.

## 10. Test the real contract

Pure operation and decision tests must use fixed inputs and explicit events. They must not require a node, database, network, runtime, environment mutation, or wall-clock sleeps. Test the production decision, not an unused table or a test-only copy of its logic.

For an operation, start from the real constructor with fixed input, call `start`, inspect relevant effects, provide result events, advance through `step`, and check completion, `finalize`, and `abort`. Test important failure paths and forbidden effects, not only successful traces.

Supply time and IDs at the phase where their meaning belongs. A single operation-start timestamp is not automatically a substitute for enqueue time, retry time, or later observations. Preserve production uniqueness and required retry reuse.

Use explicit start, release, and completion signals for async ownership tests. Arbitrary sleeps or fixed counts of `yield_now()` are not evidence that a particular task reached its required state.

Keep a small boundary suite for actual storage, protocols, scheduling, stream lifetime, and observed task completion. Test the actual caller that adds a timeout or cancellation boundary, not just the lower-level helper in isolation.

Moving or renaming tests must preserve their intended selection. Inspect the compiled full and fast selections, then execute them. A selected name is not an executed test, and a no-I/O test target can still have substantial compilation cost.

## 11. Preserve external names and representations

The naming rule governs internal identifiers. It does not authorize changing established API strings, persisted bytes, configuration names, or required external interfaces.

| Contract | Preserve during internal changes |
| --- | --- |
| Serialization and storage | Field names, enum tags, record encodings, keys, and historical decoding. |
| Public APIs | Schema names, operation IDs, routes, errors, and response formats. |
| Configuration | Environment variables, configuration keys, flags, defaults, and accepted values. |
| Interfaces | Required trait methods and other externally imposed names. |

Use explicit mappings where a shorter internal identifier must retain an external name. Preserve both reading and writing behavior, not merely acceptance of the old spelling. Keep narrow exceptions for names imposed by external interfaces.

Check generated API output and frozen historical fixtures where relevant. A current encoder/decoder round trip alone is not proof of historical compatibility. Migrate internal callers completely rather than keeping long aliases indefinitely.

## 12. Enforce one policy

Use the repository formatter. Avoid manual alignment and decorative source formatting that fights it. Run the style check through normal local and CI commands, with small tests for the checker itself.

The checker must cover every category the policy claims to regulate: names, folder contents including maintained assets, the shared-prefix trigger, comments, and explicit exceptions. Use token-aware inspection where text matching would confuse strings, macros, or comments with declarations.

### Apply folder rules in this order

| Order | Decision |
| --- | --- |
| 1 | Recognize required roots and explicit external constraints. |
| 2 | Find three or more related sibling files with a meaningful shared domain prefix and require grouping. |
| 3 | Accept the resulting three- or four-file domain as an exception to the five-file minimum. |
| 4 | Apply the normal five-file minimum to other optional folders; hoist singletons and unjustified small groups. |
| 5 | Recheck names, paths, test discovery, imports, assets, and public contracts after moves. |

Record the domain identity of a required three-file group in the checker's explicit rule data where needed. After prefixes disappear from child names, validation must not mistake the correctly grouped family for a new violation. Do not exempt all small folders to solve that problem.

Include checker cases for three versus two prefixed files, a valid three-file domain after grouping, singleton test companions, an unrelated shared verb, fields/variants/constants, source assets, and long logical comments. Keep exceptions narrow and reviewable.

Style tooling may inspect source layout and names. Domain tests must not scan source text for filenames, private helpers, or call strings to prove behavior. A passing style gate does not prove correct authorization, persistence, or shutdown.

## 13. Complete moves and verify the result

When applying these rules, establish the final domain grouping before moving files. Remove repeated prefixes within their module, update imports and module paths, migrate callers, preserve intended test selection, and update asset references and documentation in the same work batch.

Use one active writer per file during parallel changes. Finish with one consistent combined tree, not several individually passing branches or stale compatibility facades. Do not repair a broken import by restoring a prohibited wrapper or singleton folder.

Completion requires the style gate, formatting, relevant lint/build checks, inspected and executed tests, applicable feature selections, fixture-byte checks, and public compatibility checks for the final revision. State missing tools or services as blockers. Never equate cancellation with completion, a listing with a test run, or an earlier revision's pass with current evidence.

**A contributor should be able to predict where code belongs, understand names without decoding abbreviations, read the main sequence without chasing wrappers, and find a short explanation wherever the reason is not obvious.**
