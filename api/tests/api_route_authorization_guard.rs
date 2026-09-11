use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

const ROUTES_DIR: &str = "src/routes";
const METHODS: &[&str] = &[
    "any", "delete", "get", "head", "options", "patch", "post", "put", "trace",
];
/// REST authorization boundaries. `ensure_permission` also matches
/// `ensure_permission_with` and the `metadata` module wrapper around it;
/// `require_owner` is the user-node device plane's owner gate; the operations
/// metadata create entry point runs the single `authorize` boundary inside
/// `aruna-operations`, which this scanner cannot follow.
const BOUNDARY: &[&str] = &[
    "create_metadata_authorized",
    "ensure_permission",
    "permission_granted",
    "require_owner",
    "request_authorization::authorize",
];

/// Routed handlers that reach no REST boundary call, with the reason each one
/// is still authorized. A new route must gain a check or a reviewed entry here.
const ALLOWLIST: &[(&str, &str, &str)] = &[
    (
        "assistant/mod.rs",
        "create_provider",
        "self-scoped: the provider is created with the caller's user id and stored under the caller's owner key",
    ),
    (
        "assistant/mod.rs",
        "delete_chat",
        "self-scoped: DeleteChatOperation keys the chat head by the caller's user id",
    ),
    (
        "assistant/mod.rs",
        "delete_provider",
        "self-scoped: DeleteProviderOperation refuses a provider owned by another user",
    ),
    (
        "assistant/mod.rs",
        "get_models",
        "self-scoped: load_provider requires the provider to belong to the caller",
    ),
    (
        "assistant/mod.rs",
        "get_turns",
        "self-scoped: ReadChatTurnsOperation reads the chat head by the caller's user id",
    ),
    (
        "assistant/mod.rs",
        "list_chats",
        "self-scoped: ListChatHeadsOperation iterates only the caller's own chat heads",
    ),
    (
        "assistant/mod.rs",
        "list_providers",
        "self-scoped: ListProviderOperation lists providers under the caller's owner key",
    ),
    (
        "assistant/mod.rs",
        "patch_provider",
        "self-scoped: load_provider and UpdateProviderOperation both require the caller's user id",
    ),
    (
        "assistant/mod.rs",
        "poll_login",
        "self-scoped: load_provider requires the ChatGPT provider to belong to the caller",
    ),
    (
        "assistant/mod.rs",
        "proxy_get",
        "self-scoped: load_provider requires the provider to belong to the caller",
    ),
    (
        "assistant/mod.rs",
        "proxy_post",
        "self-scoped: load_provider requires the provider to belong to the caller",
    ),
    (
        "assistant/mod.rs",
        "put_chat",
        "self-scoped: WriteChatHeadOperation reads and writes under the caller's user id",
    ),
    (
        "assistant/mod.rs",
        "put_turn",
        "self-scoped: WriteChatTurnOperation keys the turn and head by the caller's user id",
    ),
    (
        "assistant/mod.rs",
        "start_login",
        "self-scoped: the pending ChatGPT provider is created for the caller's user id",
    ),
    (
        "assistant/mod.rs",
        "test_provider",
        "self-scoped: load_provider requires the provider to belong to the caller",
    ),
    (
        "credentials.rs",
        "list_s3_credentials",
        "self-scoped: only credentials whose identity is the caller",
    ),
    (
        "credentials/sessions.rs",
        "list_s3_sessions",
        "self-scoped: ListS3SessionsOperation filters to the caller's user identity",
    ),
    (
        "credentials/sessions.rs",
        "revoke_s3_session",
        "self-scoped: RevokeS3SessionOperation refuses a session of another user",
    ),
    (
        "drs.rs",
        "get_authorizations",
        "public DRS auth-scheme discovery, resolves no object",
    ),
    (
        "drs.rs",
        "get_service_info",
        "public DRS service-info discovery",
    ),
    (
        "group_join.rs",
        "own_joins",
        "self-scoped request list with unrestricted realm auth and deny policies; operation filters by caller",
    ),
    (
        "group_join.rs",
        "submit_join",
        "self-scoped request with unrestricted realm auth and deny policies; operation binds requester to caller",
    ),
    (
        "group_join.rs",
        "withdraw_join",
        "self-scoped withdrawal with unrestricted realm auth and deny policies; operation checks stored requester",
    ),
    (
        "groups.rs",
        "get_group",
        "realm directory read, member-only fields hidden by map_roles_with_visibility",
    ),
    (
        "groups.rs",
        "get_group_usage",
        "group membership checked before any group counter is read",
    ),
    (
        "groups.rs",
        "leave_group",
        "self-scoped: removes only the caller from the group",
    ),
    (
        "groups.rs",
        "list_group_members",
        "group membership checked before the member list is built",
    ),
    (
        "groups.rs",
        "list_groups",
        "realm directory read, member-only fields hidden by build_api_groups",
    ),
    (
        "info.rs",
        "get_realm_info",
        "public realm descriptor, topology added only for realm members",
    ),
    (
        "info.rs",
        "get_usage",
        "realm-wide counters intentionally open to every realm member",
    ),
    (
        "job_audit.rs",
        "get_job_audit",
        "self-scoped: family_report and family_audit answer NotFound unless the \
         caller is the stored submitter of the request",
    ),
    (
        "job_session.rs",
        "end_session",
        "self-scoped: read_owned_job requires the caller to be the creator",
    ),
    (
        "job_session.rs",
        "get_session",
        "self-scoped: read_owned_job requires the caller to be the creator",
    ),
    (
        "job_session.rs",
        "interrupt_session",
        "self-scoped: read_owned_job requires the caller to be the creator",
    ),
    (
        "job_session.rs",
        "list_scratch",
        "self-scoped: read_owned_job requires the caller to be the creator",
    ),
    (
        "job_session.rs",
        "read_scratch",
        "self-scoped: read_owned_job requires the caller to be the creator",
    ),
    (
        "job_session.rs",
        "stage_inputs",
        "self-scoped: read_owned_job requires the caller to be the creator, and \
         copy_object enforces the caller's own read permission on each source",
    ),
    (
        "job_session.rs",
        "stream_session",
        "self-scoped: read_owned_job requires the caller to be the creator",
    ),
    (
        "job_session.rs",
        "submit_cell",
        "self-scoped: read_owned_job requires the caller to be the creator",
    ),
    (
        "jobs.rs",
        "cancel_job",
        "self-scoped: read_owned_job requires the caller to be the creator",
    ),
    (
        "jobs.rs",
        "get_job",
        "self-scoped: read_owned_job requires the caller to be the creator",
    ),
    (
        "jobs.rs",
        "get_job_artifact",
        "self-scoped: read_owned_job requires the caller to be the creator",
    ),
    (
        "jobs.rs",
        "get_job_report",
        "self-scoped: read_owned_job requires the caller to be the creator",
    ),
    (
        "jobs.rs",
        "head_job_artifact",
        "self-scoped: read_owned_job requires the caller to be the creator",
    ),
    (
        "jobs.rs",
        "list_jobs",
        "self-scoped: the job owner index is keyed by the caller",
    ),
    (
        "metadata.rs",
        "export_metadata_rocrate",
        "ensure_record_readable checks READ on the record path",
    ),
    (
        "metadata.rs",
        "get_metadata_document",
        "ensure_record_readable checks READ on the record path",
    ),
    (
        "metadata.rs",
        "get_metadata_path",
        "candidates dropped unless the record is readable",
    ),
    (
        "metadata.rs",
        "get_profile_validation_status",
        "per-document visibility checked inside the routed status operation (GetVisibleMetadataDocumentRequest), forwarded under the caller's token",
    ),
    (
        "metadata.rs",
        "list_all_metadata_documents",
        "records filtered by GroupPermissionRules and policies",
    ),
    (
        "metadata.rs",
        "list_metadata_documents",
        "records filtered by GroupPermissionRules and policies",
    ),
    (
        "metadata.rs",
        "metadata_reference_preflight",
        "realm bearer; per-document visibility checked inside the preflight operation, restricted references surface only as a boolean",
    ),
    (
        "metadata.rs",
        "metadata_references",
        "each backlink filtered by can_read_record",
    ),
    (
        "metadata.rs",
        "profile_validation_capabilities",
        "realm bearer read of static evaluator capabilities; no resource is addressed",
    ),
    (
        "metadata.rs",
        "query_all_metadata",
        "graphs filtered by per-record read visibility",
    ),
    (
        "metadata.rs",
        "query_metadata_document",
        "ensure_record_readable checks READ on the record path",
    ),
    (
        "metadata.rs",
        "revalidate_profile",
        "realm bearer; per-document visibility checked inside the routed status operation (GetVisibleMetadataDocumentRequest), forwarded under the caller's token",
    ),
    (
        "metadata.rs",
        "search_metadata",
        "hits filtered by per-record read visibility",
    ),
    (
        "notifications.rs",
        "delete_watch",
        "self-scoped: the watch key is prefixed with the owner",
    ),
    (
        "notifications.rs",
        "list_notifications",
        "self-scoped: recipient is the caller, records revalidated",
    ),
    (
        "notifications.rs",
        "list_watches",
        "self-scoped: owner is the caller, unreadable watches redacted",
    ),
    (
        "notifications.rs",
        "mark_read",
        "self-scoped: ids intersected with the caller's own notifications",
    ),
    (
        "notifications.rs",
        "stream_notifications",
        "self-scoped: streams only the caller's unread state",
    ),
    (
        "notifications.rs",
        "unread_count",
        "self-scoped: counts only the caller's visible notifications",
    ),
    (
        "oai.rs",
        "handle_oai",
        "public OAI-PMH provider by protocol; enumerates only the anonymous \
         visibility index and re-checks anonymous metadata.read per record",
    ),
    (
        "oai.rs",
        "handle_oai_post",
        "public OAI-PMH provider by protocol; same gate as the GET transport",
    ),
    (
        "onboarding.rs",
        "bootstrap_onboarding",
        "public enrollment gated by the onboarding secret and node proof",
    ),
    (
        "pid.rs",
        "resolve_pid",
        "public w3id landing: the authority re-checks anonymous metadata.read per \
         record and answers 302, 404, or a 410 tombstone",
    ),
    (
        "pid.rs",
        "resolve_profile_pid",
        "anonymous landing resolution that redirects to the rocrate route, which enforces document authorization",
    ),
    (
        "placement.rs",
        "create_placement_policy",
        "realm-admin WRITE checked inside CreatePolicyOperation; a forwarding \
         holder re-runs the same check under the caller's token",
    ),
    (
        "placement.rs",
        "get_placement_coverage",
        "realm-config READ checked inside PolicyCoverageOperation",
    ),
    (
        "placement.rs",
        "get_placement_diagnostics",
        "realm-config READ checked inside PolicyDiagnosticsOperation",
    ),
    (
        "placement.rs",
        "get_placement_policy",
        "realm-bearer read of an immutable replicated policy document every \
         realm node can fetch to evaluate placement",
    ),
    (
        "placement.rs",
        "list_placement_policies",
        "realm-config READ checked inside ListPoliciesOperation",
    ),
    (
        "placement.rs",
        "resolve_placement_quarantine",
        "realm-admin WRITE checked inside ResolveQuarantineOperation",
    ),
    (
        "rocrate_import.rs",
        "upload_rocrate",
        "self-scoped: spools a hidden blob owned by the caller",
    ),
    (
        "search.rs",
        "bucket_search",
        "realm-wide fan-out with no single permission path; candidates filtered \
         by per-bucket READ and policies",
    ),
    (
        "search.rs",
        "object_search",
        "realm bearer; every live head is re-checked against group READ, token path restrictions and request policies inside the search operation",
    ),
    (
        "sessions.rs",
        "create_session",
        "self-scoped: creates a bearer only for the unrestricted caller",
    ),
    (
        "sessions.rs",
        "delete_session",
        "self-scoped: the operation hides records owned by another user",
    ),
    (
        "sessions.rs",
        "list_sessions",
        "self-scoped: the owner index is keyed by the caller",
    ),
    (
        "staging.rs",
        "get_staging_job",
        "self-scoped: read_owned_job requires the caller to be the creator",
    ),
    (
        "staging.rs",
        "list_staging_jobs",
        "self-scoped: the job owner index is keyed by the caller",
    ),
    (
        "sync.rs",
        "get_sync",
        "self-scoped: ensure_creator limits it to the relationship creator",
    ),
    (
        "sync.rs",
        "list_sync",
        "self-scoped: relationships filtered to the caller's own",
    ),
    (
        "tes.rs",
        "cancel_task",
        "self-scoped: read_owned_job requires the caller to be the creator",
    ),
    (
        "tes.rs",
        "get_task",
        "self-scoped: read_owned_job requires the caller to be the creator",
    ),
    (
        "tes.rs",
        "list_tasks",
        "self-scoped: the job owner index is keyed by the caller",
    ),
    (
        "tes.rs",
        "service_info",
        "public GA4GH TES service-info discovery",
    ),
    (
        "users.rs",
        "get_token",
        "self-scoped: the token is minted for the authenticated identity",
    ),
    (
        "users.rs",
        "get_user_info",
        "self-scoped: every read is keyed by the caller's user id",
    ),
    (
        "users.rs",
        "list_user_devices",
        "self-scoped: realm-config nodes and enrollments filtered to the owner \
         carried by the caller's credential",
    ),
    (
        "users.rs",
        "patch_user_info",
        "self-scoped: updates only the caller's own user record",
    ),
    (
        "users.rs",
        "register_user",
        "public OIDC registration, the admin variant needs an onboarding secret",
    ),
    (
        "users.rs",
        "resolve_users",
        "unrestricted realm directory read with policy checks; only user-selected public fields returned",
    ),
    (
        "users.rs",
        "revoke_user_device",
        "self-scoped: RemoveDeviceNodeOperation re-checks that the node is a \
         User device owned by the caller",
    ),
    (
        "users.rs",
        "search_users",
        "unrestricted realm directory read with policy checks; only user-selected public fields match",
    ),
    (
        "users/vault.rs",
        "delete_vault",
        "self-scoped: deletes only the caller's own vault record",
    ),
    (
        "users/vault.rs",
        "get_vault",
        "self-scoped: reads only the caller's own vault record",
    ),
    (
        "users/vault.rs",
        "put_vault",
        "self-scoped: writes only the caller's own vault record",
    ),
];

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Handler {
    module: String,
    name: String,
}

struct Module {
    bodies: BTreeMap<String, String>,
    imports: BTreeMap<String, String>,
    handlers: Vec<String>,
}

#[test]
fn unguarded_routes_allowlisted() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let modules = load_modules(manifest_dir);
    let actual = scan_routes(&modules);
    let allowed = allowlist();

    let unexpected = actual.difference(&allowed).collect::<Vec<_>>();
    let stale = allowed.difference(&actual).collect::<Vec<_>>();

    assert!(
        ALLOWLIST.is_sorted_by_key(|(module, handler, _)| (*module, *handler)),
        "route authorization allowlist must stay sorted by module then handler"
    );
    if !unexpected.is_empty() || !stale.is_empty() {
        panic!(
            "Route authorization coverage drifted. Every routed handler must reach \
             ensure_permission, ensure_permission_with, permission_granted or \
             request_authorization::authorize, directly or through a helper. Add the \
             missing check instead of extending the allowlist unless the route is \
             deliberately public or self-scoped.\n\n\
             Unexpected unguarded handlers:\n{}\nStale allowlist entries:\n{}",
            format_handlers(&unexpected),
            format_handlers(&stale),
        );
    }
}

fn allowlist() -> BTreeSet<Handler> {
    ALLOWLIST
        .iter()
        .map(|(module, name, _)| Handler {
            module: (*module).to_owned(),
            name: (*name).to_owned(),
        })
        .collect()
}

fn scan_routes(modules: &BTreeMap<String, Module>) -> BTreeSet<Handler> {
    let mut unguarded = BTreeSet::new();

    for (name, module) in modules {
        for handler in &module.handlers {
            assert!(
                has_body(modules, name, handler, &mut BTreeSet::new()),
                "routed handler {handler} has no body or import in {name}"
            );
            if !is_guarded(modules, name, handler, &mut BTreeSet::new()) {
                unguarded.insert(Handler {
                    module: name.clone(),
                    name: handler.clone(),
                });
            }
        }
    }

    unguarded
}

/// Proves a routed handler has a definition, following the same resolved
/// import chain `is_guarded` follows before it looks for the boundary.
fn has_body(
    modules: &BTreeMap<String, Module>,
    module: &str,
    name: &str,
    seen: &mut BTreeSet<(String, String)>,
) -> bool {
    if !seen.insert((module.to_owned(), name.to_owned())) {
        return false;
    }
    let Some(current) = modules.get(module) else {
        return false;
    };
    current.bodies.contains_key(name)
        || current
            .imports
            .get(name)
            .is_some_and(|origin| has_body(modules, origin, name, seen))
}

/// A handler is guarded when its own body or any function it can reach inside
/// `src/routes` calls the REST authorization boundary.
fn is_guarded(
    modules: &BTreeMap<String, Module>,
    module: &str,
    name: &str,
    seen: &mut BTreeSet<(String, String)>,
) -> bool {
    if !seen.insert((module.to_owned(), name.to_owned())) {
        return false;
    }
    let Some(current) = modules.get(module) else {
        return false;
    };
    let Some(body) = current.bodies.get(name) else {
        return current
            .imports
            .get(name)
            .is_some_and(|origin| is_guarded(modules, origin, name, seen));
    };
    if BOUNDARY.iter().any(|pattern| body.contains(pattern)) {
        return true;
    }
    called_names(body)
        .iter()
        .any(|called| is_guarded(modules, module, called, seen))
}

fn load_modules(manifest_dir: &Path) -> BTreeMap<String, Module> {
    let routes_dir = manifest_dir.join(ROUTES_DIR);
    let mut files = Vec::new();
    collect_sources(&routes_dir, &mut files);
    files.sort();

    let keys = files
        .iter()
        .filter_map(|path| module_key(&routes_dir, path))
        .collect::<BTreeSet<_>>();

    files
        .iter()
        .filter_map(|path| {
            let key = module_key(&routes_dir, path)?;
            let source = fs::read_to_string(path)
                .unwrap_or_else(|err| panic!("failed to read {path:?}: {err}"));
            let source = strip_tests(&mask_source(&source));
            Some((
                key.clone(),
                Module {
                    bodies: fn_bodies(&source),
                    imports: route_imports(&source, &key, &keys),
                    handlers: router_handlers(&source),
                },
            ))
        })
        .collect()
}

/// The module key is the source path relative to `src/routes`, so nested
/// modules keep their directory and `mod.rs` is a module like any other.
fn module_key(routes_dir: &Path, path: &Path) -> Option<String> {
    let relative = path.strip_prefix(routes_dir).ok()?;
    let parts = relative
        .components()
        .map(|part| part.as_os_str().to_str())
        .collect::<Option<Vec<_>>>()?;
    Some(parts.join("/"))
}

fn collect_sources(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap_or_else(|err| panic!("failed to read {dir:?}: {err}")) {
        let path = entry
            .unwrap_or_else(|err| panic!("failed to read entry in {dir:?}: {err}"))
            .path();

        if path.is_dir() {
            collect_sources(&path, files);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

/// Handler idents registered by the module router, plus a count check so a route
/// form this scanner cannot read fails loudly instead of going unnoticed.
/// Modules register through `routes!`, whose arguments are the routed handlers.
fn router_handlers(source: &str) -> Vec<String> {
    let mut handlers = Vec::new();
    let mut routes = 0usize;

    for start in occurrences(source, "fn router") {
        let Some(body) = block_at(source, start) else {
            continue;
        };
        routes += occurrences(body, ".route(").len();
        for at in occurrences(body, "routes!(") {
            routes += 1;
            let Some(group) = group_at(body, at) else {
                continue;
            };
            for item in group.split(',') {
                let item = item.trim();
                if !item.is_empty() && item.bytes().all(is_ident_byte) {
                    handlers.push(item.to_owned());
                }
            }
        }
        for method in METHODS {
            for at in occurrences(body, method) {
                if at > 0 && is_ident_byte(body.as_bytes()[at - 1]) {
                    continue;
                }
                let rest = &body[at + method.len()..];
                if let Some(handler) = wrapped_ident(rest) {
                    handlers.push(handler);
                }
            }
        }
    }

    assert!(
        handlers.len() >= routes,
        "found {} handlers for {routes} route registrations; a route form is unparsed",
        handlers.len()
    );
    handlers.sort();
    handlers.dedup();
    handlers
}

/// Returns the balanced parenthesis group that follows `from`, without its
/// delimiters.
fn group_at(source: &str, from: usize) -> Option<&str> {
    let bytes = source.as_bytes();
    let open = from + source[from..].find('(')?;
    let mut depth = 0usize;

    for (index, byte) in bytes.iter().enumerate().skip(open) {
        match byte {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&source[open + 1..index]);
                }
            }
            _ => {}
        }
    }

    None
}

/// Reads `(handler)` directly after a method-router name.
fn wrapped_ident(source: &str) -> Option<String> {
    let inner = source.strip_prefix('(')?.trim_start();
    let end = inner.find(|byte: char| !is_ident_byte(byte as u8))?;
    let (ident, rest) = inner.split_at(end);
    if ident.is_empty() || !rest.trim_start().starts_with(')') {
        return None;
    }
    Some(ident.to_owned())
}

fn fn_bodies(source: &str) -> BTreeMap<String, String> {
    let mut bodies = BTreeMap::new();

    for start in occurrences(source, "fn ") {
        if start > 0 && is_ident_byte(source.as_bytes()[start - 1]) {
            continue;
        }
        let rest = &source[start + 3..].trim_start();
        let offset = source.len() - rest.len();
        let Some(end) = rest.find(|byte: char| !is_ident_byte(byte as u8)) else {
            continue;
        };
        // A signature without a body ends at `;` before its brace.
        let terminator = rest[end..].find(';').map(|at| at + end);
        let brace = rest[end..].find('{').map(|at| at + end);
        if terminator.is_some_and(|at| brace.is_none_or(|brace| at < brace)) {
            continue;
        }
        if let Some(body) = block_at(source, offset) {
            bodies
                .entry(rest[..end].to_owned())
                .or_insert_with(|| body.to_owned());
        }
    }

    bodies
}

/// Maps idents imported from another route module back to that module, so a
/// shared helper or a handler re-exported through `mod.rs` resolves across
/// files. Absolute, `super::`, `self::` and sibling paths are resolved against
/// the real module key set.
fn route_imports(source: &str, module: &str, keys: &BTreeSet<String>) -> BTreeMap<String, String> {
    let mut imports = BTreeMap::new();

    for start in occurrences(source, "use ") {
        let rest = &source[start + 4..];
        let Some(end) = rest.find(';') else {
            continue;
        };
        let statement = rest[..end].trim();
        let (head, group) = match statement.split_once('{') {
            Some((head, tail)) => {
                let Some(close) = tail.find('}') else {
                    continue;
                };
                let group = &tail[..close];
                if group.contains('{') {
                    continue;
                }
                (head.trim().trim_end_matches("::").trim(), group.to_owned())
            }
            None => {
                let Some((head, item)) = statement.rsplit_once("::") else {
                    continue;
                };
                (head.trim(), item.trim().to_owned())
            }
        };
        let Some(base) = import_base(head, module) else {
            continue;
        };
        for item in group.split(',') {
            let Some((parent, ident)) = import_item(item) else {
                continue;
            };
            let path = join_path(&base, &[parent.as_str()]);
            if let Some(origin) = resolve_module(keys, &path) {
                imports.insert(ident, origin);
            }
        }
    }

    imports
}

/// Resolves an import head to a path relative to `src/routes`.
fn import_base(path: &str, module: &str) -> Option<String> {
    let mut segments = path.split("::").map(str::trim);
    let first = segments.next()?;
    let rest = segments.collect::<Vec<_>>();

    match first {
        "crate" => {
            let (first, rest) = rest.split_first()?;
            if *first != "routes" {
                return None;
            }
            Some(rest.join("/"))
        }
        "super" => {
            let mut base = parent_path(&module_path(module));
            let mut rest = rest.as_slice();
            while rest.first() == Some(&"super") {
                base = parent_path(&base);
                rest = &rest[1..];
            }
            Some(join_path(&base, rest))
        }
        "self" => Some(join_path(&module_path(module), &rest)),
        ident => {
            let mut segments = vec![ident];
            segments.extend(rest);
            Some(join_path(&module_dir(module), &segments))
        }
    }
}

/// Splits an imported item into its parent path and local binding name.
fn import_item(item: &str) -> Option<(String, String)> {
    let item = item.trim();
    if item.is_empty() || item.contains('{') {
        return None;
    }
    let (path, alias) = match item.split_once(" as ") {
        Some((path, alias)) => (path.trim(), Some(alias.trim())),
        None => (item, None),
    };
    let (parent, name) = match path.rsplit_once("::") {
        Some((parent, name)) => (parent.trim(), name.trim()),
        None => ("", path),
    };
    let ident = alias.unwrap_or(name);
    if ident.is_empty() || !ident.bytes().all(is_ident_byte) {
        return None;
    }
    Some((parent.to_owned(), ident.to_owned()))
}

fn resolve_module(keys: &BTreeSet<String>, path: &str) -> Option<String> {
    if path.is_empty() {
        return keys.contains("mod.rs").then(|| "mod.rs".to_owned());
    }
    [format!("{path}.rs"), format!("{path}/mod.rs")]
        .into_iter()
        .find(|key| keys.contains(key))
}

/// Module path without its file part: `a/b/c.rs` and `a/b/c/mod.rs` are both
/// `a/b/c`, the routes root is empty.
fn module_path(module: &str) -> String {
    if module == "mod.rs" {
        return String::new();
    }
    module
        .strip_suffix("/mod.rs")
        .or_else(|| module.strip_suffix(".rs"))
        .unwrap_or(module)
        .to_owned()
}

fn parent_path(path: &str) -> String {
    path.rsplit_once('/')
        .map_or_else(String::new, |(parent, _)| parent.to_owned())
}

/// Directory holding a module's siblings.
fn module_dir(module: &str) -> String {
    if module == "mod.rs" {
        return String::new();
    }
    module
        .rsplit_once('/')
        .map_or_else(String::new, |(dir, _)| dir.to_owned())
}

fn join_path(base: &str, segments: &[&str]) -> String {
    let joined = segments.join("/");
    if base.is_empty() {
        joined
    } else if joined.is_empty() {
        base.to_owned()
    } else {
        format!("{base}/{joined}")
    }
}

fn called_names(body: &str) -> BTreeSet<String> {
    let bytes = body.as_bytes();
    let mut names = BTreeSet::new();
    let mut start = None;

    for (index, byte) in bytes.iter().enumerate() {
        if is_ident_byte(*byte) {
            start.get_or_insert(index);
            continue;
        }
        if let Some(from) = start.take()
            && *byte == b'('
            && (from == 0 || bytes[from - 1] != b'.')
        {
            names.insert(body[from..index].to_owned());
        }
    }

    names
}

/// Returns the balanced brace block that follows `from`.
fn block_at(source: &str, from: usize) -> Option<&str> {
    let bytes = source.as_bytes();
    let open = from + source[from..].find('{')?;
    let mut depth = 0usize;

    for (index, byte) in bytes.iter().enumerate().skip(open) {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&source[open..=index]);
                }
            }
            _ => {}
        }
    }

    None
}

/// Blanks `#[cfg(test)]` items so fixtures never satisfy the guard.
fn strip_tests(source: &str) -> String {
    let mut source = source.to_owned();

    while let Some(start) = source.find("#[cfg(test)]") {
        let tail = &source[start..];
        let brace = tail.find('{');
        let terminator = tail.find(';');
        let end = match (brace, terminator) {
            (Some(brace), terminator) if terminator.is_none_or(|at| brace < at) => {
                start + brace + block_at(tail, brace).map_or(1, str::len)
            }
            (_, Some(at)) => start + at + 1,
            _ => source.len(),
        };
        source.replace_range(start..end, &" ".repeat(end - start));
    }

    source
}

/// Blanks comments and literals so braces and boundary names inside them never
/// reach the scanner. Byte positions stay stable.
fn mask_source(source: &str) -> String {
    let bytes = source.as_bytes();
    let mut masked = bytes.to_vec();
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                while index < bytes.len() && bytes[index] != b'\n' {
                    masked[index] = b' ';
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index = mask_comment(bytes, &mut masked, index);
            }
            b'r' if matches!(bytes.get(index + 1), Some(b'#' | b'"')) => {
                index = mask_raw(bytes, &mut masked, index);
            }
            b'"' => index = mask_string(bytes, &mut masked, index),
            b'\'' => index = mask_char(bytes, &mut masked, index),
            _ => index += 1,
        }
    }

    String::from_utf8(masked).expect("masking only replaces whole bytes with spaces")
}

fn mask_comment(bytes: &[u8], masked: &mut [u8], from: usize) -> usize {
    let mut index = from;
    let mut depth = 0usize;

    while index < bytes.len() {
        if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
            depth += 1;
        } else if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
            depth -= 1;
            masked[index] = b' ';
            masked[index + 1] = b' ';
            index += 2;
            if depth == 0 {
                return index;
            }
            continue;
        }
        if bytes[index] != b'\n' {
            masked[index] = b' ';
        }
        index += 1;
    }

    index
}

fn mask_raw(bytes: &[u8], masked: &mut [u8], from: usize) -> usize {
    let mut index = from + 1;
    while bytes.get(index) == Some(&b'#') {
        index += 1;
    }
    if bytes.get(index) != Some(&b'"') {
        return from + 1;
    }
    let hashes = index - from - 1;
    masked[from..=index].fill(b' ');
    index += 1;

    while index < bytes.len() {
        if bytes[index] == b'"' && bytes[index + 1..].iter().take(hashes).all(|at| *at == b'#') {
            masked[index..=index + hashes].fill(b' ');
            return index + hashes + 1;
        }
        if bytes[index] != b'\n' {
            masked[index] = b' ';
        }
        index += 1;
    }

    index
}

fn mask_string(bytes: &[u8], masked: &mut [u8], from: usize) -> usize {
    masked[from] = b' ';
    let mut index = from + 1;

    while index < bytes.len() {
        match bytes[index] {
            b'\\' => {
                masked[index] = b' ';
                if bytes.get(index + 1).is_some_and(|byte| *byte != b'\n') {
                    masked[index + 1] = b' ';
                }
                index += 2;
            }
            b'"' => {
                masked[index] = b' ';
                return index + 1;
            }
            b'\n' => index += 1,
            _ => {
                masked[index] = b' ';
                index += 1;
            }
        }
    }

    index
}

/// Masks a character literal and leaves lifetimes alone.
fn mask_char(bytes: &[u8], masked: &mut [u8], from: usize) -> usize {
    let end = if bytes.get(from + 1) == Some(&b'\\') {
        bytes[from + 2..]
            .iter()
            .position(|byte| *byte == b'\'')
            .map(|at| from + 2 + at)
    } else {
        (bytes.get(from + 2) == Some(&b'\'')).then_some(from + 2)
    };

    match end {
        Some(end) => {
            masked[from..=end].fill(b' ');
            end + 1
        }
        None => from + 1,
    }
}

fn occurrences(source: &str, needle: &str) -> Vec<usize> {
    source.match_indices(needle).map(|(at, _)| at).collect()
}

fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn format_handlers(handlers: &[&Handler]) -> String {
    if handlers.is_empty() {
        return "    none\n".to_owned();
    }

    handlers
        .iter()
        .map(|handler| format!("    ({:?}, {:?}, \"\"),\n", handler.module, handler.name))
        .collect()
}
