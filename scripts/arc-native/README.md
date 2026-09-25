# Native Aruna Git, LFS and ARCitect

Every metadata document automatically receives an ARC Git repository during metadata
materialization. Git and LFS are served by Aruna's own REST listener. The node uses Git's
HTTP backend, an Aruna receive helper and pinned ARCtrl for ISA conversion. LFS bytes use
Aruna's ordinary authorization, routing, quota, checksum and versioned-object operations.

Creation remains asynchronous. The signed initial commit appears when the accepted
metadata has been converted successfully. Existing documents without a repository are
also initialized on authorized Git access. Discover its URLs and conversion status with:

```bash
curl --fail-with-body \
  -H "Authorization: Bearer $ARUNA_TOKEN" \
  "$ARUNA_API_URL/api/v1/metadata/$ARUNA_DOCUMENT_ID/git"
```

The response contains `clone_url`, `lfs_url`, `bucket`, source `revision`, snapshot `commit`
and any conversion `error`. Missing required information or local data produces an error
instead of publishing an invalid snapshot. Git URLs have this form:

```text
https://node.example/api/v1/git/<document-id>.git
```

Use a credential manager with username `aruna` and an Aruna bearer token as password.
The node provisions a group-local `arc-<group-id>` bucket through ordinary bucket operations.
The token needs the appropriate READ/WRITE grants on the document and the repository's
`git-lfs/<document-id>/` object prefix in its bucket. Restricted tokens remain restricted.
Native Git passwords are bearer tokens, not S3 access secrets. Use HTTPS outside loopback.

The server supports multiple branches, merges through normal fast-forward ref updates,
lightweight and annotated commit tags, atomic pushes, and branch/tag deletion. Non-fast-
forward branch replacement and replacement of an existing tag are refused. The `aruna`
branch is reserved for server-generated metadata snapshots and `main` cannot be deleted. Tags must
resolve to commits. Symlinks, submodules and alternate `.lfsconfig` endpoints are currently
rejected. Git request/response bodies are bounded to 64 MiB; LFS uploads use the node's
RO-Crate source-size limit and stream to storage. Receive validation accepts at most
128 distinct commits per push (new commits plus updated ref targets) and 10,000 paths
per tree. Large data should use LFS.

Before refs become visible, validation parses ISA workbooks, checks required investigation
sections and values, study/assay registrations, local data references, CWL v1.2 schema and
referenced LFS availability. The CWL reference validator is restricted to ARC-local imports;
it does not execute workflows or fetch external resources. External-resource accessibility,
scientific correctness and publication/reproducibility readiness are not certified.
LFS mappings preserve exact VersionIds when S3 key heads change. Administrative version
purge can still remove those bytes. Git repository maintenance and publication validation
remain separate work.

## Holders and history

Git state is not tied to one node. Every push, server snapshot and LFS lock is a Git record
on the metadata document's topic, next to its metadata events. Packs are stored as Aruna
objects. Each document holder keeps a disposable cache under `storage_path/git` and rebuilds
it from the records, so any holder serves clones, fetches, pushes and LFS downloads.

A ref update applies when its old value matches or it fast-forwards. When two holders accept
competing updates to one ref, one wins on every holder. The other is kept as
`refs/conflicts/<ref>/<record id>`, for example `refs/conflicts/heads/main/<record id>`.
Fetch and merge it to resolve the race; nothing pushed is dropped.

A `File` entity whose `contentUrl` names an exact Aruna object version appears in the ARC as
an LFS pointer to that object. Downloading it requires READ on the object. An object the
snapshot's author cannot read stays out of the ARC; its entity still describes it.

LFS locks use the standard Git LFS lock API under `/git/{repository}/info/lfs/locks`. A lock
is a replicated claim: the earliest claim on a path wins on every holder. A push that changes
a path locked by another user is rejected until the lock is released.

Neither history has a hard cap. Metadata events are counted in windows of 1024 events or
16 MiB per document. Before a window fills, a holder writes a checkpoint event with the
current graph state, and the next window starts there. Each node writes a document through
one CRDT actor, so vector clocks grow with the number of writing nodes, not with edits.
Git records use the same idea: after 256 new records, a holder writes a Git checkpoint with
the current refs and locks, plus the packs and records added since the previous checkpoint.

## Versions API

The same history is available as plain JSON under `/api/v1/metadata/{document_id}`, without
Git. A version is one commit. `main` holds the live metadata, `aruna` holds the graph
snapshots, and other branches are drafts. Branch and tag names in paths are URL-encoded.

| Endpoint | Purpose |
| --- | --- |
| `GET /versions?branch=&limit=&cursor=` | Versions of a branch, newest first |
| `GET /versions/{version}` | One version and the files it changed |
| `GET /versions/{version}/rocrate` | The ISA-derived RO-Crate of a version |
| `GET /compare?from=&to=` | Changed entities, properties and files between two versions |
| `GET`, `POST /branches`; `DELETE /branches/{name}` | List, create and delete branches |
| `PUT /branches/{name}/rocrate` | Save new metadata on a draft branch as a new version |
| `POST /branches/{name}/merge` | Merge into another branch; merging into `main` updates the live metadata |
| `GET`, `POST /tags`; `DELETE /tags/{name}` | List, create and delete tags |
| `GET /conflicts`; `POST /conflicts/{id}/merge`; `DELETE /conflicts/{id}` | Review, merge or discard kept conflicts |

Writes accept `If-Match` with the branch head the client last saw and answer 412 when the
branch moved. A merge answers 409 when both sides changed the same metadata property to
different values, or the same non-metadata file. Nothing changes then. Edit the draft to the
wanted values and merge again. Workbook and RO-Crate file conflicts are resolved by merging
the metadata and generating those files again. Server-made versions name the Aruna user in
an `Aruna-User` commit trailer, shown as `author.user_id`.

## Metadata representation

The implementation follows DataPLANT's [ISA RO-Crate mapping](https://github.com/nfdi4plants/isa-ro-crate-profile/blob/release/profile/isa_ro_crate_mapping.md)
and the [ARC 2.1 specification](https://github.com/nfdi4plants/ARC-specification/blob/2.1/ARC%20specification.md).
The ISA RO-Crate profile is a draft; the actual converter is pinned to ARCtrl 3.2.1.

| ISA concept | RO-Crate representation |
| --- | --- |
| Investigation, study, assay | `Dataset`, distinguished by `additionalType`, linked with `hasPart` |
| Title, description, identifier | `name`, `description`, `identifier` |
| Public release date, people | `datePublished`, `creator` with `Person` entities |
| Experimental process and protocol | `LabProcess` and `LabProtocol`, with inputs, results and protocol links |
| Sources, samples and materials | `Sample` entities |
| Data files | `File` entities |
| Ontology annotations | `DefinedTerm` and ontology identifiers |
| Parameters, characteristics, factors, units | `PropertyValue`, including property and unit identifiers |

The two editing paths have explicit revision boundaries:

- `POST /api/v1/metadata` creates an RO-Crate from scaffold fields or accepts supplied
  RO-Crate JSON-LD. Scaffold creation uses RO-Crate 1.3; supplied 1.2/1.3 crates retain
  their version. The document has a root Dataset and linked data/contextual entities.
- `GET /api/v1/metadata/{document_id}/rocrate` exports that document's metadata graph.
- Materialization converts the selected metadata into ISA workbooks and a derived
  `ro-crate-metadata.json`. Generic crates become investigation-only ARCs using their
  supplied fields. No studies, assays or experimental facts are invented.
- `aruna-metadata.json` retains the exact selected Aruna JSON-LD, including its original
  1.2/1.3 context, identifiers and fields outside ISA. The ARCtrl-derived representation
  uses its supported RO-Crate 1.2 context. Additional source information is not discarded.
- The protected `aruna` branch records signed snapshots. Each commit names its source event in
  an `Aruna-Revision` trailer. `main` fast-forwards while it still equals the previous snapshot.
- After a client changed `main`, graph edits arrive as a signed merge commit with parents `main`
  and `aruna`. It updates `aruna-metadata.json` and the derived `ro-crate-metadata.json`. ISA
  workbooks are replaced only when their ISA meaning differs from the graph. Client files,
  data and LFS pointers stay. Pull before the next push, as with any shared Git branch.
- A push to `main` merges into the metadata document before its refs move. Only values changed
  between the old and new `main` are applied, from the ISA workbooks and `aruna-metadata.json`.
  Graph fields that ISA cannot express, such as keywords or custom properties, stay. ISA ids
  such as `#Person_Ada_Lovelace` are matched to existing graph entities. The update runs as the
  pushing user through the normal RO-Crate replace operation. If it is refused, so is the push.
- Push preserves incoming commit IDs and spreadsheet bytes. Obtain current ISA-derived
  metadata for any branch, tag or commit through
  `GET /api/v1/metadata/{id}/git/rocrate?revision=main`. The response names the exact resolved
  commit. A stale derived `ro-crate-metadata.json` committed by a client is not used.
- Other branches are drafts: they never change the metadata document until merged into `main`.
  LFS uploads store bytes and exact versions; ISA annotations determine their scientific role.

Referenced local data must be supplied through Git/LFS. Generation can reuse files already
present on `main`; it does not invent empty payloads or download arbitrary URLs. A valid
working ARC can still lack the contacts, assays, workflows or evidence required for publication.

## Runtime requirements

Native hosting requires Unix, Git, Python 3.13 with `blob/arc-requirements.txt`, and configured
Git signing. The image includes Git, GPG, Python, ARCtrl and the CWL validator. Operators must
provide a service signing key and Git configuration, for example using `GIT_CONFIG_GLOBAL`
and `GNUPGHOME` pointing at their mounted configuration/key store. Generated commits have
the service author `Aruna <git@aruna.local>` and matching author/committer timestamps.
The signing key must be usable by the unattended service. Missing runtime/signing/storage
infrastructure leaves work pending for retry; it never creates an unsigned fallback commit.

## ARCitect client patch

`arcitect.patch` applies to ARCitect revision
`ce986322028973b59cbcb93b462dcb614741e385` (package version 1.7.0).
It makes only the client changes needed for authenticated custom remotes:

- Enable Commit and DataHUB Sync without requiring a GitLab login.
- Read configured Git author identity when no DataHUB user is logged in.
- Enable Push for a selected custom remote; the remote still authenticates every request.

Apply it in a separate ARCitect checkout with `git apply /path/to/arcitect.patch`.
Its DataHUB login and discovery browser remain GitLab-specific. Configure the native Aruna
URL as a custom remote, use a credential manager, and open the local ARC in ARCitect.
The tested dependency baseline is `@nfdi4plants/arctrl@3.0.0-beta.15`, as named in ARCitect's
manifest. A fresh install's newer ARCtrl 3.2.1 changes the contract API and breaks this
ARCitect reader even for a local ARC; the test pins the declared baseline without altering
Aruna's protocol or rewriting ARC contents.

## Reproduce the integration test

The ignored `git_native` test starts a temporary Aruna node and native REST listener. It
checks automatic signed repository creation without an activation request, graph snapshots,
ISA-derived commit exports, real Git/LFS transfer, historical content after S3 overwrite,
read-only token denial, invalid ARC branches, atomic rejection, branches and tags.
With `ARUNA_ARCITECT` set, it also runs the actual patched Electron app with Playwright.

Prepare a separate ARCitect checkout at the revision above, apply the patch, install its
dependencies plus `@nfdi4plants/arctrl@3.0.0-beta.15` and `playwright`, then build it using
its own npm recipes. Electron's checksum-verified runtime and Xvfb are also required.
Set `ARUNA_ARCITECT` to that checkout and `ARUNA_XVFB` to the Xvfb executable. Put Git LFS
on PATH. The test uses a private profile and Git configuration, retains existing signing
and safety hooks, and assigns ephemeral ports to ARCitect's unused auxiliary services.

```bash
CARGO_BUILD_JOBS=2 RUST_TEST_THREADS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  uv run --no-project --with-requirements scripts/arc-native/requirements.txt \
  sh -c 'export ARUNA_ARC_PYTHON="$(command -v python)"; \
    exec nice -n 19 ionice -c3 cargo test --locked -p aruna --no-default-features \
    --test git_native -- --ignored --nocapture'
```

The Python code in this test generates a fixture and drives clients; it does not serve
Git/LFS or proxy Aruna requests. ARCitect itself performs clone, LFS fetch, signed commit
and UI Push. The test verifies actual remote refs and recovered payload bytes rather than
trusting a success dialog. No external DataHUB account is used or modified.

Repository READ covers its complete Git history, including ordinary Git files. Choose
the metadata document's reader scope accordingly; public grants also apply to signed-in
readers. LFS content additionally requires object READ. Native hosting currently requires
Unix. GitLab-compatible login and discovery APIs are not implemented.
