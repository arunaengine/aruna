<!-- Setup and test guide for native Aruna Git, LFS and ARCitect. -->
<!-- Copyright (c) 2026 The Aruna Contributors -->
<!-- SPDX-License-Identifier: MIT or Apache-2.0 -->

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
branch is reserved for server-generated metadata snapshots. Tags must
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
purge can still remove those bytes. Git repository maintenance, cross-node failover and
publication validation remain separate work. Node-local Git files live under `storage_path/git`.
The owner is fixed by the original creation event: its authoring node when it was a holder,
otherwise the first recorded holder. Later placement changes do not select another writer.
Use that node's endpoint; automatic Git failover is not implemented.

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
- The protected `aruna` branch records signed snapshots. `main` starts at the same commit
  and follows graph updates while it still equals the previous snapshot. If a client has
  changed `main`, it remains intact; the new graph snapshot stays on `aruna` for an explicit merge.
- Push preserves incoming commit IDs and spreadsheet bytes. Obtain current ISA-derived
  metadata for any branch, tag or commit through
  `GET /api/v1/metadata/{id}/git/rocrate?revision=main`. The response names the exact resolved
  commit. A stale JSON-LD file committed by a client is not used as the ISA source of truth.
- A pushed branch does not silently replace the collaborative graph or another branch.
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
Unix. LFS locking and GitLab-compatible login/discovery APIs are not implemented.
