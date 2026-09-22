# Native Aruna Git, LFS and ARCitect

Git and LFS are served by Aruna's own REST listener. No Python bridge process or separate
S3 credentials are needed by Git clients. The node uses Git's HTTP backend and an Aruna
receive helper; LFS bytes use Aruna's ordinary authorization, routing, quota, checksum
and versioned-object operations. Repository bindings are persisted in node-local storage.

Enable Git for an existing metadata document and a same-group bucket after the metadata
document is readable. Aruna accepts metadata creation asynchronously, so its initial
acceptance response can precede registry visibility.

```bash
curl --fail-with-body -X POST \
  -H "Authorization: Bearer $ARUNA_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"bucket":"arc-storage","arc":true}' \
  "$ARUNA_API_URL/api/v1/metadata/$ARUNA_DOCUMENT_ID/git"
```

The response contains `clone_url` and `lfs_url`. Git URLs have this form:

```text
https://node.example/api/v1/git/<document-id>.git
```

Use a credential manager with username `aruna` and an Aruna bearer token as password.
The token needs the appropriate READ/WRITE grants on the document and the repository's
`git-lfs/<document-id>/` object prefix in its bucket. Restricted tokens remain restricted.
Native Git passwords are bearer tokens, not S3 access secrets. Use HTTPS outside loopback.

The server supports multiple branches, merges through normal fast-forward ref updates,
lightweight and annotated commit tags, atomic pushes, and branch/tag deletion. Non-fast-
forward branch replacement and replacement of an existing tag are refused. Tags must
resolve to commits. Symlinks, submodules and alternate `.lfsconfig` endpoints are currently
rejected. Git request/response bodies are bounded to 64 MiB; LFS uploads use the node's
RO-Crate source-size limit and stream to storage. Receive validation accepts at most
128 distinct commits per push (new commits plus updated ref targets) and 10,000 paths
per tree. Large data should use LFS.

ARC mode checks the investigation archive signature, required directory metadata paths,
and referenced LFS availability before refs become visible. It does not certify full ISA
semantics or every ARC specification requirement. Existing Aruna metadata is not silently
replaced by pushed ISA spreadsheets; Git history is the committed file representation.
LFS mappings preserve exact VersionIds when S3 key heads change. Administrative version
purge can still remove those bytes. Git repository maintenance, cross-node failover and
full ARC validation remain separate work. Node-local Git files live under `storage_path/git`.

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
checks real Git/LFS push, clone, historical content after S3 overwrite, read-only token
denial, malformed content, invalid ARC branches, atomic rejection, branches and tags.
With `ARUNA_ARCITECT` set, it also runs the actual patched Electron app with Playwright.

Prepare a separate ARCitect checkout at the revision above, apply the patch, install its
dependencies plus `@nfdi4plants/arctrl@3.0.0-beta.15` and `playwright`, then build it using
its own npm recipes. Electron's checksum-verified runtime and Xvfb are also required.
Set `ARUNA_ARCITECT` to that checkout and `ARUNA_XVFB` to the Xvfb executable. Put Git LFS
on PATH. The test uses a private profile and Git configuration, retains existing signing
and safety hooks, and assigns ephemeral ports to ARCitect's unused auxiliary services.

```bash
CARGO_BUILD_JOBS=2 RUST_TEST_THREADS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  uv run --no-project --with-requirements scripts/arc-bridge/requirements.txt \
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
