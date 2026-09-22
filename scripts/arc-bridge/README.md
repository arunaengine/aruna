# Aruna Git/LFS ARC PoC

This standalone bridge serves one private ARC repository over Git smart HTTP and Git LFS.
LFS uploads are verified by SHA-256 and size, stored through Aruna S3, and read using the
returned VersionId. An explicit publication request creates private, commit-specific
RO-Crate metadata through Aruna REST. Aruna's production services are unchanged.

The bridge is for one trusted operator on loopback. Its shared token represents that
operator's configured Aruna credentials. It does not implement multi-user permission
mapping, TLS, repository failover, automatic metadata synchronization or ARC certification.

Prerequisites: Python 3.12+, uv, Git, Git LFS, and an existing Aruna group with a dedicated
bucket, S3 credentials and a bearer token allowed to create metadata under `arc-poc/`.
Use a new private bucket for the experiment. Keep its objects and bridge state together.

## Run

From the repository root, provide these variables through your private environment:

```text
ARUNA_API_URL        REST origin, for example http://127.0.0.1:3000
ARUNA_S3_URL         S3 origin, for example http://127.0.0.1:1337
ARUNA_TOKEN          Aruna bearer token
ARUNA_GROUP_ID       Existing group ID
ARUNA_BUCKET         Dedicated existing bucket
AWS_ACCESS_KEY_ID    Group S3 access key
AWS_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION  Defaults to eu-central-1
ARC_BRIDGE_TOKEN    Separate random bridge password, at least 24 characters
```

Never put credentials in repository files or remote URLs. Start the bridge with:

```bash
uv run --no-project --with-requirements scripts/arc-bridge/requirements.txt \
  python scripts/arc-bridge/bridge.py serve \
  --state .claude/arc-state --name crate --port 8097
```

Generate a synthetic study/assay ARC in a new empty directory:

```bash
uv run --no-project --with-requirements scripts/arc-bridge/requirements.txt \
  python scripts/arc-bridge/bridge.py scaffold .claude/arc-source
```

Initialize that directory as a Git repository, retaining your normal author, signing
and safety-hook configuration. Configure LFS without replacing existing Git hooks:

```bash
git init --initial-branch=main .claude/arc-source
cd .claude/arc-source
git lfs install --local --skip-repo
git remote add origin http://127.0.0.1:8097/crate.git
git add .gitattributes isa.investigation.xlsx ro-crate-metadata.json assays studies workflows runs
git diff --cached
```

Create a signed commit following your usual workflow. Authenticate to the bridge with
username `operator` and `ARC_BRIDGE_TOKEN` as password using your credential manager.
Upload LFS content before pushing the Git branch:

```bash
git lfs push origin main
git push -u origin main
```

Clone with a standard client. If LFS filters are not already configured, clone with
`GIT_LFS_SKIP_SMUDGE=1`, then run `git lfs install --local --skip-repo` and `git lfs pull`
inside the clone. No global configuration or hook replacement is needed.

## HTTP interface

All routes require the bridge token, either as a Bearer token or an HTTP Basic password.

| Route | Behavior |
| --- | --- |
| `GET /status` | Repository name, Git URL, validation scope and upload limit |
| `/{name}.git/info/refs` and Git pack routes | Standard authenticated Git transport |
| `POST /{name}.git/info/lfs/objects/batch` | Standard LFS basic transfer negotiation |
| `PUT /lfs/{sha256}` | Verify and store a payload in Aruna |
| `GET /lfs/{sha256}` | Read the recorded exact Aruna version |
| `POST /publish/{commit}` | Publish private metadata for a reachable commit; repeatable |

Publication returns the accepted Aruna metadata record and `validation: poc-arc-subset`.
Metadata acceptance does not prove Aruna's asynchronous search projection is ready.
Publication is explicit and is not part of Git push acknowledgement. The metadata copy
adds exact S3 locations for LFS files and the source commit as `version`; committed bytes
are unchanged. Ordinary Git files remain in Git. S3 URLs still require S3 authorization.

## Validation and limits

The receive hook reads committed ISA workbooks using pinned ARCtrl 3.2.1. It checks the
investigation, study/assay registrations, matching RO-Crate identities and verified LFS
objects for every newly reachable commit, including non-default branches. Missing files,
unuploaded LFS content, unsupported pointers and non-fast-forward updates are rejected.

This is a tested study/assay subset, not full ARC specification validation. Workflows,
runs, symlinks, submodules, alternate `.lfsconfig` endpoints, tags and branch deletion are
unsupported. No promise is made
that ARCtrl parsing covers every ARC MUST or that all scientific relationships agree.
Limits: 200 files per tree, 32 new commits per push, 1 MiB per ordinary Git file,
16 MiB total Git tree/request, and 64 MiB per LFS object/Git response. Metadata workbooks
must be ordinary Git files. Converters and hooks are not a production sandbox.

SQLite persists the Aruna/repository binding, LFS VersionIds and successful publications.
The bridge has no deletion API. It does not add retention pins to Aruna: administrative
version purge or bucket deletion can break historical checkout. A crash between S3 PUT
and recording its VersionId can leave an extra version. Duplicate uploads may also leave
extra versions. No power-loss, garbage-collection or complete backup guarantee is claimed.
Changing the configured backend/group for existing state is refused.

## Verify

Focused Python tests:

```bash
uv run --no-project --with-requirements scripts/arc-bridge/requirements.txt \
  python -m unittest discover -s scripts/arc-bridge -p test_bridge.py -v
```

The optional Rust test starts a real temporary Aruna node, creates a group and bucket,
then runs signed Git commits and actual Git/LFS clients. It verifies historical checkout
after an S3 key-head overwrite, idempotent private metadata publication, unauthorized
requests, incorrect digests, missing LFS data and invalid non-default branches.

Put `git-lfs` on PATH. Run Cargo inside the pinned uv environment so the Python
interpreter and receive-hook dependencies remain available for the whole test:

```bash
CARGO_BUILD_JOBS=2 RUST_TEST_THREADS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  uv run --no-project --with-requirements scripts/arc-bridge/requirements.txt \
  sh -c 'export ARUNA_ARC_PYTHON="$(command -v python)"; \
    exec nice -n 19 ionice -c3 cargo test --locked -p aruna --no-default-features \
    --test arc_interface -- --ignored --nocapture'
```

The test requires a configured signing key and preserves existing client safety hooks.
It uses ephemeral listeners and temporary data, shuts down its node, and publishes nothing
outside that temporary node. It is ignored in ordinary test runs because of these external
tool requirements. It does not establish production or full ARC compliance.

Protocol references: [Git HTTP](https://git-scm.com/docs/git-http-backend),
[Git LFS](https://github.com/git-lfs/git-lfs/tree/main/docs/api),
[ARCtrl](https://nfdi4plants.org/nfdi4plants.knowledgebase/arctrl/),
[ARC specification](https://github.com/nfdi4plants/ARC-specification).
