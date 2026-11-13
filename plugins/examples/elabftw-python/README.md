<!-- Setup and behavior of the Python eLabFTW example that syncs experiments and datasets. -->
<!-- Copyright (c) 2026 The Aruna Contributors -->
<!-- SPDX-License-Identifier: MIT or Apache-2.0 -->

# eLabFTW sync example in Python

`elabftw-python` links eLabFTW experiments with Aruna datasets and keeps title, tags and body in
step both ways. eLabFTW stores its uploads in an Aruna bucket through Aruna's S3 API, and each
upload shows up in the dataset as a `File` entity. It is a short example to read; the Rust example
`elabftw-sync` in `plugins/examples/elabftw` does the same work with field-by-field merges.

- An experiment with the tag `aruna` (setting `tag`) gets a dataset below `meta/elabftw/`.
- A dataset made in Aruna gets no experiment. Links always start in eLabFTW.
- The link is kept in the plugin state, in the dataset's `identifier.elabftw` and in the
  experiment's extra fields "Aruna dataset" and "Aruna dataset id". Please do not edit
  `identifier.elabftw`; the plugin puts it back on its next visit.

It needs eLabFTW 6.0 or later. It polls every five minutes; eLabFTW 6.0 has no webhooks.

## Files

| File | Content |
| --- | --- |
| `app.py` | The handlers and the host calls through the WIT imports |
| `sync.py` | One link: compare, copy, enrol, resolve, uploads, and the run's budget guard |
| `walk.py` | The walk over datasets and tagged experiments, and the report |
| `elab.py` | The eLabFTW API calls and the size of eLabFTW's answers |
| `aruna.py` | The handler helper of every Python example |
| `tests/` | A fake eLabFTW and a fake Aruna host, and the tests |

## Requirements and setup

- One Aruna node holds the S3 credential, eLabFTW's bucket and the plugin's receiver.
- An eLabFTW user for the sync, such as `aruna-sync`: a normal team member with a read-write API
  key.
- A group service account for eLabFTW's storage, never the plugin's own account.

`<realm>`, `<group>`, `<node>`, `<bucket>` and `<prefix>` stand for your values.

1. Create the group secret with the sync user's API key, of kind `header`:

   ```json
   POST /api/v1/data/groups/<group>/secrets
   { "name": "elab-key", "kind": "header", "settings": { "header": "Authorization" },
     "fields": { "value": "<api key>" } }
   ```

2. Build, publish and install the plugin:

   ```sh
   aruna-cli plugin build plugins/examples/elabftw-python
   aruna-cli plugin publish plugins/examples/elabftw-python/elabftw-python-0.1.0.wasm
   aruna-cli plugin install elabftw-python --group <group> \
     --setting elab_url=https://elab.example.org --setting sync_user=<sync user id> \
     --setting aruna_url=https://portal.example.org \
     --setting bucket=/<realm>/g/<group>/data/<node>/<bucket> --setting prefix=<prefix> \
     --grant 'metadata.read=/<realm>/g/<group>/meta/elabftw/**' \
     --grant 'metadata.write=/<realm>/g/<group>/meta/elabftw/**' \
     --grant 'metadata.create=/<realm>/g/<group>/meta/elabftw/**' \
     --grant 'objects.read=/<realm>/g/<group>/data/<node>/<bucket>/<prefix>/**' \
     --secret elab_key=group:elab-key
   ```

3. On the receiver node, create the bucket and the storage account's credential, confined to
   eLabFTW's prefix. Without `--expires` the key never expires.

   ```sh
   aruna-cli s3 credential create --group <group> --account <account id> \
     --path '<bucket>/<prefix>/**' --expires 365d
   ```

4. In eLabFTW, set `ELAB_AWS_ACCESS_KEY` and `ELAB_AWS_SECRET_KEY` to the key pair, and in
   Sysconfig, tab "Uploads", choose S3 storage with the receiver's endpoint, path-style requests,
   the bucket `<bucket>` and the prefix `<prefix>`.

5. Tag each experiment that should sync, and give the sync user write access: list it in
   `canwrite`, or set `canwrite_base` to Team or wider. Without write access the plugin syncs
   only from eLabFTW to Aruna and marks the link `no_write`.

Every run lists `<bucket>/<prefix>/` first; a missing bucket fails the run with a hint. The
bucket may be shared, but every group member with WRITE on the prefix can replace eLabFTW's
objects; leave WRITE on the prefix to the storage account where you can.

## How changes are synced

- The plugin keeps one fingerprint per link of the last synced record: title, tags (without the
  sync tag), body and its format. If only one side changed since then, its whole record is copied
  to the other side.
- If both sides changed, the link is marked `conflict` and nothing is written until a manual run
  picks a side. This also holds when the two sides changed different fields.
- Before it changes anything, the plugin saves a pending marker on the link. If a run stops in the
  middle of a write, the next visit links equal records, and marks differing records `conflict`.
  An undo made in the meantime is never overwritten.
- After a write to eLabFTW, the body as eLabFTW stored it (its HTML cleaner may change markup)
  goes back to the dataset as `text` and, as plain text, `description`. Editing only the
  `description` in Aruna is not synced.
- A dataset is created once per experiment (keyed `elabftw/<id>`). If its first creation was cut
  short and the experiment changed meanwhile, the link is marked `conflict`.
- Uploads become `File` entities with `@id` `elabftw/uploads/<id>`. Properties an Aruna user added,
  such as a `license`, stay when an upload changes. Other file entities are left alone.
- Deletes are not propagated. A link whose experiment, sync tag or dataset is gone is marked
  `gone`; a dataset counts as gone only after a direct read finds it missing.

## The walk

Each timer run continues one walk from where the last run stopped: first through the group's
datasets, then through the tagged experiments, then the next cycle starts. A run stops starting
new links after 40 seconds, or when its host budget runs low, and saves its place. An eLabFTW
edit reaches Aruna when the walk reaches the link; Aruna edits go out at once through the
`dataset_updated` event. A link that stops its run every time is marked `failed` and skipped
until the next cycle, so it never holds up the others.

## Manual runs

Run them with `aruna-cli plugin run <install id> --input '<json>'`.

| Input | Effect |
| --- | --- |
| `{"report": true}` | Marked and unlinked entries, at most 48 KiB per page, with `next` while more follow |
| `{"report": true, "next": <cursor>}` | The next page of the report |
| `{"resolve": <experiment id>, "keep": "elabftw"}` | eLabFTW's record wins; `"aruna"` lets Aruna's win |
| `{"experiment": <id>}` | Syncs one experiment now, or enrols it |

Marks: `conflict`, `too_large`, `gone`, `no_write`, `blocked` (a link field clashes with a user
field of another type), `failed` (with the reason) and, in the report, `unlinked` and
`unfinished` (an enrolment that did not finish).

## Size limits

The plugin host allows 1 MiB per request or answer, and 16 MiB and 1000 calls per run.

- An eLabFTW answer holds the body twice (`body` and `body_html`), so bodies of a few hundred KiB
  sync. A larger experiment is marked `too_large`; eLabFTW's own limit is far higher.
- Before writing a body to eLabFTW, the plugin estimates eLabFTW's answer and does not send a
  write whose answer would not fit.
- eLabFTW's HTML cleaner can grow a body several times, for example by repeating formatting
  elements around each block, and Markdown is rendered into `body_html`. A body that grows past
  the host limit makes that experiment fail to sync, marked `too_large`, until a person shortens
  it in eLabFTW; then a manual run with `"keep": "elabftw"` settles the link.
- A link whose writes do not fit one run, such as a very large tag change, is marked `too_large`.

## Accepted overwrite windows

eLabFTW has no conditional update. A person's edit that lands between the plugin's read and its
write of the same experiment (seconds) is overwritten. Two plugin runs on one link at the same
moment can end in a `conflict` mark that a manual run settles.

## Tests

From this folder, with uv:

```sh
uv run --no-project --python 3.12 python -m unittest discover -s tests -t . -p 'test_*.py'
```

`plugins/tests/elabftw_python.rs` builds the plugin and runs it in the plugin runtime; it runs
with `ARUNA_PLUGIN_LANGUAGES=1 cargo test -p aruna-plugins --test elabftw_python`.
