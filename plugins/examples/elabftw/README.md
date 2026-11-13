<!-- Setup and behavior of the eLabFTW example plugin that syncs experiments and datasets. -->
<!-- Copyright (c) 2026 The Aruna Contributors -->
<!-- SPDX-License-Identifier: MIT or Apache-2.0 -->

# eLabFTW sync example

`elabftw-sync` links eLabFTW experiments with Aruna datasets and keeps them in step both ways.
eLabFTW stores its uploads in an Aruna bucket through Aruna's S3 API, so file bytes exist once.

- An experiment with the tag `aruna` (setting `tag`) gets a dataset at `meta/elabftw/<id>`.
- A dataset made in Aruna below `meta/elabftw/` gets an experiment with that tag.
- Title, tags and body go both ways. Status, date and owner go to the dataset as
  `creativeWorkStatus`, `dateCreated` and `author`. Each upload becomes a `File` entity.
- Files that reach the dataset another way show up as links in the experiment's
  "Aruna files" field.
- The link is kept in the plugin state, in the dataset's `identifier.elabftw` and in the
  experiment's extra fields "Aruna dataset", "Aruna dataset id" and "Aruna files".

It needs eLabFTW 6.0 or later. It polls every five minutes; eLabFTW 6.0 has no webhooks.

## Requirements

- One Aruna node holds the S3 credential, eLabFTW's bucket and the plugin's receiver. A
  credential works only on the node that issued it, and the plugin reads object information
  only on its receiver.
- An eLabFTW user for the sync, such as `aruna-sync`: a normal team member, not an admin, with
  a read-write API key.
- A group service account for eLabFTW's storage, never the plugin's own account.

## Setup

These steps are for a group admin. `<realm>`, `<group>`, `<node>` and `<bucket>` stand for your
values; `<prefix>` is eLabFTW's path prefix, a folder name without slashes at its ends, such as
`elabftw`.

1. Create the group secret with the API key of the sync user, of kind `header`:

   ```json
   POST /api/v1/data/groups/<group>/secrets
   { "name": "elab-key", "kind": "header", "settings": { "header": "Authorization" },
     "fields": { "value": "<api key>" } }
   ```

2. Publish and install the plugin. The grants keep it to its own part of the group:

   ```sh
   aruna-cli plugin build plugins/examples/elabftw
   aruna-cli plugin publish target/wasm32-unknown-unknown/release/elabftw-sync-0.1.0.wasm
   aruna-cli plugin install elabftw-sync --group <group> \
     --setting elab_url=https://elab.example.org --setting sync_user=<sync user id> \
     --setting aruna_url=https://portal.example.org --setting writers=3,group:4 \
     --setting bucket=/<realm>/g/<group>/data/<node>/<bucket> --setting prefix=<prefix> \
     --grant 'metadata.read=/<realm>/g/<group>/meta/elabftw/**' \
     --grant 'metadata.write=/<realm>/g/<group>/meta/elabftw/**' \
     --grant 'metadata.create=/<realm>/g/<group>/meta/elabftw/**' \
     --grant 'objects.read=/<realm>/g/<group>/data/<node>/<bucket>/<prefix>/**' \
     --secret elab_key=group:elab-key
   ```

   Experiments the plugin creates are readable by the team (`canread_base` Team) and writable
   by their owner, the sync user (`canwrite_base` User), and by `writers`: eLabFTW user ids,
   and `group:<id>` for team groups. The connection reaches only `/api/v2/experiments` of
   `elab_url` with GET, POST and PATCH.

3. Read the receiver with `aruna-cli plugin installs --group <group>`. The receiver is the
   first eligible holder of the group's root metadata bucket. When `<node>` in the `bucket`
   setting is another node, remove the install and install again with the receiver's node.

4. On the receiver, create the bucket and the storage account's credential. The account's
   role needs WRITE on `/<realm>/g/<group>/data/<node>/<bucket>/<prefix>/**`.

   ```sh
   aruna-cli s3 credential create --group <group> --account <account id> \
     --path '<bucket>/<prefix>/**' --expires 365d
   ```

   The credential is confined to `<bucket>/<prefix>/**` with WRITE, which also allows reads.
   With an empty prefix the pattern is `<bucket>/**`, the whole bucket. Without `--expires` the
   key never expires. The access key and the secret are shown once.

5. In eLabFTW, set the container environment `ELAB_AWS_ACCESS_KEY` and `ELAB_AWS_SECRET_KEY` to
   the key pair. In Sysconfig, tab "Uploads", choose S3 storage with the receiver's S3
   endpoint, path-style requests, the bucket name `<bucket>` and the path prefix `<prefix>`.
   Restart eLabFTW.

6. Give each experiment that should sync the tag, and write access for the sync user: list it in
   `canwrite`, or set `canwrite_base` to Team or wider.
   Without write access the plugin syncs only from eLabFTW to Aruna and marks the link
   `no_write`.

To rotate the credential before it expires, create a new one, update the container secrets,
restart eLabFTW and revoke the old one.

Every run lists `<bucket>/<prefix>/` first. A bucket on another node, or a missing one, fails
the run with a message that names the receiver.

## Shared buckets

The bucket may be shared with other uses. Every group member with WRITE on `<bucket>/<prefix>`
can replace or delete objects that eLabFTW points to; eLabFTW then serves other bytes, or none,
under the old name. Where the group can, leave WRITE on the prefix to the storage account only,
through a role or a request policy. The daily reconciliation compares each upload's size with
the stored object and marks the link `changed_in_storage` when they differ. It cannot compare
hashes.

## How changes are synced

- Every run reserves a link in the plugin state before it changes anything, so two runs never
  change one link at once. A run that finds a link reserved by another run leaves it.
- Each field is compared with a snapshot of what both sides held after the last sync. A field
  changed on one side goes to the other. Changes to different fields merge. Tags changed on both
  sides merge one tag at a time.
- The same field changed on both sides to different values is not written. The link is marked
  `conflict` until a manual run settles it.
- Deletes are not propagated. A link whose experiment, tag or dataset is gone is marked `gone`.
- File entities are merged, never rebuilt: properties an Aruna user added, such as a `license`,
  stay when an upload changes.
- A dataset create is keyed by `elabftw/<experiment id>`, so a retried create gives the same
  dataset. An experiment create whose outcome is unknown, or whose run stopped before it
  saved the outcome, waits until a lookup finds the experiment or a person settles it.

## Bodies

The body is kept in the dataset as `text` (as eLabFTW stores it), `description` (plain text)
and `encodingFormat` (`text/html` or `text/markdown`).

- eLabFTW filters HTML bodies on every save. After the plugin writes a body, the stored form
  goes back to the dataset, so `text` and `description` agree again.
- A change only in markup, such as attribute order or quotes, is cosmetic and is not written
  anywhere. Changed text, whitespace, links or formatting is a change. Markdown is compared byte
  for byte.
- Editing only the `description` in Aruna replaces the eLabFTW body with that text: paragraphs
  and line breaks for HTML, the text as it is for Markdown. Earlier formatting is lost.
- When `text` and `description` both change and disagree, `text` wins.

## Accepted overwrite windows

eLabFTW has no conditional update. The plugin reads an experiment right before it writes, but a
person's edit that lands between that read and the write can be lost:

- the title, by the plugin's title write;
- the body, by the plugin's body write; eLabFTW may keep a revision of the earlier body, but not
  after a small change to a recently changed experiment;
- every extra field of the experiment, once per link, when the plugin creates its link fields
  with `metadatamerge`;
- the plugin's own link fields, by `updatemetadatafield`.

Tags and other fields are not affected. A person in exclusive edit mode blocks the plugin's
writes instead; the link is retried later. A user field named like a link field with another
type marks the link `blocked` until the experiment changes.

## Reconciliation and reports

Once a day the plugin compares every linked pair in small chunks across runs. Values that
differ while neither side changed since the last sync mark the link `out_of_sync` with the
fields. The run log gets one summary line per pass.

Manual runs, with `aruna-cli plugin run <install id> --input '<json>'`:

| Input | Effect |
| --- | --- |
| `{"report": true, "after": 0}` | One page of marked links (at most 100), the totals per mark, and `next` while more follow |
| `{"resolve": <experiment id>, "keep": "elabftw"}` | The marked fields take the eLabFTW values; `"aruna"` takes Aruna's |
| `{"resolve_create": "<dataset id>", "experiment": <id>}` | Links an experiment found after an unknown create |
| `{"resolve_create": "<dataset id>", "experiment": "absent"}` | Allows a new create after you checked eLabFTW |
| `{"reconcile": true}` | Starts a full reconciliation now |
| `{"experiment": <id>}` | Syncs one experiment now |

Marks: `conflict`, `out_of_sync`, `changed_in_storage`, `too_large`, `gone`, `blocked`,
`no_write`, `unknown_create`, `failed` (with the reason; retried after 15 minutes) and
`unfinished` (the run's budget ran out inside the link; the next run goes on).

## Limits

- An eLabFTW answer or request, a dataset read or write, must each fit 1 MiB. The eLabFTW answer
  holds the body twice (`body` and `body_html`), so bodies of a few hundred KiB sync; larger
  ones are marked `too_large`. eLabFTW's own limit is far higher.
- Every run keeps to one budget of about 900 host calls and 15 MiB, below the runtime's 1000
  calls and 16 MiB. Failed links, the quick pass and the reconciliation share it, and each stops
  where the budget ends: the quick pass keeps its listing offset, a dataset's file pages build up
  in state across runs, and a storage check keeps its position.
- The file pages read across runs are kept as one state value of at most 256 KiB: the plugin's
  own file entities and the `@id` of every other file. A dataset with more files is marked
  `too_large`.
- Plugin state holds about 500 bytes per link plus its tag set while that stays below 8 KiB,
  so about 30 000 links fit in the 16 MiB per install.
- `text`, `description`, `encodingFormat`, `creativeWorkStatus`, `dateCreated`, `author` and the
  file `sha256` are schema.org and RO-Crate terms. A group metadata profile that refuses them on
  a dataset makes the writes fail; check the profile before you install.
