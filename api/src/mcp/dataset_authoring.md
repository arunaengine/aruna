# Dataset Authoring

A user who asks for "a dataset from the files in bucket X" wants one metadata
document describing objects that already exist. The bucket carries the file
facts. The user carries everything else. This document is the order in which to
collect both.

## Inventory the bucket

1. `list_buckets` answers the readable buckets with the owning `group_id`. That
   group is the default owner of the new dataset.
2. `list_objects` answers one page. Pass `prefix` when the user named a folder,
   and repeat the call with `cursor` set to the previous `next_cursor` until
   `next_cursor` is absent. An inventory built from one page is incomplete.
3. Tell the user how many objects the listing found before building entities
   from it. When the count is large or the prefix pulled in unrelated keys,
   confirm the scope first instead of describing thousands of files.

One object becomes one `File` entity:

| Property         | Source                                                    |
| ---------------- | --------------------------------------------------------- |
| `@id`            | The object key, or the `s3://bucket/key` URL.              |
| `contentUrl`     | `s3://bucket/key`, always in that form.                    |
| `name`           | The key, usually its last segment.                         |
| `contentSize`    | `size` from the listing, in bytes.                         |
| `encodingFormat` | `content_type`, or the media type the extension implies.   |
| `dateModified`   | `last_modified` from the listing.                          |

Every File entity is listed in the root Dataset's `hasPart`.

## Derive what the data supports

Small text objects usually answer more than the file listing does. Read them
with `read_object`, which returns a UTF-8 window of at most 1 MiB and refuses
binary content:

* `README`, `README.md`: a name, a description, keywords.
* `LICENSE`, `LICENSE.txt`: the license, matched to its SPDX IRI.
* `CITATION.cff`, `codemeta.json`, an existing `ro-crate-metadata.json`: authors,
  title, identifiers, publication date.
* Key structure: shared prefixes, dates and sample ids that suggest keywords.

Name the source of every suggestion, for example "from README.md line 1" or
"from the LICENSE file". A suggestion with no source is a guess and does not
belong in the proposal.

## Ask once

Ask for what the data cannot answer in one compact message, with the derived
suggestion beside each field so the user can accept or edit it:

| Field                  | Note                                                        |
| ---------------------- | ----------------------------------------------------------- |
| `name`                 | Short title of the dataset.                                  |
| `description`          | One paragraph.                                               |
| `creator` or `author`  | A `Person` or `Organization` entity, with an ORCID or ROR id when the user knows it. |
| `license`              | An IRI, for example `https://spdx.org/licenses/CC-BY-4.0`.   |
| `datePublished`        | An ISO date such as `2026-09-02`.                            |
| `keywords`             | A few terms.                                                 |
| Profile                | One of the Profiles `list_profiles` answers, or none.        |
| Group and path         | Default to the bucket's `group_id` and `datasets/<bucket>`, or `datasets/<prefix>` when a prefix scoped the inventory. |

## Rules

* Mark suggestions as suggestions. The user accepts, edits, or drops each one.
* Never invent a person, an organization, an ORCID, a ROR id, a license, or a
  date. An unknown value is asked for once and otherwise left out.
* A field the user declines stays absent from the crate. Do not restate it.
* At most one follow-up round after the first question. Build with what the user
  gave rather than asking a third time.
* Required means what the chosen Profile's SHACL demands plus the RO-Crate 1.3
  root minimum: `name`, `description`, `datePublished`, and `license`. Everything
  else is optional and is never a reason to block or nag.
* Show the assembled crate and the file count, then call `validate_dataset`,
  repair every structural violation and Profile finding, and validate again.
* Call `create_dataset` only after the user has confirmed the summary. A 201 is
  durable acceptance but not immediate readability, so poll `get_dataset`.
* A denied tool call is the user's decision. Do not retry it; say what was
  denied and ask what should change.

## Minimal crate

```json
{
  "@context": "https://w3id.org/ro/crate/1.3/context",
  "@graph": [
    {
      "@id": "ro-crate-metadata.json",
      "@type": "CreativeWork",
      "conformsTo": { "@id": "https://w3id.org/ro/crate/1.3" },
      "about": { "@id": "./" }
    },
    {
      "@id": "./",
      "@type": "Dataset",
      "name": "Mouse liver RNA-seq 2026",
      "description": "Raw reads and the run manifest for the 2026 liver series.",
      "datePublished": "2026-09-02",
      "license": { "@id": "https://spdx.org/licenses/CC-BY-4.0" },
      "conformsTo": { "@id": "https://w3id.org/aruna/profile/01JZ8Y6T0K4W7M2N9Q5R3S8V1X" },
      "hasPart": [{ "@id": "s3://mouse-liver/reads/sample-01.fastq.gz" }]
    },
    {
      "@id": "s3://mouse-liver/reads/sample-01.fastq.gz",
      "@type": "File",
      "name": "sample-01.fastq.gz",
      "contentUrl": "s3://mouse-liver/reads/sample-01.fastq.gz",
      "contentSize": 41234567,
      "encodingFormat": "application/gzip",
      "dateModified": "2026-08-30T11:04:12Z"
    }
  ]
}
```

The root carries exactly one Profile IRI in `conformsTo`, in the form
`https://w3id.org/aruna/profile/<document_id>`. Omit the property when no
Profile was chosen. Creators, keywords, and every other property the user
supplied are added to the root the same way.
