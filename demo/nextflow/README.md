# Nextflow on Aruna TES

This minimal pipeline runs three containerized processes through Aruna's GA4GH TES API. Nextflow stages its work files in an Aruna-hosted S3 bucket and sends no Aruna-specific TES tags.

## Prerequisites

- A running Aruna node with a healthy Docker executor (`ARUNA_COMPUTE_DOCKER=true`), an explicit `ARUNA_COMPUTE_DOCKER_DISK_BYTES`, and a container-reachable `S3_PUBLIC_URL`.
- An existing Aruna S3 bucket and an Aruna S3 credential for its group.
- Nextflow with Java 17 or later.

## Run

From this directory, export the Aruna endpoints and credentials:

```bash
export ARUNA_API_URL=http://127.0.0.1:3000/api/v1
export ARUNA_S3_URL=http://127.0.0.1:1337
export ARUNA_ACCESS_KEY_ID='<aruna-access-key-id>'
export ARUNA_ACCESS_SECRET='<aruna-access-secret>'
export ARUNA_S3_REGION=eu-central-1
export ARUNA_WORK_BUCKET='<existing-aruna-bucket>'

nextflow run main.nf
```

Nextflow prints the final `summary.txt` path; all work artifacts remain below `s3://$ARUNA_WORK_BUCKET/nextflow-work` in Aruna.
