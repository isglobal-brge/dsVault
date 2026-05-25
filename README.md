# dsimaging-admin

Admin CLI for creating and operating `dsimaging-store` deployments and the
medical imaging datasets stored in them.

## Install

```bash
pip install dsimaging-admin
```

## Create a store

`dsimaging-admin store init` provisions the configured backend. With MinIO it
writes a Docker Compose project for MinIO plus the `dsimaging-store` controller:

```bash
dsimaging-admin store init ./study-store \
  --controller-image davidsarratgonzalez/dsimaging-store:latest
dsimaging-admin store up ./study-store
dsimaging-admin store doctor ./study-store
```

For local controller development, build from a checked-out `dsimaging-store`
repo instead of using an image:

```bash
dsimaging-admin store init ./study-store \
  --store-source /path/to/dsimaging-store
dsimaging-admin store up ./study-store
```

With AWS it creates and wires the S3 data plane and SQS notification plane. It
uses boto3's default credential chain unless `--access-key` and `--secret-key`
are supplied explicitly:

```bash
dsimaging-admin --backend aws --region eu-west-1 --bucket my-imaging \
  store init --kms-key arn:aws:kms:eu-west-1:111122223333:key/...
```

If `--kms-key` is omitted, the bucket uses SSE-S3 (`AES256`). The AWS path needs
IAM permissions for `s3:CreateBucket`, `s3:PutBucketVersioning`,
`s3:PutBucketEncryption`, `s3:PutBucketNotification`, `sqs:CreateQueue` and
`sqs:SetQueueAttributes`. The resolved SQS queue URL is saved under
`aws.sqs_queue_url` in `~/.dsimaging.yaml`.

Use the generated connection details as a reusable CLI profile:

```bash
dsimaging-admin init \
  --endpoint http://127.0.0.1:9000 \
  --controller-url http://127.0.0.1:8080 \
  --bucket imaging-data
```

## Dataset operations

```bash
# Publish with staging, publish lock, skip-if-hash-matches and DICOM checks.
dsimaging-admin dataset publish \
  --dataset-id study_ct_v1 \
  --source /data/study_ct \
  --metadata /data/study_ct/clinical.csv \
  --modality ct

# Inspect and verify.
dsimaging-admin dataset list
dsimaging-admin dataset status study_ct_v1
dsimaging-admin dataset verify study_ct_v1
dsimaging-admin doctor

# Modify, rebuild artifacts from S3, copy, download or delete.
dsimaging-admin dataset modify study_ct_v1 \
  --metadata /data/study_ct/clinical_v2.csv \
  --add-images /data/study_ct/new_images \
  --yes
dsimaging-admin dataset rescan study_ct_v1
dsimaging-admin dataset copy study_ct_v1 study_ct_v2 --yes
dsimaging-admin dataset download study_ct_v1 ./debug/study_ct_v1
dsimaging-admin dataset delete study_ct_v1 --yes --purge-versions
```

The former top-level dataset commands (`publish`, `list`, `status`, `verify`,
`delete`, `download`, `copy`, `rescan`, `reconcile`) remain as deprecated aliases
and print a warning before delegating to the `dataset` subgroup.

## Local operator UI

The optional Streamlit dashboard wraps the same store and dataset operations in a
guided local interface:

```bash
pip install "dsimaging-admin[ui]"
dsimaging-admin ui launch
```

By default it binds to `127.0.0.1:8501`. This is an operator-only tool for
administrators with storage access: it has full visibility of bucket paths,
object sizes, manifests, controller responses and backend errors. It does not
add an authentication layer, so keep the default localhost bind unless you are
running it inside a trusted administrative environment. Access keys, secret keys,
KMS keys and webhook tokens are entered as password fields and are only shown as
set/not-set indicators after entry. Installing `streamlit-autorefresh`
separately enables automatic controller polling; otherwise the UI keeps a manual
refresh button.

All reporting commands that are useful for automation support JSON output:

```bash
dsimaging-admin dataset list --output json
dsimaging-admin dataset status study_ct_v1 --output json
dsimaging-admin dataset verify study_ct_v1 --output json
dsimaging-admin doctor --output json
```

## What `publish` does

1. Scans your local image directory and optional masks under `source/masks/`,
   `masks/`, `source/labels/`, or `labels/`.
2. Runs basic DICOM sanity checks for series UID, modality and instance order.
3. Computes SHA-256 content hashes.
4. Skips uploads that already match the current dataset hash indexes.
5. Uploads through `datasets/<id>/.staging-*` and a `.publish-lock`, then copies
   into `datasets/<id>/source/...`.
6. Generates and uploads:
   - `manifest.yaml`
   - `indexes/content_hash_index.parquet`
   - `indexes/masks_content_hash_index.parquet` when masks exist
   - `metadata/sample_manifests.parquet`
   - `metadata/samples.parquet`

Use `--dry-run` to scan and show the upload plan without S3 writes, `--no-skip`
to force uploads, or `--no-atomic` to disable staging.

`dataset modify` never deletes existing objects. `--metadata` validates and
replaces the metadata parquet after confirmation, `--add-images` and
`--add-masks` upload new content-addressed objects when the current hash indexes
do not already contain them, then the dataset indexes and manifest are rebuilt
from S3. Use `--dry-run` to list planned uploads without writing.

## Configuration

`~/.dsimaging.yaml` supports multiple profiles:

```yaml
default_profile: default
profiles:
  default:
    backend: auto
    endpoint: http://127.0.0.1:9000
    controller_url: http://127.0.0.1:8080
    bucket: imaging-data
    access_key: minioadmin
    secret_key: minioadmin123
    region: ""
    aws:
      sqs_queue_url: ""
```

Environment variables override profile values:

| Variable | Default | Description |
|---|---|---|
| `DSIMAGING_PROFILE` | `default` | Config profile |
| `DSIMAGING_ENDPOINT` | `http://127.0.0.1:9000` | S3/MinIO endpoint |
| `DSIMAGING_CONTROLLER_URL` | (empty) | dsimaging-store controller URL |
| `DSIMAGING_ACCESS_KEY` | `minioadmin` | S3 access key |
| `DSIMAGING_SECRET_KEY` | `minioadmin123` | S3 secret key |
| `DSIMAGING_BUCKET` | `imaging-data` | Bucket name |
| `DSIMAGING_REGION` | (empty) | S3 region |
| `DSIMAGING_BACKEND` | `auto` | Backend override: `auto`, `minio`, `aws` or `s3-compatible` |

## Dataset layout in S3

```text
s3://<bucket>/datasets/<dataset_id>/
  manifest.yaml
  metadata/
    samples.parquet
    sample_manifests.parquet
  indexes/
    content_hash_index.parquet
    masks_content_hash_index.parquet
  source/
    images/
    masks/
  derived/
  qc/
```

Store creation and dataset management only target dsimaging-store deployments.
