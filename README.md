# dsimaging-admin

Admin CLI for creating and operating `dsimaging-store` deployments and the
medical imaging datasets stored in them.

## Install

```bash
pip install dsimaging-admin
```

Python 3.10 or newer is required.

## Create a store

`dsimaging-admin store init` provisions the configured backend. With MinIO it
writes a Docker Compose project for MinIO plus the `dsimaging-store` controller:

```bash
dsimaging-admin store init ./study-store
dsimaging-admin store up ./study-store
dsimaging-admin store doctor ./study-store
```

The generated project pins the published multi-architecture controller, MinIO
and MinIO client images by both release tag and manifest-list digest. Use
`--controller-image` only to select a deliberate controller upgrade.
The controller image is also directly available as
`davidsarrat/dsimaging-store:0.3.10` for `linux/amd64` and `linux/arm64`.

New local projects generate unique MinIO credentials and an operator token in
`./study-store/.env` (mode `0600`). MinIO's API, console and controller are
bound to `127.0.0.1` by default. Configure a CLI profile from those generated
values before running dataset commands; no shared MinIO credential is built in.
Credentials from an existing connection profile are not reused for a new
store. Pass `store init --access-key ... --secret-key ...` only when an
explicit provisioning override is required.

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
  store init ./aws-store \
  --kms-key arn:aws:kms:eu-west-1:111122223333:key/...
```

If `--kms-key` is omitted, the bucket uses SSE-S3 (`AES256`). The AWS path needs
IAM permissions for `s3:CreateBucket`, `s3:PutBucketVersioning`,
`s3:PutBucketEncryption`, `s3:PutBucketNotification`, `sqs:CreateQueue` and
`sqs:SetQueueAttributes`. The resolved SQS queue URL is saved under
`aws.sqs_queue_url` in `~/.dsimaging.yaml`.

Reading and verifying a versioned collection also requires
`s3:ListBucket`, `s3:GetObject`, and `s3:GetObjectVersion`. When the bucket
uses SSE-KMS, grant `kms:Decrypt` for the selected key as well. Keep the usual
write/delete permissions limited to identities that actually publish or
modify collections.

Use the generated connection details as a reusable CLI profile:

```bash
dsimaging-admin init \
  --endpoint http://127.0.0.1:9000 \
  --controller-url http://127.0.0.1:8080 \
  --bucket imaging-data
```

## Dataset operations

```bash
# Publish with staging, a publish lock and DICOM checks.
dsimaging-admin dataset publish \
  --dataset-id study_ct_v1 \
  --source /data/study_ct \
  --metadata /data/study_ct/clinical.csv \
  --privacy-unit-column patient_id \
  --label-column diagnosis \
  --public-label-level case \
  --public-label-level control \
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
dsimaging-admin dataset delete study_ct_v1 --yes
```

Ordinary deletion preserves version history. Permanently removing every object
version and delete marker is a separate, irreversible operator decision:

```bash
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

The dashboard is deliberately loopback-only; use an SSH tunnel when operating
it remotely. Stored access keys, secret keys and controller tokens are never
prefilled into browser widget state. Enter any credentials needed for that UI
session explicitly, or use the CLI profile directly.

By default it binds to `127.0.0.1:8501`. This is an operator-only tool for
administrators with storage access: it has full visibility of bucket paths,
object sizes, manifests, controller responses and backend errors. It does not
add an authentication layer, so keep the default localhost bind unless you are
running it through a loopback SSH tunnel. Access keys, secret keys,
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
4. Uploads through `datasets/<id>/.staging-*` and a `.publish-lock`, then copies
   into `datasets/<id>/source/...`; derived objects are generated before upload
   and `manifest.yaml` is written last.
5. Generates and uploads:
   - `manifest.yaml`
   - `indexes/content_hash_index.parquet`
   - `indexes/masks_content_hash_index.parquet` when masks exist
   - `metadata/sample_manifests.parquet`
   - `metadata/samples.parquet`

Use `--dry-run` to scan and show the upload plan without S3 writes. Published
datasets always use staging and an atomic publication lock; `--no-atomic` is
retained only to produce an explicit compatibility error.

Every publication pins this disclosure-control contract under `manifest.yaml`'s
`metadata` mapping:

```yaml
id_col: sample_id
privacy_unit: patient
privacy_unit_col: patient_id
privacy_unit_canonicalization: trim-utf8-v2
label_col: diagnosis  # omitted when --label-column is not supplied
label_levels: [case, control]  # omitted unless explicitly approved by the operator
```

`--privacy-unit-column` is required. Every discovered sample must have a
non-empty value in that metadata column and, when declared, in `--label-column`.
Repeat `--public-label-level` to declare the finite label vocabulary that may be
used by disclosure-controlled aggregate results. Undeclared values, unsafe
identifiers, duplicates, and values equal to sample or patient identifiers are
rejected; without this explicit vocabulary, label distributions stay withheld.
The metadata `sample_id` roster must match the discovered image roster exactly;
duplicates, missing rows and unrelated extra rows are rejected. The patient and
optional label columns must be dedicated columns, while multiple distinct
samples may belong to the same patient. Publishing to any existing dataset is
rejected by default. Use `--replace` for an explicit atomic replacement; it
stages the full new source set, removes stale historical source objects, retains
a rollback copy until the new manifest succeeds.

`dataset modify` never deletes existing objects. `--metadata` validates and
replaces the metadata parquet after confirmation, `--add-images` and
`--add-masks` upload new content-addressed objects when the current hash indexes
do not already contain them, then the dataset indexes and manifest are rebuilt
from S3. Rescan, copy and modify use the same owned dataset lock and rollback
path. Copy also holds the source collection stable while taking its snapshot.
They preserve the pinned metadata contract and other manifest fields;
incomplete replacement metadata fails without rewriting the manifest. Use
`--dry-run` to list planned uploads without writing.

## Configuration

`~/.dsimaging.yaml` supports multiple profiles:

```yaml
default_profile: default
profiles:
  default:
    backend: auto
    endpoint: http://127.0.0.1:9000
    controller_url: http://127.0.0.1:8080
    controller_token: <operator token>
    bucket: imaging-data
    access_key: <value from the store .env MINIO_ROOT_USER>
    secret_key: <value from the store .env MINIO_ROOT_PASSWORD>
    region: ""
    aws:
      sqs_queue_url: ""
```

Environment variables override profile values:

| Variable | Default | Description |
|---|---|---|
| `DSIMAGING_PROFILE` | configured `default_profile`, then `default` | Config profile |
| `DSIMAGING_ENDPOINT` | `http://127.0.0.1:9000` | S3/MinIO endpoint |
| `DSIMAGING_CONTROLLER_URL` | (empty) | dsimaging-store controller URL |
| `DSIMAGING_CONTROLLER_TOKEN` | (empty) | Bearer token for controller inventory and manual reconcile |
| `DSIMAGING_ACCESS_KEY` | unset | S3 access key (required for local MinIO) |
| `DSIMAGING_SECRET_KEY` | unset | S3 secret key (required for local MinIO) |
| `DSIMAGING_BUCKET` | `imaging-data` | Bucket name |
| `DSIMAGING_REGION` | (empty) | S3 region |
| `DSIMAGING_BACKEND` | `auto` | Backend override: `auto`, `minio`, `aws` or `s3-compatible` |

`store init` generates a controller token in the project's `.env`, whose mode
is set to `0600`. Pass that value through `DSIMAGING_CONTROLLER_TOKEN` (or the
profile key above) when using `dataset list`, `dataset status`, `dataset
reconcile`, `doctor`, or the operator UI with the controller. The controller's
unauthenticated health endpoint reports only coarse liveness; inventory and
manual reconciliation remain disabled until a token is configured. For a
manually deployed MinIO or AWS controller, generate and configure the same
long random value on both sides.

Generated local Compose projects publish the controller on `127.0.0.1` only;
MinIO delivers notifications over the internal Compose network.

`dataset verify` checks the published manifest and its pinned patient contract,
requires every URI to remain under the collection's own prefix, cross-checks
the exact image/metadata/sample-manifest roster and row contents, validates mask
mappings, sizes and selected object hashes, and refuses verification while a
publish lock exists. Missing, extra, duplicate, orphaned, cross-collection, or
corrupt publication metadata makes verification fail rather than accepting a
reduced view of the collection.

Fractional verification rotates its cryptographic sample on every invocation,
so scheduled checks do not retain a permanent blind spot. `--quick` skips a
SHA-256 read only when an immutable recorded S3 version ID still matches; an
ETag alone is never treated as proof of content integrity.

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
