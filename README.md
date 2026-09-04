# dsimaging-admin

Admin CLI for creating and operating `dsimaging-store` deployments and the
medical imaging datasets stored in them.

## Install

```bash
pip install dsimaging-admin
```

Python 3.10 or newer is required.

## Create a store

The normal local path is one command:

```bash
dsimaging-admin store setup ./study-store
```

It checks Docker Compose before creating anything, creates or validates the
project, starts MinIO and the controller, waits for the complete doctor, and
saves `study-store` as the active profile. Repeating the command is
non-destructive. Afterwards the lifecycle commands can omit their path:

```bash
dsimaging-admin store ps
dsimaging-admin store doctor
dsimaging-admin store down
```

The generated project pins the published multi-architecture controller, MinIO
and MinIO client images by both release tag and manifest-list digest. Use
`--controller-image` only to select a deliberate controller upgrade.
The controller image is also directly available as
`davidsarrat/dsimaging-store:0.3.11` for `linux/amd64` and `linux/arm64`.

New local projects generate unique MinIO credentials and an operator token in
`./study-store/.env` (mode `0600`). MinIO's API, console and controller are
bound to `127.0.0.1` by default. The saved YAML profile contains only the
project path; credentials are resolved from its private `.env` at execution
time and are not copied into `~/.dsimaging.yaml`. No shared MinIO credential is
built in.

For local controller development, build from a checked-out `dsimaging-store`
repo instead of using an image:

```bash
dsimaging-admin store init ./study-store \
  --store-source /path/to/dsimaging-store
dsimaging-admin store up ./study-store
```

For AWS, create a non-secret profile and provision its S3/SQS control plane. It
uses boto3's normal credential chain:

```bash
dsimaging-admin profile add aws-prod \
  --backend aws --region eu-west-1 --bucket my-imaging
dsimaging-admin --profile aws-prod store provision \
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

Existing S3-compatible stores are connect-only:

```bash
dsimaging-admin profile add lab-s3 --backend s3-compatible \
  --endpoint https://s3.example.org --bucket imaging
dsimaging-admin --profile lab-s3 doctor
```

Keep remote credentials in environment variables or the provider credential
chain. Endpoint and controller URLs containing userinfo, query strings, or
fragments are rejected so credentials cannot be hidden in a persisted URL.

## Dataset operations

```bash
# Common path: SOURCE/metadata.csv (or .parquet) is inferred when unique.
dsimaging-admin dataset publish study_ct_v1 /data/study_ct \
  --modality ct

# Advanced metadata/label contract.
dsimaging-admin dataset publish study_ct_v2 /data/study_ct \
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

Ordinary deletion requires bucket versioning and preserves recoverable history.
Permanently removing every object version and delete marker is a separate,
irreversible operator decision:

```bash
dsimaging-admin dataset purge study_ct_v1 --yes --confirm study_ct_v1
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
it remotely. Secret widgets are never prefilled. A local-store profile resolves
its private `.env` only in the server-side process; changing profile, endpoint,
backend, or controller URL clears session-entered credentials for the old
destination. Remote credentials come from the process environment/provider
chain or are entered for that UI session.

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

Recognized source formats are NIfTI (`.nii`, `.nii.gz`), NRRD, MetaImage
(`.mha`, `.mhd`), DICOM, whole-slide SVS, TIFF, PNG, and JPEG. The store is
format-agnostic after publication; an analysis job still needs a compatible
reader for the selected format.

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
The UI preflight fingerprints every image, mask, and metadata file. It rescans
before publishing, while the atomic uploader compares the uploaded hashes with
the local scan and rolls back if a file changes mid-upload.

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

When the metadata contains an exact `patient_id` column it is selected as the
privacy unit; otherwise `--privacy-unit-column` is required. Every discovered
sample must have a non-empty value in that metadata column and, when declared,
in `--label-column`.
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

`~/.dsimaging.yaml` supports multiple non-secret profiles. A local setup stores
only a pointer:

```yaml
default_profile: study-store
profiles:
  study-store:
    kind: local-store
    store_path: /absolute/path/to/study-store
  aws-prod:
    backend: aws
    bucket: imaging-prod
    region: eu-west-1
```

Environment variables override profile values:

| Variable | Default | Description |
|---|---|---|
| `DSIMAGING_PROFILE` | configured `default_profile`, then `default` | Config profile |
| `DSIMAGING_ENDPOINT` | local MinIO URL; empty for AWS | S3/MinIO endpoint |
| `DSIMAGING_CONTROLLER_URL` | (empty) | dsimaging-store controller URL |
| `DSIMAGING_CONTROLLER_TOKEN` | (empty) | Bearer token for controller inventory and manual reconcile |
| `DSIMAGING_ACCESS_KEY` | unset | S3 access key (required for local MinIO) |
| `DSIMAGING_SECRET_KEY` | unset | S3 secret key (required for local MinIO) |
| `DSIMAGING_BUCKET` | `imaging-data` | Bucket name |
| `DSIMAGING_REGION` | (empty) | S3 region |
| `DSIMAGING_BACKEND` | `auto` | Backend override: `auto`, `minio`, `aws` or `s3-compatible` |
| `DSIMAGING_SQS_QUEUE_URL` | profile value, then unset | AWS controller event queue URL |

`store setup` generates a controller token in the project's `.env`, whose mode
is set to `0600`, and resolves it automatically through the local-store pointer.
For a separately deployed controller, pass it through
`DSIMAGING_CONTROLLER_TOKEN`. Tokens are scoped to the exact configured
controller URL and are dropped if a command changes that URL. The controller's
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
