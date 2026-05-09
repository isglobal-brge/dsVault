# dsimaging-admin

Admin CLI for managing medical imaging datasets in S3/MinIO for DataSHIELD.

## Install

```bash
pip install dsimaging-admin
```

## Quick start

```bash
# 1. Publish a local dataset to MinIO
dsimaging-admin --endpoint http://minio:9000 publish \
  --dataset-id lung_ct_v1 \
  --source /data/lung_ct \
  --modality ct

# 2. List published datasets
dsimaging-admin --endpoint http://minio:9000 list

# 3. Check health
dsimaging-admin --endpoint http://minio:9000 doctor

# 4. Re-scan after adding images
dsimaging-admin --endpoint http://minio:9000 rescan --dataset-id lung_ct_v1
```

For repeated use, create `~/.dsimaging.yaml` once:

```bash
dsimaging-admin init
```

## What `publish` does

1. Scans your local image directory (NIfTI, DICOM, NRRD, etc.) and optional
   masks under `source/masks/`, `masks/`, `source/labels/`, or `labels/`
2. Computes SHA-256 content hash for every file
3. Uploads images to `s3://<bucket>/datasets/<dataset_id>/source/images/`
4. Uploads masks, when present, to `s3://<bucket>/datasets/<dataset_id>/source/masks/`
5. Generates and uploads:
   - `manifest.yaml` (dataset descriptor)
   - `content_hash_index.parquet` (dedup index)
   - `masks_content_hash_index.parquet` (mask dedup index, when masks exist)
   - `sample_manifests.parquet` (multi-file sample support)
   - `samples.parquet` (basic metadata)
6. Optionally registers the dataset as an Opal resource
7. Prints the DataSHIELD resource configuration

`publish` can register the resource directly in Opal:

```bash
dsimaging-admin --endpoint http://localhost:9000 publish \
  --dataset-id lung_ct_v1 \
  --source /data/lung_ct \
  --modality ct \
  --opal-url https://opal.example.org \
  --opal-user administrator \
  --opal-password "$OPAL_PASSWORD" \
  --opal-project IMAGING
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `DSIMAGING_ENDPOINT` | `http://127.0.0.1:9000` | S3/MinIO endpoint |
| `DSIMAGING_ACCESS_KEY` | `minioadmin` | S3 access key |
| `DSIMAGING_SECRET_KEY` | `minioadmin123` | S3 secret key |
| `DSIMAGING_BUCKET` | `imaging-data` | Bucket name |
| `DSIMAGING_REGION` | (empty) | S3 region |
| `OPAL_TOKEN` | (empty) | Optional Opal token for `publish --opal-url` |
| `OPAL_USER` | (empty) | Optional Opal username for `publish --opal-url` |
| `OPAL_PASSWORD` | (empty) | Optional Opal password for `publish --opal-url` |

## Rescan

`rescan --dataset-id <id>` rebuilds `content_hash_index.parquet`,
`masks_content_hash_index.parquet` when masks exist, `sample_manifests.parquet`,
`samples.parquet` and `manifest.yaml` from the current contents of
`source/images/` and `source/masks/`. This is the same contract maintained
automatically by the dsimaging-store controller when MinIO webhooks are enabled.

## Dataset layout in S3

```
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
