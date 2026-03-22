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

## What `publish` does

1. Scans your local image directory (NIfTI, DICOM, NRRD, etc.)
2. Computes SHA-256 content hash for every file
3. Uploads images to `s3://<bucket>/datasets/<dataset_id>/source/images/`
4. Generates and uploads:
   - `manifest.yaml` (dataset descriptor)
   - `content_hash_index.parquet` (dedup index)
   - `sample_manifests.parquet` (multi-file sample support)
   - `samples.parquet` (basic metadata)
5. Prints the DataSHIELD resource configuration

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `DSIMAGING_ENDPOINT` | `http://127.0.0.1:9000` | S3/MinIO endpoint |
| `DSIMAGING_ACCESS_KEY` | `minioadmin` | S3 access key |
| `DSIMAGING_SECRET_KEY` | `minioadmin123` | S3 secret key |
| `DSIMAGING_BUCKET` | `imaging-data` | Bucket name |
| `DSIMAGING_REGION` | (empty) | S3 region |

## Dataset layout in S3

```
s3://<bucket>/datasets/<dataset_id>/
  manifest.yaml
  metadata/
    samples.parquet
    sample_manifests.parquet
  indexes/
    content_hash_index.parquet
  source/
    images/
  derived/
  qc/
```
