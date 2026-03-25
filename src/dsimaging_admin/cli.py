"""dsimaging-admin CLI."""

import os
import sys
import tempfile

import click

from . import __version__
from .s3 import create_client, list_datasets, list_objects
from .manifest import (
    scan_images, generate_manifest, write_manifest_yaml,
    build_hash_index, build_sample_manifests, build_samples_metadata,
)


CONFIG_PATH = os.path.expanduser("~/.dsimaging.yaml")


def _load_config() -> dict:
    """Load config from ~/.dsimaging.yaml if it exists."""
    if not os.path.exists(CONFIG_PATH):
        return {}
    try:
        import yaml
        with open(CONFIG_PATH) as f:
            data = yaml.safe_load(f) or {}
        return data.get("default", data)
    except Exception:
        return {}


def _default(envvar: str, config_key: str, fallback: str) -> str:
    """Resolve: env var > config file > hardcoded fallback."""
    val = os.environ.get(envvar, "")
    if val:
        return val
    cfg = _load_config()
    val = cfg.get(config_key, "")
    if val:
        return str(val)
    return fallback


@click.group()
@click.version_option(__version__)
@click.option("--endpoint", default=_default("DSIMAGING_ENDPOINT", "endpoint", "http://127.0.0.1:9000"),
              help="S3/MinIO endpoint URL")
@click.option("--access-key", default=_default("DSIMAGING_ACCESS_KEY", "access_key", "minioadmin"),
              help="S3 access key")
@click.option("--secret-key", default=_default("DSIMAGING_SECRET_KEY", "secret_key", "minioadmin123"),
              help="S3 secret key")
@click.option("--bucket", default=_default("DSIMAGING_BUCKET", "bucket", "imaging-data"),
              help="S3 bucket name")
@click.option("--region", default=_default("DSIMAGING_REGION", "region", ""),
              help="S3 region (empty for MinIO)")
@click.pass_context
def main(ctx, endpoint, access_key, secret_key, bucket, region):
    """Admin CLI for managing medical imaging datasets in S3/MinIO.

    Configuration priority: CLI flags > environment variables > ~/.dsimaging.yaml
    """
    ctx.ensure_object(dict)
    ctx.obj["s3"] = create_client(endpoint, access_key, secret_key, region)
    ctx.obj["bucket"] = bucket
    ctx.obj["endpoint"] = endpoint
    ctx.obj["access_key"] = access_key
    ctx.obj["secret_key"] = secret_key


@main.command("init")
@click.option("--endpoint", prompt="S3/MinIO endpoint", default="http://127.0.0.1:9000")
@click.option("--bucket", prompt="Bucket name", default="imaging-data")
@click.option("--access-key", prompt="Access key", default="minioadmin")
@click.option("--secret-key", prompt="Secret key", hide_input=True, default="minioadmin123")
@click.option("--region", prompt="Region (empty for MinIO)", default="")
def init_config(endpoint, bucket, access_key, secret_key, region):
    """Create ~/.dsimaging.yaml configuration file."""
    import yaml
    config = {
        "default": {
            "endpoint": endpoint,
            "bucket": bucket,
            "access_key": access_key,
            "secret_key": secret_key,
            "region": region,
        }
    }
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(config, f, default_flow_style=False)
    os.chmod(CONFIG_PATH, 0o600)
    click.echo(f"Config saved to {CONFIG_PATH}")


@main.command()
@click.option("--dataset-id", required=True, help="Dataset identifier")
@click.option("--source", required=True, type=click.Path(exists=True),
              help="Local directory containing images")
@click.option("--modality", default="unknown", help="Imaging modality (ct, mri, etc.)")
@click.option("--opal-url", default=None, help="Opal server URL for resource registration")
@click.option("--opal-token", default=None, help="Opal auth token")
@click.option("--opal-project", default="IMAGING", help="Opal project name")
@click.pass_context
def publish(ctx, dataset_id, source, modality, opal_url, opal_token, opal_project):
    """Publish a local dataset to S3/MinIO.

    Scans images, computes hashes, generates manifests and indexes,
    uploads everything to S3, and optionally registers as a DataSHIELD resource.
    """
    import pyarrow.parquet as pq

    s3 = ctx.obj["s3"]
    bucket = ctx.obj["bucket"]
    prefix = f"datasets/{dataset_id}"

    click.echo(f"Publishing dataset: {dataset_id}")
    click.echo(f"  Source: {os.path.abspath(source)}")

    # 1. Scan images
    click.echo("  Scanning images...")
    samples = scan_images(source)
    if not samples:
        click.echo("  ERROR: No image files found.", err=True)
        sys.exit(1)
    click.echo(f"  Found {len(samples)} samples")

    # 2. Upload images
    click.echo("  Uploading images to S3...")
    for sample in samples:
        if sample["source_kind"] == "single_file":
            key = f"{prefix}/source/images/{sample['primary_filename']}"
            s3.upload_file(sample["local_path"], bucket, key)
        elif sample["source_kind"] == "dicom_series":
            base_dir = os.path.dirname(sample["local_path"])
            for f_info in sample["files"]:
                local = os.path.join(base_dir, f_info["path"])
                key = f"{prefix}/source/images/{f_info['path']}"
                s3.upload_file(local, bucket, key)
    click.echo(f"  Uploaded to s3://{bucket}/{prefix}/source/images/")

    # 3. Generate and upload indexes
    with tempfile.TemporaryDirectory() as tmpdir:
        click.echo("  Building content hash index...")
        idx = build_hash_index(samples, bucket, prefix)
        idx_path = os.path.join(tmpdir, "content_hash_index.parquet")
        pq.write_table(idx, idx_path)
        s3.upload_file(idx_path, bucket, f"{prefix}/indexes/content_hash_index.parquet")

        click.echo("  Building sample manifests...")
        sm = build_sample_manifests(samples)
        sm_path = os.path.join(tmpdir, "sample_manifests.parquet")
        pq.write_table(sm, sm_path)
        s3.upload_file(sm_path, bucket, f"{prefix}/metadata/sample_manifests.parquet")

        click.echo("  Building samples metadata...")
        meta = build_samples_metadata(samples)
        meta_path = os.path.join(tmpdir, "samples.parquet")
        pq.write_table(meta, meta_path)
        s3.upload_file(meta_path, bucket, f"{prefix}/metadata/samples.parquet")

        click.echo("  Building manifest...")
        manifest = generate_manifest(dataset_id, bucket, prefix, modality)
        manifest_path = os.path.join(tmpdir, "manifest.yaml")
        write_manifest_yaml(manifest, manifest_path)
        s3.upload_file(manifest_path, bucket, f"{prefix}/manifest.yaml")

    # 4. Summary
    endpoint = ctx.obj["endpoint"]
    click.echo("")
    click.echo(click.style(f"Dataset '{dataset_id}' published!", fg="green", bold=True))
    click.echo(f"  Location: s3://{bucket}/{prefix}/")
    click.echo(f"  Samples:  {len(samples)}")
    click.echo("")
    click.echo("  To use in R:")
    click.echo(f'    ds.radiomics.extract(conns, dataset_id = "{dataset_id}", ...)')
    click.echo("")
    click.echo("  DataSHIELD resource config:")
    click.echo(f"    URL:      imaging+dataset://{dataset_id}")
    click.echo(f"    Endpoint: {endpoint}")
    click.echo(f"    Bucket:   {bucket}")
    click.echo(f"    Prefix:   {prefix}")


@main.command("list")
@click.pass_context
def list_cmd(ctx):
    """List published datasets."""
    datasets = list_datasets(ctx.obj["s3"], ctx.obj["bucket"])
    if not datasets:
        click.echo("No datasets found.")
        return
    click.echo(f"Datasets in s3://{ctx.obj['bucket']}/datasets/:")
    for ds in datasets:
        status_color = "green" if ds["status"] == "published" else "yellow"
        click.echo(f"  {ds['dataset_id']} [{click.style(ds['status'], fg=status_color)}]")


@main.command()
@click.pass_context
def doctor(ctx):
    """Check system health."""
    s3 = ctx.obj["s3"]
    bucket = ctx.obj["bucket"]

    click.echo("dsimaging-admin health check")
    click.echo("=" * 40)

    # 1. Connectivity
    click.echo("\n1. S3 connectivity:")
    try:
        s3.list_buckets()
        click.echo(click.style("   OK", fg="green") + ": Connected")
    except Exception as e:
        click.echo(click.style("   FAIL", fg="red") + f": {e}")
        return

    # 2. Bucket
    click.echo(f"\n2. Bucket '{bucket}':")
    try:
        s3.head_bucket(Bucket=bucket)
        click.echo(click.style("   OK", fg="green") + ": Exists")
    except Exception:
        click.echo(click.style("   FAIL", fg="red") + ": Not found")
        click.echo(f"   Create with: aws s3 mb s3://{bucket} --endpoint-url ...")
        return

    # 3. Versioning
    click.echo("\n3. Versioning:")
    try:
        resp = s3.get_bucket_versioning(Bucket=bucket)
        status = resp.get("Status", "Disabled")
        if status == "Enabled":
            click.echo(click.style("   OK", fg="green") + ": Enabled")
        else:
            click.echo(click.style("   WARN", fg="yellow") + f": {status}")
    except Exception as e:
        click.echo(click.style("   FAIL", fg="red") + f": {e}")

    # 4. Datasets
    click.echo("\n4. Datasets:")
    datasets = list_datasets(s3, bucket)
    for ds in datasets:
        color = "green" if ds["status"] == "published" else "yellow"
        click.echo(f"   {click.style(ds['status'].upper(), fg=color)}: {ds['dataset_id']}")
    if not datasets:
        click.echo("   (none)")

    click.echo(f"\n5. Summary: {len(datasets)} dataset(s)")


@main.command()
@click.option("--dataset-id", required=True)
@click.pass_context
def rescan(ctx, dataset_id):
    """Re-scan and update indexes for a dataset."""
    s3 = ctx.obj["s3"]
    bucket = ctx.obj["bucket"]
    prefix = f"datasets/{dataset_id}"

    click.echo(f"Rescanning: {dataset_id}")

    objects = list_objects(s3, bucket, f"{prefix}/source/images/")
    from .hashing import is_image_file, sample_id_from_filename

    images = [o for o in objects if is_image_file(o["key"].split("/")[-1])]
    click.echo(f"  Found {len(images)} image objects")

    click.echo("  Computing hashes (downloading each file)...")
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = {k: [] for k in [
        "sample_id", "uri", "content_hash", "size",
        "last_modified", "version_id", "etag", "source_kind"
    ]}

    with tempfile.TemporaryDirectory() as tmpdir:
        for img in images:
            filename = img["key"].split("/")[-1]
            local = os.path.join(tmpdir, filename)
            s3.download_file(bucket, img["key"], local)
            from .hashing import sha256_file
            ch = sha256_file(local)
            os.unlink(local)

            rows["sample_id"].append(sample_id_from_filename(filename))
            rows["uri"].append(f"s3://{bucket}/{img['key']}")
            rows["content_hash"].append(ch)
            rows["size"].append(img["size"])
            rows["last_modified"].append(img["last_modified"])
            rows["version_id"].append(None)
            rows["etag"].append(None)
            rows["source_kind"].append("single_file")

        idx_path = os.path.join(tmpdir, "content_hash_index.parquet")
        pq.write_table(pa.table(rows), idx_path)
        s3.upload_file(idx_path, bucket, f"{prefix}/indexes/content_hash_index.parquet")

    click.echo(f"  Index updated: {len(images)} entries")
    click.echo(click.style("Rescan complete.", fg="green"))


if __name__ == "__main__":
    main()
