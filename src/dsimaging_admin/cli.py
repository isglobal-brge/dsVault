"""dsimaging-admin CLI."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from urllib.parse import quote

import click

from . import __version__
from .s3 import create_client, list_datasets, list_objects
from .manifest import (
    scan_images, scan_masks, scan_s3_images, scan_s3_masks,
    validate_dataset_id, generate_manifest, write_manifest_yaml,
    build_hash_index, build_mask_hash_index, build_sample_manifests,
    build_samples_metadata, read_metadata_table,
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
    """Resolve CLI defaults as env var > config file > fallback."""
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
    ctx.obj["region"] = region


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
@click.option("--metadata", default=None, type=click.Path(exists=True, dir_okay=False),
              help="Optional CSV/Parquet metadata with a sample_id column")
@click.option("--modality", default="unknown", help="Imaging modality (ct, mri, etc.)")
@click.option("--opal-url", default=None, help="Opal server URL for resource registration")
@click.option("--opal-token", default=None, envvar="OPAL_TOKEN", help="Opal auth token")
@click.option("--opal-user", default=None, envvar="OPAL_USER", help="Opal username")
@click.option("--opal-password", default=None, envvar="OPAL_PASSWORD", help="Opal password")
@click.option("--opal-project", default="IMAGING", help="Opal project name")
@click.option("--opal-resource", default=None, help="Opal resource name (defaults to dataset_id)")
@click.option("--opal-replace", is_flag=True, help="Replace an existing Opal resource")
@click.option("--opal-insecure", is_flag=True, help="Disable TLS certificate verification for Opal")
@click.option("--resource-endpoint", default=None,
              help="Endpoint stored in the Opal resource URL (for container-internal S3 DNS)")
@click.pass_context
def publish(ctx, dataset_id, source, metadata, modality, opal_url, opal_token,
            opal_user, opal_password, opal_project, opal_resource,
            opal_replace, opal_insecure, resource_endpoint):
    """Publish a local dataset to S3/MinIO.

    Scans images, computes hashes, generates manifests and indexes,
    uploads everything to S3, and optionally registers as a DataSHIELD resource.
    """
    try:
        validate_dataset_id(dataset_id)
    except ValueError as e:
        raise click.ClickException(str(e))

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

    click.echo("  Scanning masks...")
    masks = scan_masks(source, sample_ids=[sample["sample_id"] for sample in samples])
    if masks:
        click.echo(f"  Found {len(masks)} masks")
    else:
        click.echo("  No masks found")

    extra_metadata = None
    if metadata:
        click.echo("  Reading metadata...")
        try:
            extra_metadata = read_metadata_table(metadata)
        except ValueError as e:
            raise click.ClickException(str(e)) from e
        click.echo(
            f"  Metadata columns: {', '.join(extra_metadata.column_names)}"
        )

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

    if masks:
        click.echo("  Uploading masks to S3...")
        for mask in masks:
            key = f"{prefix}/source/masks/{mask['uri_path']}"
            s3.upload_file(mask["local_path"], bucket, key)
        click.echo(f"  Uploaded to s3://{bucket}/{prefix}/source/masks/")

    # 3. Generate and upload indexes
    _write_dataset_artifacts(s3, bucket, prefix, dataset_id, modality, samples,
                             masks=masks, extra_metadata=extra_metadata)

    resource_url = _resource_url(dataset_id, resource_endpoint or ctx.obj["endpoint"], bucket,
                                 prefix, ctx.obj.get("region", ""))
    if opal_url:
        click.echo("  Registering Opal resource...")
        _register_opal_resource(
            opal_url=opal_url,
            project=opal_project,
            name=opal_resource or dataset_id,
            resource_url=resource_url,
            access_key=ctx.obj["access_key"],
            secret_key=ctx.obj["secret_key"],
            token=opal_token,
            username=opal_user,
            password=opal_password,
            replace=opal_replace,
            verify=not opal_insecure,
        )

    # 4. Summary
    endpoint = ctx.obj["endpoint"]
    click.echo("")
    click.echo(click.style(f"Dataset '{dataset_id}' published!", fg="green", bold=True))
    click.echo(f"  Location: s3://{bucket}/{prefix}/")
    click.echo(f"  Samples:  {len(samples)}")
    click.echo(f"  Masks:    {len(masks)}")
    click.echo("")
    click.echo("  To use in R:")
    click.echo(f'    ds.imaging.radiomics.extract(conns, dataset_id = "{dataset_id}", ...)')
    click.echo("")
    click.echo("  DataSHIELD resource config:")
    click.echo(f"    URL:      {resource_url}")
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

    try:
        validate_dataset_id(dataset_id)
    except ValueError as e:
        raise click.ClickException(str(e))

    click.echo(f"Rescanning: {dataset_id}")

    objects = list_objects(s3, bucket, f"{prefix}/source/images/")
    click.echo(f"  Found {len(objects)} objects under source/images/")
    mask_objects = list_objects(s3, bucket, f"{prefix}/source/masks/")
    click.echo(f"  Found {len(mask_objects)} objects under source/masks/")

    click.echo("  Computing hashes and rebuilding dataset artifacts...")
    samples = scan_s3_images(s3, bucket, prefix, objects)
    if not samples:
        raise click.ClickException("No supported image objects found.")
    masks = scan_s3_masks(s3, bucket, prefix, mask_objects,
                          sample_ids=[sample["sample_id"] for sample in samples])

    modality = _existing_modality(s3, bucket, prefix, fallback="unknown")
    extra_metadata = _read_existing_samples_metadata(s3, bucket, prefix)
    _write_dataset_artifacts(s3, bucket, prefix, dataset_id, modality, samples,
                             masks=masks, extra_metadata=extra_metadata)

    click.echo(f"  Index updated: {len(samples)} samples, {len(masks)} masks")
    click.echo(click.style("Rescan complete.", fg="green"))


def _write_dataset_artifacts(s3, bucket: str, prefix: str, dataset_id: str,
                             modality: str, samples: list[dict],
                             masks: list[dict] | None = None,
                             extra_metadata=None) -> None:
    import pyarrow.parquet as pq

    masks = masks or []
    with tempfile.TemporaryDirectory() as tmpdir:
        click.echo("  Building content hash index...")
        idx = build_hash_index(samples, bucket, prefix)
        idx_path = os.path.join(tmpdir, "content_hash_index.parquet")
        pq.write_table(idx, idx_path)
        s3.upload_file(idx_path, bucket, f"{prefix}/indexes/content_hash_index.parquet")

        if masks:
            click.echo("  Building mask content hash index...")
            mask_idx = build_mask_hash_index(masks, bucket, prefix)
            mask_idx_path = os.path.join(tmpdir, "masks_content_hash_index.parquet")
            pq.write_table(mask_idx, mask_idx_path)
            s3.upload_file(mask_idx_path, bucket,
                           f"{prefix}/indexes/masks_content_hash_index.parquet")

        click.echo("  Building sample manifests...")
        sm = build_sample_manifests(samples)
        sm_path = os.path.join(tmpdir, "sample_manifests.parquet")
        pq.write_table(sm, sm_path)
        s3.upload_file(sm_path, bucket, f"{prefix}/metadata/sample_manifests.parquet")

        click.echo("  Building samples metadata...")
        meta = build_samples_metadata(samples, extra_metadata=extra_metadata)
        meta_path = os.path.join(tmpdir, "samples.parquet")
        pq.write_table(meta, meta_path)
        s3.upload_file(meta_path, bucket, f"{prefix}/metadata/samples.parquet")

        click.echo("  Building manifest...")
        manifest = generate_manifest(dataset_id, bucket, prefix, modality,
                                     has_masks=bool(masks))
        manifest_path = os.path.join(tmpdir, "manifest.yaml")
        write_manifest_yaml(manifest, manifest_path)
        s3.upload_file(manifest_path, bucket, f"{prefix}/manifest.yaml")


def _read_existing_samples_metadata(s3, bucket: str, prefix: str):
    import pyarrow.parquet as pq

    try:
        response = s3.get_object(Bucket=bucket, Key=f"{prefix}/metadata/samples.parquet")
        body = response["Body"]
        try:
            data = body.read()
        finally:
            body.close()
        if not data:
            return None
        import pyarrow as pa
        return pq.read_table(pa.BufferReader(data))
    except Exception:
        return None


def _existing_modality(s3, bucket: str, prefix: str, fallback: str) -> str:
    try:
        import yaml
        response = s3.get_object(Bucket=bucket, Key=f"{prefix}/manifest.yaml")
        body = response["Body"]
        try:
            manifest = yaml.safe_load(body.read()) or {}
        finally:
            body.close()
        return manifest.get("modality") or fallback
    except Exception:
        return fallback


def _resource_url(dataset_id: str, endpoint: str, bucket: str, prefix: str,
                  region: str = "") -> str:
    parts = [
        f"endpoint={quote(endpoint, safe='')}",
        f"bucket={quote(bucket, safe='')}",
        f"prefix={quote(prefix, safe='')}",
    ]
    if region:
        parts.append(f"region={quote(region, safe='')}")
    return f"imaging+dataset://{dataset_id}?" + "&".join(parts)


def _register_opal_resource(opal_url: str, project: str, name: str,
                            resource_url: str, access_key: str,
                            secret_key: str, token: str | None,
                            username: str | None, password: str | None,
                            replace: bool, verify: bool) -> None:
    try:
        import requests
    except ImportError as e:
        raise click.ClickException("requests is required for Opal registration") from e

    if not verify:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    session = requests.Session()
    session.verify = verify
    headers = {"Accept": "application/json"}
    if token:
        headers["X-Opal-Auth"] = token
    elif username and password:
        session.auth = (username, password)
    else:
        raise click.ClickException(
            "Provide --opal-token or --opal-user/--opal-password for Opal registration."
        )

    base = opal_url.rstrip("/")

    def request(method: str, path: str, **kwargs):
        response = session.request(method, f"{base}/ws/{path.lstrip('/')}",
                                   headers=headers, timeout=30, **kwargs)
        if response.status_code >= 400:
            raise click.ClickException(
                f"Opal {method} {path} failed with {response.status_code}: "
                f"{response.text[:500]}"
            )
        return response

    project_resp = session.get(f"{base}/ws/project/{project}", headers=headers,
                               timeout=30, verify=verify)
    if project_resp.status_code == 404:
        request("POST", "projects", json={"name": project, "title": project})
    elif project_resp.status_code >= 400:
        raise click.ClickException(
            f"Opal project check failed with {project_resp.status_code}: "
            f"{project_resp.text[:500]}"
        )

    resource_resp = session.get(f"{base}/ws/project/{project}/resource/{name}",
                                headers=headers, timeout=30, verify=verify)
    if resource_resp.status_code == 200:
        if not replace:
            click.echo(f"  Opal resource {project}.{name} already exists; keeping it")
            return
        request("DELETE", f"project/{project}/resource/{name}")
    elif resource_resp.status_code != 404:
        raise click.ClickException(
            f"Opal resource check failed with {resource_resp.status_code}: "
            f"{resource_resp.text[:500]}"
        )

    parameters = {"url": resource_url, "format": None, "_package": None}
    credentials = {
        "identity": access_key,
        "identifier": access_key,
        "secret": secret_key,
    }
    payload = {
        "provider": "resourcer",
        "factory": "default",
        "project": project,
        "name": name,
        "description": "dsimaging-store dataset",
        "parameters": json.dumps(parameters),
        "credentials": json.dumps(credentials),
    }
    request("POST", f"project/{project}/resources", json=payload)
    click.echo(click.style(f"  Registered Opal resource {project}.{name}", fg="green"))


if __name__ == "__main__":
    main()
