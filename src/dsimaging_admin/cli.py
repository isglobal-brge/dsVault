"""dsimaging-admin CLI."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import uuid
from pathlib import Path

import click

from . import __version__
from . import controller as controller_api
from .s3 import (
    copy_object,
    create_client,
    delete_keys,
    delete_object_versions,
    detect_backend,
    get_object_bytes,
    head_object,
    list_datasets,
    list_object_versions,
    list_objects,
    provision_aws_store,
    put_object_bytes,
)
from .manifest import (
    build_hash_index,
    build_mask_hash_index,
    build_sample_manifests,
    build_samples_metadata,
    generate_manifest,
    read_metadata_table,
    scan_images,
    scan_masks,
    scan_s3_images,
    scan_s3_masks,
    validate_dataset_id,
    validate_dicom_series,
    write_manifest_yaml,
)
from .store import (
    DEFAULT_CONTROLLER_IMAGE,
    compose_down,
    compose_logs,
    compose_ps,
    compose_up,
    init_store,
    load_store_config,
    store_doctor,
)
from .verify import verify_dataset


CONFIG_PATH = os.path.expanduser("~/.dsimaging.yaml")
PUBLISH_LOCK = ".publish-lock"


def _load_all_config() -> dict:
    if not os.path.exists(CONFIG_PATH):
        return {}
    try:
        import yaml
        with open(CONFIG_PATH, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _load_config(profile: str | None = None) -> dict:
    data = _load_all_config()
    if not data:
        return {}
    selected = profile or data.get("default_profile") or "default"
    profiles = data.get("profiles")
    if isinstance(profiles, dict):
        return profiles.get(selected, {})
    if selected == "default":
        return data.get("default", data)
    return {}


def _write_config_profile(profile: str, values: dict, set_default: bool = True) -> None:
    import yaml

    data = _load_all_config()
    if "profiles" not in data:
        existing = data.get("default", data) if data else {}
        data = {"profiles": {}}
        if existing:
            data["profiles"]["default"] = existing
    data.setdefault("profiles", {})[profile] = values
    if set_default:
        data["default_profile"] = profile
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)
    os.chmod(CONFIG_PATH, 0o600)


def _resolve(cli_value: str | None, envvar: str, cfg: dict,
             key: str, fallback: str) -> str:
    if cli_value not in (None, ""):
        return str(cli_value)
    val = os.environ.get(envvar, "")
    if val:
        return val
    val = cfg.get(key, "")
    if val not in (None, ""):
        return str(val)
    return fallback


def _resolve_optional(cli_value: str | None, envvar: str, cfg: dict,
                      key: str) -> str | None:
    if cli_value not in (None, ""):
        return str(cli_value)
    val = os.environ.get(envvar, "")
    if val:
        return val
    val = cfg.get(key, "")
    if val not in (None, ""):
        return str(val)
    return None


def _resolve_backend(cli_value: str | None, cfg: dict) -> str:
    if cli_value not in (None, ""):
        return str(cli_value)
    val = os.environ.get("DSIMAGING_BACKEND", "")
    if val:
        return val
    val = cfg.get("backend", "")
    if val:
        return str(val)
    return "auto"


def _persist_aws_queue_url(profile: str, queue_url: str, bucket: str | None = None,
                           region: str | None = None,
                           endpoint: str | None = None) -> None:
    data = _load_all_config()
    if "profiles" in data and isinstance(data["profiles"], dict):
        profiles = data.setdefault("profiles", {})
        target = profiles.setdefault(profile or data.get("default_profile") or "default", {})
        data.setdefault("default_profile", profile or data.get("default_profile") or "default")
    else:
        profile_name = profile or "default"
        existing = data.get("default", data) if data else {}
        data = {"default_profile": profile_name, "profiles": {profile_name: existing}}
        target = data["profiles"][profile_name]
    target["backend"] = "aws"
    if bucket:
        target["bucket"] = bucket
    if region:
        target["region"] = region
    if endpoint:
        target["endpoint"] = endpoint
    target.setdefault("aws", {})["sqs_queue_url"] = queue_url
    import yaml
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)
    os.chmod(CONFIG_PATH, 0o600)


def _echo_json(payload: dict | list) -> None:
    click.echo(json.dumps(payload, indent=2, sort_keys=True, default=str))


def _warn_dataset_alias(command: str) -> None:
    click.echo(
        f"dsimaging-admin {command}: deprecated; "
        f"use 'dsimaging-admin dataset {command}' instead",
        err=True,
    )


class _DeprecatedDatasetAlias(click.Command):
    def __init__(self, target: click.Command, alias_name: str):
        super().__init__(
            name=alias_name,
            params=target.params,
            callback=target.callback,
            help=(target.help or "").rstrip()
            + "\n\nDeprecated alias; use "
            + f"'dsimaging-admin dataset {alias_name}' instead.",
            short_help=f"Deprecated alias for dataset {alias_name}.",
            context_settings=target.context_settings,
        )
        self._alias_name = alias_name

    def invoke(self, ctx):
        _warn_dataset_alias(self._alias_name)
        return super().invoke(ctx)


@click.group()
@click.version_option(__version__)
@click.option("--profile", default=lambda: os.environ.get("DSIMAGING_PROFILE", "default"),
              help="Configuration profile in ~/.dsimaging.yaml")
@click.option("--endpoint", default=None, help="S3/MinIO endpoint URL")
@click.option("--access-key", default=None, help="S3 access key")
@click.option("--secret-key", default=None, help="S3 secret key")
@click.option("--bucket", default=None, help="S3 bucket name")
@click.option("--region", default=None, help="S3 region (empty for MinIO)")
@click.option("--controller-url", default=None,
              help="dsimaging-store controller URL")
@click.option("--skip-controller", is_flag=True,
              help="Do not call the dsimaging-store controller")
@click.option("--backend",
              type=click.Choice(["auto", "minio", "aws", "s3-compatible"]),
              default=None,
              help="Storage backend override")
@click.pass_context
def main(ctx, profile, endpoint, access_key, secret_key, bucket, region,
         controller_url, skip_controller, backend):
    """Admin CLI for managing dsimaging-store deployments and datasets."""
    cfg = _load_config(profile)
    backend = _resolve_backend(backend, cfg)
    raw_endpoint = _resolve_optional(endpoint, "DSIMAGING_ENDPOINT", cfg, "endpoint")
    endpoint = raw_endpoint or ("" if backend == "aws" else "http://127.0.0.1:9000")
    resolved_backend, backend_rationale = detect_backend(endpoint or None, backend)
    if resolved_backend == "aws":
        access_key = _resolve_optional(access_key, "DSIMAGING_ACCESS_KEY", cfg, "access_key")
        secret_key = _resolve_optional(secret_key, "DSIMAGING_SECRET_KEY", cfg, "secret_key")
    else:
        access_key = _resolve(access_key, "DSIMAGING_ACCESS_KEY", cfg, "access_key",
                              "minioadmin")
        secret_key = _resolve(secret_key, "DSIMAGING_SECRET_KEY", cfg, "secret_key",
                              "minioadmin123")
    bucket = _resolve(bucket, "DSIMAGING_BUCKET", cfg, "bucket", "imaging-data")
    region = _resolve(region, "DSIMAGING_REGION", cfg, "region", "")
    controller_url = _resolve(controller_url, "DSIMAGING_CONTROLLER_URL", cfg,
                              "controller_url", "")

    ctx.ensure_object(dict)
    ctx.obj["s3"] = create_client(endpoint, access_key, secret_key, region)
    ctx.obj["bucket"] = bucket
    ctx.obj["endpoint"] = endpoint
    ctx.obj["access_key"] = access_key
    ctx.obj["secret_key"] = secret_key
    ctx.obj["region"] = region
    ctx.obj["backend"] = resolved_backend
    ctx.obj["backend_rationale"] = backend_rationale
    ctx.obj["profile"] = profile
    ctx.obj["controller_url"] = controller_url
    ctx.obj["skip_controller"] = skip_controller


@main.command("init")
@click.option("--profile", default="default", help="Profile name to write")
@click.option("--endpoint", prompt="S3/MinIO endpoint", default="http://127.0.0.1:9000")
@click.option("--bucket", prompt="Bucket name", default="imaging-data")
@click.option("--access-key", prompt="Access key", default="minioadmin")
@click.option("--secret-key", prompt="Secret key", hide_input=True, default="minioadmin123")
@click.option("--region", prompt="Region (empty for MinIO)", default="")
@click.option("--controller-url", prompt="Controller URL", default="http://127.0.0.1:8080")
@click.option("--set-default/--no-set-default", default=True,
              help="Make this profile the default profile")
def init_config(profile, endpoint, bucket, access_key, secret_key, region,
                controller_url, set_default):
    """Create or update a ~/.dsimaging.yaml profile."""
    _write_config_profile(profile, {
        "endpoint": endpoint,
        "bucket": bucket,
        "access_key": access_key,
        "secret_key": secret_key,
        "region": region,
        "controller_url": controller_url,
    }, set_default=set_default)
    click.echo(f"Config profile '{profile}' saved to {CONFIG_PATH}")


@main.group("store")
def store_group():
    """Create and operate local dsimaging-store Compose projects."""


@main.group("dataset")
def dataset_group():
    """Manage datasets inside an imaging store."""


@main.group("ui")
def ui_group():
    """Launch the local operator dashboard."""


@ui_group.command("launch")
@click.option("--host", default="127.0.0.1", show_default=True,
              help="Interface to bind the local dashboard server")
@click.option("--port", default=8501, show_default=True,
              help="Port for the local dashboard server")
@click.option("--open-browser", is_flag=True,
              help="Ask Streamlit to open a browser window")
def ui_launch(host, port, open_browser):
    """Open the Streamlit operator dashboard."""
    if host not in ("127.0.0.1", "localhost"):
        click.echo(
            "WARNING: dsimaging-admin ui is an unauthenticated local operator tool; "
            f"binding to {host} may expose storage administration controls.",
            err=True,
        )
    try:
        from .ui import launch_ui
    except ImportError as e:
        raise click.ClickException(str(e)) from e
    launch_ui(host=host, port=port, open_browser=open_browser)


@store_group.command("init")
@click.argument("path", required=False, default=".", type=click.Path(file_okay=False))
@click.option("--force", is_flag=True, help="Overwrite generated store files")
@click.option("--controller-image", default=DEFAULT_CONTROLLER_IMAGE,
              show_default=True, help="Controller image for image-based stores")
@click.option("--store-source", default=None, type=click.Path(file_okay=False),
              help="Use a local dsimaging-store checkout for controller builds")
@click.option("--backend", type=click.Choice(["auto", "minio", "aws", "s3-compatible"]),
              default=None, help="Backend override for provisioning")
@click.option("--kms-key", default=None,
              help="AWS KMS key ARN for SSE-KMS; empty uses SSE-S3 AES256")
@click.option("--access-key", default=None, help="S3 access key override")
@click.option("--secret-key", default=None, help="S3 secret key override")
@click.option("--bucket", default=None, help="Bucket name override")
@click.option("--minio-port", default=9000, show_default=True)
@click.option("--console-port", default=9001, show_default=True)
@click.option("--controller-port", default=8080, show_default=True)
@click.option("--reconcile-interval", default=10, show_default=True)
@click.pass_context
def store_init(ctx, path, force, controller_image, store_source, backend, kms_key,
               access_key, secret_key, bucket, minio_port, console_port,
               controller_port,
               reconcile_interval):
    """Provision a store: MinIO Compose locally, or AWS S3/SQS on AWS.

    AWS mode requires IAM permissions for s3:CreateBucket,
    s3:PutBucketVersioning, s3:PutBucketEncryption,
    s3:PutBucketNotification, sqs:CreateQueue and sqs:SetQueueAttributes.
    """
    bucket = bucket or ctx.obj["bucket"]
    backend_override = backend or ctx.obj.get("backend") or "auto"
    resolved_backend, rationale = detect_backend(ctx.obj.get("endpoint") or None,
                                                 backend_override)
    if resolved_backend == "aws":
        aws_endpoint = ctx.obj.get("endpoint") or None
        if aws_endpoint and "amazonaws.com" not in aws_endpoint.lower():
            aws_endpoint = None
        try:
            report = provision_aws_store(
                aws_endpoint,
                bucket,
                region=ctx.obj.get("region") or "us-east-1",
                access_key=access_key if access_key not in (None, "") else ctx.obj.get("access_key"),
                secret_key=secret_key if secret_key not in (None, "") else ctx.obj.get("secret_key"),
                kms_key=kms_key,
            )
        except Exception as e:
            raise click.ClickException(str(e)) from e
        click.echo("AWS store provisioning")
        click.echo(f"  Backend: {resolved_backend} ({rationale})")
        click.echo(f"  Bucket:  {bucket}")
        click.echo(f"  Region:  {report['region']}")
        for step in report["steps"]:
            color = "green" if step["status"] == "ok" else "yellow"
            click.echo(
                f"  {click.style(step['status'].upper(), fg=color)} "
                f"{step['name']}: {step['detail']}"
            )
        if report.get("sqs_queue_url"):
            _persist_aws_queue_url(ctx.obj.get("profile") or "default",
                                   report["sqs_queue_url"],
                                   bucket=bucket,
                                   region=report["region"],
                                   endpoint=aws_endpoint)
            click.echo(f"  SQS queue URL saved to {CONFIG_PATH}")
        return

    access_key = access_key or ctx.obj.get("access_key") or "minioadmin"
    secret_key = secret_key or ctx.obj.get("secret_key") or "minioadmin123"
    try:
        cfg = init_store(
            path,
            force=force,
            controller_image=controller_image,
            store_source=store_source,
            access_key=access_key,
            secret_key=secret_key,
            bucket=bucket,
            minio_port=minio_port,
            console_port=console_port,
            controller_port=controller_port,
            reconcile_interval=reconcile_interval,
        )
    except Exception as e:
        raise click.ClickException(str(e)) from e
    click.echo(f"Store initialized at {cfg.path}")
    click.echo(f"  S3 endpoint:    {cfg.endpoint}")
    click.echo(f"  Controller URL: {cfg.controller_url}")
    click.echo(f"  Bucket:         {cfg.bucket}")


@store_group.command("up")
@click.argument("path", default=".", type=click.Path(file_okay=False))
@click.option("--no-build", is_flag=True, help="Do not pass --build to docker compose")
def store_up(path, no_build):
    """Start a store."""
    _echo_command_output(compose_up(path, build=not no_build))


@store_group.command("down")
@click.argument("path", default=".", type=click.Path(file_okay=False))
@click.option("--volumes", is_flag=True, help="Remove store volumes")
def store_down(path, volumes):
    """Stop a store."""
    _echo_command_output(compose_down(path, volumes=volumes))


@store_group.command("ps")
@click.argument("path", default=".", type=click.Path(file_okay=False))
def store_ps(path):
    """Show store containers."""
    _echo_command_output(compose_ps(path))


@store_group.command("logs")
@click.argument("path", default=".", type=click.Path(file_okay=False))
@click.option("--service", default=None, help="Service name")
@click.option("--tail", default=100, show_default=True)
def store_logs(path, service, tail):
    """Show store logs."""
    _echo_command_output(compose_logs(path, service=service, tail=tail))


@store_group.command("config")
@click.argument("path", default=".", type=click.Path(file_okay=False))
@click.option("--output", type=click.Choice(["text", "json"]), default="text")
def store_config(path, output):
    """Print connection details for a store directory."""
    cfg = load_store_config(path)
    if output == "json":
        _echo_json(cfg.to_dict())
        return
    click.echo(f"Path:           {cfg.path}")
    click.echo(f"S3 endpoint:    {cfg.endpoint}")
    click.echo(f"Controller URL: {cfg.controller_url}")
    click.echo(f"Bucket:         {cfg.bucket}")
    click.echo(f"Access key:     {cfg.access_key}")


@store_group.command("doctor")
@click.argument("path", default=".", type=click.Path(file_okay=False))
@click.option("--output", type=click.Choice(["text", "json"]), default="text")
def store_doctor_cmd(path, output):
    """Check Docker, controller and S3 health for a store directory."""
    result = store_doctor(path)
    if output == "json":
        _echo_json(result)
        return
    click.echo("dsimaging-store health check")
    click.echo("=" * 32)
    for name in ("docker", "controller", "s3"):
        item = result[name]
        status = "OK" if item.get("ok") else "FAIL"
        color = "green" if item.get("ok") else "red"
        detail = item.get("error") or item.get("bucket") or ""
        click.echo(f"{click.style(status, fg=color)} {name}: {detail}")
    if not result["ok"]:
        raise click.ClickException("store health check failed")


@dataset_group.command("publish")
@click.option("--dataset-id", required=True, help="Dataset identifier")
@click.option("--source", required=True, type=click.Path(exists=True),
              help="Local directory containing images")
@click.option("--metadata", default=None, type=click.Path(exists=True, dir_okay=False),
              help="Optional CSV/Parquet metadata with a sample_id column")
@click.option("--modality", default="unknown", help="Imaging modality (ct, mri, etc.)")
@click.option("--atomic/--no-atomic", default=True,
              help="Publish through a staging prefix and publish lock")
@click.option("--no-skip", is_flag=True,
              help="Upload objects even when the current dataset index already matches")
@click.option("--dry-run", is_flag=True, help="Scan and plan without S3 writes")
@click.option("--skip-dicom-checks", is_flag=True, help="Skip pre-publish DICOM sanity checks")
@click.pass_context
def publish(ctx, dataset_id, source, metadata, modality, atomic, no_skip, dry_run,
            skip_dicom_checks):
    """Publish a local dataset to S3/MinIO."""
    try:
        validate_dataset_id(dataset_id)
    except ValueError as e:
        raise click.ClickException(str(e)) from e

    s3 = ctx.obj["s3"]
    bucket = ctx.obj["bucket"]
    prefix = f"datasets/{dataset_id}"

    click.echo(f"Publishing dataset: {dataset_id}")
    click.echo(f"  Source: {os.path.abspath(source)}")

    click.echo("  Scanning images...")
    samples = scan_images(source)
    if not samples:
        raise click.ClickException("No image files found.")
    click.echo(f"  Found {len(samples)} samples")

    if not skip_dicom_checks:
        warnings = validate_dicom_series(source)
        for warning in warnings:
            click.echo(click.style(f"  WARN: {warning}", fg="yellow"))

    click.echo("  Scanning masks...")
    masks = scan_masks(source, sample_ids=[sample["sample_id"] for sample in samples])
    click.echo(f"  Found {len(masks)} masks" if masks else "  No masks found")

    extra_metadata = None
    if metadata:
        click.echo("  Reading metadata...")
        try:
            extra_metadata = read_metadata_table(metadata)
        except ValueError as e:
            raise click.ClickException(str(e)) from e
        click.echo(f"  Metadata columns: {', '.join(extra_metadata.column_names)}")

    skip_enabled = not no_skip
    sample_uploads, sample_skips = _partition_uploads(s3, bucket, prefix, samples,
                                                      source_path="images",
                                                      enabled=skip_enabled)
    mask_uploads, mask_skips = _partition_uploads(s3, bucket, prefix, masks,
                                                  source_path="masks",
                                                  enabled=skip_enabled)

    click.echo(f"  Images to upload: {len(sample_uploads)}; skipped: {len(sample_skips)}")
    click.echo(f"  Masks to upload:  {len(mask_uploads)}; skipped: {len(mask_skips)}")
    if dry_run:
        click.echo(click.style("Dry run complete; no S3 writes performed.", fg="green"))
        return

    if atomic:
        _atomic_upload_sources(s3, bucket, prefix, sample_uploads, mask_uploads)
    else:
        _upload_sources(s3, bucket, prefix, sample_uploads, mask_uploads)

    _write_dataset_artifacts(s3, bucket, prefix, dataset_id, modality, samples,
                             masks=masks, extra_metadata=extra_metadata)

    click.echo("")
    click.echo(click.style(f"Dataset '{dataset_id}' published!", fg="green", bold=True))
    click.echo(f"  Location: s3://{bucket}/{prefix}/")
    click.echo(f"  Samples:  {len(samples)}")
    click.echo(f"  Masks:    {len(masks)}")


@dataset_group.command("list")
@click.option("--output", type=click.Choice(["text", "json"]), default="text")
@click.option("--controller-url", default=None, help="Override controller URL")
@click.option("--skip-controller", is_flag=True, help="Use S3 only")
@click.pass_context
def list_cmd(ctx, output, controller_url, skip_controller):
    """List datasets."""
    datasets = list_datasets(ctx.obj["s3"], ctx.obj["bucket"])
    ctrl_url = controller_url or ctx.obj.get("controller_url")
    if ctrl_url and not (skip_controller or ctx.obj.get("skip_controller")):
        try:
            datasets = _merge_controller_datasets(
                datasets, controller_api.datasets(ctrl_url)
            )
        except Exception as e:
            if output == "text":
                click.echo(click.style(f"WARN: controller unavailable: {e}", fg="yellow"))

    if output == "json":
        _echo_json({"datasets": datasets})
        return
    if not datasets:
        click.echo("No datasets found.")
        return
    click.echo(f"Datasets in s3://{ctx.obj['bucket']}/datasets/:")
    for ds in datasets:
        status_color = "green" if ds["status"] == "published" else "yellow"
        extras = []
        if ds.get("dirty"):
            extras.append(click.style("dirty", fg="yellow"))
        if ds.get("last_error"):
            extras.append(click.style("error", fg="red"))
        suffix = f" ({', '.join(extras)})" if extras else ""
        click.echo(f"  {ds['dataset_id']} [{click.style(ds['status'], fg=status_color)}]{suffix}")


@main.command()
@click.option("--controller-url", default=None, help="Override controller URL")
@click.option("--skip-controller", is_flag=True, help="Skip controller checks")
@click.option("--output", type=click.Choice(["text", "json"]), default="text")
@click.pass_context
def doctor(ctx, controller_url, skip_controller, output):
    """Check S3 and dsimaging-store health."""
    result = _doctor_result(
        ctx,
        controller_url=controller_url,
        skip_controller=skip_controller,
    )
    if output == "json":
        _echo_json(result)
        return
    click.echo("dsimaging-admin health check")
    click.echo("=" * 40)
    for check in result["checks"]:
        color = {"OK": "green", "WARN": "yellow", "FAIL": "red"}[check["status"]]
        click.echo(f"{click.style(check['status'], fg=color)} {check['name']}: {check['detail']}")
    if not result["ok"]:
        raise click.ClickException("health check failed")


@dataset_group.command("status")
@click.argument("dataset_id")
@click.option("--controller-url", default=None, help="Override controller URL")
@click.option("--skip-controller", is_flag=True, help="Skip controller state")
@click.option("--output", type=click.Choice(["text", "json"]), default="text")
@click.pass_context
def status(ctx, dataset_id, controller_url, skip_controller, output):
    """Show detailed status for one dataset."""
    _validate_dataset_cli(dataset_id)
    payload = _dataset_status(
        ctx.obj["s3"], ctx.obj["bucket"], dataset_id,
        controller_url=controller_url or ctx.obj.get("controller_url"),
        skip_controller=skip_controller or ctx.obj.get("skip_controller"),
    )
    if output == "json":
        _echo_json(payload)
        return
    click.echo(f"Dataset: {dataset_id}")
    click.echo(f"  Status:       {payload['status']}")
    click.echo(f"  Images:       {payload['objects']['images']}")
    click.echo(f"  Masks:        {payload['objects']['masks']}")
    click.echo(f"  Hash rows:    {payload['indexes'].get('content_hash_rows')}")
    click.echo(f"  Mask rows:    {payload['indexes'].get('mask_hash_rows')}")
    manifest = payload.get("manifest") or {}
    click.echo(f"  Schema:       {manifest.get('schema_version', '-')}")
    if payload.get("controller"):
        ctrl = payload["controller"]
        click.echo(f"  Dirty:        {ctrl.get('dirty', False)}")
        if ctrl.get("last_error"):
            click.echo(f"  Last error:   {ctrl['last_error']}")


@dataset_group.command("verify")
@click.argument("dataset_id")
@click.option("--sample-fraction", default=1.0, show_default=True, type=float)
@click.option("--quick", is_flag=True, help="Trust unchanged S3 ETags as a quick check")
@click.option("--output", type=click.Choice(["text", "json"]), default="text")
@click.pass_context
def verify_cmd(ctx, dataset_id, sample_fraction, quick, output):
    """Verify source objects against content hash indexes."""
    _validate_dataset_cli(dataset_id)
    try:
        result = verify_dataset(
            ctx.obj["s3"], ctx.obj["bucket"], dataset_id,
            sample_fraction=sample_fraction, quick=quick,
        )
    except Exception as e:
        raise click.ClickException(str(e)) from e
    payload = result.to_dict()
    if output == "json":
        _echo_json(payload)
    else:
        status_text = "OK" if result.ok else "DRIFT"
        color = "green" if result.ok else "red"
        click.echo(f"{click.style(status_text, fg=color)} {dataset_id}: "
                   f"checked={result.checked}, skipped={result.skipped}, "
                   f"missing={result.missing}, mismatch={result.mismatched}, "
                   f"extra={result.extra}, quick_ok={result.quick_ok}")
        for issue in result.issues[:50]:
            click.echo(f"  {issue.issue}: {issue.sample_id or issue.uri} - {issue.detail}")
        if len(result.issues) > 50:
            click.echo(f"  ... {len(result.issues) - 50} more issue(s)")
    if not result.ok:
        sys.exit(2)


@dataset_group.command("delete")
@click.argument("dataset_id")
@click.option("--yes", is_flag=True, help="Confirm deletion")
@click.option("--purge-versions", is_flag=True,
              help="Delete all S3 versions and delete markers under the dataset")
@click.pass_context
def delete_cmd(ctx, dataset_id, yes, purge_versions):
    """Delete a dataset prefix from S3."""
    _validate_dataset_cli(dataset_id)
    if not yes:
        raise click.ClickException("Refusing to delete without --yes")
    s3 = ctx.obj["s3"]
    bucket = ctx.obj["bucket"]
    prefix = f"datasets/{dataset_id}/"
    objects = list_objects(s3, bucket, prefix)
    current_deleted = delete_keys(s3, bucket, [obj["key"] for obj in objects])
    version_deleted = 0
    if purge_versions:
        versions = list_object_versions(s3, bucket, prefix)
        version_deleted = delete_object_versions(s3, bucket, versions)
    click.echo(click.style(f"Deleted dataset '{dataset_id}'", fg="green"))
    click.echo(f"  Current objects: {current_deleted}")
    if purge_versions:
        click.echo(f"  Versions/delete markers: {version_deleted}")


@dataset_group.command("download")
@click.argument("dataset_id")
@click.argument("dest", type=click.Path(file_okay=False))
@click.option("--overwrite", is_flag=True, help="Overwrite existing local files")
@click.pass_context
def download(ctx, dataset_id, dest, overwrite):
    """Download a dataset prefix for inspection."""
    _validate_dataset_cli(dataset_id)
    s3 = ctx.obj["s3"]
    bucket = ctx.obj["bucket"]
    prefix = f"datasets/{dataset_id}/"
    objects = list_objects(s3, bucket, prefix)
    if not objects:
        raise click.ClickException("dataset has no objects")
    root = Path(dest).expanduser().resolve()
    for obj in objects:
        rel = obj["key"][len(prefix):]
        target = root / rel
        if target.exists() and not overwrite:
            raise click.ClickException(f"Refusing to overwrite {target}; use --overwrite")
        target.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, obj["key"], str(target))
    click.echo(click.style(f"Downloaded {len(objects)} object(s) to {root}", fg="green"))


@dataset_group.command("copy")
@click.argument("src_id")
@click.argument("dst_id")
@click.option("--yes", is_flag=True, help="Confirm copy")
@click.option("--replace", is_flag=True, help="Delete destination first if it exists")
@click.pass_context
def copy_cmd(ctx, src_id, dst_id, yes, replace):
    """Server-side copy a dataset's source objects and rebuild artifacts."""
    _validate_dataset_cli(src_id)
    _validate_dataset_cli(dst_id)
    if not yes:
        raise click.ClickException("Refusing to copy without --yes")
    s3 = ctx.obj["s3"]
    bucket = ctx.obj["bucket"]
    src_prefix = f"datasets/{src_id}"
    dst_prefix = f"datasets/{dst_id}"
    existing = list_objects(s3, bucket, f"{dst_prefix}/")
    if existing and not replace:
        raise click.ClickException("destination exists; use --replace")
    if existing:
        delete_keys(s3, bucket, [obj["key"] for obj in existing])
    sources = list_objects(s3, bucket, f"{src_prefix}/source/")
    if not sources:
        raise click.ClickException("source dataset has no source objects")
    for obj in sources:
        suffix = obj["key"][len(src_prefix):]
        copy_object(s3, bucket, obj["key"], f"{dst_prefix}{suffix}")
    objects = list_objects(s3, bucket, f"{dst_prefix}/source/images/")
    masks_objects = list_objects(s3, bucket, f"{dst_prefix}/source/masks/")
    samples = scan_s3_images(s3, bucket, dst_prefix, objects)
    masks = scan_s3_masks(s3, bucket, dst_prefix, masks_objects,
                          sample_ids=[sample["sample_id"] for sample in samples])
    modality = _existing_modality(s3, bucket, src_prefix, fallback="unknown")
    extra_metadata = _read_existing_samples_metadata(s3, bucket, src_prefix)
    _write_dataset_artifacts(s3, bucket, dst_prefix, dst_id, modality, samples,
                             masks=masks, extra_metadata=extra_metadata)
    click.echo(click.style(f"Copied {src_id} -> {dst_id}", fg="green"))
    click.echo(f"  Source objects: {len(sources)}")
    click.echo(f"  Samples:        {len(samples)}")
    click.echo(f"  Masks:          {len(masks)}")


@dataset_group.command("rescan")
@click.argument("dataset_id")
@click.pass_context
def rescan(ctx, dataset_id):
    """Re-scan and update indexes for a dataset from current S3 contents."""
    _validate_dataset_cli(dataset_id)
    click.echo(f"Rescanning: {dataset_id}")
    counts = _rescan_dataset_artifacts(ctx.obj["s3"], ctx.obj["bucket"], dataset_id)
    click.echo(f"  Found {counts['objects']} objects under source/images/")
    click.echo(f"  Found {counts['mask_objects']} objects under source/masks/")
    click.echo(f"  Index updated: {counts['samples']} samples, {counts['masks']} masks")
    click.echo(click.style("Rescan complete.", fg="green"))


@dataset_group.command("reconcile")
@click.argument("dataset_id")
@click.option("--controller-url", default=None, help="Override controller URL")
@click.pass_context
def reconcile(ctx, dataset_id, controller_url):
    """Ask a reachable controller to reconcile one dataset."""
    _validate_dataset_cli(dataset_id)
    ctrl_url = controller_url or ctx.obj.get("controller_url")
    if not ctrl_url:
        raise click.ClickException("No controller URL configured")
    try:
        payload = controller_api.reconcile(ctrl_url, dataset_id)
    except Exception as e:
        raise click.ClickException(str(e)) from e
    _echo_json(payload)


for _dataset_alias_name, _dataset_alias_command in {
    "publish": publish,
    "list": list_cmd,
    "status": status,
    "verify": verify_cmd,
    "delete": delete_cmd,
    "download": download,
    "copy": copy_cmd,
    "rescan": rescan,
    "reconcile": reconcile,
}.items():
    main.add_command(
        _DeprecatedDatasetAlias(_dataset_alias_command, _dataset_alias_name),
        _dataset_alias_name,
    )


@dataset_group.command("modify")
@click.argument("dataset_id")
@click.option("--metadata", default=None, type=click.Path(exists=True, dir_okay=False),
              help="Replace metadata/samples.parquet from a CSV/Parquet file")
@click.option("--add-images", default=None, type=click.Path(exists=True, file_okay=False),
              help="Add image objects from a local directory")
@click.option("--add-masks", default=None, type=click.Path(exists=True, file_okay=False),
              help="Add mask objects from a local directory")
@click.option("--dry-run", is_flag=True, help="Plan changes without uploading or rewriting indexes")
@click.option("--yes", is_flag=True,
              help="Confirm metadata replacement without an interactive prompt")
@click.pass_context
def modify(ctx, dataset_id, metadata, add_images, add_masks, dry_run, yes):
    """Modify an existing dataset without deleting current objects."""
    _validate_dataset_cli(dataset_id)
    if not any([metadata, add_images, add_masks]):
        raise click.ClickException("Provide --metadata, --add-images or --add-masks")

    s3 = ctx.obj["s3"]
    bucket = ctx.obj["bucket"]
    prefix = f"datasets/{dataset_id}"
    if not head_object(s3, bucket, f"{prefix}/manifest.yaml"):
        raise click.ClickException(f"Dataset '{dataset_id}' is not published")

    extra_metadata = None
    if metadata:
        try:
            extra_metadata = read_metadata_table(metadata)
        except ValueError as e:
            raise click.ClickException(str(e)) from e
        metadata_exists = bool(head_object(s3, bucket, f"{prefix}/metadata/samples.parquet"))
        if metadata_exists and not (yes or dry_run):
            click.confirm(
                f"Replace metadata for dataset '{dataset_id}'?",
                abort=True,
            )

    sample_uploads: list[dict] = []
    sample_skips: list[dict] = []
    mask_uploads: list[dict] = []
    mask_skips: list[dict] = []
    if add_images:
        samples = scan_images(add_images)
        sample_uploads, sample_skips = _partition_uploads(
            s3, bucket, prefix, samples, source_path="images", enabled=True
        )
    if add_masks:
        existing_objects = list_objects(s3, bucket, f"{prefix}/source/images/")
        existing_samples = scan_s3_images(s3, bucket, prefix, existing_objects)
        sample_ids = [sample["sample_id"] for sample in existing_samples]
        local_sample_ids = [sample["sample_id"] for sample in sample_uploads + sample_skips]
        masks = scan_masks(add_masks, sample_ids=sample_ids + local_sample_ids)
        mask_uploads, mask_skips = _partition_uploads(
            s3, bucket, prefix, masks, source_path="masks", enabled=True
        )

    click.echo(f"Modifying dataset: {dataset_id}")
    if metadata:
        click.echo(f"  Metadata replacement: {os.path.abspath(metadata)}")
    click.echo(f"  Images to upload: {len(sample_uploads)}; skipped: {len(sample_skips)}")
    click.echo(f"  Masks to upload:  {len(mask_uploads)}; skipped: {len(mask_skips)}")
    if dry_run:
        for sample in sample_uploads[:50]:
            click.echo(f"  would upload image: {sample['sample_id']}")
        for mask in mask_uploads[:50]:
            click.echo(f"  would upload mask: {mask['sample_id']}:{mask['primary_filename']}")
        click.echo(click.style("Dry run complete; no S3 writes performed.", fg="green"))
        return

    for sample in sample_uploads:
        _upload_sample(s3, bucket, prefix, sample, "images")
    for mask in mask_uploads:
        _upload_sample(s3, bucket, prefix, mask, "masks")
    counts = _rescan_dataset_artifacts(s3, bucket, dataset_id,
                                       extra_metadata=extra_metadata)
    click.echo(
        click.style(
            f"Dataset '{dataset_id}' modified: "
            f"{counts['samples']} samples, {counts['masks']} masks indexed.",
            fg="green",
        )
    )


def _echo_command_output(output: str) -> None:
    if output:
        click.echo(output)


def _validate_dataset_cli(dataset_id: str) -> None:
    try:
        validate_dataset_id(dataset_id)
    except ValueError as e:
        raise click.ClickException(str(e)) from e


def _rescan_dataset_artifacts(s3, bucket: str, dataset_id: str,
                              extra_metadata=None) -> dict:
    prefix = f"datasets/{dataset_id}"
    objects = list_objects(s3, bucket, f"{prefix}/source/images/")
    mask_objects = list_objects(s3, bucket, f"{prefix}/source/masks/")
    samples = scan_s3_images(s3, bucket, prefix, objects)
    if not samples:
        raise click.ClickException("No supported image objects found.")
    masks = scan_s3_masks(s3, bucket, prefix, mask_objects,
                          sample_ids=[sample["sample_id"] for sample in samples])
    modality = _existing_modality(s3, bucket, prefix, fallback="unknown")
    if extra_metadata is None:
        extra_metadata = _read_existing_samples_metadata(s3, bucket, prefix)
    _write_dataset_artifacts(s3, bucket, prefix, dataset_id, modality, samples,
                             masks=masks, extra_metadata=extra_metadata)
    return {
        "objects": len(objects),
        "mask_objects": len(mask_objects),
        "samples": len(samples),
        "masks": len(masks),
    }


def _partition_uploads(s3, bucket: str, prefix: str, samples: list[dict],
                       source_path: str, enabled: bool) -> tuple[list[dict], list[dict]]:
    if not enabled or not samples:
        return samples, []
    index_key = (
        f"{prefix}/indexes/content_hash_index.parquet"
        if source_path == "images"
        else f"{prefix}/indexes/masks_content_hash_index.parquet"
    )
    existing_hashes = _existing_hashes(s3, bucket, index_key)
    uploads = []
    skips = []
    for sample in samples:
        keys = _sample_destination_keys(prefix, sample, source_path)
        if sample.get("content_hash") in existing_hashes and _all_keys_exist(s3, bucket, keys):
            skips.append(sample)
        else:
            uploads.append(sample)
    return uploads, skips


def _existing_hashes(s3, bucket: str, key: str) -> set[str]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        data = get_object_bytes(s3, bucket, key)
        table = pq.read_table(pa.BufferReader(data), columns=["content_hash"])
        return {value for value in table.column("content_hash").to_pylist() if value}
    except Exception:
        return set()


def _sample_destination_keys(prefix: str, sample: dict, source_path: str) -> list[str]:
    root = f"{prefix}/source/{source_path}"
    if sample["source_kind"] == "single_file":
        return [f"{root}/{sample['primary_filename']}"]
    if sample["source_kind"] == "dicom_series":
        return [f"{root}/{item['path']}" for item in sample["files"]]
    return [f"{root}/{sample['uri_path']}"]


def _all_keys_exist(s3, bucket: str, keys: list[str]) -> bool:
    return all(head_object(s3, bucket, key) for key in keys)


def _upload_sources(s3, bucket: str, prefix: str,
                    samples: list[dict], masks: list[dict]) -> None:
    click.echo("  Uploading source objects to S3...")
    for sample in samples:
        _upload_sample(s3, bucket, prefix, sample, "images")
    for mask in masks:
        _upload_sample(s3, bucket, prefix, mask, "masks")


def _atomic_upload_sources(s3, bucket: str, prefix: str,
                           samples: list[dict], masks: list[dict]) -> None:
    staging_prefix = f"{prefix}/.staging-{uuid.uuid4().hex}"
    lock_key = f"{prefix}/{PUBLISH_LOCK}"
    copied_keys: list[str] = []
    click.echo(f"  Publishing through staging prefix: {staging_prefix}")
    put_object_bytes(
        s3, bucket, lock_key,
        json.dumps({"status": "publishing", "staging_prefix": staging_prefix}).encode(),
        content_type="application/json",
    )
    try:
        for sample in samples:
            _upload_sample(s3, bucket, staging_prefix, sample, "images")
        for mask in masks:
            _upload_sample(s3, bucket, staging_prefix, mask, "masks")
        staged = list_objects(s3, bucket, f"{staging_prefix}/")
        for obj in staged:
            suffix = obj["key"][len(staging_prefix):]
            dest_key = f"{prefix}{suffix}"
            copy_object(s3, bucket, obj["key"], dest_key)
            copied_keys.append(dest_key)
        delete_keys(s3, bucket, [obj["key"] for obj in staged])
    except Exception:
        if copied_keys:
            delete_keys(s3, bucket, copied_keys)
        staged = list_objects(s3, bucket, f"{staging_prefix}/")
        if staged:
            delete_keys(s3, bucket, [obj["key"] for obj in staged])
        delete_keys(s3, bucket, [lock_key])
        raise
    delete_keys(s3, bucket, [lock_key])


def _upload_sample(s3, bucket: str, prefix: str, sample: dict, source_path: str) -> None:
    root = f"{prefix}/source/{source_path}"
    if sample["source_kind"] == "single_file":
        s3.upload_file(sample["local_path"], bucket, f"{root}/{sample['primary_filename']}")
    elif sample["source_kind"] == "dicom_series":
        base_dir = os.path.dirname(sample["local_path"])
        for f_info in sample["files"]:
            local = os.path.join(base_dir, f_info["path"])
            s3.upload_file(local, bucket, f"{root}/{f_info['path']}")
    else:
        s3.upload_file(sample["local_path"], bucket, f"{root}/{sample['uri_path']}")


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
    import pyarrow as pa
    import pyarrow.parquet as pq

    try:
        data = get_object_bytes(s3, bucket, f"{prefix}/metadata/samples.parquet")
        if not data:
            return None
        return pq.read_table(pa.BufferReader(data))
    except Exception:
        return None


def _existing_modality(s3, bucket: str, prefix: str, fallback: str) -> str:
    try:
        import yaml
        data = get_object_bytes(s3, bucket, f"{prefix}/manifest.yaml")
        manifest = yaml.safe_load(data) or {}
        return manifest.get("modality") or fallback
    except Exception:
        return fallback


def _read_manifest(s3, bucket: str, prefix: str) -> dict | None:
    try:
        import yaml
        data = get_object_bytes(s3, bucket, f"{prefix}/manifest.yaml")
        return yaml.safe_load(data) or {}
    except Exception:
        return None


def _parquet_row_count(s3, bucket: str, key: str) -> int | None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        data = get_object_bytes(s3, bucket, key)
        return pq.read_table(pa.BufferReader(data)).num_rows
    except Exception:
        return None


def _dataset_status(s3, bucket: str, dataset_id: str,
                    controller_url: str | None, skip_controller: bool) -> dict:
    prefix = f"datasets/{dataset_id}"
    manifest_meta = head_object(s3, bucket, f"{prefix}/manifest.yaml")
    image_objects = list_objects(s3, bucket, f"{prefix}/source/images/")
    mask_objects = list_objects(s3, bucket, f"{prefix}/source/masks/")
    payload = {
        "dataset_id": dataset_id,
        "status": "published" if manifest_meta else "incomplete",
        "objects": {
            "images": len(image_objects),
            "masks": len(mask_objects),
        },
        "manifest": _read_manifest(s3, bucket, prefix),
        "manifest_object": manifest_meta,
        "indexes": {
            "content_hash_rows": _parquet_row_count(
                s3, bucket, f"{prefix}/indexes/content_hash_index.parquet"
            ),
            "mask_hash_rows": _parquet_row_count(
                s3, bucket, f"{prefix}/indexes/masks_content_hash_index.parquet"
            ),
        },
        "controller": None,
    }
    if controller_url and not skip_controller:
        try:
            for item in controller_api.datasets(controller_url):
                if item.get("dataset_id") == dataset_id:
                    payload["controller"] = item
                    break
        except Exception as e:
            payload["controller"] = {"error": str(e)}
    return payload


def _doctor_result(ctx, controller_url: str | None, skip_controller: bool) -> dict:
    s3 = ctx.obj["s3"]
    bucket = ctx.obj["bucket"]
    ctrl_url = controller_url or ctx.obj.get("controller_url")
    checks = []

    def add(name: str, status: str, detail: str) -> None:
        checks.append({"name": name, "status": status, "detail": detail})

    try:
        s3.list_buckets()
        add("S3 connectivity", "OK", ctx.obj["endpoint"])
    except Exception as e:
        add("S3 connectivity", "FAIL", str(e))
        return {"ok": False, "checks": checks}

    try:
        s3.head_bucket(Bucket=bucket)
        add("Bucket", "OK", bucket)
    except Exception as e:
        add("Bucket", "FAIL", f"{bucket}: {e}")

    try:
        resp = s3.get_bucket_versioning(Bucket=bucket)
        status = resp.get("Status", "Disabled")
        add("Versioning", "OK" if status == "Enabled" else "WARN", status)
    except Exception as e:
        add("Versioning", "FAIL", str(e))

    keep = head_object(s3, bucket, "datasets/.keep")
    add("Init marker", "OK" if keep else "WARN",
        "datasets/.keep present" if keep else "datasets/.keep missing")

    datasets_s3 = []
    try:
        datasets_s3 = list_datasets(s3, bucket)
        add("Dataset count", "OK", str(len(datasets_s3)))
    except Exception as e:
        add("Dataset count", "FAIL", str(e))

    controller_payload = None
    controller_datasets = []
    if ctrl_url and not (skip_controller or ctx.obj.get("skip_controller")):
        try:
            controller_payload = controller_api.health(ctrl_url)
            detail = (
                f"bucket={controller_payload.get('bucket')}, "
                f"dirty={len(controller_payload.get('dirty_datasets', []))}, "
                f"errors={len(controller_payload.get('last_errors', {}))}"
            )
            add("Controller health", "OK", detail)
            controller_datasets = controller_api.datasets(ctrl_url)
            add("Controller datasets", "OK", str(len(controller_datasets)))
            if controller_payload.get("bucket") == bucket:
                add("Controller bucket", "OK", bucket)
            else:
                add("Controller bucket", "WARN",
                    f"controller={controller_payload.get('bucket')} cli={bucket}")
            webhook_prefix = controller_payload.get("webhook_prefix")
            add("Controller webhook prefix",
                "OK" if webhook_prefix == "datasets/" else "WARN",
                str(webhook_prefix))
            expected = {"datasets/<dataset_id>/source/images/",
                        "datasets/<dataset_id>/source/masks/"}
            advertised = set(controller_payload.get("source_prefixes", []))
            if expected.issubset(advertised):
                add("Controller source prefixes", "OK", "images and masks")
            else:
                add("Controller source prefixes", "WARN", f"advertised={sorted(advertised)}")
        except Exception as e:
            add("Controller health", "WARN", str(e))

    if controller_datasets:
        s3_ids = {item["dataset_id"] for item in datasets_s3}
        ctrl_ids = {item["dataset_id"] for item in controller_datasets}
        if s3_ids == ctrl_ids:
            add("S3/controller dataset parity", "OK", "matching dataset ids")
        else:
            add("S3/controller dataset parity", "WARN",
                f"only_s3={sorted(s3_ids - ctrl_ids)}, only_controller={sorted(ctrl_ids - s3_ids)}")

    ok = not any(check["status"] == "FAIL" for check in checks)
    return {
        "ok": ok,
        "checks": checks,
        "controller": controller_payload,
        "datasets": datasets_s3,
    }


def _merge_controller_datasets(s3_datasets: list[dict],
                               controller_datasets: list[dict]) -> list[dict]:
    by_id = {item["dataset_id"]: dict(item) for item in s3_datasets}
    for item in controller_datasets:
        dataset_id = item.get("dataset_id")
        if not dataset_id:
            continue
        merged = by_id.setdefault(dataset_id, {"dataset_id": dataset_id})
        merged.update({
            "status": item.get("status", merged.get("status", "unknown")),
            "dirty": item.get("dirty", False),
            "last_reconcile": item.get("last_reconcile"),
            "last_error": item.get("last_error"),
        })
    return sorted(by_id.values(), key=lambda item: item["dataset_id"])


if __name__ == "__main__":
    main()
