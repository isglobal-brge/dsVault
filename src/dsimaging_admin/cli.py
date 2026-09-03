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
    metadata_contract_from_manifest,
    read_metadata_table,
    scan_images,
    scan_masks,
    scan_s3_images,
    scan_s3_masks,
    validate_dataset_id,
    validate_dicom_series,
    validate_manifest_scope,
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
    data = _load_all_config()
    if "profiles" not in data:
        existing = data.get("default", data) if data else {}
        data = {"profiles": {}}
        if existing:
            data["profiles"]["default"] = existing
    data.setdefault("profiles", {})[profile] = values
    if set_default:
        data["default_profile"] = profile
    _write_config(data)


def _write_config(data: dict) -> None:
    import yaml

    config_path = Path(CONFIG_PATH)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{config_path.name}.", dir=config_path.parent, text=True)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            fd = -1
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, config_path)
        os.chmod(config_path, 0o600)
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


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
    _write_config(data)


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
@click.option("--controller-token", default=None,
              help="Operator bearer token (prefer DSIMAGING_CONTROLLER_TOKEN)")
@click.option("--skip-controller", is_flag=True,
              help="Do not call the dsimaging-store controller")
@click.option("--backend",
              type=click.Choice(["auto", "minio", "aws", "s3-compatible"]),
              default=None,
              help="Storage backend override")
@click.pass_context
def main(ctx, profile, endpoint, access_key, secret_key, bucket, region,
         controller_url, controller_token, skip_controller, backend):
    """Admin CLI for managing dsimaging-store deployments and datasets."""
    cfg = _load_config(profile)
    backend = _resolve_backend(backend, cfg)
    # A stored profile endpoint applies only to store backends (auto/minio/
    # s3-compatible); when AWS is requested, only an explicit CLI flag or
    # DSIMAGING_ENDPOINT env value may set the endpoint.
    endpoint_cfg = {} if backend == "aws" else cfg
    raw_endpoint = _resolve_optional(endpoint, "DSIMAGING_ENDPOINT", endpoint_cfg,
                                     "endpoint")
    endpoint = raw_endpoint or ("" if backend == "aws" else "http://127.0.0.1:9000")
    resolved_backend, backend_rationale = detect_backend(endpoint or None, backend)
    if resolved_backend == "aws":
        access_key = _resolve_optional(access_key, "DSIMAGING_ACCESS_KEY", cfg, "access_key")
        secret_key = _resolve_optional(secret_key, "DSIMAGING_SECRET_KEY", cfg, "secret_key")
    else:
        access_key = _resolve_optional(
            access_key, "DSIMAGING_ACCESS_KEY", cfg, "access_key")
        secret_key = _resolve_optional(
            secret_key, "DSIMAGING_SECRET_KEY", cfg, "secret_key")
    bucket = _resolve(bucket, "DSIMAGING_BUCKET", cfg, "bucket", "imaging-data")
    region = _resolve(region, "DSIMAGING_REGION", cfg, "region", "")
    controller_url = _resolve(controller_url, "DSIMAGING_CONTROLLER_URL", cfg,
                              "controller_url", "")
    controller_token = _resolve_optional(
        controller_token, "DSIMAGING_CONTROLLER_TOKEN", cfg,
        "controller_token")

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
    ctx.obj["controller_token"] = controller_token
    ctx.obj["skip_controller"] = skip_controller


@main.command("init")
@click.option("--profile", default="default", help="Profile name to write")
@click.option("--endpoint", prompt="S3/MinIO endpoint", default="http://127.0.0.1:9000")
@click.option("--bucket", prompt="Bucket name", default="imaging-data")
@click.option("--access-key", prompt="Access key")
@click.option("--secret-key", prompt="Secret key", hide_input=True,
              confirmation_prompt=True)
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


@main.group("ui", invoke_without_command=True)
@click.option("--host", default="127.0.0.1", show_default=True,
              help="Interface to bind the local dashboard server")
@click.option("--port", default=8501, show_default=True,
              help="Port for the local dashboard server")
@click.option("--open-browser", is_flag=True,
              help="Ask Streamlit to open a browser window")
@click.pass_context
def ui_group(ctx, host, port, open_browser):
    """Launch the local operator dashboard."""
    if ctx.invoked_subcommand is None:
        ctx.invoke(ui_launch, host=host, port=port, open_browser=open_browser)


@ui_group.command("launch")
@click.option("--host", default="127.0.0.1", show_default=True,
              help="Interface to bind the local dashboard server")
@click.option("--port", default=8501, show_default=True,
              help="Port for the local dashboard server")
@click.option("--open-browser", is_flag=True,
              help="Ask Streamlit to open a browser window")
def ui_launch(host, port, open_browser):
    """Open the Streamlit operator dashboard."""
    if host not in ("127.0.0.1", "localhost", "::1"):
        raise click.ClickException(
            "The unauthenticated operator dashboard may only bind to loopback. "
            "Use an SSH tunnel for remote administration."
        )
    try:
        from .ui import launch_ui
    except ImportError as e:
        raise click.ClickException(str(e)) from e
    launch_ui(host=host, port=port, open_browser=open_browser)


@store_group.command("init")
@click.argument("path", required=True, type=click.Path(file_okay=False))
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
@click.option("--minio-port", default=None, type=int,
              help="MinIO port [default: the global --endpoint port, else 9000]")
@click.option("--console-port", default=9001, show_default=True)
@click.option("--controller-port", default=8080, show_default=True)
@click.option("--reconcile-interval", default=10, show_default=True)
@click.pass_context
def store_init(ctx, path, force, controller_image, store_source, backend, kms_key,
               access_key, secret_key, bucket, minio_port, console_port,
               controller_port,
               reconcile_interval):
    """Provision a store: MinIO Compose locally, or AWS S3/SQS on AWS.

    PATH is the explicit destination directory for the generated Compose
    project files (docker-compose.yml, .env, init-bucket.sh); files are
    never written implicitly to the current directory.

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

    # Connection profiles describe existing stores. Reusing their credentials
    # for a newly provisioned local store would silently defeat the unique
    # credential default; only options on `store init` may override generation.
    access_key = access_key or None
    secret_key = secret_key or None
    if minio_port is None:
        minio_port = _local_endpoint_port(ctx.obj.get("endpoint")) or 9000
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
    root = Path(path).expanduser().resolve()
    if not ((root / "docker-compose.yml").exists() and (root / ".env").exists()):
        raise click.ClickException(
            f"{path} is not a dsimaging-store project "
            "(missing docker-compose.yml/.env); "
            "run 'dsimaging-admin store init <dir>' first "
            "or pass the store directory"
        )
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
        if not result["ok"]:
            sys.exit(1)
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
              help="CSV/Parquet metadata required by the patient privacy contract")
@click.option("--privacy-unit-column", required=True,
              help="Metadata column containing the patient privacy unit")
@click.option("--label-column", default=None,
              help="Optional complete metadata label/outcome column")
@click.option("--public-label-level", "label_levels", multiple=True,
              help="Approved public label value; repeat for each releasable level")
@click.option("--modality", default="unknown", help="Imaging modality (ct, mri, etc.)")
@click.option("--replace", is_flag=True,
              help="Explicitly replace an existing dataset (atomic mode only)")
@click.option("--atomic/--no-atomic", default=True,
              help="Publish through a staging prefix and publish lock")
@click.option("--dry-run", is_flag=True, help="Scan and plan without S3 writes")
@click.option("--skip-dicom-checks", is_flag=True, help="Skip pre-publish DICOM sanity checks")
@click.pass_context
def publish(ctx, dataset_id, source, metadata, privacy_unit_column, label_column,
            label_levels, modality, replace, atomic, dry_run,
            skip_dicom_checks):
    """Publish a local dataset to S3/MinIO."""
    if not atomic:
        raise click.ClickException(
            "non-atomic dataset publishing is disabled"
        )
    try:
        validate_dataset_id(dataset_id)
    except ValueError as e:
        raise click.ClickException(str(e)) from e

    s3 = ctx.obj["s3"]
    bucket = ctx.obj["bucket"]
    prefix = f"datasets/{dataset_id}"
    existing_objects = list_objects(s3, bucket, f"{prefix}/")
    if existing_objects and not replace:
        raise click.ClickException(
            "dataset already exists; use --replace for an explicit atomic replacement"
        )

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

    try:
        build_samples_metadata(
            samples, extra_metadata=extra_metadata,
            privacy_unit_col=privacy_unit_column, label_col=label_column,
            label_levels=label_levels,
        )
    except ValueError as e:
        raise click.ClickException(str(e)) from e

    # New publications have no prior index, while replacements must stage the
    # complete source set so stale historical objects can be removed safely.
    sample_uploads = samples
    mask_uploads = masks
    click.echo(f"  Images to upload: {len(sample_uploads)}")
    click.echo(f"  Masks to upload:  {len(mask_uploads)}")
    if dry_run:
        click.echo(click.style("Dry run complete; no S3 writes performed.", fg="green"))
        return

    transaction = None
    try:
        if atomic:
            transaction = _atomic_upload_sources(
                s3, bucket, prefix, sample_uploads, mask_uploads,
                replace=replace, require_empty=not replace,
            )
        else:
            _upload_sources(s3, bucket, prefix, sample_uploads, mask_uploads)

        transaction["mutated"] = True
        published_objects = list_objects(
            s3, bucket, f"{prefix}/source/images/", include_version_ids=True)
        published_mask_objects = list_objects(
            s3, bucket, f"{prefix}/source/masks/", include_version_ids=True)
        published_samples = scan_s3_images(
            s3, bucket, prefix, published_objects)
        published_masks = scan_s3_masks(
            s3, bucket, prefix, published_mask_objects,
            sample_ids=[sample["sample_id"] for sample in published_samples],
        )
        _write_dataset_artifacts(
            s3, bucket, prefix, dataset_id, modality, published_samples,
            masks=published_masks, extra_metadata=extra_metadata,
            expected_images=published_objects,
            expected_masks=published_mask_objects,
            privacy_unit_col=privacy_unit_column, label_col=label_column,
            label_levels=label_levels,
            publish_lock=transaction["publish_lock"],
        )
    except Exception:
        if transaction is not None:
            _finish_atomic_publish(s3, bucket, prefix, transaction, commit=False)
        raise
    else:
        if transaction is not None:
            if not _finish_atomic_publish(
                    s3, bucket, prefix, transaction, commit=True):
                raise RuntimeError("dataset publication lock ownership was lost")

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
                datasets, controller_api.datasets(
                    ctrl_url, token=ctx.obj.get("controller_token"))
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
        if ds.get("has_error"):
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
        if not result["ok"]:
            sys.exit(1)
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
        controller_token=ctx.obj.get("controller_token"),
        skip_controller=skip_controller or ctx.obj.get("skip_controller"),
    )
    controller = payload.get("controller") or {}
    controller_failed = bool(
        controller.get("error") or controller.get("has_error"))
    if output == "json":
        _echo_json(payload)
        if controller_failed:
            sys.exit(1)
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
        if ctrl.get("has_error"):
            click.echo("  Last reconcile error: yes (see controller logs)")
    if controller_failed:
        sys.exit(1)


@dataset_group.command("verify")
@click.argument("dataset_id")
@click.option("--sample-fraction", default=1.0, show_default=True, type=float)
@click.option(
    "--quick", is_flag=True,
    help="Skip hashing only when the recorded immutable S3 version still matches",
)
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
    targets = [
        (obj, _safe_download_target(root, obj["key"][len(prefix):]))
        for obj in objects
    ]
    if not overwrite:
        existing = next((target for _, target in targets if target.exists()), None)
        if existing is not None:
            raise click.ClickException(
                f"Refusing to overwrite {existing}; use --overwrite")
    for obj, target in targets:
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
    if src_id == dst_id:
        raise click.ClickException("source and destination datasets must differ")
    if not yes:
        raise click.ClickException("Refusing to copy without --yes")
    s3 = ctx.obj["s3"]
    bucket = ctx.obj["bucket"]
    src_prefix = f"datasets/{src_id}"
    dst_prefix = f"datasets/{dst_id}"
    existing = list_objects(s3, bucket, f"{dst_prefix}/")
    if existing and not replace:
        raise click.ClickException("destination exists; use --replace")
    source_lock = None
    transaction = None
    try:
        try:
            source_lock = _acquire_publish_lock(
                s3, bucket, src_prefix, status="copy-source")
        except Exception as exc:
            raise click.ClickException(
                "source dataset has an active publication"
            ) from exc
        source_manifest = _read_manifest_strict(s3, bucket, src_prefix)
        contract = metadata_contract_from_manifest(source_manifest)
        sources = list_objects(s3, bucket, f"{src_prefix}/source/")
        if not sources:
            raise click.ClickException("source dataset has no source objects")
        extra_metadata = _read_existing_samples_metadata(s3, bucket, src_prefix)
        transaction = _atomic_upload_sources(
            s3, bucket, dst_prefix, [], [], replace=False,
            require_empty=not replace,
        )
        current = [
            obj["key"] for obj in list_objects(
                s3, bucket, f"{dst_prefix}/")
            if obj["key"] != f"{dst_prefix}/{PUBLISH_LOCK}"
            and not obj["key"].startswith(f"{dst_prefix}/.staging-")
            and not obj["key"].startswith(f"{dst_prefix}/.backup-")
        ]
        if current and not replace:
            raise RuntimeError("destination changed while copy was starting")
        transaction["mutated"] = True
        if current:
            _delete_keys_exact(s3, bucket, current)
        for obj in sources:
            suffix = obj["key"][len(src_prefix):]
            copy_object(s3, bucket, obj["key"], f"{dst_prefix}{suffix}")
        if _object_inventory(sources) != _object_inventory(list_objects(
                s3, bucket, f"{src_prefix}/source/")):
            raise RuntimeError("source dataset changed while it was being copied")
        objects = list_objects(
            s3, bucket, f"{dst_prefix}/source/images/", include_version_ids=True)
        masks_objects = list_objects(
            s3, bucket, f"{dst_prefix}/source/masks/", include_version_ids=True)
        samples = scan_s3_images(s3, bucket, dst_prefix, objects)
        masks = scan_s3_masks(
            s3, bucket, dst_prefix, masks_objects,
            sample_ids=[sample["sample_id"] for sample in samples])
        _write_dataset_artifacts(
            s3, bucket, dst_prefix, dst_id,
            source_manifest.get("modality") or "unknown", samples,
            masks=masks, extra_metadata=extra_metadata,
            expected_images=objects,
            expected_masks=masks_objects,
            privacy_unit_col=contract["privacy_unit_col"],
            label_col=contract.get("label_col"),
            label_levels=contract.get("label_levels"),
            existing_manifest=source_manifest,
            publish_lock=transaction["publish_lock"],
        )
    except Exception:
        if transaction is not None:
            _finish_atomic_publish(
                s3, bucket, dst_prefix, transaction, commit=False)
        raise
    else:
        if not _finish_atomic_publish(
                s3, bucket, dst_prefix, transaction, commit=True):
            raise RuntimeError("destination publication lock ownership was lost")
    finally:
        if source_lock is not None:
            _release_publish_lock(s3, bucket, source_lock)
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
    s3 = ctx.obj["s3"]
    bucket = ctx.obj["bucket"]
    prefix = f"datasets/{dataset_id}"
    transaction = None
    try:
        transaction = _atomic_upload_sources(
            s3, bucket, prefix, [], [], replace=False)
        transaction["mutated"] = True
        counts = _rescan_dataset_artifacts(
            s3, bucket, dataset_id,
            publish_lock=transaction["publish_lock"],
        )
    except Exception:
        if transaction is not None:
            _finish_atomic_publish(
                s3, bucket, prefix, transaction, commit=False)
        raise
    else:
        if not _finish_atomic_publish(
                s3, bucket, prefix, transaction, commit=True):
            raise RuntimeError("dataset publication lock ownership was lost")
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
        payload = controller_api.reconcile(
            ctrl_url, dataset_id, token=ctx.obj.get("controller_token"))
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
    try:
        metadata_contract_from_manifest(_read_manifest_strict(s3, bucket, prefix))
    except ValueError as e:
        raise click.ClickException(str(e)) from e

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

    transaction = None
    try:
        # Upload every requested local object after acquiring the lock. Entries
        # classified as skips during planning are included so a concurrent
        # pre-lock change cannot silently omit part of the requested update.
        transaction = _atomic_upload_sources(
            s3, bucket, prefix,
            sample_uploads + sample_skips,
            mask_uploads + mask_skips,
            replace=False,
        )
        transaction["mutated"] = True
        counts = _rescan_dataset_artifacts(
            s3, bucket, dataset_id, extra_metadata=extra_metadata,
            publish_lock=transaction["publish_lock"],
        )
    except Exception:
        if transaction is not None:
            _finish_atomic_publish(
                s3, bucket, prefix, transaction, commit=False)
        raise
    else:
        if not _finish_atomic_publish(
                s3, bucket, prefix, transaction, commit=True):
            raise RuntimeError("dataset publication lock ownership was lost")
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


def _local_endpoint_port(endpoint: str | None) -> int | None:
    """Return the explicit port of a local endpoint URL, if any."""
    if not endpoint:
        return None
    from urllib.parse import urlsplit
    try:
        parts = urlsplit(endpoint)
        if parts.hostname not in ("127.0.0.1", "localhost"):
            return None
        return parts.port
    except ValueError:
        return None


def _validate_dataset_cli(dataset_id: str) -> None:
    try:
        validate_dataset_id(dataset_id)
    except ValueError as e:
        raise click.ClickException(str(e)) from e


def _safe_download_target(root: Path, relative: str) -> Path:
    if (not relative or relative.startswith("/") or
            any(char in relative for char in ("\\", "\r", "\n")) or
            any(part in {"", ".", ".."} for part in relative.split("/"))):
        raise click.ClickException("dataset contains an unsafe object path")
    target = (root / relative).resolve()
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise click.ClickException(
            "dataset contains an unsafe object path") from exc
    return target


def _rescan_dataset_artifacts(s3, bucket: str, dataset_id: str,
                              extra_metadata=None, *,
                              publish_lock: dict | None = None) -> dict:
    prefix = f"datasets/{dataset_id}"
    objects = list_objects(
        s3, bucket, f"{prefix}/source/images/", include_version_ids=True)
    mask_objects = list_objects(
        s3, bucket, f"{prefix}/source/masks/", include_version_ids=True)
    samples = scan_s3_images(s3, bucket, prefix, objects)
    if not samples:
        raise click.ClickException("No supported image objects found.")
    masks = scan_s3_masks(s3, bucket, prefix, mask_objects,
                          sample_ids=[sample["sample_id"] for sample in samples])
    try:
        existing_manifest = _read_manifest_strict(s3, bucket, prefix)
        contract = metadata_contract_from_manifest(existing_manifest)
    except ValueError as e:
        raise click.ClickException(str(e)) from e
    if extra_metadata is None:
        extra_metadata = _read_existing_samples_metadata(s3, bucket, prefix)
    _write_dataset_artifacts(
        s3, bucket, prefix, dataset_id,
        existing_manifest.get("modality") or "unknown", samples,
        masks=masks, extra_metadata=extra_metadata,
        expected_images=objects,
        expected_masks=mask_objects,
        privacy_unit_col=contract["privacy_unit_col"],
        label_col=contract.get("label_col"),
        label_levels=contract.get("label_levels"),
        existing_manifest=existing_manifest,
        publish_lock=publish_lock,
    )
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


def _object_inventory(objects: list[dict]) -> list[tuple]:
    return sorted(
        (
            obj["key"], int(obj.get("size", 0)), obj.get("etag"),
            obj.get("version_id"),
        )
        for obj in objects
    )


def _assert_source_inventory_unchanged(
    s3,
    bucket: str,
    prefix: str,
    expected_images: list[dict],
    expected_masks: list[dict],
) -> None:
    current_images = list_objects(
        s3, bucket, f"{prefix}/source/images/", include_version_ids=True)
    current_masks = list_objects(
        s3, bucket, f"{prefix}/source/masks/", include_version_ids=True)
    if (_object_inventory(current_images) != _object_inventory(expected_images) or
            _object_inventory(current_masks) != _object_inventory(expected_masks)):
        raise RuntimeError(
            "dataset source inventory changed while artifacts were built")


def _delete_keys_exact(s3, bucket: str, keys: list[str]) -> None:
    if keys and delete_keys(s3, bucket, keys) != len(keys):
        raise RuntimeError("S3 object deletion was incomplete")


def _upload_sources(s3, bucket: str, prefix: str,
                    samples: list[dict], masks: list[dict]) -> None:
    click.echo("  Uploading source objects to S3...")
    for sample in samples:
        _upload_sample(s3, bucket, prefix, sample, "images")
    for mask in masks:
        _upload_sample(s3, bucket, prefix, mask, "masks")


def _atomic_upload_sources(s3, bucket: str, prefix: str,
                           samples: list[dict], masks: list[dict], *,
                           replace: bool = False,
                           require_empty: bool = False) -> dict:
    staging_prefix = f"{prefix}/.staging-{uuid.uuid4().hex}"
    backup_prefix = f"{prefix}/.backup-{uuid.uuid4().hex}"
    click.echo(f"  Publishing through staging prefix: {staging_prefix}")
    try:
        publish_lock = _acquire_publish_lock(
            s3, bucket, prefix, status="publishing",
            staging_prefix=staging_prefix,
        )
    except Exception as exc:
        raise RuntimeError(
            "dataset already has an active atomic publication"
        ) from exc
    mutated = False
    try:
        previous = [
            obj for obj in list_objects(s3, bucket, f"{prefix}/")
            if obj["key"] != publish_lock["key"]
            and not obj["key"].startswith(f"{prefix}/.staging-")
            and not obj["key"].startswith(f"{prefix}/.backup-")
        ]
        if require_empty and previous:
            raise RuntimeError(
                "dataset already exists; explicit replacement is required"
            )
        for obj in previous:
            suffix = obj["key"][len(prefix):]
            copy_object(s3, bucket, obj["key"], f"{backup_prefix}{suffix}")
        for sample in samples:
            _upload_sample(s3, bucket, staging_prefix, sample, "images")
        for mask in masks:
            _upload_sample(s3, bucket, staging_prefix, mask, "masks")
        staged = list_objects(s3, bucket, f"{staging_prefix}/")
        # All prior objects are now safely backed up. Only from this point may
        # rollback replace the canonical prefix from that backup.
        mutated = bool(staged)
        for obj in staged:
            suffix = obj["key"][len(staging_prefix):]
            dest_key = f"{prefix}{suffix}"
            copy_object(s3, bucket, obj["key"], dest_key)
        if replace:
            staged_destinations = {
                f"{prefix}{obj['key'][len(staging_prefix):]}" for obj in staged
            }
            stale_sources = [
                obj["key"] for obj in previous
                if f"{prefix}/source/" in obj["key"]
                and obj["key"] not in staged_destinations
            ]
            if stale_sources:
                mutated = True
                _delete_keys_exact(s3, bucket, stale_sources)
    except Exception:
        transaction = {
            "staging_prefix": staging_prefix,
            "backup_prefix": backup_prefix,
            "mutated": mutated,
            "publish_lock": publish_lock,
        }
        _finish_atomic_publish(s3, bucket, prefix, transaction, commit=False)
        raise
    return {
        "staging_prefix": staging_prefix,
        "backup_prefix": backup_prefix,
        "mutated": mutated,
        "publish_lock": publish_lock,
    }


def _finish_atomic_publish(s3, bucket: str, prefix: str,
                           transaction: dict, *, commit: bool) -> bool:
    staging_prefix = transaction["staging_prefix"]
    backup_prefix = transaction["backup_prefix"]
    publish_lock = transaction["publish_lock"]
    lock_key = publish_lock["key"]
    staged = list_objects(s3, bucket, f"{staging_prefix}/")
    backups = list_objects(s3, bucket, f"{backup_prefix}/")
    if not _publish_lock_is_owned(s3, bucket, publish_lock):
        # Another owner now controls recovery. Preserve this transaction's
        # uniquely named staging and backup snapshots for operator inspection.
        return False
    if not commit and transaction.get("mutated", False):
        current = [
            obj["key"] for obj in list_objects(s3, bucket, f"{prefix}/")
            if obj["key"] != lock_key
            and not obj["key"].startswith(f"{prefix}/.staging-")
            and not obj["key"].startswith(f"{prefix}/.backup-")
        ]
        if current:
            _delete_keys_exact(s3, bucket, current)
        for obj in backups:
            suffix = obj["key"][len(backup_prefix):]
            copy_object(s3, bucket, obj["key"], f"{prefix}{suffix}")
    cleanup = [obj["key"] for obj in staged + backups]
    if cleanup:
        _delete_keys_exact(s3, bucket, cleanup)
    return _release_publish_lock(s3, bucket, publish_lock)


def _acquire_publish_lock(s3, bucket: str, prefix: str, *, status: str,
                          staging_prefix: str | None = None) -> dict:
    """Create and identify one dataset-scoped publication lock."""
    owner = uuid.uuid4().hex
    key = f"{prefix}/{PUBLISH_LOCK}"
    payload = {"status": status, "owner": owner}
    if staging_prefix is not None:
        payload["staging_prefix"] = staging_prefix
    response = put_object_bytes(
        s3, bucket, key, json.dumps(payload).encode("utf-8"),
        content_type="application/json", if_absent=True,
    )
    return {"key": key, "owner": owner, "etag": response.get("ETag")}


def _release_publish_lock(s3, bucket: str, publish_lock: dict) -> bool:
    """Release a lock only while both its owner and object ETag still match."""
    key = publish_lock["key"]
    if not _publish_lock_is_owned(s3, bucket, publish_lock):
        return False
    kwargs = {"Bucket": bucket, "Key": key}
    if publish_lock.get("etag"):
        kwargs["IfMatch"] = publish_lock["etag"]
    try:
        s3.delete_object(**kwargs)
    except Exception:
        return False
    return True


def _publish_lock_is_owned(s3, bucket: str, publish_lock: dict) -> bool:
    try:
        current = json.loads(get_object_bytes(
            s3, bucket, publish_lock["key"]))
    except Exception:
        return False
    return current.get("owner") == publish_lock["owner"]


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
                             extra_metadata=None, *,
                             expected_images: list[dict],
                             expected_masks: list[dict],
                             privacy_unit_col: str,
                             label_col: str | None = None,
                             label_levels: list[str] | tuple[str, ...] | None = None,
                             existing_manifest: dict | None = None,
                             publish_lock: dict | None = None) -> None:
    import pyarrow.parquet as pq

    masks = masks or []
    with tempfile.TemporaryDirectory() as tmpdir:
        click.echo("  Building content hash index...")
        idx = build_hash_index(samples, bucket, prefix)
        idx_path = os.path.join(tmpdir, "content_hash_index.parquet")
        pq.write_table(idx, idx_path)

        mask_idx_path = None
        if masks:
            click.echo("  Building mask content hash index...")
            mask_idx = build_mask_hash_index(masks, bucket, prefix)
            mask_idx_path = os.path.join(tmpdir, "masks_content_hash_index.parquet")
            pq.write_table(mask_idx, mask_idx_path)

        click.echo("  Building sample manifests...")
        sm = build_sample_manifests(samples)
        sm_path = os.path.join(tmpdir, "sample_manifests.parquet")
        pq.write_table(sm, sm_path)

        click.echo("  Building samples metadata...")
        meta = build_samples_metadata(
            samples, extra_metadata=extra_metadata,
            privacy_unit_col=privacy_unit_col, label_col=label_col,
            label_levels=label_levels,
        )
        meta_path = os.path.join(tmpdir, "samples.parquet")
        pq.write_table(meta, meta_path)

        click.echo("  Building manifest...")
        manifest = generate_manifest(
            dataset_id, bucket, prefix, modality, has_masks=bool(masks),
            privacy_unit_col=privacy_unit_col, label_col=label_col,
            label_levels=label_levels,
            existing_manifest=existing_manifest,
        )
        manifest_path = os.path.join(tmpdir, "manifest.yaml")
        write_manifest_yaml(manifest, manifest_path)

        # Upload all derived data first. The manifest is the publication marker
        # and is deliberately written last.
        if (publish_lock is not None and
                not _publish_lock_is_owned(s3, bucket, publish_lock)):
            raise RuntimeError("dataset publication lock ownership was lost")
        s3.upload_file(idx_path, bucket, f"{prefix}/indexes/content_hash_index.parquet")
        if mask_idx_path:
            s3.upload_file(mask_idx_path, bucket,
                           f"{prefix}/indexes/masks_content_hash_index.parquet")
        else:
            stale_mask_index = f"{prefix}/indexes/masks_content_hash_index.parquet"
            if head_object(s3, bucket, stale_mask_index):
                _delete_keys_exact(s3, bucket, [stale_mask_index])
        s3.upload_file(sm_path, bucket, f"{prefix}/metadata/sample_manifests.parquet")
        s3.upload_file(meta_path, bucket, f"{prefix}/metadata/samples.parquet")
        if (publish_lock is not None and
                not _publish_lock_is_owned(s3, bucket, publish_lock)):
            raise RuntimeError("dataset publication lock ownership was lost")
        _assert_source_inventory_unchanged(
            s3, bucket, prefix, expected_images, expected_masks)
        if (publish_lock is not None and
                not _publish_lock_is_owned(s3, bucket, publish_lock)):
            raise RuntimeError("dataset publication lock ownership was lost")
        s3.upload_file(manifest_path, bucket, f"{prefix}/manifest.yaml")


def _read_existing_samples_metadata(s3, bucket: str, prefix: str):
    import pyarrow as pa
    import pyarrow.parquet as pq

    key = f"{prefix}/metadata/samples.parquet"
    if not head_object(s3, bucket, key):
        return None
    data = get_object_bytes(s3, bucket, key)
    if not data:
        raise ValueError("samples metadata is empty")
    try:
        return pq.read_table(pa.BufferReader(data))
    except Exception as exc:
        raise ValueError("samples metadata is corrupt") from exc


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


def _read_manifest_strict(s3, bucket: str, prefix: str) -> dict:
    key = f"{prefix}/manifest.yaml"
    if not head_object(s3, bucket, key):
        raise ValueError("dataset manifest is missing")
    try:
        import yaml
        manifest = yaml.safe_load(get_object_bytes(s3, bucket, key))
    except Exception as exc:
        raise ValueError("dataset manifest is corrupt") from exc
    if not isinstance(manifest, dict):
        raise ValueError("dataset manifest must be a mapping")
    validate_manifest_scope(manifest, bucket, prefix)
    return manifest


def _parquet_row_count(s3, bucket: str, key: str) -> int | None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        data = get_object_bytes(s3, bucket, key)
        return pq.read_table(pa.BufferReader(data)).num_rows
    except Exception:
        return None


def _dataset_status(s3, bucket: str, dataset_id: str,
                    controller_url: str | None,
                    controller_token: str | None,
                    skip_controller: bool) -> dict:
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
            for item in controller_api.datasets(
                    controller_url, token=controller_token):
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
            add("Controller health", "OK", controller_payload.get("status", "ok"))
            controller_token = ctx.obj.get("controller_token")
            if controller_token:
                controller_datasets = controller_api.datasets(
                    ctrl_url, token=controller_token)
                add("Controller datasets", "OK", str(len(controller_datasets)))
            else:
                add("Controller operator API", "WARN", "token not configured")
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
            "last_reconcile_at": item.get("last_reconcile_at"),
            "has_error": item.get("has_error", False),
        })
    return sorted(by_id.values(), key=lambda item: item["dataset_id"])


if __name__ == "__main__":
    main()
