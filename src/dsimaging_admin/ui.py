"""Streamlit dashboard for local dsimaging-store operators."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Callable

import yaml

try:
    import streamlit as st
except ImportError as exc:  # pragma: no cover - exercised when the extra is absent
    raise ImportError(
        "The dashboard requires optional UI dependencies. "
        "Install them with: pip install dsimaging-admin[ui]"
    ) from exc

try:  # pragma: no cover - optional dependency
    from streamlit_autorefresh import st_autorefresh
except ImportError:  # pragma: no cover - optional dependency
    st_autorefresh = None

import boto3
import requests

from dsimaging_admin import controller as controller_api
from dsimaging_admin.cli import (
    _atomic_upload_sources,
    _finish_atomic_publish,
    _partition_uploads,
    _read_manifest_strict,
    _read_existing_samples_metadata,
    _rescan_dataset_artifacts,
    _write_dataset_artifacts,
)
from dsimaging_admin.manifest import (
    build_samples_metadata,
    metadata_contract_from_manifest,
    read_metadata_table,
    scan_images,
    scan_masks,
    scan_s3_images,
    scan_s3_masks,
    validate_dataset_id,
)
from dsimaging_admin.s3 import (
    create_client,
    delete_keys,
    detect_backend,
    get_object_bytes,
    list_datasets,
    list_objects,
)
from dsimaging_admin.store import (
    DEFAULT_CONTROLLER_IMAGE,
    compose_down,
    compose_ps,
    compose_ps_json,
    compose_up,
    init_store,
)


DEFAULT_CONFIG_PATH = os.path.expanduser("~/.dsimaging.yaml")
BACKENDS = ["auto", "minio", "aws", "s3-compatible"]


def launch_ui(host: str = "127.0.0.1", port: int = 8501,
              open_browser: bool = False) -> None:
    script = Path(__file__).resolve()
    args = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(script),
        "--server.address",
        host,
        "--server.port",
        str(port),
        "--server.headless",
        "false" if open_browser else "true",
    ]
    raise SystemExit(subprocess.call(args))


def run_app() -> None:
    st.set_page_config(page_title="dsimaging-admin", layout="wide")
    st.title("dsimaging-admin")
    st.caption("Local operator dashboard for dsimaging-store")

    profiles = load_profiles()
    selected_profile = profile_picker(profiles)
    config = edit_connection_config(profiles.get(selected_profile, {}))

    page = st.sidebar.radio(
        "Navigation",
        [
            "Connect & Doctor",
            "Store administration",
            "Datasets",
            "Publish",
            "Modify / Rescan",
            "Delete",
            "Controller observability",
        ],
    )

    if page == "Connect & Doctor":
        render_connect_doctor(config)
    elif page == "Store administration":
        render_store_admin(config)
    elif page == "Datasets":
        render_datasets(config)
    elif page == "Publish":
        render_publish(config)
    elif page == "Modify / Rescan":
        render_modify_rescan(config)
    elif page == "Delete":
        render_delete(config)
    elif page == "Controller observability":
        render_controller(config)


def load_profiles(path: str | None = None) -> dict[str, dict]:
    config_path = path or os.environ.get("DSIMAGING_CONFIG") or DEFAULT_CONFIG_PATH
    if not os.path.exists(config_path):
        return {
            "default": {
                "endpoint": os.environ.get("DSIMAGING_ENDPOINT", "http://127.0.0.1:9000"),
                "bucket": os.environ.get("DSIMAGING_BUCKET", "imaging-data"),
                "access_key": os.environ.get("DSIMAGING_ACCESS_KEY", "minioadmin"),
                "secret_key": os.environ.get("DSIMAGING_SECRET_KEY", "minioadmin123"),
                "region": os.environ.get("DSIMAGING_REGION", ""),
                "controller_url": os.environ.get("DSIMAGING_CONTROLLER_URL", ""),
                "controller_token": os.environ.get(
                    "DSIMAGING_CONTROLLER_TOKEN", ""),
                "backend": os.environ.get("DSIMAGING_BACKEND", "auto"),
            }
        }
    try:
        with open(config_path, encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    except Exception:
        return {"default": {}}
    st.session_state["_config_default_profile"] = raw.get("default_profile")
    profiles = raw.get("profiles")
    if isinstance(profiles, dict):
        return {str(name): (value or {}) for name, value in profiles.items()}
    default = raw.get("default", raw)
    return {"default": default or {}}


def profile_picker(profiles: dict[str, dict]) -> str:
    names = sorted(profiles) or ["default"]
    default_name = (
        os.environ.get("DSIMAGING_PROFILE")
        or st.session_state.get("_config_default_profile")
    )
    index = names.index(default_name) if default_name in names else 0
    return st.sidebar.selectbox("Profile", names, index=index)


def edit_connection_config(profile: dict) -> dict:
    st.sidebar.subheader("Connection")
    endpoint = st.sidebar.text_input(
        "Endpoint",
        value=_resolve_value("DSIMAGING_ENDPOINT", profile, "endpoint", "http://127.0.0.1:9000"),
    )
    bucket = st.sidebar.text_input(
        "Bucket",
        value=_resolve_value("DSIMAGING_BUCKET", profile, "bucket", "imaging-data"),
    )
    region = st.sidebar.text_input(
        "Region",
        value=_resolve_value("DSIMAGING_REGION", profile, "region", ""),
    )
    access_key = st.sidebar.text_input(
        "Access key",
        value=_resolve_value("DSIMAGING_ACCESS_KEY", profile, "access_key", ""),
        type="password",
    )
    secret_key = st.sidebar.text_input(
        "Secret key",
        value=_resolve_value("DSIMAGING_SECRET_KEY", profile, "secret_key", ""),
        type="password",
    )
    backend = st.sidebar.selectbox(
        "Backend override",
        BACKENDS,
        index=BACKENDS.index(_normalise_backend(_resolve_value("DSIMAGING_BACKEND", profile, "backend", "auto"))),
    )
    controller_url = st.sidebar.text_input(
        "Controller URL",
        value=_resolve_value("DSIMAGING_CONTROLLER_URL", profile, "controller_url", ""),
    )
    controller_token = st.sidebar.text_input(
        "Controller operator token",
        value=_resolve_value(
            "DSIMAGING_CONTROLLER_TOKEN", profile, "controller_token", ""),
        type="password",
    )
    sqs_queue_url = st.sidebar.text_input(
        "SQS queue URL",
        value=_nested_value(profile, "aws", "sqs_queue_url")
        or os.environ.get("DSIMAGING_SQS_QUEUE_URL", ""),
    )
    st.sidebar.markdown(
        f"Access key: **{_secret_badge(access_key)}**  \n"
        f"Secret key: **{_secret_badge(secret_key)}**  \n"
        f"Controller token: **{_secret_badge(controller_token)}**"
    )
    resolved_backend, rationale = detect_backend(endpoint, backend)
    return {
        "endpoint": endpoint,
        "bucket": bucket,
        "region": region,
        "access_key": access_key,
        "secret_key": secret_key,
        "backend": backend,
        "resolved_backend": resolved_backend,
        "backend_rationale": rationale,
        "controller_url": controller_url,
        "controller_token": controller_token,
        "sqs_queue_url": sqs_queue_url,
    }


def render_connect_doctor(config: dict) -> None:
    st.header("Connect & Doctor")
    left, right = st.columns([1, 1])
    with left:
        if st.button("Detect backend"):
            backend, rationale = detect_backend(config["endpoint"], config["backend"])
            st.success(f"{backend}: {rationale}")
        if st.button("Run doctor"):
            st.session_state["doctor_checks"] = run_doctor_checks(config)
    with right:
        render_capabilities(config)

    checks = st.session_state.get("doctor_checks")
    if checks:
        st.subheader("Checks")
        rows = [
            {
                "state": check["state"],
                "check": check["check"],
                "latency_ms": check["latency_ms"],
                "reason": check["reason"],
            }
            for check in checks
        ]
        wide_dataframe(rows, hide_index=True)
        for check in checks:
            st.markdown(
                f"**{check['check']}** "
                f"`{check['state']}` "
                f"{check['latency_ms']} ms - {check['reason']}"
            )


def render_capabilities(config: dict) -> None:
    st.subheader("Capabilities")
    capability_rows = [
        ("Backend", config["resolved_backend"]),
        ("Rationale", config["backend_rationale"]),
        ("Region", config["region"] or "default"),
        ("Bucket", config["bucket"]),
        ("Controller URL", config["controller_url"] or "not set"),
        ("SQS queue URL", config["sqs_queue_url"] or "not set"),
        ("Credential source", credential_source(config)),
    ]
    for label, value in capability_rows:
        st.write(f"**{label}:** {value}")


def render_store_admin(config: dict) -> None:
    st.header("Store administration")
    backend = config["resolved_backend"]
    with st.form("store-init-form"):
        st.subheader("Init store")
        project_path = st.text_input("Local store project path", value="./study-store")
        bucket = st.text_input("Bucket name", value=config["bucket"] or "imaging-data")
        region = st.text_input("Region", value=config["region"])
        kms_key = st.text_input("KMS key", value="", type="password")
        controller_webhook = st.text_input(
            "Controller webhook URL",
            value=config["controller_url"] or "http://controller:8080/webhook/minio",
        )
        run = st.form_submit_button("Run")
    if run:
        with st.expander("Operation log", expanded=True):
            try:
                if backend == "aws":
                    init_aws_store(config | {"bucket": bucket, "region": region}, kms_key)
                    st.success("AWS bucket checks and defaults applied.")
                else:
                    cfg = init_store(
                        project_path,
                        force=True,
                        controller_image=DEFAULT_CONTROLLER_IMAGE,
                        bucket=bucket,
                        access_key=config["access_key"] or "minioadmin",
                        secret_key=config["secret_key"] or "minioadmin123",
                    )
                    st.write(cfg.to_dict())
                    st.info(f"Webhook target: {controller_webhook}")
            except Exception as exc:
                st.error(str(exc))

    st.subheader("Compose controls")
    project_path = st.text_input("Compose project path", value="./study-store", key="compose-project")
    disabled = backend == "aws"
    cols = st.columns(3)
    if cols[0].button("Up", disabled=disabled, help="Disabled for AWS backend"):
        st.code(compose_up(project_path))
    if cols[1].button("Down", disabled=disabled, help="Disabled for AWS backend"):
        st.code(compose_down(project_path))
    if cols[2].button("Refresh status", disabled=disabled):
        st.session_state["compose_status"] = compose_status(project_path)
    if disabled:
        st.info("AWS backend: the store is serverless; deploy the controller separately.")
    status = st.session_state.get("compose_status")
    if status:
        st.subheader("Compose stack status")
        st.code(status)


def render_datasets(config: dict) -> None:
    st.header("Datasets")
    s3 = make_s3_client(config)
    bucket = config["bucket"]
    try:
        rows = dataset_rows(s3, bucket)
    except Exception as exc:
        st.error(str(exc))
        return
    query = st.text_input("Filter datasets", value="")
    filtered = [
        row for row in rows
        if not query or query.lower() in row["dataset_id"].lower()
    ]
    left, right = st.columns([1.15, 1])
    with left:
        wide_dataframe(filtered, hide_index=True)
    with right:
        if not filtered:
            st.info("No datasets found.")
            return
        selected = st.selectbox("Dataset detail", [row["dataset_id"] for row in filtered])
        render_dataset_detail(s3, bucket, selected, config)


def render_dataset_detail(s3, bucket: str, dataset_id: str, config: dict) -> None:
    prefix = f"datasets/{dataset_id}"
    st.subheader(dataset_id)
    manifest_yaml = read_text_object(s3, bucket, f"{prefix}/manifest.yaml")
    with st.expander("Raw manifest YAML", expanded=True):
        st.code(manifest_yaml or "manifest.yaml not found", language="yaml")
    render_parquet_preview(s3, bucket, f"{prefix}/metadata/samples.parquet", "samples.parquet")
    render_parquet_preview(
        s3, bucket, f"{prefix}/indexes/content_hash_index.parquet",
        "content_hash_index.parquet",
    )
    render_parquet_preview(
        s3, bucket, f"{prefix}/indexes/masks_content_hash_index.parquet",
        "masks_content_hash_index.parquet",
    )
    st.subheader("Recommended DataSHIELD resource")
    block = resource_block(config, dataset_id)
    st.code(block, language="yaml")
    st.download_button(
        "Download resource block",
        data=block,
        file_name=f"{dataset_id}_resource.yaml",
        mime="text/yaml",
    )
    objects = list_objects(s3, bucket, f"{prefix}/")
    last_modified = max((obj["last_modified"] for obj in objects), default="not available")
    controller_seen = controller_dataset_metric(
        config.get("controller_url"), dataset_id,
        token=config.get("controller_token"),
    )
    st.metric("Total objects", len(objects))
    st.metric("Total bytes", human_bytes(sum(obj["size"] for obj in objects)))
    st.write(f"Last object change: {last_modified}")
    st.write(f"Last reconcile event: {controller_seen or 'not available'}")


def render_publish(config: dict) -> None:
    st.header("Publish")
    dataset_id = st.text_input("Dataset ID", key="publish-dataset")
    source_path = st.text_input("Source path", key="publish-source")
    metadata_path = st.text_input(
        "Metadata path (required)", key="publish-metadata")
    privacy_unit_column = st.text_input(
        "Patient privacy-unit column", key="publish-privacy-unit-column"
    )
    label_column = st.text_input(
        "Label column (optional)", key="publish-label-column"
    )
    replace = st.checkbox("Replace an existing dataset atomically", value=False)
    modality = st.selectbox("Modality", ["ct", "mri", "pet", "xray", "unknown"], index=0)
    resource_endpoint = st.text_input("Resource endpoint override", value=config["endpoint"])
    if resource_endpoint:
        config = config | {"endpoint": resource_endpoint}
    if st.button("Preview scan"):
        st.session_state["publish_preview"] = preview_scan(source_path)
    render_preview("publish_preview")
    if st.button("Publish"):
        progress = st.progress(0)
        log: list[str] = []
        with st.expander("Per-object log", expanded=True):
            try:
                publish_dataset(
                    config, dataset_id, source_path, metadata_path,
                    privacy_unit_column, label_column or None, modality,
                    replace, progress, log.append,
                )
                st.success("Publish complete.")
            except Exception as exc:
                st.error(str(exc))
            st.code("\n".join(log))


def render_modify_rescan(config: dict) -> None:
    st.header("Modify / Rescan")
    s3 = make_s3_client(config)
    bucket = config["bucket"]
    datasets = [row["dataset_id"] for row in safe_dataset_rows(s3, bucket)]
    if not datasets:
        st.info("No datasets found.")
        return
    dataset_id = st.selectbox("Dataset", datasets)
    metadata_path = st.text_input("Replace metadata file", key="modify-metadata")
    images_path = st.text_input("Add more images path", key="modify-images")
    masks_path = st.text_input("Add more masks path", key="modify-masks")
    if st.button("Preview modify"):
        st.session_state["modify_preview"] = preview_modify(images_path, masks_path)
    render_preview("modify_preview")
    if st.button("Apply modify"):
        log: list[str] = []
        try:
            modify_dataset(config, dataset_id, metadata_path, images_path, masks_path, log.append)
            st.success("Modify complete.")
        except Exception as exc:
            st.error(str(exc))
        st.code("\n".join(log))
    if st.button("Rescan dataset"):
        try:
            count = rescan_dataset(config, dataset_id)
            st.success(f"Rescan complete: {count['samples']} samples, {count['masks']} masks")
        except Exception as exc:
            st.error(str(exc))


def render_delete(config: dict) -> None:
    st.header("Delete")
    s3 = make_s3_client(config)
    bucket = config["bucket"]
    datasets = safe_dataset_rows(s3, bucket)
    if not datasets:
        st.info("No datasets found.")
        return
    dataset_id = st.selectbox("Dataset to delete", [row["dataset_id"] for row in datasets])
    prefix = f"datasets/{dataset_id}/"
    objects = list_objects(s3, bucket, prefix)
    total_bytes = sum(obj["size"] for obj in objects)
    derived_present = any("/derived/" in obj["key"] or "/qc/" in obj["key"] for obj in objects)
    st.warning("This deletes the selected dataset prefix from the store.")
    st.write(f"Dataset ID: **{dataset_id}**")
    st.write(f"Total objects: **{len(objects)}**")
    st.write(f"Total bytes: **{human_bytes(total_bytes)}**")
    st.write(f"Last modified: **{max((obj['last_modified'] for obj in objects), default='not available')}**")
    st.write(f"Derived assets present: **{'yes' if derived_present else 'no'}**")
    dry_run = st.checkbox("Dry run", value=True)
    typed = st.text_input("Type the dataset ID to confirm")
    if dry_run:
        with st.expander("Keys that would be deleted"):
            st.code("\n".join(obj["key"] for obj in objects[:500]))
    disabled = typed != dataset_id
    if st.button("Delete dataset", disabled=disabled):
        deleted = delete_keys(s3, bucket, [obj["key"] for obj in objects]) if not dry_run else 0
        if dry_run:
            st.info(f"Dry run: {len(objects)} objects would be deleted.")
        else:
            st.success(f"Deleted {deleted} objects.")


def render_controller(config: dict) -> None:
    st.header("Controller observability")
    controller_url = config.get("controller_url")
    if st_autorefresh:
        st_autorefresh(interval=5000, key="controller-refresh")
    else:
        st.button("Refresh")
    health_payload = controller_health(controller_url)
    st.write(f"Controller reachable: **{'yes' if health_payload.get('ok') else 'no'}**")
    st.json(health_payload)
    if config.get("sqs_queue_url"):
        st.subheader("SQS")
        st.json(sqs_depth(config))
    datasets = [row["dataset_id"] for row in safe_dataset_rows(make_s3_client(config), config["bucket"])]
    if datasets:
        selected = st.selectbox("Dataset to reconcile", datasets)
        if st.button("Reconcile dataset"):
            try:
                st.json(controller_api.reconcile(
                    controller_url, selected,
                    token=config.get("controller_token"),
                ))
            except Exception as exc:
                st.error(str(exc))
    st.subheader("Recent events")
    events = recent_events(controller_url)
    if events is None:
        st.info("Not available - controller does not expose an event log")
    else:
        st.json(events)


def run_doctor_checks(config: dict) -> list[dict]:
    s3 = make_s3_client(config)
    bucket = config["bucket"]
    checks = []

    def add(name: str, fn: Callable[[], tuple[str, str]]) -> None:
        start = time.perf_counter()
        try:
            state, reason = fn()
        except Exception as exc:
            state, reason = "fail", str(exc)
        latency_ms = int((time.perf_counter() - start) * 1000)
        checks.append({
            "check": name,
            "state": state,
            "latency_ms": latency_ms,
            "reason": reason,
        })

    add("endpoint reachable", lambda: _ok(s3.list_buckets(), config["endpoint"]))
    add("bucket exists", lambda: _ok(s3.head_bucket(Bucket=bucket), bucket))
    add("versioning enabled", lambda: _versioning_check(s3, bucket))
    add("encryption enabled", lambda: _encryption_check(s3, bucket))
    add("notification config valid", lambda: _notification_check(s3, bucket, config))
    add("controller health reachable", lambda: _controller_check(config.get("controller_url")))
    if config["resolved_backend"] == "aws" or config.get("sqs_queue_url"):
        add("SQS queue policy correct", lambda: _sqs_policy_check(config))
    return checks


def dataset_rows(s3, bucket: str) -> list[dict]:
    rows = []
    for item in list_datasets(s3, bucket):
        dataset_id = item["dataset_id"]
        prefix = f"datasets/{dataset_id}"
        objects = list_objects(s3, bucket, f"{prefix}/")
        image_objects = [obj for obj in objects if "/source/images/" in obj["key"]]
        mask_objects = [obj for obj in objects if "/source/masks/" in obj["key"]]
        samples_rows = parquet_num_rows(s3, bucket, f"{prefix}/metadata/samples.parquet")
        manifest_valid = bool(read_yaml_object(s3, bucket, f"{prefix}/manifest.yaml"))
        rows.append({
            "dataset_id": dataset_id,
            "modality": manifest_modality(s3, bucket, prefix),
            "images": len(image_objects),
            "masks": len(mask_objects),
            "metadata_rows": samples_rows,
            "total_size": human_bytes(sum(obj["size"] for obj in objects)),
            "last_modified": max((obj["last_modified"] for obj in objects), default=""),
            "manifest_valid": manifest_valid,
        })
    return sorted(rows, key=lambda row: row["last_modified"], reverse=True)


def safe_dataset_rows(s3, bucket: str) -> list[dict]:
    try:
        return dataset_rows(s3, bucket)
    except Exception:
        return []


def preview_scan(source_path: str) -> dict:
    if not source_path:
        raise ValueError("source path is required")
    samples = scan_images(source_path)
    masks = scan_masks(source_path, sample_ids=[sample["sample_id"] for sample in samples])
    first_hash = samples[0]["content_hash"] if samples else ""
    first_file_sha = first_file_digest(source_path)
    return {
        "images": len(samples),
        "masks": len(masks),
        "total_bytes": sum(sample["size"] for sample in samples + masks),
        "sample_ids": [sample["sample_id"] for sample in samples[:20]],
        "first_detected_file_sha256": first_file_sha or first_hash,
    }


def preview_modify(images_path: str, masks_path: str) -> dict:
    samples = scan_images(images_path) if images_path else []
    masks = scan_masks(masks_path, sample_ids=[sample["sample_id"] for sample in samples]) if masks_path else []
    return {
        "images": len(samples),
        "masks": len(masks),
        "total_bytes": sum(sample["size"] for sample in samples + masks),
        "sample_ids": [sample["sample_id"] for sample in samples[:20]],
        "first_detected_file_sha256": (samples[0]["content_hash"] if samples else ""),
    }


def publish_dataset(config: dict, dataset_id: str, source_path: str, metadata_path: str,
                    privacy_unit_column: str, label_column: str | None,
                    modality: str, replace: bool, progress,
                    log: Callable[[str], None]) -> None:
    validate_dataset_id(dataset_id)
    s3 = make_s3_client(config)
    bucket = config["bucket"]
    prefix = f"datasets/{dataset_id}"
    if list_objects(s3, bucket, f"{prefix}/") and not replace:
        raise ValueError(
            "dataset already exists; select explicit atomic replacement"
        )
    samples = scan_images(source_path)
    if not samples:
        raise ValueError("No image files found.")
    masks = scan_masks(source_path, sample_ids=[sample["sample_id"] for sample in samples])
    extra_metadata = read_metadata_table(metadata_path) if metadata_path else None
    build_samples_metadata(
        samples, extra_metadata=extra_metadata,
        privacy_unit_col=privacy_unit_column, label_col=label_column,
    )
    sample_uploads, sample_skips = _partition_uploads(
        s3, bucket, prefix, samples, "images", not replace
    )
    mask_uploads, mask_skips = _partition_uploads(
        s3, bucket, prefix, masks, "masks", not replace
    )
    log(f"Images to upload: {len(sample_uploads)}; skipped: {len(sample_skips)}")
    log(f"Masks to upload: {len(mask_uploads)}; skipped: {len(mask_skips)}")
    transaction = _atomic_upload_sources(
        s3, bucket, prefix, sample_uploads, mask_uploads, replace=replace,
        require_empty=not replace,
    )
    try:
        transaction["mutated"] = True
        published_objects = list_objects(
            s3, bucket, f"{prefix}/source/images/")
        published_mask_objects = list_objects(
            s3, bucket, f"{prefix}/source/masks/")
        published_samples = scan_s3_images(
            s3, bucket, prefix, published_objects)
        published_masks = scan_s3_masks(
            s3, bucket, prefix, published_mask_objects,
            sample_ids=[sample["sample_id"] for sample in published_samples],
        )
        _write_dataset_artifacts(
            s3, bucket, prefix, dataset_id, modality,
            published_samples, published_masks,
            extra_metadata, privacy_unit_col=privacy_unit_column,
            label_col=label_column,
            publish_lock=transaction["publish_lock"],
        )
    except Exception:
        _finish_atomic_publish(s3, bucket, prefix, transaction, commit=False)
        raise
    else:
        if not _finish_atomic_publish(
                s3, bucket, prefix, transaction, commit=True):
            raise RuntimeError("dataset publication lock ownership was lost")
    progress.progress(1.0)
    log("manifest and indexes uploaded")


def modify_dataset(config: dict, dataset_id: str, metadata_path: str,
                   images_path: str, masks_path: str,
                   log: Callable[[str], None]) -> None:
    validate_dataset_id(dataset_id)
    s3 = make_s3_client(config)
    bucket = config["bucket"]
    prefix = f"datasets/{dataset_id}"
    manifest = _read_manifest_strict(s3, bucket, prefix)
    metadata_contract_from_manifest(manifest)
    image_uploads = scan_images(images_path) if images_path else []
    existing = list_objects(s3, bucket, f"{prefix}/source/images/")
    existing_sample_ids = [
        sample["sample_id"]
        for sample in scan_s3_images(s3, bucket, prefix, existing)
    ]
    masks = scan_masks(
        masks_path,
        sample_ids=existing_sample_ids + [
            sample["sample_id"] for sample in image_uploads
        ],
    ) if masks_path else []
    extra_metadata = (
        read_metadata_table(metadata_path) if metadata_path
        else _read_existing_samples_metadata(s3, bucket, prefix)
    )
    transaction = _atomic_upload_sources(
        s3, bucket, prefix, image_uploads, masks, replace=False)
    try:
        transaction["mutated"] = True
        counts = _rescan_dataset_artifacts(
            s3, bucket, dataset_id, extra_metadata=extra_metadata,
            publish_lock=transaction["publish_lock"],
        )
    except Exception:
        _finish_atomic_publish(s3, bucket, prefix, transaction, commit=False)
        raise
    else:
        if not _finish_atomic_publish(
                s3, bucket, prefix, transaction, commit=True):
            raise RuntimeError("dataset publication lock ownership was lost")
    if images_path:
        for sample in image_uploads:
            log(f"uploaded image {sample['sample_id']}")
    if masks_path:
        for mask in masks:
            log(f"uploaded mask {mask['sample_id']}:{mask['primary_filename']}")
    log(f"rescan complete: {counts['samples']} samples, {counts['masks']} masks")


def rescan_dataset(config: dict, dataset_id: str, extra_metadata=None) -> dict:
    validate_dataset_id(dataset_id)
    s3 = make_s3_client(config)
    bucket = config["bucket"]
    prefix = f"datasets/{dataset_id}"
    transaction = _atomic_upload_sources(
        s3, bucket, prefix, [], [], replace=False)
    try:
        transaction["mutated"] = True
        counts = _rescan_dataset_artifacts(
            s3, bucket, dataset_id, extra_metadata=extra_metadata,
            publish_lock=transaction["publish_lock"],
        )
    except Exception:
        _finish_atomic_publish(s3, bucket, prefix, transaction, commit=False)
        raise
    else:
        if not _finish_atomic_publish(
                s3, bucket, prefix, transaction, commit=True):
            raise RuntimeError("dataset publication lock ownership was lost")
    return {"samples": counts["samples"], "masks": counts["masks"]}


def init_aws_store(config: dict, kms_key: str) -> None:
    s3 = make_s3_client(config)
    bucket = config["bucket"]
    region = config.get("region") or "us-east-1"
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        kwargs = {"Bucket": bucket}
        if region != "us-east-1":
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
        s3.create_bucket(**kwargs)
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    encryption = {
        "Rules": [{
            "ApplyServerSideEncryptionByDefault": (
                {"SSEAlgorithm": "aws:kms", "KMSMasterKeyID": kms_key}
                if kms_key else {"SSEAlgorithm": "AES256"}
            )
        }]
    }
    s3.put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration=encryption)


def make_s3_client(config: dict):
    injected = st.session_state.get("_s3_client")
    if injected is not None:
        return injected
    return create_client(
        config.get("endpoint"),
        config.get("access_key"),
        config.get("secret_key"),
        config.get("region", ""),
    )


def compose_status(path: str) -> str:
    try:
        return compose_ps_json(path)
    except Exception:
        return compose_ps(path)


def render_preview(key: str) -> None:
    preview = st.session_state.get(key)
    if not preview:
        return
    cols = st.columns(4)
    cols[0].metric("Images", preview["images"])
    cols[1].metric("Masks", preview["masks"])
    cols[2].metric("Total bytes", human_bytes(preview["total_bytes"]))
    cols[3].metric("Sample IDs shown", len(preview["sample_ids"]))
    st.write("First detected sample IDs")
    st.write(", ".join(preview["sample_ids"]) or "none")
    st.write(f"First detected file SHA-256: `{preview['first_detected_file_sha256']}`")


def render_parquet_preview(s3, bucket: str, key: str, title: str) -> None:
    table = read_parquet_table(s3, bucket, key)
    with st.expander(title):
        if table is None:
            st.write("not available")
        else:
            wide_dataframe(table.slice(0, 50).to_pandas())


def wide_dataframe(data, **kwargs) -> None:
    try:
        st.dataframe(data, width="stretch", **kwargs)
    except TypeError:
        st.dataframe(data, use_container_width=True, **kwargs)


def read_parquet_table(s3, bucket: str, key: str):
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        data = get_object_bytes(s3, bucket, key)
        return pq.read_table(pa.BufferReader(data))
    except Exception:
        return None


def parquet_num_rows(s3, bucket: str, key: str) -> int | None:
    table = read_parquet_table(s3, bucket, key)
    return None if table is None else table.num_rows


def read_text_object(s3, bucket: str, key: str) -> str | None:
    try:
        return get_object_bytes(s3, bucket, key).decode("utf-8")
    except Exception:
        return None


def read_yaml_object(s3, bucket: str, key: str) -> dict | None:
    text = read_text_object(s3, bucket, key)
    if not text:
        return None
    try:
        return yaml.safe_load(text) or {}
    except Exception:
        return None


def manifest_modality(s3, bucket: str, prefix: str) -> str:
    manifest = read_yaml_object(s3, bucket, f"{prefix}/manifest.yaml") or {}
    return manifest.get("modality") or "unknown"


def resource_block(config: dict, dataset_id: str) -> str:
    endpoint = config.get("endpoint", "")
    bucket = config.get("bucket", "")
    region = config.get("region", "")
    prefix = f"datasets/{dataset_id}"
    query = f"endpoint={endpoint}&bucket={bucket}&prefix={prefix}"
    if region:
        query += f"&region={region}"
    return yaml.safe_dump({
        "name": dataset_id,
        "url": f"imaging+dataset://{dataset_id}?{query}",
        "credentials": {
            "identity": "<configured access key>",
            "secret": "<configured secret key>",
        },
    }, sort_keys=False)


def controller_dataset_metric(controller_url: str | None, dataset_id: str, *,
                              token: str | None = None) -> str | None:
    if not controller_url:
        return None
    try:
        for item in controller_api.datasets(controller_url, token=token):
            if item.get("dataset_id") == dataset_id:
                return item.get("last_reconcile_at")
    except Exception:
        return None
    return None


def controller_health(controller_url: str | None) -> dict:
    if not controller_url:
        return {"ok": False, "reason": "controller URL is not set"}
    base = controller_url.rstrip("/")
    for path in ("/healthz", "/health"):
        try:
            response = requests.get(f"{base}{path}", timeout=5)
            payload = response.json()
            if response.status_code < 400:
                payload["ok"] = True
                payload["path"] = path
                return payload
            return {"ok": False, "path": path, "status_code": response.status_code, "body": response.text}
        except Exception:
            continue
    return {"ok": False, "reason": "controller unreachable"}


def recent_events(controller_url: str | None):
    if not controller_url:
        return None
    try:
        response = requests.get(f"{controller_url.rstrip('/')}/recent-events", timeout=5)
        if response.status_code >= 400:
            return None
        return response.json()
    except Exception:
        return None


def sqs_depth(config: dict) -> dict:
    queue_url = config.get("sqs_queue_url")
    if not queue_url:
        return {"configured": False}
    try:
        sqs = st.session_state.get("_sqs_client") or boto3.client(
            "sqs", region_name=config.get("region") or None
        )
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
            ],
        ).get("Attributes", {})
        return {"configured": True, **attrs}
    except Exception as exc:
        return {"configured": True, "error": str(exc)}


def credential_source(config: dict) -> str:
    if config.get("access_key") or config.get("secret_key"):
        return "configured static values"
    return "boto3 default chain"


def first_file_digest(source_path: str) -> str:
    root = Path(source_path)
    if not root.exists():
        return ""
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    return ""


def human_bytes(value: int | None) -> str:
    if value is None:
        return "not available"
    amount = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if amount < 1024 or unit == "TB":
            return f"{amount:.1f} {unit}" if unit != "B" else f"{int(amount)} B"
        amount /= 1024
    return f"{amount:.1f} TB"


def _resolve_value(envvar: str, profile: dict, key: str, fallback: str) -> str:
    env_value = os.environ.get(envvar)
    if env_value not in (None, ""):
        return env_value
    value = profile.get(key)
    if value not in (None, ""):
        return str(value)
    return fallback


def _nested_value(profile: dict, section: str, key: str) -> str:
    value = profile.get(section, {})
    if isinstance(value, dict):
        return str(value.get(key, "") or "")
    return ""


def _normalise_backend(value: str) -> str:
    return value if value in BACKENDS else "auto"


def _secret_badge(value: str) -> str:
    return "set [ok]" if value else "not set [x]"


def _ok(_, reason: str) -> tuple[str, str]:
    return "ok", reason


def _versioning_check(s3, bucket: str) -> tuple[str, str]:
    status = s3.get_bucket_versioning(Bucket=bucket).get("Status", "Disabled")
    return ("ok" if status == "Enabled" else "warn", status)


def _encryption_check(s3, bucket: str) -> tuple[str, str]:
    try:
        response = s3.get_bucket_encryption(Bucket=bucket)
        return "ok", json.dumps(response.get("ServerSideEncryptionConfiguration", {}))
    except Exception as exc:
        return "warn", str(exc)


def _notification_check(s3, bucket: str, config: dict) -> tuple[str, str]:
    response = s3.get_bucket_notification_configuration(Bucket=bucket)
    if config["resolved_backend"] == "aws":
        queues = response.get("QueueConfigurations", [])
        return ("ok" if queues else "warn", f"{len(queues)} SQS target(s)")
    event_configs = response.get("QueueConfigurations", []) or response.get("TopicConfigurations", [])
    return ("ok" if event_configs else "warn", f"{len(event_configs)} notification target(s)")


def _controller_check(controller_url: str | None) -> tuple[str, str]:
    payload = controller_health(controller_url)
    return ("ok" if payload.get("ok") else "warn", json.dumps(payload, default=str))


def _sqs_policy_check(config: dict) -> tuple[str, str]:
    queue_url = config.get("sqs_queue_url")
    if not queue_url:
        return "warn", "SQS queue URL is not set"
    sqs = st.session_state.get("_sqs_client") or boto3.client(
        "sqs", region_name=config.get("region") or None
    )
    attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["Policy"]).get("Attributes", {})
    policy = attrs.get("Policy", "")
    if config.get("bucket") and config["bucket"] in policy:
        return "ok", "bucket source is present in queue policy"
    return "warn", "queue policy does not name the configured bucket"


if __name__ == "__main__":
    run_app()
