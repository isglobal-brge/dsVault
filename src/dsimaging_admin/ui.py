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
import click

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

import requests

from dsimaging_admin import controller as controller_api
from dsimaging_admin.cli import (
    _atomic_upload_sources,
    _delete_dataset_current,
    _finish_atomic_publish,
    _hydrate_local_store_profile,
    _persist_aws_queue_url,
    _publication_metadata,
    _publish_dataset_atomic,
    _purge_dataset,
    _read_manifest_strict,
    _rescan_dataset_artifacts,
    _validate_controller_url,
)
from dsimaging_admin.manifest import (
    build_samples_metadata,
    metadata_contract_from_manifest,
    read_metadata_table,
    scan_images,
    scan_masks,
    scan_s3_images,
    validate_dataset_id,
    validate_dicom_series,
)
from dsimaging_admin.hashing import sha256_file
from dsimaging_admin.s3 import (
    create_client,
    create_sqs_client,
    detect_backend,
    get_object_bytes,
    is_loopback_s3_endpoint,
    is_native_aws_s3_endpoint,
    list_datasets,
    list_object_versions,
    list_objects,
    provision_aws_store,
    validate_s3_endpoint,
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
              open_browser: bool = False,
              environment: dict[str, str] | None = None) -> None:
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
    child_environment = os.environ.copy()
    child_environment.update(environment or {})
    raise SystemExit(subprocess.call(args, env=child_environment))


def run_app() -> None:
    st.set_page_config(page_title="dsimaging-admin", layout="wide")
    st.title("dsimaging-admin")
    st.caption("Local operator dashboard for dsimaging-store")

    try:
        profiles = load_profiles()
        selected_profile = profile_picker(profiles)
    except ValueError as exc:
        st.error(str(exc))
        return
    try:
        profile = _hydrate_connection_profile(
            profiles.get(selected_profile, {}))
        config = edit_connection_config(profile, selected_profile)
    except Exception as exc:
        st.error(str(exc))
        return
    config["_profile"] = selected_profile
    config["_config_path"] = (
        os.environ.get("DSIMAGING_CONFIG") or DEFAULT_CONFIG_PATH
    )
    _reset_connection_scoped_results(config)

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
        backend = os.environ.get("DSIMAGING_BACKEND", "auto")
        endpoint = os.environ.get("DSIMAGING_ENDPOINT")
        if endpoint is None:
            endpoint = (
                "" if backend in {"aws", "s3-compatible"}
                else "http://127.0.0.1:9000"
            )
        return {
            "default": {
                "endpoint": endpoint,
                "bucket": os.environ.get("DSIMAGING_BUCKET", "imaging-data"),
                "access_key": os.environ.get("DSIMAGING_ACCESS_KEY", ""),
                "secret_key": os.environ.get("DSIMAGING_SECRET_KEY", ""),
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
    except Exception as exc:
        raise ValueError("Could not read the dsimaging configuration file") from exc
    if not isinstance(raw, dict):
        raise ValueError("The dsimaging configuration must contain a mapping")
    st.session_state["_config_default_profile"] = raw.get("default_profile")
    profiles = raw.get("profiles")
    if isinstance(profiles, dict):
        if not profiles:
            raise ValueError("The dsimaging configuration defines no profiles")
        if not all(value is None or isinstance(value, dict)
                   for value in profiles.values()):
            raise ValueError("Every dsimaging profile must contain a mapping")
        if any(
            value is not None
            and value.get("aws") is not None
            and not isinstance(value["aws"], dict)
            for value in profiles.values()
        ):
            raise ValueError("Every AWS profile section must contain a mapping")
        return {str(name): (value or {}) for name, value in profiles.items()}
    if profiles is not None:
        raise ValueError("The dsimaging 'profiles' value must be a mapping")
    default = raw.get("default", raw)
    if not isinstance(default, dict):
        raise ValueError("The default dsimaging profile must contain a mapping")
    if default.get("aws") is not None and not isinstance(default["aws"], dict):
        raise ValueError("Every AWS profile section must contain a mapping")
    return {"default": default or {}}


def profile_picker(profiles: dict[str, dict]) -> str:
    names = sorted(profiles) or ["default"]
    default_name = (
        os.environ.get("DSIMAGING_PROFILE")
        or st.session_state.get("_config_default_profile")
    )
    if default_name and default_name not in names:
        raise ValueError(
            f"Configured profile '{default_name}' does not exist"
        )
    index = names.index(default_name) if default_name in names else 0
    return st.sidebar.selectbox("Profile", names, index=index)


def _hydrate_connection_profile(profile: dict) -> dict:
    """Resolve a local pointer unless an explicit remote target supersedes it."""
    try:
        return _hydrate_local_store_profile(profile)
    except click.ClickException:
        requested_backend = (
            os.environ.get("DSIMAGING_BACKEND")
            or profile.get("backend") or "auto")
        alternate_store_requested = bool(
            os.environ.get("DSIMAGING_ENDPOINT")
            or profile.get("endpoint")
            or requested_backend in {"aws", "s3-compatible"}
        )
        if (profile.get("kind") == "local-store"
                and alternate_store_requested):
            return dict(profile)
        raise


def edit_connection_config(profile: dict, profile_name: str = "default") -> dict:
    st.sidebar.subheader("Connection")
    backend = st.sidebar.selectbox(
        "Backend override",
        BACKENDS,
        index=BACKENDS.index(_normalise_backend(_resolve_value(
            "DSIMAGING_BACKEND", profile, "backend", "auto"))),
    )
    endpoint_profile = {} if backend == "aws" else profile
    endpoint_fallback = (
        "" if backend in {"aws", "s3-compatible"}
        else "http://127.0.0.1:9000"
    )
    endpoint_default = _resolve_value(
        "DSIMAGING_ENDPOINT",
        endpoint_profile,
        "endpoint",
        endpoint_fallback,
    )
    validate_s3_endpoint(endpoint_default or None)
    endpoint = st.sidebar.text_input(
        "Endpoint",
        value=endpoint_default,
    )
    bucket = st.sidebar.text_input(
        "Bucket",
        value=_resolve_value("DSIMAGING_BUCKET", profile, "bucket", "imaging-data"),
    )
    region = st.sidebar.text_input(
        "Region",
        value=_resolve_value("DSIMAGING_REGION", profile, "region", ""),
    )
    s3_credential_scope = (profile_name, backend, endpoint.rstrip("/"))
    if st.session_state.get("_s3_credential_scope") != s3_credential_scope:
        st.session_state.pop("_s3_access_key", None)
        st.session_state.pop("_s3_secret_key", None)
        st.session_state["_s3_credential_scope"] = s3_credential_scope
    entered_access_key = st.sidebar.text_input(
        "Access key",
        value="",
        type="password",
        key="_s3_access_key",
        help="Enter for this browser session; stored credentials are never prefilled.",
    )
    entered_secret_key = st.sidebar.text_input(
        "Secret key",
        value="",
        type="password",
        key="_s3_secret_key",
        help="Enter for this browser session; stored credentials are never prefilled.",
    )
    controller_url_default = _resolve_value(
        "DSIMAGING_CONTROLLER_URL", profile, "controller_url", "")
    _validate_controller_url(controller_url_default or None)
    controller_url = st.sidebar.text_input(
        "Controller URL",
        value=controller_url_default,
    )
    _validate_controller_url(controller_url or None)
    controller_credential_scope = (profile_name, controller_url.rstrip("/"))
    if (st.session_state.get("_controller_credential_scope")
            != controller_credential_scope):
        st.session_state.pop("_controller_token", None)
        st.session_state["_controller_credential_scope"] = (
            controller_credential_scope)
    entered_controller_token = st.sidebar.text_input(
        "Controller operator token",
        value="",
        type="password",
        key="_controller_token",
        help="Enter for this browser session; stored tokens are never prefilled.",
    )
    sqs_queue_url = st.sidebar.text_input(
        "SQS queue URL",
        value=os.environ.get("DSIMAGING_SQS_QUEUE_URL", "")
        or _nested_value(profile, "aws", "sqs_queue_url"),
    )
    validate_s3_endpoint(endpoint or None)
    if backend == "s3-compatible" and not endpoint:
        raise ValueError(
            "s3-compatible mode requires an explicit endpoint URL")
    if backend == "aws" and endpoint and not is_native_aws_s3_endpoint(endpoint):
        raise ValueError(
            "AWS mode accepts only a native HTTPS S3 endpoint; "
            "leave Endpoint empty to use the AWS SDK default"
        )
    local_endpoint = profile.get("_local_endpoint")
    use_local_secrets = bool(
        profile.get("kind") == "local-store"
        and local_endpoint
        and endpoint.rstrip("/") == str(local_endpoint).rstrip("/")
    )
    access_key = (
        entered_access_key
        or os.environ.get("DSIMAGING_ACCESS_KEY", "")
        or (profile.get("access_key", "") if use_local_secrets else "")
    )
    secret_key = (
        entered_secret_key
        or os.environ.get("DSIMAGING_SECRET_KEY", "")
        or (profile.get("secret_key", "") if use_local_secrets else "")
    )
    local_controller_url = profile.get("_local_controller_url")
    use_local_controller_token = bool(
        profile.get("kind") == "local-store"
        and local_controller_url
        and controller_url.rstrip("/") == str(local_controller_url).rstrip("/")
    )
    controller_token = (
        entered_controller_token
        or os.environ.get("DSIMAGING_CONTROLLER_TOKEN", "")
        or (profile.get("controller_token", "")
            if use_local_controller_token else "")
    )
    st.sidebar.markdown(
        f"Access key: **{_secret_badge(access_key)}**  \n"
        f"Secret key: **{_secret_badge(secret_key)}**  \n"
        f"Controller token: **{_secret_badge(controller_token)}**"
    )
    if use_local_secrets or use_local_controller_token:
        st.sidebar.caption(
            "Local-store credentials are resolved server-side and never "
            "prefilled into browser fields."
        )
    elif any(profile.get(key) for key in (
            "access_key", "secret_key", "controller_token")):
        st.sidebar.caption(
            "Credentials embedded in non-local profiles are deliberately not "
            "loaded into this unauthenticated dashboard."
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
        "kind": profile.get("kind"),
        "store_path": profile.get("store_path"),
    }


def _connection_scope(config: dict) -> tuple:
    return (
        config.get("_profile"),
        config.get("backend"),
        config.get("resolved_backend"),
        config.get("endpoint"),
        config.get("bucket"),
        config.get("region"),
        config.get("controller_url"),
        config.get("sqs_queue_url"),
        config.get("kind"),
        config.get("store_path"),
    )


def _reset_connection_scoped_results(config: dict) -> None:
    scope = _connection_scope(config)
    if st.session_state.get("_connection_scope") == scope:
        return
    for key in (
        "doctor_checks",
        "compose_status",
        "compose-project",
        "store-project-path",
        "store-bucket",
        "store-region",
        "store-kms-key",
        "publish_preview",
        "publish_preview_signature",
        "modify_preview",
        "modify-dataset",
        "modify-metadata",
        "modify-images",
        "modify-masks",
        "delete-dataset",
        "delete-dry-run",
        "delete-purge",
        "delete-confirmation",
    ):
        st.session_state.pop(key, None)
    st.session_state["_connection_scope"] = scope


def _has_owned_local_compose_project(config: dict) -> bool:
    return bool(
        config.get("kind") == "local-store"
        and config.get("store_path")
        and is_loopback_s3_endpoint(config.get("endpoint"))
    )


def _can_initialize_local_project(config: dict) -> bool:
    return bool(
        config.get("resolved_backend") == "minio"
        and is_loopback_s3_endpoint(config.get("endpoint"))
    )


def _owned_local_compose_path(config: dict) -> str | None:
    if not _has_owned_local_compose_project(config):
        return None
    return str(Path(config["store_path"]).expanduser().resolve())


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
    owned_compose_path = _owned_local_compose_path(config)
    local_init = _can_initialize_local_project(config)
    aws_profile_conflict = bool(
        backend == "aws" and config.get("kind") == "local-store")
    default_project = config.get("store_path") or "./study-store"
    with st.form("store-init-form"):
        st.subheader("Init store")
        project_path = st.text_input(
            "Local store project path", value=default_project,
            key="store-project-path")
        bucket = st.text_input(
            "Bucket name", value=config["bucket"] or "imaging-data",
            key="store-bucket")
        region = st.text_input(
            "Region", value=config["region"], key="store-region")
        kms_key = st.text_input(
            "KMS key", value="", type="password", key="store-kms-key")
        run = st.form_submit_button("Run", disabled=aws_profile_conflict)
    if aws_profile_conflict:
        st.warning(
            "This profile owns a local Compose project and cannot be reused "
            "for AWS provisioning. Select or create a separate AWS profile."
        )
    if run:
        with st.expander("Operation log", expanded=True):
            try:
                if backend == "aws":
                    report = init_aws_store(
                        config | {"bucket": bucket, "region": region}, kms_key)
                    st.success("AWS store provisioning complete.")
                    st.write(report["steps"])
                    if report.get("sqs_queue_url"):
                        _persist_aws_queue_url(
                            config.get("_profile") or "default",
                            report["sqs_queue_url"],
                            bucket=bucket,
                            region=report.get("region") or region,
                            endpoint=config.get("endpoint") or None,
                            config_path=config.get("_config_path"),
                        )
                        config["sqs_queue_url"] = report["sqs_queue_url"]
                        st.info("SQS queue URL saved to the selected profile.")
                elif not local_init:
                    st.error(
                        "An existing remote endpoint is connect-only; "
                        "configure it and run Doctor instead."
                    )
                else:
                    cfg = init_store(
                        project_path,
                        force=False,
                        controller_image=DEFAULT_CONTROLLER_IMAGE,
                        bucket=bucket,
                    )
                    st.write(cfg.to_dict())
            except Exception as exc:
                st.error(str(exc))

    st.subheader("Compose controls")
    st.text_input(
        "Compose project path",
        value=owned_compose_path or default_project,
        key="compose-project",
        disabled=True,
        help="Compose controls are scoped to the selected local-store profile.",
    )
    disabled = owned_compose_path is None
    cols = st.columns(3)
    if cols[0].button("Up", disabled=disabled, help="Disabled for AWS backend"):
        st.code(compose_up(owned_compose_path))
    if cols[1].button("Down", disabled=disabled, help="Disabled for AWS backend"):
        st.code(compose_down(owned_compose_path))
    if cols[2].button("Refresh status", disabled=disabled):
        st.session_state["compose_status"] = compose_status(owned_compose_path)
    if disabled:
        st.info(
            "Remote backend: local Compose controls are unavailable; "
            "operate that deployment separately."
        )
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
        "Metadata path (optional when SOURCE/metadata.csv or .parquet is unique)",
        key="publish-metadata")
    privacy_unit_column = st.text_input(
        "Patient privacy-unit column", key="publish-privacy-unit-column"
    )
    label_column = st.text_input(
        "Label column (optional)", key="publish-label-column"
    )
    label_levels_text = st.text_input(
        "Public label levels (optional, comma-separated)",
        key="publish-label-levels",
    )
    replace = st.checkbox("Replace an existing dataset atomically", value=False)
    modality = st.selectbox("Modality", ["ct", "mri", "pet", "xray", "unknown"], index=0)
    verify_mode = st.selectbox(
        "Verify before commit", ["quick", "full", "none"], index=0)
    label_levels = [
        value.strip() for value in label_levels_text.split(",")
        if value.strip()
    ]
    input_signature = hashlib.sha256(json.dumps({
        "dataset_id": dataset_id,
        "source_path": source_path,
        "metadata_path": metadata_path,
        "privacy_unit_column": privacy_unit_column,
        "label_column": label_column,
        "label_levels": label_levels_text,
        "replace": replace,
        "modality": modality,
        "verify": verify_mode,
        "profile": config.get("_profile"),
        "endpoint": config.get("endpoint"),
        "bucket": config.get("bucket"),
    }, sort_keys=True).encode("utf-8")).hexdigest()
    if st.button("Preview scan"):
        st.session_state.pop("publish_preview_signature", None)
        try:
            st.session_state["publish_preview"] = publication_preflight(
                dataset_id,
                source_path,
                metadata_path or None,
                privacy_unit_column or None,
                label_column or None,
                label_levels,
            )
            if list_objects(
                    make_s3_client(config), config["bucket"],
                    f"datasets/{dataset_id}/") and not replace:
                raise ValueError(
                    "dataset already exists; select explicit atomic replacement"
                )
            st.session_state["publish_preview_signature"] = input_signature
        except Exception as exc:
            st.error(str(exc))
    preview_current = (
        st.session_state.get("publish_preview_signature") == input_signature
    )
    if preview_current:
        render_preview("publish_preview")
    elif st.session_state.get("publish_preview"):
        st.info("Inputs changed; run Preview scan again before publishing.")
    if st.button("Publish", disabled=not preview_current):
        progress = st.progress(0)
        log: list[str] = []
        with st.expander("Per-object log", expanded=True):
            try:
                ensure_publication_preflight_current(
                    st.session_state.get("publish_preview"),
                    dataset_id,
                    source_path,
                    metadata_path or None,
                    privacy_unit_column or None,
                    label_column or None,
                    label_levels,
                )
            except Exception as exc:
                st.session_state.pop("publish_preview_signature", None)
                st.error(str(exc))
                return
            try:
                publish_dataset(
                    config, dataset_id, source_path, metadata_path,
                    privacy_unit_column, label_column or None, modality,
                    replace, progress, log.append,
                    label_levels=label_levels,
                    verify_mode=verify_mode,
                    expected_input_fingerprint=(
                        st.session_state["publish_preview"]["input_fingerprint"]),
                )
                st.success("Publish complete.")
            except Exception as exc:
                st.error(str(exc))
            st.code("\n".join(log))


def render_modify_rescan(config: dict) -> None:
    st.header("Modify / Rescan")
    s3 = make_s3_client(config)
    bucket = config["bucket"]
    rows = safe_dataset_rows(s3, bucket)
    if rows is None:
        return
    datasets = [row["dataset_id"] for row in rows]
    if not datasets:
        st.info("No datasets found.")
        return
    dataset_id = st.selectbox("Dataset", datasets, key="modify-dataset")
    metadata_path = st.text_input("Replace metadata file", key="modify-metadata")
    images_path = st.text_input("Add more images path", key="modify-images")
    masks_path = st.text_input("Add more masks path", key="modify-masks")
    if st.button("Preview modify"):
        st.session_state["modify_preview"] = preview_modify(images_path, masks_path)
    render_preview("modify_preview")
    if st.button(
            "Apply modify",
            disabled=not any((metadata_path, images_path, masks_path))):
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
    try:
        current_objects = list_objects(s3, bucket, "datasets/")
    except Exception:
        st.error("Could not list current datasets. Check the connection and credentials.")
        return
    try:
        all_versions = list_object_versions(s3, bucket, "datasets/")
        version_listing_error = False
    except Exception:
        all_versions = []
        version_listing_error = True
    current_ids = _dataset_ids_from_storage_items(current_objects)
    historical_ids = _dataset_ids_from_storage_items(all_versions)
    dataset_ids = sorted(current_ids | historical_ids)
    if not dataset_ids:
        if version_listing_error:
            st.error(
                "Could not determine the dataset inventory because version "
                "history could not be listed. Check storage permissions."
            )
        else:
            st.info("No datasets found.")
        return
    dataset_id = st.selectbox(
        "Dataset to delete", dataset_ids, key="delete-dataset")
    prefix = f"datasets/{dataset_id}/"
    objects = [item for item in current_objects if item["key"].startswith(prefix)]
    versions = [item for item in all_versions if item["key"].startswith(prefix)]
    dry_run = st.checkbox("Dry run", value=True, key="delete-dry-run")
    purge_versions = st.checkbox(
        "Purge all object versions and delete markers", value=False,
        key="delete-purge",
        help=("Leave unchecked to preserve version history. Selecting this "
              "performs the same irreversible action as 'dataset purge'."),
    )
    inventory = versions if purge_versions else objects
    total_bytes = sum(obj.get("size", 0) for obj in inventory)
    derived_present = any(
        "/derived/" in obj["key"] or "/qc/" in obj["key"]
        for obj in inventory)
    st.warning(
        "This deletes current objects under the selected dataset prefix. "
        "Enable version purging below to remove its stored history."
    )
    st.write(f"Dataset ID: **{dataset_id}**")
    inventory_label = "stored versions/markers" if purge_versions else "current objects"
    st.write(f"Total {inventory_label}: **{len(inventory)}**")
    st.write(f"Total bytes: **{human_bytes(total_bytes)}**")
    st.write(f"Last modified: **{max((obj.get('last_modified') or '' for obj in inventory), default='not available') or 'not available'}**")
    st.write(f"Derived assets present: **{'yes' if derived_present else 'no'}**")
    if not objects and versions:
        st.info(
            "Only preserved history remains; select version purging to remove it.")
    if version_listing_error:
        st.warning(
            "Version history could not be listed; irreversible purge is unavailable.")
    typed = st.text_input(
        "Type the dataset ID to confirm", key="delete-confirmation")
    if dry_run:
        with st.expander("Keys that would be deleted"):
            if purge_versions:
                st.code("\n".join(
                    f"{obj['key']} @ {obj.get('version_id')}"
                    + (" [delete marker]" if obj.get("is_delete_marker") else "")
                    for obj in inventory[:500]
                ))
            else:
                st.code("\n".join(obj["key"] for obj in inventory[:500]))
            if len(inventory) > 500:
                st.caption(
                    f"Showing the first 500 of {len(inventory)} entries; "
                    f"{len(inventory) - 500} omitted from this preview."
                )
    disabled = (
        typed != dataset_id
        or (not objects and not purge_versions)
        or (purge_versions and version_listing_error)
    )
    if st.button("Delete dataset", disabled=disabled):
        if dry_run:
            st.info(
                f"Dry run: {len(inventory)} {inventory_label} would be deleted.")
        else:
            try:
                if purge_versions:
                    version_deleted = _purge_dataset(
                        s3, bucket, dataset_id)
                else:
                    deleted = _delete_dataset_current(
                        s3, bucket, dataset_id)
            except Exception as exc:
                st.error(str(exc))
                return
            if purge_versions:
                st.success(
                    f"Deleted {version_deleted} object versions/delete markers."
                )
            else:
                st.success(
                    f"Deleted {deleted} current objects; version history was preserved."
                )


def _dataset_ids_from_storage_items(items: list[dict]) -> set[str]:
    dataset_ids = set()
    for item in items:
        parts = str(item.get("key", "")).split("/", 2)
        if len(parts) != 3 or parts[0] != "datasets":
            continue
        try:
            dataset_ids.add(validate_dataset_id(parts[1]))
        except ValueError:
            continue
    return dataset_ids


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
    rows = safe_dataset_rows(make_s3_client(config), config["bucket"])
    if rows is None:
        return
    datasets = [row["dataset_id"] for row in rows]
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


def safe_dataset_rows(s3, bucket: str) -> list[dict] | None:
    try:
        return dataset_rows(s3, bucket)
    except Exception:
        st.error("Could not list datasets. Check the connection and credentials.")
        st.info("Run Connect & Doctor before retrying this operation.")
        return None


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


def publication_preflight(dataset_id: str, source_path: str,
                          metadata_path: str | None,
                          privacy_unit_column: str | None,
                          label_column: str | None,
                          label_levels: list[str]) -> dict:
    """Validate every local publication input without opening S3."""
    validate_dataset_id(dataset_id)
    if not source_path:
        raise ValueError("source path is required")
    samples = scan_images(source_path)
    if not samples:
        raise ValueError("No image files found.")
    masks = scan_masks(
        source_path,
        sample_ids=[sample["sample_id"] for sample in samples],
    )
    metadata_path = _publication_metadata(source_path, metadata_path)
    metadata = read_metadata_table(metadata_path) if metadata_path else None
    if not privacy_unit_column:
        if metadata is not None and "patient_id" in metadata.column_names:
            privacy_unit_column = "patient_id"
        else:
            raise ValueError(
                "Could not infer the patient privacy unit; select its metadata column"
            )
    build_samples_metadata(
        samples,
        extra_metadata=metadata,
        privacy_unit_col=privacy_unit_column,
        label_col=label_column,
        label_levels=label_levels,
    )
    input_fingerprint = _publication_input_fingerprint(
        samples, masks, metadata_path)
    return {
        "images": len(samples),
        "masks": len(masks),
        "total_bytes": sum(sample["size"] for sample in samples + masks),
        "sample_ids": [sample["sample_id"] for sample in samples[:20]],
        "first_detected_file_sha256": (
            first_file_digest(source_path) or samples[0]["content_hash"]
        ),
        "metadata": metadata_path,
        "privacy_unit_column": privacy_unit_column,
        "warnings": validate_dicom_series(source_path),
        "input_fingerprint": input_fingerprint,
    }


def _publication_input_fingerprint(samples: list[dict], masks: list[dict],
                                   metadata_path: str | None) -> str:
    def asset(item: dict) -> dict:
        return {
            "sample_id": item.get("sample_id"),
            "source_kind": item.get("source_kind"),
            "uri_path": item.get("uri_path"),
            "content_hash": item.get("content_hash"),
            "size": item.get("size"),
            "files": item.get("files"),
        }

    payload = {
        "samples": [asset(item) for item in samples],
        "masks": [asset(item) for item in masks],
        "metadata_path": (
            str(Path(metadata_path).resolve()) if metadata_path else None),
        "metadata_sha256": sha256_file(metadata_path) if metadata_path else None,
    }
    return hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()


def ensure_publication_preflight_current(
    expected: dict | None,
    dataset_id: str,
    source_path: str,
    metadata_path: str | None,
    privacy_unit_column: str | None,
    label_column: str | None,
    label_levels: list[str],
) -> dict:
    """Re-scan local inputs so Publish cannot use a stale preview."""
    if not expected:
        raise ValueError("Run Preview scan before publishing.")
    current = publication_preflight(
        dataset_id,
        source_path,
        metadata_path,
        privacy_unit_column,
        label_column,
        label_levels,
    )
    if current["input_fingerprint"] != expected.get("input_fingerprint"):
        raise ValueError(
            "Local images, masks or metadata changed after Preview scan; "
            "review the updated inputs before publishing."
        )
    return current


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
                    log: Callable[[str], None], *,
                    label_levels: list[str] | None = None,
                    verify_mode: str = "quick",
                    expected_input_fingerprint: str | None = None) -> None:
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
    for warning in validate_dicom_series(source_path):
        log(f"WARN: {warning}")
    masks = scan_masks(source_path, sample_ids=[sample["sample_id"] for sample in samples])
    metadata_path = _publication_metadata(source_path, metadata_path or None)
    extra_metadata = read_metadata_table(metadata_path) if metadata_path else None
    observed_fingerprint = _publication_input_fingerprint(
        samples, masks, metadata_path)
    if (expected_input_fingerprint is not None
            and observed_fingerprint != expected_input_fingerprint):
        raise ValueError(
            "Local images, masks or metadata changed after Preview scan; "
            "review the updated inputs before publishing."
        )
    if not privacy_unit_column:
        if extra_metadata is not None and "patient_id" in extra_metadata.column_names:
            privacy_unit_column = "patient_id"
        else:
            raise ValueError(
                "Could not infer the patient privacy unit; select its metadata column"
            )
    build_samples_metadata(
        samples, extra_metadata=extra_metadata,
        privacy_unit_col=privacy_unit_column, label_col=label_column,
        label_levels=label_levels,
    )
    log(f"Images to upload: {len(samples)}")
    log(f"Masks to upload: {len(masks)}")
    _publish_dataset_atomic(
        s3, bucket, dataset_id, samples, masks,
        extra_metadata=extra_metadata,
        privacy_unit_col=privacy_unit_column,
        label_col=label_column,
        label_levels=label_levels,
        modality=modality,
        replace=replace,
        verify_mode=verify_mode,
    )
    if progress is not None:
        progress.progress(1.0)
    if verify_mode == "none":
        log("manifest and indexes uploaded; verification skipped (explicit none)")
    else:
        log(f"manifest and indexes uploaded; {verify_mode} verification complete")


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
    # Existing metadata must be read by _rescan_dataset_artifacts only after
    # the publication lock is held; otherwise a queued modify can overwrite a
    # newer metadata update with a stale pre-lock snapshot.
    extra_metadata = read_metadata_table(metadata_path) if metadata_path else None
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


def init_aws_store(config: dict, kms_key: str) -> dict:
    endpoint = config.get("endpoint") or None
    if endpoint and not is_native_aws_s3_endpoint(endpoint):
        raise ValueError(
            "AWS provisioning accepts only a native HTTPS S3 endpoint; "
            "leave the endpoint empty to use the AWS SDK default"
        )
    return provision_aws_store(
        endpoint,
        config["bucket"],
        region=config.get("region") or "us-east-1",
        access_key=config.get("access_key") or None,
        secret_key=config.get("secret_key") or None,
        kms_key=kms_key or None,
    )


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
    if preview.get("metadata"):
        st.write(f"Metadata: `{preview['metadata']}`")
    if preview.get("privacy_unit_column"):
        st.write(f"Privacy unit column: `{preview['privacy_unit_column']}`")
    for warning in preview.get("warnings", []):
        st.warning(warning)


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
    validate_s3_endpoint(endpoint or None)
    bucket = config.get("bucket", "")
    region = config.get("region", "")
    prefix = f"datasets/{dataset_id}"
    parameters = {"endpoint": endpoint, "bucket": bucket, "prefix": prefix}
    if region:
        parameters["region"] = region
    query = requests.compat.urlencode(parameters)
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
        sqs = st.session_state.get("_sqs_client") or create_sqs_client(
            config.get("access_key"),
            config.get("secret_key"),
            config.get("region") or "",
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
    sqs = st.session_state.get("_sqs_client") or create_sqs_client(
        config.get("access_key"),
        config.get("secret_key"),
        config.get("region") or "",
    )
    attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["Policy"]).get("Attributes", {})
    policy = attrs.get("Policy", "")
    if config.get("bucket") and config["bucket"] in policy:
        return "ok", "bucket source is present in queue policy"
    return "warn", "queue policy does not name the configured bucket"


if __name__ == "__main__":
    run_app()
