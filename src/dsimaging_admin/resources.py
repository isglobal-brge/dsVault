"""Pure builders for read-only DataSHIELD resource handoff plans."""

from __future__ import annotations

import json
import re
from urllib.parse import quote, urlencode, urlparse

from .manifest import validate_dataset_id
from .s3 import validate_s3_endpoint


RESOURCE_IDENTITY_ENV = "DSIMAGING_RESOURCE_ACCESS_KEY"
RESOURCE_SECRET_ENV = "DSIMAGING_RESOURCE_SECRET_KEY"
_RESOURCE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_ARMADILLO_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")
_CONSUMER_ENDPOINT_RE = re.compile(
    r"^https?://[A-Za-z0-9][A-Za-z0-9.-]*(?::([0-9]{1,5}))?$"
)
_CONSUMER_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]*[a-z0-9]$")
_CONSUMER_REGION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9-]*$")


def build_direct_resource_url(
    dataset_id: str,
    *,
    endpoint: str,
    bucket: str,
    region: str = "",
) -> str:
    """Build the direct URL accepted by the dsImaging ResourceClient."""
    _validate_consumer_location(dataset_id, endpoint, bucket, region)
    parameters = {
        "endpoint": endpoint,
        "bucket": bucket,
        "prefix": f"datasets/{dataset_id}",
    }
    if region:
        parameters["region"] = region
    return f"imaging+dataset://{dataset_id}?{urlencode(parameters)}"


def build_opal_resource_plan(
    *,
    dataset_id: str,
    profile: str,
    bucket: str,
    endpoint: str,
    region: str,
    project: str,
    resource_name: str | None,
    manifest_schema_version: int,
) -> dict:
    """Return an inert Opal handoff plan containing no credential values."""
    validate_dataset_id(dataset_id)
    project = _resource_name(project, "project")
    resource_name = _resource_name(
        resource_name or dataset_id, "resource name")
    resource_url = build_direct_resource_url(
        dataset_id, endpoint=endpoint, bucket=bucket, region=region)
    command = (
        "opalr::opal.resource_create(opal, "
        f"project = {_r_string(project)}, "
        f"name = {_r_string(resource_name)}, "
        f"url = {_r_string(resource_url)}, "
        f"identity = Sys.getenv({_r_string(RESOURCE_IDENTITY_ENV)}), "
        f"secret = Sys.getenv({_r_string(RESOURCE_SECRET_ENV)}))"
    )
    return {
        "target": "opal",
        "registered": False,
        "profile": profile,
        "dataset_id": dataset_id,
        "manifest": _manifest_summary(
            dataset_id, bucket, manifest_schema_version),
        "resource": {
            "project": project,
            "name": resource_name,
            "url": resource_url,
        },
        "credential_environment": {
            "identity_env": RESOURCE_IDENTITY_ENV,
            "secret_env": RESOURCE_SECRET_ENV,
        },
        "instructions": [
            "Install dsImaging on the Opal R server so its Resource form and resolver are available.",
            "Set the two environment variables to dedicated read-only object-store credentials.",
            "Run the R command with an authenticated opalr connection named opal.",
        ],
        "commands": [command],
    }


def build_armadillo_resource_plan(
    *,
    dataset_id: str,
    profile: str,
    bucket: str,
    endpoint: str,
    region: str,
    project: str,
    resource_name: str | None,
    armadillo_url: str,
    credentials_ref: str,
    manifest_schema_version: int,
) -> dict:
    """Return the marker Resource and node registry plan used by Armadillo."""
    validate_dataset_id(dataset_id)
    project = _armadillo_name(project, "Armadillo project")
    resource_name = _armadillo_name(
        resource_name or _default_armadillo_name(dataset_id),
        "Armadillo resource name",
    )
    credentials_ref = _resource_name(credentials_ref, "credentials_ref")
    armadillo_url = _service_base_url(armadillo_url, "Armadillo URL")
    _validate_consumer_location(dataset_id, endpoint, bucket, region)

    marker_name = f"{resource_name}_marker"
    marker_object = quote(f"markers/{marker_name}.parquet", safe="")
    marker_url = (
        f"{armadillo_url}/storage/projects/{project}/objects/{marker_object}"
    )
    resource_format = f"dsimaging-dataset:{dataset_id}"
    registry_entry = {
        "enabled": True,
        "backend": "s3",
        "manifest_uri": _manifest_uri(dataset_id, bucket),
    }
    if endpoint:
        registry_entry["endpoint"] = endpoint
    registry_entry["credentials_ref"] = credentials_ref
    if region:
        registry_entry["region"] = region

    commands = [
        "marker <- data.frame(selector = TRUE)",
        (
            "MolgenisArmadillo::armadillo.upload_table("
            f"project = {_r_string(project)}, folder = \"markers\", "
            f"table = marker, name = {_r_string(marker_name)})"
        ),
        (
            "images <- resourcer::newResource("
            f"name = {_r_string(dataset_id)}, url = {_r_string(marker_url)}, "
            f"format = {_r_string(resource_format)})"
        ),
        (
            "MolgenisArmadillo::armadillo.upload_resource("
            f"project = {_r_string(project)}, folder = \"resources\", "
            f"resource = images, name = {_r_string(resource_name)})"
        ),
    ]
    return {
        "target": "armadillo",
        "registered": False,
        "profile": profile,
        "dataset_id": dataset_id,
        "manifest": _manifest_summary(
            dataset_id, bucket, manifest_schema_version),
        "marker": {
            "project": project,
            "folder": "markers",
            "name": marker_name,
            "table": {"selector": True},
        },
        "resource": {
            "project": project,
            "folder": "resources",
            "name": resource_name,
            "descriptor_name": dataset_id,
            "url": marker_url,
            "format": resource_format,
        },
        "registry": {
            "schema_version": 1,
            dataset_id: registry_entry,
        },
        "instructions": [
            "Merge the registry fragment into the dsImaging node registry.",
            "Resolve credentials_ref from protected server configuration using read-only object-store credentials.",
            "Run the R commands as a trusted Armadillo administrator.",
        ],
        "commands": commands,
    }


def _manifest_uri(dataset_id: str, bucket: str) -> str:
    return f"s3://{bucket}/datasets/{dataset_id}/manifest.yaml"


def _manifest_summary(
        dataset_id: str, bucket: str, schema_version: int) -> dict:
    return {
        "uri": _manifest_uri(dataset_id, bucket),
        "schema_version": schema_version,
        "validated": True,
    }


def _default_armadillo_name(dataset_id: str) -> str:
    return dataset_id.replace(".", "_").replace("-", "_")


def _validate_consumer_location(
        dataset_id: str, endpoint: str, bucket: str, region: str) -> None:
    """Match the S3 location contract enforced by dsImaging's R resolver."""
    validate_dataset_id(dataset_id)
    validate_s3_endpoint(endpoint or None)
    endpoint_match = (
        _CONSUMER_ENDPOINT_RE.fullmatch(endpoint)
        if isinstance(endpoint, str) and endpoint else None
    )
    if (not isinstance(endpoint, str) or len(endpoint.encode("utf-8")) > 512 or
            endpoint and endpoint_match is None):
        raise ValueError(
            "resource endpoint must match the dsImaging HTTP(S) endpoint contract"
        )
    if endpoint_match and endpoint_match.group(1) is not None:
        if int(endpoint_match.group(1)) > 65535:
            raise ValueError("resource endpoint port must be between 1 and 65535")
    if (not isinstance(bucket, str) or not 3 <= len(bucket) <= 63 or
            not _CONSUMER_BUCKET_RE.fullmatch(bucket) or
            any(value in bucket for value in ("..", ".-", "-."))):
        raise ValueError("resource bucket must be a canonical S3 bucket name")
    if (not isinstance(region, str) or len(region.encode("utf-8")) > 64 or
            region and not _CONSUMER_REGION_RE.fullmatch(region)):
        raise ValueError(
            "resource region must use letters, digits and dashes"
        )


def _resource_name(value: str, label: str) -> str:
    if (not isinstance(value, str) or len(value) > 128 or
            not _RESOURCE_NAME_RE.fullmatch(value)):
        raise ValueError(
            f"{label} must use 1-128 letters, digits, '.', '_' or '-'"
        )
    return value


def _armadillo_name(value: str, label: str) -> str:
    if (not isinstance(value, str) or len(value) > 128 or
            not _ARMADILLO_NAME_RE.fullmatch(value)):
        raise ValueError(
            f"{label} must use 1-128 letters, digits or underscores"
        )
    return value


def _service_base_url(value: str, label: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValueError(f"{label} must be a canonical HTTP(S) URL")
    try:
        parsed = urlparse(value)
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"{label} must be a canonical HTTP(S) URL") from exc
    if (
        parsed.scheme.lower() not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.params
        or parsed.query
        or parsed.fragment
        or port is not None and not 1 <= port <= 65535
        or any(char in value for char in ("\r", "\n", "\t"))
        or any(part in {".", ".."} for part in parsed.path.split("/"))
    ):
        raise ValueError(
            f"{label} must be HTTP(S) without credentials, query parameters, "
            "fragments or dot segments"
        )
    return value.rstrip("/")


def _r_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=True)
