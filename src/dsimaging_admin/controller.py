"""HTTP helpers for dsimaging-store controllers."""

from __future__ import annotations

from urllib.parse import quote

import requests


class ControllerError(RuntimeError):
    """Raised when a dsimaging-store controller request fails."""


def normalise_controller_url(url: str | None) -> str | None:
    if not url:
        return None
    return url.rstrip("/")


def get_json(controller_url: str, path: str, timeout: float = 5.0) -> dict:
    """GET a JSON controller endpoint."""
    base = normalise_controller_url(controller_url)
    if not base:
        raise ControllerError("controller URL is empty")
    response = requests.get(f"{base}/{path.lstrip('/')}", timeout=timeout)
    if response.status_code >= 400:
        raise ControllerError(
            f"controller GET {path} failed with {response.status_code}: "
            f"{response.text[:300]}"
        )
    try:
        payload = response.json()
    except ValueError as e:
        raise ControllerError(f"controller GET {path} did not return JSON") from e
    if not isinstance(payload, dict):
        raise ControllerError(f"controller GET {path} returned non-object JSON")
    return payload


def post_json(controller_url: str, path: str, timeout: float = 30.0) -> dict:
    """POST to a JSON controller endpoint."""
    base = normalise_controller_url(controller_url)
    if not base:
        raise ControllerError("controller URL is empty")
    response = requests.post(f"{base}/{path.lstrip('/')}", timeout=timeout)
    if response.status_code >= 400:
        raise ControllerError(
            f"controller POST {path} failed with {response.status_code}: "
            f"{response.text[:300]}"
        )
    try:
        payload = response.json()
    except ValueError as e:
        raise ControllerError(f"controller POST {path} did not return JSON") from e
    if not isinstance(payload, dict):
        raise ControllerError(f"controller POST {path} returned non-object JSON")
    return payload


def health(controller_url: str, timeout: float = 5.0) -> dict:
    return get_json(controller_url, "/health", timeout=timeout)


def datasets(controller_url: str, timeout: float = 5.0) -> list[dict]:
    payload = get_json(controller_url, "/datasets", timeout=timeout)
    values = payload.get("datasets", [])
    if not isinstance(values, list):
        raise ControllerError("controller /datasets payload has no datasets list")
    return values


def reconcile(controller_url: str, dataset_id: str, timeout: float = 30.0) -> dict:
    safe_id = quote(dataset_id, safe="")
    return post_json(controller_url, f"/reconcile/{safe_id}", timeout=timeout)
