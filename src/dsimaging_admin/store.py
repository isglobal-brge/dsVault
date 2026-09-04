"""Local dsimaging-store project management."""

from __future__ import annotations

import json
import re
import secrets
import shutil
import subprocess
import threading
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from functools import wraps
from pathlib import Path

from filelock import FileLock

from .controller import ControllerError, health
from .s3 import create_client


DEFAULT_CONTROLLER_IMAGE = (
    "davidsarrat/dsimaging-store:0.3.12@"
    "sha256:9005e717142967904b1b0eeab98de1496c716fadede0c772910f3f5e85cdc74a"
)
DEFAULT_MINIO_IMAGE = (
    "minio/minio:RELEASE.2025-09-07T16-13-09Z@"
    "sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
)
DEFAULT_MC_IMAGE = (
    "minio/mc:RELEASE.2025-08-13T08-35-41Z@"
    "sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727"
)


@dataclass
class StoreConfig:
    path: str
    endpoint: str
    controller_url: str
    controller_token: str
    webhook_token: str
    bucket: str
    access_key: str
    secret_key: str
    minio_port: int
    console_port: int
    controller_port: int

    def to_dict(self) -> dict:
        data = asdict(self)
        configured = bool(self.access_key and self.secret_key)
        data.pop("access_key", None)
        data.pop("secret_key", None)
        data.pop("controller_token", None)
        data.pop("webhook_token", None)
        data["credentials"] = "configured" if configured else "missing"
        return data


_PROJECT_LOCK_STATE = threading.local()


@contextmanager
def store_project_lock(path: str | Path):
    """Serialize lifecycle operations for one canonical store path."""
    root = Path(path).expanduser().resolve()
    root.parent.mkdir(parents=True, exist_ok=True)
    lock_path = str(root.parent / f".{root.name}.dsimaging-store.lock")
    held = getattr(_PROJECT_LOCK_STATE, "paths", set())
    if lock_path in held:
        yield root
        return
    with FileLock(lock_path, mode=0o600):
        held = set(held)
        held.add(lock_path)
        _PROJECT_LOCK_STATE.paths = held
        try:
            yield root
        finally:
            held.remove(lock_path)
            _PROJECT_LOCK_STATE.paths = held


def _with_store_project_lock(function):
    @wraps(function)
    def locked(dest, *args, **kwargs):
        with store_project_lock(dest):
            return function(dest, *args, **kwargs)
    return locked


@_with_store_project_lock
def init_store(
    dest: str,
    *,
    force: bool = False,
    controller_image: str = DEFAULT_CONTROLLER_IMAGE,
    store_source: str | None = None,
    minio_image: str = DEFAULT_MINIO_IMAGE,
    mc_image: str = DEFAULT_MC_IMAGE,
    access_key: str | None = None,
    secret_key: str | None = None,
    bucket: str = "imaging-data",
    minio_port: int = 9000,
    console_port: int = 9001,
    controller_port: int = 8080,
    reconcile_interval: int = 10,
    controller_token: str | None = None,
    webhook_token: str | None = None,
) -> StoreConfig:
    """Create a dsimaging-store Compose project directory."""
    root = Path(dest).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    targets = [root / ".env", root / "docker-compose.yml", root / "init-bucket.sh"]
    existing = [path for path in targets if path.exists()]
    if existing and not force:
        names = ", ".join(path.name for path in existing)
        raise FileExistsError(f"{root} already contains generated store files: {names}")

    previous_env = _read_env(root / ".env") if force else {}
    access_key = (
        access_key or previous_env.get("MINIO_ROOT_USER") or
        f"dsimg{secrets.token_hex(12)}"
    )
    secret_key = (
        secret_key or previous_env.get("MINIO_ROOT_PASSWORD") or
        secrets.token_urlsafe(32)
    )
    controller_block = _controller_compose_block(controller_image, store_source)
    controller_token = (
        controller_token or previous_env.get("DSIMAGING_CONTROLLER_TOKEN") or
        secrets.token_urlsafe(32)
    )
    webhook_token = (
        webhook_token or previous_env.get("DSIMAGING_WEBHOOK_TOKEN") or
        secrets.token_urlsafe(32)
    )
    access_key = _safe_env_value("MinIO access key", access_key, min_length=3)
    secret_key = _safe_env_value("MinIO secret key", secret_key, min_length=8)
    controller_token = _safe_env_value(
        "controller token", controller_token, min_length=16)
    webhook_token = _safe_env_value(
        "webhook token", webhook_token, min_length=16)
    controller_image = _safe_env_value("controller image", controller_image)
    minio_image = _safe_env_value("MinIO image", minio_image)
    mc_image = _safe_env_value("MinIO client image", mc_image)
    if (not isinstance(bucket, str) or
            not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket) or
            ".." in bucket or ".-" in bucket or "-." in bucket):
        raise ValueError("bucket must be a canonical S3 bucket name")
    ports = {
        "MinIO port": minio_port,
        "MinIO console port": console_port,
        "controller port": controller_port,
    }
    for name, value in ports.items():
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
            raise ValueError(f"{name} must be an integer between 1 and 65535")
    if len(set(ports.values())) != len(ports):
        raise ValueError("store service ports must be distinct")
    if (isinstance(reconcile_interval, bool) or
            not isinstance(reconcile_interval, int) or reconcile_interval < 1):
        raise ValueError("reconcile interval must be a positive integer")
    compose = COMPOSE_TEMPLATE.format(
        minio_image=minio_image,
        mc_image=mc_image,
        controller_block=controller_block.rstrip(),
    )
    (root / "docker-compose.yml").write_text(compose, encoding="utf-8")
    env_path = root / ".env"
    env_path.touch(mode=0o600, exist_ok=True)
    env_path.chmod(0o600)
    env_path.write_text(ENV_TEMPLATE.format(
        access_key=access_key,
        secret_key=secret_key,
        minio_port=minio_port,
        console_port=console_port,
        controller_port=controller_port,
        bucket=bucket,
        reconcile_interval=reconcile_interval,
        controller_token=controller_token,
        webhook_token=webhook_token,
        controller_image=controller_image,
        minio_image=minio_image,
        mc_image=mc_image,
    ), encoding="utf-8")
    init_script = root / "init-bucket.sh"
    init_script.write_text(INIT_BUCKET_SCRIPT, encoding="utf-8")
    init_script.chmod(0o755)
    return load_store_config(str(root))


def load_store_config(path: str) -> StoreConfig:
    root = Path(path).expanduser().resolve()
    required_files = (".env", "docker-compose.yml", "init-bucket.sh")
    file_problems = []
    if not root.is_dir():
        file_problems.append("project path is not a directory")
    for name in required_files:
        artifact = root / name
        if artifact.is_symlink():
            file_problems.append(f"{name} is a symbolic link")
        elif not artifact.exists():
            file_problems.append(f"{name} is missing")
        elif not artifact.is_file():
            file_problems.append(f"{name} is not a regular file")
    if file_problems:
        raise ValueError(
            f"{root} is not a dsimaging-store project: a complete generated "
            "project requires docker-compose.yml/.env and init-bucket.sh; "
            + "; ".join(file_problems)
        )

    env = _read_env(root / ".env")
    required_values = (
        "MINIO_ROOT_USER",
        "MINIO_ROOT_PASSWORD",
        "MINIO_PORT",
        "MINIO_CONSOLE_PORT",
        "CONTROLLER_PORT",
        "BUCKET_NAME",
        "DSIMAGING_CONTROLLER_TOKEN",
        "DSIMAGING_WEBHOOK_TOKEN",
    )
    missing = [key for key in required_values if not env.get(key)]
    if missing:
        raise ValueError(
            f"{root / '.env'} is missing required generated value(s): "
            f"{', '.join(missing)}; review the existing settings and explicitly "
            "migrate the project with 'dsimaging-admin store init <path> --force'"
        )

    ports = {}
    for key in ("MINIO_PORT", "MINIO_CONSOLE_PORT", "CONTROLLER_PORT"):
        try:
            value = int(env[key])
        except ValueError as exc:
            raise ValueError(f"{key} must be an integer between 1 and 65535") from exc
        if not 1 <= value <= 65535:
            raise ValueError(f"{key} must be an integer between 1 and 65535")
        ports[key] = value
    if len(set(ports.values())) != len(ports):
        raise ValueError("store service ports must be distinct")

    _safe_env_value("MinIO access key", env["MINIO_ROOT_USER"], min_length=3)
    _safe_env_value("MinIO secret key", env["MINIO_ROOT_PASSWORD"], min_length=8)
    _safe_env_value("controller token", env["DSIMAGING_CONTROLLER_TOKEN"], min_length=16)
    _safe_env_value("webhook token", env["DSIMAGING_WEBHOOK_TOKEN"], min_length=16)
    bucket = env["BUCKET_NAME"]
    if (not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket) or
            ".." in bucket or ".-" in bucket or "-." in bucket):
        raise ValueError("BUCKET_NAME must be a canonical S3 bucket name")

    minio_port = ports["MINIO_PORT"]
    controller_port = ports["CONTROLLER_PORT"]
    return StoreConfig(
        path=str(root),
        endpoint=f"http://127.0.0.1:{minio_port}",
        controller_url=f"http://127.0.0.1:{controller_port}",
        controller_token=env["DSIMAGING_CONTROLLER_TOKEN"],
        webhook_token=env["DSIMAGING_WEBHOOK_TOKEN"],
        bucket=bucket,
        access_key=env["MINIO_ROOT_USER"],
        secret_key=env["MINIO_ROOT_PASSWORD"],
        minio_port=minio_port,
        console_port=ports["MINIO_CONSOLE_PORT"],
        controller_port=controller_port,
    )


@_with_store_project_lock
def compose_up(path: str, build: bool = True) -> str:
    cmd = _compose_cmd(path) + ["up", "-d"]
    if build:
        cmd.append("--build")
    return _run(cmd, cwd=path)


@_with_store_project_lock
def compose_down(path: str, volumes: bool = False) -> str:
    cmd = _compose_cmd(path) + ["down"]
    if volumes:
        cmd.append("-v")
    return _run(cmd, cwd=path)


def compose_ps(path: str) -> str:
    return _run(_compose_cmd(path) + ["ps"], cwd=path)


def compose_ps_json(path: str) -> str:
    return _run(_compose_cmd(path) + ["ps", "--format", "json"], cwd=path)


def compose_logs(path: str, service: str | None = None, tail: int = 100) -> str:
    cmd = _compose_cmd(path) + ["logs", "--tail", str(tail)]
    if service:
        cmd.append(service)
    return _run(cmd, cwd=path)


def store_doctor(path: str) -> dict:
    cfg = load_store_config(path)
    result = {"store": cfg.to_dict(), "docker": {}, "controller": {}, "s3": {}}
    try:
        result["docker"]["ps"] = compose_ps(path)
        result["docker"]["ok"] = True
    except Exception as e:
        result["docker"]["ok"] = False
        result["docker"]["error"] = str(e)

    try:
        result["controller"] = health(cfg.controller_url)
        result["controller"]["ok"] = True
    except (ControllerError, Exception) as e:
        result["controller"] = {"ok": False, "error": str(e)}

    s3_result = {"ok": False, "bucket": cfg.bucket}
    try:
        s3 = create_client(cfg.endpoint, cfg.access_key, cfg.secret_key, "")
        s3.head_bucket(Bucket=cfg.bucket)
    except Exception as e:
        s3_result["error"] = str(e)
    else:
        failures = []
        try:
            status = s3.get_bucket_versioning(Bucket=cfg.bucket).get(
                "Status", "Disabled")
            versioning_ok = status == "Enabled"
            s3_result["versioning"] = {"ok": versioning_ok, "status": status}
            if not versioning_ok:
                failures.append(f"bucket versioning is {status}")
        except Exception as e:
            s3_result["versioning"] = {
                "ok": False, "status": "unknown", "error": str(e),
            }
            failures.append(f"bucket versioning check failed: {e}")

        try:
            notification = s3.get_bucket_notification_configuration(
                Bucket=cfg.bucket)
            notification_ok = _has_generated_webhook_notification(notification)
            s3_result["notification"] = {
                "ok": notification_ok,
                "target": "arn:minio:sqs::DSIMAGING:webhook",
                "prefix": "datasets/",
            }
            if not notification_ok:
                failures.append("generated webhook notification is missing or invalid")
        except Exception as e:
            s3_result["notification"] = {"ok": False, "error": str(e)}
            failures.append(f"webhook notification check failed: {e}")

        try:
            s3.head_object(Bucket=cfg.bucket, Key="datasets/.keep")
            s3_result["init_marker"] = {
                "ok": True, "key": "datasets/.keep",
            }
        except Exception as e:
            s3_result["init_marker"] = {
                "ok": False, "key": "datasets/.keep", "error": str(e),
            }
            failures.append("init marker datasets/.keep is missing or inaccessible")

        s3_result["ok"] = not failures
        if failures:
            s3_result["error"] = "; ".join(failures)
    result["s3"] = s3_result
    result["ok"] = all(
        result[name].get("ok") for name in ("docker", "controller", "s3")
    )
    return result


def _has_generated_webhook_notification(config: dict) -> bool:
    for item in config.get("QueueConfigurations", []):
        if item.get("QueueArn") != "arn:minio:sqs::DSIMAGING:webhook":
            continue
        events = [event for event in item.get("Events", [])
                  if isinstance(event, str)]
        if not (any(event.startswith("s3:ObjectCreated:") for event in events)
                and any(event.startswith("s3:ObjectRemoved:") for event in events)):
            continue
        rules = item.get("Filter", {}).get("Key", {}).get("FilterRules", [])
        if any(
            str(rule.get("Name", "")).lower() == "prefix"
            and rule.get("Value") == "datasets/"
            for rule in rules
            if isinstance(rule, dict)
        ):
            return True
    return False


def _compose_cmd(path: str) -> list[str]:
    if not shutil.which("docker"):
        raise RuntimeError("docker command not found")
    root = Path(path).expanduser().resolve()
    return ["docker", "compose", "--project-directory", str(root)]


def check_compose_prerequisites() -> None:
    """Fail before project generation when Docker Compose is unavailable."""
    if not shutil.which("docker"):
        raise RuntimeError("docker command not found")
    proc = subprocess.run(
        ["docker", "compose", "version"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            proc.stdout.strip() or "docker compose is not available")


def _run(cmd: list[str], cwd: str) -> str:
    proc = subprocess.run(
        cmd,
        cwd=str(Path(cwd).expanduser().resolve()),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stdout.strip() or f"command failed: {' '.join(cmd)}")
    return proc.stdout.rstrip()


def _controller_compose_block(controller_image: str, store_source: str | None) -> str:
    if store_source:
        controller_dir = Path(store_source).expanduser().resolve() / "controller"
        if not controller_dir.exists():
            raise FileNotFoundError(f"controller build directory not found: {controller_dir}")
        return f"""\
    build:
      context: {json.dumps(str(controller_dir))}
"""
    return """\
    image: ${DSIMAGING_STORE_CONTROLLER_IMAGE}
"""


def _safe_env_value(name: str, value: str, *, min_length: int = 1) -> str:
    """Validate an unquoted Compose dotenv value before writing it."""
    if (not isinstance(value, str) or len(value) < min_length or
            len(value.encode("utf-8")) > 2048 or value.strip() != value or
            not re.fullmatch(r"[A-Za-z0-9._~:/+@=-]+", value)):
        raise ValueError(f"{name} contains characters unsafe for a Compose .env file")
    return value


def _read_env(path: Path) -> dict[str, str]:
    values = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


COMPOSE_TEMPLATE = """\
services:
  minio:
    image: ${{DSIMAGING_STORE_MINIO_IMAGE:-{minio_image}}}
    command: server /data --console-address ":9001"
    ports:
      - "127.0.0.1:${{MINIO_PORT:-9000}}:9000"
      - "127.0.0.1:${{MINIO_CONSOLE_PORT:-9001}}:9001"
    environment:
      MINIO_ROOT_USER: ${{MINIO_ROOT_USER:?MINIO_ROOT_USER is required}}
      MINIO_ROOT_PASSWORD: ${{MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}}
      MINIO_NOTIFY_WEBHOOK_ENABLE_DSIMAGING: "on"
      MINIO_NOTIFY_WEBHOOK_ENDPOINT_DSIMAGING: "http://controller:8080/webhook/minio"
      MINIO_NOTIFY_WEBHOOK_AUTH_TOKEN_DSIMAGING: "Bearer ${{DSIMAGING_WEBHOOK_TOKEN:?DSIMAGING_WEBHOOK_TOKEN is required}}"
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 10s
      timeout: 5s
      retries: 3
    restart: unless-stopped

  controller:
{controller_block}
    ports:
      - "127.0.0.1:${{CONTROLLER_PORT:-8080}}:8080"
    environment:
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ROOT_USER: ${{MINIO_ROOT_USER:?MINIO_ROOT_USER is required}}
      MINIO_ROOT_PASSWORD: ${{MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}}
      BUCKET_NAME: ${{BUCKET_NAME:-imaging-data}}
      RECONCILE_INTERVAL_SECONDS: ${{RECONCILE_INTERVAL_SECONDS:-10}}
      DSIMAGING_CONTROLLER_TOKEN: ${{DSIMAGING_CONTROLLER_TOKEN:-}}
      DSIMAGING_WEBHOOK_TOKEN: ${{DSIMAGING_WEBHOOK_TOKEN:?DSIMAGING_WEBHOOK_TOKEN is required}}
      DSIMAGING_MAX_WEBHOOK_BODY_BYTES: ${{DSIMAGING_MAX_WEBHOOK_BODY_BYTES:-1048576}}
    depends_on:
      minio:
        condition: service_healthy
    restart: unless-stopped

  init:
    image: ${{DSIMAGING_STORE_MC_IMAGE:-{mc_image}}}
    entrypoint: ["/bin/sh", "/init-bucket.sh"]
    volumes:
      - ./init-bucket.sh:/init-bucket.sh:ro
    environment:
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ROOT_USER: ${{MINIO_ROOT_USER:?MINIO_ROOT_USER is required}}
      MINIO_ROOT_PASSWORD: ${{MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}}
      BUCKET_NAME: ${{BUCKET_NAME:-imaging-data}}
      WEBHOOK_RETRIES: ${{WEBHOOK_RETRIES:-12}}
      WEBHOOK_RETRY_SECONDS: ${{WEBHOOK_RETRY_SECONDS:-5}}
    depends_on:
      minio:
        condition: service_healthy

volumes:
  minio-data:
"""


ENV_TEMPLATE = """\
# dsimaging-store generated by dsimaging-admin
MINIO_ROOT_USER={access_key}
MINIO_ROOT_PASSWORD={secret_key}
MINIO_PORT={minio_port}
MINIO_CONSOLE_PORT={console_port}
CONTROLLER_PORT={controller_port}
BUCKET_NAME={bucket}
RECONCILE_INTERVAL_SECONDS={reconcile_interval}
DSIMAGING_MAX_WEBHOOK_BODY_BYTES=1048576
DSIMAGING_CONTROLLER_TOKEN={controller_token}
DSIMAGING_WEBHOOK_TOKEN={webhook_token}
WEBHOOK_RETRIES=12
WEBHOOK_RETRY_SECONDS=5
DSIMAGING_STORE_CONTROLLER_IMAGE={controller_image}
DSIMAGING_STORE_MINIO_IMAGE={minio_image}
DSIMAGING_STORE_MC_IMAGE={mc_image}
"""


INIT_BUCKET_SCRIPT = """\
#!/bin/sh
set -e

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:?MINIO_ROOT_USER is required}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}"
BUCKET_NAME="${BUCKET_NAME:-imaging-data}"
WEBHOOK_RETRIES="${WEBHOOK_RETRIES:-12}"
WEBHOOK_RETRY_SECONDS="${WEBHOOK_RETRY_SECONDS:-5}"

printf '{"url":"%s","accessKey":"%s","secretKey":"%s","api":"s3v4","path":"auto"}\n' \
  "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" \
  | mc alias import local/ >/dev/null

if mc ls "local/${BUCKET_NAME}" >/dev/null 2>&1; then
  echo "[init] Bucket '${BUCKET_NAME}' already exists"
else
  mc mb "local/${BUCKET_NAME}"
  echo "[init] Bucket '${BUCKET_NAME}' created"
fi

mc version enable "local/${BUCKET_NAME}"
mkdir -p /tmp/dsimaging-init
: > /tmp/dsimaging-init/.keep
mc cp /tmp/dsimaging-init/.keep "local/${BUCKET_NAME}/datasets/.keep" 2>/dev/null || true
configure_webhook() {
  attempt=1
  while [ "${attempt}" -le "${WEBHOOK_RETRIES}" ]; do
    # `mc event add` is not idempotent for an overlapping filter. Remove only
    # this controller target first; other bucket notification targets remain.
    mc event remove "local/${BUCKET_NAME}" arn:minio:sqs::DSIMAGING:webhook \
      --event put,delete --prefix "datasets/" \
      >/dev/null 2>&1 || true
    if output=$(mc event add "local/${BUCKET_NAME}" arn:minio:sqs::DSIMAGING:webhook \
      --event put,delete \
      --prefix "datasets/" 2>&1); then
      echo "[init] Webhook notification configured"
      return 0
    fi

    echo "[init] Webhook notification attempt ${attempt}/${WEBHOOK_RETRIES} failed: ${output}"
    attempt=$((attempt + 1))
    if [ "${attempt}" -le "${WEBHOOK_RETRIES}" ]; then
      sleep "${WEBHOOK_RETRY_SECONDS}"
    fi
  done

  echo "[init] ERROR: Unable to configure webhook notification after ${WEBHOOK_RETRIES} attempts" >&2
  return 1
}

configure_webhook

echo "[init] MinIO initialization complete"
"""
