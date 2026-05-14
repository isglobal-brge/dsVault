"""Local dsimaging-store project management."""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass, asdict
from pathlib import Path

from .controller import ControllerError, health
from .s3 import create_client


DEFAULT_CONTROLLER_IMAGE = "davidsarratgonzalez/dsimaging-store:latest"
DEFAULT_MINIO_IMAGE = "minio/minio:latest"
DEFAULT_MC_IMAGE = "minio/mc:latest"


@dataclass
class StoreConfig:
    path: str
    endpoint: str
    controller_url: str
    bucket: str
    access_key: str
    secret_key: str
    minio_port: int
    console_port: int
    controller_port: int

    def to_dict(self) -> dict:
        data = asdict(self)
        data.pop("secret_key", None)
        return data


def init_store(
    dest: str,
    *,
    force: bool = False,
    controller_image: str = DEFAULT_CONTROLLER_IMAGE,
    store_source: str | None = None,
    minio_image: str = DEFAULT_MINIO_IMAGE,
    mc_image: str = DEFAULT_MC_IMAGE,
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin123",
    bucket: str = "imaging-data",
    minio_port: int = 9000,
    console_port: int = 9001,
    controller_port: int = 8080,
    reconcile_interval: int = 10,
) -> StoreConfig:
    """Create a dsimaging-store Compose project directory."""
    root = Path(dest).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    targets = [root / ".env", root / "docker-compose.yml", root / "init-bucket.sh"]
    existing = [path for path in targets if path.exists()]
    if existing and not force:
        names = ", ".join(path.name for path in existing)
        raise FileExistsError(f"{root} already contains generated store files: {names}")

    controller_block = _controller_compose_block(controller_image, store_source)
    compose = COMPOSE_TEMPLATE.format(
        minio_image=minio_image,
        mc_image=mc_image,
        controller_block=controller_block.rstrip(),
    )
    (root / "docker-compose.yml").write_text(compose, encoding="utf-8")
    (root / ".env").write_text(ENV_TEMPLATE.format(
        access_key=access_key,
        secret_key=secret_key,
        minio_port=minio_port,
        console_port=console_port,
        controller_port=controller_port,
        bucket=bucket,
        reconcile_interval=reconcile_interval,
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
    env = _read_env(root / ".env")
    minio_port = int(env.get("MINIO_PORT", "9000"))
    controller_port = int(env.get("CONTROLLER_PORT", "8080"))
    return StoreConfig(
        path=str(root),
        endpoint=f"http://127.0.0.1:{minio_port}",
        controller_url=f"http://127.0.0.1:{controller_port}",
        bucket=env.get("BUCKET_NAME", "imaging-data"),
        access_key=env.get("MINIO_ROOT_USER", "minioadmin"),
        secret_key=env.get("MINIO_ROOT_PASSWORD", "minioadmin123"),
        minio_port=minio_port,
        console_port=int(env.get("MINIO_CONSOLE_PORT", "9001")),
        controller_port=controller_port,
    )


def compose_up(path: str, build: bool = True) -> str:
    cmd = _compose_cmd(path) + ["up", "-d"]
    if build:
        cmd.append("--build")
    return _run(cmd, cwd=path)


def compose_down(path: str, volumes: bool = False) -> str:
    cmd = _compose_cmd(path) + ["down"]
    if volumes:
        cmd.append("-v")
    return _run(cmd, cwd=path)


def compose_ps(path: str) -> str:
    return _run(_compose_cmd(path) + ["ps"], cwd=path)


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

    try:
        s3 = create_client(cfg.endpoint, cfg.access_key, cfg.secret_key, "")
        s3.head_bucket(Bucket=cfg.bucket)
        result["s3"] = {"ok": True, "bucket": cfg.bucket}
    except Exception as e:
        result["s3"] = {"ok": False, "bucket": cfg.bucket, "error": str(e)}
    result["ok"] = all(
        result[name].get("ok") for name in ("docker", "controller", "s3")
    )
    return result


def _compose_cmd(path: str) -> list[str]:
    if not shutil.which("docker"):
        raise RuntimeError("docker command not found")
    root = Path(path).expanduser().resolve()
    return ["docker", "compose", "--project-directory", str(root)]


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
      context: {controller_dir}
"""
    return """\
    image: ${DSIMAGING_STORE_CONTROLLER_IMAGE}
"""


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
      - "${{MINIO_PORT:-9000}}:9000"
      - "${{MINIO_CONSOLE_PORT:-9001}}:9001"
    environment:
      MINIO_ROOT_USER: ${{MINIO_ROOT_USER:-minioadmin}}
      MINIO_ROOT_PASSWORD: ${{MINIO_ROOT_PASSWORD:-minioadmin123}}
      MINIO_NOTIFY_WEBHOOK_ENABLE_DSIMAGING: "on"
      MINIO_NOTIFY_WEBHOOK_ENDPOINT_DSIMAGING: "http://controller:8080/webhook/minio"
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
      - "${{CONTROLLER_PORT:-8080}}:8080"
    environment:
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ROOT_USER: ${{MINIO_ROOT_USER:-minioadmin}}
      MINIO_ROOT_PASSWORD: ${{MINIO_ROOT_PASSWORD:-minioadmin123}}
      BUCKET_NAME: ${{BUCKET_NAME:-imaging-data}}
      RECONCILE_INTERVAL_SECONDS: ${{RECONCILE_INTERVAL_SECONDS:-10}}
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
      MINIO_ROOT_USER: ${{MINIO_ROOT_USER:-minioadmin}}
      MINIO_ROOT_PASSWORD: ${{MINIO_ROOT_PASSWORD:-minioadmin123}}
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
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin123}"
BUCKET_NAME="${BUCKET_NAME:-imaging-data}"
WEBHOOK_RETRIES="${WEBHOOK_RETRIES:-12}"
WEBHOOK_RETRY_SECONDS="${WEBHOOK_RETRY_SECONDS:-5}"

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" --api s3v4

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
    if output=$(mc event add "local/${BUCKET_NAME}" arn:minio:sqs::DSIMAGING:webhook \
      --event put,delete \
      --prefix "datasets/" 2>&1); then
      echo "[init] Webhook notification configured"
      return 0
    fi

    if echo "${output}" | grep -qi "already"; then
      echo "[init] Webhook notification already configured"
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
