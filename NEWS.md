# NEWS

## 0.3.6

- Test isolation: added a hermetic `tests/conftest.py` that redirects `HOME`
  (and `USERPROFILE`) to a fresh temporary directory and patches
  `dsimaging_admin.cli.CONFIG_PATH`, so the suite never reads the invoking
  user's real `~/.dsimaging.yaml`.
- Config precedence: an explicit `--backend aws` (flag, `DSIMAGING_BACKEND`
  env, or profile backend key) is no longer silently overridden by a stored
  profile endpoint; with the AWS backend the endpoint is taken only from
  `--endpoint` or `DSIMAGING_ENDPOINT`. Store backends (auto/minio/
  s3-compatible) keep the previous resolution order.
- Store scaffolding: `store init` now requires an explicit destination
  directory instead of defaulting to the current directory; `store up`
  validates that the target directory contains docker-compose.yml and .env
  before invoking Docker Compose; and the scaffolded `.env` `MINIO_PORT`
  is derived from the global `--endpoint` port for local endpoints when
  `--minio-port` is not passed explicitly (explicit `--minio-port` wins).
