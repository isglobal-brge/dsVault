# NEWS

## 0.3.10

- S3 publication and reconciliation now hash the exact immutable object
  versions recorded in collection indexes and derive each listing from one
  coherent HEAD snapshot.
- Quick verification trusts a stored content hash only when it is explicitly
  attested to the same non-null S3 version; legacy indexes are fully rehashed.
- JPEG files using either `.jpg` or `.jpeg` are recognized consistently.

## 0.3.9

- Collection publication carries an explicit public label vocabulary through
  copy, reconciliation and verification, while rejecting vocabularies that
  expose sample or patient identifiers.
- Quick verification now uses immutable non-null S3 object versions and falls
  back to full verification whenever that guarantee is unavailable.
- Store configuration writes are atomic and private, the UI applies the same
  DICOM checks as the CLI, and generated stores use a dedicated authenticated
  webhook credential.
- Local store creation no longer inherits credentials from a connection
  profile; fresh unique credentials are generated unless explicit overrides
  are supplied to `store init`.

## 0.3.8

- The declared minimum Python version is now 3.10, matching the requirement of
  the supported `pydicom>=3.0.0` dependency.
- Generated stores pin the controller, MinIO and MinIO client images by release
  tag and multi-architecture manifest digest instead of following `latest`.
- Store manifests and index rows are confined to one canonical collection
  prefix; verification cross-checks hashes, sizes, source kinds, file rosters
  and metadata instead of accepting internally inconsistent artifacts.
- Local scans reject traversal-like paths and symbolic links so publishing a
  collection cannot silently import files from another filesystem tree;
  dataset downloads also remain confined to their requested destination.
- New local stores use unique generated MinIO credentials, bind API and console
  ports to loopback, and require explicit credentials instead of shared
  defaults. Forced regeneration preserves existing generated credentials.
- Repeated MinIO initialization replaces only the controller's matching event
  rule before adding it, avoiding overlapping notification filters without
  relying on tools absent from the `minio/mc` image.

## 0.3.7

- Dataset manifests now require and preserve the patient-level privacy-unit
  contract used by dsImaging admission.
- Sample IDs are canonical and unique, patient IDs are stored using the shared
  `trim-utf8-v2` representation, and mask-to-sample mappings reject duplicates
  and orphans.
- Atomic publication uses an S3 conditional lock, stages replacements, writes
  the manifest last, and restores the previous dataset on failure.
- Publication locks carry a unique owner and are released conditionally; copy
  locks its source snapshot, and metadata must match the image roster exactly.
- Verification now checks the complete image, metadata and mask contract and
  fails closed for corrupt or incomplete publication artifacts.
- Generated stores keep a random controller operator token in a mode-`0600`
  `.env`; controller inventory and manual reconciliation clients send it as a
  bearer token while health remains a coarse liveness check.

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
