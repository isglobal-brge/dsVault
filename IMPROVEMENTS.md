# IMPROVEMENTS

Validated status after the `dsimaging-admin` / `dsimaging-store` work for
store-centric administration.

## Product boundary

`dsimaging-admin` manages `dsimaging-store` deployments and the datasets inside
those stores. It must stay agnostic about clients that may consume those
datasets later.

That means Opal registration is not part of the public admin surface. Earlier
notes in this file suggested an `opal` module and `--opal-*` publish flags; that
was the wrong boundary for this project and has been intentionally removed.

## Implemented

### Store management

- `store init` generates a ready-to-run `dsimaging-store` directory with
  Docker Compose, `.env`, MinIO init script and controller settings.
- `store up`, `store down`, `store ps`, `store logs`, `store config` and
  `store doctor` operate that generated store from the admin CLI.
- The default controller, MinIO and MinIO client images are pinned by release
  tag and multi-architecture manifest digest, with local controller-build
  support for development.

### Dataset administration

- `delete <dataset-id> --yes [--purge-versions]` removes current dataset
  objects, with optional version purging for versioned buckets.
- `verify <dataset-id> [--quick] [--sample-fraction ...] [--output text|json]`
  checks S3 objects against `content_hash_index.parquet` and mask indexes.
- `status <dataset-id> [--output text|json]` reports object counts, manifest
  state, hash-index rows and controller state when reachable.
- `download <dataset-id> <dest>` pulls a dataset locally for inspection.
- `copy <src-id> <dst-id> --yes` performs server-side S3 copies for renames or
  forks.
- `list --output text|json` now ignores deleted prefixes that only remain as
  version history and can merge controller state.
- `reconcile <dataset-id>` asks the store controller to rebuild artifacts for a
  dataset.

### Publish robustness

- `publish` is atomic by default: uploads go to a staging prefix, then are
  copied into the canonical dataset prefix after the upload completes.
- A `.publish-lock` marker prevents the controller from reconciling a dataset
  while the CLI is still publishing it.
- `--no-atomic`, `--no-skip`, `--dry-run` and `--skip-dicom-checks` cover the
  operational escape hatches.
- Pre-publish DICOM checks validate consistent series UID, modality and
  instance ordering when `pydicom` can parse the series.

### Store observability

- `doctor --output text|json` checks S3 connectivity, bucket existence,
  versioning, init marker, controller health, webhook prefix/source prefixes and
  controller/S3 dataset parity.
- `--controller-url` and `--skip-controller` are global options.
- Multi-profile config is supported through `profiles.<name>` in
  `~/.dsimaging.yaml` and `--profile <name>`.

### Shared scan/hash/manifest core

The controller now imports the scan, hash-index and manifest builders from
`dsimaging_admin.manifest` instead of carrying a divergent copy. This keeps
CLI-side publish/rescan and controller-side reconcile on the same rules for
images, masks, metadata and manifests.

## Explicitly not implemented

- Opal commands and `--opal-*` flags, by product decision.
- A web UI; the CLI plus MinIO console remains the operating surface.
- Image-level processing such as DICOM de-identification, resampling or format
  conversion. Those belong in imaging/radiomics pipelines, not store admin.

## Remaining follow-ups

- Add CI release automation with Trusted Publishing so future PyPI releases do
  not require a manual API token.
- Decide whether a successful or failed `verify` should write an audit artifact
  such as `indexes/last_verify.json`.
- Add richer structured runtime logging if operators need machine-readable log
  streams beyond the existing `--output json` command outputs.
