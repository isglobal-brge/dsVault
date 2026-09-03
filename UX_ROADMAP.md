# dsimaging-admin UX roadmap

This document defines the proposed operator experience for the next feature
release. It is a plan, not the current command contract. Existing commands and
flags remain supported until each proposal below is implemented and tested.

## Goals

- A normal local setup should need one setup command and one publish command.
- Safe defaults must not hide which store, profile, dataset, or privacy unit is
  being used.
- Local, AWS, and existing S3-compatible stores should share lifecycle and
  verification behavior without pretending they are provisioned identically.
- Credentials stay in the generated private `.env`, the AWS credential chain,
  or another server-side credential provider. Human and JSON output never
  includes credential values.
- Advanced controls remain available through explicit flags and profiles,
  outside the common path.

## Proposed command surface

```text
dsimaging-admin
├── profile  add | list | show | use
├── store    setup | init | provision | up | down | ps | logs | doctor | config
├── dataset  publish | list | status | verify | modify | copy
│            download | rescan | reconcile | delete | purge
├── doctor
└── ui
```

`dataset` remains the canonical term because changing it to `collection` would
add migration cost without improving the operator model. Deprecated top-level
dataset aliases remain callable with a warning but should be hidden from the
main help page.

## Common local path

The intended fresh-store flow is:

```bash
dsimaging-admin store setup ./study-store
dsimaging-admin dataset publish study_ct_v1 /data/study_ct --modality ct
dsimaging-admin dataset status study_ct_v1
```

`store setup` is an additive wrapper around existing operations. It should:

1. validate the Docker/Compose prerequisites;
2. create a new store project, or recognize an already valid one;
3. start it and wait for MinIO, bucket initialization, and controller health;
4. verify bucket versioning and run the complete doctor;
5. save a default local-store profile without copying secrets.

The profile stores only a pointer to the private project:

```yaml
default_profile: study-store
profiles:
  study-store:
    kind: local-store
    store_path: /absolute/path/to/study-store
```

Connection values are resolved from its mode-`0600` `.env` at execution time.
The precedence is:

```text
explicit CLI flag > DSIMAGING_* environment > explicit profile field
> local store_path > built-in default
```

With a local-store profile active, `store up`, `down`, `doctor`, and `logs` do
not require a path. An explicit path still wins. Setup is idempotent for a
valid generated project and never enables `--force` implicitly.

## Publication and verification

The shorter positional publish form above should retain every existing
advanced option. It may infer only unambiguous, non-sensitive facts:

- use metadata automatically only when exactly one `metadata.csv` or
  `metadata.parquet` exists;
- use `patient_id` only when that exact column exists, otherwise require
  `--privacy-unit-column`;
- never infer a label column or public label levels;
- show a complete preflight plan before the first write;
- run quick verification as part of the atomic transaction by default.

The proposed control is `--verify full|quick|none`, defaulting to `quick`.
`none` must always be explicit. A failed verification rolls back before the
publish lock is released and must not leave a visible partial dataset.

Deletion and irreversible purging become visibly separate operations:

```bash
dsimaging-admin dataset delete study_ct_v1  # preserves version history
dsimaging-admin dataset purge study_ct_v1   # removes all versions/markers
```

The existing `delete --purge-versions` flag remains temporarily as a warned
compatibility alias.

## Advanced stores

AWS provisioning should not require an unused local path:

```bash
dsimaging-admin profile add aws-prod \
  --backend aws --region eu-west-1 --bucket my-imaging
dsimaging-admin --profile aws-prod store provision --kms-key <KMS-ARN>
```

An existing S3-compatible endpoint is connect-only:

```bash
dsimaging-admin profile add lab-s3 \
  --backend s3-compatible --endpoint https://s3.example.org --bucket imaging
dsimaging-admin --profile lab-s3 doctor
```

The AWS operation uses the existing complete bucket/versioning/encryption/SQS
provisioner. Custom endpoints are never mistaken for a request to create a
local Compose project.

## Operator UI

The seven current pages should become four task-focused views:

- **Home:** active profile, health, start/stop, and logs.
- **Datasets:** inventory, detail, quick/full verify, and a collapsed danger
  zone.
- **Publish:** `Source -> Preflight -> Confirm -> Verify` wizard.
- **Advanced:** connection, AWS/custom endpoint, and controller details.

For a local-store profile, the UI process resolves `.env` credentials
server-side. Secret widgets stay blank and credentials never enter rendered
output. Publication remains disabled until the current inputs have a successful
preflight; changing an input invalidates that preflight.

## Output contract

Human output should show numbered stages, one final state, and the exact next
action. JSON mode emits one parseable document to stdout; diagnostics go to
stderr. Exit codes remain truthful. Tracebacks require an explicit debug mode.
URLs and structured error fields must redact credentials, authorization
headers, userinfo, and query strings.

Example:

```text
[1/5] Docker ............ OK
[2/5] MinIO ............. OK
[3/5] Bucket/versioning . OK
[4/5] Controller ........ OK
[5/5] Profile ........... study-store (default)

Store ready. Credentials: configured (not shown)
Next: dsimaging-admin dataset publish ...
```

## Acceptance criteria

1. Fresh local setup completes end to end with one command and stores only a
   `kind`/`store_path` profile.
2. A second setup is non-destructive and leaves the `.env` bytes and mode
   unchanged; a partial foreign directory fails without overwriting it.
3. Profile precedence and pathless local lifecycle commands follow the stated
   order, with explicit paths taking priority.
4. Publish inference succeeds only for one metadata file and an exact
   `patient_id`; ambiguous inputs fail before any S3 write.
5. Quick verification occurs before commit; drift rolls back and returns a
   non-zero status.
6. Existing datasets require explicit replacement before any write.
7. Delete preserves versions, purge removes them, and both require distinct
   confirmations.
8. The local UI connects through `store_path` without rendering or serializing
   credential values.
9. Changing publish inputs invalidates the UI preview; quick/full verification
   uses the same implementation as the CLI.
10. AWS provisioning is path-free and complete; custom S3 is connect-only.
11. Legacy commands remain callable with warnings but are absent from primary
    help.
12. Every JSON response is a single redacted document with a truthful process
    status.
