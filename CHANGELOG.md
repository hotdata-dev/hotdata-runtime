# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- fix(load): retry an `append` load instead of running it at most once.

  `append` was excluded from retries on the grounds that it is not idempotent:
  if the server commits but the response is lost, a retry would duplicate rows.
  That is not how the server behaves. It keys a receipt on `upload_id`, and a
  re-POST of the same id replays the committed result instead of applying the
  load again — so what makes a retry safe is re-sending the same upload, not
  the mode. This client stages once, in `upload_parquet`, outside the retried
  operation, so the invariant holds for every mode.

  The exclusion cost real availability. The destination serialises writes per
  table and refuses rather than queues, so concurrent writers to one table get
  `409 RESOURCE_LOCKED` — and an append had no budget to wait it out, whatever
  `max_retries` the caller had configured.

  `HotdataClient.load_managed_table(file=...)` uploads inside the call and so
  does not hold the invariant. It is unwrapped and unaffected.

- fix(errors): classify a 409 by its `error.code` rather than by the status alone.

  `CONFLICT` is now terminal: it means the request cannot succeed as posted, so
  the previous behaviour spent the entire retry budget arriving at the same
  answer. `RESOURCE_LOCKED` stays transient. A 409 with no error envelope — a
  failed query result, say — is classified as before.

- fix(retry): honour `Retry-After`, and jitter the backoff.

  `Retry-After` is taken as a floor on the ramp, capped like the ramp so a bad
  header cannot park an attempt for an hour. Jitter of up to +50% is added on
  top and never subtracted, so a stated `Retry-After` is not undercut. Without
  it, writers that collided on one table retry in lockstep and collide again.

  This lengthens a 20-attempt budget from 285s to roughly 316-405s.

### Added

- `HotdataError` carries `status_code`, `code` and `retry_after_seconds`. The
  message is flattened and truncated for readability, so it could not serve as
  a discriminator; these can.

## [0.12.1] - 2026-08-18

### Fixed

- fix(load): submit managed loads as a job and poll, instead of holding one request open

## [0.12.0] - 2026-08-11

### Added

- Table storage layout, both directions. `add_managed_table()` and
  `create_managed_database()` take `partition_by` / `sorted_by`, and
  `managed_table_layout()` reads back what was actually declared as a
  `TableLayout`. `TablePartitionKey` and `TableSortKey` are re-exported so
  callers need one import.

  Both halves matter because a layout is fixed when the table is created and
  there is no alter path: a table declared without one keeps that shape until it
  is recreated and its data rewritten. So declaring is not enough — a caller has
  to be able to confirm it took, and to refuse to load when it cannot.

  `managed_table_layout()` raises `KeyError` for a table that is not declared,
  rather than returning an empty layout. "Not there" and "declared without a
  layout" lead to opposite decisions for a caller.

  Until now this package could not express a layout at all, which is why at least
  one consumer hand-built the HTTP request instead. The generated key models are
  passed through rather than wrapped, so the transform vocabulary stays exactly
  the API's.

### Changed

- Require `hotdata>=0.9.0,<0.10`. 0.9.0 is the first release whose models carry
  `partition_by` / `sorted_by` on the add-table request, the create-database
  table declarations, and the table-info response. On an older `hotdata` the
  fields would be silently dropped by the model and the table declared without a
  layout, returning success — which is the failure this feature exists to end.

## [0.11.0] - 2026-08-11

### Changed

- Cap the `hotdata` dependency to the current minor (`>=0.8.0,<0.9`). This
  package wraps a *generated* client, so an SDK minor can remove a model field
  or a `Configuration` keyword this wrapper passes, and there is no regeneration
  step here to surface it — an uncapped floor turns an SDK release into a break
  in this package, in versions already published. Raise the cap deliberately
  after running the suite against the new minor.

### Removed

- **Breaking:** session/sandbox support is gone. `HotdataClient` no longer accepts
  `session_id=`, `HotdataClient.session_id` is removed, `default_session_id()` and
  the `HOTDATA_SANDBOX` read are gone, and `list_workspaces()`,
  `resolve_workspace_selection()` and `pick_workspace()` lose their `session_id`
  parameter — note that loss is **positional**, so a three-argument call raises an
  arity `TypeError` rather than an unexpected-keyword one.
  `workspace_health_lines()` no longer emits a `sandbox` line.

  **Why now.** The server stopped enforcing session scoping some time ago, so the
  value already reached nothing. What makes removal urgent rather than tidy is
  that the SDK is dropping the `SessionId` security scheme: against that release
  `Configuration(session_id=...)` raises `TypeError` instead of setting a header,
  and this package passed it unconditionally — so every `HotdataClient(...)`
  would fail at construction. This package still pins `hotdata<0.9`, so nothing
  is broken today; the change is what lets the cap be raised later without a
  second breaking release.

  **Migrating.** Drop `session_id=` from `HotdataClient(...)`, stop reading
  `client.session_id`, stop setting `HOTDATA_SANDBOX`, and pass two arguments to
  the workspace helpers. Adapters that re-export session context in their own
  signatures — a `session_id=` parameter, a `session_id` metadata key — need to
  remove it from theirs too, which makes their own release breaking in turn.

- `hotdata_framework.http` and `default_http_retries()`. The module existed only
  to build the `retries=` policy removed under Fixed below, and had no other
  callers. It predates `hotdata._retry`, which supersedes it.

### Fixed

- A `POST` is no longer replayed because of a response status. `HotdataClient`
  passed its own `retries=` into `Configuration`, which replaced the generated
  SDK's policy wholesale with one listing `POST` in `allowed_methods` alongside
  a `(502, 503, 504)` forcelist — so an intermediary timing out a long request
  produced a silent, identical re-`POST` while the server was still working on
  the first one. For a load that is not idempotent: the duplicate collides with
  the write lock the original holds and is refused.

  The override is removed and the SDK's own default now applies. It is the
  policy this wrapper was reaching for — `hotdata._retry` retries a
  *pre-response* connection reset (the stale pooled socket case, where the
  server did no work) on any method, while leaving read timeouts and status
  retries idempotent-only.

## [0.10.0] - 2026-08-07

### Added

- `create_index(database, table, columns=..., index_type=...)` builds a `bm25`,
  `vector`, or `sorted` index on a managed table, matching `hotdata indexes create`
  in the CLI. The build is a background job whose submit call reports success even
  when the build later fails, so this polls the job and raises `RuntimeError` with
  its error message; `wait=False` returns as soon as the job is accepted. Returns
  `CreateIndexResult`, also exported.

## [0.9.0] - 2026-07-23

### Added

- `list_managed_tables`, `load_managed_table`, `add_managed_table`,
  `delete_managed_table`, `delete_managed_database`, and `execute_sql` accept an
  already-resolved `ManagedDatabase` (as returned by `create_managed_database`)
  in place of a name/id. When passed one, they skip the `get_database` /
  `list_databases` read probe. This lets an API key scoped to create + load but
  not read `/databases` bootstrap a managed database and load into it within a
  single run: the caller holds the `ManagedDatabase` from `create` and drives
  the load/add/query ops with zero reads. The name/id string path is unchanged.

## [0.8.0] - 2026-07-20

### Changed

- `load_managed_table` accepts a `key` argument — the merge key columns for
  `delete`/`update`/`upsert` loads, matched per-load instead of requiring a key
  declared at table creation. Omit it to use the table's declared key; ignored
  for `replace`/`append`. Requires `hotdata>=0.8.0`.

## [0.7.3] - 2026-07-16

### Changed

- `upload_parquet()` now delegates to the SDK's `hotdata.uploads.UploadsApi.upload_file()` instead of hand-rolling the session → PUT → finalize flow. Uploads gain concurrent part PUTs under a peak-memory budget, per-part retries, and ETag/size validation, making large uploads substantially faster. Errors still surface as `RuntimeError` with the underlying `ApiException` as the direct cause.

### Fixed

- `classify_sdk_error` now classifies HTTP 501 (Not Implemented) as terminal instead of transient — a permanent capability gap must not burn retries.

## [0.7.2] - 2026-07-15

### Removed

- The `POST /v1/files` fallback in `upload_parquet()`. Presigned upload sessions (`POST /v1/uploads`) are now required; a server that responds 501 raises a clear `RuntimeError` instead of silently falling back to the full-file-in-memory upload path.

## [0.7.1] - 2026-07-15

### Changed

- `upload_parquet()` now uses the presigned upload session API (`POST /v1/uploads`) instead of reading the entire file into memory before uploading. For multipart mode the file is streamed one `part_size` chunk at a time, eliminating the memory spike that caused OOM on large Parquet files. Falls back to `POST /v1/files` when the server returns 501.

## [0.7.0] - 2026-07-14

### Added

- `load_managed_table(..., mode=...)` selects the load mode (`replace` (default), `append`, `delete`, `update`, `upsert`) instead of always replacing the table. `replace`/`append` apply the upload directly; `delete`/`update`/`upsert` match rows by the table's declared key. Backward compatible — omitting `mode` still replaces.
- `create_managed_database(..., keys={table: [cols]})` and `add_managed_table(..., key=[cols])` declare a table's row-identity key, enabling the key-based load modes on it. Requires a `hotdata` client whose managed-table decl models carry `key` (see the dependency floor bump); tables declared without a key stay `replace`/`append`-only.

### Fixed

- `load_managed_table(..., mode="append")` is no longer retried on transient errors. Every other mode is idempotent, but retrying an `append` whose commit succeeded before the response was received would duplicate the uploaded rows; `append` now runs at most once. `mode` is also now typed as a literal of the accepted values.

## [0.6.3] - 2026-07-08

### Added

- `HotdataClient` and `ManagedDatabaseClient` accept `request_timeout` (seconds, or a `(connect, read)` pair). The generated SDK otherwise issues every HTTP request with urllib3's no-timeout default, so a stalled or unreachable server blocks the calling thread indefinitely; the new parameter applies a socket-level deadline to every call through the client while still honoring an explicit per-call `_request_timeout`. Also exported as `apply_default_request_timeout(api_client, timeout)` for callers holding a raw generated client. Default remains no timeout (behavior unchanged unless opted in).

## [0.6.2] - 2026-07-08

### Changed

- Repository text cleanup: the changelog and test docstrings no longer reference external issue trackers. No functional changes; 0.6.2 is byte-identical to 0.6.1 in package code.

## [0.6.1] - 2026-07-08

### Fixed

- `ManagedDatabaseClient.fetch_table` now carries the `X-Database-Id` scope header on the result poll, the query-run poll, and the Arrow fetch — not only on the query submit. Results of database-scoped queries are themselves database-scoped, so every read against an existing synced table (merge/append loads, dlt state restore) failed with `400: Bad Request` once the table had data.
- API error messages now include the response body (flattened, truncated to 500 chars). `400: Bad Request` alone hid the server's actual explanation.

### Changed

- The `hotdata` SDK dependency is now `>=0.6.0`, and the scope above rides its native `x_database_id` parameters (`get_result`, `get_query_run`, `get_result_arrow`). Note 0.6.0 made `x_database_id` **required** on `get_result_arrow`, so older framework releases cannot run on it.

## [0.6.0] - 2026-06-30

### Added

- `HotdataClient.add_managed_table(database, table, *, schema)` declares a new table on an existing managed database (wrapping the SDK `add_database_table` endpoint). This allows additive schema evolution without recreating the database.

## [0.5.0] - 2026-06-28

### Changed

- Adopt the `hotdata` 0.5.0 SDK surface (dependency bumped from `>=0.4.1` to `>=0.5.0`). The release is backward compatible for everything the framework uses; the only API changes are additive (a new optional `format` field on `LoadManagedTableRequest` and an optional `format` parameter on `ResultsApi.get_result`), so no framework code changes were required.

## [0.4.1] - 2026-06-26

### Fixed

- `ManagedDatabaseClient.fetch_table` now waits for the persisted result to reach `ready` before fetching it as Arrow on the synchronous query path (it previously only waited on the async path). This fixes failures on read-modify-write loads (merge/append) and state reads against the live backend, where the result is often still `processing` when the inline preview returns.

## [0.4.0] - 2026-06-26

### Changed

- **Renamed the distribution from `hotdata-runtime` to `hotdata-framework`** and the import package from `hotdata_runtime` to `hotdata_framework`. Consumers should depend on `hotdata-framework` and use `import hotdata_framework`. The GitHub repository is now `sdk-python-framework`.
- Added PyPI classifiers, keywords, and an updated description identifying the project as a Python framework.

## [0.3.0] - 2026-06-22

### Added

- Adopt the `hotdata` 0.4.1 SDK surface.
- New typed error-handling public API: `HotdataError`, `HotdataTerminalError`, `HotdataTransientError`, and `classify_sdk_error` (`hotdata_framework/errors.py`).
- `ManagedDatabaseClient` for managed database operations (`hotdata_framework/managed_client.py`).
- `py.typed` marker so downstream consumers pick up inline type information.

### Changed

- Bump the `hotdata` dependency pin to `>=0.4.1`.
- Add ruff and mypy tooling configuration and dev dependencies (`ruff>=0.5`, `mypy>=1.5`); apply ruff lint/format cleanup across the package.


## [0.2.4] - 2026-06-01

### Changed

- Release 0.2.4

## [0.2.3] - 2026-05-27

### Changed

- Release 0.2.3

## [0.2.2] - 2026-05-27

### Changed

- Release 0.2.2

## [0.2.1] - 2026-05-24

### Added

- `execute_sql` accepts an optional `database` keyword argument. When provided, the database name is resolved to an ID and sent as the `X-Database-Id` header so SQL can reference managed database tables as `"default"."<schema>"."<table>"`. Behaviour is unchanged when `database` is omitted.

## [0.2.0] - 2026-05-24

### Changed

- Switch managed database operations from the connections API to the dedicated `/databases` API (`hotdata>=0.2.3` required).
- `create_managed_database` first parameter renamed from `name` to `description` (keyword-only).
- `ManagedDatabase` dataclass: replace `name`/`source_type` fields with `description`/`default_connection_id`.
- `resolve_managed_database` tries direct ID lookup first, then falls back to a description scan.
- `list_managed_databases` now fetches all databases regardless of source type.
- `list_managed_tables`, `load_managed_table`, and `delete_managed_table` use `default_connection_id` instead of database `id` for connection-scoped operations.

### Added

- `create_managed_database` accepts an optional `expires_at` parameter.

### Removed

- `MANAGED_SOURCE_TYPE`, `build_managed_config`, and `create_connection_request` removed from the public API.

## [0.1.1] - 2026-05-19

### Added

- Managed database helpers on `HotdataClient`.

## [0.1.0] - 2026-05-06

### Added

- Initial release.
