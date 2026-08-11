# hotdata-framework Contract

`hotdata-framework` is the framework-agnostic runtime contract for Hotdata integrations.

## Scope

This package provides shared primitives for:

- Environment and workspace resolution
- Query execution and polling
- Normalized tabular result handling
- Basic workspace health checks

## Public Runtime Contract

The supported import surface is:

- `HotdataClient`
- `QueryResult`
- `from_env`
- `workspace_health_lines`
- `default_api_key`
- `default_host`
- `explicit_workspace_id`
- `list_workspaces`
- `normalize_host`
- `pick_workspace`
- `resolve_workspace_selection`
- `ResultSummary`
- `RunHistoryItem`
- `WorkspaceSelection`
- `ManagedDatabase`
- `ManagedTable`
- `LoadManagedTableResult`
- `CreateIndexResult`
- `DEFAULT_SCHEMA`
- `is_parquet_path`

Adapters should import from `hotdata_framework` and treat this surface as the stable API.

## Semantic Guarantees

### `HotdataClient`

- Represents runtime context: API key, host, workspace.
- `from_env()` resolves runtime context from env vars and selected workspace.
- `execute_sql(sql)` returns `QueryResult` or raises `RuntimeError`/`TimeoutError`.
- `get_result(result_id)` returns a ready `QueryResult` and waits for readiness when needed.
- `connections()` returns the connections API wrapper for adapter UI/status features.
- `query_runs()` returns the query-runs API wrapper for adapter history views.
- `results()` returns the results API wrapper for adapter result pickers.
- `list_recent_results(...)` returns normalized `ResultSummary` entries.
- `list_run_history(limit=...)` returns normalized `RunHistoryItem` entries.
- `list_qualified_table_names(...)` returns sorted fully qualified table names.
- `columns_for_qualified(qualified, connection_id=...)` resolves table columns, and
  adapters should pass `connection_id` when known.
- `uploads()` returns the uploads API wrapper for parquet staging.
- `list_managed_databases()` returns all databases via the `/databases` API.
- `resolve_managed_database(name_or_id)` resolves a database by id (direct lookup) or description (list scan). A `403` from `/databases` surfaces as `RuntimeError` (forbidden, not absent), preserving the underlying `ApiException` as `__cause__`.
- `create_managed_database(description=..., schema=..., tables=..., expires_at=...)` creates a database via the `/databases` API and optionally declares tables up front. Returns a `ManagedDatabase` (id + `default_connection_id`) sufficient to load without a further read.
- `delete_managed_database(name_or_id)` deletes a database via the `/databases` API.
- `list_managed_tables(database, schema=...)` lists tables in a managed database.
- `upload_parquet(path)` uploads a local parquet file and returns an upload id.
- `load_managed_table(database, table, schema=..., upload_id=..., file=...)` publishes parquet data into a declared managed table.
- `delete_managed_table(database, table, schema=...)` deletes a managed table.
- `create_index(database, table, schema=..., columns=..., index_type=..., index_name=...)` builds a `"sorted"`, `"bm25"`, or `"vector"` index on a managed table and returns a `CreateIndexResult`. It is the framework-side equivalent of the CLI's `hotdata indexes create`; indexing a table on a plain (non-managed) connection is out of scope. `index_name` defaults to `{table}_{columns}_{index_type}`, matching the CLI's derivation when `--name` is omitted. `index_type` is required rather than defaulting to the API's `"sorted"`. The build runs as a background job; the call polls it to a terminal state and raises `RuntimeError` with the job's `error_message` when it fails, because the submit call reports success regardless. `wait=False` returns as soon as the job is accepted, with `status="pending"` and a `job_id` for the caller to poll. For `index_type="vector"`, omitting `embedding_provider_id` indexes an existing vector column and `metric` (`"l2"`, `"cosine"`, `"dot"`) selects the distance function the index accelerates — a query using a different function silently falls back to a full scan; setting `embedding_provider_id` indexes a source *text* column instead, and the returned `source_column` names the column to pass to `vector_distance`. Argument combinations the server would silently ignore raise `ValueError` before any request is sent.
- The `database` argument of `list_managed_tables`, `load_managed_table`, `add_managed_table`, `delete_managed_table`, `delete_managed_database`, `create_index`, and `execute_sql` accepts a name/id **or** an already-resolved `ManagedDatabase`. Passing a `ManagedDatabase` skips the name/id read probe, so a create-scoped key that cannot read `/databases` can load into a database it just created.

### `QueryResult`

- Canonical tabular result model with `columns`, `rows`, and `row_count`.
- Carries server identifiers and execution metadata when available.
- `to_pandas()` converts to a DataFrame with stable column ordering.
- `to_records(max_rows=...)` returns row dicts keyed by column names.
- `metadata_dict()` returns normalized result metadata for adapter rendering.

### Env Resolution

- `default_api_key()` reads `HOTDATA_API_KEY`.
- `default_host()` reads `HOTDATA_API_URL` (default: `https://api.hotdata.dev`) and normalizes it.
- `explicit_workspace_id()` reads `HOTDATA_WORKSPACE` (workspace public id).
- `pick_workspace()` prefers explicit env workspace, then active workspace, then first workspace.
- `resolve_workspace_selection()` is the canonical workspace selection algorithm. It returns `WorkspaceSelection` with selected workspace id, selection source, and discovered workspaces when auto-selected.

## Adapter Responsibilities

Framework packages (Jupyter, Marimo, LangChain, LangGraph, LlamaIndex, Streamlit) own:

- Framework-native lifecycle and state management
- Rendering/UI concerns
- Tool/agent wrappers and callback integration

They should not duplicate runtime env/workspace/query semantics.

## Runtime Non-Goals

`hotdata-framework` does not define framework UI primitives and does not require framework dependencies.

## Versioning Policy

- Backward-incompatible contract changes require a major version bump.
- Additive contract changes are minor versions.
- Bug fixes that preserve contract semantics are patch versions.

## Enforcement

Contract stability is enforced by tests that verify the public export surface and key behavioral invariants.
