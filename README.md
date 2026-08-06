# hotdata-framework

**A Python framework for building Hotdata integrations.**

Shared runtime primitives for Hotdata integrations: workspace/session semantics, execution context, query state, run history, and replayable result handles. Framework packages (Marimo, Jupyter, Streamlit, LangGraph) depend on this package.

Runtime boundary and guarantees are defined in `CONTRACT.md`.

## Features

- **Environment-driven client setup** — create clients from `HOTDATA_API_KEY`, optional `HOTDATA_API_URL`, `HOTDATA_WORKSPACE`, and `HOTDATA_SANDBOX`.
- **Workspace resolution** — choose an explicit workspace from env, otherwise discover workspaces and select the active workspace or first available workspace.
- **Sandbox/session propagation** — pass sandbox session context through the SDK via `X-Session-Id`.
- **HTTP resilience** — configure SDK retries for transient connection failures and retry SQL execution on stale pooled sockets.
- **SQL execution helper** — run SQL through `POST /v1/query`, poll async query runs when needed, and return a `QueryResult`.
- **Result utilities** — convert query results to records, pandas DataFrames, or metadata dictionaries for adapter display layers.
- **History helpers** — list recent results and query run history with normalized dataclasses.
- **Managed databases** — create Hotdata-owned catalogs, declare tables, upload parquet, and load managed tables (mirrors `hotdata databases` in the CLI).
- **Indexes** — build BM25, vector, or sorted indexes on managed tables (parity with `hotdata indexes create`), waiting on the background build and surfacing its failure instead of reporting a phantom success.
- **Health helpers** — build compact API/workspace health summaries for UI integrations.

Install:

```bash
uv pip install hotdata-framework
# or: pip install hotdata-framework
```

Example:

```bash
python examples/basic_usage.py
```

Development (uses **uv**; creates `.venv/` in this repo):

```bash
uv sync --locked
uv run pytest
```

`uv.lock` is checked in so CI can run `uv sync --locked`. The default **dev** group (pytest) is enabled via `[tool.uv] default-groups`.
