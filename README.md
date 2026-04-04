# PJM-data-dash

A serverless analytics dashboard for PJM electricity market data, modeled after
[djouallah/analytics-as-code](https://github.com/djouallah/analytics-as-code).
Everything — ingest, transforms, tests, and the dashboard itself — lives in Git
and runs on GitHub Actions. No servers, no databases, no accounts required to
get started.

---

## Architecture overview

```
EIA bulk CSV downloads (DA/RT LMPs, hourly load — no auth required)
  │
  ├─ GitHub Actions daily (ingest.yml)
  │    └─ scripts/backfill_eia.py --refresh  → Parquet files committed to `data` branch
  │
  ├─ GitHub Actions daily + after ingest (transform.yml)
  │    ├─ dbt run --target prod  → reads Parquet, builds mart tables (DuckDB in-memory)
  │    └─ scripts/export_duckdb.py  → writes optimised .duckdb files to dashboard/
  │         └─ deploys to GitHub Pages
  │
  └─ Browser: dashboard/index.html
       ├─ DuckDB-WASM: client-side SQL queries against .duckdb files
       ├─ ECharts: interactive charts (LMPs, load, regulation)
       └─ OPFS cache: avoids re-downloading files on revisit
```

### Inspired by analytics-as-code

| analytics-as-code (AEMO) | PJM-data-dash |
|---|---|
| AEMO SCADA + price feeds | EIA bulk CSVs (+ optional PJM API) |
| Apache Iceberg REST catalog | Parquet files on `data` branch |
| dbt-duckdb incremental models | dbt-duckdb incremental models |
| GitHub Actions scheduler | GitHub Actions scheduler |
| DuckDB-WASM + ECharts dashboard | DuckDB-WASM + ECharts dashboard |

The main simplification: we skip the Iceberg catalog and store raw data as
Parquet files directly on a `data` git branch. Iceberg can be layered on later
if write throughput demands it.

---

## Data sources

### EIA bulk CSV (primary — zero auth)

The pipeline uses EIA's pre-built annual CSV files. No API key, no account,
no pagination — just direct HTTP downloads:

```
https://www.eia.gov/electricity/wholesalemarkets/csv/pjm_lmp_da_hr_zones_{YYYY}.csv
https://www.eia.gov/electricity/wholesalemarkets/csv/pjm_lmp_da_hr_hubs_{YYYY}.csv
https://www.eia.gov/electricity/wholesalemarkets/csv/pjm_lmp_rt_5min_zones_{YYYY}Q{Q}.csv
https://www.eia.gov/electricity/wholesalemarkets/csv/pjm_load_act_hr_{YYYY}.csv
```

| File | Grain | Coverage | Notes |
|---|---|---|---|
| `pjm_lmp_da_hr_zones` | Hourly | 2022–present | DA LMPs by zone |
| `pjm_lmp_da_hr_hubs` | Hourly | 2022–present | DA LMPs by hub |
| `pjm_lmp_rt_5min_zones` | 5-min | 2022–present | RT LMPs (quarterly files) |
| `pjm_load_act_hr` | Hourly | 2022–present | Metered zonal load |

EIA updates these files approximately weekly. `ingest.yml` re-downloads the
current year's files daily to pick up new data.

Browse the full catalog: `https://www.eia.gov/electricity/wholesalemarkets/data.php?rto=pjm`

### Scope

- **Zone**: DUQ (Duquesne Light)
- **Hub**: Western Hub (`pnode_id=51217`)

### PJM Data Miner 2 API (optional — requires free API key)

For load forecasts and higher-frequency data not available from EIA, you can
optionally use the PJM API via `scripts/fetch_pjm.py`. Register for a free
key at `https://apiportal.pjm.com`.

| Feed | Grain | Retention | Notes |
|---|---|---|---|
| `da_hrl_lmps` | Hourly | 731 days | Day-ahead LMPs by pnode/zone |
| `rt_hrl_lmps` | Hourly | 731 days | Real-time LMPs |
| `hrl_load_metered` | Hourly | 731 days | Metered zonal load |
| `load_frcstd_7_day` | 30-min updates | Rolling 7 days | Load forecast (EIA does not have this) |

---

## Project structure

```
PJM-data-dash/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── staging/
│   │   ├── stg_da_lmps.sql          # normalise DA LMP feed
│   │   ├── stg_rt_lmps.sql          # normalise RT LMP feed
│   │   ├── stg_load.sql             # normalise metered load feed
│   │   └── stg_load_forecast.sql    # normalise load forecast feed
│   ├── dimensions/
│   │   ├── dim_pnode.sql            # zone/hub lookup
│   │   └── dim_calendar.sql         # date/hour calendar
│   └── marts/
│       ├── mart_lmps.sql            # merged DA+RT fact with spread
│       └── mart_load.sql            # load actual vs forecast
├── tests/
│   └── schema.yml                   # uniqueness + not-null tests
├── scripts/
│   ├── backfill_eia.py              # EIA CSV → Parquet (primary ingest)
│   ├── fetch_pjm.py                 # PJM API → Parquet (optional, needs API key)
│   └── export_duckdb.py             # dbt marts → dashboard .duckdb files
├── dashboard/
│   └── index.html                   # DuckDB-WASM + ECharts dashboard
└── .github/
    └── workflows/
        ├── ci.yml                   # dbt build on PR (in-memory, no live data)
        ├── ingest.yml               # daily: EIA CSV → Parquet
        ├── backfill.yml             # one-time: bootstrap 2+ years of history
        └── transform.yml            # daily + after ingest: dbt + export + deploy
```

---

## Dashboard features

| Chart | Description |
|---|---|
| LMP time series | DA vs RT hourly prices, Western Hub + DUQ zone |
| DA–RT spread | Hourly price difference (positive = DA > RT) |
| Load vs forecast | Metered MW vs 7-day forecast for DUQ zone |
| Price duration curve | Hours sorted by price for any date range |
| Regulation prices | Hourly regulation market clearing prices |

**Adaptive resolution** (mirrors analytics-as-code pattern):
- ≤ 7 days selected → 5-min grain (RT only)
- 8–30 days → hourly grain
- > 30 days → daily aggregates

---

## Dashboard .duckdb file tiers

| File | Content | Refresh |
|---|---|---|
| `pjm_dim.duckdb` | Calendar + pnode dimensions | Daily |
| `pjm_today.duckdb` | Last 7 days, 5-min RT grain | Every 30 min |
| `pjm_data_{YYYY}H{1,2}.duckdb` | Historical half-yearly, hourly | Daily |
| `pjm_daily_agg.duckdb` | Full history, daily grain | Daily |

---

## Local development

```bash
# Install dependencies
pip install dbt-duckdb pandas pyarrow requests

# Run CI build (no live data needed — uses synthetic seed data)
dbt build --target ci

# Download EIA data (DA LMPs + load for all available years)
python scripts/backfill_eia.py --output data/

# Run transforms against local Parquet files
dbt run --target prod

# Export dashboard files
python scripts/export_duckdb.py --data-dir data/ --output dashboard/

# Open dashboard
open dashboard/index.html
```

---

## GitHub Actions setup

1. Fork this repo
2. Enable GitHub Pages (Settings → Pages → source: `gh-pages` branch)
3. Run the **backfill.yml** workflow once to bootstrap 2+ years of history
4. Optionally add `PJM_API_KEY` secret (from `apiportal.pjm.com`) if you
   want load forecasts via `fetch_pjm.py` — not required for the default pipeline

The workflows run automatically:
- **ci.yml** — validates SQL on every PR
- **ingest.yml** — refreshes current-year EIA data daily (no API key needed)
- **transform.yml** — runs dbt and deploys dashboard daily

---

## Service worker (required for DuckDB-WASM)

GitHub Pages does not allow custom response headers, but DuckDB-WASM requires
[cross-origin isolation](https://web.dev/cross-origin-isolation-guide/) to use
`SharedArrayBuffer`. `dashboard/coi-serviceworker.js` solves this by
intercepting same-origin responses and injecting:

```
Cross-Origin-Embedder-Policy: credentialless
Cross-Origin-Opener-Policy:   same-origin
```

`index.html` registers the worker before loading DuckDB and reloads once on
first visit so the headers take effect. No server config changes are needed.

---

## Implementation roadmap

| Phase | Status | Goal |
|---|---|---|
| 1 | ✅ done | Scaffold: dbt models, ingest script, CI workflow, dashboard shell |
| 2 | ✅ done | EIA-based ingest: DA LMPs + hourly load from EIA (zero auth) |
| 3 | pending | Dashboard v1: wire live data, LMP time series + load chart |
| 4 | pending | Dashboard v2: price duration curves, regulation prices, date range drill-down |
| 5 | optional | Migrate Parquet storage to Apache Iceberg for scalability |

---

## Technology credits

- [djouallah/analytics-as-code](https://github.com/djouallah/analytics-as-code) — architecture reference
- [dbt-duckdb](https://github.com/duckdb/dbt-duckdb) — SQL transforms
- [DuckDB-WASM](https://duckdb.org/docs/api/wasm/overview) — browser-side SQL
- [Apache ECharts](https://echarts.apache.org/) — visualisation
- [EIA Wholesale Markets](https://www.eia.gov/electricity/wholesalemarkets/) — primary data source (zero auth)
- [PJM Data Miner 2](https://dataminer2.pjm.com/) — optional API for load forecasts
